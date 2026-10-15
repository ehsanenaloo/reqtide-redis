-- Record the verdict of a circuit permit, or abandon it.
-- KEYS: as circuit_acquire.lua.
-- ARGV: 1 to 13 as circuit_acquire.lua, then 14 mode (record or abandon), 15 verdict (s success, f failure, n neutral;
--       any letter is ignored for abandon), 16 attempt (32 hex), 17 owner (32 hex), 18 incarnation (32 hex),
--       19 generation, 20 expiry seconds, 21 expiry microseconds (the permit's backend expiry, exact), 22 kind (c or p).
-- Reply (four strings): { status, from-state, to-state, reason letter } with status 1 applied, 2 already finalized,
-- 3 stale ownership; the fact is empty when the call moved no circuit. Registered errors: RT_NO_REGISTRY, RT_IDENTITY,
-- RT_CORRUPT, RT_CONFIG, RT_CAPACITY (a time bucket reached its sample ceiling).
--
-- Only a matching active permit (incarnation, generation, attempt, kind and exact expiry, not yet expired) changes
-- anything. Its slot becomes a terminal record in place, so finalization allocates nothing. A permit whose kind no longer
-- fits the phase is consumed and stale. A neutral verdict and an abandon give no sample and no recovery credit.

local meta_key, pkey, akey, qkey, xkey, wkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
rt_cb.params(
    ARGV[3], rt_state.int_arg(ARGV[4], 1), rt_state.int_arg(ARGV[5], 0), rt_state.int_arg(ARGV[6], 1),
    rt_state.int_arg(ARGV[7], 0), rt_state.int_arg(ARGV[8], 1), rt_state.int_arg(ARGV[9], 1),
    rt_state.int_arg(ARGV[10], 1), rt_state.int_arg(ARGV[11], 1), rt_state.int_arg(ARGV[12], 1))
local fingerprint = ARGV[13]
local mode = ARGV[14]
local verdict = ARGV[15]
if (mode ~= 'record' and mode ~= 'abandon') or (verdict ~= 's' and verdict ~= 'f' and verdict ~= 'n') then
    error(redis.error_reply('RT_CONFIG the finalization mode or verdict is invalid'), 0)
end
local attempt = rt_state.hex_arg(ARGV[16])
local owner = rt_state.hex_arg(ARGV[17])
local incarnation = rt_state.hex_arg(ARGV[18])
local generation = rt_state.int_arg(ARGV[19], 1)
local permit_s = rt_state.int_arg(ARGV[20], 0)
local permit_u = rt_state.int_arg(ARGV[21], 0)
local kind = ARGV[22]
if kind ~= 'c' and kind ~= 'p' then
    error(redis.error_reply('RT_CONFIG the permit kind is invalid'), 0)
end
if type(fingerprint) ~= 'string' or string.len(fingerprint) < 1 or string.len(fingerprint) > 300 then
    error(redis.error_reply('RT_CONFIG the settings fingerprint is invalid'), 0)
end

local state = rt_cb.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state ~= nil and state.fp ~= fingerprint then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to different circuit settings'), 0)
end

local ctx = {}

local function reply(status)
    local from, to, reason = rt_cb.fact(ctx)
    return { status, from, to, reason }
end

local function write_back()
    if state ~= nil then
        state.ls_s, state.ls_u = es, eu
        rt_cb.save(state)
        rt_cb.index(xkey, state)
    end
    rt_state.save_counters(meta_key, meta)
    registry.commit_watermark(meta_key, meta, es, eu)
end

if state == nil then
    write_back()
    return reply('3')
end

-- Computed before any mutation, so a deadline that cannot be represented fails the call with no partial change.
local counts = mode == 'record' and verdict ~= 'n'
local failure = mode == 'record' and verdict == 'f'
local closed_kind = kind == 'c'
local until_s, until_u
if counts and (closed_kind or failure) then
    until_s, until_u = rt_time.add_micros(es, eu, state.od)
end

if counts and closed_kind then
    rt_cb.check_sample_room(state, wkey, es, eu)
end

-- Records the call reads later are read now, before the first write, so a damaged one fails the call with nothing changed.
if state.inc == incarnation then
    rt_cb.permit(state, owner)
    rt_cb.terminal(state, owner)
    if counts and closed_kind then
        rt_cb.totals(state, wkey, es, eu)
    end
end

rt_cb.expire(ctx, meta, state, akey, wkey, es, eu)
rt_cc.drain_tombstones(meta, state, qkey, es, eu)

if state.inc ~= incarnation then
    write_back()
    return reply('3')
end

local xs, xu, held_attempt, held_kind, held_generation = rt_cb.permit(state, owner)
if xs == nil then
    local ds, du, tombstone_attempt, tombstone_generation = rt_cb.terminal(state, owner)
    local status = '3'
    if ds ~= nil and not rt_state.reached(ds, du, es, eu) and tombstone_attempt == attempt and tombstone_generation == generation then
        status = '2'
    end
    write_back()
    return reply(status)
end
if held_attempt ~= attempt or held_kind ~= kind or held_generation ~= generation or xs ~= permit_s or xu ~= permit_u then
    write_back()
    return reply('3')
end

state.la_s, state.la_u = es, eu
if (closed_kind and state.ph ~= rt_cb.CLOSED) or (not closed_kind and state.ph ~= rt_cb.HALF_OPEN) then
    rt_cb.consume(meta, state, akey, qkey, owner, attempt, kind, generation, es, eu)
    write_back()
    return reply('3')
end

rt_cb.consume(meta, state, akey, qkey, owner, attempt, kind, generation, es, eu)
if not counts then
    rt_cb.settle(ctx, meta, state, akey, wkey)
    write_back()
    return reply('1')
end

if closed_kind then
    rt_cb.add_sample(state, wkey, failure, es, eu)
    local samples, failures = rt_cb.totals(state, wkey, es, eu)
    if samples >= state.mn and exact_int.compare_products(failures, state.rd, samples, state.rn) >= 0 then
        rt_cb.enter_open(ctx, meta, state, akey, until_s, until_u, 'T')
    end
elseif failure then
    rt_cb.enter_open(ctx, meta, state, akey, until_s, until_u, 'P')
else
    state.ps = state.ps + 1
    rt_cb.settle(ctx, meta, state, akey, wkey)
end
write_back()
return reply('1')
