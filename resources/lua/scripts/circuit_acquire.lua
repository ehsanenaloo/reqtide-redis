-- Acquire a circuit permit.
-- KEYS: 1 registry metadata hash, 2 circuit partition hash, 3 active-permit sorted set, 4 registry tombstone queue,
--       5 registry rate, budget and circuit index (sorted set), 6 time-bucket hash.
-- ARGV: 1 registry identity, 2 schema version, 3 window kind (c count, t time), 4 window (samples, or microseconds),
--       5 bucket granularity (microseconds, 0 for a count window), 6 minimum samples, 7 ratio numerator, 8 ratio
--       denominator, 9 open duration (microseconds), 10 probe capacity, 11 probe success threshold, 12 permit lease
--       (microseconds), 13 settings fingerprint, 14 attempt (32 hex), 15 owner (32 hex), 16 incarnation to use if the
--       partition has to be created (32 hex, chosen by the caller from a secure source).
-- Reply (nine strings): { '1', incarnation, generation, expiry seconds, expiry microseconds, kind letter (c or p), then
-- the transition fact } accepted; { '2', wait microseconds, '', '', '', '', fact } open; { '3', '', ... , fact } probe
-- capacity exhausted. The fact is three strings, from-state, to-state and reason letter, or three empty strings.
-- Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG (domain, settings differ from the identity's, the
-- permit owner is in use by another attempt), RT_CAPACITY (a ceiling, after reclaiming).
--
-- A call that finds the same attempt already holding the owner answers with the same permit. Every check that can fail
-- runs before the first write; the reclamation of expired permits and terminal records is neutral.

local meta_key, pkey, akey, qkey, xkey, wkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local settings = rt_cb.params(
    ARGV[3], rt_state.int_arg(ARGV[4], 1), rt_state.int_arg(ARGV[5], 0), rt_state.int_arg(ARGV[6], 1),
    rt_state.int_arg(ARGV[7], 0), rt_state.int_arg(ARGV[8], 1), rt_state.int_arg(ARGV[9], 1),
    rt_state.int_arg(ARGV[10], 1), rt_state.int_arg(ARGV[11], 1), rt_state.int_arg(ARGV[12], 1))
local fingerprint = ARGV[13]
local attempt = rt_state.hex_arg(ARGV[14])
local owner = rt_state.hex_arg(ARGV[15])
local new_incarnation = rt_state.hex_arg(ARGV[16])
if type(fingerprint) ~= 'string' or string.len(fingerprint) < 1 or string.len(fingerprint) > 300 then
    error(redis.error_reply('RT_CONFIG the settings fingerprint is invalid'), 0)
end

local state = rt_cb.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
local expiry_s, expiry_u = rt_time.add_micros(es, eu, settings.lz)

if state ~= nil then
    if state.fp ~= fingerprint then
        error(redis.error_reply('RT_CONFIG the policy identity is already bound to different circuit settings'), 0)
    end
    local xs, xu, held_attempt = rt_cb.permit(state, owner)
    if xs ~= nil and not rt_state.reached(xs, xu, es, eu) and held_attempt ~= attempt then
        error(redis.error_reply('RT_CONFIG the permit owner token is already in use by another attempt'), 0)
    end
end

local ctx = {}
local current = state or { key = pkey, na = 0, nt = 0 }

local function reply(status, a, b, c, d, e)
    local from, to, reason = rt_cb.fact(ctx)
    return { status, a or '', b or '', c or '', d or '', e or '', from, to, reason }
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

if state ~= nil then
    state.la_s, state.la_u = es, eu
    rt_cb.expire(ctx, meta, state, akey, wkey, es, eu)
end
rt_cc.drain_tombstones(meta, current, qkey, es, eu)

if state ~= nil then
    local xs, xu, held_attempt, kind = rt_cb.permit(state, owner)
    if xs ~= nil and held_attempt == attempt then
        write_back()
        return reply('1', state.inc, exact_int.format_int(state.gen), exact_int.format_int(xs), exact_int.format_int(xu), kind)
    end
    if state.ph == rt_cb.OPEN and not rt_state.reached(state.ou_s, state.ou_u, es, eu) then
        local wait = rt_time.diff_micros(state.ou_s, state.ou_u, es, eu)
        write_back()
        return reply('2', exact_int.format_int(wait))
    end
    if state.ph == rt_cb.HALF_OPEN and (state.ap >= state.pc or state.ps >= state.pt) then
        write_back()
        return reply('3')
    end
end

-- From here the call admits: a Closed sample, a probe in HalfOpen, or the Open to HalfOpen transition plus a probe.
local function has_record_room()
    return meta.records < meta.max_records
end
if not has_record_room() then
    rt_cc.evict_tombstones(meta, current, qkey, has_record_room)
end
if not has_record_room() then
    write_back()
    error(redis.error_reply('RT_CAPACITY the state registry is at capacity; new allocation rejected'), 0)
end
-- One partition may hold active permits up to the record ceiling minus a quarter of it, so a slow dependency with many
-- calls in flight cannot take the records every other circuit needs (the same rule as the process-local backend).
if state ~= nil and state.ph == rt_cb.CLOSED and state.na >= math.max(1, meta.max_records - math.floor(meta.max_records / 4)) then
    write_back()
    error(redis.error_reply('RT_CAPACITY the circuit partition holds the most active permits one partition may hold'), 0)
end
if state == nil and meta.partitions >= meta.max_partitions then
    rt_rate.reclaim(meta, xkey, qkey, es, eu)
    if meta.partitions >= meta.max_partitions then
        write_back()
        error(redis.error_reply('RT_CAPACITY the state registry is at capacity; new allocation rejected'), 0)
    end
end

if state == nil then
    state = rt_cb.create(pkey, fingerprint, new_incarnation, settings, es, eu)
    meta.partitions = meta.partitions + 1
end
if state.ph == rt_cb.OPEN then
    rt_cb.enter_half_open(ctx, meta, state, akey)
end
local kind = state.ph == rt_cb.CLOSED and 'c' or 'p'
rt_cb.add_permit(meta, state, akey, owner, attempt, kind, expiry_s, expiry_u)
write_back()
return reply('1', state.inc, exact_int.format_int(state.gen), exact_int.format_int(expiry_s), exact_int.format_int(expiry_u), kind)
