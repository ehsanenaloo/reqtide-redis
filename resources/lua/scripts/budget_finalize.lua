-- Commit or release a retry reservation.
-- KEYS: 1 registry metadata hash, 2 budget partition hash, 3 pending-reservation sorted set, 4 registry tombstone queue,
--       5 registry rate and budget index (sorted set).
-- ARGV: 1 registry identity, 2 schema version, 3 capacity (tokens), 4 refill (tokens), 5 refill interval (microseconds),
--       6 settings fingerprint, 7 mode (commit or release), 8 attempt (32 hex), 9 owner (32 hex), 10 incarnation (32 hex),
--       11 generation, 12 expiry seconds, 13 expiry microseconds (the reservation's backend expiry, exact).
-- Reply: { '1' } committed, { '2' } released, { '3' } already committed, { '4' } already released, { '5' } expired,
-- { '6' } stale ownership. Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG.
--
-- Only a matching pending reservation (incarnation, generation, owner, attempt and exact expiry, not yet expired) changes
-- anything; its slot becomes the terminal record in place, so finalization allocates nothing and cannot fail on a ceiling.
-- A repeat finds the record while it is retained and nothing afterwards. A release refunds the cost capped at the new
-- headroom; a commit refunds nothing.

local meta_key, pkey, akey, qkey, xkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
rt_rb.params(rt_state.int_arg(ARGV[3], 1), rt_state.int_arg(ARGV[4], 1), rt_state.int_arg(ARGV[5], 1))
local fingerprint = ARGV[6]
local mode = ARGV[7]
if mode ~= 'commit' and mode ~= 'release' then
    error(redis.error_reply('RT_CONFIG the finalization mode is invalid'), 0)
end
local attempt = rt_state.hex_arg(ARGV[8])
local owner = rt_state.hex_arg(ARGV[9])
local incarnation = rt_state.hex_arg(ARGV[10])
local generation = rt_state.int_arg(ARGV[11], 1)
local permit_s = rt_state.int_arg(ARGV[12], 0)
local permit_u = rt_state.int_arg(ARGV[13], 0)
if type(fingerprint) ~= 'string' or string.len(fingerprint) < 1 or string.len(fingerprint) > 200 then
    error(redis.error_reply('RT_CONFIG the settings fingerprint is invalid'), 0)
end

local state = rt_rb.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state ~= nil and state.fp ~= fingerprint then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to different retry budget settings'), 0)
end

local function write_back()
    rt_state.save_counters(meta_key, meta)
    registry.commit_watermark(meta_key, meta, es, eu)
end

if state == nil or state.inc ~= incarnation or state.gen ~= generation then
    write_back()
    return { '6' }
end

-- The owner's records are read before the first write, so a damaged one fails the call with nothing changed.
rt_rb.pending(state, owner)
rt_rb.terminal(state, owner)

-- Accrue with the old pending cost, then expire (including this owner when its lease ended at or before now).
rt_rb.transition(meta, state, akey, qkey, es, eu)
rt_cc.drain_tombstones(meta, state, qkey, es, eu)

local status = '6'
local xs, xu, pending_attempt = rt_rb.pending(state, owner)
if xs ~= nil then
    if xs == permit_s and xu == permit_u and pending_attempt == attempt then
        rt_rb.finalize(meta, state, akey, qkey, owner, es, eu, mode == 'commit')
        status = mode == 'commit' and '1' or '2'
    end
else
    local ds, du, letter = rt_rb.terminal(state, owner)
    if ds ~= nil and not rt_state.reached(ds, du, es, eu) then
        if letter == 'c' then
            status = '3'
        elseif letter == 'r' then
            status = '4'
        else
            status = '5'
        end
    end
end

state.ls_s, state.ls_u = es, eu
rt_rb.save(state)
rt_rb.index(xkey, state)
write_back()
return { status }
