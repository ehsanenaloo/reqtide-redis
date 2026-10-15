-- Read-only, advisory inspection of a retry budget.
-- KEYS: 1 registry metadata hash, 2 budget partition hash, 3 pending-reservation sorted set.
-- ARGV: 1 registry identity, 2 schema version, 3 capacity (tokens), 4 refill (tokens), 5 refill interval (microseconds),
--       6 settings fingerprint.
-- Reply: { '1', available tokens, '' } one reservation would be granted now; { '0', available tokens, wait microseconds
-- or '' } it would not (an empty wait: held reservations leave no headroom). It writes nothing: no record is allocated, no
-- terminal record is created, no cleanup is done and the watermark is not advanced; the accrue-then-expire ordering is
-- applied to local values at the protected effective time. With no stored state it reads as a fresh full bucket.

local meta_key, pkey, akey = KEYS[1], KEYS[2], KEYS[3]

registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local capacity_tokens = rt_state.int_arg(ARGV[3], 1)
local settings = rt_rb.params(capacity_tokens, rt_state.int_arg(ARGV[4], 1), rt_state.int_arg(ARGV[5], 1))
local fingerprint = ARGV[6]
if type(fingerprint) ~= 'string' or string.len(fingerprint) < 1 or string.len(fingerprint) > 200 then
    error(redis.error_reply('RT_CONFIG the settings fingerprint is invalid'), 0)
end

local state = rt_rb.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state == nil then
    return { '1', exact_int.format_int(capacity_tokens), '' }
end
if state.fp ~= fingerprint then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to different retry budget settings'), 0)
end

local balance, pending, remainder = rt_rb.view(state, akey, es, eu)
local available = exact_int.mul_add_div_floor_capped(balance, 1, 0, rt_rb.COST, exact_int.EXACT_MAX)
if balance >= rt_rb.COST then
    return { '1', exact_int.format_int(available), '' }
end
local wait = rt_rb.wait(state, balance, pending, remainder)
return { '0', exact_int.format_int(available), wait == nil and '' or exact_int.format_int(wait) }
