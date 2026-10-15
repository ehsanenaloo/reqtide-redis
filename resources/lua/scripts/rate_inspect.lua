-- Read-only inspection of a shared rate limit.
-- KEYS: 1 registry metadata hash, 2 rate partition hash.
-- ARGV: as rate_consume.lua (the last one is the requested cost, which may exceed the capacity).
-- Reply: { '1', available } the cost would be accepted; { '2', available, retry microseconds, capped } it would not;
-- { '0', available } the cost is above the capacity (impossible, no wait). It never debits, resets, allocates, extends
-- retention, reclaims or advances the watermark; with no stored state it evaluates the documented fresh state at the
-- protected effective time (max of watermark, stored time and server time).

local meta_key, pkey = KEYS[1], KEYS[2]

registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local kind = ARGV[3]
local fingerprint = ARGV[4]
local capacity = rt_state.int_arg(ARGV[5], 1)
local a = rt_state.int_arg(ARGV[6], 1)
local b = rt_state.int_arg(ARGV[7], 0)
local cost = rt_state.int_arg(ARGV[8], 1)
if type(fingerprint) ~= 'string' or string.len(fingerprint) < 1 or string.len(fingerprint) > 200 then
    error(redis.error_reply('RT_CONFIG the settings fingerprint is invalid'), 0)
end
local p = rt_rate.params(kind, capacity, a, b)

local state = rt_rate.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state ~= nil and state.fp ~= fingerprint then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to different rate settings'), 0)
end

local view = rt_rate.project(p, state, es, eu)
if cost > capacity then
    return { '0', exact_int.format_int(rt_rate.available(p, view)) }
end
local result = rt_rate.evaluate(p, view, cost)
if result.accepted then
    return { '1', exact_int.format_int(result.available) }
end
return {
    '2',
    exact_int.format_int(result.available),
    exact_int.format_int(result.retry),
    result.capped and '1' or '0',
}
