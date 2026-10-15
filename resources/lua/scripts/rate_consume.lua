-- Consume tokens from a shared rate limit.
-- KEYS: 1 registry metadata hash, 2 rate partition hash, 3 registry rate and budget index (sorted set), 4 registry tombstone queue.
-- ARGV: 1 registry identity, 2 schema version, 3 algorithm (tb, fw, fh, sc), 4 settings fingerprint, 5 capacity,
--       6 refill tokens (tb) or window microseconds, 7 refill interval microseconds (tb) or 0, 8 cost (whole tokens).
-- Reply: { '1', available before the cost } accepted; { '2', available, retry microseconds, '1' if the wait does not fit
-- the exact range else '0' } rejected. Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG (domain,
-- settings differ from the identity's, cost above the capacity), RT_CAPACITY (partition ceiling, after reclaiming idle
-- fresh-equivalent partitions).
--
-- A rejection still stores the projected (refilled or rolled) state: it is exactly equivalent and keeps the refill
-- remainder. The effective time is the larger of the registry watermark, the partition's last effective time and the
-- server time, so a backward clock never refills or resets anything.

local meta_key, pkey, xkey, qkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]

registry.require_access(false)
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
if cost > capacity then
    error(redis.error_reply('RT_CONFIG the cost is above the capacity'), 0)
end

local state = rt_rate.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state ~= nil and state.fp ~= fingerprint then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to different rate settings'), 0)
end

local view = rt_rate.project(p, state, es, eu)
local result = rt_rate.evaluate(p, view, cost)

if state == nil then
    if meta.partitions >= meta.max_partitions then
        rt_rate.reclaim(meta, xkey, qkey, es, eu)
    end
    if meta.partitions >= meta.max_partitions then
        rt_state.save_counters(meta_key, meta)
        registry.commit_watermark(meta_key, meta, es, eu)
        error(redis.error_reply('RT_CAPACITY the state registry is at capacity; new allocation rejected'), 0)
    end
    meta.partitions = meta.partitions + 1
    state = { key = pkey, fp = fingerprint, kind = kind, capacity = capacity, a = a, b = b }
end

state.ls_s, state.ls_u = es, eu
state.v1, state.v2 = result.view.v1, result.view.v2
state.wid, state.ws_s, state.ws_u = result.view.wid, result.view.ws_s, result.view.ws_u
rt_rate.save(state)
rt_rate.index(xkey, state)
rt_state.save_counters(meta_key, meta)
registry.commit_watermark(meta_key, meta, es, eu)

if result.accepted then
    return { '1', exact_int.format_int(result.available) }
end
return {
    '2',
    exact_int.format_int(result.available),
    exact_int.format_int(result.retry),
    result.capped and '1' or '0',
}
