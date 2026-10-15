-- Explicit, host-authorized creation of a registry. Idempotent for the same identity, schema and ceilings.
-- KEYS: 1 metadata hash, 2 registry index, 3 registry tombstone queue; ARGV: identity, schema version, max partitions, max
-- records. Validates everything before the first write. Reply: registry.status (created flag first).
--
-- A registry that does not exist is created only when the index and the queue do not exist either: they belong to a retired
-- registry that was not purged (RedisRegistryAdmin::purgeRetired), and adopting them would start the counters at zero over
-- records that are still there. Every partition is reachable from the index or the queue, so these two keys stand for all
-- of them. The error is RT_LEFTOVERS and nothing is written.

local meta_key = KEYS[1]
local identity = ARGV[1]
if #KEYS ~= 3 then
    error(redis.error_reply('RT_CONFIG registry arguments are outside their domain'), 0)
end
registry.require_access(true)
registry.check_identity(identity)
local ok, schema, max_partitions, max_records = pcall(function()
    return exact_int.parse_int(ARGV[2]), exact_int.parse_int(ARGV[3]), exact_int.parse_int(ARGV[4])
end)
if not ok or schema < 1 or max_partitions < 1 or max_records < 1 then
    error(redis.error_reply('RT_CONFIG registry arguments are outside their domain'), 0)
end

if redis.call('EXISTS', meta_key) == 1 then
    local meta = registry.load(meta_key, identity, schema)
    if meta.max_partitions ~= max_partitions or meta.max_records ~= max_records then
        error(redis.error_reply('RT_CONFIG registry exists with different ceilings'), 0)
    end
    return registry.status(meta, false)
end

if redis.call('EXISTS', KEYS[2], KEYS[3]) ~= 0 then
    error(redis.error_reply('RT_LEFTOVERS keys of a retired registry exist under this prefix'), 0)
end

local seconds, micros = rt_time.read()
redis.call('HSET', meta_key,
    'schema', exact_int.format_int(schema),
    'identity', identity,
    'wm_s', exact_int.format_int(seconds),
    'wm_u', exact_int.format_int(micros),
    'partitions', '0',
    'records', '0',
    'max_partitions', exact_int.format_int(max_partitions),
    'max_records', exact_int.format_int(max_records))
return registry.status(registry.load(meta_key, identity, schema), true)
