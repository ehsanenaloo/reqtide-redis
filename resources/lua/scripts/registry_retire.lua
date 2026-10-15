-- Explicit, host-authorized retirement of a registry. Deletes only the metadata hash, including the authority
-- watermark; the host is responsible for having invalidated old owners first. Partition and record
-- keys are not touched: their count is returned so that the loss is observable, and RedisRegistryAdmin::purgeRetired
-- removes them. KEYS[1] metadata hash; ARGV: identity, schema version. Reply: registry.status of what was retired.

registry.require_access(true)
local ok, schema = pcall(exact_int.parse_int, ARGV[2])
if not ok or schema < 1 then
    error(redis.error_reply('RT_CONFIG registry arguments are outside their domain'), 0)
end
registry.check_identity(ARGV[1])
local meta = registry.load(KEYS[1], ARGV[1], schema)
redis.call('DEL', KEYS[1])
return registry.status(meta, false)
