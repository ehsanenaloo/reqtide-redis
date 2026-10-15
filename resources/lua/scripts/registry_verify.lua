-- Read-only check of a registry: present, expected identity and schema, valid watermark and counters.
-- KEYS[1] metadata hash; ARGV: identity, schema version. Writes nothing. Reply: registry.status.

local ok, schema = pcall(exact_int.parse_int, ARGV[2])
if not ok or schema < 1 then
    error(redis.error_reply('RT_CONFIG registry arguments are outside their domain'), 0)
end
registry.check_identity(ARGV[1])
return registry.status(registry.load(KEYS[1], ARGV[1], schema), false)
