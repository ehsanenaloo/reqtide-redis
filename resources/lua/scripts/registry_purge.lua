-- Removes keys a retired registry left behind, only while the registry does not exist.
-- KEYS: 1 registry metadata hash, 2 and the following: the keys to remove (one SCAN page, chosen by the caller).
-- ARGV: none. Reply: the number of keys removed.
--
-- The metadata check and the removal run in one script, so a registry that is initialized again between the scan and the
-- removal is never emptied: the script then fails with RT_EXISTS and removes nothing. The keys are taken from KEYS and no
-- other key is read or written, but they are keys of different names, so the script needs a standalone server (it is not
-- usable on Redis Cluster, where the keys of one call may live on different nodes).
-- Registered errors: RT_EXISTS (the registry exists), RT_ACL (the user may not run EXISTS or UNLINK on a key), RT_CONFIG.

if #KEYS < 1 or #KEYS > 1001 or #ARGV ~= 0 then
    error(redis.error_reply('RT_CONFIG the purge arguments are outside their domain'), 0)
end
for _, key in ipairs(KEYS) do
    if not redis.acl_check_cmd(key == KEYS[1] and 'EXISTS' or 'UNLINK', key) then
        error(redis.error_reply('RT_ACL the Redis user may not run a command these scripts need'), 0)
    end
end
if redis.call('EXISTS', KEYS[1]) ~= 0 then
    error(redis.error_reply('RT_EXISTS the registry exists'), 0)
end
if #KEYS == 1 then
    return 0
end
return redis.call('UNLINK', unpack(KEYS, 2))
