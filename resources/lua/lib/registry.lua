-- Registry metadata and the authority watermark.
--
-- Text placed after exact_int.lua and time.lua. It defines the local table `registry` only.
--
-- The registry is one hash at KEYS[1]. It is created only by explicit host-authorized initialization; no execution or
-- inspection script ever creates it. Fields: schema, identity, wm_s and wm_u (the authority watermark pair),
-- partitions and records (live counters), max_partitions and max_records (ceilings). Errors are registered codes:
--   RT_NO_REGISTRY  the registry hash is absent (never initialized, retired, or lost);
--   RT_IDENTITY     the stored identity is not the expected one;
--   RT_CORRUPT      a field is missing or malformed, or the schema differs;
--   RT_CONFIG       an input outside its domain.

local registry = {}

do
    local FIELDS = { 'schema', 'identity', 'wm_s', 'wm_u', 'partitions', 'records', 'max_partitions', 'max_records' }
    local NUMERIC = { 'schema', 'wm_s', 'wm_u', 'partitions', 'records', 'max_partitions', 'max_records' }

    -- Validates an identity token: 1 to 64 of letters, digits, dot, underscore, hyphen.
    function registry.check_identity(identity)
        if type(identity) ~= 'string' or string.len(identity) < 1 or string.len(identity) > 64
            or not string.find(identity, '^[A-Za-z0-9_.%-]+$') then
            error(redis.error_reply('RT_CONFIG registry identity is not a valid token'), 0)
        end
    end

    local FULL_ACCESS = {
        { 'HSET', 'f', 'v' }, { 'HDEL', 'f' }, { 'DEL' }, { 'ZADD', '0', 'm' }, { 'ZREM', 'm' }, { 'RPUSH', 'v' }, { 'LPOP' },
        { 'HGET', 'f' }, { 'HMGET', 'f' }, { 'HGETALL' }, { 'HEXISTS', 'f' }, { 'HLEN' }, { 'EXISTS' }, { 'LINDEX', '0' },
        { 'ZRANGE', '0', '-1' }, { 'ZRANGEBYSCORE', '-inf', '+inf' },
    }
    local ADMIN_ACCESS = { { 'HSET', 'f', 'v' }, { 'DEL' }, { 'EXISTS' }, { 'HGETALL' } }

    -- Before the first write of a mutating script: every command the script may use must be allowed for the calling user
    -- on every key the script was given (ACL), so that a denial can never come after an earlier write.
    -- `admin` limits the check to the registry administration commands. Raises RT_ACL.
    function registry.require_access(admin)
        local list = admin and ADMIN_ACCESS or FULL_ACCESS
        for _, key in ipairs(KEYS) do
            for _, entry in ipairs(list) do
                local ok = redis.acl_check_cmd(entry[1], key, unpack(entry, 2))
                if not ok then
                    error(redis.error_reply('RT_ACL the Redis user may not run a command these scripts need'), 0)
                end
            end
        end
        if not redis.acl_check_cmd('TIME') then
            error(redis.error_reply('RT_ACL the Redis user may not run a command these scripts need'), 0)
        end
    end

    -- Reads and validates the registry; raises a registered error otherwise. Returns a table of validated fields.
    function registry.load(meta_key, expected_identity, expected_schema)
        if redis.call('EXISTS', meta_key) == 0 then
            error(redis.error_reply('RT_NO_REGISTRY registry is not initialized'), 0)
        end
        local flat = redis.call('HGETALL', meta_key)
        local raw = {}
        for i = 1, #flat, 2 do
            raw[flat[i]] = flat[i + 1]
        end
        for _, name in ipairs(FIELDS) do
            if raw[name] == nil then
                error(redis.error_reply('RT_CORRUPT registry field is missing'), 0)
            end
        end
        local meta = { identity = raw.identity }
        for _, name in ipairs(NUMERIC) do
            local ok, value = pcall(exact_int.parse_int, raw[name])
            if not ok then
                error(redis.error_reply('RT_CORRUPT registry field is not a valid integer'), 0)
            end
            meta[name] = value
        end
        if meta.schema ~= expected_schema then
            error(redis.error_reply('RT_CORRUPT registry schema version differs'), 0)
        end
        if meta.identity ~= expected_identity then
            error(redis.error_reply('RT_IDENTITY registry identity differs from the expected one'), 0)
        end
        if meta.wm_u >= 1000000 or meta.max_partitions < 1 or meta.max_records < 1
            or meta.partitions > meta.max_partitions or meta.records > meta.max_records then
            error(redis.error_reply('RT_CORRUPT registry fields are inconsistent'), 0)
        end
        return meta
    end

    -- Start of a mutating or inspecting command: validates the registry, reads the server time and returns the meta, the
    -- effective time pair and the regression fact. The stored state time defaults to zero (no state yet). Writes nothing.
    function registry.begin(meta_key, expected_identity, expected_schema, state_seconds, state_micros)
        local meta = registry.load(meta_key, expected_identity, expected_schema)
        local ob_s, ob_u = rt_time.read()
        local es, eu, regressed = rt_time.effective(meta.wm_s, meta.wm_u, state_seconds or 0, state_micros or 0, ob_s, ob_u)
        return meta, es, eu, regressed
    end

    -- Advances the watermark to the effective time of a successful mutating command (never backwards). Call only after
    -- every check of the command has passed, in the same script as the state write.
    function registry.commit_watermark(meta_key, meta, es, eu)
        if rt_time.compare(es, eu, meta.wm_s, meta.wm_u) > 0 then
            redis.call('HSET', meta_key, 'wm_s', exact_int.format_int(es), 'wm_u', exact_int.format_int(eu))
        end
    end

    -- The status reply shared by the admin scripts: created flag, watermark pair, counters and ceilings, as strings.
    function registry.status(meta, created)
        return {
            created and '1' or '0',
            exact_int.format_int(meta.wm_s),
            exact_int.format_int(meta.wm_u),
            exact_int.format_int(meta.partitions),
            exact_int.format_int(meta.records),
            exact_int.format_int(meta.max_partitions),
            exact_int.format_int(meta.max_records),
        }
    end
end

-- Every script of the package needs redis.acl_check_cmd, which exists from Redis 7.0. This runs before the first command of
-- any script, so an older server fails the call with nothing written.
if redis.acl_check_cmd == nil then
    error(redis.error_reply('RT_VERSION these scripts need Redis 7.0 or newer'), 0)
end
