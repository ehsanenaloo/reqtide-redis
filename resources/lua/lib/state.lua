-- Shared helpers of the state scripts.
--
-- Text placed after exact_int.lua, time.lua, text.lua and registry.lua. It defines the local table `rt_state` only.
--
--   * a time pair stored inside a hash value or a queue entry is the text "<seconds> <microseconds>", built only from
--     exact_int.format_int and rt_text.join and read back with a strict pattern and exact_int.parse_int;
--   * the registry counters (`partitions`, `records`) are held in the table returned by registry.load while a script
--     runs and are written back once, by rt_state.save_counters, before the script returns or raises a registered error;
--   * a script validates every argument and every stored value it needs before its first write.

local rt_state = {}

do
    local RETENTION_MICROS = 120000000
    rt_state.RETENTION_MICROS = RETENTION_MICROS
    rt_state.RETENTION_SECONDS = 120


    -- A non-negative exact integer argument (decimal text) not below `minimum`.
    function rt_state.int_arg(text, minimum)
        local ok, value = pcall(exact_int.parse_int, text)
        if not ok or value < minimum then
            error(redis.error_reply('RT_CONFIG a script argument is outside its domain'), 0)
        end
        return value
    end

    -- A 128-bit identifier: exactly 32 lowercase hexadecimal characters.
    function rt_state.hex_arg(text)
        if type(text) ~= 'string' or string.len(text) ~= 32 or not string.find(text, '^[0-9a-f]+$') then
            error(redis.error_reply('RT_CONFIG a script identifier is not 32 lowercase hexadecimal characters'), 0)
        end
        return text
    end

    -- A time pair as stored text.
    function rt_state.pair_text(seconds, microseconds)
        return rt_text.join(' ', exact_int.format_int(seconds), exact_int.format_int(microseconds))
    end

    -- Reads a stored pair; `text` may be false (absent field) and then yields nil.
    function rt_state.pair_read(text)
        if text == false or text == nil then
            return nil
        end
        local s, u = string.match(text, '^(%d+) (%d+)$')
        if s == nil then
            error(redis.error_reply('RT_CORRUPT a stored time pair is malformed'), 0)
        end
        local ok, seconds, microseconds = pcall(function()
            return exact_int.parse_int(s), exact_int.parse_int(u)
        end)
        if not ok then
            error(redis.error_reply('RT_CORRUPT a stored time pair is malformed'), 0)
        end
        rt_time.pair(seconds, microseconds)
        return seconds, microseconds
    end

    -- A stored integer field (HMGET result): required, exact.
    function rt_state.field_int(value)
        if value == false or value == nil then
            error(redis.error_reply('RT_CORRUPT a state field is missing'), 0)
        end
        local ok, number = pcall(exact_int.parse_int, value)
        if not ok then
            error(redis.error_reply('RT_CORRUPT a state field is not a valid integer'), 0)
        end
        return number
    end

    function rt_state.save_counters(meta_key, meta)
        if meta.partitions < 0 or meta.records < 0 or meta.partitions > meta.max_partitions or meta.records > meta.max_records then
            error(redis.error_reply('RT_CORRUPT the registry counters would leave their bounds'), 0)
        end
        redis.call('HSET', meta_key,
            'partitions', exact_int.format_int(meta.partitions),
            'records', exact_int.format_int(meta.records))
    end

    -- Whether a deadline pair has been reached: expired at equality (a lease and a tombstone are valid on [start, end)).
    function rt_state.reached(deadline_s, deadline_u, now_s, now_u)
        return rt_time.compare(deadline_s, deadline_u, now_s, now_u) <= 0
    end
end
