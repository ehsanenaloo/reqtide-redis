-- Audited string join for Reqtide Redis scripts.
--
-- Text that the script loader places after exact_int.lua. It defines the local table `rt_text` only.
--
-- Field names and pair values are built from identifiers that are already strings (hex owner ids, key names) and from
-- integers that exact_int.format_int has already turned into text. This is the only place a script may join strings. It
-- accepts values of type string and nothing else, so a Lua number can never be turned into text here (Redis Lua prints a
-- number with 14 digits and would corrupt a value near the exact range silently). The static scan allows the join only
-- between the region markers below.

local rt_text = {}

do
    -- BEGIN join_text (the only string join; strings only)
    function rt_text.join(separator, ...)
        local parts = { ... }
        for i = 1, select('#', ...) do
            if type(parts[i]) ~= 'string' then
                error(redis.error_reply('RT_CORRUPT a text join received a value that is not text'), 0)
            end
        end
        return table.concat(parts, separator)
    end
    -- END join_text
end
