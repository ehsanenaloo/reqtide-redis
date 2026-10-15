-- Backend-authoritative time for Reqtide Redis scripts.
--
-- Text that the script loader places after exact_int.lua. It defines the local table `rt_time` only.
--
-- A time is always a pair (seconds, microseconds), never one combined number: epoch microseconds exceed LUA_EXACT_MAX
-- soon after the year 2255 and, more importantly, a combined float would hide a rounding error. Every component is a
-- non-negative exact integer, microseconds < 1,000,000. Pairs are compared component-wise; durations are added with a
-- checked carry using exact_int.

local rt_time = {}

do
    local MICROS = 1000000
    local EXACT_MAX = exact_int.EXACT_MAX
    -- Largest whole-second difference whose microsecond total can still be exact.
    local MAX_DIFF_SECONDS = 9007199254

    local function fail_corrupt(message)
        error(redis.error_reply(message), 0)
    end

    -- Validates a pair and returns it.
    function rt_time.pair(seconds, microseconds)
        if not (exact_int.is_exact_integer(seconds) and exact_int.is_exact_integer(microseconds) and microseconds < MICROS) then
            fail_corrupt('RT_CORRUPT time pair is not normalized')
        end
        return seconds, microseconds
    end

    -- Redis TIME as a validated pair. The reply is two decimal strings.
    function rt_time.read()
        local reply = redis.call('TIME')
        local ok, seconds, microseconds = pcall(function()
            return exact_int.parse_int(reply[1]), exact_int.parse_int(reply[2])
        end)
        if not ok then
            fail_corrupt('RT_CORRUPT server time is not a valid pair')
        end
        return rt_time.pair(seconds, microseconds)
    end

    function rt_time.compare(s1, u1, s2, u2)
        if s1 ~= s2 then
            return s1 < s2 and -1 or 1
        end
        if u1 ~= u2 then
            return u1 < u2 and -1 or 1
        end
        return 0
    end

    function rt_time.max(s1, u1, s2, u2)
        if rt_time.compare(s1, u1, s2, u2) >= 0 then
            return s1, u1
        end
        return s2, u2
    end

    -- pair + a duration in microseconds (an exact integer), with a checked carry.
    function rt_time.add_micros(seconds, microseconds, duration)
        local carry, rest = exact_int.mul_add_div_floor_capped(duration, 1, 0, MICROS, EXACT_MAX)
        local new_micros = microseconds + rest
        if new_micros >= MICROS then
            new_micros = new_micros - MICROS
            if carry >= EXACT_MAX then
                error(redis.error_reply('RT_CONFIG time addition leaves the exact range'), 0)
            end
            carry = carry + 1
        end
        if carry > EXACT_MAX - seconds then
            error(redis.error_reply('RT_CONFIG time addition leaves the exact range'), 0)
        end
        return seconds + carry, new_micros
    end

    -- later - earlier in microseconds; the pair must not be earlier, and the result must be exact.
    function rt_time.diff_micros(s_late, u_late, s_early, u_early)
        if rt_time.compare(s_late, u_late, s_early, u_early) < 0 then
            error(redis.error_reply('RT_CORRUPT negative time difference'), 0)
        end
        local seconds = s_late - s_early
        if seconds > MAX_DIFF_SECONDS then
            error(redis.error_reply('RT_CONFIG time difference leaves the exact range'), 0)
        end
        local total = seconds * MICROS + (u_late - u_early)
        if total > EXACT_MAX then
            error(redis.error_reply('RT_CONFIG time difference leaves the exact range'), 0)
        end
        return total
    end

    -- effective = max(watermark, stored state time, observed), compared as pairs. Also reports whether the observed
    -- time lies before the retained maximum (a regression): the caller then uses the retained maximum and may return the
    -- fact to the bridge. Nothing here writes.
    function rt_time.effective(wm_s, wm_u, st_s, st_u, ob_s, ob_u)
        rt_time.pair(wm_s, wm_u)
        rt_time.pair(st_s, st_u)
        rt_time.pair(ob_s, ob_u)
        local keep_s, keep_u = rt_time.max(wm_s, wm_u, st_s, st_u)
        local es, eu = rt_time.max(keep_s, keep_u, ob_s, ob_u)
        local regressed = rt_time.compare(ob_s, ob_u, keep_s, keep_u) < 0
        return es, eu, regressed
    end
end
