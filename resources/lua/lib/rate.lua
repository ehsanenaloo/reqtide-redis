-- Rate-limit partitions: token bucket, fixed window (aligned and first-hit) and approximate sliding counter
--.
--
-- Text placed after rt_state, rt_cc, rt_rb and rt_cb. It defines the local table `rt_rate` only.
--
-- Every duration is a whole number of microseconds (the decision resolution of the shared backends); the PHP side rounds a
-- configured duration up, so a refill is never faster and a window never shorter than configured. Token amounts are
-- scaled by 1,000,000. All products, quotients and comparisons that can leave the exact range go through exact_int.
--
-- One hash per partition (key pkey):
--   fp             the settings fingerprint (bound to the policy identity; a different one is RT_CONFIG)
--   kind, cap, pa, pb   the settings, for maintenance: kind tb (cap tokens, pa refill tokens, pb interval),
--                  fw (aligned fixed window), fh (first-hit fixed window), sc (sliding counter): pa is the window
--   ls_s, ls_u     last effective time (the origin of the next elapsed-time calculation)
--   v1, v2         tb: scaled balance and refill remainder; fw, fh: count; sc: current and previous count
--   wid            fw, sc: id of the stored window, floor(epoch microseconds / window)
--   ws_s, ws_u     fh: start of the stored window
-- The registry-wide sorted set xkey holds every rate partition (member pkey) with the score "earliest second at which
-- maintenance may examine it". A partition is removed only when it is equivalent to fresh state and 120 seconds have
-- passed since its last write; a partition that is not stays protected and is examined again later.

local rt_rate = {}

do
    local SCALE = 1000000
    local MICROS = 1000000
    local MAX_TOKENS = 9007199254
    local RETENTION_SECONDS = 120
    local RECLAIM_BATCH = 128
    rt_rate.RECLAIM_BATCH = RECLAIM_BATCH
    local EXACT_MAX = exact_int.EXACT_MAX

    -- The settings of a policy from script arguments (or from a stored partition); raises RT_CONFIG outside the domain.
    function rt_rate.params(kind, capacity, a, b)
        if kind ~= 'tb' and kind ~= 'fw' and kind ~= 'fh' and kind ~= 'sc' then
            error(redis.error_reply('RT_CONFIG unknown rate algorithm'), 0)
        end
        if capacity < 1 or a < 1 then
            error(redis.error_reply('RT_CONFIG a rate setting is outside its domain'), 0)
        end
        if kind == 'tb' and (capacity > MAX_TOKENS or a > MAX_TOKENS or b < 1) then
            error(redis.error_reply('RT_CONFIG a rate setting is outside its domain'), 0)
        end
        if kind ~= 'tb' and capacity > MAX_TOKENS then
            error(redis.error_reply('RT_CONFIG a rate setting is outside its domain'), 0)
        end
        return { kind = kind, capacity = capacity, a = a, b = b }
    end

    function rt_rate.load(pkey)
        if redis.call('EXISTS', pkey) == 0 then
            return nil
        end
        local raw = redis.call('HMGET', pkey, 'fp', 'kind', 'cap', 'pa', 'pb', 'ls_s', 'ls_u', 'v1', 'v2', 'wid', 'ws_s', 'ws_u')
        if raw[1] == false or raw[2] == false then
            error(redis.error_reply('RT_CORRUPT a rate partition field is missing'), 0)
        end
        local state = {
            key = pkey,
            fp = raw[1],
            kind = raw[2],
            capacity = rt_state.field_int(raw[3]),
            a = rt_state.field_int(raw[4]),
            b = rt_state.field_int(raw[5]),
            ls_s = rt_state.field_int(raw[6]),
            ls_u = rt_state.field_int(raw[7]),
            v1 = rt_state.field_int(raw[8]),
            v2 = rt_state.field_int(raw[9]),
            wid = rt_state.field_int(raw[10]),
            ws_s = rt_state.field_int(raw[11]),
            ws_u = rt_state.field_int(raw[12]),
        }
        rt_time.pair(state.ls_s, state.ls_u)
        rt_time.pair(state.ws_s, state.ws_u)
        local ok = pcall(rt_rate.params, state.kind, state.capacity, state.a, state.b)
        if not ok then
            error(redis.error_reply('RT_CORRUPT a rate partition setting is invalid'), 0)
        end
        return state
    end

    function rt_rate.save(state)
        redis.call('HSET', state.key,
            'fp', state.fp,
            'kind', state.kind,
            'cap', exact_int.format_int(state.capacity),
            'pa', exact_int.format_int(state.a),
            'pb', exact_int.format_int(state.b),
            'ls_s', exact_int.format_int(state.ls_s),
            'ls_u', exact_int.format_int(state.ls_u),
            'v1', exact_int.format_int(state.v1),
            'v2', exact_int.format_int(state.v2),
            'wid', exact_int.format_int(state.wid),
            'ws_s', exact_int.format_int(state.ws_s),
            'ws_u', exact_int.format_int(state.ws_u))
    end

    -- floor(n / d) without a native division near the exact range.
    local function floor_div(n, d)
        local quotient = exact_int.mul_add_div_floor_capped(n, 1, 0, d, EXACT_MAX)
        return quotient
    end

    -- Window id and position of the effective time on the epoch axis: floor and remainder of epoch microseconds by the
    -- window, computed on limbs (epoch microseconds are never one native number).
    local function window_position(es, eu, window)
        local epoch = exact_int.add(exact_int.mul(exact_int.from_num(es), exact_int.from_num(MICROS)), exact_int.from_num(eu))
        local quotient, remainder = exact_int.divmod(epoch, exact_int.from_num(window))
        local id = exact_int.to_num(quotient)
        local position = exact_int.to_num(remainder)
        if id == nil or position == nil then
            error(redis.error_reply('RT_CONFIG the window id leaves the exact range'), 0)
        end
        return id, position
    end

    -- ---- sliding counter -------------------------------------------------------------------------------------------

    local function sliding_fits(p, current, cost, previous, x)
        return exact_int.compare_weighted_sum(current + cost, p.a, previous, p.a - x, p.capacity, p.a) <= 0
    end

    local function sliding_available(p, view)
        local weighted = exact_int.mul_sub_ceil_div_capped(view.v2, p.a - view.pos, 0, p.a, EXACT_MAX)
        return math.max(0, p.capacity - view.v1 - weighted)
    end

    -- Smallest wait (microseconds) after which the same cost fits, over at most the two-window horizon; the second
    -- value is true when the wait does not fit the exact range (never rounded down).
    local function sliding_wait(p, current, cost, previous, x)
        local window = p.a
        local last = window - 1
        if x < last and sliding_fits(p, current, cost, previous, last) then
            local lo, hi = x + 1, last
            while lo < hi do
                local mid = lo + math.floor((hi - lo) / 2)
                if sliding_fits(p, current, cost, previous, mid) then
                    hi = mid
                else
                    lo = mid + 1
                end
            end
            return lo - x, false
        end
        local until_next = window - x
        if sliding_fits(p, 0, cost, current, last) then
            local lo, hi = 0, last
            while lo < hi do
                local mid = lo + math.floor((hi - lo) / 2)
                if sliding_fits(p, 0, cost, current, mid) then
                    hi = mid
                else
                    lo = mid + 1
                end
            end
            if lo > EXACT_MAX - until_next then
                return EXACT_MAX, true
            end
            return until_next + lo, false
        end
        if window > EXACT_MAX - until_next then
            return EXACT_MAX, true
        end
        return until_next + window, false
    end

    -- ---- projection to the effective time ----------------------------------------------------------------------------

    -- The state at (es, eu) without changing anything: a view table with v1, v2, wid, ws_s, ws_u and, for the window
    -- algorithms, pos (position in the window, microseconds) or elapsed (first-hit).
    function rt_rate.project(p, state, es, eu)
        if p.kind == 'tb' then
            local cap_s = p.capacity * SCALE
            if state == nil then
                return { v1 = cap_s, v2 = 0, wid = 0, ws_s = 0, ws_u = 0 }
            end
            if state.v1 > cap_s or state.v2 >= p.b then
                error(redis.error_reply('RT_CORRUPT a token bucket state is out of bounds'), 0)
            end
            local deficit = cap_s - state.v1
            if deficit <= 0 then
                return { v1 = cap_s, v2 = 0, wid = 0, ws_s = 0, ws_u = 0 }
            end
            local elapsed = rt_time.diff_micros(es, eu, state.ls_s, state.ls_u)
            if elapsed == 0 then
                return { v1 = state.v1, v2 = state.v2, wid = 0, ws_s = 0, ws_u = 0 }
            end
            local added, remainder, capped = exact_int.mul_add_div_floor_capped(elapsed, p.a * SCALE, state.v2, p.b, deficit)
            if capped then
                return { v1 = cap_s, v2 = 0, wid = 0, ws_s = 0, ws_u = 0 }
            end
            return { v1 = state.v1 + added, v2 = remainder, wid = 0, ws_s = 0, ws_u = 0 }
        end

        if p.kind == 'fw' then
            local id, position = window_position(es, eu, p.a)
            if state ~= nil and state.v1 > p.capacity then
                error(redis.error_reply('RT_CORRUPT a window count is out of bounds'), 0)
            end
            local count = 0
            if state ~= nil and state.wid == id then
                count = state.v1
            end
            return { v1 = count, v2 = 0, wid = id, pos = position, ws_s = 0, ws_u = 0 }
        end

        if p.kind == 'fh' then
            if state == nil then
                return { v1 = 0, v2 = 0, wid = 0, ws_s = es, ws_u = eu, elapsed = 0 }
            end
            if state.v1 > p.capacity then
                error(redis.error_reply('RT_CORRUPT a window count is out of bounds'), 0)
            end
            local elapsed = rt_time.diff_micros(es, eu, state.ws_s, state.ws_u)
            if elapsed < p.a then
                return { v1 = state.v1, v2 = 0, wid = 0, ws_s = state.ws_s, ws_u = state.ws_u, elapsed = elapsed }
            end
            return { v1 = 0, v2 = 0, wid = 0, ws_s = es, ws_u = eu, elapsed = 0 }
        end

        -- sc
        local id, position = window_position(es, eu, p.a)
        if state == nil then
            return { v1 = 0, v2 = 0, wid = id, pos = position, ws_s = 0, ws_u = 0 }
        end
        if state.v1 > p.capacity or state.v2 > p.capacity then
            error(redis.error_reply('RT_CORRUPT a sliding counter state is out of bounds'), 0)
        end
        local windows = id - state.wid
        if windows <= 0 then
            return { v1 = state.v1, v2 = state.v2, wid = id, pos = position, ws_s = 0, ws_u = 0 }
        elseif windows == 1 then
            return { v1 = 0, v2 = state.v1, wid = id, pos = position, ws_s = 0, ws_u = 0 }
        end
        return { v1 = 0, v2 = 0, wid = id, pos = position, ws_s = 0, ws_u = 0 }
    end

    -- Whole tokens a single consumption could take right now.
    function rt_rate.available(p, view)
        if p.kind == 'tb' then
            return floor_div(view.v1, SCALE)
        elseif p.kind == 'fw' or p.kind == 'fh' then
            return p.capacity - view.v1
        end
        return sliding_available(p, view)
    end

    -- Evaluates `cost` (whole tokens, not above the capacity) against a projected view. Returns a table: accepted, view
    -- (the state after the debit, or the unchanged projection), available (before the cost), retry (microseconds, when
    -- rejected) and capped (the wait does not fit the exact range).
    function rt_rate.evaluate(p, view, cost)
        local available = rt_rate.available(p, view)

        if p.kind == 'tb' then
            local cost_s = cost * SCALE
            if view.v1 >= cost_s then
                return { accepted = true, view = { v1 = view.v1 - cost_s, v2 = view.v2, wid = 0, ws_s = 0, ws_u = 0 }, available = available }
            end
            -- Smallest tick count d with floor((d * R + remainder) / W) >= need, i.e. d = ceil((need * W - remainder) / R).
            local wait, capped = exact_int.mul_sub_ceil_div_capped(cost_s - view.v1, p.b, view.v2, p.a * SCALE, EXACT_MAX)
            return { accepted = false, view = view, available = available, retry = wait, capped = capped }
        end

        if p.kind == 'fw' or p.kind == 'fh' then
            if cost <= available then
                local after = { v1 = view.v1 + cost, v2 = 0, wid = view.wid, ws_s = view.ws_s, ws_u = view.ws_u }
                return { accepted = true, view = after, available = available }
            end
            -- The window is [start, start + W); exactly at its end a new window starts.
            local elapsed = view.elapsed
            if p.kind == 'fw' then
                elapsed = view.pos
            end
            return { accepted = false, view = view, available = available, retry = p.a - elapsed, capped = false }
        end

        if sliding_fits(p, view.v1, cost, view.v2, view.pos) then
            local after = { v1 = view.v1 + cost, v2 = view.v2, wid = view.wid, ws_s = 0, ws_u = 0 }
            return { accepted = true, view = after, available = available }
        end
        local wait, capped = sliding_wait(p, view.v1, cost, view.v2, view.pos)
        return { accepted = false, view = view, available = available, retry = wait, capped = capped }
    end

    -- Whether removing the partition cannot change any later result: the projected state equals fresh state.
    local function fresh_equivalent(p, view)
        if p.kind == 'tb' then
            return view.v1 == p.capacity * SCALE
        elseif p.kind == 'fw' or p.kind == 'fh' then
            return view.v1 == 0
        end
        return view.v1 == 0 and view.v2 == 0
    end

    -- Bounded cleanup at a partition ceiling (one batch of at most 128 candidates): removes the partitions that are
    -- fresh-equivalent and idle for 120 seconds; the others get a later examination time. Returns the number removed.
    local function reclaim_rate_partition(meta, xkey, member, es, eu)
        local state = rt_rate.load(member)
        if state == nil then
            redis.call('ZREM', xkey, member)
            return 0
        end
        local idle_s, idle_u = rt_time.add_micros(state.ls_s, state.ls_u, RETENTION_SECONDS * MICROS)
        local p = rt_rate.params(state.kind, state.capacity, state.a, state.b)
        -- Not idle for the retention yet: the examination second (whole seconds) has come but the last fraction of
        -- the retention has not. The score stays, so it is a candidate again within a second and a partition at the
        -- ceiling is reclaimed at its exact instant.
        if not rt_state.reached(idle_s, idle_u, es, eu) then
            return 0
        end
        if fresh_equivalent(p, rt_rate.project(p, state, es, eu)) then
            redis.call('DEL', member)
            redis.call('ZREM', xkey, member)
            meta.partitions = meta.partitions - 1
            return 1
        end
        -- Still protective: behind the other candidates, so a head of drained partitions cannot starve them.
        redis.call('ZADD', xkey, exact_int.format_int(es + RETENTION_SECONDS), member)
        return 0
    end

    local function reclaim_budget_partition(meta, xkey, qkey, member, es, eu)
        local budget = rt_rb.load(member)
        local outcome = rt_rb.reclaim_one(meta, budget, rt_text.join('', member, ':a'), qkey, es, eu)
        if outcome == 'removed' then
            redis.call('ZREM', xkey, member)
            return 1
        end
        if outcome == 'later' then
            redis.call('ZADD', xkey, exact_int.format_int(es + RETENTION_SECONDS), member)
        else
            rt_rb.index(xkey, budget)
        end
        return 0
    end

    local function reclaim_circuit_partition(meta, xkey, qkey, member, es, eu)
        local circuit = rt_cb.load(member)
        local outcome = rt_cb.reclaim_one(meta, circuit, rt_text.join('', member, ':a'), rt_text.join('', member, ':w'), es, eu)
        if outcome == 'removed' then
            redis.call('ZREM', xkey, member)
            return 1
        end
        if outcome == 'later' then
            redis.call('ZADD', xkey, exact_int.format_int(es + RETENTION_SECONDS), member)
        else
            rt_cb.index(xkey, circuit)
        end
        return 0
    end

    -- Expired leases of a concurrency partition, then the partition itself when it holds nothing. A partition with live
    -- leases is examined again at the second its first lease ends.
    local function reclaim_concurrency_partition(meta, xkey, member, es, eu)
        local state = rt_cc.load(member)
        if state == nil then
            redis.call('ZREM', xkey, member)
            return 0
        end
        local akey = rt_text.join('', member, ':a')
        rt_cc.expire_actives(meta, state, akey, es, eu)
        if rt_cc.delete_if_empty(meta, state, akey) then
            redis.call('ZREM', xkey, member)
            return 1
        end
        rt_cc.save(state)
        rt_cc.index(xkey, state, akey)
        return 0
    end

    -- A cooldown whose not-before instant and cleanup grace have passed behaves exactly like an absent one.
    local function reclaim_cooldown_partition(meta, xkey, member, es, eu)
        local state = rt_cd.load(member)
        if state == nil then
            redis.call('ZREM', xkey, member)
            return 0
        end
        local rs, ru = rt_cd.removal(state)
        if rt_state.reached(rs, ru, es, eu) then
            redis.call('DEL', member)
            redis.call('ZREM', xkey, member)
            meta.partitions = meta.partitions - 1
            meta.records = meta.records - 1
            return 1
        end
        rt_cd.index(xkey, state)
        return 0
    end

    -- One candidate. Every reclaim_* function reads and validates its partition before its first write, so an error raised
    -- here has changed neither the partition nor the counters held in `meta`.
    local function reclaim_member(meta, xkey, qkey, member, es, eu)
        local kind = redis.call('HGET', member, 'kind')
        if kind == 'cc' then
            return reclaim_concurrency_partition(meta, xkey, member, es, eu)
        elseif kind == 'cd' then
            return reclaim_cooldown_partition(meta, xkey, member, es, eu)
        elseif kind == 'rb' then
            return reclaim_budget_partition(meta, xkey, qkey, member, es, eu)
        elseif kind == 'cb' then
            return reclaim_circuit_partition(meta, xkey, qkey, member, es, eu)
        end
        return reclaim_rate_partition(meta, xkey, member, es, eu)
    end

    -- Returns the number of partitions removed, the number of candidates examined (a full batch means more may wait) and
    -- the number of candidates that could not be read (damaged; deferred one retention, never deleted or repaired).
    -- A candidate that cannot be read (a damaged partition or a member that is not a partition) is not an error of the
    -- caller: it is moved behind the other candidates, so it cannot hold up the batch or stop the call, and the counters
    -- stay in step with what was deleted.
    function rt_rate.reclaim(meta, xkey, qkey, es, eu)
        local candidates = redis.call('ZRANGEBYSCORE', xkey, '-inf', exact_int.format_int(es), 'LIMIT', 0, RECLAIM_BATCH)
        local removed = 0
        local damaged = 0
        for _, member in ipairs(candidates) do
            local ok, count = pcall(reclaim_member, meta, xkey, qkey, member, es, eu)
            if ok then
                removed = removed + count
            else
                damaged = damaged + 1
                redis.call('ZADD', xkey, exact_int.format_int(es + RETENTION_SECONDS), member)
            end
        end
        return removed, #candidates, damaged
    end

    -- Records that maintenance may look at the partition after its retention.
    function rt_rate.index(xkey, state)
        redis.call('ZADD', xkey, exact_int.format_int(state.ls_s + RETENTION_SECONDS), state.key)
    end
end
