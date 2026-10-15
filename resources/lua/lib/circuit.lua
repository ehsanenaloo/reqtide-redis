-- Circuit breaker partitions.
--
-- Text placed after rt_cc. It defines the local table `rt_cb` only.
--
-- One hash per partition (key pkey). The fields of a concurrency partition (fp, inc, gen, ls_s, ls_u, na = active permits,
-- nt = terminal records, keep = 1) plus:
--   kind = cb; ph phase (0 Closed, 1 Open, 2 HalfOpen); ou_s and ou_u the open deadline; ps probe successes; ap probes
--   in flight; la_s and la_u last activity (the origin of the idle retention);
--   settings, bound to the identity: wk (c count window, t time window), wn (samples, or window microseconds), wg (bucket
--   granularity microseconds), mn minimum samples, rn and rd the failure ratio, od open duration, pc probe capacity,
--   pt probe success threshold, lz permit lease (all durations in microseconds);
--   the count window: rg the last outcomes as text (1 failure, 0 success, oldest overwritten first), rh the next index to
--   overwrite, rf the failures in rg.
-- "a:<owner>" holds an active permit "<expiry seconds> <expiry microseconds> <attempt> <c|p> <generation>" (c Closed
-- sample, p HalfOpen probe); the sorted set akey orders the permits by expiry second. "t:<owner>" holds a terminal record
-- "<deadline seconds> <deadline microseconds> <attempt> <generation>" and uses the registry tombstone queue of rt_cc. The
-- time window keeps one field per bucket in the hash wkey: bucket id to "<samples> <failures>".
--
-- Every transition increments the generation, drops every active permit (the permits of an earlier generation become
-- stale) and is reported as one fact per call (first source state, last target state; a net no-op is dropped).

local rt_cb = {}

do
    local MICROS = 1000000
    local EXACT_MAX = exact_int.EXACT_MAX
    local MAX_SAMPLES_PER_BUCKET = 8796093022207
    local MAX_COUNT_WINDOW = 10000
    local CLOSED, OPEN, HALF_OPEN = 0, 1, 2
    local NAMES = { [0] = 'c', [1] = 'o', [2] = 'h' }

    rt_cb.CLOSED, rt_cb.OPEN, rt_cb.HALF_OPEN = CLOSED, OPEN, HALF_OPEN

    local function field(prefix, owner)
        return rt_text.join(':', prefix, owner)
    end

    -- Settings from script arguments; raises RT_CONFIG outside the domain.
    function rt_cb.params(window_kind, window, granularity, minimum, ratio_n, ratio_d, open_us, probes, threshold, lease_us)
        if (window_kind ~= 'c' and window_kind ~= 't') or window < 1 or minimum < 1 or ratio_d < 1 or open_us < 1
            or probes < 1 or threshold < 1 or threshold > probes or lease_us < 1 then
            error(redis.error_reply('RT_CONFIG a circuit setting is outside its domain'), 0)
        end
        if window_kind == 'c' and window > MAX_COUNT_WINDOW then
            error(redis.error_reply('RT_CONFIG a circuit setting is outside its domain'), 0)
        end
        if window_kind == 't' and granularity < 1 then
            error(redis.error_reply('RT_CONFIG a circuit setting is outside its domain'), 0)
        end
        return {
            wk = window_kind, wn = window, wg = granularity, mn = minimum, rn = ratio_n, rd = ratio_d, od = open_us,
            pc = probes, pt = threshold, lz = lease_us,
        }
    end

    function rt_cb.load(pkey)
        local state = rt_cc.load(pkey)
        if state == nil then
            return nil
        end
        local raw = redis.call('HMGET', pkey,
            'kind', 'ph', 'ou_s', 'ou_u', 'ps', 'ap', 'la_s', 'la_u',
            'wk', 'wn', 'wg', 'mn', 'rn', 'rd', 'od', 'pc', 'pt', 'lz', 'rg', 'rh', 'rf')
        if raw[1] ~= 'cb' or not state.keep then
            error(redis.error_reply('RT_CORRUPT the partition is not a circuit breaker'), 0)
        end
        state.ph = rt_state.field_int(raw[2])
        state.ou_s = rt_state.field_int(raw[3])
        state.ou_u = rt_state.field_int(raw[4])
        state.ps = rt_state.field_int(raw[5])
        state.ap = rt_state.field_int(raw[6])
        state.la_s = rt_state.field_int(raw[7])
        state.la_u = rt_state.field_int(raw[8])
        state.wk = raw[9]
        state.wn = rt_state.field_int(raw[10])
        state.wg = rt_state.field_int(raw[11])
        state.mn = rt_state.field_int(raw[12])
        state.rn = rt_state.field_int(raw[13])
        state.rd = rt_state.field_int(raw[14])
        state.od = rt_state.field_int(raw[15])
        state.pc = rt_state.field_int(raw[16])
        state.pt = rt_state.field_int(raw[17])
        state.lz = rt_state.field_int(raw[18])
        state.rg = raw[19] or ''
        state.rh = rt_state.field_int(raw[20])
        state.rf = rt_state.field_int(raw[21])
        rt_time.pair(state.ou_s, state.ou_u)
        rt_time.pair(state.la_s, state.la_u)
        if state.gen >= EXACT_MAX then
            error(redis.error_reply('RT_CORRUPT the generation space is exhausted'), 0)
        end
        local ok = pcall(rt_cb.params, state.wk, state.wn, state.wg, state.mn, state.rn, state.rd, state.od, state.pc, state.pt, state.lz)
        if not ok or state.ph > HALF_OPEN or state.ap > state.na or string.len(state.rg) > state.wn
            or state.rf > string.len(state.rg) or (state.wk == 'c' and state.rh >= state.wn) then
            error(redis.error_reply('RT_CORRUPT a circuit state is out of bounds'), 0)
        end
        return state
    end

    function rt_cb.save(state)
        rt_cc.save(state)
        redis.call('HSET', state.key,
            'keep', '1',
            'kind', 'cb',
            'ph', exact_int.format_int(state.ph),
            'ou_s', exact_int.format_int(state.ou_s),
            'ou_u', exact_int.format_int(state.ou_u),
            'ps', exact_int.format_int(state.ps),
            'ap', exact_int.format_int(state.ap),
            'la_s', exact_int.format_int(state.la_s),
            'la_u', exact_int.format_int(state.la_u),
            'wk', state.wk,
            'wn', exact_int.format_int(state.wn),
            'wg', exact_int.format_int(state.wg),
            'mn', exact_int.format_int(state.mn),
            'rn', exact_int.format_int(state.rn),
            'rd', exact_int.format_int(state.rd),
            'od', exact_int.format_int(state.od),
            'pc', exact_int.format_int(state.pc),
            'pt', exact_int.format_int(state.pt),
            'lz', exact_int.format_int(state.lz),
            'rg', state.rg,
            'rh', exact_int.format_int(state.rh),
            'rf', exact_int.format_int(state.rf))
    end

    function rt_cb.create(pkey, fingerprint, incarnation, settings, es, eu)
        return {
            key = pkey, fp = fingerprint, inc = incarnation, gen = 1, ls_s = es, ls_u = eu, na = 0, nt = 0, keep = true,
            ph = CLOSED, ou_s = 0, ou_u = 0, ps = 0, ap = 0, la_s = es, la_u = eu,
            wk = settings.wk, wn = settings.wn, wg = settings.wg, mn = settings.mn, rn = settings.rn, rd = settings.rd,
            od = settings.od, pc = settings.pc, pt = settings.pt, lz = settings.lz, rg = '', rh = 0, rf = 0,
        }
    end

    -- ---- permits and terminal records ------------------------------------------------------------------------------

    -- The active permit of an owner: expiry pair, attempt, kind letter and generation, or nil.
    function rt_cb.permit(state, owner)
        local text = redis.call('HGET', state.key, field('a', owner))
        if text == false then
            return nil
        end
        local s, u, attempt, kind, generation = string.match(text, '^(%d+) (%d+) (%x+) ([cp]) (%d+)$')
        if s == nil then
            error(redis.error_reply('RT_CORRUPT an active permit is malformed'), 0)
        end
        local ok, xs, xu, gen = pcall(function()
            return exact_int.parse_int(s), exact_int.parse_int(u), exact_int.parse_int(generation)
        end)
        if not ok then
            error(redis.error_reply('RT_CORRUPT an active permit is malformed'), 0)
        end
        rt_time.pair(xs, xu)
        return xs, xu, attempt, kind, gen
    end

    -- The terminal record of an owner: deadline pair, attempt and generation, or nil.
    function rt_cb.terminal(state, owner)
        local text = redis.call('HGET', state.key, field('t', owner))
        if text == false then
            return nil
        end
        local s, u, attempt, generation = string.match(text, '^(%d+) (%d+) (%x+) (%d+)$')
        if s == nil then
            error(redis.error_reply('RT_CORRUPT a terminal record is malformed'), 0)
        end
        local ok, ds, du, gen = pcall(function()
            return exact_int.parse_int(s), exact_int.parse_int(u), exact_int.parse_int(generation)
        end)
        if not ok then
            error(redis.error_reply('RT_CORRUPT a terminal record is malformed'), 0)
        end
        rt_time.pair(ds, du)
        return ds, du, attempt, gen
    end

    function rt_cb.add_permit(meta, state, akey, owner, attempt, kind, xs, xu)
        redis.call('HSET', state.key, field('a', owner),
            rt_text.join(' ', rt_state.pair_text(xs, xu), attempt, kind, exact_int.format_int(state.gen)))
        redis.call('ZADD', akey, exact_int.format_int(xs), owner)
        state.na = state.na + 1
        meta.records = meta.records + 1
        if kind == 'p' then
            state.ap = state.ap + 1
        end
    end

    -- Moves an active permit into a terminal record; the slot it held is reused (no allocation).
    local function consume(meta, state, akey, qkey, owner, attempt, kind, generation, es, eu)
        redis.call('ZREM', akey, owner)
        redis.call('HDEL', state.key, field('a', owner))
        state.na = state.na - 1
        if kind == 'p' then
            state.ap = state.ap - 1
        end
        if redis.call('HEXISTS', state.key, field('t', owner)) == 1 then
            redis.call('HDEL', state.key, field('t', owner))
            state.nt = state.nt - 1
            meta.records = meta.records - 1
        end
        local ds, du = rt_time.add_micros(es, eu, rt_state.RETENTION_MICROS)
        redis.call('HSET', state.key, field('t', owner),
            rt_text.join(' ', rt_state.pair_text(ds, du), attempt, exact_int.format_int(generation)))
        redis.call('RPUSH', qkey, rt_text.join(' ', exact_int.format_int(ds), exact_int.format_int(du), state.key, owner))
        state.nt = state.nt + 1
    end
    rt_cb.consume = consume

    -- Every active permit issued under an earlier generation becomes stale at a transition.
    local function drop_permits(meta, state, akey)
        local deleted = 0
        for _, owner in ipairs(redis.call('ZRANGE', akey, 0, -1)) do
            deleted = deleted + redis.call('HDEL', state.key, field('a', owner))
        end
        redis.call('DEL', akey)
        state.na = state.na - deleted
        meta.records = meta.records - deleted
        state.ap = 0
    end

    -- ---- transitions ------------------------------------------------------------------------------------------------

    -- Records the transition of the call's own partition (first source state, last target state; a net no-op is dropped).
    local function note(ctx, from, to, reason)
        local source = ctx.transition and ctx.transition.from or NAMES[from]
        if source == NAMES[to] then
            ctx.transition = nil
        else
            ctx.transition = { from = source, to = NAMES[to], reason = reason }
        end
    end

    local function next_generation(state)
        if state.gen >= EXACT_MAX then
            error(redis.error_reply('RT_CORRUPT the generation space is exhausted'), 0)
        end
        return state.gen + 1
    end

    function rt_cb.clear_window(state, wkey)
        state.rg, state.rh, state.rf = '', 0, 0
        redis.call('DEL', wkey)
    end

    -- Host-authorized manual reset: Closed, empty history, every active permit stale, generation advanced. The incarnation
    -- and the terminal records stay. Returns the new generation.
    function rt_cb.reset(meta, state, akey, wkey)
        local generation = next_generation(state)
        drop_permits(meta, state, akey)
        rt_cb.clear_window(state, wkey)
        state.ph = CLOSED
        state.ps = 0
        state.gen = generation
        return generation
    end

    -- Closes only when the success threshold is reached and no admitted probe is still unresolved.
    function rt_cb.settle(ctx, meta, state, akey, wkey)
        if state.ph == HALF_OPEN and state.ap == 0 and state.ps >= state.pt then
            local generation = next_generation(state)
            drop_permits(meta, state, akey)
            rt_cb.clear_window(state, wkey)
            note(ctx, HALF_OPEN, CLOSED, 'S')
            state.ph = CLOSED
            state.ps = 0
            state.gen = generation
        end
    end

    -- `until_s`, `until_u`: the open deadline, computed before any mutation by the caller.
    function rt_cb.enter_open(ctx, meta, state, akey, until_s, until_u, reason)
        local generation = next_generation(state)
        drop_permits(meta, state, akey)
        note(ctx, state.ph, OPEN, reason)
        state.ph = OPEN
        state.ou_s, state.ou_u = until_s, until_u
        state.ps = 0
        state.gen = generation
    end

    function rt_cb.enter_half_open(ctx, meta, state, akey)
        local generation = next_generation(state)
        drop_permits(meta, state, akey)
        note(ctx, OPEN, HALF_OPEN, 'E')
        state.ph = HALF_OPEN
        state.ps = 0
        state.gen = generation
    end

    -- Removes permits whose lease has been reached (neutral: no sample, no recovery credit), at most EXPIRE_BATCH per call,
    -- oldest first. An expired permit that is not read stays counted as active: it can refuse a probe, never admit one too
    -- many, and the next call continues. A freed probe may let a HalfOpen circuit close. A permit whose record is malformed
    -- is left as it is and its index score moves to the next second so that it cannot take the batch of every later call;
    -- the function never raises after it has removed something.
    local EXPIRE_BATCH = 128

    function rt_cb.expire(ctx, meta, state, akey, wkey, es, eu)
        local freed = false
        for _, owner in ipairs(redis.call('ZRANGEBYSCORE', akey, '-inf', exact_int.format_int(es), 'LIMIT', 0, EXPIRE_BATCH)) do
            local readable, xs, xu, _, kind = pcall(rt_cb.permit, state, owner)
            if not readable then
                redis.call('ZADD', akey, exact_int.format_int(es + 1), owner)
            elseif xs == nil then
                redis.call('ZREM', akey, owner)
            elseif rt_state.reached(xs, xu, es, eu) then
                redis.call('ZREM', akey, owner)
                redis.call('HDEL', state.key, field('a', owner))
                state.na = state.na - 1
                meta.records = meta.records - 1
                if kind == 'p' then
                    state.ap = state.ap - 1
                    freed = true
                end
            end
        end
        if freed then
            rt_cb.settle(ctx, meta, state, akey, wkey)
        end
    end

    -- ---- windows ----------------------------------------------------------------------------------------------------

    local function epoch_micros(es, eu)
        return exact_int.add(exact_int.mul(exact_int.from_num(es), exact_int.from_num(MICROS)), exact_int.from_num(eu))
    end

    local function bucket_id(es, eu, granularity)
        local quotient = exact_int.divmod(epoch_micros(es, eu), exact_int.from_num(granularity))
        local id = exact_int.to_num(quotient)
        if id == nil then
            error(redis.error_reply('RT_CONFIG the bucket id leaves the exact range'), 0)
        end
        return id
    end

    -- Buckets older than floor((now - window) / granularity) leave the window (every bucket stays while now <= window).
    local function prune(state, wkey, es, eu)
        if state.wk ~= 't' then
            return
        end
        local now = epoch_micros(es, eu)
        local window = exact_int.from_num(state.wn)
        local minimum = 0
        if exact_int.compare(now, window) > 0 then
            minimum = exact_int.to_num(exact_int.divmod(exact_int.sub(now, window), exact_int.from_num(state.wg)))
        end
        local flat = redis.call('HGETALL', wkey)
        for i = 1, #flat, 2 do
            local ok, id = pcall(exact_int.parse_int, flat[i])
            if not ok then
                error(redis.error_reply('RT_CORRUPT a time bucket id is malformed'), 0)
            end
            if id < minimum then
                redis.call('HDEL', wkey, flat[i])
            end
        end
    end

    -- Records a completed non-neutral outcome.
    function rt_cb.add_sample(state, wkey, failure, es, eu)
        if state.wk == 'c' then
            local mark = failure and '1' or '0'
            if string.len(state.rg) < state.wn then
                state.rg = rt_text.join('', state.rg, mark)
            else
                if string.sub(state.rg, state.rh + 1, state.rh + 1) == '1' then
                    state.rf = state.rf - 1
                end
                state.rg = rt_text.join('', string.sub(state.rg, 1, state.rh), mark, string.sub(state.rg, state.rh + 2))
                state.rh = (state.rh + 1) % state.wn
            end
            if failure then
                state.rf = state.rf + 1
            end
            return
        end
        prune(state, wkey, es, eu)
        local id = exact_int.format_int(bucket_id(es, eu, state.wg))
        local text = redis.call('HGET', wkey, id)
        local total, failures = 0, 0
        if text ~= false then
            local t, f = string.match(text, '^(%d+) (%d+)$')
            if t == nil then
                error(redis.error_reply('RT_CORRUPT a time bucket is malformed'), 0)
            end
            total, failures = exact_int.parse_int(t), exact_int.parse_int(f)
        end
        if total >= MAX_SAMPLES_PER_BUCKET then
            error(redis.error_reply('RT_CAPACITY a circuit time bucket reached its sample ceiling'), 0)
        end
        redis.call('HSET', wkey, id, rt_text.join(' ', exact_int.format_int(total + 1), exact_int.format_int(failures + (failure and 1 or 0))))
    end

    -- Raises RT_CAPACITY, before any change, when the bucket that would take the next sample is full.
    function rt_cb.check_sample_room(state, wkey, es, eu)
        if state.wk ~= 't' then
            return
        end
        local text = redis.call('HGET', wkey, exact_int.format_int(bucket_id(es, eu, state.wg)))
        if text ~= false then
            local t = string.match(text, '^(%d+) %d+$')
            if t == nil or exact_int.parse_int(t) >= MAX_SAMPLES_PER_BUCKET then
                error(redis.error_reply('RT_CAPACITY a circuit time bucket reached its sample ceiling'), 0)
            end
        end
    end

    -- Samples and failures currently inside the window.
    function rt_cb.totals(state, wkey, es, eu)
        if state.wk == 'c' then
            return string.len(state.rg), state.rf
        end
        prune(state, wkey, es, eu)
        local samples, failures = 0, 0
        local flat = redis.call('HGETALL', wkey)
        for i = 2, #flat, 2 do
            local t, f = string.match(flat[i], '^(%d+) (%d+)$')
            if t == nil then
                error(redis.error_reply('RT_CORRUPT a time bucket is malformed'), 0)
            end
            samples = samples + exact_int.parse_int(t)
            failures = failures + exact_int.parse_int(f)
        end
        return samples, failures
    end

    function rt_cb.window_empty(state, wkey, es, eu)
        if state.wk == 'c' then
            return state.rg == ''
        end
        prune(state, wkey, es, eu)
        return redis.call('HLEN', wkey) == 0
    end

    -- ---- idle reclamation ---------------------------------------------------------------------------------------------

    -- One registry-index member: neutral expiry of leases, then deletion of a Closed partition without permits, terminal
    -- records or window content that has been idle for the retention. Open, HalfOpen and count history stay.
    -- Returns 'removed', 'keep' (not idle long enough) or 'later'.
    function rt_cb.reclaim_one(meta, state, akey, wkey, es, eu)
        local ctx = {}
        rt_cb.expire(ctx, meta, state, akey, wkey, es, eu)
        state.ls_s, state.ls_u = es, eu
        local idle_s, idle_u = rt_time.add_micros(state.la_s, state.la_u, rt_state.RETENTION_MICROS)
        local outcome = 'later'
        if not rt_state.reached(idle_s, idle_u, es, eu) then
            outcome = 'keep'
        elseif state.ph == CLOSED and state.na == 0 and state.nt == 0 then
            -- A damaged window is not empty: the expiry above has already written, so this cannot raise.
            local readable, empty = pcall(rt_cb.window_empty, state, wkey, es, eu)
            if readable and empty then
                redis.call('DEL', state.key, akey, wkey)
                meta.partitions = meta.partitions - 1
                return 'removed'
            end
        end
        rt_cb.save(state)
        return outcome
    end

    function rt_cb.index(xkey, state)
        redis.call('ZADD', xkey, exact_int.format_int(state.la_s + rt_state.RETENTION_SECONDS), state.key)
    end

    -- The three reply fields of a transition fact (empty when there is none).
    function rt_cb.fact(ctx)
        if ctx.transition == nil then
            return '', '', ''
        end
        return ctx.transition.from, ctx.transition.to, ctx.transition.reason
    end
end
