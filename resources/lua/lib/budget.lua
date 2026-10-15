-- Retry budget partitions: a token bucket with pending reservations.
--
-- Text placed after rt_cc. It defines the local table `rt_rb` only.
--
-- Amounts are scaled units (1 token = 1,000,000); one reservation costs one token. The invariant is
-- 0 <= balance + pending cost <= capacity. Before every reserve, commit, release or expiry the elapsed refill is accrued
-- with the OLD pending cost and clamped to the OLD headroom (capacity - pending cost); then every reservation whose lease
-- has been reached becomes an Expired record in place and refunds only its own cost, capped at the NEW headroom. A held
-- reservation therefore never mints a token through retroactive refill or refund.
--
-- One hash per partition (key pkey). The fields of a concurrency partition (fp, inc, gen, ls_s, ls_u, na = pending
-- count, nt = terminal records) plus: keep = 1, kind = rb, cap, rate, itv (settings), bal, rem, pend (bucket), lr_s and
-- lr_u (last refill: the origin of the next elapsed time), lm_s and lm_u (last mutation: the origin of the idle
-- retention). "a:<owner>" holds "<expiry seconds> <expiry microseconds> <attempt>", "t:<owner>" holds
-- "<deadline seconds> <deadline microseconds> <c|r|x>" (committed, released, expired). The sorted set akey orders the
-- pending reservations by expiry second. Terminal records use the registry tombstone queue of rt_cc.

local rt_rb = {}

do
    local SCALE = 1000000
    local COST = SCALE
    local MICROS = 1000000
    local MAX_TOKENS = 9007199254
    local EXACT_MAX = exact_int.EXACT_MAX

    local function field(prefix, owner)
        return rt_text.join(':', prefix, owner)
    end

    rt_rb.COST = COST

    -- Settings from script arguments: capacity and refill in tokens, interval and lease in microseconds.
    function rt_rb.params(capacity, refill, interval)
        if capacity < 1 or capacity > MAX_TOKENS or refill < 1 or refill > MAX_TOKENS or interval < 1 then
            error(redis.error_reply('RT_CONFIG a retry budget setting is outside its domain'), 0)
        end
        return { capacity = capacity * SCALE, rate = refill * SCALE, interval = interval }
    end

    function rt_rb.load(pkey)
        local state = rt_cc.load(pkey)
        if state == nil then
            return nil
        end
        local raw = redis.call('HMGET', pkey, 'kind', 'cap', 'rate', 'itv', 'bal', 'rem', 'pend', 'lr_s', 'lr_u', 'lm_s', 'lm_u')
        if raw[1] ~= 'rb' or not state.keep then
            error(redis.error_reply('RT_CORRUPT the partition is not a retry budget'), 0)
        end
        state.capacity = rt_state.field_int(raw[2])
        state.rate = rt_state.field_int(raw[3])
        state.interval = rt_state.field_int(raw[4])
        state.bal = rt_state.field_int(raw[5])
        state.rem = rt_state.field_int(raw[6])
        state.pend = rt_state.field_int(raw[7])
        state.lr_s = rt_state.field_int(raw[8])
        state.lr_u = rt_state.field_int(raw[9])
        state.lm_s = rt_state.field_int(raw[10])
        state.lm_u = rt_state.field_int(raw[11])
        rt_time.pair(state.lr_s, state.lr_u)
        rt_time.pair(state.lm_s, state.lm_u)
        if state.capacity < COST or state.rate < 1 or state.interval < 1 or state.rem >= state.interval
            or state.pend ~= state.na * COST or state.bal + state.pend > state.capacity then
            error(redis.error_reply('RT_CORRUPT a retry budget state is out of bounds'), 0)
        end
        return state
    end

    function rt_rb.save(state)
        rt_cc.save(state)
        redis.call('HSET', state.key,
            'keep', '1',
            'kind', 'rb',
            'cap', exact_int.format_int(state.capacity),
            'rate', exact_int.format_int(state.rate),
            'itv', exact_int.format_int(state.interval),
            'bal', exact_int.format_int(state.bal),
            'rem', exact_int.format_int(state.rem),
            'pend', exact_int.format_int(state.pend),
            'lr_s', exact_int.format_int(state.lr_s),
            'lr_u', exact_int.format_int(state.lr_u),
            'lm_s', exact_int.format_int(state.lm_s),
            'lm_u', exact_int.format_int(state.lm_u))
    end

    -- A fresh full bucket at (es, eu).
    function rt_rb.create(pkey, fingerprint, incarnation, settings, es, eu)
        return {
            key = pkey, fp = fingerprint, inc = incarnation, gen = 1, ls_s = es, ls_u = eu, na = 0, nt = 0, keep = true,
            capacity = settings.capacity, rate = settings.rate, interval = settings.interval,
            bal = settings.capacity, rem = 0, pend = 0, lr_s = es, lr_u = eu, lm_s = es, lm_u = eu,
        }
    end

    -- Pending reservation of an owner: expiry pair and attempt, or nil.
    function rt_rb.pending(state, owner)
        local text = redis.call('HGET', state.key, field('a', owner))
        if text == false then
            return nil
        end
        local s, u, attempt = string.match(text, '^(%d+) (%d+) (%x+)$')
        if s == nil then
            error(redis.error_reply('RT_CORRUPT a pending reservation is malformed'), 0)
        end
        local ok, xs, xu = pcall(function()
            return exact_int.parse_int(s), exact_int.parse_int(u)
        end)
        if not ok then
            error(redis.error_reply('RT_CORRUPT a pending reservation is malformed'), 0)
        end
        rt_time.pair(xs, xu)
        return xs, xu, attempt
    end

    -- Terminal record of an owner: deadline pair and status letter, or nil.
    function rt_rb.terminal(state, owner)
        local text = redis.call('HGET', state.key, field('t', owner))
        if text == false then
            return nil
        end
        local s, u, status = string.match(text, '^(%d+) (%d+) ([crx])$')
        if s == nil then
            error(redis.error_reply('RT_CORRUPT a terminal record is malformed'), 0)
        end
        local ok, ds, du = pcall(function()
            return exact_int.parse_int(s), exact_int.parse_int(u)
        end)
        if not ok then
            error(redis.error_reply('RT_CORRUPT a terminal record is malformed'), 0)
        end
        rt_time.pair(ds, du)
        return ds, du, status
    end

    -- Balance and remainder after refill to (es, eu) with the CURRENT pending cost; changes nothing.
    local function accrued(state, es, eu)
        local elapsed = rt_time.diff_micros(es, eu, state.lr_s, state.lr_u)
        if elapsed <= 0 then
            return state.bal, state.rem
        end
        local headroom = state.capacity - state.pend
        local deficit = headroom - state.bal
        if deficit <= 0 then
            return state.bal, 0
        end
        local added, remainder, capped = exact_int.mul_add_div_floor_capped(elapsed, state.rate, state.rem, state.interval, deficit)
        if capped then
            return headroom, 0
        end
        return state.bal + added, remainder
    end

    -- The owners whose lease has been reached, in no particular order (the refund does not depend on the order). A member of
    -- the index without a record is stale: it is removed from the index unless `read_only` is set (inspection writes
    -- nothing and does not count it). A malformed record raises before anything of the caller is written.
    -- At most EXPIRE_BATCH reservations are read per call, oldest first. A reservation that is not read stays held: it can
    -- only reduce the headroom, never raise it, and the next call continues.
    local EXPIRE_BATCH = 128

    local function expired_owners(state, akey, es, eu, read_only)
        local result = {}
        for _, owner in ipairs(redis.call('ZRANGEBYSCORE', akey, '-inf', exact_int.format_int(es), 'LIMIT', 0, EXPIRE_BATCH)) do
            local xs, xu = rt_rb.pending(state, owner)
            if xs == nil then
                if not read_only then
                    redis.call('ZREM', akey, owner)
                end
            elseif rt_state.reached(xs, xu, es, eu) then
                result[#result + 1] = owner
            end
        end
        return result
    end

    -- Accrue, then expire: the ordering required before any reserve, commit, release or expiry.
    function rt_rb.transition(meta, state, akey, qkey, es, eu)
        state.bal, state.rem = accrued(state, es, eu)
        state.lr_s, state.lr_u = es, eu
        local owners = expired_owners(state, akey, es, eu)
        if #owners == 0 then
            return
        end
        local ds, du = rt_time.add_micros(es, eu, rt_state.RETENTION_MICROS)
        for _, owner in ipairs(owners) do
            redis.call('ZREM', akey, owner)
            redis.call('HDEL', state.key, field('a', owner))
            state.na = state.na - 1
            state.pend = state.pend - COST
            state.bal = math.min(state.bal + COST, state.capacity - state.pend)
            redis.call('HSET', state.key, field('t', owner), rt_text.join(' ', rt_state.pair_text(ds, du), 'x'))
            redis.call('RPUSH', qkey, rt_text.join(' ', exact_int.format_int(ds), exact_int.format_int(du), state.key, owner))
            state.nt = state.nt + 1
        end
        state.lm_s, state.lm_u = es, eu
    end

    -- Balance, pending cost and remainder as they would be after transition, without changing anything.
    function rt_rb.view(state, akey, es, eu)
        local balance, remainder = accrued(state, es, eu)
        local pending = state.pend
        for _ = 1, #expired_owners(state, akey, es, eu, true) do
            pending = pending - COST
            balance = math.min(balance + COST, state.capacity - pending)
        end
        return balance, pending, remainder
    end

    -- Earliest wait (microseconds) until one more token is available; nil when held reservations leave no headroom for
    -- it or the wait does not fit the exact range.
    function rt_rb.wait(state, balance, pending, remainder)
        if state.capacity - pending < COST then
            return nil
        end
        local ticks, capped = exact_int.mul_sub_ceil_div_capped(COST - balance, state.interval, remainder, state.rate, EXACT_MAX)
        if capped then
            return nil
        end
        return ticks
    end

    -- Converts the pending reservation of an owner into a terminal record in place (commit: c, release: r).
    function rt_rb.finalize(meta, state, akey, qkey, owner, es, eu, commit)
        local ds, du = rt_time.add_micros(es, eu, rt_state.RETENTION_MICROS)
        redis.call('ZREM', akey, owner)
        redis.call('HDEL', state.key, field('a', owner))
        state.na = state.na - 1
        state.pend = state.pend - COST
        if not commit then
            state.bal = math.min(state.bal + COST, state.capacity - state.pend)
        end
        redis.call('HSET', state.key, field('t', owner), rt_text.join(' ', rt_state.pair_text(ds, du), commit and 'c' or 'r'))
        redis.call('RPUSH', qkey, rt_text.join(' ', exact_int.format_int(ds), exact_int.format_int(du), state.key, owner))
        state.nt = state.nt + 1
        state.lm_s, state.lm_u = es, eu
    end

    function rt_rb.add_pending(meta, state, akey, owner, attempt, xs, xu)
        redis.call('HSET', state.key, field('a', owner), rt_text.join(' ', rt_state.pair_text(xs, xu), attempt))
        redis.call('ZADD', akey, exact_int.format_int(xs), owner)
        state.na = state.na + 1
        state.bal = state.bal - COST
        state.pend = state.pend + COST
        meta.records = meta.records + 1
    end

    -- Idle reclamation of one registry-index member: applies the accrue-then-expire transition (so expired reservations become
    -- terminal records and the balance is current), then removes a full bucket without records that has been idle for the
    -- retention. Returns 'removed', 'keep' (not idle long enough) or 'later' (protective: look again behind the others).
    function rt_rb.reclaim_one(meta, state, akey, qkey, es, eu)
        rt_rb.transition(meta, state, akey, qkey, es, eu)
        state.ls_s, state.ls_u = es, eu
        local idle_s, idle_u = rt_time.add_micros(state.lm_s, state.lm_u, rt_state.RETENTION_MICROS)
        local outcome = 'later'
        if not rt_state.reached(idle_s, idle_u, es, eu) then
            outcome = 'keep'
        elseif state.na == 0 and state.nt == 0 and state.bal == state.capacity then
            redis.call('DEL', state.key, akey)
            meta.partitions = meta.partitions - 1
            return 'removed'
        end
        rt_rb.save(state)
        return outcome
    end

    function rt_rb.index(xkey, state)
        redis.call('ZADD', xkey, exact_int.format_int(state.lm_s + rt_state.RETENTION_SECONDS), state.key)
    end
end
