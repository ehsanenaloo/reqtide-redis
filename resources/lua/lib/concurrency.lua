-- Concurrency partitions: leases, tombstones and bounded reclamation.
--
-- Text placed after rt_state. It defines the local table `rt_cc` only.
--
-- Keys of one partition, all passed in KEYS:
--   pkey  hash   fixed fields fp (capacity and lease text), inc (incarnation), gen (generation), ls_s and ls_u (last
--                effective time), na (active owners, expired ones not yet removed included), nt (tombstones, expired ones
--                not yet removed included); one field "a:<owner>" per active owner (its lease expiry pair) and one field
--                "t:<owner>" per tombstone (its retention deadline pair).
--   akey  zset   member owner, score expiry seconds: the expiry order of the active owners. The score is only an
--                index; the exact decision uses the pair in the hash.
-- Registry-wide, in KEYS:
--   qkey  list   one element "<deadline seconds> <deadline microseconds> <partition key> <owner>" per tombstone, in
--                creation order. The retention is the constant 120 seconds and the effective time never decreases, so the
--                creation order is the deadline order and the head is both the oldest and the first to expire.
--
-- A tombstone slot is the slot of the owner that was released, converted in place: releasing allocates nothing. The value of
-- "t:<owner>" starts with the deadline pair and may carry more text (the retry budget adds a status letter). A partition
-- with the field keep = 1 (a retry budget, which carries a balance) is never deleted for being empty. An entry of
-- qkey whose tombstone no longer exists, or exists with another deadline, is stale and skipped without effect; an entry that
-- is malformed, or names a partition that cannot be read, is removed from the queue without effect.

local rt_cc = {}

do
    local DRAIN_BATCH = 128
    local EVICTION_BATCH_MAX = 64
    -- Queue entries one eviction may examine, removed or not: a long run of stale entries costs a bounded amount of
    -- work per script and is worked off by the following calls and by maintenance.
    local EVICTION_EXAMINE_MAX = 256
    -- Lease candidates one call reads from the index of a partition.
    local EXPIRE_BATCH = 128
    rt_cc.EXPIRE_BATCH = EXPIRE_BATCH
    rt_cc.DRAIN_BATCH = DRAIN_BATCH

    local function field(prefix, owner)
        return rt_text.join(':', prefix, owner)
    end

    -- The partition state, or nil when the key does not exist. Raises RT_CORRUPT for a damaged record.
    function rt_cc.load(pkey)
        if redis.call('EXISTS', pkey) == 0 then
            return nil
        end
        local raw = redis.call('HMGET', pkey, 'fp', 'inc', 'gen', 'ls_s', 'ls_u', 'na', 'nt', 'keep')
        if raw[1] == false or raw[2] == false then
            error(redis.error_reply('RT_CORRUPT a concurrency partition field is missing'), 0)
        end
        local state = {
            key = pkey,
            fp = raw[1],
            inc = raw[2],
            gen = rt_state.field_int(raw[3]),
            ls_s = rt_state.field_int(raw[4]),
            ls_u = rt_state.field_int(raw[5]),
            na = rt_state.field_int(raw[6]),
            nt = rt_state.field_int(raw[7]),
            keep = raw[8] == '1',
        }
        rt_time.pair(state.ls_s, state.ls_u)
        if string.len(state.inc) ~= 32 or not string.find(state.inc, '^[0-9a-f]+$') or state.gen < 1 then
            error(redis.error_reply('RT_CORRUPT a concurrency partition identity is invalid'), 0)
        end
        return state
    end

    -- Writes the fixed fields of a partition state.
    function rt_cc.save(state)
        redis.call('HSET', state.key,
            'kind', 'cc',
            'fp', state.fp,
            'inc', state.inc,
            'gen', exact_int.format_int(state.gen),
            'ls_s', exact_int.format_int(state.ls_s),
            'ls_u', exact_int.format_int(state.ls_u),
            'na', exact_int.format_int(state.na),
            'nt', exact_int.format_int(state.nt))
    end

    -- The lease expiry pair of an active owner, or nil.
    function rt_cc.active_expiry(state, owner)
        return rt_state.pair_read(redis.call('HGET', state.key, field('a', owner)))
    end

    -- The retention deadline pair of a tombstone, or nil.
    function rt_cc.tombstone_deadline(state, owner)
        return rt_state.pair_read(redis.call('HGET', state.key, field('t', owner)))
    end

    -- Removes active owners of the partition whose lease has been reached (neutral: no outcome is recorded), reading at
    -- most EXPIRE_BATCH candidates from the index, oldest first. Returns the number removed. An expired lease that is not
    -- read in this call still counts as active, so a partial reclaim can only refuse an owner, never admit one too many.
    -- A lease whose record is malformed is left as it is and is not counted as expired; its index score moves to the next
    -- second so it cannot take the batch of every later call, and this function never raises after it has removed
    -- something.
    function rt_cc.expire_actives(meta, state, akey, now_s, now_u)
        local candidates = redis.call('ZRANGEBYSCORE', akey, '-inf', exact_int.format_int(now_s), 'LIMIT', 0, EXPIRE_BATCH)
        local removed = 0
        for _, owner in ipairs(candidates) do
            local readable, xs, xu = pcall(rt_cc.active_expiry, state, owner)
            if not readable then
                redis.call('ZADD', akey, exact_int.format_int(now_s + 1), owner)
            elseif xs == nil then
                redis.call('ZREM', akey, owner)
            elseif rt_state.reached(xs, xu, now_s, now_u) then
                redis.call('ZREM', akey, owner)
                redis.call('HDEL', state.key, field('a', owner))
                state.na = state.na - 1
                meta.records = meta.records - 1
                removed = removed + 1
            end
        end
        return removed
    end

    -- Removes one tombstone named by a queue entry, if it is still that tombstone. `current` is the partition state held
    -- in memory by the calling script (changed in memory only, written by the caller); any other partition is updated in
    -- Redis and deleted when it holds nothing. Returns true when a tombstone was removed. It returns false, before it
    -- writes anything, when the named partition cannot be read or its counters cannot take the removal: the caller
    -- consumes the queue entry and the record stays counted, so a damaged partition never raises here.
    local function remove_tombstone(meta, current, ds, du, pkey, owner)
        local state = current
        if pkey ~= current.key then
            local readable, loaded = pcall(rt_cc.load, pkey)
            if not readable or loaded == nil then
                return false
            end
            state = loaded
        end
        local stored = redis.call('HGET', pkey, field('t', owner))
        if stored == false or string.match(stored, '^%d+ %d+') ~= rt_state.pair_text(ds, du) then
            return false
        end
        if state.nt < 1 or meta.records < 1 then
            return false
        end
        redis.call('HDEL', pkey, field('t', owner))
        state.nt = state.nt - 1
        meta.records = meta.records - 1
        if pkey ~= current.key then
            if state.na == 0 and state.nt == 0 and not state.keep then
                redis.call('DEL', pkey)
                meta.partitions = meta.partitions - 1
            else
                redis.call('HSET', pkey, 'nt', exact_int.format_int(state.nt))
            end
        end
        return true
    end

    -- The head of the queue: nil when the queue is empty, false when the head is malformed, otherwise the deadline pair,
    -- the partition key and the owner.
    local function read_entry(qkey)
        local head = redis.call('LINDEX', qkey, 0)
        if head == false then
            return nil
        end
        local s, u, pkey, owner = string.match(head, '^(%d+) (%d+) (%S+) ([0-9a-f]+)$')
        if s == nil or string.len(owner) ~= 32 then
            return false
        end
        local ok, ds, du = pcall(function()
            local seconds, micros = exact_int.parse_int(s), exact_int.parse_int(u)
            rt_time.pair(seconds, micros)
            return seconds, micros
        end)
        if not ok then
            return false
        end
        return ds, du, pkey, owner
    end

    -- Removes the expired tombstones at the head of the queue, at most DRAIN_BATCH entries (one maintenance batch).
    -- Returns the number of entries consumed. A malformed head entry names nothing that can be found again: it is removed
    -- and counted, so it neither blocks the entries behind it nor is examined again by every later call.
    function rt_cc.drain_tombstones(meta, current, qkey, now_s, now_u)
        local consumed = 0
        while consumed < DRAIN_BATCH do
            local ds, du, pkey, owner = read_entry(qkey)
            if ds == nil then
                break
            end
            if ds == false then
                redis.call('LPOP', qkey)
            elseif rt_state.reached(ds, du, now_s, now_u) then
                redis.call('LPOP', qkey)
                remove_tombstone(meta, current, ds, du, pkey, owner)
            else
                break
            end
            consumed = consumed + 1
        end
        return consumed
    end

    -- Evicts the oldest tombstones whether expired or not, until `needed` returns true, the batch
    -- (max(1, min(64, max_records / 16))) is used, or EVICTION_EXAMINE_MAX queue entries were examined. Active owners
    -- are never touched. A malformed entry is removed and uses one place of the batch; a stale entry is removed and uses
    -- only examination budget.
    function rt_cc.evict_tombstones(meta, current, qkey, needed)
        local batch = math.max(1, math.min(EVICTION_BATCH_MAX, math.floor(meta.max_records / 16)))
        local consumed = 0
        local examined = 0
        while consumed < batch and examined < EVICTION_EXAMINE_MAX and not needed() do
            local ds, du, pkey, owner = read_entry(qkey)
            if ds == nil then
                break
            end
            redis.call('LPOP', qkey)
            examined = examined + 1
            if ds == false or remove_tombstone(meta, current, ds, du, pkey, owner) then
                consumed = consumed + 1
            end
        end
    end

    -- Turns the active owner's slot into its tombstone in place: no allocation, so it cannot hit a ceiling.
    function rt_cc.finalize(meta, state, akey, qkey, owner, es, eu)
        local ds, du = rt_time.add_micros(es, eu, rt_state.RETENTION_MICROS)
        redis.call('ZREM', akey, owner)
        redis.call('HDEL', state.key, field('a', owner))
        redis.call('HSET', state.key, field('t', owner), rt_state.pair_text(ds, du))
        redis.call('RPUSH', qkey, rt_text.join(' ', exact_int.format_int(ds), exact_int.format_int(du), state.key, owner))
        state.na = state.na - 1
        state.nt = state.nt + 1
    end

    -- Records a new active owner. The caller has checked the ceilings.
    function rt_cc.add_active(meta, state, akey, owner, xs, xu)
        redis.call('HSET', state.key, field('a', owner), rt_state.pair_text(xs, xu))
        redis.call('ZADD', akey, exact_int.format_int(xs), owner)
        state.na = state.na + 1
        meta.records = meta.records + 1
    end

    -- Removes an expired tombstone of the named owner (a reused owner identity), keeping the counters.
    function rt_cc.drop_own_tombstone(meta, state, owner)
        redis.call('HDEL', state.key, field('t', owner))
        state.nt = state.nt - 1
        meta.records = meta.records - 1
    end

    -- Registers the partition with the registry index at the second its first lease ends; a partition without a lease
    -- needs no examination (its tombstones leave through the queue) and is removed from the index.
    function rt_cc.index(xkey, state, akey)
        local first = redis.call('ZRANGE', akey, 0, 0, 'WITHSCORES')
        if first[2] == nil then
            redis.call('ZREM', xkey, state.key)
        else
            redis.call('ZADD', xkey, first[2], state.key)
        end
    end

    -- Deletes a partition that holds nothing: the next acquire creates a new incarnation.
    function rt_cc.delete_if_empty(meta, state, akey)
        if state.na == 0 and state.nt == 0 then
            redis.call('DEL', state.key, akey)
            meta.partitions = meta.partitions - 1
            return true
        end
        return false
    end
end
