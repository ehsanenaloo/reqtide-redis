-- Acquire one concurrency permit.
-- KEYS: 1 registry metadata hash, 2 partition hash, 3 active-owner sorted set, 4 registry tombstone queue, 5 registry index.
-- ARGV: 1 registry identity, 2 schema version, 3 capacity, 4 lease (microseconds), 5 owner (32 hex), 6 incarnation to use
--       if the partition has to be created (32 hex, chosen by the caller from a secure source).
-- Reply: { '1', incarnation, generation, expiry seconds, expiry microseconds } when accepted, { '0' } when the capacity
-- is exhausted. Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG (domain, different settings for the
-- identity, duplicate owner), RT_CAPACITY (a ceiling, after reclaiming expired state and evicting the oldest tombstones).
--
-- Every check that can fail runs before the first write of the command; reclamation writes are neutral (they remove
-- expired or evictable records only). Time is read once, from the server, and combined with the registry watermark and the
-- partition's last effective time, so a backward clock cannot expire a lease early.

local meta_key, pkey, akey, qkey, xkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local capacity = rt_state.int_arg(ARGV[3], 1)
local lease = rt_state.int_arg(ARGV[4], 1)
local owner = rt_state.hex_arg(ARGV[5])
local new_incarnation = rt_state.hex_arg(ARGV[6])
local fingerprint = rt_text.join(' ', exact_int.format_int(capacity), exact_int.format_int(lease))

local state = rt_cc.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)

-- Checks that precede any write.
if state ~= nil then
    if state.fp ~= fingerprint then
        error(redis.error_reply('RT_CONFIG the policy identity is already bound to different concurrency settings'), 0)
    end
    local xs, xu = rt_cc.active_expiry(state, owner)
    if xs ~= nil and not rt_state.reached(xs, xu, es, eu) then
        error(redis.error_reply('RT_CONFIG a permit owner identity must be unique'), 0)
    end
    local ds, du = rt_cc.tombstone_deadline(state, owner)
    if ds ~= nil and not rt_state.reached(ds, du, es, eu) then
        error(redis.error_reply('RT_CONFIG a permit owner identity must be unique'), 0)
    end
end
local expiry_s, expiry_u = rt_time.add_micros(es, eu, lease)

local current = state or { key = pkey, na = 0, nt = 0 }

local function write_back()
    if state ~= nil then
        state.ls_s, state.ls_u = es, eu
        rt_cc.save(state)
        rt_cc.index(xkey, state, akey)
    end
    rt_state.save_counters(meta_key, meta)
    registry.commit_watermark(meta_key, meta, es, eu)
end

-- Neutral reclamation: expired leases of this partition, expired tombstones at the head of the queue.
if state ~= nil then
    rt_cc.expire_actives(meta, state, akey, es, eu)
    local ds, du = rt_cc.tombstone_deadline(state, owner)
    if ds ~= nil then
        rt_cc.drop_own_tombstone(meta, state, owner)
    end
end
rt_cc.drain_tombstones(meta, current, qkey, es, eu)
if state ~= nil and rt_cc.delete_if_empty(meta, state, akey) then
    state = nil
    current = { key = pkey, na = 0, nt = 0 }
end

if state ~= nil and state.na >= capacity then
    write_back()
    return { '0' }
end

-- Room for one record, and for one partition when it is new.
local function has_room()
    return meta.records < meta.max_records and (state ~= nil or meta.partitions < meta.max_partitions)
end
if not has_room() then
    -- Expired leases and idle partitions elsewhere; this partition is held in memory, so it is kept out of the index
    -- for the call (write_back registers it again).
    if state ~= nil then
        redis.call('ZREM', xkey, pkey)
    end
    rt_rate.reclaim(meta, xkey, qkey, es, eu)
end
if not has_room() then
    rt_cc.evict_tombstones(meta, current, qkey, has_room)
end
if not has_room() then
    write_back()
    error(redis.error_reply('RT_CAPACITY the state registry is at capacity; new allocation rejected'), 0)
end

if state == nil then
    state = { key = pkey, fp = fingerprint, inc = new_incarnation, gen = 1, ls_s = es, ls_u = eu, na = 0, nt = 0 }
    meta.partitions = meta.partitions + 1
end
rt_cc.add_active(meta, state, akey, owner, expiry_s, expiry_u)
write_back()
return {
    '1',
    state.inc,
    exact_int.format_int(state.gen),
    exact_int.format_int(expiry_s),
    exact_int.format_int(expiry_u),
}
