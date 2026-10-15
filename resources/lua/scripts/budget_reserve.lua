-- Reserve one retry token.
-- KEYS: 1 registry metadata hash, 2 budget partition hash, 3 pending-reservation sorted set, 4 registry tombstone queue,
--       5 registry rate and budget index (sorted set).
-- ARGV: 1 registry identity, 2 schema version, 3 capacity (tokens), 4 refill (tokens), 5 refill interval (microseconds),
--       6 reservation lease (microseconds), 7 settings fingerprint, 8 attempt (32 hex), 9 owner (32 hex), 10 incarnation
--       to use if the partition has to be created (32 hex, chosen by the caller from a secure source).
-- Reply: { '1', incarnation, generation, expiry seconds, expiry microseconds } reserved; { '0', wait microseconds or '' }
-- exhausted. Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG (domain, settings differ from the
-- identity's, duplicate owner), RT_CAPACITY (a ceiling, after reclaiming expired records and idle partitions and
-- evicting the oldest terminal records).
--
-- Every check that can fail runs before the first write. The accrue-then-expire transition and the reclamation of
-- terminal records are neutral writes. Pending reservations and balances are never evicted.

local meta_key, pkey, akey, qkey, xkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local settings = rt_rb.params(rt_state.int_arg(ARGV[3], 1), rt_state.int_arg(ARGV[4], 1), rt_state.int_arg(ARGV[5], 1))
local lease = rt_state.int_arg(ARGV[6], 1)
local fingerprint = ARGV[7]
local attempt = rt_state.hex_arg(ARGV[8])
local owner = rt_state.hex_arg(ARGV[9])
local new_incarnation = rt_state.hex_arg(ARGV[10])
if type(fingerprint) ~= 'string' or string.len(fingerprint) < 1 or string.len(fingerprint) > 200 then
    error(redis.error_reply('RT_CONFIG the settings fingerprint is invalid'), 0)
end

local state = rt_rb.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
local expiry_s, expiry_u = rt_time.add_micros(es, eu, lease)

-- Checks that precede any write.
if state ~= nil then
    if state.fp ~= fingerprint then
        error(redis.error_reply('RT_CONFIG the policy identity is already bound to different retry budget settings'), 0)
    end
    local pending_s = rt_rb.pending(state, owner)
    local terminal_s, terminal_u = rt_rb.terminal(state, owner)
    if pending_s ~= nil or (terminal_s ~= nil and not rt_state.reached(terminal_s, terminal_u, es, eu)) then
        error(redis.error_reply('RT_CONFIG a reservation owner identity must be unique'), 0)
    end
end

local current = state or { key = pkey, na = 0, nt = 0 }

local function write_back()
    if state ~= nil then
        state.ls_s, state.ls_u = es, eu
        rt_rb.save(state)
        rt_rb.index(xkey, state)
    end
    rt_state.save_counters(meta_key, meta)
    registry.commit_watermark(meta_key, meta, es, eu)
end

if state ~= nil then
    rt_rb.transition(meta, state, akey, qkey, es, eu)
    if redis.call('HEXISTS', pkey, rt_text.join(':', 't', owner)) == 1 then
        rt_cc.drop_own_tombstone(meta, state, owner)
    end
end
rt_cc.drain_tombstones(meta, current, qkey, es, eu)

if state ~= nil and state.bal < rt_rb.COST then
    local wait = rt_rb.wait(state, state.bal, state.pend, state.rem)
    write_back()
    return { '0', wait == nil and '' or exact_int.format_int(wait) }
end

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
    state = rt_rb.create(pkey, fingerprint, new_incarnation, settings, es, eu)
    meta.partitions = meta.partitions + 1
end
rt_rb.add_pending(meta, state, akey, owner, attempt, expiry_s, expiry_u)
state.lm_s, state.lm_u = es, eu
write_back()
return {
    '1',
    state.inc,
    exact_int.format_int(state.gen),
    exact_int.format_int(expiry_s),
    exact_int.format_int(expiry_u),
}
