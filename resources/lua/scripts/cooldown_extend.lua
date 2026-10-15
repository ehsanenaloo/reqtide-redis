-- Extend a shared cooldown: atomic max(existing, new), never shortens.
-- KEYS: 1 registry metadata hash, 2 cooldown hash, 3 registry index, 4 registry tombstone queue.
-- ARGV: 1 registry identity, 2 schema version, 3 extension (microseconds), 4 cleanup grace (microseconds).
-- Reply: { '0', '0' } clear, { '1', remaining microseconds } active. A zero extension of an absent cooldown allocates
-- nothing. A new cooldown counts as one partition and one record; at a ceiling the command raises RT_CAPACITY before any
-- change to a cooldown, after reclaiming expired cooldowns, leases and tombstones and evicting the oldest tombstones. The
-- record has no expiry: bounded maintenance removes it when its not-before instant and cleanup grace have passed.
-- Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG (domain, different cleanup grace, arithmetic range),
-- RT_CAPACITY.

local meta_key, ckey, xkey, qkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local extension = rt_state.int_arg(ARGV[3], 0)
local grace = rt_state.int_arg(ARGV[4], 0)

local state = rt_cd.load(ckey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state ~= nil and state.gr ~= grace then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to a different cleanup grace'), 0)
end
-- The not-before instant and the end of the cleanup grace must both stay in the exact range.
local new_s, new_u = rt_time.add_micros(es, eu, extension)
rt_time.add_micros(new_s, new_u, grace)

if state ~= nil then
    state.nb_s, state.nb_u = rt_time.max(state.nb_s, state.nb_u, new_s, new_u)
else
    if rt_time.compare(new_s, new_u, es, eu) <= 0 then
        -- A zero extension protects nothing and must not allocate.
        registry.commit_watermark(meta_key, meta, es, eu)
        return { '0', '0' }
    end
    local function has_room()
        return meta.records < meta.max_records and meta.partitions < meta.max_partitions
    end
    local none = { key = '', na = 0, nt = 0 }
    if not has_room() then
        rt_rate.reclaim(meta, xkey, qkey, es, eu)
        rt_cc.drain_tombstones(meta, none, qkey, es, eu)
    end
    if not has_room() then
        rt_cc.evict_tombstones(meta, none, qkey, has_room)
    end
    if not has_room() then
        rt_state.save_counters(meta_key, meta)
        registry.commit_watermark(meta_key, meta, es, eu)
        error(redis.error_reply('RT_CAPACITY the state registry is at capacity; new allocation rejected'), 0)
    end
    state = { key = ckey, nb_s = new_s, nb_u = new_u, gr = grace }
    meta.partitions = meta.partitions + 1
    meta.records = meta.records + 1
    rt_state.save_counters(meta_key, meta)
end
state.ls_s, state.ls_u = es, eu
rt_cd.save(state)
rt_cd.index(xkey, state)
registry.commit_watermark(meta_key, meta, es, eu)
return rt_cd.decision(state, es, eu)
