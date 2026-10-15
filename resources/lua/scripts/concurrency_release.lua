-- Release one concurrency permit.
-- KEYS: 1 registry metadata hash, 2 partition hash, 3 active-owner sorted set, 4 registry tombstone queue, 5 registry index.
-- ARGV: 1 registry identity, 2 schema version, 3 capacity, 4 lease (microseconds), 5 owner (32 hex), 6 incarnation (32 hex),
--       7 generation, 8 expiry seconds, 9 expiry microseconds (the permit's backend expiry, exact).
-- Reply: { '1' } applied, { '2' } already finalized (the tombstone is still retained), { '3' } stale ownership.
-- Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG (domain, different settings for the identity).
--
-- A release acts only on a matching active owner (incarnation, generation, owner and exact expiry, not yet expired), so a
-- repeat after the tombstone is gone finds no active owner and changes nothing. It converts the owner's slot into its
-- tombstone in place and allocates nothing, so it cannot fail on a ceiling.

local meta_key, pkey, akey, qkey, xkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local capacity = rt_state.int_arg(ARGV[3], 1)
local lease = rt_state.int_arg(ARGV[4], 1)
local owner = rt_state.hex_arg(ARGV[5])
local incarnation = rt_state.hex_arg(ARGV[6])
local generation = rt_state.int_arg(ARGV[7], 1)
local permit_s = rt_state.int_arg(ARGV[8], 0)
local permit_u = rt_state.int_arg(ARGV[9], 0)
local fingerprint = rt_text.join(' ', exact_int.format_int(capacity), exact_int.format_int(lease))

local state = rt_cc.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)

if state ~= nil and state.fp ~= fingerprint then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to different concurrency settings'), 0)
end

-- The owner's records are read before the first write, so a damaged one fails the call with nothing changed.
if state ~= nil and state.inc == incarnation then
    rt_cc.active_expiry(state, owner)
    rt_cc.tombstone_deadline(state, owner)
end

local function write_back()
    if state ~= nil then
        if rt_cc.delete_if_empty(meta, state, akey) then
            state = nil
        else
            state.ls_s, state.ls_u = es, eu
            rt_cc.save(state)
            rt_cc.index(xkey, state, akey)
        end
    end
    rt_state.save_counters(meta_key, meta)
    registry.commit_watermark(meta_key, meta, es, eu)
end

if state == nil or state.inc ~= incarnation or state.gen ~= generation then
    write_back()
    return { '3' }
end

-- An expired lease (including exactly at expiry) is removed neutrally first, so it is stale.
rt_cc.expire_actives(meta, state, akey, es, eu)
rt_cc.drain_tombstones(meta, state, qkey, es, eu)

local status = '3'
local xs, xu = rt_cc.active_expiry(state, owner)
if xs ~= nil then
    if xs == permit_s and xu == permit_u then
        rt_cc.finalize(meta, state, akey, qkey, owner, es, eu)
        status = '1'
    end
else
    local ds, du = rt_cc.tombstone_deadline(state, owner)
    if ds ~= nil and not rt_state.reached(ds, du, es, eu) then
        status = '2'
    end
end
write_back()
return { status }
