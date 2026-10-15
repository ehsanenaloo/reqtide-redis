-- Manual reset of one circuit partition. A manual reset advances the generation.
-- KEYS: 1 registry metadata hash, 2 partition hash, 3 active-permit sorted set, 4 time-bucket hash, 5 registry index.
-- ARGV: 1 registry identity, 2 schema version.
-- Reply: { '0' } when the partition does not exist, { '1', generation } with the new generation otherwise.
-- Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT (damaged state or exhausted generation space), RT_CONFIG.
--
-- The circuit becomes Closed with an empty health history, every active permit becomes stale, the generation advances by one
-- and the incarnation and the terminal records stay. It is an explicit operator action: no automatic path calls it.

local meta_key, pkey, akey, wkey, xkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]

registry.require_access(false)
registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)

local state = rt_cb.load(pkey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state == nil then
    registry.commit_watermark(meta_key, meta, es, eu)
    return { '0' }
end

local generation = rt_cb.reset(meta, state, akey, wkey)
state.ls_s, state.ls_u = es, eu
state.la_s, state.la_u = es, eu
rt_cb.save(state)
rt_cb.index(xkey, state)
rt_state.save_counters(meta_key, meta)
registry.commit_watermark(meta_key, meta, es, eu)
return { '1', exact_int.format_int(generation) }
