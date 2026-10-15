-- Read-only inspection of a shared cooldown.
-- KEYS: 1 registry metadata hash, 2 cooldown hash.
-- ARGV: 1 registry identity, 2 schema version, 3 cleanup grace (microseconds).
-- Reply: { '0', '0' } clear, { '1', remaining microseconds } active. It writes nothing: no record is allocated, no retention
-- is extended and the watermark is not advanced; the effective time is computed locally as the larger of the registry
-- watermark, the stored last effective time and the server time, so a backward clock cannot shorten a cooldown.
-- Registered errors: RT_NO_REGISTRY, RT_IDENTITY, RT_CORRUPT, RT_CONFIG (different cleanup grace for the identity).

local meta_key, ckey = KEYS[1], KEYS[2]

registry.check_identity(ARGV[1])
local schema = rt_state.int_arg(ARGV[2], 1)
local grace = rt_state.int_arg(ARGV[3], 0)

local state = rt_cd.load(ckey)
local meta, es, eu = registry.begin(meta_key, ARGV[1], schema, state and state.ls_s or 0, state and state.ls_u or 0)
if state == nil then
    return { '0', '0' }
end
if state.gr ~= grace then
    error(redis.error_reply('RT_CONFIG the policy identity is already bound to a different cleanup grace'), 0)
end
return rt_cd.decision(state, es, eu)
