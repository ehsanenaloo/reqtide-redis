-- Cooldown partitions.
--
-- Text placed after rt_state. It defines the local table `rt_cd` only.
--
-- One hash per cooldown: kind = cd, nb_s and nb_u (the not-before instant), ls_s and ls_u (last effective time), gr (cleanup grace in
-- microseconds, bound to the policy identity). Admission is blocked while the effective time is before the not-before
-- instant and allowed at equality.

local rt_cd = {}

do
    function rt_cd.load(ckey)
        if redis.call('EXISTS', ckey) == 0 then
            return nil
        end
        local raw = redis.call('HMGET', ckey, 'nb_s', 'nb_u', 'ls_s', 'ls_u', 'gr')
        local state = {
            key = ckey,
            nb_s = rt_state.field_int(raw[1]),
            nb_u = rt_state.field_int(raw[2]),
            ls_s = rt_state.field_int(raw[3]),
            ls_u = rt_state.field_int(raw[4]),
            gr = rt_state.field_int(raw[5]),
        }
        rt_time.pair(state.nb_s, state.nb_u)
        rt_time.pair(state.ls_s, state.ls_u)
        return state
    end

    function rt_cd.save(state)
        redis.call('HSET', state.key,
            'kind', 'cd',
            'nb_s', exact_int.format_int(state.nb_s),
            'nb_u', exact_int.format_int(state.nb_u),
            'ls_s', exact_int.format_int(state.ls_s),
            'ls_u', exact_int.format_int(state.ls_u),
            'gr', exact_int.format_int(state.gr))
    end

    -- The instant at which maintenance may remove the cooldown: the not-before instant plus the cleanup grace.
    function rt_cd.removal(state)
        return rt_time.add_micros(state.nb_s, state.nb_u, state.gr)
    end

    function rt_cd.index(xkey, state)
        local rs = rt_cd.removal(state)
        redis.call('ZADD', xkey, exact_int.format_int(rs), state.key)
    end

    -- The reply of both scripts: clear, or active with the exact remaining microseconds.
    function rt_cd.decision(state, es, eu)
        if rt_time.compare(es, eu, state.nb_s, state.nb_u) >= 0 then
            return { '0', '0' }
        end
        return { '1', exact_int.format_int(rt_time.diff_micros(state.nb_s, state.nb_u, es, eu)) }
    end
end
