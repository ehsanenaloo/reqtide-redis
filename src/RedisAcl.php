<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

/**
 * The Redis ACL rules that the library needs, and no more. Give them to `ACL SETUSER <name> on ><password> ...` for two
 * different users:
 *
 *  - the **execution** user, used by the application processes: it may run the shipped scripts and the commands they use,
 *    only on the keys of one registry (`{prefix}:*`), and nothing else;
 *  - the **administration** user, used by the host for {@see RedisRegistryAdmin} and {@see RedisCircuitAdmin}: the same
 *    rules and four more commands (`SCAN` and `UNLINK` for the purge of a retired registry, `CONFIG GET` for the server
 *    policy report, `INFO` for the check of the server version).
 *
 * The tests create users from exactly these rules, run every operation with them, and remove one rule at a time to show
 * that each rule is needed and that a missing one is refused before the first write. A script checks its own commands with
 * `redis.acl_check_cmd` before writing, so a user with a rule missing gets `StateUnavailableException` and no partial write.
 *
 * Scripts are sent by digest; the text is loaded with `SCRIPT LOAD` only when the server has lost its cache.
 */
final class RedisAcl
{
    /** Script execution. The connection set-up of Predis needs no rule (shown by the tests: a user without HELLO and CLIENT SETINFO works). */
    private const CONNECTION = ['evalsha', 'script|load'];

    /** The commands the shipped scripts call (they are checked at the start of every mutating script). */
    private const SCRIPT_COMMANDS = [
        'hset', 'hdel', 'del', 'zadd', 'zrem', 'rpush', 'lpop',
        'hget', 'hmget', 'hgetall', 'hexists', 'hlen', 'exists', 'lindex', 'zrange', 'zrangebyscore', 'time',
    ];

    /** Commands of the administration user only. */
    private const ADMINISTRATION = ['scan', 'unlink', 'config|get', 'info'];

    private function __construct() {}

    /**
     * @return list<string> tokens for `ACL SETUSER`, after the name, `on` and the password
     */
    public static function execution(RedisRegistry $registry): array
    {
        return [
            'resetkeys',
            'resetchannels',
            '-@all',
            ...array_map(static fn(string $command): string => '+' . $command, [...self::CONNECTION, ...self::SCRIPT_COMMANDS]),
            '~' . $registry->keyPrefix . ':*',
        ];
    }

    /**
     * @return list<string> tokens for `ACL SETUSER`, after the name, `on` and the password
     */
    public static function administration(RedisRegistry $registry): array
    {
        return [
            ...self::execution($registry),
            ...array_map(static fn(string $command): string => '+' . $command, self::ADMINISTRATION),
        ];
    }
}
