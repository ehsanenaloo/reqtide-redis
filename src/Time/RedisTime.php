<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Time;

use Enaxon\Reqtide\Exception\InvalidConfigurationException;

/**
 * Checked conversions between Core durations (nanoseconds) and the Redis decision resolution (microseconds), with the
 * shared-state maximum of LUA_EXACT_MAX microseconds.
 *
 * @internal
 */
final class RedisTime
{
    public const EXACT_MAX = 9_007_199_254_740_991;

    private function __construct() {}

    /**
     * Rounds up to whole microseconds, so a wait is never shorter than the exact value; a non-zero duration below one
     * microsecond becomes one microsecond. Rejects what exceeds the exact range.
     */
    public static function nanosecondsToMicroseconds(int $nanoseconds): int
    {
        if ($nanoseconds < 0) {
            throw new InvalidConfigurationException('A duration cannot be negative.');
        }
        $micros = intdiv($nanoseconds, 1000) + ($nanoseconds % 1000 === 0 ? 0 : 1);
        if ($micros > self::EXACT_MAX) {
            throw new InvalidConfigurationException('The duration exceeds the shared-state maximum.');
        }

        return $micros;
    }

    /** Back to nanoseconds; fits int64 for every valid microsecond value (checked, not assumed). */
    public static function microsecondsToNanoseconds(int $microseconds): int
    {
        if ($microseconds < 0 || $microseconds > self::EXACT_MAX) {
            throw new InvalidConfigurationException('The duration exceeds the shared-state maximum.');
        }

        return $microseconds * 1000;
    }

    /** Duration plus a retention or lease grace, checked against the exact range before use (for example a cooldown plus 60 s). */
    public static function withGrace(int $microseconds, int $graceMicroseconds): int
    {
        if ($microseconds < 0 || $graceMicroseconds < 0 || $microseconds > self::EXACT_MAX - $graceMicroseconds) {
            throw new InvalidConfigurationException('The duration plus its grace exceeds the shared-state maximum.');
        }

        return $microseconds + $graceMicroseconds;
    }
}
