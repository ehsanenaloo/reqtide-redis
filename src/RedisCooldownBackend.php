<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\IndeterminateStateException;
use Enaxon\Reqtide\Exception\StateCapacityExceededException;
use Enaxon\Reqtide\Exception\StateUnavailableException;
use Enaxon\Reqtide\Policy\CooldownPolicy;
use Enaxon\Reqtide\Redis\Client\CommandEffect;
use Enaxon\Reqtide\Redis\Client\RedisBridge;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;
use Enaxon\Reqtide\Redis\State\ScriptReply;
use Enaxon\Reqtide\Redis\State\StateKind;
use Enaxon\Reqtide\Redis\Time\RedisTime;
use Enaxon\Reqtide\State\CooldownBackend;
use Enaxon\Reqtide\State\CooldownDecision;
use Enaxon\Reqtide\State\CooldownStatus;
use Enaxon\Reqtide\State\StateBackendCapabilities;
use Enaxon\Reqtide\Value\CommandId;
use Enaxon\Reqtide\Value\Duration;

/**
 * Shared cooldown on Redis. One atomic script call per operation; the server clock is the
 * only clock (the effective time is the larger of the registry watermark, the stored last effective time and the server
 * time), so a backward clock cannot shorten a cooldown. `extend` is `max(existing, new)` and never shortens; admission is
 * blocked while the effective time is before the not-before instant and allowed at equality.
 *
 * Durations are rounded up to whole microseconds, so a cooldown is never shorter than requested. `CommandId` is accepted
 * for contract symmetry and is not stored: the library never sends a mutation twice, and `max` makes a repeated extension
 * idempotent. A lost reply to `extend` is {@see IndeterminateStateException}; nothing replays it.
 *
 * A cooldown counts as one partition and one record of the registry. A record has no expiry time: it is removed by
 * {@see RedisRegistryMaintenance}, or at a ceiling, once its not-before instant and its cleanup grace have passed.
 */
final class RedisCooldownBackend implements CooldownBackend
{
    private readonly RedisBridge $bridge;

    /**
     * @param RedisBridge|null $bridge internal seam for tests; production code leaves it null
     */
    public function __construct(RedisConnectionOptions $connection, private readonly RedisRegistry $registry, ?RedisBridge $bridge = null)
    {
        $this->bridge = $bridge ?? RedisBridge::forOptions($connection);
    }

    public function capabilities(): StateBackendCapabilities
    {
        $capabilities = $this->bridge->capabilities();

        return new StateBackendCapabilities(
            $capabilities->scope,
            Duration::nanoseconds((int) round($capabilities->ioTimeoutSeconds * 1_000_000_000)),
            Duration::microseconds($capabilities->clockResolutionMicroseconds()),
        );
    }

    /**
     * @throws StateUnavailableException|CorruptStateException
     */
    public function inspect(CooldownPolicy $policy): CooldownDecision
    {
        $layout = $this->registry->keys();
        $reply = $this->bridge->execute(
            ScriptLibrary::cooldownInspect(),
            [$layout->metadata(), $layout->policyPartition(StateKind::Cooldown, $policy->identity)],
            [$this->registry->identity, (string) RedisRegistry::SCHEMA_VERSION, (string) RedisTime::nanosecondsToMicroseconds($policy->cleanupGrace->nanoseconds)],
            CommandEffect::ReadOnly,
        );

        return self::decision($reply);
    }

    /**
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException|StateCapacityExceededException
     */
    public function extend(CooldownPolicy $policy, Duration $extension, CommandId $command): CooldownDecision
    {
        $micros = RedisTime::nanosecondsToMicroseconds($extension->nanoseconds);
        $grace = RedisTime::nanosecondsToMicroseconds($policy->cleanupGrace->nanoseconds);
        RedisTime::withGrace($micros, $grace);

        $layout = $this->registry->keys();
        $reply = $this->bridge->execute(
            ScriptLibrary::cooldownExtend(),
            [$layout->metadata(), $layout->policyPartition(StateKind::Cooldown, $policy->identity), $layout->rateIndex(), $layout->tombstoneQueue()],
            [$this->registry->identity, (string) RedisRegistry::SCHEMA_VERSION, (string) $micros, (string) $grace],
            CommandEffect::Mutating,
        );

        return self::decision($reply);
    }

    private static function decision(mixed $reply): CooldownDecision
    {
        [$status, $remaining] = ScriptReply::strings($reply, 2, 2);
        $micros = ScriptReply::integer($remaining);
        if ($status === '0' && $micros === 0) {
            return new CooldownDecision(CooldownStatus::Clear, Duration::zero());
        }
        if ($status === '1' && $micros > 0) {
            return new CooldownDecision(CooldownStatus::Active, Duration::nanoseconds(RedisTime::microsecondsToNanoseconds($micros)));
        }

        throw new CorruptStateException('A Redis state reply has an unexpected shape.');
    }
}
