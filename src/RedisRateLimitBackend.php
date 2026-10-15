<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Admission\AdmissionDecision;
use Enaxon\Reqtide\Admission\AdmissionStatus;
use Enaxon\Reqtide\Admission\RateLimitInspection;
use Enaxon\Reqtide\Error\ReasonCode;
use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\IndeterminateStateException;
use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Exception\StateCapacityExceededException;
use Enaxon\Reqtide\Exception\StateUnavailableException;
use Enaxon\Reqtide\Policy\FixedWindowAnchor;
use Enaxon\Reqtide\Policy\FixedWindowOptions;
use Enaxon\Reqtide\Policy\RateLimitPolicy;
use Enaxon\Reqtide\Policy\SlidingCounterOptions;
use Enaxon\Reqtide\Policy\TokenBucketOptions;
use Enaxon\Reqtide\Redis\Client\CommandEffect;
use Enaxon\Reqtide\Redis\Client\RedisBridge;
use Enaxon\Reqtide\Redis\Script\Script;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;
use Enaxon\Reqtide\Redis\State\ScriptReply;
use Enaxon\Reqtide\Redis\State\StateKind;
use Enaxon\Reqtide\Redis\Time\RedisTime;
use Enaxon\Reqtide\State\RateLimitBackend;
use Enaxon\Reqtide\State\StateBackendCapabilities;
use Enaxon\Reqtide\Value\CommandId;
use Enaxon\Reqtide\Value\Duration;
use Enaxon\Reqtide\Value\NonNegativeInt;
use Enaxon\Reqtide\Value\PositiveInt;

/**
 * Shared rate limits on Redis: token bucket, fixed window (aligned to the server epoch, or first-hit) and approximate
 * sliding counter. One atomic script call per operation; the server clock is
 * the only clock and a backward clock never refills or resets anything.
 *
 * Decision resolution is one microsecond: a configured refill interval or window is rounded up to whole microseconds,
 * so a refill is never faster and a window never shorter than configured (the settings stay bound to the policy identity
 * in nanoseconds). Waits are exact ceilings of the same arithmetic, converted to nanoseconds, and never early. Admission
 * is decided with exact integer arithmetic on limbs; no float is involved. `consume` is never sent twice: a lost reply is
 * {@see IndeterminateStateException}, the debit is not claimed, and nothing reconciles or replays it. `CommandId` is
 * accepted for contract symmetry and is not stored.
 *
 * `inspect` is mutation-free: it debits, resets, allocates, reclaims and advances nothing. At the partition ceiling a new
 * partition first reclaims idle partitions that are equivalent to fresh state (bounded, 128 per call), then is rejected
 * with {@see StateCapacityExceededException}; protective state is never evicted.
 */
final class RedisRateLimitBackend implements RateLimitBackend
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
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException|StateCapacityExceededException
     */
    public function consume(RateLimitPolicy $policy, PositiveInt $cost, CommandId $command): AdmissionDecision
    {
        $settings = self::settings($policy);
        if ($cost->value > $settings['capacity']) {
            return new AdmissionDecision(AdmissionStatus::Impossible, ReasonCode::RateCostExceedsCapacity->value, null, null);
        }

        $items = $this->run(ScriptLibrary::rateConsume(), $policy, $settings, $cost->value, CommandEffect::Mutating);
        if ($items[0] === '1' && count($items) === 2) {
            $remaining = ScriptReply::integer($items[1]) - $cost->value;
            if ($remaining < 0) {
                throw new CorruptStateException('A Redis state reply has an unexpected shape.');
            }

            return new AdmissionDecision(AdmissionStatus::Accepted, ReasonCode::RateAccepted->value, new NonNegativeInt($remaining), null);
        }
        if ($items[0] === '2' && count($items) === 4) {
            [$available, $retry] = self::rejection($items);

            return new AdmissionDecision(
                AdmissionStatus::Rejected,
                $retry === null ? ReasonCode::RateWaitUnrepresentable->value : ReasonCode::RateExhausted->value,
                new NonNegativeInt($available),
                $retry,
            );
        }

        throw new CorruptStateException('A Redis state reply has an unexpected shape.');
    }

    /**
     * @throws StateUnavailableException|CorruptStateException
     */
    public function inspect(RateLimitPolicy $policy, PositiveInt $requestedCost): RateLimitInspection
    {
        $settings = self::settings($policy);
        // A cost above the capacity is only asked about, never evaluated; the script reports the available amount.
        $cost = min($requestedCost->value, $settings['capacity'] + 1);

        $items = $this->run(ScriptLibrary::rateInspect(), $policy, $settings, $cost, CommandEffect::ReadOnly);
        if ($items[0] === '0' && count($items) === 2) {
            return new RateLimitInspection(new NonNegativeInt(ScriptReply::integer($items[1])), false, null);
        }
        if ($items[0] === '1' && count($items) === 2) {
            return new RateLimitInspection(new NonNegativeInt(ScriptReply::integer($items[1])), true, null);
        }
        if ($items[0] === '2' && count($items) === 4) {
            [$available, $retry] = self::rejection($items);

            return new RateLimitInspection(new NonNegativeInt($available), false, $retry);
        }

        throw new CorruptStateException('A Redis state reply has an unexpected shape.');
    }

    /**
     * @param array{kind: string, fingerprint: string, capacity: int, a: int, b: int} $settings
     * @return list<string>
     */
    private function run(Script $script, RateLimitPolicy $policy, array $settings, int $cost, CommandEffect $effect): array
    {
        $layout = $this->registry->keys();
        $reply = $this->bridge->execute(
            $script,
            [$layout->metadata(), $layout->policyPartition(StateKind::RateLimit, $policy->identity), $layout->rateIndex(), $layout->tombstoneQueue()],
            [
                $this->registry->identity,
                (string) RedisRegistry::SCHEMA_VERSION,
                $settings['kind'],
                $settings['fingerprint'],
                (string) $settings['capacity'],
                (string) $settings['a'],
                (string) $settings['b'],
                (string) $cost,
            ],
            $effect,
        );

        return ScriptReply::strings($reply, 2, 4);
    }

    /**
     * @param list<string> $items
     * @return array{0: int, 1: Duration|null} available tokens and the wait (null when it does not fit the exact range)
     */
    private static function rejection(array $items): array
    {
        $available = ScriptReply::integer($items[1]);
        $micros = ScriptReply::integer($items[2]);
        if ($items[3] === '1') {
            return [$available, null];
        }
        if ($items[3] !== '0' || $micros < 1) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        return [$available, Duration::nanoseconds(RedisTime::microsecondsToNanoseconds($micros))];
    }

    /**
     * The script arguments of a policy. Durations become whole microseconds, rounded up; the fingerprint keeps the
     * configured nanoseconds, so any change of a setting is a different binding.
     *
     * @return array{kind: string, fingerprint: string, capacity: int, a: int, b: int}
     * @throws InvalidConfigurationException
     */
    private static function settings(RateLimitPolicy $policy): array
    {
        $options = $policy->algorithm;

        return match (true) {
            $options instanceof TokenBucketOptions => [
                'kind' => 'tb',
                'fingerprint' => 'token_bucket:' . $options->capacityUnits->value . ':' . $options->refillUnits->value . ':' . $options->refillInterval->nanoseconds,
                'capacity' => $options->capacityUnits->value,
                'a' => $options->refillUnits->value,
                'b' => RedisTime::nanosecondsToMicroseconds($options->refillInterval->nanoseconds),
            ],
            $options instanceof FixedWindowOptions => [
                'kind' => $options->anchor === FixedWindowAnchor::Aligned ? 'fw' : 'fh',
                'fingerprint' => 'fixed_window:' . $options->capacityUnits->value . ':' . $options->window->nanoseconds . ':' . $options->anchor->value,
                'capacity' => $options->capacityUnits->value,
                'a' => RedisTime::nanosecondsToMicroseconds($options->window->nanoseconds),
                'b' => 0,
            ],
            $options instanceof SlidingCounterOptions => [
                'kind' => 'sc',
                'fingerprint' => 'sliding_counter:' . $options->capacityUnits->value . ':' . $options->window->nanoseconds,
                'capacity' => $options->capacityUnits->value,
                'a' => RedisTime::nanosecondsToMicroseconds($options->window->nanoseconds),
                'b' => 0,
            ],
            default => throw new InvalidConfigurationException('Unsupported rate algorithm options implementation.'),
        };
    }
}
