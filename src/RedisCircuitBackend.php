<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Error\ReasonCode;
use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\IndeterminateStateException;
use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Exception\StateCapacityExceededException;
use Enaxon\Reqtide\Exception\StateUnavailableException;
use Enaxon\Reqtide\Outcome\CircuitVerdict;
use Enaxon\Reqtide\Policy\CircuitPolicy;
use Enaxon\Reqtide\Policy\CountCircuitWindow;
use Enaxon\Reqtide\Policy\TimeCircuitWindow;
use Enaxon\Reqtide\Redis\Client\CommandEffect;
use Enaxon\Reqtide\Redis\Client\RedisBridge;
use Enaxon\Reqtide\Redis\Script\Script;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;
use Enaxon\Reqtide\Redis\State\ScriptReply;
use Enaxon\Reqtide\Redis\State\StateKind;
use Enaxon\Reqtide\Redis\Time\RedisTime;
use Enaxon\Reqtide\State\CircuitAdmission;
use Enaxon\Reqtide\State\CircuitAdmissionStatus;
use Enaxon\Reqtide\State\CircuitBackend;
use Enaxon\Reqtide\State\CircuitPermit;
use Enaxon\Reqtide\State\CircuitPermitKind;
use Enaxon\Reqtide\State\CircuitTransition;
use Enaxon\Reqtide\State\FinalizationResult;
use Enaxon\Reqtide\State\FinalizationStatus;
use Enaxon\Reqtide\State\StateBackendCapabilities;
use Enaxon\Reqtide\Value\AttemptId;
use Enaxon\Reqtide\Value\BackendClockKind;
use Enaxon\Reqtide\Value\BackendExpiry;
use Enaxon\Reqtide\Value\BackendInstant;
use Enaxon\Reqtide\Value\CommandId;
use Enaxon\Reqtide\Value\Duration;
use Enaxon\Reqtide\Value\Generation;
use Enaxon\Reqtide\Value\IncarnationId;
use Enaxon\Reqtide\Value\PermitOwnerId;

/**
 * Shared circuit breaker on Redis. One atomic script call per operation;
 * the server clock is the only clock.
 *
 * Closed, Open and HalfOpen follow the memory backend exactly. The window is the exact last N completed non-neutral
 * outcomes, or whole time buckets that intersect the window (bucket ids on the epoch of the server clock). The failure
 * ratio is compared with exact integer arithmetic. Every transition increments the generation, makes the permits of the
 * earlier generation stale and is reported as one fact of the call. Open and HalfOpen state, active permits and count
 * history are never reclaimed or evicted; terminal records are, oldest first, under record pressure. A permit expires
 * neutrally at its lease end (no sample, no recovery credit). A lost reply is {@see IndeterminateStateException}; nothing
 * replays it.
 *
 * The open duration, the permit lease, the time window and the bucket granularity are rounded up to whole microseconds,
 * a wait to the end of the open period is exact in microseconds. The settings stay bound to the policy identity in
 * nanoseconds. `CommandId` is accepted for contract symmetry and is not stored. The manual reset is an operator action
 * and is not part of this class; see {@see RedisCircuitAdmin}.
 */
final class RedisCircuitBackend implements CircuitBackend
{
    private const STATES = ['c' => CircuitTransition::CLOSED, 'o' => CircuitTransition::OPEN, 'h' => CircuitTransition::HALF_OPEN];

    private const REASONS = [
        'T' => ReasonCode::CircuitThresholdExceeded,
        'P' => ReasonCode::CircuitProbeFailed,
        'S' => ReasonCode::CircuitProbesSucceeded,
        'E' => ReasonCode::CircuitOpenElapsed,
    ];

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
    public function acquire(CircuitPolicy $policy, AttemptId $attempt, PermitOwnerId $owner, CommandId $command): CircuitAdmission
    {
        $items = ScriptReply::strings(
            $this->run(ScriptLibrary::circuitAcquire(), $policy, [$attempt->lowerHex, $owner->lowerHex, bin2hex(random_bytes(16))]),
            9,
            9,
        );
        $transition = self::transition($items[6], $items[7], $items[8]);

        if ($items[0] === '2') {
            $wait = ScriptReply::integer($items[1]);
            if ($wait < 1) {
                throw new CorruptStateException('A Redis state reply has an unexpected shape.');
            }

            return new CircuitAdmission(CircuitAdmissionStatus::Open, null, Duration::nanoseconds(RedisTime::microsecondsToNanoseconds($wait)), $transition);
        }
        if ($items[0] === '3') {
            return new CircuitAdmission(CircuitAdmissionStatus::ProbeCapacityExhausted, null, null, $transition);
        }
        if ($items[0] !== '1') {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        $kind = match ($items[5]) {
            'c' => CircuitPermitKind::ClosedSample,
            'p' => CircuitPermitKind::HalfOpenProbe,
            default => throw new CorruptStateException('A Redis state reply has an unexpected shape.'),
        };
        try {
            $permit = new CircuitPermit(
                $policy->identity,
                IncarnationId::fromLowerHex($items[1]),
                new Generation(ScriptReply::integer($items[2])),
                $attempt,
                $owner,
                $kind,
                $policy->permitLease,
                BackendExpiry::shared(new BackendInstant(ScriptReply::integer($items[3]), ScriptReply::integer($items[4]))),
            );
        } catch (InvalidConfigurationException) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        return new CircuitAdmission(CircuitAdmissionStatus::Accepted, $permit, null, $transition);
    }

    /**
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException|StateCapacityExceededException
     */
    public function record(CircuitPolicy $policy, CircuitPermit $permit, CircuitVerdict $verdict, CommandId $command): FinalizationResult
    {
        return $this->finalize($policy, $permit, 'record', match ($verdict) {
            CircuitVerdict::Success => 's',
            CircuitVerdict::Failure => 'f',
            CircuitVerdict::Neutral => 'n',
        });
    }

    /**
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException
     */
    public function abandon(CircuitPolicy $policy, CircuitPermit $permit, CommandId $command): FinalizationResult
    {
        return $this->finalize($policy, $permit, 'abandon', 'n');
    }

    private function finalize(CircuitPolicy $policy, CircuitPermit $permit, string $mode, string $verdict): FinalizationResult
    {
        $instant = $permit->expiresAt->sharedInstant;
        if (!$permit->policy->equals($policy->identity) || $permit->expiresAt->clockKind !== BackendClockKind::SharedAuthority || $instant === null) {
            // A permit of another policy or of a process-local backend cannot own anything here.
            return new FinalizationResult(FinalizationStatus::StaleOwnership);
        }

        $items = ScriptReply::strings(
            $this->run(ScriptLibrary::circuitFinalize(), $policy, [
                $mode,
                $verdict,
                $permit->attempt->lowerHex,
                $permit->owner->lowerHex,
                $permit->incarnation->lowerHex,
                (string) $permit->generation->value,
                (string) $instant->seconds,
                (string) $instant->microseconds,
                $permit->kind === CircuitPermitKind::ClosedSample ? 'c' : 'p',
            ]),
            4,
            4,
        );

        return new FinalizationResult(
            match ($items[0]) {
                '1' => FinalizationStatus::Applied,
                '2' => FinalizationStatus::AlreadyFinalized,
                '3' => FinalizationStatus::StaleOwnership,
                default => throw new CorruptStateException('A Redis state reply has an unexpected shape.'),
            },
            self::transition($items[1], $items[2], $items[3]),
        );
    }

    /**
     * @param list<string> $arguments the arguments after the settings (and their fingerprint)
     */
    private function run(Script $script, CircuitPolicy $policy, array $arguments): mixed
    {
        $layout = $this->registry->keys();
        $partition = $layout->policyPartition(StateKind::Circuit, $policy->identity);
        [$kind, $window, $granularity] = self::window($policy);

        return $this->bridge->execute(
            $script,
            [$layout->metadata(), $partition, $partition . ':a', $layout->tombstoneQueue(), $layout->rateIndex(), $partition . ':w'],
            [
                $this->registry->identity,
                (string) RedisRegistry::SCHEMA_VERSION,
                $kind,
                (string) $window,
                (string) $granularity,
                (string) $policy->minimumSamples->value,
                (string) $policy->failureThreshold->numerator->value,
                (string) $policy->failureThreshold->denominator->value,
                (string) RedisTime::nanosecondsToMicroseconds($policy->openDuration->nanoseconds),
                (string) $policy->probeCapacity->value,
                (string) $policy->probeSuccessThreshold->value,
                (string) RedisTime::nanosecondsToMicroseconds($policy->permitLease->nanoseconds),
                self::fingerprint($policy),
                ...$arguments,
            ],
            CommandEffect::Mutating,
        );
    }

    /**
     * @return array{0: string, 1: int, 2: int} window kind, samples or microseconds, bucket granularity in microseconds
     */
    private static function window(CircuitPolicy $policy): array
    {
        $window = $policy->window;
        if ($window instanceof CountCircuitWindow) {
            return ['c', $window->completedSamples->value, 0];
        }
        if ($window instanceof TimeCircuitWindow) {
            return ['t', RedisTime::nanosecondsToMicroseconds($window->window->nanoseconds), RedisTime::nanosecondsToMicroseconds($window->bucketGranularity->nanoseconds)];
        }

        throw new InvalidConfigurationException('Unsupported circuit window options.');
    }

    private static function fingerprint(CircuitPolicy $policy): string
    {
        $window = $policy->window;
        $shape = match (true) {
            $window instanceof CountCircuitWindow => 'c:' . $window->completedSamples->value,
            $window instanceof TimeCircuitWindow => 't:' . $window->window->nanoseconds . ':' . $window->bucketGranularity->nanoseconds,
            default => throw new InvalidConfigurationException('Unsupported circuit window options.'),
        };

        return implode('|', [
            $shape,
            $policy->minimumSamples->value,
            $policy->failureThreshold->numerator->value,
            $policy->failureThreshold->denominator->value,
            $policy->openDuration->nanoseconds,
            $policy->probeCapacity->value,
            $policy->probeSuccessThreshold->value,
            $policy->permitLease->nanoseconds,
        ]);
    }

    private static function transition(string $from, string $to, string $reason): ?CircuitTransition
    {
        if ($from === '' && $to === '' && $reason === '') {
            return null;
        }
        if (!isset(self::STATES[$from], self::STATES[$to], self::REASONS[$reason])) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        try {
            return new CircuitTransition(self::STATES[$from], self::STATES[$to], self::REASONS[$reason]->value);
        } catch (InvalidConfigurationException) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }
    }
}
