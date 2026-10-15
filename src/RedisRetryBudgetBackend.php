<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\IndeterminateStateException;
use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Exception\StateCapacityExceededException;
use Enaxon\Reqtide\Exception\StateUnavailableException;
use Enaxon\Reqtide\Policy\RetryBudgetPolicy;
use Enaxon\Reqtide\Redis\Client\CommandEffect;
use Enaxon\Reqtide\Redis\Client\RedisBridge;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;
use Enaxon\Reqtide\Redis\State\ScriptReply;
use Enaxon\Reqtide\Redis\State\StateKind;
use Enaxon\Reqtide\Redis\Time\RedisTime;
use Enaxon\Reqtide\State\RetryBudgetBackend;
use Enaxon\Reqtide\State\RetryBudgetFinalizationResult;
use Enaxon\Reqtide\State\RetryBudgetFinalizationStatus;
use Enaxon\Reqtide\State\RetryBudgetInspection;
use Enaxon\Reqtide\State\RetryBudgetReservation;
use Enaxon\Reqtide\State\RetryReservationDecision;
use Enaxon\Reqtide\State\RetryReservationStatus;
use Enaxon\Reqtide\State\StateBackendCapabilities;
use Enaxon\Reqtide\Value\AttemptId;
use Enaxon\Reqtide\Value\BackendClockKind;
use Enaxon\Reqtide\Value\BackendExpiry;
use Enaxon\Reqtide\Value\BackendInstant;
use Enaxon\Reqtide\Value\CommandId;
use Enaxon\Reqtide\Value\Duration;
use Enaxon\Reqtide\Value\Generation;
use Enaxon\Reqtide\Value\IncarnationId;
use Enaxon\Reqtide\Value\NonNegativeInt;
use Enaxon\Reqtide\Value\PermitOwnerId;

/**
 * Shared retry budget with pending reservations on Redis. One atomic script call per
 * operation; the server clock is the only clock.
 *
 * A token bucket scaled by 1,000,000 where `balance + pending <= capacity`. Before every reserve, commit, release or
 * expiry the elapsed refill is accrued with the old pending cost, then every reservation whose lease has been reached
 * becomes an Expired record and refunds only its own cost, so a held reservation never mints a token. A commit refunds
 * nothing (only later refill restores the headroom); a release refunds the cost capped at the new headroom. Only the
 * first transition of a pending reservation authorizes a dispatch; a repeat answers with the stored disposition while
 * the 120-second record is retained and as stale ownership afterwards, and never debits or refunds twice.
 *
 * The refill interval and the reservation lease are rounded up to whole microseconds (the lease is therefore never
 * shorter, the refill never faster); the settings stay bound to the policy identity in nanoseconds. A lost reply is
 * {@see IndeterminateStateException}; nothing replays it, and an unfinished reservation ends by lease expiry.
 * `CommandId` is accepted for contract symmetry and is not stored. `inspect` is advisory and mutation-free.
 *
 * Pending reservations and balances are never evicted. Under record-ceiling pressure expired and then the oldest terminal
 * records are removed before new allocation is rejected with {@see StateCapacityExceededException}.
 */
final class RedisRetryBudgetBackend implements RetryBudgetBackend
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
    public function inspect(RetryBudgetPolicy $policy): RetryBudgetInspection
    {
        $reply = $this->bridge->execute(
            ScriptLibrary::budgetInspect(),
            $this->keys($policy, false),
            [
                $this->registry->identity,
                (string) RedisRegistry::SCHEMA_VERSION,
                (string) $policy->capacityTokens->value,
                (string) $policy->refillTokens->value,
                (string) RedisTime::nanosecondsToMicroseconds($policy->refillInterval->nanoseconds),
                self::fingerprint($policy),
            ],
            CommandEffect::ReadOnly,
        );

        [$flag, $available, $wait] = ScriptReply::strings($reply, 3, 3);
        $tokens = new NonNegativeInt(ScriptReply::integer($available));
        if ($flag === '1' && $wait === '') {
            return new RetryBudgetInspection($tokens, true, null);
        }
        if ($flag === '0') {
            return new RetryBudgetInspection($tokens, false, $wait === '' ? null : self::wait($wait));
        }

        throw new CorruptStateException('A Redis state reply has an unexpected shape.');
    }

    /**
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException|StateCapacityExceededException
     */
    public function reserve(RetryBudgetPolicy $policy, AttemptId $attempt, PermitOwnerId $owner, CommandId $command): RetryReservationDecision
    {
        $reply = $this->bridge->execute(
            ScriptLibrary::budgetReserve(),
            $this->keys($policy, true),
            [
                $this->registry->identity,
                (string) RedisRegistry::SCHEMA_VERSION,
                (string) $policy->capacityTokens->value,
                (string) $policy->refillTokens->value,
                (string) RedisTime::nanosecondsToMicroseconds($policy->refillInterval->nanoseconds),
                (string) RedisTime::nanosecondsToMicroseconds($policy->reservationLease->nanoseconds),
                self::fingerprint($policy),
                $attempt->lowerHex,
                $owner->lowerHex,
                bin2hex(random_bytes(16)),
            ],
            CommandEffect::Mutating,
        );

        $items = ScriptReply::strings($reply, 2, 5);
        if ($items[0] === '0' && count($items) === 2) {
            return new RetryReservationDecision(RetryReservationStatus::Exhausted, null, $items[1] === '' ? null : self::wait($items[1]));
        }
        if ($items[0] !== '1' || count($items) !== 5) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }
        try {
            $reservation = new RetryBudgetReservation(
                $policy->identity,
                IncarnationId::fromLowerHex($items[1]),
                new Generation(ScriptReply::integer($items[2])),
                $attempt,
                $owner,
                $policy->reservationLease,
                BackendExpiry::shared(new BackendInstant(ScriptReply::integer($items[3]), ScriptReply::integer($items[4]))),
            );
        } catch (InvalidConfigurationException) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        return new RetryReservationDecision(RetryReservationStatus::Reserved, $reservation, null);
    }

    /**
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException
     */
    public function commit(RetryBudgetPolicy $policy, RetryBudgetReservation $reservation, CommandId $command): RetryBudgetFinalizationResult
    {
        return $this->finalize($policy, $reservation, 'commit');
    }

    /**
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException
     */
    public function release(RetryBudgetPolicy $policy, RetryBudgetReservation $reservation, CommandId $command): RetryBudgetFinalizationResult
    {
        return $this->finalize($policy, $reservation, 'release');
    }

    private function finalize(RetryBudgetPolicy $policy, RetryBudgetReservation $reservation, string $mode): RetryBudgetFinalizationResult
    {
        $instant = $reservation->expiresAt->sharedInstant;
        if (!$reservation->policy->equals($policy->identity) || $reservation->expiresAt->clockKind !== BackendClockKind::SharedAuthority || $instant === null) {
            // A reservation of another policy or of a process-local backend cannot own anything here.
            return new RetryBudgetFinalizationResult(RetryBudgetFinalizationStatus::StaleOwnership);
        }

        $reply = $this->bridge->execute(
            ScriptLibrary::budgetFinalize(),
            $this->keys($policy, true),
            [
                $this->registry->identity,
                (string) RedisRegistry::SCHEMA_VERSION,
                (string) $policy->capacityTokens->value,
                (string) $policy->refillTokens->value,
                (string) RedisTime::nanosecondsToMicroseconds($policy->refillInterval->nanoseconds),
                self::fingerprint($policy),
                $mode,
                $reservation->attempt->lowerHex,
                $reservation->owner->lowerHex,
                $reservation->incarnation->lowerHex,
                (string) $reservation->generation->value,
                (string) $instant->seconds,
                (string) $instant->microseconds,
            ],
            CommandEffect::Mutating,
        );

        [$code] = ScriptReply::strings($reply, 1, 1);

        return new RetryBudgetFinalizationResult(match ($code) {
            '1' => RetryBudgetFinalizationStatus::Committed,
            '2' => RetryBudgetFinalizationStatus::Released,
            '3' => RetryBudgetFinalizationStatus::AlreadyCommitted,
            '4' => RetryBudgetFinalizationStatus::AlreadyReleased,
            '5' => RetryBudgetFinalizationStatus::Expired,
            '6' => RetryBudgetFinalizationStatus::StaleOwnership,
            default => throw new CorruptStateException('A Redis state reply has an unexpected shape.'),
        });
    }

    /** @return list<string> */
    private function keys(RetryBudgetPolicy $policy, bool $withQueue): array
    {
        $layout = $this->registry->keys();
        $partition = $layout->policyPartition(StateKind::RetryBudget, $policy->identity);

        return $withQueue
            ? [$layout->metadata(), $partition, $partition . ':a', $layout->tombstoneQueue(), $layout->rateIndex()]
            : [$layout->metadata(), $partition, $partition . ':a'];
    }

    private static function wait(string $micros): Duration
    {
        $value = ScriptReply::integer($micros);
        if ($value < 1) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        return Duration::nanoseconds(RedisTime::microsecondsToNanoseconds($value));
    }

    private static function fingerprint(RetryBudgetPolicy $policy): string
    {
        return $policy->capacityTokens->value . '|' . $policy->refillTokens->value . '|'
            . $policy->refillInterval->nanoseconds . '|' . $policy->reservationLease->nanoseconds;
    }
}
