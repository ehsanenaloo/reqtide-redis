<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\IndeterminateStateException;
use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Exception\StateCapacityExceededException;
use Enaxon\Reqtide\Exception\StateUnavailableException;
use Enaxon\Reqtide\Policy\ConcurrencyPolicy;
use Enaxon\Reqtide\Redis\Client\CommandEffect;
use Enaxon\Reqtide\Redis\Client\RedisBridge;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;
use Enaxon\Reqtide\Redis\State\ScriptReply;
use Enaxon\Reqtide\Redis\State\StateKind;
use Enaxon\Reqtide\Redis\Time\RedisTime;
use Enaxon\Reqtide\State\ConcurrencyAdmission;
use Enaxon\Reqtide\State\ConcurrencyAdmissionStatus;
use Enaxon\Reqtide\State\ConcurrencyBackend;
use Enaxon\Reqtide\State\ConcurrencyPermit;
use Enaxon\Reqtide\State\FinalizationResult;
use Enaxon\Reqtide\State\FinalizationStatus;
use Enaxon\Reqtide\State\StateBackendCapabilities;
use Enaxon\Reqtide\Value\BackendClockKind;
use Enaxon\Reqtide\Value\BackendExpiry;
use Enaxon\Reqtide\Value\BackendInstant;
use Enaxon\Reqtide\Value\CommandId;
use Enaxon\Reqtide\Value\Duration;
use Enaxon\Reqtide\Value\Generation;
use Enaxon\Reqtide\Value\IncarnationId;
use Enaxon\Reqtide\Value\PermitOwnerId;

/**
 * Shared concurrency leases on Redis. One atomic script call per operation.
 *
 * A lease is valid on `[acquiredAt, expiresAt)` of the server clock and expired at equality; the permit carries the exact
 * backend expiry and a release acts only on a matching active owner (partition, incarnation, generation, owner and exact
 * expiry). Releasing converts the slot of the owner into a 120-second tombstone in place, so a repeat answers
 * `AlreadyFinalized` while the tombstone lives and `StaleOwnership` afterwards, and can never free a slot twice. A
 * partition that holds nothing is deleted; the next acquire creates a new incarnation.
 *
 * Capacity pressure first reclaims expired leases (of the partition and of other partitions) and expired tombstones, then
 * evicts the oldest tombstones registry-wide, and only then rejects new allocation with
 * {@see StateCapacityExceededException}. Active owners are never evicted. {@see RedisRegistryMaintenance} does the same
 * reclamation without an allocation.
 *
 * `CommandId` is accepted for contract symmetry and is not stored: the library never sends a mutation twice, and a
 * repeated acquire with the same owner is rejected as a duplicate owner identity. A lost reply is
 * {@see IndeterminateStateException}; the lease then ends by expiry.
 */
final class RedisConcurrencyBackend implements ConcurrencyBackend
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
    public function acquire(ConcurrencyPolicy $policy, PermitOwnerId $owner, CommandId $command): ConcurrencyAdmission
    {
        $reply = $this->bridge->execute(
            ScriptLibrary::concurrencyAcquire(),
            $this->keys($policy),
            [
                $this->registry->identity,
                (string) RedisRegistry::SCHEMA_VERSION,
                (string) $policy->capacity->value,
                (string) RedisTime::nanosecondsToMicroseconds($policy->lease->nanoseconds),
                $owner->lowerHex,
                bin2hex(random_bytes(16)),
            ],
            CommandEffect::Mutating,
        );

        $items = ScriptReply::strings($reply, 1, 5);
        if ($items === ['0']) {
            return new ConcurrencyAdmission(ConcurrencyAdmissionStatus::CapacityExhausted, null);
        }
        if (count($items) !== 5 || $items[0] !== '1') {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }
        try {
            $permit = new ConcurrencyPermit(
                $policy->identity,
                IncarnationId::fromLowerHex($items[1]),
                new Generation(ScriptReply::integer($items[2])),
                $owner,
                $policy->lease,
                BackendExpiry::shared(new BackendInstant(ScriptReply::integer($items[3]), ScriptReply::integer($items[4]))),
            );
        } catch (InvalidConfigurationException) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        return new ConcurrencyAdmission(ConcurrencyAdmissionStatus::Accepted, $permit);
    }

    /**
     * @throws StateUnavailableException|CorruptStateException|IndeterminateStateException
     */
    public function release(ConcurrencyPolicy $policy, ConcurrencyPermit $permit, CommandId $command): FinalizationResult
    {
        $instant = $permit->expiresAt->sharedInstant;
        if (!$permit->policy->equals($policy->identity) || $permit->expiresAt->clockKind !== BackendClockKind::SharedAuthority || $instant === null) {
            // A permit of another policy or of a process-local backend cannot own anything here.
            return new FinalizationResult(FinalizationStatus::StaleOwnership);
        }

        $reply = $this->bridge->execute(
            ScriptLibrary::concurrencyRelease(),
            $this->keys($policy),
            [
                $this->registry->identity,
                (string) RedisRegistry::SCHEMA_VERSION,
                (string) $policy->capacity->value,
                (string) RedisTime::nanosecondsToMicroseconds($policy->lease->nanoseconds),
                $permit->owner->lowerHex,
                $permit->incarnation->lowerHex,
                (string) $permit->generation->value,
                (string) $instant->seconds,
                (string) $instant->microseconds,
            ],
            CommandEffect::Mutating,
        );

        [$status] = ScriptReply::strings($reply, 1, 1);

        return new FinalizationResult(match ($status) {
            '1' => FinalizationStatus::Applied,
            '2' => FinalizationStatus::AlreadyFinalized,
            '3' => FinalizationStatus::StaleOwnership,
            default => throw new CorruptStateException('A Redis state reply has an unexpected shape.'),
        });
    }

    /** @return list<string> */
    private function keys(ConcurrencyPolicy $policy): array
    {
        $layout = $this->registry->keys();
        $partition = $layout->policyPartition(StateKind::Concurrency, $policy->identity);

        return [$layout->metadata(), $partition, $partition . ':a', $layout->tombstoneQueue(), $layout->rateIndex()];
    }
}
