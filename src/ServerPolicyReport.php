<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

/**
 * What the server is configured to do when it runs out of memory. Reqtide keeps protective state (quota balances, open
 * circuits, leases) as ordinary keys. With the policy `noeviction` the server refuses writes when it is full and every
 * command ends in a definite failure (`StateUnavailableException`). With any policy that evicts keys, the server may
 * delete a partition or the registry without any signal: the protection of that partition silently starts again from
 * nothing. A missing registry is reported as unavailable; a missing partition is not.
 */
final readonly class ServerPolicyReport
{
    /**
     * @param string|null $evictionPolicy the `maxmemory-policy` value, or null when the server refused to tell
     * @param int|null $maxMemoryBytes the `maxmemory` value in bytes (0 means no limit), or null when unknown
     */
    public function __construct(public ?string $evictionPolicy, public ?int $maxMemoryBytes) {}

    /**
     * true: the server refuses writes instead of deleting keys (policy `noeviction`) or has no memory limit;
     * false: the server may delete state keys under memory pressure;
     * null: the policy could not be read, so nothing is known.
     */
    public function protectsState(): ?bool
    {
        if ($this->evictionPolicy === null) {
            return null;
        }
        if ($this->evictionPolicy === 'noeviction' || $this->maxMemoryBytes === 0) {
            return true;
        }

        return false;
    }
}
