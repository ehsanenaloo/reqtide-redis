<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\InfrastructureException;
use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Redis\Client\CommandEffect;
use Enaxon\Reqtide\Redis\Client\RedisBridge;
use Enaxon\Reqtide\Redis\Script\Script;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;

/**
 * The only way to create, check or retire the stored registry. The host authorizes each call by making it, with its own
 * administrative connection. Execution and inspection never create a missing registry (there is no automatic
 * bootstrap), so total loss of Redis data is reported as an absent registry, never silently re-created.
 *
 * Failures are the Core exceptions of the bridge: an absent registry is `StateUnavailableException`; a different stored
 * identity, a different schema or a damaged field is `CorruptStateException`; ceilings that differ from an existing
 * registry are `InvalidConfigurationException`.
 *
 * Retirement deletes the metadata, including the authority watermark. It does not touch partition or record keys; the
 * returned status shows how many were left behind, and {@see purgeRetired()} removes them.
 */
final class RedisRegistryAdmin
{
    private readonly RedisBridge $bridge;

    public function __construct(RedisConnectionOptions $administrativeConnection, private readonly RedisRegistry $registry)
    {
        $this->bridge = RedisBridge::forOptions($administrativeConnection);
    }

    /**
     * Creates the registry, or confirms an identical existing one (idempotent). It refuses, with nothing written, a
     * server older than Redis 7.0, a server in Cluster mode and a key prefix that still holds the index or the tombstone queue of a retired
     * registry that was not purged (run {@see purgeRetired()} first): adopting them would start the counters at zero over
     * records that are still there. All refusals are `InvalidConfigurationException`.
     */
    public function initialize(): RegistryStatus
    {
        $this->bridge->requireSupportedServer();
        $layout = $this->registry->keys();

        return $this->run(ScriptLibrary::registryInit(), CommandEffect::Mutating, [
            $this->registry->identity,
            (string) RedisRegistry::SCHEMA_VERSION,
            (string) $this->registry->maxPartitions,
            (string) $this->registry->maxRecords,
        ], [$layout->rateIndex(), $layout->tombstoneQueue()]);
    }

    /** Checks presence, identity, schema and the integrity of the watermark and counters; writes nothing. */
    public function verify(): RegistryStatus
    {
        return $this->run(ScriptLibrary::registryVerify(), CommandEffect::ReadOnly, [$this->registry->identity, (string) RedisRegistry::SCHEMA_VERSION]);
    }

    /** Deletes the metadata and the watermark; reports what was retired. */
    public function retire(): RegistryStatus
    {
        return $this->run(ScriptLibrary::registryRetire(), CommandEffect::Mutating, [$this->registry->identity, (string) RedisRegistry::SCHEMA_VERSION]);
    }

    /**
     * Removes the partition, record, queue and index keys that a retired registry left behind, so that the same key
     * prefix can be initialized again without stale state. It refuses to run while the registry exists and never
     * touches the metadata key. It reads the keyspace in pages (SCAN) and stops when the scan is complete; run it again
     * after an interruption. Keys of other registries and unrelated keys are not touched. Each page of keys is removed by
     * a script that checks again, in the same step, that the registry does not exist, so a registry initialized again
     * while the purge runs is never emptied (the call then fails with `InvalidConfigurationException`). The keys are
     * named outright, so Redis Cluster is not supported (the scripts of the package are not either).
     *
     * @return int the number of keys removed
     * @throws InfrastructureException
     * @throws InvalidConfigurationException when the registry still exists
     */
    public function purgeRetired(): int
    {
        $layout = $this->registry->keys();
        $cursor = '0';
        $removed = 0;
        do {
            [$cursor, $count] = $this->bridge->purgeStep($layout->metadata(), $layout->prefix, $cursor);
            $removed += $count;
        } while ($cursor !== '0');

        return $removed;
    }

    /**
     * Reads the eviction policy and the memory limit of the server with the administrative connection. A server whose
     * policy can evict keys can delete protective state without any signal; see {@see ServerPolicyReport}. An ACL that
     * forbids `CONFIG GET` gives a report whose answer is unknown, never a guess. A server older than Redis 7.0 or in Cluster mode is refused.
     *
     * @throws InfrastructureException
     * @throws InvalidConfigurationException when the server is older than Redis 7.0 or in Cluster mode
     */
    public function inspectServer(): ServerPolicyReport
    {
        $this->bridge->requireSupportedServer();
        $policy = $this->bridge->serverSetting('maxmemory-policy');
        $limit = $this->bridge->serverSetting('maxmemory');

        return new ServerPolicyReport(
            $policy !== null && preg_match('~^[a-z-]{1,32}\z~', $policy) === 1 ? $policy : null,
            $limit !== null && preg_match('~^(0|[1-9][0-9]{0,17})\z~', $limit) === 1 ? (int) $limit : null,
        );
    }

    /**
     * @param list<string> $arguments
     * @param list<string> $extraKeys keys after the metadata key
     * @throws InfrastructureException
     */
    private function run(Script $script, CommandEffect $effect, array $arguments, array $extraKeys = []): RegistryStatus
    {
        $reply = $this->bridge->execute($script, [$this->registry->keys()->metadata(), ...$extraKeys], $arguments, $effect);
        if (!is_array($reply) || count($reply) !== 7) {
            throw new CorruptStateException('The registry reply has an unexpected shape.');
        }
        $numbers = [];
        foreach (array_slice(array_values($reply), 1) as $item) {
            if (!is_string($item) || preg_match('~^(0|[1-9][0-9]{0,15})\z~', $item) !== 1) {
                throw new CorruptStateException('The registry reply has an unexpected shape.');
            }
            $numbers[] = (int) $item;
        }

        return new RegistryStatus(array_values($reply)[0] === '1', ...$numbers);
    }
}
