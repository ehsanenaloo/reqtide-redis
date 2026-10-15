<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Redis\State\KeyLayout;

/**
 * Immutable identity of one registry, the isolation unit of shared state: key prefix, expected identity, ceilings and
 * the host-managed secret for keyed hashing of tenant and subject dimensions.
 *
 * The registry itself exists in Redis only after the host initializes it with RedisRegistryAdmin; this object never
 * creates anything. The secret must be at least 32 bytes, stay the same for the life of the registry (changing it
 * changes every hashed key) and must not be a value that is also used as a credential elsewhere.
 */
final readonly class RedisRegistry
{
    public const SCHEMA_VERSION = 1;

    public const DEFAULT_MAX_PARTITIONS = 10_000;

    public const DEFAULT_MAX_RECORDS = 20_000;

    private const MAX_CEILING = 1_000_000;

    private KeyLayout $keys;

    /**
     * @param string $keyPrefix 1 to 64 letters, digits, dots, underscores or hyphens
     * @param string $identity token stored in the registry; a different stored identity is refused (guards against two deployments sharing a prefix)
     * @param int $maxPartitions ceiling of allocated protective partitions
     * @param int $maxRecords ceiling of retained owner, reservation and tombstone records
     */
    public function __construct(
        public string $keyPrefix,
        public string $identity,
        #[\SensitiveParameter]
        private string $secret,
        public int $maxPartitions = self::DEFAULT_MAX_PARTITIONS,
        public int $maxRecords = self::DEFAULT_MAX_RECORDS,
    ) {
        if (preg_match('~^[A-Za-z0-9_.-]{1,64}\z~', $identity) !== 1) {
            throw new InvalidConfigurationException('The registry identity must be 1 to 64 letters, digits, dots, underscores or hyphens.');
        }
        foreach ([$maxPartitions, $maxRecords] as $ceiling) {
            if ($ceiling < 1 || $ceiling > self::MAX_CEILING) {
                throw new InvalidConfigurationException('Registry ceilings must be between 1 and 1000000.');
            }
        }
        $this->keys = new KeyLayout($keyPrefix, $secret);
    }

    /** @internal */
    public function keys(): KeyLayout
    {
        return $this->keys;
    }

    /** @return array<string, mixed> */
    public function __debugInfo(): array
    {
        return ['keyPrefix' => $this->keyPrefix, 'identity' => $this->identity, 'secret' => '***', 'maxPartitions' => $this->maxPartitions, 'maxRecords' => $this->maxRecords];
    }

    /** @return never */
    public function __serialize(): array
    {
        throw new \LogicException('A registry carries a secret and is not serializable.');
    }
}
