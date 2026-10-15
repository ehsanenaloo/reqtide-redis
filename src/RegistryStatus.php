<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

/**
 * What a registry administration call found or did. The watermark is the authority time: the latest effective backend
 * time any command has used, as a (seconds, microseconds) pair of Redis time.
 */
final readonly class RegistryStatus
{
    public function __construct(
        public bool $created,
        public int $watermarkSeconds,
        public int $watermarkMicroseconds,
        public int $partitions,
        public int $records,
        public int $maxPartitions,
        public int $maxRecords,
    ) {}
}
