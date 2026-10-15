<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Client;

use Enaxon\Reqtide\State\StateScope;

/**
 * What the Redis backends built on this bridge declare to Core: shared authority, a finite I/O timeout and a server
 * clock with one-microsecond resolution.
 *
 * @internal
 */
final readonly class BridgeCapabilities
{
    public const CLOCK_RESOLUTION_MICROSECONDS = 1;

    public StateScope $scope;

    public function __construct(public float $ioTimeoutSeconds)
    {
        $this->scope = StateScope::SharedAuthority;
    }

    public function clockResolutionMicroseconds(): int
    {
        return self::CLOCK_RESOLUTION_MICROSECONDS;
    }
}
