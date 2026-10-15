<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\State;

/**
 * The five kinds of shared state; the value is the short, fixed key segment.
 *
 * @internal
 */
enum StateKind: string
{
    case RateLimit = 'rl';
    case Circuit = 'cb';
    case Concurrency = 'cc';
    case RetryBudget = 'rb';
    case Cooldown = 'cd';
}
