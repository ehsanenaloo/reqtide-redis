<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Client;

/**
 * Whether a command can change state. It decides how an uncertain failure is reported: a mutating command that may have
 * been applied is indeterminate, a read-only one is merely unavailable.
 *
 * @internal
 */
enum CommandEffect
{
    case Mutating;
    case ReadOnly;
}
