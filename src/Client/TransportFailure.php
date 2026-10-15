<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Client;

/**
 * A failure of the Redis client library, stripped of its object graph. The Predis exception that caused it is not kept: its
 * trace records the arguments of the frames it passed, and during authentication those include the command object that
 * carries the user name and the password. This exception keeps the class, the message and the place of the original.
 *
 * Create it as `new TransportFailure(TransportFailure::describe($failure))`: a factory method that takes the original as an
 * argument would put the original into the trace arguments of the new exception.
 *
 * @internal
 */
final class TransportFailure extends \RuntimeException
{
    public static function describe(\Throwable $failure): string
    {
        return sprintf('%s: %s (%s:%d)', $failure::class, $failure->getMessage(), basename($failure->getFile()), $failure->getLine());
    }
}
