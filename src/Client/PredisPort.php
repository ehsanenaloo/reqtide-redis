<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Client;

use Enaxon\Reqtide\Redis\RedisConnectionOptions;
use Predis\Client;

/**
 * The Predis implementation of the port. Commands go through `executeRaw`, which bypasses the Predis retry wrapper
 * entirely, and the connection parameters come only from RedisConnectionOptions (which has no retry key). A failure of
 * Predis is rethrown as {@see TransportFailure}, which carries no trace arguments of the connection set-up.
 *
 * @internal
 */
final class PredisPort implements RedisPort
{
    private readonly Client $client;

    public function __construct(RedisConnectionOptions $options)
    {
        $this->client = new Client($options->predisParameters());
    }

    public function connect(): void
    {
        try {
            $this->quietly(fn() => $this->client->connect());
        } catch (\Throwable $failure) {
            throw new TransportFailure(TransportFailure::describe($failure));
        }
    }

    public function execute(array $arguments): PortReply
    {
        $error = false;
        try {
            $value = $this->quietly(function () use ($arguments, &$error): mixed {
                return $this->client->executeRaw($arguments, $error);
            });
        } catch (\Throwable $failure) {
            throw new TransportFailure(TransportFailure::describe($failure));
        }

        return new PortReply($error === true, $value);
    }

    /**
     * Runs a client call with PHP warnings of the stream layer (a failed TLS handshake, a closed socket) turned off: the
     * failure reaches the caller as the exception of the client, and a warning printed on top of it would only leak
     * details of the connection into the output of the host application.
     *
     * @template T
     * @param \Closure(): T $call
     * @return T
     */
    private function quietly(\Closure $call): mixed
    {
        set_error_handler(static fn(int $level): bool => ($level & (E_WARNING | E_NOTICE)) !== 0);
        try {
            return $call();
        } finally {
            restore_error_handler();
        }
    }
}
