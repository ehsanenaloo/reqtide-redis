<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Client;

/**
 * The narrow seam between the bridge and a Redis client library. An implementation never retries, never reconnects
 * behind the caller's back for a command it already started to write, and reports a server error reply as data.
 * Anything it throws is a transport failure.
 *
 * @internal
 */
interface RedisPort
{
    /** Establishes the connection (idempotent). A failure here happened before any command of the library was written. */
    public function connect(): void;

    /**
     * Sends one command and reads its reply.
     *
     * @param list<string> $arguments command name first
     */
    public function execute(array $arguments): PortReply;
}
