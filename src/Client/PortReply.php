<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Client;

/**
 * One server reply. When $isError is true, $value is the server's error text (for example "NOSCRIPT ...").
 *
 * @internal
 */
final readonly class PortReply
{
    public function __construct(public bool $isError, public mixed $value) {}
}
