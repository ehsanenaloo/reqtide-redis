<?php

declare(strict_types=1);

/**
 * Shared plumbing for the Redis examples. Not part of the Reqtide API.
 *
 * The server comes from the environment: REDIS_HOST (default redis74) and REDIS_PORT (default 6379). The examples run
 * against a plain local server without a password. In your project the connection options carry your host, user,
 * password and TLS settings.
 */

namespace Reqtide\RedisExamples;

require __DIR__ . '/autoload.php';

use Enaxon\Reqtide\Redis\RedisConnectionOptions;

function say(string $line = ''): void
{
    echo $line, "\n";
}

function check(bool $condition, string $what): void
{
    if (!$condition) {
        fwrite(STDERR, "CHECK FAILED: {$what}\n");
        exit(1);
    }
}

function connection(float $readWriteSeconds = 2.0): RedisConnectionOptions
{
    $host = getenv('REDIS_HOST');
    $port = getenv('REDIS_PORT');

    return new RedisConnectionOptions(
        is_string($host) && $host !== '' ? $host : 'redis74',
        1.0,
        $readWriteSeconds,
        is_string($port) && ctype_digit($port) ? (int) $port : 6379,
    );
}
