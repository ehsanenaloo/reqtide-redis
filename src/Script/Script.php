<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Script;

use Enaxon\Reqtide\Exception\InvalidConfigurationException;

/**
 * A Lua script that ships with the library: fixed text and its SHA-1 digest. User data never becomes part of the text;
 * it travels in KEYS and ARGV.
 *
 * @internal
 */
final readonly class Script
{
    public string $digest;

    public function __construct(public string $text)
    {
        if ($text === '') {
            throw new InvalidConfigurationException('A Redis script must not be empty.');
        }
        $this->digest = sha1($text);
    }
}
