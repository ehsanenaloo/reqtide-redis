<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\State;

use Enaxon\Reqtide\Exception\CorruptStateException;

/**
 * Strict reading of a script reply: a flat list of decimal or token strings of a known length. Anything else is a
 * corrupt-state error with a fixed message (the reply is never copied into it).
 *
 * @internal
 */
final class ScriptReply
{
    private function __construct() {}

    /**
     * @param int<1, max> $minimum
     * @param int<1, max> $maximum
     * @return list<string>
     * @throws CorruptStateException
     */
    public static function strings(mixed $reply, int $minimum, int $maximum): array
    {
        if (!is_array($reply) || !array_is_list($reply) || count($reply) < $minimum || count($reply) > $maximum) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }
        foreach ($reply as $item) {
            if (!is_string($item)) {
                throw new CorruptStateException('A Redis state reply has an unexpected shape.');
            }
        }

        return $reply;
    }

    /** @throws CorruptStateException */
    public static function integer(string $text): int
    {
        if (preg_match('~^(0|[1-9][0-9]{0,15})\z~', $text) !== 1) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        return (int) $text;
    }
}
