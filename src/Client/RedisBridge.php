<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Client;

use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\IndeterminateStateException;
use Enaxon\Reqtide\Exception\InfrastructureException;
use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Exception\StateCapacityExceededException;
use Enaxon\Reqtide\Exception\StateUnavailableException;
use Enaxon\Reqtide\Redis\RedisConnectionOptions;
use Enaxon\Reqtide\Redis\Script\Script;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;

/**
 * Runs the library's scripts on one Redis connection and turns every outcome into a typed result:
 *
 *  - a failure while connecting happened before any command of the library was written: unavailable;
 *  - a server error that rejects the command before it runs (NOSCRIPT after the single load, OOM, BUSY, LOADING,
 *    READONLY, NOPERM, MISCONF, NOAUTH, WRONGPASS): definitely not applied, unavailable;
 *  - a registered script error code (`RT_...`): definitely not applied by design, mapped to its typed exception;
 *  - WRONGTYPE: a key under the registry prefix holds a value of the wrong type, which is damaged state (the scripts read
 *    before they write the decision of the call), mapped like a damaged field; a refusal of a redirect (MOVED, ASK,
 *    CLUSTERDOWN) and a refusal to accept the connection (maximum number of clients) are definite rejections;
 *  - a transport failure after the command was handed to the connection (reset, timeout, truncated or missing reply),
 *    an unknown server error, or any other throwable: the command may have been applied, so a mutating command is
 *    indeterminate and a read-only command is unavailable.
 *
 * A command is sent at most once, except the documented single `EVALSHA` after a positive `NOSCRIPT` (which proves the
 * script did not run). A transport failure is never answered with a second send. Exception messages are fixed text; the
 * original throwable is kept as the previous exception and never copied into the message.
 *
 * @internal
 */
final class RedisBridge
{
    /** Script error codes (the first word of the error reply) and what they mean. */
    private const SCRIPT_ERROR_CODES = [
        'RT_CORRUPT' => CorruptStateException::class,
        'RT_IDENTITY' => CorruptStateException::class,
        'RT_NO_REGISTRY' => StateUnavailableException::class,
        'RT_ACL' => StateUnavailableException::class,
        'RT_CAPACITY' => StateCapacityExceededException::class,
        'RT_CONFIG' => InvalidConfigurationException::class,
        'RT_EXISTS' => InvalidConfigurationException::class,
        'RT_LEFTOVERS' => InvalidConfigurationException::class,
        'RT_VERSION' => InvalidConfigurationException::class,
        'WRONGTYPE' => CorruptStateException::class,
    ];

    /** Fixed text for the codes whose cause the caller can act on; the other codes get the generic text. */
    private const SCRIPT_ERROR_MESSAGES = [
        'RT_EXISTS' => 'The registry still exists; retire it before removing its leftovers.',
        'RT_LEFTOVERS' => 'Keys of a retired registry still exist under this key prefix; remove them with purgeRetired() before initializing.',
        'RT_VERSION' => 'The Redis server is older than the minimum of 7.0.',
    ];

    /** The scripts reach keys they derive themselves, which Redis Cluster does not allow. */
    private const CLUSTER_MESSAGE = 'The Redis server runs in Cluster mode, which is not supported: the scripts use keys that are not passed in KEYS.';

    /** First Redis version the scripts run on (redis.acl_check_cmd). */
    private const MINIMUM_MAJOR_VERSION = 7;

    /** Server errors that reject a command before it runs. */
    private const DEFINITE_REJECTIONS = ['OOM', 'BUSY', 'LOADING', 'READONLY', 'NOPERM', 'MISCONF', 'NOAUTH', 'WRONGPASS', 'TRYAGAIN', 'MASTERDOWN', 'MOVED', 'ASK', 'CLUSTERDOWN'];

    public function __construct(private readonly RedisPort $port, private readonly BridgeCapabilities $capabilities) {}

    public static function forOptions(RedisConnectionOptions $options): self
    {
        return new self(new PredisPort($options), new BridgeCapabilities($options->readWriteTimeoutSeconds));
    }

    public function capabilities(): BridgeCapabilities
    {
        return $this->capabilities;
    }

    /**
     * @param list<string> $keys
     * @param list<string> $arguments
     * @throws InfrastructureException
     * @throws InvalidConfigurationException
     */
    public function execute(Script $script, array $keys, array $arguments, CommandEffect $effect): mixed
    {
        try {
            $this->port->connect();
        } catch (\Throwable $failure) {
            throw new StateUnavailableException('The Redis state backend is unavailable (connect failed).', 0, $failure);
        }

        $reply = $this->send(['EVALSHA', $script->digest, (string) count($keys), ...$keys, ...$arguments], $effect);
        if ($reply->isError && self::errorCode($reply->value) === 'NOSCRIPT') {
            $this->load($script);
            $reply = $this->send(['EVALSHA', $script->digest, (string) count($keys), ...$keys, ...$arguments], $effect);
            if ($reply->isError && self::errorCode($reply->value) === 'NOSCRIPT') {
                throw new StateUnavailableException('The Redis state backend is unavailable (script could not be loaded).');
            }
        }

        if ($reply->isError) {
            throw $this->fromServerError($reply->value, $effect);
        }

        return $reply->value;
    }

    /**
     * Reads one server setting with CONFIG GET. Returns null when the server refuses the command (an execution user
     * normally has no right to it) or does not know the parameter.
     *
     * @throws StateUnavailableException when the server cannot be reached or the reply is cut
     */
    public function serverSetting(string $parameter): ?string
    {
        try {
            $this->port->connect();
            $reply = $this->port->execute(['CONFIG', 'GET', $parameter]);
        } catch (\Throwable $failure) {
            throw new StateUnavailableException('The Redis state backend is unavailable (read failed).', 0, $failure);
        }
        if ($reply->isError || !is_array($reply->value)) {
            return null;
        }
        $items = array_values($reply->value);
        if (count($items) === 2 && is_string($items[0]) && $items[0] === $parameter && is_string($items[1])) {
            return $items[1];
        }

        return null;
    }

    /**
     * Checks that the server is a Redis the scripts run on (7.0 or newer, which has `redis.acl_check_cmd`, and not in
     * Cluster mode), with INFO. A
     * server that refuses INFO or does not say its version is not refused here: the scripts check for themselves and fail
     * before their first command on a server that is too old. Writes nothing.
     *
     * @throws InvalidConfigurationException when the server reports a version below the minimum or Cluster mode
     * @throws StateUnavailableException when the server cannot be reached or the reply is cut
     */
    public function requireSupportedServer(): void
    {
        try {
            $this->port->connect();
            $reply = $this->port->execute(['INFO', 'server']);
        } catch (\Throwable $failure) {
            throw new StateUnavailableException('The Redis state backend is unavailable (read failed).', 0, $failure);
        }
        if ($reply->isError || !is_string($reply->value)) {
            return;
        }
        if (preg_match('~^redis_mode:cluster\r?$~m', $reply->value) === 1) {
            throw new InvalidConfigurationException(self::CLUSTER_MESSAGE);
        }
        if (preg_match('~^redis_version:([0-9]+)\.~m', $reply->value, $match) !== 1) {
            return;
        }
        if ((int) $match[1] < self::MINIMUM_MAJOR_VERSION) {
            throw new InvalidConfigurationException(self::SCRIPT_ERROR_MESSAGES['RT_VERSION']);
        }
    }

    /**
     * One bounded step of removing the keys a retired registry left behind: reads one page of keys named like
     * `{prefix}:i`, `{prefix}:t` and `{prefix}:p:...` and unlinks them in one script that first checks that the metadata
     * key does not exist, so a registry initialized again between the scan and the removal loses nothing. The metadata key
     * is never touched. Idempotent; a lost reply is indeterminate and the step can simply be repeated.
     *
     * The page comes from SCAN on one server and the script names its keys outright, so this needs a standalone server (as
     * the scripts do, which also reach keys they derive themselves); Redis Cluster is not supported.
     *
     * @return array{0: string, 1: int} the cursor for the next step ("0" when the scan is complete) and the number of keys unlinked
     * @throws InfrastructureException
     * @throws InvalidConfigurationException when the registry exists
     */
    public function purgeStep(string $metadataKey, string $prefix, string $cursor): array
    {
        try {
            $this->port->connect();
        } catch (\Throwable $failure) {
            throw new StateUnavailableException('The Redis state backend is unavailable (connect failed).', 0, $failure);
        }
        if (preg_match('~^(0|[1-9][0-9]{0,19})\z~', $cursor) !== 1) {
            throw new InvalidConfigurationException('The purge cursor is not valid.');
        }
        $page = $this->send(['SCAN', $cursor, 'MATCH', $prefix . ':*', 'COUNT', '500'], CommandEffect::ReadOnly);
        $items = is_array($page->value) ? array_values($page->value) : [];
        if ($page->isError || count($items) !== 2 || !is_string($items[0]) || !is_array($items[1])) {
            throw new StateUnavailableException('The Redis state backend is unavailable (read failed).');
        }
        $owned = [];
        $head = $prefix . ':';
        foreach ($items[1] as $key) {
            if (is_string($key) && ($key === $head . 'i' || $key === $head . 't' || str_starts_with($key, $head . 'p:'))) {
                $owned[$key] = $key;
            }
        }
        // The script runs for an empty page too: it is what refuses a registry that exists.
        $removed = $this->execute(ScriptLibrary::registryPurge(), [$metadataKey, ...array_values($owned)], [], CommandEffect::Mutating);
        if (!is_int($removed)) {
            throw new CorruptStateException('The purge reply has an unexpected shape.');
        }

        return [$items[0], $removed];
    }

    /** @param list<string> $command */
    private function send(array $command, CommandEffect $effect): PortReply
    {
        try {
            return $this->port->execute($command);
        } catch (\Throwable $failure) {
            throw self::uncertain($effect, $failure);
        }
    }

    private function load(Script $script): void
    {
        try {
            $reply = $this->port->execute(['SCRIPT', 'LOAD', $script->text]);
        } catch (\Throwable $failure) {
            // Loading a script changes no Reqtide state, so this failure is never indeterminate.
            throw new StateUnavailableException('The Redis state backend is unavailable (script load failed).', 0, $failure);
        }
        if ($reply->isError || $reply->value !== $script->digest) {
            throw new StateUnavailableException('The Redis state backend is unavailable (script load was refused).');
        }
    }

    private static function uncertain(CommandEffect $effect, \Throwable $failure): InfrastructureException
    {
        return $effect === CommandEffect::Mutating
            ? new IndeterminateStateException('The outcome of a Redis state command is indeterminate.', 0, $failure)
            : new StateUnavailableException('The Redis state backend is unavailable (read failed).', 0, $failure);
    }

    private function fromServerError(mixed $value, CommandEffect $effect): \Throwable
    {
        $code = self::errorCode($value);
        // An ACL denial of a command inside a script. Definite only because every script validates (reads the clock and
        // checks its input) before its first write; the fault tests must prove that for each script.
        if (is_string($value) && str_contains($value, 'ACL failure in script')) {
            return new StateUnavailableException('The Redis state backend is unavailable (acl).');
        }
        // The server refused to accept the connection, so no command of the library reached it.
        if (is_string($value) && str_starts_with($value, 'ERR max number of clients reached')) {
            return new StateUnavailableException('The Redis state backend is unavailable (max clients).');
        }
        if (isset(self::SCRIPT_ERROR_CODES[$code])) {
            $class = self::SCRIPT_ERROR_CODES[$code];

            return new $class(self::SCRIPT_ERROR_MESSAGES[$code] ?? 'The Redis state backend rejected the command (' . strtolower($code) . ').');
        }
        if (in_array($code, self::DEFINITE_REJECTIONS, true)) {
            return new StateUnavailableException('The Redis state backend is unavailable (' . strtolower($code) . ').');
        }

        // An error reply this bridge does not know: conservative, as for any other unknown failure.
        return self::uncertain($effect, new \RuntimeException('Unrecognized Redis error reply.'));
    }

    /** The first word of an error reply, which is its code (for example NOSCRIPT, OOM, RT_CAPACITY). */
    private static function errorCode(mixed $value): string
    {
        if (!is_string($value)) {
            return '';
        }
        $space = strpos($value, ' ');

        return $space === false ? $value : substr($value, 0, $space);
    }
}
