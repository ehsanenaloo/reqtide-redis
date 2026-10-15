<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Exception\InvalidConfigurationException;

/**
 * Immutable options for one direct Redis connection (standalone, RESP2).
 *
 * Both timeouts are required and finite: a bridge that can wait forever would turn a Redis stall into a stalled worker.
 * There is deliberately no retry option, no persistent-connection option and no way to turn certificate or host name
 * verification off: a command is never sent twice by the library, and a TLS connection is always verified. The password
 * is hidden from `var_dump` and refuses serialization; it is never part of an exception message.
 */
final readonly class RedisConnectionOptions
{
    private const MAX_TIMEOUT_SECONDS = 3600.0;

    /**
     * @param string $host host name or address; no scheme, no port, no whitespace
     * @param float $connectTimeoutSeconds finite, greater than zero and at most one hour
     * @param float $readWriteTimeoutSeconds finite, greater than zero and at most one hour
     * @param bool $tls connect with TLS; peer and host name are always verified
     * @param ?string $caFile CA bundle file for TLS; null uses the system trust store
     * @param ?string $tlsServerName name to verify instead of the host (for connections by address)
     */
    public function __construct(
        public string $host,
        public float $connectTimeoutSeconds,
        public float $readWriteTimeoutSeconds,
        public int $port = 6379,
        public ?string $username = null,
        #[\SensitiveParameter]
        private ?string $password = null,
        public bool $tls = false,
        public ?string $caFile = null,
        public ?string $tlsServerName = null,
    ) {
        if ($host === '' || preg_match('~[\s/\\\\]|://~', $host) === 1) {
            throw new InvalidConfigurationException('The Redis host must be a non-empty host name or address without scheme, port or whitespace.');
        }
        if ($port < 1 || $port > 65535) {
            throw new InvalidConfigurationException('The Redis port must be between 1 and 65535.');
        }
        foreach ([$connectTimeoutSeconds, $readWriteTimeoutSeconds] as $timeout) {
            if (!is_finite($timeout) || $timeout <= 0.0 || $timeout > self::MAX_TIMEOUT_SECONDS) {
                throw new InvalidConfigurationException('Redis timeouts must be finite, greater than zero and at most 3600 seconds.');
            }
        }
        if ($username !== null && $password === null) {
            throw new InvalidConfigurationException('A Redis user name requires a password.');
        }
        if (!$tls && ($caFile !== null || $tlsServerName !== null)) {
            throw new InvalidConfigurationException('TLS settings were given for a connection without TLS.');
        }
        if ($caFile !== null && ($caFile === '' || !is_file($caFile))) {
            throw new InvalidConfigurationException('The TLS CA file does not exist.');
        }
        if ($tlsServerName !== null && $tlsServerName === '') {
            throw new InvalidConfigurationException('The TLS server name must not be empty.');
        }
    }

    /**
     * The only parameters ever given to the Predis connection. There is no `retry` key (Predis then makes no retry),
     * no persistence, and TLS verification is always on.
     *
     * @internal
     * @return array<string, mixed>
     */
    public function predisParameters(): array
    {
        $parameters = [
            'scheme' => $this->tls ? 'tls' : 'tcp',
            'host' => $this->host,
            'port' => $this->port,
            'timeout' => $this->connectTimeoutSeconds,
            'read_write_timeout' => $this->readWriteTimeoutSeconds,
            'persistent' => false,
        ];
        if ($this->username !== null) {
            $parameters['username'] = $this->username;
        }
        if ($this->password !== null) {
            $parameters['password'] = $this->password;
        }
        if ($this->tls) {
            $ssl = ['verify_peer' => true, 'verify_peer_name' => true];
            if ($this->caFile !== null) {
                $ssl['cafile'] = $this->caFile;
            }
            if ($this->tlsServerName !== null) {
                $ssl['peer_name'] = $this->tlsServerName;
            }
            $parameters['ssl'] = $ssl;
        }

        return $parameters;
    }

    /** @return array<string, mixed> */
    public function __debugInfo(): array
    {
        return [
            'host' => $this->host,
            'port' => $this->port,
            'username' => $this->username,
            'password' => $this->password === null ? null : '***',
            'tls' => $this->tls,
            'connectTimeoutSeconds' => $this->connectTimeoutSeconds,
            'readWriteTimeoutSeconds' => $this->readWriteTimeoutSeconds,
        ];
    }

    /** @return never */
    public function __serialize(): array
    {
        throw new \LogicException('Redis connection options carry a credential and are not serializable.');
    }
}
