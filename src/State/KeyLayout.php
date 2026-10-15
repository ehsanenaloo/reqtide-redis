<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\State;

use Enaxon\Reqtide\Exception\InvalidConfigurationException;
use Enaxon\Reqtide\Value\PolicyIdentity;

/**
 * Key names. Keys are semi-public: they appear in logs, `MONITOR` output and operator tooling.
 *
 *   {prefix}:m                                                         registry metadata (one hash)
 *   {prefix}:p:{kind}:{application}:{environment}:{policy}:{profile}:{hash}   one partition
 *   {partition}:a                                                      concurrency only: the active-owner index of a partition
 *   {prefix}:t                                                         registry-wide queue of retained tombstones
 *
 * Application, environment, policy id and profile version stay readable so that an operator can tell what a key is for.
 * Tenant and subject are never written: they enter only through a keyed hash (HMAC-SHA-256 with the registry secret,
 * length-prefixed fields so that ("ab","c") and ("a","bc") differ), truncated to 128 bits. Nothing secret is part of a key.
 *
 * @internal
 */
final readonly class KeyLayout
{
    private const TOKEN = '~^[A-Za-z0-9_.-]{1,64}\z~';
    private const MAX_DIMENSION_BYTES = 4096;

    public function __construct(public string $prefix, #[\SensitiveParameter] private string $secret)
    {
        if (preg_match(self::TOKEN, $prefix) !== 1) {
            throw new InvalidConfigurationException('The registry key prefix must be 1 to 64 letters, digits, dots, underscores or hyphens.');
        }
        if (strlen($secret) < 32) {
            throw new InvalidConfigurationException('The registry secret must be at least 32 bytes.');
        }
    }

    public function metadata(): string
    {
        return $this->prefix . ':m';
    }

    /** Registry-wide index of rate-limit partitions for bounded reclamation (one sorted set per registry, see lib/rate.lua). */
    public function rateIndex(): string
    {
        return $this->prefix . ':i';
    }

    /** Registry-wide queue of retained tombstones in creation order (one list per registry, see lib/concurrency.lua). */
    public function tombstoneQueue(): string
    {
        return $this->prefix . ':t';
    }

    /**
     * The partition key of a policy identity. An application-wide partition (no tenant) uses a tenant text that no
     * tenant identifier can have, so it can never equal a real tenant.
     */
    public function policyPartition(StateKind $kind, PolicyIdentity $identity): string
    {
        $partition = $identity->partition;

        return $this->partition(
            $kind,
            $partition->application->value,
            $partition->environment->value,
            $identity->policy->value,
            $identity->profileVersion->value,
            $partition->tenant->value ?? "\x1e",
            $partition->subject->value,
        );
    }

    /**
     * Checks the identity of a policy against what a key can hold, without building a key: application, environment,
     * policy id and profile version of 1 to 64 characters. Core allows 128, so a longer one is refused here, when an
     * environment is built, and not at the first call.
     *
     * @throws InvalidConfigurationException
     */
    public function assertIdentity(PolicyIdentity $identity): void
    {
        $this->partition(StateKind::Concurrency, $identity->partition->application->value, $identity->partition->environment->value, $identity->policy->value, $identity->profileVersion->value, '', '');
    }

    public function partition(
        StateKind $kind,
        string $application,
        string $environment,
        string $policyId,
        string $profileVersion,
        string $tenant,
        string $subject,
    ): string {
        foreach ([$application, $environment, $policyId, $profileVersion] as $token) {
            if (preg_match(self::TOKEN, $token) !== 1) {
                throw new InvalidConfigurationException('Application, environment, policy id and profile version must be 1 to 64 letters, digits, dots, underscores or hyphens; Redis keys hold at most 64 characters of each, Core allows 128.');
            }
        }
        if (strlen($tenant) > self::MAX_DIMENSION_BYTES || strlen($subject) > self::MAX_DIMENSION_BYTES) {
            throw new InvalidConfigurationException('Tenant and subject must be at most 4096 bytes.');
        }

        $input = 'reqtide-key-v1' . pack('N', strlen($tenant)) . $tenant . pack('N', strlen($subject)) . $subject;
        $hash = substr(hash_hmac('sha256', $input, $this->secret), 0, 32);

        return implode(':', [$this->prefix, 'p', $kind->value, $application, $environment, $policyId, $profileVersion, $hash]);
    }

    /** @return array<string, mixed> */
    public function __debugInfo(): array
    {
        return ['prefix' => $this->prefix, 'secret' => '***'];
    }

    /** @return never */
    public function __serialize(): array
    {
        throw new \LogicException('A key layout carries a secret and is not serializable.');
    }
}
