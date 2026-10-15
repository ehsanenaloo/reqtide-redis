<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Outcome\OutcomeClassifier;
use Enaxon\Reqtide\Pipeline\CompiledPipeline;
use Enaxon\Reqtide\Pipeline\PipelineDefinition;
use Enaxon\Reqtide\Pipeline\PipelineExplanation;
use Enaxon\Reqtide\Pipeline\RuntimeEnvironment;
use Enaxon\Reqtide\Runtime\SchedulingCapability;
use Enaxon\Reqtide\Runtime\SynchronousRuntimeAdapter;
use Enaxon\Reqtide\Telemetry\EventSink;
use Enaxon\Reqtide\Telemetry\NullEventSink;
use Enaxon\Reqtide\Time\BoundedBlockingScheduler;
use Enaxon\Reqtide\Time\SecureRandomSource;
use Enaxon\Reqtide\Time\SystemClock;

/**
 * The shared-state counterpart of `InMemoryEnvironment`: a `RuntimeEnvironment` whose rate limit, circuit breaker,
 * bulkhead, retry budget and cooldown live in one Redis registry, for exactly the policies the definition configures.
 *
 *     $builder = PipelineBuilder::profile('billing', 'v1')->partition('shop', 'prod', 'billing-api') ...;
 *     $pipeline = $builder->compile(RedisEnvironment::for($builder->definition(), $classifier, $connection, $registry));
 *
 * Creating the environment performs no I/O: no connection is opened and nothing is read or written until the first
 * operation. The registry must have been created by the host with {@see RedisRegistryAdmin::initialize()}; a missing one
 * rejects every protected call and is never created here. The clock of all decisions is the Redis server's.
 *
 * Every operation runs one backend call per policy step. A failure of the store is a Core infrastructure exception and is
 * never answered with fresh state; see {@see ServerPolicyReport} for the one server setting that can still lose state.
 */
final class RedisEnvironment
{
    private function __construct() {}

    public static function for(
        PipelineDefinition $definition,
        OutcomeClassifier $classifier,
        RedisConnectionOptions $connection,
        RedisRegistry $registry,
        ?EventSink $events = null,
    ): RuntimeEnvironment {
        $clock = new SystemClock();

        // No I/O: the identities are checked against the key layout now, so a name that Core accepts but a Redis key cannot
        // hold fails when the environment is built and not at the first call.
        foreach ([$definition->rateLimit, $definition->circuit, $definition->concurrency, $definition->retryBudget, $definition->cooldown] as $policy) {
            if ($policy !== null) {
                $registry->keys()->assertIdentity($policy->identity);
            }
        }

        return new RuntimeEnvironment(
            $clock,
            new BoundedBlockingScheduler($clock),
            new SecureRandomSource(),
            $classifier,
            new SynchronousRuntimeAdapter(SchedulingCapability::BoundedBlocking),
            $events ?? new NullEventSink(),
            $definition->rateLimit === null ? null : new RedisRateLimitBackend($connection, $registry),
            $definition->circuit === null ? null : new RedisCircuitBackend($connection, $registry),
            $definition->concurrency === null ? null : new RedisConcurrencyBackend($connection, $registry),
            $definition->retryBudget === null ? null : new RedisRetryBudgetBackend($connection, $registry),
            $definition->cooldown === null ? null : new RedisCooldownBackend($connection, $registry),
        );
    }

    /**
     * The explanation of a compiled pipeline plus the facts of its shared state, without any I/O: the state scope, the
     * read and write timeout of the connection, the registry identity and schema, its ceilings and the TLS setting. No
     * host, credential, key prefix or secret appears.
     */
    public static function explain(CompiledPipeline $pipeline, RedisConnectionOptions $connection, RedisRegistry $registry): PipelineExplanation
    {
        $base = $pipeline->explain();

        return new PipelineExplanation(
            $base->profileId,
            $base->profileVersion,
            $base->enabledPolicies,
            [
                ...$base->capabilityNotes,
                'state_scope=shared_authority',
                'state_clock=redis_server',
                'state_io_timeout_seconds=' . rtrim(rtrim(sprintf('%.3F', $connection->readWriteTimeoutSeconds), '0'), '.'),
                'state_tls=' . ($connection->tls ? 'true' : 'false'),
                'registry_identity=' . $registry->identity,
                'registry_schema=' . RedisRegistry::SCHEMA_VERSION,
                'registry_max_partitions=' . $registry->maxPartitions,
                'registry_max_records=' . $registry->maxRecords,
            ],
        );
    }
}
