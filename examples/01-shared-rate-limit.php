<?php

declare(strict_types=1);

/**
 * 01 - One rate limit shared through Redis.
 *
 * Two pipelines are built separately, each with its own Redis connection, the way two PHP processes would build them.
 * They share one registry, so they draw from one token bucket: three tokens in total, whoever asks. The fourth call is
 * refused and its callable never runs.
 *
 * Both pipelines live in this one script to keep the example short. The tests of the package run the same thing with
 * separate operating-system processes.
 *
 * Setup that the application never does: the host creates the registry once (RedisRegistryAdmin::initialize()). The
 * registry here uses a throwaway key prefix and is removed at the end.
 *
 * Run:  tools/run-redis-examples.sh
 */

namespace Reqtide\RedisExamples;

require __DIR__ . '/support.php';

use Enaxon\Reqtide\Error\ProblemCarrier;
use Enaxon\Reqtide\Exception\AdmissionDeferredException;
use Enaxon\Reqtide\Exception\AdmissionRejectedException;
use Enaxon\Reqtide\Execution\AttemptContext;
use Enaxon\Reqtide\Outcome\ExceptionClassifier;
use Enaxon\Reqtide\Pipeline\PipelineBuilder;
use Enaxon\Reqtide\Policy\AttemptTimeoutRequirement;
use Enaxon\Reqtide\Redis\RedisEnvironment;
use Enaxon\Reqtide\Redis\RedisRegistry;
use Enaxon\Reqtide\Redis\RedisRegistryAdmin;
use Enaxon\Reqtide\Value\Duration;

$registry = new RedisRegistry('example-' . bin2hex(random_bytes(6)), 'example-shared-limit', random_bytes(32));
$admin = new RedisRegistryAdmin(connection(), $registry);
$admin->initialize();

$policy = $admin->inspectServer();
check($policy->protectsState(), 'the server must use maxmemory-policy noeviction');

/** What each "process" does: build the same pipeline on the Redis environment. */
$build = static function () use ($registry) {
    $builder = PipelineBuilder::profile('shared-limit', 'v1')
        ->partition('example', 'local', 'reports-api')
        ->attemptTimeout(Duration::seconds(2), AttemptTimeoutRequirement::Cooperative)
        // Three tokens, one refilled per hour: three calls for the life of this script, across both pipelines.
        ->rateLimit(PipelineBuilder::tokenBucket(3, 1, Duration::seconds(3600)));

    return $builder->compile(RedisEnvironment::for($builder->definition(), ExceptionClassifier::create(), connection(), $registry));
};

$served = ['A' => 0, 'B' => 0];
$callable = static function (string $who) use (&$served): \Closure {
    return static function (AttemptContext $attempt) use ($who, &$served): string {
        ++$served[$who];

        return 'served';
    };
};

$pipelines = ['A' => $build(), 'B' => $build()];
$sequence = ['A', 'B', 'A', 'B', 'A'];
foreach ($sequence as $i => $who) {
    $pipeline = $pipelines[$who];
    $number = $i + 1;
    try {
        $pipeline->execute($callable($who), $pipeline->newContext(Duration::seconds(30), operationId: 'shared-limit-' . $number));
        say("call {$number} via {$who}: served");
    } catch (AdmissionDeferredException|AdmissionRejectedException $e) {
        check($e instanceof ProblemCarrier, 'a refusal carries a Problem');
        say("call {$number} via {$who}: refused ({$e->problem()->code->value})");
    }
}
say("served by A: {$served['A']}, served by B: {$served['B']}");
check($served['A'] + $served['B'] === 3, 'exactly three calls were served across both pipelines');

$admin->retire();
$admin->purgeRetired();
say('ok');
