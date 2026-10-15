<?php

declare(strict_types=1);

/**
 * 02 - Failure demo: Redis is not reachable.
 *
 * The shared state cannot be read, so Reqtide cannot decide whether the call is allowed. It does not guess and it does
 * not use fresh local state in its place: the callable is not run and the caller gets a typed exception. This script
 * needs no server; it connects to a port where nothing listens.
 *
 * Take from this:
 *  - A state failure is an infrastructure exception (StateUnavailableException), not a refusal by a policy.
 *  - The callable ran 0 times. Whether to run the work without protection is a decision for your code, made here in the
 *    catch block and not made for you.
 *  - Nothing was written: the failure happened before any command was sent.
 *
 * Run:  tools/run-redis-examples.sh
 */

namespace Reqtide\RedisExamples;

require __DIR__ . '/support.php';

use Enaxon\Reqtide\Completion\CompletionDisposition;
use Enaxon\Reqtide\Exception\StateUnavailableException;
use Enaxon\Reqtide\Execution\AttemptContext;
use Enaxon\Reqtide\Outcome\ExceptionClassifier;
use Enaxon\Reqtide\Pipeline\PipelineBuilder;
use Enaxon\Reqtide\Policy\AttemptTimeoutRequirement;
use Enaxon\Reqtide\Redis\RedisConnectionOptions;
use Enaxon\Reqtide\Redis\RedisEnvironment;
use Enaxon\Reqtide\Redis\RedisRegistry;
use Enaxon\Reqtide\Value\Duration;

$unreachable = new RedisConnectionOptions('127.0.0.1', 0.5, 0.5, 1);
$registry = new RedisRegistry('example-down', 'example-down', random_bytes(32));

$builder = PipelineBuilder::profile('redis-down', 'v1')
    ->partition('example', 'local', 'reports-api')
    ->attemptTimeout(Duration::seconds(2), AttemptTimeoutRequirement::Cooperative)
    ->rateLimit(PipelineBuilder::tokenBucket(3, 1, Duration::seconds(3600)));
$pipeline = $builder->compile(RedisEnvironment::for($builder->definition(), ExceptionClassifier::create(), $unreachable, $registry));

$invocations = 0;
$work = static function (AttemptContext $attempt) use (&$invocations): string {
    ++$invocations;

    return 'done';
};

try {
    $pipeline->execute($work, $pipeline->newContext(Duration::seconds(30), operationId: 'redis-down-1'));
    check(false, 'a call with no reachable state must not succeed');
} catch (StateUnavailableException $e) {
    say('execute(): ' . $e::class);
}
say("callable invocations: {$invocations}");
check($invocations === 0, 'the callable was not run');

$completion = $pipeline->executeDetailed($work, $pipeline->newContext(Duration::seconds(30), operationId: 'redis-down-2'));
say('detailed disposition: ' . $completion->disposition->value);
check($completion->disposition !== CompletionDisposition::Succeeded, 'detailed mode does not report success');
check($invocations === 0, 'still not run');

say('ok');
