<?php

declare(strict_types=1);

/**
 * 03 - explain() for a pipeline on shared state, without any network traffic.
 *
 * RedisEnvironment::explain() adds the facts of the shared state to the usual explanation: where the state lives, whose
 * clock decides, the I/O timeout, TLS, and the registry identity and ceilings. It opens no connection (this script
 * points at a port where nothing listens) and prints no host, password, key prefix or secret.
 *
 * Run:  tools/run-redis-examples.sh
 */

namespace Reqtide\RedisExamples;

require __DIR__ . '/support.php';

use Enaxon\Reqtide\Outcome\ExceptionClassifier;
use Enaxon\Reqtide\Pipeline\PipelineBuilder;
use Enaxon\Reqtide\Policy\AttemptTimeoutRequirement;
use Enaxon\Reqtide\Redis\RedisConnectionOptions;
use Enaxon\Reqtide\Redis\RedisEnvironment;
use Enaxon\Reqtide\Redis\RedisRegistry;
use Enaxon\Reqtide\Value\Duration;

$connection = new RedisConnectionOptions('127.0.0.1', 0.5, 1.5, 1, 'app', 'not-a-real-password');
$registry = new RedisRegistry('example-explain', 'example-explain', random_bytes(32), 500, 900);

$builder = PipelineBuilder::profile('explain-shared', 'v1')
    ->partition('example', 'local', 'reports-api')
    ->attemptTimeout(Duration::seconds(2), AttemptTimeoutRequirement::Cooperative)
    ->rateLimit(PipelineBuilder::tokenBucket(10, 10, Duration::seconds(1)))
    ->bulkhead(3, Duration::seconds(10));
$pipeline = $builder->compile(RedisEnvironment::for($builder->definition(), ExceptionClassifier::create(), $connection, $registry));

$explanation = RedisEnvironment::explain($pipeline, $connection, $registry);
say("profile: {$explanation->profileId->value} {$explanation->profileVersion->value}");
say('enabled policies: ' . implode(', ', $explanation->enabledPolicies));
$text = '';
foreach ($explanation->capabilityNotes as $note) {
    say("  {$note}");
    $text .= $note . "\n";
}
check(str_contains($text, 'state_scope=shared_authority'), 'the state scope is shared');
check(!str_contains($text, 'not-a-real-password') && !str_contains($text, 'example-explain-secret'), 'no secret in the explanation');

say('ok');
