<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis;

use Enaxon\Reqtide\Exception\CorruptStateException;
use Enaxon\Reqtide\Exception\InfrastructureException;
use Enaxon\Reqtide\Redis\Client\CommandEffect;
use Enaxon\Reqtide\Redis\Client\RedisBridge;
use Enaxon\Reqtide\Redis\Script\ScriptLibrary;
use Enaxon\Reqtide\Redis\State\ScriptReply;
use Enaxon\Reqtide\Redis\State\StateKind;
use Enaxon\Reqtide\Value\Generation;
use Enaxon\Reqtide\Value\PolicyIdentity;

/**
 * Operator actions on circuit breaker partitions, run with the host's own connection. A reset is never called by the
 * library: the host decides, and every permit issued before it becomes stale.
 */
final class RedisCircuitAdmin
{
    private readonly RedisBridge $bridge;

    public function __construct(RedisConnectionOptions $connection, private readonly RedisRegistry $registry, ?RedisBridge $bridge = null)
    {
        $this->bridge = $bridge ?? RedisBridge::forOptions($connection);
    }

    /**
     * Closes the circuit, discards its health history and every active permit, and advances the generation. The
     * incarnation and the terminal records stay. A lost reply is an indeterminate result; the reset is never repeated.
     *
     * @phpstan-impure
     * @return Generation|null the new generation, or null when the partition does not exist
     * @throws InfrastructureException
     */
    public function reset(PolicyIdentity $identity): ?Generation
    {
        $layout = $this->registry->keys();
        $partition = $layout->policyPartition(StateKind::Circuit, $identity);
        $reply = $this->bridge->execute(
            ScriptLibrary::circuitReset(),
            [$layout->metadata(), $partition, $partition . ':a', $partition . ':w', $layout->rateIndex()],
            [$this->registry->identity, (string) RedisRegistry::SCHEMA_VERSION],
            CommandEffect::Mutating,
        );
        $items = ScriptReply::strings($reply, 1, 2);
        if ($items[0] === '0' && count($items) === 1) {
            return null;
        }
        if ($items[0] !== '1' || count($items) !== 2) {
            throw new CorruptStateException('A Redis state reply has an unexpected shape.');
        }

        return new Generation(ScriptReply::integer($items[1]));
    }
}
