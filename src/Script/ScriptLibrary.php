<?php

declare(strict_types=1);

namespace Enaxon\Reqtide\Redis\Script;

use Enaxon\Reqtide\Exception\InvalidConfigurationException;

/**
 * Assembles the shipped Lua resources into scripts: the shared libraries in a fixed order, the libraries of the state
 * kind, then the script body. The text is read from `resources/lua` once per process and never contains user data.
 *
 * @internal
 */
final class ScriptLibrary
{
    /** Libraries every script gets, in order. */
    private const LIBRARIES = ['lib/exact_int.lua', 'lib/time.lua', 'lib/text.lua', 'lib/registry.lua', 'lib/state.lua'];

    /**
     * The libraries of the scripts that share the registry index of rate limits and retry budgets: the budget and the
     * rate libraries are loaded together because reclamation of idle partitions handles both.
     */
    public const INDEXED = ['lib/concurrency.lua', 'lib/budget.lua', 'lib/circuit.lua', 'lib/cooldown.lua', 'lib/rate.lua'];

    /** @var array<string, Script> */
    private static array $cache = [];

    private function __construct() {}

    public static function registryInit(): Script
    {
        return self::compose('scripts/registry_init.lua');
    }

    public static function registryVerify(): Script
    {
        return self::compose('scripts/registry_verify.lua');
    }

    public static function registryRetire(): Script
    {
        return self::compose('scripts/registry_retire.lua');
    }

    public static function registryPurge(): Script
    {
        return self::compose('scripts/registry_purge.lua');
    }

    public static function cooldownInspect(): Script
    {
        return self::compose('scripts/cooldown_inspect.lua', ['lib/cooldown.lua']);
    }

    public static function cooldownExtend(): Script
    {
        return self::compose('scripts/cooldown_extend.lua', self::INDEXED);
    }

    public static function concurrencyAcquire(): Script
    {
        return self::compose('scripts/concurrency_acquire.lua', self::INDEXED);
    }

    public static function concurrencyRelease(): Script
    {
        return self::compose('scripts/concurrency_release.lua', ['lib/concurrency.lua']);
    }

    public static function rateConsume(): Script
    {
        return self::compose('scripts/rate_consume.lua', self::INDEXED);
    }

    public static function rateInspect(): Script
    {
        return self::compose('scripts/rate_inspect.lua', self::INDEXED);
    }

    public static function budgetReserve(): Script
    {
        return self::compose('scripts/budget_reserve.lua', self::INDEXED);
    }

    public static function budgetFinalize(): Script
    {
        return self::compose('scripts/budget_finalize.lua', self::INDEXED);
    }

    public static function budgetInspect(): Script
    {
        return self::compose('scripts/budget_inspect.lua', self::INDEXED);
    }

    public static function circuitAcquire(): Script
    {
        return self::compose('scripts/circuit_acquire.lua', self::INDEXED);
    }

    public static function circuitFinalize(): Script
    {
        return self::compose('scripts/circuit_finalize.lua', self::INDEXED);
    }

    public static function circuitReset(): Script
    {
        return self::compose('scripts/circuit_reset.lua', self::INDEXED);
    }

    public static function maintain(): Script
    {
        return self::compose('scripts/maintain.lua', self::INDEXED);
    }

    /**
     * The shared libraries and the libraries of a state kind as one text, without a script body (for test drivers that
     * place their own body after the production libraries).
     *
     * @param list<string> $libraries
     */
    public static function libraryText(array $libraries = []): string
    {
        $parts = [];
        foreach ([...self::LIBRARIES, ...$libraries] as $relative) {
            $path = dirname(__DIR__, 2) . '/resources/lua/' . $relative;
            $text = is_file($path) ? file_get_contents($path) : false;
            if ($text === false) {
                throw new InvalidConfigurationException('A shipped Lua resource is missing: ' . $relative);
            }
            $parts[] = $text;
        }

        return implode("\n", $parts);
    }

    /**
     * @param string $body path below resources/lua
     * @param list<string> $libraries libraries of the state kind, placed after the shared ones
     */
    public static function compose(string $body, array $libraries = []): Script
    {
        if (isset(self::$cache[$body])) {
            return self::$cache[$body];
        }

        $parts = [];
        foreach ([...self::LIBRARIES, ...$libraries, $body] as $relative) {
            $path = dirname(__DIR__, 2) . '/resources/lua/' . $relative;
            $text = is_file($path) ? file_get_contents($path) : false;
            if ($text === false) {
                throw new InvalidConfigurationException('A shipped Lua resource is missing: ' . $relative);
            }
            $parts[] = $text;
        }

        return self::$cache[$body] = new Script(implode("\n", $parts));
    }
}
