# Reqtide Redis

**Shared state for Reqtide, so several PHP processes obey one rate limit, one circuit breaker and one bulkhead.**

This package is the Redis state backend of [Reqtide PHP](https://github.com/ehsanenaloo/reqtide). Install it next to the core: `composer require enaxon/reqtide enaxon/reqtide-redis`. It is a beta (version 0.x): the API can change between minor versions. It was tested on the command line with PHP 8.3 to 8.5, Redis 7.4.11 and 8.2.10 and Predis 3.7, including TLS, and in one PHP-FPM lane. Redis 7.0 is the minimum. Sentinel, Cluster and Valkey are not supported.

## What exists today

| Item | State |
| --- | --- |
| Rate limit (token bucket, fixed window, sliding counter), circuit breaker, bulkhead, retry budget, cooldown on Redis | Works. One server-side script call for each step |
| `RedisEnvironment`: a ready `RuntimeEnvironment` for a pipeline | Works |
| Redis 7.4.11 and 8.2.10, Predis 3.7, PHP 8.3 to 8.5 command line | Tested locally in Docker, including TLS |
| Redis older than 7.0 | Not supported: the scripts use `redis.acl_check_cmd`, which Redis 7.0 added. `initialize()` and `inspectServer()` refuse an older server, and every script fails before its first command on one |
| Redis Cluster | Not supported by design; `initialize()` and `inspectServer()` refuse a server in Cluster mode. The scripts also touch keys they derive themselves (members of the registry index and entries of the tombstone queue), beyond the keys passed to them. The ACL pattern `~prefix:*` covers those keys |
| PHP-FPM (PHP 8.4, nginx, six workers) | Tested in CI: the state is shared by requests and workers, also under concurrent requests |
| Other PHP versions under FPM, Apache, Sentinel, Valkey, replicas | Not tested, not supported |

## Set up

1. Create one registry for each deployment. A registry is the isolation unit: a key prefix, an identity and two ceilings. The application never creates it.

```php
$registry = new RedisRegistry('billing-prod', 'eu1-billing', $secret); // $secret: 32 bytes or more, kept for the life of the registry
$admin = new RedisRegistryAdmin($adminConnection, $registry);
$admin->initialize();           // idempotent; a different identity or ceilings is refused
$admin->inspectServer();        // reports maxmemory-policy; protectsState() must be true
```

2. Give the application a user with only the rules in `RedisAcl::execution($registry)`, and the host a user with `RedisAcl::administration($registry)`.

```
ACL SETUSER app on >password resetkeys resetchannels -@all +evalsha +script|load +hset ... ~billing-prod:*
```

Print the exact tokens with `RedisAcl::execution($registry)`. The tests create users from these rules, run every operation, and remove one rule at a time: each rule is needed, and with one missing the first call fails as unavailable and writes nothing.

3. Build the pipeline on the Redis environment. Creating it opens no connection.

```php
$connection = new RedisConnectionOptions('redis.internal', 1.0, 0.5, 6379, 'app', $password, tls: true, caFile: '/etc/ssl/redis-ca.pem');
$pipeline = $builder->compile(RedisEnvironment::for($builder->definition(), $classifier, $connection, $registry));
RedisEnvironment::explain($pipeline, $connection, $registry); // scope, timeout, registry identity; no I/O
```

4. Run maintenance from a worker or a scheduler: `(new RedisRegistryMaintenance($connection, $registry))->step()` until `more` is false. It removes expired leases, finished cooldowns, expired records and idle partitions, 128 at a time. Nothing runs it for you.

## Examples

`examples/` holds three scripts that run against a local server: a rate limit shared by two pipelines, what happens when Redis is down, and `explain()` for shared state. From the development repository, `tools/run-redis-examples.sh` starts Redis in Docker, runs them and compares their output with `examples/expected/`.

## Server settings

- Minimum version: Redis 7.0. The project tests Redis 7.4.11 and 8.2.10.
- Set `maxmemory-policy noeviction`. With a policy that evicts keys, the server can delete a partition without any signal and its protection starts again from nothing. Only the loss of the registry itself is reported.
- The state lives in Redis memory. A restart without persistence, a flush or a failover loses it, and a lost registry rejects every protected call until you initialize it again. No fresh state is ever used in place of lost state.
- TLS: set `tls: true`. Without `tls: true` the password and every command travel in clear text; use TLS on any network you do not fully control. The server certificate and the host name are always verified; there is no option to turn that off. Use `caFile` for a private CA and `tlsServerName` when you connect by address.

## When something goes wrong

| What happened | What you get | What was written |
| --- | --- | --- |
| Server unreachable, TLS refused, password wrong, user lacks a rule, server full (`noeviction`) | `StateUnavailableException`; the callable is not run | Nothing |
| Connection lost or timed out after a command was sent | `IndeterminateStateException` for a write, `StateUnavailableException` for a read; the library never sends it again | Unknown for a write: it may have been applied once |
| Registry missing | `StateUnavailableException` | Nothing; the registry is not created |
| Stored data damaged | `CorruptStateException` | Nothing of the call. Cleanup of expired state that ran before the damage was found stays, and the registry counters stay in step with it |
| A key under the prefix holds a value of the wrong type | `CorruptStateException` | Nothing of the call |
| Registry ceiling reached | `StateCapacityExceededException`, after expired state was reclaimed and the oldest finished records evicted | Nothing |

After an uncertain debit the operation is not run. After an uncertain release or finalization the result of your call is returned with incomplete accounting. After an uncertain retry commit no retry is sent.

## Operator actions

| Action | Call |
| --- | --- |
| Close a circuit and make its permits stale | `RedisCircuitAdmin::reset($policyIdentity)` |
| Retire a registry | `RedisRegistryAdmin::retire()`, then `purgeRetired()` to remove what it left. `initialize()` refuses a prefix that still holds the index or the tombstone queue of a retired registry, and `purgeRetired()` refuses to run while the registry exists |
| Check the server policy | `RedisRegistryAdmin::inspectServer()` (needs `CONFIG GET` and `INFO`) |

## Cost of one call

A script call is atomic and blocks the server while it runs. Most calls touch a few keys. Two paths cost time in proportion to the records of one partition: a circuit that changes state removes every permit it still holds, and a concurrency partition removes the leases that expired. Both are bounded by the registry ceiling `maxRecords`, which can be up to 1,000,000. Keep `maxRecords` as low as the load needs.

## Not planned for the first release

Redis Sentinel, Redis Cluster, Valkey, and protection that survives data loss on restart or failover.

## License

[MIT](LICENSE).
