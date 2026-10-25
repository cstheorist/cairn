# Roadmap

The current baseline is an executable PostgreSQL protocol server over native
Tarantool tables, including replicated shards, automatic failover and distributed
transactions. The complete performance/compatibility objective remains active.

## OLTP performance

- Close the measured gap on point reads, mixed read/write operations and
  cross-shard transfers; a single favorable workload is insufficient.
- Reduce leader CPU and storage round trips without weakening durability,
  isolation, native committed-row visibility or outcome handling.
- Re-run repeated matched YugabyteDB/TiDB comparisons, with session isolation,
  resources, source/image evidence, latency and retries stated explicitly.
- Broaden contention, data size, failure-under-load and multi-host coverage.

## PostgreSQL compatibility

- Expand types, expressions and aggregates with PostgreSQL differential tests.
- Add indexes, richer constraints, catalogs/ORM compatibility and schema support.
- Add savepoints and transactional DDL with protocol and recovery coverage.
- Extend session, cancellation and isolation controls as their semantics are implemented.

## Distribution and operations

- Online metadata/shard movement and reconfiguration.
- Safe transaction metadata/replay-protection collection and transaction heartbeats.
- TLS, SQL identities/ACLs, observability, graceful lifecycle and coordinated backups.

## Verification

- Extend the checked contracts as mechanisms change and retain negative checks.
- Increase composed protocol/locking coverage and fault histories.
- Keep the executable verified core shared with runtime and storage.

[Tarn](https://github.com/cstheorist/tarn) is the related MySQL project. Its
commit/locking changes are useful design input; PostgreSQL behavior and Cairn's
implementation must be validated independently.
