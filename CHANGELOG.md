# Changelog

## Unreleased

### Added

- Initial Cairn workspace: PostgreSQL wire protocol and SQL execution over native
  Tarantool tables, with independent common, IPROTO, SQL, wire, verified and server crates.
- Replicated shards, automatic leader discovery, fencing, recoverable cross-shard
  transactions, predicate locks and an atomic single-shard commit path.
- Optional durable-vote, local-finish and incarnation-bound volatile-open protocols.
- Direct proof-based remote finish and a final home step combining application,
  lock release, outcome and global cleanup.
- Bounded point-read batches, checked typed increment pushdown and a configurable
  storage connection pool.
- Optional bounded IPROTO pipelining with coalesced writes, exact SYNC ownership,
  cancellation/deadline retirement and conditional generation fencing.
- Eleven bounded TLA+ models, forty negative checks, six reachability controls,
  fourteen executable Verus functions and the matching storage-side native core.
- PostgreSQL differential, concurrency, native protocol and failure-recovery suites.
- Matched-resource YugabyteDB/TiDB harness with explicit isolation, source/image
  hashes, restart/replication checks and per-key expected values.

### Changed

- Public point reads retain the committed tuple through temporary OPEN row-write
  claims. Durable write claims and exclusive table claims still block; explicit
  transaction reads, writes and scans retain strict locking. Phase tests and four
  leader-failure cases cover prepare, restart, partial finish and acknowledgement.
- Storage reuses immutable tuple validators and metadata handles, and reuses
  claim snapshots during prepare/finish without changing WAL records or ordering.

### Fixed

- Extended-query point reads retain their dependencies through Flush and later
  statements before Sync, preventing a serializability cycle. BEGIN adopts the
  existing implicit transaction, and explicit scope survives Sync. Thirteen
  PostgreSQL 16 differential cases exercise all four commit modes.
- INTEGER and BOOLEAN text input follows PostgreSQL 16 whitespace, spelling,
  radix/separator, width and syntax-versus-overflow rules.
- Native transport rejects nonfinal push responses, ambiguous response identity,
  malformed maps and trailing data before mapping SQL errors or retryable fences.
- Bind parameters reject invalid UTF-8 and embedded NUL with PostgreSQL's
  `22021`, accept nonzero binary BOOLEAN values, and distinguish short from
  overlong binary scalars. Error recovery and transaction rollback remain intact.
- Isolated-leader fencing probes have bounded native/host deadlines and explicit
  process exit; timeouts and fencing failures still reconnect the test node.

- Native protocol tests tolerate explicit MVCC rollback when recovery races an
  idempotent resolution phase. Transport uncertainty and mutations remain fail-fast.

### Known gaps

- Full PostgreSQL/ORM compatibility, secondary indexes, additional types,
  transactional DDL and savepoints remain open.
- Online resharding, safe metadata collection, long transaction heartbeats,
  TLS and coordinated backup/restore remain open.
- The requested OLTP performance advantage over TiDB is not established.
