# Choosing a performance platform

For our goal, keep Tarantool as a deliberate specialized bet: low-latency SQL
plus access to the same committed data through an existing native tuple
interface. Optimize coordination and bytes moved rather than assuming RAM
wins. TiDB/TiKV remains our strongest general OLTP throughput reference and
leading general scale-out foundation; Cairn now demonstrates a clear 64-client
point-read advantage under this budget. YugabyteDB remains a substantial
PostgreSQL query foundation, with optimized storage paths that our capped
benchmark does not establish as fundamentally slow. These are engineering
judgments with different scopes, not a universal database ranking. The
scale-out judgment combines local measurements with documented architecture;
we have not measured a multi-host scalability curve.

| Platform | Strongest case | Main qualification |
| --- | --- | --- |
| TiDB/TiKV | General OLTP throughput reference; independently scalable SQL and transactional storage | Different isolation and native-data interface; no demonstrated bandwidth ceiling here |
| Tarantool through Cairn | Demonstrated 64-client point-read advantage and ordinary native tuples | Coordination, per-instance execution and fixed topology remain limits |
| Tarantool through Tarn | Related MySQL frontend, memtx/vinyl and leaner commit paths | Different acknowledgement/lease contracts; published numbers are unmatched |
| YugabyteDB | PostgreSQL-based query engine and distributed persistent storage | Full query-stack/resource costs; current tails need attribution, not architectural dismissal |

The accepted 1/16/64-client passes freshly reran all three engines; none reuses
historical comparator results. They use frozen Cairn
commit `1a94b50`, source fingerprint `a274ae67471ab42a`, three ten-second repeats,
two-second warmup, 10,000 rows, 128-byte payloads, six database CPUs/six GiB,
RF3, quorum two and fsync durability. Cairn uses mode 3, eight pipelined storage
connections per shard and the storage CPU layout. YugabyteDB is
`2026.1.2.0-b137`; TiDB/TiKV/PD are `8.5.4`. Rates are medians:

| Workload | Engine | 1 client, operations/s | 16 clients, operations/s | 64 clients, operations/s |
| --- | --- | ---: | ---: | ---: |
| Point read | Cairn | 2,566 | 24,926 | 48,916 |
| Point read | YugabyteDB | 3,578 | 8,333 | 8,031 |
| Point read | TiDB | 4,578 | 28,228 | 32,914 |
| 90/10 read/increment | Cairn | 2,338 | 12,692 | 23,080 |
| 90/10 read/increment | YugabyteDB | 2,132 | 3,840 | 4,715 |
| 90/10 read/increment | TiDB | 3,925 | 18,296 | 24,201 |
| Cross-partition transfer | Cairn | 378 | 1,200 | 1,995 |
| Cross-partition transfer | YugabyteDB | 235 | 549 | 793 |
| Cross-partition transfer | TiDB | 689 | 2,301 | 3,532 |

At 64 clients, Cairn's read throughput is 1.49 times TiDB's, with median per-run
p50/p99 of 0.974/17.991 ms versus 1.352/26.679 ms. This is a demonstrated narrow
win. Mixed throughput reaches 95% of TiDB, while transfer reaches 56%:
transfer p50/p99 is 23.675/76.054 ms for Cairn and 12.801/49.903 ms for TiDB.
It is not an OLTP-wide win.

At one client, point-read p50/p99 is 0.373/0.565 ms for Cairn, 0.271/0.432 ms
for YugabyteDB and 0.213/0.372 ms for TiDB. Transfer p50 is 2.553, 4.100 and
1.361 ms respectively. TiDB has the lowest measured low-load point latency;
Cairn's 64-client read-capacity win does not establish a lower latency floor.
These are end-to-end measurements including client, gateway and network, not
universal engine minima.

The [results discussion](../bench/RESULTS.md#current-source-across-concurrency-october-25-2026)
and retained evidence for [1 client](../bench/results/platform-2026-10-25/clients-1/),
[16 clients](../bench/results/platform-2026-10-25/clients-16/) and
[64 clients](../bench/results/platform-2026-10-25/clients-64/) preserve the raw
repeats, source/image identities and strict-report checks. These warm-cache,
single-host, closed-loop runs provide repeat ranges, not production confidence
or a sustained-storage limit.

At 16 clients, YugabyteDB's read p50 is 0.511 ms versus Cairn's 0.602 ms, despite
lower throughput and much higher p99. QPS alone cannot identify its latency
floor. Three YugabyteDB nodes each combine master, TServer and PostgreSQL under
two CPUs/two GiB, and all clients connect to `yb1`. TiDB separates its 1.5-CPU SQL
server from three 1.25-CPU KV servers and three 0.25-CPU PD servers. Cairn assigns 0.75 CPU
to its gateway and 0.875 CPU to each of six replicas. Equal aggregate budgets
do not imply equal bottlenecks or configured tuple arenas. Resource contention
or SQL concentration may explain some YugabyteDB tails, but that is an inference
requiring measurement.

TiDB translates SQL into transactional KV requests, while Regions distribute
key ranges and Raft leadership across TiKV nodes. Dedicated read, scheduler,
Raft, log-writer, apply and compaction pools provide parallelism. Ordering
within a Region remains necessary; more cores or nodes do not automatically
accelerate one hot key. SQL and storage capacity can scale separately.
[Architecture](https://docs.pingcap.com/tidb/stable/tidb-architecture/),
[execution pools](https://docs.pingcap.com/tidb/stable/tune-tikv-thread-performance/).

TiDB offers optimized commit paths. Eligible
single-Region transactions can use 1PC; eligible distributed transactions can
use async commit, moving completion work after acknowledgement. Pessimistic
locks can be pipelined or held in leader memory, so locking does not always
require a synchronous durable write per statement. Our transfers deliberately
cross partitions; 1PC should not be credited to that workload. Actual selected
paths and persisted/session settings need recording rather than inference from
defaults. [Commit controls](https://docs.pingcap.com/tidb/stable/system-variables/#tidb_enable_1pc-new-in-v50),
[pessimistic locking](https://docs.pingcap.com/tidb/stable/pessimistic-transaction/).

YugabyteDB reuses PostgreSQL's language/query layer over DocDB. Its eligible
single-row mutations bypass transaction-status coordination. Distributed
transactions replicate provisional records, commit through a status tablet,
then apply and clean up asynchronously. Both YugabyteDB and TiKV use leader
leases/safe-read mechanisms: a strong point read does not inherently require a
fresh quorum round. Extra network hops depend on placement and request paths.
[Query layer](https://docs.yugabyte.com/stable/architecture/query-layer/),
[single-row paths and leases](https://docs.yugabyte.com/stable/architecture/transactions/single-row-transactions/),
[distributed I/O](https://docs.yugabyte.com/stable/architecture/transactions/transactional-io-path/),
[TiKV lease reads](https://tikv.org/blog/lease-read/).

Tarantool memtx keeps tuples and indexes in RAM, with one transaction-processor
thread per instance; networking, WAL and replication have other threads.
Fibers overlap waits without making database execution arbitrarily parallel.
Additional instances/shards expose more execution capacity, but Cairn's fixed
ownership and lack of online movement make that an engineering task. TiKV's
many Regions and YugabyteDB's tablets offer a different partition-management
foundation. Cairn also retains transaction metadata without GC. These limits
matter beyond short runs. [Memtx execution](https://www.tarantool.io/docs/tarantool/en/3_x/platform/engines/memtx),
[Cairn design](design.md).

For the current mode-3 two-update cross-shard transfer, source inspection finds
eight storage RPCs across seven serial phases and four logical synchronous WAL
transactions. Whole payloads appear in intents and again in user-row
replacement. Those are not four physical fsyncs: Tarantool already groups WAL,
confirmation and notification work. Reducing phases and metadata bytes is a
concrete optimization hypothesis. The one-in-flight-read-batch-per-shard worker
is likewise an implementation ceiling, not an engine law.
[Coordinator](../crates/cairn-sql/src/transaction.rs),
[storage phases](../lua/volatile.lua).

The native interface is a material requirement. Cairn stores committed user
rows as ordinary Tarantool tuples, readable through iproto SELECT; its strong
`cairn.get` path additionally applies leadership and claim checks. Raw native
reads retain local visibility and do not promise cross-shard snapshots.
TiKV RawKV is not a TiDB logical-row reader. API v2 separates TiDB, standalone
TxnKV and RawKV data modes; older mixing was unsafe. Transactional tools can
access TiDB data using its key/row codecs and snapshot handling, but that is a
codec-aware integration, not an existing typed tuple interface. A custom SQL
layer over TxnKV is possible, with its own format and compatibility obligations.
[Cairn visibility](transactions.md),
[TiKV API boundary](https://tikv.org/docs/dev/concepts/explore-tikv-features/api-v2/),
[key-format specification](https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md).

Bandwidth needs separate accounting. YugabyteDB can update an individual column
subdocument without rewriting unchanged text into that logical mutation;
packed inserts and later compaction can still contain the complete row.
TiDB 8.5.4 normally encodes a replacement row value, including unchanged stored
columns, while omitting eligible handle/default/virtual fields. This suggests
a YugabyteDB advantage for small updates to wide rows, not an exact byte ratio.
Our repeated-text payload is also highly compressible.
[Packed-row design](https://docs.yugabyte.com/stable/architecture/docdb/packed-rows/),
[pinned YugabyteDB write path](https://github.com/yugabyte/yugabyte-db/blob/2026.1.2.0-b137/src/yb/docdb/pgsql_operation.cc#L1686),
[pinned TiDB update path](https://github.com/pingcap/tidb/blob/v8.5.4/pkg/table/tables/tables.go#L452).

Encoded keys, timestamps, indexes, intents, protocol records, quorum replication
and cleanup multiply work beyond application payload. Async acknowledgement
changes the critical path, not total work. DocDB uses its Raft log instead of
an additional RocksDB WAL; it would be wrong to count two independent WAL
rounds. Both LSM-based systems still flush, compact and reclaim versions.
Memtx removes user-table LSM compaction, not WAL, replication, tuple allocation,
snapshots or transaction metadata. [DocDB storage costs](https://docs.yugabyte.com/stable/architecture/docdb/performance/),
[TiKV storage](https://docs.pingcap.com/tidb/stable/tidb-storage/).

Tarn is another Rust/Lua frontend over Tarantool, not a distinct C storage
engine. At reviewed commit `c435e4056b453f60f0f1667c84a53a19214046ad`, it supports
memtx and disk-backed vinyl, with protocol metadata remaining in memtx. Its
gateway buffers writes, validates reads through native MVCC streams, then runs
parallel PREPARE followed by parallel APPLY. Cairn instead maintains OPEN
claims, authenticated scope/receipts and a home-last, all-release-before-ACK
contract. Different transaction plumbing explains Tarn's leaner path without
implying a fundamentally different storage engine.
[Parallel phases](https://github.com/cstheorist/tarn/blob/c435e4056b453f60f0f1667c84a53a19214046ad/crates/tarn-sql/src/txn.rs#L683).
Its prepare/apply path can acknowledge after irrevocable preparation if application
keeps failing; optional ack-on-prepare is earlier still. Direct native readers
can therefore lag an acknowledged SQL commit. Its leader-read lease also has
timing assumptions. These differ from Cairn's current application-before-ACK
contract and must accompany latency comparisons.
[Tarn contract](https://github.com/cstheorist/tarn/blob/c435e4056b453f60f0f1667c84a53a19214046ad/docs/design.md),
[ACK fallback implementation](https://github.com/cstheorist/tarn/blob/c435e4056b453f60f0f1667c84a53a19214046ad/crates/tarn-sql/src/txn.rs#L801).

Tarn's two-shard headline results use RF1 and `wal_mode=write`, on a different
machine; its newer vinyl/TiDB experiment uses RF1 and tmpfs. A tmpfs cache miss
is not an SSD read, and direct native SELECT bypasses SQL coordination. None
establishes superiority over our RF3/fsync comparison. Vinyl broadens the
Tarantool option beyond RAM, but adds cache misses and LSM maintenance that
Cairn's current memtx implementation has not measured. Native Tarn layouts also
include a bucket field and schema-evolution considerations. Moving payloads
to vinyl does not eliminate RAM-resident protocol metadata or prove Cairn
correct on a new engine.
[Tarn evidence and caveats](https://github.com/cstheorist/tarn/blob/c435e4056b453f60f0f1667c84a53a19214046ad/bench/RESULTS.md),
[vinyl contract](https://github.com/cstheorist/tarn/blob/c435e4056b453f60f0f1667c84a53a19214046ad/docs/design.md#211-tables-on-disk-vinyl),
[native layout](https://github.com/cstheorist/tarn/blob/c435e4056b453f60f0f1667c84a53a19214046ad/crates/tarn-sql/src/catalog.rs#L289).

Correctness costs must remain visible. Our YugabyteDB sessions use snapshot
REPEATABLE READ; TiDB's REPEATABLE READ is snapshot isolation and permits write
skew. Pessimistic mode does not make ordinary reads serializable. Cairn's strict
claims and Tarn's validation therefore do different work. Successful arithmetic
transfer checks do not establish equivalence for arbitrary read/write or
predicate transactions. Current documentation also describes evolving defaults;
pinned versions and actual settings govern the measurements.
[YugabyteDB isolation](https://docs.yugabyte.com/stable/architecture/transactions/isolation-levels/),
[TiDB isolation](https://docs.pingcap.com/tidb/stable/transaction-isolation-levels/).

The remaining measurements should answer specific decisions:

1. Broaden the concurrency sweep and report latency,
   retries, errors and resource utilization at equal throughput and equal p99.
2. Sweep incompressible payloads and partial/full-row updates; count application,
   RPC, replication and disk bytes per committed operation. Measure useful
   payload bandwidth separately from network consumption.
3. Run longer steady-state writes and datasets exceeding caches on real SSDs;
   include compaction, metadata growth, checkpoint and restart costs.
4. Separate hot-key/hot-partition limits from many-partition scaling; distribute
   SQL connections, vary cores per process, and measure cross-zone RTT effects.
5. Match durability, isolation and native-read-after-ACK requirements, including
   failure/recovery windows; record actual optimized commit paths.

Until those pass, there is no direct Gbit/s, cold-SSD or production-scale
performance evidence. The practical choice is to retain Tarantool for the
native-tuple specialization, attack phase count and work amplification, and use
TiDB as the general OLTP throughput reference while keeping YugabyteDB as
a serious PostgreSQL-oriented alternative.
