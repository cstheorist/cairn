# Benchmark evidence

Cairn's performance goal is OLTP across point reads, writes and distributed
transactions. The goal remains open. These development runs do not establish
an OLTP-wide advantage over TiDB or a production-wide ranking. The current
64-client control does demonstrate a point-read advantage under its stated budget.

## Current source across concurrency, October 25, 2026

These fresh controls measure Cairn commit `1a94b50`, including bounded IPROTO
pipelining, committed point reads through temporary OPEN claims, and the
extended-query transaction-boundary fix. Each concurrency reruns all three
engines: no comparator results are reused between the one-, 16- and 64-client
controls. Every workload has three ten-second samples after two seconds of
warmup, 10,000 rows and 128-byte text payloads. Rates below are medians.

| Clients | Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s |
|---:|---|---:|---:|---:|
| 1 | read | 2,566 | 3,578 | 4,578 |
| 1 | mixed | 2,338 | 2,132 | 3,925 |
| 1 | transfer | 378 | 235 | 689 |
| 16 | read | 24,926 | 8,333 | 28,228 |
| 16 | mixed | 12,692 | 3,840 | 18,296 |
| 16 | transfer | 1,200 | 549 | 2,301 |
| 64 | read | 48,916 | 8,031 | 32,914 |
| 64 | mixed | 23,080 | 4,715 | 24,201 |
| 64 | transfer | 1,995 | 793 | 3,532 |

Selected latencies are median per-run p50/p99 in milliseconds:

| Clients | Workload | Cairn p50 / p99 | YugabyteDB p50 / p99 | TiDB p50 / p99 |
|---:|---|---:|---:|---:|
| 1 | read | 0.373 / 0.565 | 0.271 / 0.432 | 0.213 / 0.372 |
| 1 | transfer | 2.553 / 4.152 | 4.100 / 6.446 | 1.361 / 2.323 |
| 64 | read | 0.974 / 17.991 | 1.460 / 84.225 | 1.352 / 26.679 |
| 64 | transfer | 23.675 / 76.054 | 92.149 / 186.967 | 12.801 / 49.903 |

Cairn's 64-client read median is 1.49 times TiDB's, with lower p50 and p99.
Its mixed throughput is 95% of TiDB's and its transfer throughput is 56%.
TiDB has lower one-client point-read and transfer latency and leads mixed and
transfer throughput at every measured concurrency. YugabyteDB's one-client
point-read latency also beats Cairn's. These results distinguish low-load
latency from throughput under concurrency; they establish no OLTP-wide win.

Each engine has RF3, quorum two and durable WAL acknowledgement within six
database CPUs/six GiB. Cairn uses mode 3, eight pipelined storage connections
per shard and the storage allocation: gateway 0.75 CPU/1.5 GiB, six replicas
at 0.875 CPU/768 MiB each. The load generator has four CPUs/768 MiB. Profiling
and error logging are disabled. The frozen source archive is
`source-a274ae67471ab42a.tar.gz`; the load binary SHA-256 is
`a70535c3cfeb59f1067f53f4cfad6c161cd7393e17b6c481987241c40d68c546`.

YugabyteDB `2026.1.2.0-b137` uses explicitly verified REPEATABLE READ and
fail-on-conflict, with all SQL connections to `yb1`. Its three nodes each
combine master, TServer and PostgreSQL within two CPUs/two GiB. TiDB/TiKV/PD
`8.5.4` uses pessimistic REPEATABLE READ, separately capped SQL, KV and PD
roles, and the documented byte-identical-code TiKV debug-section removal.
Cairn uses strict transaction claims. These isolation contracts differ for
general read/write histories. Transfers here use two arithmetic updates on
different storage partitions; the mix is 90% point reads/10% increments.

Replication, per-key expected values, aggregate resources, source/result hashes
and before/after restart/OOM checks passed for all nine engine/concurrency
combinations. Strict reporting accepted the curated
[one-client](results/platform-2026-10-25/clients-1/),
[16-client](results/platform-2026-10-25/clients-16/) and
[64-client](results/platform-2026-10-25/clients-64/) evidence. Their summaries
retain repeat ranges and sample standard deviations. These are sequential,
single-host, warm-cache, closed-loop experiments, with no production confidence
interval or measured network/disk bandwidth. Retained test data, CPU quotas,
client placement and repeated-text compressibility constrain interpretation.
The runs do not isolate the causal effect of individual code changes.

Use the [harness](README.md) with `BENCH_CLIENTS=1`, `16` or `64` and
`--cairn-commit volatile --cairn-connections 8 --cairn-cpu-layout storage
--cairn-pipeline`; keep the default three ten-second repeats. The
[platform comparison](../docs/platform-performance.md) separates architectural
judgment from these measurements and reviews Tarn's different evidence.

## Repeated Cairn comparison, October 25, 2026

These runs use volatile OPEN with fused home finish (`CAIRN_VOTE_COMMIT=3`),
32 storage sockets per shard, the storage helper/snapshot optimizations,
16 clients, 10,000 rows and 128-byte payloads. Each workload has three
ten-second samples after two seconds of warmup. Every engine has RF3,
majority durability and six CPUs/six GiB; the compiled client has a separate
four-CPU/768-MiB budget. Restart, replication and every-key correctness checks
passed. Profiling is disabled.

| Workload | Cairn standard | Cairn storage allocation | Cairn standard recheck | YugabyteDB | TiDB |
|---|---:|---:|---:|---:|---:|
| read | 25,150 | 26,260 | 25,563 | 8,091 | 28,951 |
| mixed | 11,555 | 13,742 | 10,552 | 4,542 | 19,326 |
| transfer | 1,090 | 1,346 | 1,022 | 726 | 2,380 |

Values are median operations/s. The standard layout gives the gateway 1.5 CPUs
and each of six storage nodes 0.75 CPU. The optional storage allocation gives
the gateway 0.75 CPU and every storage node 0.875 CPU. Both retain the same
memory, durability and election settings. Equal storage caps preserve the new
leader's allocation after failover; the configured total remains six CPUs.

The storage allocation's mixed median was about 19% above the first standard
control and 30% above the final control; its transfer median was about 23% and
32% higher, respectively. Read sample ranges overlap. Transfer p99 medians were
44.9 ms with the storage allocation versus 68.5 ms and 73.7 ms for the controls.
These sequential single-host samples have no confidence interval and do not
isolate the effect of earlier code optimizations. The default allocation stays
unchanged. Leader death, quorum loss, network partition/heal, the native/SQL
suites and all volatile-open crash boundaries also passed under the alternate
six-CPU allocation.

With that allocation Cairn reached 3.25x, 3.03x and 1.85x YugabyteDB's read,
mixed and transfer medians. TiDB remained ahead on all three workloads.
YugabyteDB used explicitly verified REPEATABLE READ and fail-on-conflict;
TiDB used pessimistic REPEATABLE READ. Cairn uses strict two-phase locking.
These isolation contracts are not generally equivalent: the transfers are
write-only arithmetic updates across two logical partitions, and the mix is
90% point reads/10% increments. This is not a general SQL or production ranking.

The [first standard run](results/cairn-rf3-2026-10-25/standard/),
[storage allocation](results/cairn-rf3-2026-10-25/storage/) and
[final standard control](results/cairn-rf3-2026-10-25/standard-recheck/) retain raw
results, environment evidence, source archives/hashes and checked summaries.
YugabyteDB and TiDB were measured once with three repeats each; the allocation
experiment and final control reuse those same comparator results, rather than
representing additional independent runs of those engines.

## Historical repeated baseline

The original development name was **Fast Postgres**. This baseline used the
initial durable coordinator protocol, before vote/volatile/fused-finish changes:
16 clients, 10,000 rows, 128-byte payloads, two-second warmup, and three ten-second
runs per workload. Rates below are medians. Each database had RF3, fsync and a
six-CPU/six-GiB database budget; the load generator had a separate four-CPU budget.
YugabyteDB used SERIALIZABLE with fail-on-conflict; TiDB used pessimistic
REPEATABLE READ. Transfers always crossed logical partitions.

| Workload | Working-tree PostgreSQL server | YugabyteDB | TiDB |
|---|---:|---:|---:|
| read | 27,576 | 1,426 | 27,876 |
| mixed | 7,853 | 455 | 15,903 |
| transfer | 332 | 69 | 2,030 |

[Raw results and environment evidence](results/historical-rf3-16/) retain their
original names, values and image IDs. They describe the earlier working tree.

## Short diagnostic after fused finish

This separate run used 16 clients, a one-second warmup and one three-second
sample per workload. The PostgreSQL server used volatile OPEN state, fused home
finish and eight sockets per shard. YugabyteDB used explicitly verified
REPEATABLE READ; TiDB verified pessimistic REPEATABLE READ. Resource, RF3,
restart and per-key correctness checks passed for all three engines.

| Workload | Working-tree PostgreSQL server | YugabyteDB | TiDB |
|---|---:|---:|---:|
| read | 25,626 | 8,210 | 30,306 |
| mixed | 11,776 | 4,785 | 15,601 |
| transfer | 922 | 775 | 2,574 |

[Raw diagnostics, source archives and hashes](results/diagnostic-finish-16/) are
retained for investigation. The short duration and single sample do not support
a repeated comparative claim. Changing YugabyteDB's isolation made a substantial
difference; the historical SERIALIZABLE rates must keep that label.

A later [single-engine 32-socket diagnostic](results/diagnostic-pool-32/) reached
about 1,141 transfers/s. It was not a fresh three-engine repeated comparison.
Leader CPU throttling remains an observed bottleneck under the small per-node
resource limits. Further implementation and matched repeated measurements are
required.

## Reproduce current code

Use [the harness guide](README.md). Default runs use three ten-second repeats
and keep per-file source hashes, a source archive, load binary/image hashes,
container resource/startup evidence, replication checks and every expected key
value. `scripts/report_comparison.py` rejects short, incomplete, mismatched or
uncorrected runs. Historical archives should be interpreted with the source
version they contain; the current repository uses the Cairn name and layout.
