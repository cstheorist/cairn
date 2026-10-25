# Benchmark evidence

Cairn's performance goal is OLTP across point reads, writes and distributed
transactions. The goal remains open. These development runs do not establish
an advantage over TiDB or a production-wide ranking.

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
