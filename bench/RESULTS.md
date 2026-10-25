# Benchmark evidence

Cairn's performance goal is OLTP across point reads, writes and distributed
transactions. The goal remains open. These development runs do not establish
an advantage over TiDB or a production-wide ranking.

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
