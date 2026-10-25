| Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s | Cairn/YugabyteDB | Cairn/TiDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| read | 48,916 | 8,031 | 32,914 | 6.09x | 1.49x |
| mixed | 23,080 | 4,715 | 24,201 | 4.90x | 0.95x |
| transfer | 1,995 | 793 | 3,532 | 2.52x | 0.56x |

Medians of repeated runs. single-host, warm-cache, closed-loop development comparison; no production-wide claim.

Isolation: cairn: strict two-phase locking; yugabyte: repeatable read; tidb: repeatable read with pessimistic writes.

Cairn configuration: CAIRN_VOTE_COMMIT=3; CAIRN_STORAGE_CONNECTIONS=8; cpu_layout=storage; CAIRN_STORAGE_PIPELINE=1.
