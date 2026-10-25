| Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s | Cairn/YugabyteDB | Cairn/TiDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| read | 24,926 | 8,333 | 28,228 | 2.99x | 0.88x |
| mixed | 12,692 | 3,840 | 18,296 | 3.31x | 0.69x |
| transfer | 1,200 | 549 | 2,301 | 2.19x | 0.52x |

Medians of repeated runs. single-host, warm-cache, closed-loop development comparison; no production-wide claim.

Isolation: cairn: strict two-phase locking; yugabyte: repeatable read; tidb: repeatable read with pessimistic writes.

Cairn configuration: CAIRN_VOTE_COMMIT=3; CAIRN_STORAGE_CONNECTIONS=8; cpu_layout=storage; CAIRN_STORAGE_PIPELINE=1.
