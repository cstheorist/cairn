| Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s | Cairn/YugabyteDB | Cairn/TiDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| read | 2,566 | 3,578 | 4,578 | 0.72x | 0.56x |
| mixed | 2,338 | 2,132 | 3,925 | 1.10x | 0.60x |
| transfer | 378 | 235 | 689 | 1.61x | 0.55x |

Medians of repeated runs. single-host, warm-cache, closed-loop development comparison; no production-wide claim.

Isolation: cairn: strict two-phase locking; yugabyte: repeatable read; tidb: repeatable read with pessimistic writes.

Cairn configuration: CAIRN_VOTE_COMMIT=3; CAIRN_STORAGE_CONNECTIONS=8; cpu_layout=storage; CAIRN_STORAGE_PIPELINE=1.
