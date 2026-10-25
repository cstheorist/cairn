| Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s | Cairn/YugabyteDB | Cairn/TiDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| read | 25,563 | 8,091 | 28,951 | 3.16x | 0.88x |
| mixed | 10,552 | 4,542 | 19,326 | 2.32x | 0.55x |
| transfer | 1,022 | 726 | 2,380 | 1.41x | 0.43x |

Medians of repeated runs. single-host, warm-cache, closed-loop development comparison; no production-wide claim.

Isolation: cairn: strict two-phase locking; yugabyte: repeatable read; tidb: repeatable read with pessimistic writes.

Cairn configuration: CAIRN_VOTE_COMMIT=3; CAIRN_STORAGE_CONNECTIONS=32; cpu_layout=standard.
