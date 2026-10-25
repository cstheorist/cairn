| Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s | Cairn/YugabyteDB | Cairn/TiDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| read | 26,260 | 8,091 | 28,951 | 3.25x | 0.91x |
| mixed | 13,742 | 4,542 | 19,326 | 3.03x | 0.71x |
| transfer | 1,346 | 726 | 2,380 | 1.85x | 0.57x |

Medians of repeated runs. single-host, warm-cache, closed-loop development comparison; no production-wide claim.

Isolation: cairn: strict two-phase locking; yugabyte: repeatable read; tidb: repeatable read with pessimistic writes.

Cairn configuration: CAIRN_VOTE_COMMIT=3; CAIRN_STORAGE_CONNECTIONS=32; cpu_layout=storage.
