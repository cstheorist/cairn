| Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s | Cairn/YugabyteDB | Cairn/TiDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| read | 25,150 | 8,091 | 28,951 | 3.11x | 0.87x |
| mixed | 11,555 | 4,542 | 19,326 | 2.54x | 0.60x |
| transfer | 1,090 | 726 | 2,380 | 1.50x | 0.46x |

Medians of repeated runs. single-host, warm-cache, closed-loop development comparison; no production-wide claim.

Isolation: cairn: strict two-phase locking; yugabyte: repeatable read; tidb: repeatable read with pessimistic writes.

Cairn configuration: CAIRN_VOTE_COMMIT=3; CAIRN_STORAGE_CONNECTIONS=32; cpu_layout=standard.
