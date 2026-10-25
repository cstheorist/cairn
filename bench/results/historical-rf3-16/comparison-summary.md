| Workload | Fast Postgres ops/s | YugabyteDB ops/s | TiDB ops/s | Fast/YugabyteDB | Fast/TiDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| read | 27,576 | 1,426 | 27,876 | 19.34x | 0.99x |
| mixed | 7,853 | 455 | 15,903 | 17.25x | 0.49x |
| transfer | 332 | 69 | 2,030 | 4.81x | 0.16x |

Medians of repeated runs. single-host, warm-cache, closed-loop development comparison; no production-wide claim.
