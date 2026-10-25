# Performance measurements

The included harness compares two access paths to the same data: PostgreSQL prepared point reads through the Rust gateway and direct primary-index reads through Tarantool's existing iproto interface. It verifies returned values, warms each client, uses a fixed random seed per client, and records throughput and p50/p95/p99 latency as JSON.

```sh
make up test
docker compose --profile test run --rm tests python /bench/read.py \
    --clients 4 --rows 1000 --payload 128 --seconds 10
```

The default output is `artifacts/read-benchmark.json`. Startup, connection establishment, initial loading, and warmup are outside the timed read workload. The measured elapsed time includes finishing the last requests. Values are 128-byte strings by default; initial loading uses durable single-row SQL INSERTs, but their throughput is not measured.

One [recorded development run on October 24, 2026](results/local-2026-10-24.json), with four clients, 1,000 rows and ten seconds per access path, produced:

| Path | Reads/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| PostgreSQL gateway | 17,659 | 214 microseconds | 416 microseconds |
| Native Tarantool SELECT | 24,949 | 141 microseconds | 387 microseconds |

This is one run with no confidence interval. The two paths use different client libraries. These figures must be read with the constraints below.

This is a closed-loop, warm-cache, single-host Docker smoke benchmark. Python, different client libraries, thread scheduling, and network serialization contribute to latency and throughput. Client-side and open-loop queueing outside the request cycle are not represented. The native measurements are a useful reference for the storage path, not a controlled isolation of gateway overhead. The gateway and both shards share the same machine, and each shard has one copy of its data. Test tables remain in development volumes.

## Replicated comparison harness

`make compare` runs a compiled Rust client against Cairn, YugabyteDB `2026.1.2.0-b137`, and TiDB/TiKV/PD `8.5.4`, one database at a time. Each receives an aggregate limit of six CPUs and six GiB; the load container has four CPUs and 768 MiB. All data has three replicas and majority durable acknowledgement. Cairn uses fsync WAL and linearizable reads, YugabyteDB uses durable WAL, explicit repeatable-read transactions and fail-on-conflict by default, and TiDB uses synchronous Raft logs, leader reads and pessimistic repeatable-read transactions. Those isolation levels are not generally equivalent; the measured transfer workload performs only arithmetic writes to two keys and checks every committed key value afterward.

```sh
make ha-up
make compare
# Short diagnostic; not a substitute for the repeated default run:
BENCH_SECONDS=3 BENCH_WARMUP=1 BENCH_REPEATS=1 python3 scripts/run_comparison.py --databases cairn
```

The default workload has 10,000 rows, 128-byte text payloads and 16 clients. It runs point reads, a 90/10 read/increment mix, and two-key transfers across different storage partitions, each for three ten-second runs after two seconds of warmup. PostgreSQL paths use tokio-postgres; TiDB uses mysql_async and MySQL binary prepared statements. One operation is outstanding per connection. Latency includes serialization retries and finishing requests; closed-loop coordinated omission is not corrected. The driver validates payloads, row identity, row count, global balance and every key's expected value after each run.

Results and failures are retained in `artifacts/`. Only complete results paired with environment evidence and no container restarts/OOMs are accepted. The harness records resource limits, image IDs, durability/replication settings, source hashes and result hashes. Short or failed diagnostic runs are not performance wins. The single-host Docker environment is a development comparison, with no confidence claim about production deployments or datasets exceeding memory.

New measurements also retain a source archive with per-file hashes. The load image carries a checked fingerprint of its driver sources; `--no-build` refuses a stale image. Storage images include the Lua sources, so rebuilding after a script change recreates the storage processes. `make comparison-report` verifies evidence and hashes, requires matching client/workload settings and at least three ten-second repeats, and reports medians, ranges and sample standard deviations. The user-prioritized workload is OLTP: reads, writes and distributed transactions all matter. A point-read win alone does not establish an OLTP-wide advantage.

Use `python3 scripts/run_comparison.py --cairn-commit votes` to measure the optional vote-based commit path after `make vote-test`. The default remains the earlier coordinator-record path. Raw results name the selected gateway settings. Exact mode, isolation levels and the TiKV debug-section change below must accompany any comparative claim.

`--cairn-commit local-finish` selects the independent apply/unlock variant (`CAIRN_VOTE_COMMIT=2`); validate it with `make local-finish-test` first. Every shard still durably installs its changes before the SQL commit is acknowledged. The native tests check interleaved reads/writes while another shard remains unfinished, capability separation from barrier-mode transactions, and crash recovery.

`--cairn-commit volatile` selects temporary OPEN state (`CAIRN_VOTE_COMMIT=3`), validated with `make volatile-test`. Open statements, enlistment and membership freeze do not write WAL. Prepare atomically persists the entire read/write lock set, staged rows, YES vote and recovery scope. A sole participant installs its rows and durable outcome in one synchronous transaction. Native user tuples remain committed data. Leader incarnations fence lost temporary state; remote prepared votes retain enough information to recover before the home coordinator exists. Benchmark acceptance still requires all replicas, fsync and per-key result checks.

The TiKV image is derived from the official ARM image by removing DWARF debug sections. A heap profile found roughly 1.8 GB per node allocated while the backtrace library decompressed those sections at startup, causing OOMs under the small resource limit. The build checks that the executable `.text` section is byte-identical before and after stripping and retains both binary and code hashes. Runtime source code is unchanged. Explicit small-node cache/thread limits and disabled heap profiling are in `tikv.toml`. YugabyteDB's default wait-on-conflict mode produced repeated deadlock failures in diagnostics; the recorded configuration instead uses its documented [fail-on-conflict mode](https://docs.yugabyte.com/stable/architecture/transactions/concurrency-control/). Both configuration choices must accompany any result citation.

New comparisons default YugabyteDB to explicit REPEATABLE READ, matching TiDB's selected isolation for these point operations and write-only transfers. Use `--yugabyte-isolation serializable` or `read-committed` for controls. Every worker verifies its actual session setting before preparing statements. Earlier archived results used SERIALIZABLE; they must retain that label. YugabyteDB documents [snapshot versus serializable isolation](https://docs.yugabyte.com/stable/explore/transactions/isolation-levels/) and the extra read/write conflict detection of the latter. Equality of this benchmark's expected values does not establish equivalence for general predicate/read-write workloads.

For diagnostics, `CAIRN_PROFILE_RPC=1` emits per-shard/function RPC counts, failures and latency buckets once per second. `CAIRN_LOG_ERRORS=1` logs wire error codes and connection identities without SQL text or parameters. Both are disabled in normal comparisons and retained in gateway-setting evidence when enabled. Profiled or short runs are diagnostics rather than the repeated comparative result.

`--cairn-connections N` selects 1..64 storage sockets per shard (default 8). The selected `CAIRN_STORAGE_CONNECTIONS` is recorded with the other gateway settings. Known SQL errors keep a fully consumed, correctly matched native connection reusable; a transport failure, timeout or leadership fence retires it. Failed mutations are still never replayed automatically.

`--cairn-cpu-layout standard|storage` selects the benchmark CPU allocation. The default `standard` keeps the gateway at 1.5 CPUs and each of six storage nodes at 0.75 CPU. The optional `storage` layout applies `resources-storage.yaml` after `resources.yaml`: the gateway gets 0.75 CPU and each storage node gets 0.875 CPU. Both total six CPUs and retain the same six GiB memory limit, RF3, quorum two, fsync and election configuration. All replicas receive equal caps, so leader movement does not change the new leader's allocated capacity.

```sh
python3 scripts/run_comparison.py --databases cairn --cairn-commit volatile --cairn-cpu-layout storage
```

This optional experiment follows a diagnostic that observed leader throttling alongside unused gateway quota. It is not a measured improvement: a smaller gateway cap may hurt reads, mixed workloads or recovery bursts. Validate faults and repeat every workload before drawing conclusions. Cairn metadata records `cpu_layout`; evidence still records actual container limits and checks the aggregate budget, and source archives include both resource files. Report the selected allocation with any result, retain the standard layout as a control, and use fresh matched repeated measurements for comparative claims.

## Broader performance coverage

- Match replica count, durable commit policy, read consistency, hardware, core/memory limits, network placement, dataset, indexes, key distribution, and client concurrency.
- Use a dataset larger than cache as a separate workload, alongside warm-cache point reads. Record memory consumption and storage amplification.
- Include insert, update, read/write mixes, contention, range scans, and multi-statement/cross-shard transactions only where semantics match.
- Run a compiled load generator on separate hosts, include an open-loop latency workload to expose queueing, and report saturation/error behavior.
- Repeat enough times to report run-to-run variance; capture CPU profiles, p99/p99.9 latency, recovery time, and correctness checks.
- Record exact server builds, deployment manifests, database settings, and raw results. Do not compare an unreplicated in-memory point-read prototype against replicated transactional workloads and call the result a database-wide win.

The historical native-versus-gateway smoke numbers above are separate from this replicated harness and must not be mixed with its results.
