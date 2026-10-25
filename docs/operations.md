# Operations

Cairn's supplied deployments are for local development. The single-node-per-shard
Compose project is `cairn`; the replicated project is `cairn-ha`. Both define two
shards. The server accepts a configured ordered list of up to 1,024 shards.

## Run and inspect

```sh
scripts/cluster.sh start -n 2 -r 1 -p 55432
scripts/cluster.sh status
scripts/cluster.sh stop

scripts/cluster.sh start -n 2 -r 3 -p 55433
scripts/cluster.sh status -r 3
scripts/cluster.sh logs -r 3
```

Stopping preserves volumes. The PostgreSQL ports bind to loopback. Native ports
are 3301/3302 for the simple deployment and 3401..3403/3411..3413 for replication.
Use `make ha-test` for leader death, quorum loss and partition/heal tests.

## Gateway configuration

| Variable | Default / meaning |
|---|---|
| `CAIRN_LISTEN` | `127.0.0.1:55432` outside Docker |
| `CAIRN_SHARDS` | Comma-separated shards; `|` separates replicas within a shard |
| `CAIRN_STORAGE_USER` | `gateway` |
| `CAIRN_STORAGE_PASSWORD` | Required native service password |
| `CAIRN_PASSWORD` | Required PostgreSQL password |
| `CAIRN_STORAGE_CONNECTIONS` | 8 sockets per shard; allowed range 1..64 |
| `CAIRN_READ_BATCH_US` | 50 microseconds; public point-read busy-yield collection window, integer 0..1000 |
| `CAIRN_VOTE_COMMIT` | 0 default coordinator protocol; 1 votes; 2 local finish; 3 volatile OPEN |
| `CAIRN_RECOVERY` | Enabled unless exactly `0`; disabling is for deterministic fault tests |
| `CAIRN_LOG_ERRORS` | `1` logs SQLSTATE, message tag and connection identity |
| `CAIRN_PROFILE_RPC` | `1` emits per-function RPC latency/count diagnostics |
| `CAIRN_PG_PORT` | Host port override in the supplied Compose files |

The shard order/count is persisted and checked. Reordering or changing it on an
existing dataset is refused; online resharding is pending. Each shard accepts
1..9 replica addresses. Leadership changes invalidate cached connections.

For `CAIRN_READ_BATCH_US`, zero drains already queued reads before freezing the
batch without yielding to collect more. Requests arriving during a storage RPC
wait for the next batch. Empty, malformed and out-of-range values fail startup.
The collection window is not a bound on scheduler or response latency. Compare
values with `scripts/run_comparison.py --cairn-read-batch-us US`, which records
and checks both the requested value and the actual gateway environment.

## Storage configuration

`CAIRN_SHARD_ID`, `CAIRN_SHARD_COUNT`, `CAIRN_GATEWAY_PASSWORD` and
`CAIRN_READER_PASSWORD` are required by the storage bootstrap.
`CAIRN_MANAGED_CONFIG=1` selects the replicated YAML configuration.
`CAIRN_TX_TIMEOUT` defaults to 30 seconds for open transactions.
`CAIRN_TX_SECRET` must have at least 16 bytes and agree across storage nodes;
replace the development key for a real deployment. Key rotation is pending.

The replicated configuration uses three copies, quorum two, fsync WAL, MVCC and
strict election fencing. A newly writable incarnation durably advances its
counter before accepting work.

## Outcomes and recovery

| SQLSTATE | Application meaning |
|---|---|
| `40001` | Known conflict/abort; retry the whole transaction |
| `40003` | Commit may be established; preserve its ID and recover the outcome |
| `08006` | Transport failure; mutation outcome may be unknown |
| `57P03` | Storage leadership unavailable/fenced |
| `25P02` | Transaction is failed; issue ROLLBACK |
| `54000` | Explicit execution/resource limit |

The recovery worker finishes durable decisions and resolves immutable votes.
An OPEN coordinator record can already represent a committed unanimous vote
set; raw OPEN is not evidence of rollback. Unprepared state lost on failover is
fenced by its incarnation. Prepared state survives in synchronous spaces.

Metadata and tombstones are retained. Long-running transaction heartbeats,
safe collection and coordinated backup/restore orchestration remain pending.
Independent volume copies are not a consistent cross-shard backup.

## Security boundary

The PostgreSQL frontend uses cleartext password authentication without TLS.
The native protocol also has no transport encryption. The development SQL user
is `cairn`, with database `cairn` and password `local-postgres`. The native
`reader` is read-only; `gateway` executes only named storage functions;
the HA configuration's `operator` is an administrative test credential.

SQL sessions share one service identity and database. Roles, per-user SQL ACLs,
TLS and credential/key rotation are pending. Restrict these prototypes to trusted
networks and use [private reporting](../SECURITY.md) for vulnerabilities.

## Development naming

Early development used the working name Fast Postgres. Historical benchmark
evidence retains its original names and hashes. Cairn uses separate Compose
project names and volumes; earlier development volumes have not been migrated.
