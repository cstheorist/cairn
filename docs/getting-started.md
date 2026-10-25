# Getting started

## Run

Docker Desktop or Docker Engine with Compose is sufficient for the server and integration tests:

```sh
make up
make test
```

The separate replicated deployment uses two three-node replica sets with synchronous user spaces, strict election fencing, automatic leader discovery, and linearizable SQL reads:

```sh
make ha-up
make ha-test
```

Its PostgreSQL gateway binds to `127.0.0.1:55433`; native replica ports are `3401..3403` and `3411..3413`. The HA fault test kills a leader, removes a majority, restores it, disconnects a live leader from the cluster network, and checks recovery. The native reader also supports a strong read by calling `cairn.ping`, then `cairn.get(table, key, term)` on the writable leader. Raw native SELECT from a follower can lag replication. `operator:local-operator` is an administrative credential in the isolated development HA configuration.

The development cluster has two independent Tarantool storage nodes and one gateway. Storage uses `memtx` with a persistent, `fsync`-backed WAL. Host ports bind to loopback:

| Interface | Address | Credentials |
| --- | --- | --- |
| PostgreSQL | `127.0.0.1:55432` | user/database `cairn`, password `local-postgres` |
| Tarantool shard 0 | `127.0.0.1:3301` | read-only user `reader`, password `local-reader` |
| Tarantool shard 1 | `127.0.0.1:3302` | read-only user `reader`, password `local-reader` |

These are local development credentials. The gateway currently uses cleartext password authentication without TLS; use the loopback deployment for development. Native gateway RPC credentials are separate from the PostgreSQL credential. SQL clients share one service identity and one database.

```sh
PGPASSWORD=local-postgres psql 'host=127.0.0.1 port=55432 user=cairn dbname=cairn sslmode=disable'
```

Run each statement separately, for example interactively in `psql`:

```sql
CREATE TABLE accounts (id BIGINT PRIMARY KEY, value TEXT, enabled BOOLEAN NOT NULL);
INSERT INTO accounts VALUES (42, 'hello', true);
SELECT id, value, enabled FROM accounts WHERE id = 42;
UPDATE accounts SET value = 'updated' WHERE id = 42;
```

Psycopg works with `autocommit=True`, including prepared queries, parameter binding, and binary results:

```python
import psycopg

connection = psycopg.connect(
    'host=127.0.0.1 port=55432 user=cairn dbname=cairn password=local-postgres sslmode=disable',
    autocommit=True,
)
print(connection.execute('SELECT * FROM accounts WHERE id = %s', (42,)).fetchone())
connection.close()
```

## Read the same table as key/value

There is no SQL-only copy, blob encoding, or gateway-side data cache. `accounts` is a Tarantool space with a native primary index and the SQL column order as its tuple format. Native reads bypass the Rust gateway:

```python
import tarantool

# The owner of signed BIGINT key k is (k + 2**63) % shard_count.
# Key 42 belongs to shard 0 in the supplied two-shard deployment.
connection = tarantool.Connection('127.0.0.1', 3301, user='reader', password='local-reader')
print(connection.select('accounts', [42]).data)
# [[42, 'updated', True]]
connection.close()
```

Inside Tarantool, the equivalent operation is `box.space.accounts:get{42}`. Native clients must route to the owning shard. The public native account has per-table read grants; writes and arbitrary Lua execution are denied. Mutations go through validated storage functions so routing and SQL constraints cannot be bypassed by the native reader.


## Build without Docker

The workspace pins Rust 1.99.1 for development and Verus. Docker release builds also test the supported Rust 1.85 baseline.

```sh
cargo build --release -p cairn-server
```

The server needs running storage nodes and explicit passwords:

```sh
CAIRN_STORAGE_PASSWORD=local-gateway CAIRN_PASSWORD=local-postgres \
  target/release/cairn-server
```

Use [operations](operations.md) for replica groups, ports and protocol selection.
