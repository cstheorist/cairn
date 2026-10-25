# Design

Cairn is a PostgreSQL frontend over ordinary Tarantool spaces. The Rust SQL
coordinator routes integer primary keys to fixed shards. Each replicated shard
is a Tarantool replica set with synchronous user/metadata spaces and automatic
leader election.

## Components

| Component | Responsibility |
|---|---|
| `cairn-common` | Columns, table schemas and PostgreSQL errors |
| `cairn-verified` | Extracted routing, bounds, arithmetic and incarnation guards |
| `cairn-iproto` | Native transport, topology, connection pools and storage calls |
| `cairn-sql` | Parsing, planning, bounded execution, sessions and commit coordination |
| `cairn-pgwire` | PostgreSQL startup, simple/extended messages and portal state |
| `cairn-server` | Configuration, startup, sessions and recovery workers |
| `lua/` | Owner checks, native tables, locks, intents, votes and application |
| `tla/` | Bounded protocol contracts and deliberately weakened variants |

## Storage and visibility

Each SQL table is one native formatted space per shard, with an integer primary
index and SQL column order. Staged transaction values live in metadata until a
commit decision is irrevocable. The public native reader has per-table read
access; validated storage procedures perform writes.

Public point reads and `cairn.get` use a linearizable barrier and reject durable
write claims and all exclusive table claims. Temporary OPEN row intents leave
the previous committed tuple readable; prepare durably copies their claims with
YES before they can commit. Explicit transaction reads hold row claims, while
scans hold table claims for predicates and absent rows. Raw native SELECT retains
Tarantool's local visibility and can observe follower lag or cross-shard apply progress.

## Commit protocols

The default protocol durably records coordinator membership, prepares every
participant, records a decision, applies changes, establishes global completion
and releases locks. A sole colocated participant combines these steps in one
synchronous transaction.

Optional modes derive commit from unanimous immutable YES votes and allow atomic
local application/unlock after that decision. The volatile-open mode keeps
unprepared state temporary, binds it to durable storage incarnations, and copies
the complete read/write claim set into synchronous storage before YES. Remote
votes retain an authenticated scope for recovering a missing home coordinator.

The fastest current distributed path sends unanimous YES receipts directly to
remote finish procedures. The home finishes last and atomically installs its
rows and final cleanup after authenticating every remote release.

See [transaction semantics](transactions.md) for each transition, certificate,
failure rule and current limit. [Architecture](architecture.md) covers routing
and execution choices. [Verification](verification.md) distinguishes checked
contracts from trusted implementation/storage assumptions.

## Limits

The shard list and ownership formula are fixed. CREATE is atomic per shard,
with explicit repair for partial creation; transactional DDL and movement are
pending. Locks fail fast on conflict. Metadata and replay tombstones are retained.
The SQL executor and wire caches have explicit bounds documented in
[SQL compatibility](sql-compatibility.md).
