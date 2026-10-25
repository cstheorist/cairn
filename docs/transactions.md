# Distributed transactions

Transactions use strict two-phase locking with durable intents and a recoverable two-phase commit protocol. The first participating shard hosts the coordinator in a synchronous space; each participant stores its locks, intents, and phase in its own synchronous spaces. New UUIDv8 transaction IDs encode their coordinator in the first two bytes; legacy UUIDv4 IDs retain shard-zero routing. Explicit BEGIN/COMMIT/ROLLBACK, read-only mode, own writes, failed transaction state, disconnect rollback, and extended-query pipeline rollback are implemented.

## Commit path

1. The coordinator records a unique transaction ID. It durably enlists a shard before issuing that shard's enlistment certificate.
2. Participants acquire shared read locks or exclusive write locks. Writes update intent records; native user tuples remain unchanged. Conflicts fail immediately with SQLSTATE `40001`, avoiding distributed deadlocks.
3. Each participant durably prepares and issues an authenticated prepare receipt. The coordinator requires every enlisted participant's receipt before recording COMMIT.
4. A decision certificate authorizes idempotent application of all intents. Participants retain locks and issue apply receipts only after local durable commit.
5. The coordinator requires all apply receipts before recording global completion and issuing a completion certificate. Participants then release locks and issue release receipts.
6. The coordinator records cleanup after all release receipts. The SQL COMMIT response follows successful completion.

Each receipt is an HMAC over the transaction ID, protocol stage, and sorted participant identities. It is issued after the corresponding local commit has returned. A gateway cannot invent a missing prepare or application receipt. Storage nodes and the shared HMAC key remain trusted; this is crash-fault tolerance, not Byzantine fault tolerance. The development key must be replaced consistently on all nodes for a real deployment, and rotation is not implemented.

For the first operation, coordinator creation, local enlistment and the first locks/intents share one synchronous transaction on the same shard. A failure rolls back the entire start, and the enlistment token is returned only after commit. A lost response is not replayed; the recorded coordinator remains discoverable for recovery. Later participants retain the separate durable enlistment step.

Rollback can durably create an ABORT tombstone when an atomic start left no coordinator record. This fences a delayed start with that same identifier, making rollback safe even when the first response was lost or the first operation failed before creating metadata.

## Reads and failure behavior

When the only participant is the coordinator shard, `tx_commit_local` combines the durable decision, all user tuple changes, completion, tombstone and lock cleanup in one synchronous WAL transaction. The storage function checks the durable participant list before entering this path. Repeating it returns the existing decision without applying old writes again. Cross-shard transactions retain the receipt protocol above; independent prepare, apply and release requests run concurrently, preserving sorted certificate order.

Public autocommit point reads retain the direct storage path. They check durable row-write claims and all exclusive table claims in the same confirmed/linearizable transaction as the tuple read. Only temporary OPEN row-write claims are bypassed: their private intents have not changed user tuples, so the previous committed value remains readable. Prepare atomically copies those claims into durable storage with YES, protecting every unfinished committed write. Local finish installs all participant rows and releases its claims atomically. Explicit transaction reads take shared locks and read their own intents; writes and scans keep their existing conflict checks. Raw native SELECT is still available and returns actual committed local tuples; it does not provide a cross-shard snapshot and can see application progress or follower lag.

`make open-point-test` exercises OPEN insert/update/delete, strict transaction conflicts, exclusive table claims, durable preparation, partial application, local commit/abort, replay and reads after acknowledgement. It then kills leaders at four OPEN/prepared/local-commit boundaries and checks incarnation fencing, durable resolution and restored visibility. The bounded point/batch model and its negative controls are described in [verification](verification.md).

The gateway's recovery worker resolves durable decisions, completes all participants, and releases locks. Expired OPEN records are durably aborted. Participant tombstones reject delayed operations after cleanup; replaying an old apply receipt cannot overwrite a later transaction. Recovery work is bounded to four concurrent transactions per gateway.

Scans hold shared table read locks to protect predicates and absent rows. Point writers hold intention-write table locks and exclusive row locks. Multiple scans can coexist, as can writers to different keys. A transaction that both scans and writes upgrades to an exclusive combined table lock; retaining both claims prevents another scan from observing values that can change underneath it. Conflicting upgrades fail with `40001`. Scans paginate through primary indexes, merging the transaction's staged insert/update/delete intents.

Only idempotent protocol phases are retried after transport uncertainty or a local transaction conflict. General mutations are not automatically replayed. SQLSTATE `40003` reports a commit whose response/completion needs outcome recovery and includes its transaction ID; applications must not treat this as a known rollback. SQLSTATE `40001` indicates a conflict before a commit decision and permits retrying the whole transaction.

## Optional durable-vote commit

`compose.votes.yaml` enables `CAIRN_VOTE_COMMIT=1`. The default deployment retains the earlier coordinator-record protocol. `make vote-test` exercises the optional path with deterministic native clients, SQL regressions and coordinator failover.

In this mode, the coordinator's prepare atomically freezes its durable participant list. Each participant records an immutable YES when preparing, or an immutable NO when voting to abort. Every YES receipt is required for a commit certificate. A transaction whose home has already voted YES can abort only with a real participant NO; if all votes are YES, it must complete the commit. A lost prepare response therefore cannot be reported as a known rollback when durable votes could establish commit.

The commit certificate derives from replicated YES records, and the completion certificate derives from replicated application receipts. These two certificate steps add no coordinator WAL writes. All participants still apply before any lock is released, and SQL acknowledgement still follows durable release and coordinator cleanup. Cleanup materializes the final coordinator status. Until then its stored record can say OPEN even when durable votes already imply COMMIT; recovery checks the votes rather than treating that status as permission to abort.

Recovery resolves frozen transactions immediately when it finds all YES votes or any NO. It leaves an incomplete, unexpired vote set alone. After the OPEN deadline, it can request an irrevocable NO from a still-open participant. A prepare racing that request either wins with YES or loses to NO; the two votes cannot coexist on one participant. Existing legacy transactions remain recoverable alongside this protocol. It retains the same native-read visibility contract, metadata retention and verification boundaries.

`CAIRN_VOTE_COMMIT=2` adds independent local finish to the vote protocol. A distinct signed capability authorizes one atomic transaction that applies a shard's complete changes and releases that shard's locks after the global decision is irrevocable. Other shards retain their write locks until their own changes are installed; strong SQL reads cannot retrieve their old versions. A distributed query crossing an unfinished shard conflicts and aborts rather than returning a mixed snapshot. Acknowledgement waits for every shard's durable finish receipt and coordinator cleanup. Barrier-mode vote transactions cannot use this capability. Run `make local-finish-test` to exercise this path, including coordinator failure after only one shard has finished.

## Optional volatile OPEN state

`CAIRN_VOTE_COMMIT=3` uses temporary spaces for unprepared locks, intents, participant records and coordinator membership. Run `make volatile-test` before measuring this path. User table tuples still change only during durable commit application. Statement success inside an open transaction does not promise survival of that transaction after a leader failure; lost state must abort rather than silently continue.

Every writable storage incarnation first increments a synchronous, non-wrapping unsigned counter. Transaction IDs append the home incarnation to their UUIDv8, and enlistment tokens bind a participant's immutable incarnation and deadline. A participant cannot be rebound after failover. Used-identity markers prevent reopening a closed or expired transaction within an incarnation. A new incarnation clears temporary spaces while APIs remain fenced; old tokens and home identities then fail their verified incarnation checks.

Commit freezes membership without WAL and returns an authenticated scope containing all participant incarnation bindings. Each prepare atomically copies all staged writes and row/table read claims into synchronous spaces, records YES, and retains the complete scope. Home prepare registers its durable coordinator in that same transaction. Missing, expired or mismatched temporary state records NO; it cannot prepare an empty replacement transaction. Recovery scans participant scopes as well as coordinator records. A remote YES can therefore recover a missing home coordinator, while an unexpired home that still has its open state is left alone until its vote or deadline.

A single participant installs rows, the final outcome and replay tombstone in one synchronous transaction. Distributed transactions use unanimous durable votes followed by independent local finish, and acknowledgement waits for every durable finish and coordinator cleanup. Unfrozen rollback uses temporary used markers without WAL; uncertain local commit rollback also competes on a durable outcome key. Once freeze/prepare may have occurred, rollback resolves votes and can report an already committed outcome. These paths preserve the same `40001` versus `40003` distinction as the durable protocols.

The gateway can supply all authenticated YES receipts directly to each remote finish, avoiding a separate coordinator certificate request. Remote participants finish concurrently. The home finishes last and combines its own row installation, lock release, final outcome and global cleanup in one synchronous transaction. It requires every remote durable release receipt before making those changes. A missing or forged receipt cannot authorize early completion. Existing receipt-based recovery also handles a gateway failure between remote finish and the final home step.

Used markers are retained until a durable tombstone takes over or the storage incarnation changes. At one million temporary used identities, the shard refuses additional identities rather than evicting replay protection. Durable transaction metadata also remains retained; safe garbage collection is pending.

## Current limits and pending work

- The OPEN deadline is a hard transaction duration bound, defaulting to 30 seconds (`CAIRN_TX_TIMEOUT` on storage nodes). Long-lived transaction heartbeats/configuration are pending.
- Transaction metadata and tombstones are retained. Safe garbage collection needs a certificate lifetime/generation protocol before deleting replay protection.
- Transactional DDL, savepoints, and multi-statement simple-query requests are pending. CREATE is not currently rolled back with later statements in an extended-query pipeline.
- One mutation statement can change up to 4,096 rows. Scans are bounded to 100,000 rows and 64 MiB per query, and RETURNING results to 16 MiB. Primary-key moves currently require one matching row.
- Extended-query reads and mutations retain their transaction dependencies until Sync. Flush is not a boundary, and BEGIN promotes the existing implicit transaction without releasing its claims. A standalone point read uses the public fast path only when the immediate complete empty Sync is already buffered; fragmented delivery takes the strict transaction path. Explicit transactions survive Sync.
- Certificate operations, Lua state transitions, cryptography and asynchronous orchestration are not proved end to end by Verus. The executable commit guard and bounded TLA+ protocol models cover explicit parts of the design.
