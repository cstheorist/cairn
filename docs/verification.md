# Verification boundary

## Verus

`crates/cairn-verified/verify.rs` contains fourteen executable functions with checked contracts:

| Function | Contract | Actual use |
| --- | --- | --- |
| `normalize_key` | Result equals mathematical signed key + 2^63, without arithmetic overflow | Every routed SQL key |
| `shard_for_key` | Mathematical modulo of the normalized key; result is below any nonzero shard count | Storage-node selection |
| `valid_frame` | Accepts exactly lengths between four and the configured maximum | Startup and frontend framing |
| `payload_length` | Subtraction cannot underflow under the guard's precondition | Bounded frontend allocation |
| `accepts_topology` | Both ID and count agree, and count is positive | Startup topology checks |
| `accepts_leader` | Writable leader epoch is at least the minimum observed epoch | Replica leader discovery |
| `may_decide_commit` | Every nonempty participant set is prepared and no abort is recorded | Distributed commit decision guard |
| `transaction_home` | Scoped UUIDs name their coordinator; legacy IDs map to shard zero | Coordinator routing |
| `checked_add_i64` | A result equals the mathematical sum; absence means overflow | SQL integer arithmetic |
| `checked_sub_i64` | A result equals the mathematical difference; absence means overflow | SQL integer arithmetic |
| `may_commit_locally` | There is exactly one participant, colocated with its coordinator | Atomic single-shard commit eligibility |
| `valid_read_batch` | A nonempty bounded request batch has exactly as many responses as requests | Batched point-read response validation |
| `next_incarnation` | Successor is exactly one greater; absence means the unsigned counter is exhausted | Durable storage incarnation allocation before becoming writable |
| `may_use_open_state` | State is present and its saved, requested and current incarnations all agree | Volatile participant state and transaction identity fencing |

Executable bodies between marked comments are extracted verbatim into `crates/cairn-verified/src/lib.rs`; there is no separate handwritten routing implementation in Cargo. The generator supplies ordinary Rust signatures and strips away contracts, not executable statements. Both the generator and matching Rust/Verus compilation semantics are trusted. CI checks extraction drift. The caller-side preconditions are established by explicit guards and cluster construction, reviewed and tested, rather than proved for the whole server.

The Lua implementation uses equivalent unsigned normalization for storage-side routing. Cross-language agreement was exercised at the signed 64-bit boundaries, negative keys, zero, and positive keys in the two-shard cluster; the Lua code is not verified by Verus.

`crates/cairn-verified/native` compiles the generated core into a small `no_std` C ABI library loaded by Tarantool. Typed primary-key increment pushdown calls the same verified `checked_add_i64` body used by Rust expressions; Lua checks NULL/column-width constraints and retains the durable lock/intent path. The wrapper, C ABI, LuaJIT FFI and compilation are trusted boundaries. An independent ctypes check compares 10,121 boundary/random sums against Python arbitrary-precision arithmetic on macOS and in the Linux storage deployment. PostgreSQL differential tests check result values, overflow SQLSTATEs, NULLs, missing rows, residual predicates, transaction rollback and contended updates.

There are no `assume`, `admit`, or external-body annotations in the checked Verus source. This does not imply verification of async execution, atomics in the connection pool, parser behavior, authentication, MessagePack encoding, or the PostgreSQL state machine.

## TLA+

`tla/ShardStore.tla` models a fixed shard owner function, finite write requests, complete-row commits, a durable WAL, acknowledgement, response loss, process crashes, and restoration from durable state. A lost response may leave an operation committed and unacknowledged. It deliberately does not introduce a retry transition.

The model checks:

1. `TypeOK`: states remain in their declared sets.
2. `NoMisplacedRows`: a key is never stored on a different shard.
3. `NoFalseAcknowledgement`: each acknowledged operation has a durable WAL entry, even if a later write overwrites its value.
4. `RecoveredView`: each online shard's readable memory agrees with durable storage.
5. `LatestCommittedRow`: a stored row is the last committed complete version of its key.

The checked configuration has two shards, two normalized keys, and three operations, including competing writes to one key. TLC generated 3,159 states and visited 814 distinct states, with search depth 12. Safety passed for that finite configuration.

`ReplicaEpoch.tla` checks the quorum/epoch contract: majority durability, log agreement, leader completeness, stale-epoch commit rejection, and strong-read safety during crashes and link partitions. With three nodes, two operations and two epochs, TLC generated 364,573 states and visited 43,072 distinct states. Commit is abstracted as an atomic quorum operation; in-flight uncommitted WAL entries, full Raft internals, and changing membership are not modeled.

`TwoPhaseCommit.tla` checks enlistment, durable prepare receipts, immutable commit/abort decisions, application, global completion, lock release, acknowledgement, late operations, and service crashes. Atomic first-operation setup composes local enlistment and staging. An atomic single-shard commit transition is permitted only for a sole colocated participant. With two participants, TLC generated 4,473 states and visited 1,056 distinct states. Eight invariants check prepare requirements, receipt ordering, application before release, absence of partial SQL-visible rows, abort behavior, acknowledgement, and tombstones fencing late operations. Authenticators are abstracted as unforgeable receipts issued only after durable transitions.

`PredicateLocks.tla` checks shared/exclusive row locks and table scan, intention-write, and combined scan/write modes. Six invariants cover lock compatibility, repeatable reads, phantom protection, absence of dirty reads, and a serial order for committed histories. TLC generated 18,593 states and visited 3,640 distinct states for two transactions and two keys, with depth 10. The first run found a writer-to-scan upgrade that dropped the writer claim; the corrected Lua lock protocol and a live-client regression test retain both claims with an exclusive combined mode. This model abstracts commit/apply/release as one atomic transition, whose protocol contract is checked separately by `TwoPhaseCommit`; there is no composed refinement proof.

`ReadBatch.tla` checks acknowledged-write freshness and request membership while writes, invocations, capture, completion and abandonment interleave. A request arriving after capture cannot join that barrier. TLC generated 1,777,429 states and visited 602,601 distinct states for three requests, two keys and versions zero through two, with depth 17. Four invariants cover types, membership, read-after-acknowledgement freshness and absence of premature responses. The model assumes the linearizable barrier contract checked separately by `ReplicaEpoch`; it is not a composed refinement proof. The runtime freezes each request vector before its storage RPC starts, validates echoed table/key envelopes, and splits oversized replies using only read-only retries.

These are bounded safety models. There is no fairness/liveness claim, unbounded theorem, complete SQL/schema model, or proof that all Rust/Lua code refines these state machines.

`VoteCommit.tla` covers the optional `CAIRN_VOTE_COMMIT=1` path. The coordinator's durable YES or NO freezes membership. Unanimous immutable YES receipts certify commit; an immutable NO certifies abort. Issuing a certificate does not require another coordinator WAL record. Application receipts establish completion before locks can be released. TLC generated 2,095 states and visited 488 distinct states, with depth 14, for two participants and service crashes. Eleven invariants check vote exclusivity, frozen membership, unique decisions, unanimity, abort evidence, application, release and acknowledgement. This abstracts authentication and quorum storage in the same way as the existing two-phase model.

`LocalFinish.tla` checks the optional `CAIRN_VOTE_COMMIT=2` path's point-transaction contract. After an irrevocable logical decision, each shard atomically installs its rows and releases its local locks. Two interleaved transactions read, stage writes, decide, independently finish keys, acknowledge, and encounter shard outages. Six invariants check lock compatibility, agreement with logical commit order, agreement between unlocked physical rows and logical state, acknowledged application and retention of unfinished write locks. TLC generated 206,785 states and visited 53,904 distinct states, with depth 19. This model assumes decision safety from `VoteCommit`; it covers point transactions rather than a composed proof of the SQL executor, predicate scans or the Rust/Lua implementation.

`VolatileOpen.tla` checks the optional `CAIRN_VOTE_COMMIT=3` path. Transaction identities bind their home incarnation, participant enlistments bind immutable local incarnations, and used markers prevent reopening a lost or closed transaction within an incarnation. Preparation copies all acknowledged staged writes into durable storage before YES. Each durable vote stores an authenticated frozen scope, so remote YES votes can recover a coordinator that had not yet prepared. Home YES and coordinator registration are atomic. Expiry, process restart, client loss, unprepared rollback and decision recovery interleave. TLC generated 35,265 states and visited 7,324 distinct states, with depth 16. Eight invariants cover types, home identity fencing, unique frozen scopes, preservation of acknowledged writes, home registration, unique decisions and aborted transaction fencing. This model abstracts row/table locking and local installation through the separately checked predicate-lock and local-finish contracts; there is no composed refinement proof.

`scripts/check_model_sensitivity.py` checks eighteen isolated temporary mutants: acknowledgement before durable commit, undersized quorums, late admission to a captured read batch, commit without all prepares, local commit with multiple participants, dropping a writer claim on a scan upgrade, changing frozen membership, incomplete YES votes, abort without a NO, releasing barrier-mode locks before completion, allowing reads through write locks, unlocking an active transaction, unlocking without installing rows, accepting an old home incarnation, rebinding an enlisted participant incarnation, reopening a used participant identity, preparing without valid temporary state, and issuing home YES without coordinator registration. Each must violate its corresponding safety invariant. Original models are not modified.

## Trusted storage assumptions and implementation checks

The atomic durable `Commit` transition assumes Tarantool returns from its commit path only after the configured WAL guarantee and exposes complete committed tuples. Tarantool, its C/Lua runtime, filesystem behavior, storage hardware, and the network libraries are not proved here. The model does not establish replication safety or survival of disk loss.

Independent Psycopg and Tarantool Python clients test SQL/native representation agreement, binary parameter/result encoding, duplicate protection, NULLs, updates/deletes, access restrictions, signed-key routing, malformed framing, pipeline error recovery, and exhausted portals. The recovery script SIGKILLs a storage process after successful writes, checks that the unaffected shard is usable, and verifies WAL recovery and incomplete-DDL repair. Tests are evidence about these executions, not proofs of the remaining implementation.

Tools used for the initial check: Verus `0.2026.10.18.7d82994` with Rust `1.99.1`, TLA+ tools fetched from the `v1.8.0` release URL with the SHA-256 recorded in the bootstrap script, Java 21, Tarantool `3.3.5`, and a release gateway build with Rust `1.85.1`. The working native toolchain is isolated under `.tools`; these binary artifacts are not committed.
