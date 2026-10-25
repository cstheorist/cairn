--------------------------- MODULE ShardStore ---------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

\* Bounded safety model of static routing and the storage commit boundary.
\* Atomic, fsync-backed Tarantool commit is an ASSUMPTION, not proved here.
\* Keys are normalized unsigned keys; signed normalization is checked in Rust/Lua.
CONSTANTS ShardCount, OperationCount, NoRow
ASSUME /\ ShardCount \in Nat /\ ShardCount > 0
       /\ OperationCount \in Nat /\ OperationCount > 0
Shards == 0..(ShardCount - 1)
Keys == {0, 1}
Operations == 1..OperationCount
Owner(k) == k % ShardCount
OpKey(op) == IF op = 1 THEN 0 ELSE 1
OpRow(op) == <<op, op * 10>>
Rows == {NoRow} \cup {OpRow(op) : op \in Operations}
Phases == {"new", "sent", "stored", "acknowledged", "unknown"}

VARIABLES online, disk, memory, wal, phase, acknowledgements
vars == <<online, disk, memory, wal, phase, acknowledgements>>

Init ==
    /\ online = [s \in Shards |-> TRUE]
    /\ disk = [s \in Shards |-> [k \in Keys |-> NoRow]]
    /\ memory = disk
    /\ wal = [s \in Shards |-> <<>>]
    /\ phase = [op \in Operations |-> "new"]
    /\ acknowledgements = {}

Send(op) ==
    /\ phase[op] = "new"
    /\ phase' = [phase EXCEPT ![op] = "sent"]
    /\ UNCHANGED <<online, disk, memory, wal, acknowledgements>>

Commit(op) ==
    LET shard == Owner(OpKey(op)) IN
    /\ phase[op] = "sent"
    /\ online[shard]
    /\ disk' = [disk EXCEPT ![shard][OpKey(op)] = OpRow(op)]
    /\ memory' = [memory EXCEPT ![shard][OpKey(op)] = OpRow(op)]
    /\ wal' = [wal EXCEPT ![shard] = Append(@, op)]
    /\ phase' = [phase EXCEPT ![op] = "stored"]
    /\ UNCHANGED <<online, acknowledgements>>

Acknowledge(op) ==
    /\ phase[op] = "stored"
    /\ online[Owner(OpKey(op))]
    /\ phase' = [phase EXCEPT ![op] = "acknowledged"]
    /\ acknowledgements' = acknowledgements \cup {op}
    /\ UNCHANGED <<online, disk, memory, wal>>

LoseResponse(op) ==
    /\ phase[op] \in {"sent", "stored"}
    /\ phase' = [phase EXCEPT ![op] = "unknown"]
    /\ UNCHANGED <<online, disk, memory, wal, acknowledgements>>

Crash(shard) ==
    /\ online[shard]
    /\ online' = [online EXCEPT ![shard] = FALSE]
    /\ memory' = [memory EXCEPT ![shard] = [k \in Keys |-> NoRow]]
    /\ phase' = [op \in Operations |->
        IF Owner(OpKey(op)) = shard /\ phase[op] \in {"sent", "stored"}
        THEN "unknown" ELSE phase[op]]
    /\ UNCHANGED <<disk, wal, acknowledgements>>

Restart(shard) ==
    /\ ~online[shard]
    /\ online' = [online EXCEPT ![shard] = TRUE]
    /\ memory' = [memory EXCEPT ![shard] = disk[shard]]
    /\ UNCHANGED <<disk, wal, phase, acknowledgements>>

Next ==
    \/ \E op \in Operations : Send(op) \/ Commit(op) \/ Acknowledge(op) \/ LoseResponse(op)
    \/ \E shard \in Shards : Crash(shard) \/ Restart(shard)

TypeOK ==
    /\ online \in [Shards -> BOOLEAN]
    /\ disk \in [Shards -> [Keys -> Rows]]
    /\ memory \in [Shards -> [Keys -> Rows]]
    /\ wal \in [Shards -> Seq(Operations)]
    /\ phase \in [Operations -> Phases]
    /\ acknowledgements \subseteq Operations

NoMisplacedRows == \A s \in Shards, k \in Keys : s # Owner(k) => disk[s][k] = NoRow
NoFalseAcknowledgement == \A op \in acknowledgements :
    \E index \in 1..Len(wal[Owner(OpKey(op))]) : wal[Owner(OpKey(op))][index] = op
RecoveredView == \A s \in Shards : online[s] => memory[s] = disk[s]
LatestCommittedRow == \A s \in Shards, k \in Keys :
    LET indices == {i \in 1..Len(wal[s]) : OpKey(wal[s][i]) = k} IN
    IF indices = {} THEN disk[s][k] = NoRow
    ELSE \E last \in indices :
        /\ \A i \in indices : i <= last
        /\ disk[s][k] = OpRow(wal[s][last])

Spec == Init /\ [][Next]_vars
=============================================================================
