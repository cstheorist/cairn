--------------------------- MODULE LocalFinish ---------------------------
EXTENDS Integers, FiniteSets, TLC
\* Composition contract for point transactions: an irrevocable global decision
\* precedes independent atomic application/unlock on each storage participant.
\* VoteCommit supplies decision safety; ReplicaEpoch supplies durable storage.
Transactions == {1,2}
Keys == {1,2}
Other(t) == 3-t
VARIABLES online, phase, locks, intents, observed, logicalRows, rows,
          consistent, applied, acknowledged
vars == <<online,phase,locks,intents,observed,logicalRows,rows,
          consistent,applied,acknowledged>>
Init ==
    /\ online = Keys
    /\ phase = [t \in Transactions |-> "active"]
    /\ locks = [t \in Transactions |-> [k \in Keys |-> "none"]]
    /\ intents = [t \in Transactions |-> {}]
    /\ observed = [t \in Transactions |-> [k \in Keys |-> -1]]
    /\ logicalRows = [k \in Keys |-> 0] /\ rows = logicalRows
    /\ consistent = [t \in Transactions |-> TRUE]
    /\ applied = [t \in Transactions |-> {}]
    /\ acknowledged = {}
Read(t,k) ==
    /\ phase[t] = "active" /\ k \in online
    /\ locks[Other(t)][k] # "write"
    /\ locks' = [locks EXCEPT ![t][k] = IF @ = "write" THEN @ ELSE "read"]
    /\ observed' = IF k \in intents[t] THEN observed
        ELSE [observed EXCEPT ![t][k] = rows[k]]
    /\ UNCHANGED <<online,phase,intents,logicalRows,rows,consistent,applied,acknowledged>>
Write(t,k) ==
    /\ phase[t] = "active" /\ k \in online
    /\ locks[Other(t)][k] = "none"
    /\ locks' = [locks EXCEPT ![t][k] = "write"]
    /\ intents' = [intents EXCEPT ![t] = @ \cup {k}]
    /\ UNCHANGED <<online,phase,observed,logicalRows,rows,consistent,applied,acknowledged>>
Commit(t) ==
    /\ phase[t] = "active"
    /\ phase' = [phase EXCEPT ![t] = "committed"]
    \* Record, rather than assume, agreement with the logical commit order.
    /\ consistent' = [consistent EXCEPT ![t] =
        \A k \in Keys : observed[t][k] = -1 \/ observed[t][k] = logicalRows[k]]
    /\ logicalRows' = [k \in Keys |-> IF k \in intents[t] THEN t ELSE logicalRows[k]]
    /\ UNCHANGED <<online,locks,intents,observed,rows,applied,acknowledged>>
Abort(t) ==
    /\ phase[t] = "active"
    /\ phase' = [phase EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<online,locks,intents,observed,logicalRows,rows,consistent,applied,acknowledged>>
Finish(t,k) ==
    /\ k \in online /\ phase[t] \in {"committed","aborted"}
    /\ locks[t][k] # "none"
    /\ rows' = IF phase[t] = "committed" /\ k \in intents[t]
        THEN [rows EXCEPT ![k] = t] ELSE rows
    /\ applied' = IF phase[t] = "committed" /\ k \in intents[t]
        THEN [applied EXCEPT ![t] = @ \cup {k}] ELSE applied
    /\ locks' = [locks EXCEPT ![t][k] = "none"]
    /\ UNCHANGED <<online,phase,intents,observed,logicalRows,consistent,acknowledged>>
Acknowledge(t) ==
    /\ phase[t] = "committed"
    /\ \A k \in Keys : locks[t][k] = "none"
    /\ acknowledged' = acknowledged \cup {t}
    /\ UNCHANGED <<online,phase,locks,intents,observed,logicalRows,rows,consistent,applied>>
Toggle(k) ==
    /\ online' = IF k \in online THEN online \ {k} ELSE online \cup {k}
    /\ UNCHANGED <<phase,locks,intents,observed,logicalRows,rows,consistent,applied,acknowledged>>
Next ==
    \/ \E t \in Transactions,k \in Keys : Read(t,k) \/ Write(t,k) \/ Finish(t,k)
    \/ \E t \in Transactions : Commit(t) \/ Abort(t) \/ Acknowledge(t)
    \/ \E k \in Keys : Toggle(k)
TypeOK ==
    /\ online \subseteq Keys
    /\ phase \in [Transactions -> {"active","committed","aborted"}]
    /\ locks \in [Transactions -> [Keys -> {"none","read","write"}]]
    /\ intents \in [Transactions -> SUBSET Keys]
    /\ observed \in [Transactions -> [Keys -> {-1,0,1,2}]]
    /\ logicalRows \in [Keys -> {0,1,2}] /\ rows \in [Keys -> {0,1,2}]
    /\ consistent \in [Transactions -> BOOLEAN]
    /\ applied \in [Transactions -> SUBSET Keys]
    /\ acknowledged \subseteq Transactions
CompatibleLocks ==
    \A t \in Transactions,k \in Keys :
        locks[t][k] = "write" => locks[Other(t)][k] = "none"
ReadsMatchLogicalCommitOrder ==
    \A t \in Transactions : phase[t] = "committed" => consistent[t]
UnlockedRowsMatchLogicalState ==
    \A k \in Keys : (\A t \in Transactions : locks[t][k] # "write") =>
        rows[k] = logicalRows[k]
AcknowledgedWritesWereApplied ==
    \A t \in acknowledged :
        /\ phase[t] = "committed" /\ applied[t] = intents[t]
        /\ \A k \in Keys : locks[t][k] = "none"
PreparedWritesRemainLocked ==
    \A t \in Transactions : \A k \in intents[t] \ applied[t] :
        phase[t] # "aborted" => locks[t][k] = "write"
Spec == Init /\ [][Next]_vars
=============================================================================
