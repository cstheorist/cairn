-------------------------- MODULE PredicateLocks --------------------------
EXTENDS Integers, FiniteSets, TLC
\* Two transactions, two keys (an absent row is value zero). Scans hold a
\* table read lock; writers hold intention-exclusive and exclusive row locks.
\* Commit/apply/release is atomic here; TwoPhaseCommit checks its refinement
\* contract separately. No transaction drops locks before it finishes.
Transactions == {1, 2}
Keys == {1, 2}
Other(t) == 3 - t
VARIABLES phase, mode, locks, intents, observed, rows
vars == <<phase, mode, locks, intents, observed, rows>>

Init ==
    /\ phase = [t \in Transactions |-> "active"]
    /\ mode = [t \in Transactions |-> "none"]
    /\ locks = [t \in Transactions |-> [k \in Keys |-> "none"]]
    /\ intents = [t \in Transactions |-> {}]
    /\ observed = [t \in Transactions |-> [k \in Keys |-> -1]]
    /\ rows = [k \in Keys |-> 0]

Read(t, k) ==
    /\ phase[t] = "active"
    /\ locks[Other(t)][k] # "write"
    /\ locks' = [locks EXCEPT ![t][k] = IF @ = "write" THEN @ ELSE "read"]
    /\ observed' = IF k \in intents[t] THEN observed
        ELSE [observed EXCEPT ![t][k] = rows[k]]
    /\ UNCHANGED <<phase, mode, intents, rows>>
Scan(t) ==
    /\ phase[t] = "active"
    /\ mode[Other(t)] \in {"none","scan"}
    /\ mode[t] \in {"none","scan"} \/ mode[Other(t)] = "none"
    /\ mode' = [mode EXCEPT ![t] = IF @ \in {"write","scan_write"} THEN "scan_write" ELSE "scan"]
    /\ observed' = [observed EXCEPT ![t] =
        [k \in Keys |-> IF k \in intents[t] THEN observed[t][k] ELSE rows[k]]]
    /\ UNCHANGED <<phase, locks, intents, rows>>
Write(t, k) ==
    /\ phase[t] = "active"
    /\ mode[Other(t)] \in {"none","write"}
    /\ mode[t] \in {"none","write"} \/ mode[Other(t)] = "none"
    /\ locks[Other(t)][k] = "none"
    /\ mode' = [mode EXCEPT ![t] = IF @ \in {"scan","scan_write"} THEN "scan_write" ELSE "write"]
    /\ locks' = [locks EXCEPT ![t][k] = "write"]
    /\ intents' = [intents EXCEPT ![t] = @ \cup {k}]
    /\ UNCHANGED <<phase, observed, rows>>
Finish(t, commit) ==
    /\ phase[t] = "active"
    /\ phase' = [phase EXCEPT ![t] = IF commit THEN "committed" ELSE "aborted"]
    /\ rows' = IF commit THEN [k \in Keys |-> IF k \in intents[t] THEN t ELSE rows[k]] ELSE rows
    /\ mode' = [mode EXCEPT ![t] = "none"]
    /\ locks' = [locks EXCEPT ![t] = [k \in Keys |-> "none"]]
    /\ UNCHANGED <<intents, observed>>

Next ==
    \/ \E t \in Transactions, k \in Keys : Read(t,k) \/ Write(t,k)
    \/ \E t \in Transactions : Scan(t) \/ Finish(t,TRUE) \/ Finish(t,FALSE)
TypeOK ==
    /\ phase \in [Transactions -> {"active","committed","aborted"}]
    /\ mode \in [Transactions -> {"none","scan","write","scan_write"}]
    /\ locks \in [Transactions -> [Keys -> {"none","read","write"}]]
    /\ intents \in [Transactions -> SUBSET Keys]
    /\ observed \in [Transactions -> [Keys -> {-1,0,1,2}]]
    /\ rows \in [Keys -> {0,1,2}]
CompatibleTableLocks ==
    \A t \in Transactions :
        /\ (mode[t] = "scan" => mode[Other(t)] \in {"none","scan"})
        /\ (mode[t] = "scan_write" => mode[Other(t)] = "none")
CompatibleRowLocks ==
    \A t \in Transactions, k \in Keys :
        locks[t][k] = "write" => locks[Other(t)][k] = "none"
RepeatableReadsAndNoPhantoms ==
    \A t \in Transactions, k \in Keys :
        phase[t] = "active" /\ observed[t][k] # -1 => rows[k] = observed[t][k]
ReadsMatchOrder(first, second) ==
    /\ \A k \in Keys : observed[first][k] \in {-1,0}
    /\ \A k \in Keys : observed[second][k] \in
        {-1, IF k \in intents[first] THEN first ELSE 0}
SerializableCommittedHistory ==
    (\A t \in Transactions : phase[t] = "committed") =>
        ReadsMatchOrder(1,2) \/ ReadsMatchOrder(2,1)
NoDirtyReads ==
    \A t \in Transactions, k \in Keys :
        observed[t][k] = Other(t) => phase[Other(t)] = "committed"
Spec == Init /\ [][Next]_vars
=============================================================================
