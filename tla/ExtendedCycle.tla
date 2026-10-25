---------------------------- MODULE ExtendedCycle ----------------------------
EXTENDS Integers, FiniteSets, TLC
\* Bounded extended-query dependency contract, not a PostgreSQL wire parser.
\* Client A reads key 1, then writes key 2. Client B reads key 2, then writes
\* key 1. A Sync between A's operations makes them separate transactions.
\* Flush does not. BEGIN promotes the same implicit transaction to explicit;
\* an explicit transaction survives Sync. Commit/application is atomic here,
\* relying on the separately checked storage/locking protocol contracts.
\* Transaction 1 is A's first cycle, 2 is B, and 3 is A after an implicit Sync.
Transactions == {1,2,3}
Keys == {1,2}
Views == {"sync","sync_tail","partial_sync","missing","flush","execute","begin","malformed"}
Successors == {"Sync","Flush","Write","Begin","BadSync"}
KnownSyncViews == {"sync","sync_tail"}
Allowed(view,next) ==
    CASE view \in {"sync","sync_tail","partial_sync"} -> next = "Sync"
      [] view = "missing" -> next \in Successors
      [] view = "flush" -> next = "Flush"
      [] view = "execute" -> next = "Write"
      [] view = "begin" -> next = "Begin"
      [] OTHER -> next = "BadSync"
VARIABLES a, bpc, phase, locks, seen, writes, rows, abortedHistory
vars == <<a,bpc,phase,locks,seen,writes,rows,abortedHistory>>
FastEligible == a.mode = "none" /\ a.prefix \in KnownSyncViews
OtherWrites(t,k) == \E other \in Transactions \ {t} : locks[other][k] = "write"
OtherClaims(t,k) == \E other \in Transactions \ {t} : locks[other][k] # "none"
Clear(t) == [locks EXCEPT ![t] = [k \in Keys |-> "none"]]
Apply(t) == [k \in Keys |-> IF k \in writes[t] THEN t ELSE rows[k]]
Committed == {t \in Transactions : phase[t] = "committed"}
\* Each key has at most one committed writer in this bounded program. Thus
\* read-from and initial-version anti-dependencies describe its complete graph.
Dependency(from,to) == from # to /\
    ((\E k \in Keys : seen[to][k] = from /\ k \in writes[from])
     \/ (\E k \in Keys : seen[from][k] = 0 /\ k \in writes[to])
     \/ (from = 1 /\ to = 3))
Init ==
    /\ a = [pc |-> "choose", prefix |-> "unset", next |-> "unset",
        mode |-> "none", owner |-> 1, fast |-> FALSE, readMode |-> "none",
        claimed |-> FALSE, flushed |-> FALSE, promoted |-> FALSE,
        synced |-> FALSE, ended |-> FALSE, wroteAfterSync |-> FALSE]
    /\ bpc = "read"
    /\ phase = [t \in Transactions |-> IF t = 3 THEN "unused" ELSE "active"]
    /\ locks = [t \in Transactions |-> [k \in Keys |-> "none"]]
    /\ seen = [t \in Transactions |-> [k \in Keys |-> -1]]
    /\ writes = [t \in Transactions |-> {}]
    /\ rows = [k \in Keys |-> 0] /\ abortedHistory = {}
Choose(view,next) ==
    /\ a.pc = "choose" /\ Allowed(view,next)
    /\ a' = [a EXCEPT !.pc = "read", !.prefix = view, !.next = next]
    /\ UNCHANGED <<bpc,phase,locks,seen,writes,rows,abortedHistory>>
StartExplicit ==
    /\ a.pc = "read" /\ a.mode = "none"
    /\ a' = [a EXCEPT !.mode = "explicit"]
    /\ UNCHANGED <<bpc,phase,locks,seen,writes,rows,abortedHistory>>
AbortA(nextPc) ==
    /\ a' = [a EXCEPT !.pc = nextPc, !.mode = IF @ = "none" THEN "implicit" ELSE @]
    /\ phase' = [phase EXCEPT ![a.owner] = "aborted"]
    /\ locks' = Clear(a.owner) /\ abortedHistory' = abortedHistory \cup {a.owner}
    /\ UNCHANGED <<bpc,seen,writes,rows>>
ReadA ==
    /\ a.pc = "read"
    /\ IF ~FastEligible /\ OtherWrites(1,1) THEN AbortA("next") ELSE
        /\ a' = [a EXCEPT !.pc = "next", !.fast = FastEligible,
            !.readMode = a.mode, !.claimed = ~FastEligible,
            !.mode = IF FastEligible THEN "none" ELSE IF @ = "none" THEN "implicit" ELSE @]
        /\ seen' = [seen EXCEPT ![1][1] = rows[1]]
        /\ locks' = IF FastEligible THEN locks ELSE [locks EXCEPT ![1][1] = "read"]
        /\ UNCHANGED <<bpc,phase,writes,rows,abortedHistory>>
FlushA ==
    /\ a.pc = "next" /\ a.next = "Flush"
    /\ a' = [a EXCEPT !.pc = "continuation", !.flushed = TRUE]
    /\ UNCHANGED <<bpc,phase,locks,seen,writes,rows,abortedHistory>> \* Flush retains claims
PromoteBegin ==
    /\ (a.pc = "next" /\ a.next = "Begin") \/
        (a.pc = "continuation" /\ a.flushed /\ ~a.promoted)
    /\ a.mode = "implicit" /\ phase[1] = "active"
    /\ a' = [a EXCEPT !.pc = "continuation", !.mode = "explicit", !.promoted = TRUE]
    /\ UNCHANGED <<bpc,phase,locks,seen,writes,rows,abortedHistory>> \* BEGIN adopts claims
RejectBegin ==
    /\ (a.pc = "next" /\ a.next = "Begin") \/
        (a.pc = "continuation" /\ a.flushed /\ ~a.promoted)
    /\ a.mode # "implicit" \/ phase[1] # "active"
    /\ AbortA("written")
ReadSyncImplicit ==
    /\ a.pc = "next" /\ a.next = "Sync" /\ a.mode # "explicit"
    /\ a' = [a EXCEPT !.pc = "after_sync", !.mode = "none", !.owner = 3, !.ended = TRUE]
    /\ phase' = [phase EXCEPT ![1] = IF @ = "active" THEN "committed" ELSE @]
    /\ locks' = Clear(1)
    /\ UNCHANGED <<bpc,seen,writes,rows,abortedHistory>>
SyncExplicit ==
    /\ a.mode = "explicit" /\ ~a.synced
    /\ (a.pc = "next" /\ a.next = "Sync") \/ a.pc \in {"continuation","written"}
    /\ a' = [a EXCEPT !.pc = IF @ = "next" THEN "continuation" ELSE @, !.synced = TRUE]
    /\ UNCHANGED <<bpc,phase,locks,seen,writes,rows,abortedHistory>> \* explicit Sync retains claims
BadSync ==
    /\ a.pc = "next" /\ a.next = "BadSync"
    /\ AbortA("written")
WriteA ==
    /\ a.pc \in {"continuation","after_sync"} \/ (a.pc = "next" /\ a.next = "Write")
    /\ phase[a.owner] \in {"active","unused"}
    /\ IF OtherClaims(a.owner,2) THEN AbortA("written") ELSE
        /\ a' = [a EXCEPT !.pc = "written", !.mode = IF @ = "none" THEN "implicit" ELSE @,
            !.wroteAfterSync = a.synced]
        /\ phase' = [phase EXCEPT ![a.owner] = "active"]
        /\ locks' = [locks EXCEPT ![a.owner][2] = "write"]
        /\ writes' = [writes EXCEPT ![a.owner] = @ \cup {2}]
        /\ UNCHANGED <<bpc,seen,rows,abortedHistory>>
EndA ==
    /\ a.pc = "written"
    /\ a' = [a EXCEPT !.pc = "done", !.mode = "none"]
    /\ phase' = [phase EXCEPT ![a.owner] = IF @ = "active" THEN "committed" ELSE @]
    /\ rows' = IF phase[a.owner] = "active" THEN Apply(a.owner) ELSE rows
    /\ locks' = Clear(a.owner)
    /\ UNCHANGED <<bpc,seen,writes,abortedHistory>>
\* EndA abstracts Sync for implicit state, COMMIT/ROLLBACK for explicit state.
\* SyncExplicit itself never commits, releases, or changes transaction identity.
AbortB ==
    /\ phase' = [phase EXCEPT ![2] = "aborted"] /\ locks' = Clear(2)
    /\ abortedHistory' = abortedHistory \cup {2} /\ bpc' = "done"
    /\ UNCHANGED <<a,seen,writes,rows>>
ReadB ==
    /\ bpc = "read"
    /\ IF OtherWrites(2,2) THEN AbortB ELSE
        /\ bpc' = "write" /\ seen' = [seen EXCEPT ![2][2] = rows[2]]
        /\ locks' = [locks EXCEPT ![2][2] = "read"]
        /\ UNCHANGED <<a,phase,writes,rows,abortedHistory>>
WriteB ==
    /\ bpc = "write"
    /\ IF OtherClaims(2,1) THEN AbortB ELSE
        /\ bpc' = "commit" /\ writes' = [writes EXCEPT ![2] = {1}]
        /\ locks' = [locks EXCEPT ![2][1] = "write"]
        /\ UNCHANGED <<a,phase,seen,rows,abortedHistory>>
CommitB ==
    /\ bpc = "commit" /\ bpc' = "done"
    /\ phase' = [phase EXCEPT ![2] = "committed"]
    /\ rows' = Apply(2) /\ locks' = Clear(2)
    /\ UNCHANGED <<a,seen,writes,abortedHistory>>
Next ==
    \/ \E view \in Views,next \in Successors : Choose(view,next)
    \/ StartExplicit \/ ReadA \/ FlushA \/ PromoteBegin \/ RejectBegin
    \/ ReadSyncImplicit \/ SyncExplicit \/ BadSync \/ WriteA \/ EndA
    \/ ReadB \/ WriteB \/ CommitB
TypeOK ==
    /\ a \in [pc : {"choose","read","next","continuation","written","after_sync","done"},
        prefix : Views \cup {"unset"}, next : Successors \cup {"unset"},
        mode : {"none","implicit","explicit"}, owner : {1,3}, fast : BOOLEAN,
        readMode : {"none","implicit","explicit"}, claimed : BOOLEAN,
        flushed : BOOLEAN, promoted : BOOLEAN, synced : BOOLEAN, ended : BOOLEAN, wroteAfterSync : BOOLEAN]
    /\ bpc \in {"read","write","commit","done"}
    /\ phase \in [Transactions -> {"unused","active","committed","aborted"}]
    /\ locks \in [Transactions -> [Keys -> {"none","read","write"}]]
    /\ seen \in [Transactions -> [Keys -> {-1,0,1,2,3}]]
    /\ writes \in [Transactions -> SUBSET Keys] /\ rows \in [Keys -> {0,1,2,3}]
    /\ abortedHistory \subseteq Transactions
CompatibleClaims == \A t \in Transactions,k \in Keys :
    locks[t][k] = "write" => ~OtherClaims(t,k)
DependencyClaimHeld == a.claimed /\ phase[1] = "active" => locks[1][1] = "read"
FastReadRequiresKnownSync == a.fast =>
    a.prefix \in {"sync","sync_tail"} /\ a.readMode = "none" /\ a.next = "Sync"
FreshOwnerAfterReadSync == a.ended => a.owner = 3
ExplicitScopeThroughSync == a.synced /\ a.mode = "explicit" /\ phase[1] = "active" =>
    a.owner = 1 /\ ~a.ended
NoFailedRevival == \A t \in abortedHistory : phase[t] = "aborted"
OneCommittedWriterPerKey == \A k \in Keys :
    Cardinality({t \in Committed : k \in writes[t]}) <= 1
CommittedHistoriesSerializable ==
    \E rank \in [Committed -> 1..3] :
        \A from,to \in Committed : Dependency(from,to) => rank[from] < rank[to]
Spec == Init /\ [][Next]_vars
=============================================================================
