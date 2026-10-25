--------------------------- MODULE OpenPointRead ---------------------------
EXTENDS Integers, FiniteSets, Sequences, TLC
\* Companion to LocalFinish and ReadBatch, not a refinement of either model.
\* Public point readers bypass only temporary OPEN row-write claims. Explicit
\* transaction reads/writes still respect every row claim and table-X claim.
\* Prepare atomically copies all local claims with YES; logical commit requires
\* every participant's YES. Finish atomically installs local rows and unlocks.
\* A public batch captures one coherent durable-claim/native-row MVCC view.
\* durable labels row claims; tableX is a continuous blocking abstraction of
\* the OPEN/durable table-X handoff. Predicate-lock acquisition is not proved.
\* ReplicaEpoch supplies the barrier and VolatileOpen supplies identity/recovery
\* fencing. Crashes, authenticators, SQL values and predicate scans are abstract.
CONSTANT BatchMode
Transactions == IF BatchMode THEN {1} ELSE {1,2}
Keys == IF BatchMode THEN {1,2,3} ELSE {1,2}
Shards == {1,2}
Requests == IF BatchMode THEN {1,2} ELSE {1}
MaxBatch == IF BatchMode THEN 2 ELSE 1
Owner(k) == IF BatchMode THEN (IF k = 3 THEN 2 ELSE 1) ELSE k
Values == {0} \cup Transactions
VARIABLES phase, locks, tableX, intents, participants, yes, durable,
          rows, logicalRows, history, commitAt, consistent, applied, acknowledged,
          state, key, ticket, issued, invokeCut, minimum, result, responseCut,
          cohort, memberCut, batchPhase, batchShard, cutoff, members,
          viewRows, viewBlocked, collected, observed
vars == <<phase,locks,tableX,intents,participants,yes,durable,
          rows,logicalRows,history,commitAt,consistent,applied,acknowledged,
          state,key,ticket,issued,invokeCut,minimum,result,responseCut,
          cohort,memberCut,batchPhase,batchShard,cutoff,members,
          viewRows,viewBlocked,collected,observed>>
Claimed(t,p) == {k \in Keys : Owner(k) = p /\ locks[t][k] # "none"}
Parts(t) == {Owner(k) : k \in {q \in Keys : locks[t][q] # "none"}} \cup tableX[t]
OtherClaims(t,k) ==
    \E other \in Transactions \ {t} : locks[other][k] # "none"
OtherWrites(t,k) ==
    \E other \in Transactions \ {t} : locks[other][k] = "write"
OtherTableX(t,p) == \E other \in Transactions \ {t} : p \in tableX[other]
DurableWriter(k) == \E t \in Transactions :
    k \in durable[t] /\ locks[t][k] = "write"
TableWriter(k) == \E t \in Transactions : Owner(k) \in tableX[t]
Blocked(k) == DurableWriter(k) \/ TableWriter(k)
Version(value) == IF value = 0 THEN 1 ELSE commitAt[value]
AckVersions(k) == {1} \cup {commitAt[t] : t \in {other \in acknowledged : k \in intents[other]}}
ObservedVersions(k) == {1} \cup {Version(result[r]) : r \in
    {q \in Requests : state[q] = "done" /\ result[q] \in Transactions /\ k \in intents[result[q]]}}
Maximum(s) == CHOOSE n \in s : \A m \in s : n >= m
SnapshotAfter(t) == [k \in Keys |-> IF k \in intents[t] THEN t ELSE logicalRows[k]]
ReadsConsistent(t) ==
    \A k \in Keys : observed[t][k] = -1 \/ observed[t][k] = logicalRows[k]
\* observed is separate from public reader results: only strict transaction
\* reads participate in the writer's serialization-order check.
Init ==
    /\ phase = [t \in Transactions |-> "active"]
    /\ locks = [t \in Transactions |-> [k \in Keys |-> "none"]]
    /\ tableX = [t \in Transactions |-> {}]
    /\ intents = [t \in Transactions |-> {}]
    /\ participants = [t \in Transactions |-> {}]
    /\ yes = [t \in Transactions |-> {}] /\ durable = [t \in Transactions |-> {}]
    /\ rows = [k \in Keys |-> 0] /\ logicalRows = rows /\ history = <<rows>>
    /\ commitAt = [t \in Transactions |-> 0]
    /\ observed = [t \in Transactions |-> [k \in Keys |-> -1]]
    /\ consistent = [t \in Transactions |-> TRUE]
    /\ applied = [t \in Transactions |-> {}] /\ acknowledged = {}
    /\ state = [r \in Requests |-> "new"] /\ key = [r \in Requests |-> 1]
    /\ ticket = [r \in Requests |-> 0] /\ issued = 0
    /\ invokeCut = [r \in Requests |-> 0]
    /\ minimum = [r \in Requests |-> [ack |-> 1, seen |-> 1]]
    /\ result = [r \in Requests |-> -1] /\ responseCut = [r \in Requests |-> 0]
    /\ cohort = [r \in Requests |-> {}] /\ memberCut = [r \in Requests |-> 0]
    /\ batchPhase = "idle" /\ batchShard = 1 /\ cutoff = 0 /\ members = {}
    /\ viewRows = rows /\ viewBlocked = {} /\ collected = [r \in Requests |-> -1]
StrictRead(t,k) ==
    /\ phase[t] = "active" /\ observed[t][k] = -1 /\ k \notin intents[t]
    /\ ~OtherWrites(t,k) /\ ~OtherTableX(t,Owner(k))
    /\ locks' = [locks EXCEPT ![t][k] = IF @ = "write" THEN @ ELSE "read"]
    /\ observed' = [observed EXCEPT ![t][k] = rows[k]]
    /\ UNCHANGED <<phase,tableX,intents,participants,yes,durable,rows,logicalRows,
        history,commitAt,consistent,applied,acknowledged,state,key,ticket,issued,
        invokeCut,minimum,result,responseCut,cohort,memberCut,batchPhase,
        batchShard,cutoff,members,viewRows,viewBlocked,collected>>
Write(t,k) ==
    /\ phase[t] = "active" /\ k \notin intents[t]
    /\ ~OtherClaims(t,k) /\ ~OtherTableX(t,Owner(k))
    /\ locks' = [locks EXCEPT ![t][k] = "write"]
    /\ intents' = [intents EXCEPT ![t] = @ \cup {k}]
    /\ UNCHANGED <<phase,tableX,participants,yes,durable,rows,logicalRows,history,
        commitAt,observed,consistent,applied,acknowledged,state,key,ticket,issued,
        invokeCut,minimum,result,responseCut,cohort,memberCut,batchPhase,
        batchShard,cutoff,members,viewRows,viewBlocked,collected>>
ClaimTableX(t,p) ==
    /\ phase[t] = "active" /\ p \notin tableX[t]
    /\ \E k \in intents[t] : Owner(k) = p
    /\ ~OtherTableX(t,p)
    /\ \A k \in Keys : Owner(k) = p => ~OtherClaims(t,k)
    /\ tableX' = [tableX EXCEPT ![t] = @ \cup {p}]
    /\ UNCHANGED <<phase,locks,intents,participants,yes,durable,rows,logicalRows,
        history,commitAt,observed,consistent,applied,acknowledged,state,key,
        ticket,issued,invokeCut,minimum,result,responseCut,cohort,memberCut,
        batchPhase,batchShard,cutoff,members,viewRows,viewBlocked,collected>>
Freeze(t) ==
    /\ phase[t] = "active" /\ intents[t] # {}
    /\ phase' = [phase EXCEPT ![t] = "frozen"]
    /\ participants' = [participants EXCEPT ![t] = Parts(t)]
    /\ UNCHANGED <<locks,tableX,intents,yes,durable,rows,logicalRows,history,
        commitAt,observed,consistent,applied,acknowledged,state,key,ticket,issued,
        invokeCut,minimum,result,responseCut,cohort,memberCut,batchPhase,
        batchShard,cutoff,members,viewRows,viewBlocked,collected>>
Prepare(t,p) ==
    /\ phase[t] = "frozen" /\ p \in participants[t] \ yes[t]
    /\ durable' = [durable EXCEPT ![t] = @ \cup Claimed(t,p)]
    /\ yes' = [yes EXCEPT ![t] = @ \cup {p}]
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,rows,logicalRows,
        history,commitAt,observed,consistent,applied,acknowledged,state,key,
        ticket,issued,invokeCut,minimum,result,responseCut,cohort,memberCut,
        batchPhase,batchShard,cutoff,members,viewRows,viewBlocked,collected>>
Commit(t) ==
    /\ phase[t] = "frozen" /\ yes[t] = participants[t]
    /\ phase' = [phase EXCEPT ![t] = "committed"]
    /\ consistent' = [consistent EXCEPT ![t] = ReadsConsistent(t)]
    /\ logicalRows' = SnapshotAfter(t) /\ history' = Append(history,SnapshotAfter(t))
    /\ commitAt' = [commitAt EXCEPT ![t] = Len(history)+1]
    /\ UNCHANGED <<locks,tableX,intents,participants,yes,durable,rows,observed,
        applied,acknowledged,state,key,ticket,issued,invokeCut,minimum,result,
        responseCut,cohort,memberCut,batchPhase,batchShard,cutoff,members,
        viewRows,viewBlocked,collected>>
LocalCommit(t) ==
    /\ phase[t] = "active" /\ intents[t] # {} /\ Cardinality(Parts(t)) = 1
    /\ phase' = [phase EXCEPT ![t] = "committed"]
    /\ participants' = [participants EXCEPT ![t] = Parts(t)]
    /\ consistent' = [consistent EXCEPT ![t] = ReadsConsistent(t)]
    /\ rows' = [k \in Keys |-> IF k \in intents[t] THEN t ELSE rows[k]]
    /\ logicalRows' = SnapshotAfter(t)
    /\ history' = Append(history,SnapshotAfter(t))
    /\ commitAt' = [commitAt EXCEPT ![t] = Len(history)+1]
    /\ locks' = [locks EXCEPT ![t] = [k \in Keys |-> "none"]]
    /\ tableX' = [tableX EXCEPT ![t] = {}]
    /\ applied' = [applied EXCEPT ![t] = intents[t]]
    /\ UNCHANGED <<intents,yes,durable,observed,acknowledged,state,key,ticket,
        issued,invokeCut,minimum,result,responseCut,cohort,memberCut,batchPhase,
        batchShard,cutoff,members,viewRows,viewBlocked,collected>>
Abort(t) ==
    /\ phase[t] = "active" \/ (phase[t] = "frozen" /\ yes[t] # participants[t])
    /\ phase' = [phase EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<locks,tableX,intents,participants,yes,durable,rows,logicalRows,
        history,commitAt,observed,consistent,applied,acknowledged,state,key,
        ticket,issued,invokeCut,minimum,result,responseCut,cohort,memberCut,
        batchPhase,batchShard,cutoff,members,viewRows,viewBlocked,collected>>
Finish(t,p) ==
    /\ phase[t] \in {"committed","aborted"}
    /\ Claimed(t,p) # {} \/ p \in tableX[t]
    /\ rows' = IF phase[t] = "committed" THEN
        [k \in Keys |-> IF k \in intents[t] /\ Owner(k) = p THEN t ELSE rows[k]] ELSE rows
    /\ applied' = IF phase[t] = "committed" THEN
        [applied EXCEPT ![t] = @ \cup {k \in intents[t] : Owner(k) = p}] ELSE applied
    /\ locks' = [locks EXCEPT ![t] =
        [k \in Keys |-> IF Owner(k) = p THEN "none" ELSE locks[t][k]]]
    /\ tableX' = [tableX EXCEPT ![t] = @ \ {p}]
    /\ durable' = [durable EXCEPT ![t] = {k \in @ : Owner(k) # p}]
    /\ UNCHANGED <<phase,intents,participants,yes,logicalRows,history,commitAt,
        observed,consistent,acknowledged,state,key,ticket,issued,invokeCut,
        minimum,result,responseCut,cohort,memberCut,batchPhase,batchShard,
        cutoff,members,viewRows,viewBlocked,collected>>
Acknowledge(t) ==
    /\ phase[t] = "committed" /\ t \notin acknowledged
    /\ \A k \in Keys : locks[t][k] = "none"
    /\ acknowledged' = acknowledged \cup {t}
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,state,key,ticket,
        issued,invokeCut,minimum,result,responseCut,cohort,memberCut,batchPhase,
        batchShard,cutoff,members,viewRows,viewBlocked,collected>>
Invoke(r,k) ==
    /\ state[r] = "new"
    /\ state' = [state EXCEPT ![r] = "pending"] /\ key' = [key EXCEPT ![r] = k]
    /\ ticket' = [ticket EXCEPT ![r] = issued+1] /\ issued' = issued+1
    /\ invokeCut' = [invokeCut EXCEPT ![r] = Len(history)]
    /\ minimum' = [minimum EXCEPT ![r] =
        [ack |-> Maximum(AckVersions(k)), seen |-> Maximum(ObservedVersions(k))]]
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,acknowledged,
        result,responseCut,cohort,memberCut,batchPhase,batchShard,cutoff,members,
        viewRows,viewBlocked,collected>>
Capture(p) ==
    /\ batchPhase = "idle" /\ \E r \in Requests : state[r] = "pending" /\ Owner(key[r]) = p
    /\ batchPhase' = "collecting" /\ batchShard' = p /\ cutoff' = issued
    /\ collected' = [r \in Requests |-> -1]
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,acknowledged,
        state,key,ticket,issued,invokeCut,minimum,result,responseCut,cohort,
        memberCut,members,viewRows,viewBlocked>>
Admit(r) ==
    /\ batchPhase = "collecting" /\ ticket[r] <= cutoff
    /\ state[r] = "pending" /\ Owner(key[r]) = batchShard
    /\ Cardinality(members) < MaxBatch
    /\ members' = members \cup {r} /\ state' = [state EXCEPT ![r] = "running"]
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,acknowledged,
        key,ticket,issued,invokeCut,minimum,result,responseCut,cohort,memberCut,
        batchPhase,batchShard,cutoff,viewRows,viewBlocked,collected>>
FreezeBatch ==
    /\ batchPhase = "collecting" /\ members # {}
    /\ batchPhase' = "reading"
    \* Only one shard is observed: recording zero elsewhere does not require
    \* or claim a coherent cross-shard storage snapshot.
    /\ viewRows' = [k \in Keys |-> IF Owner(k) = batchShard THEN rows[k] ELSE 0]
    /\ viewBlocked' = {k \in Keys : Owner(k) = batchShard /\ Blocked(k)}
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,acknowledged,
        state,key,ticket,issued,invokeCut,minimum,result,responseCut,cohort,
        memberCut,batchShard,cutoff,members,collected>>
Collect(r) ==
    /\ batchPhase = "reading" /\ r \in members /\ collected[r] = -1
    /\ collected' = [collected EXCEPT ![r] = viewRows[key[r]]]
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,acknowledged,
        state,key,ticket,issued,invokeCut,minimum,result,responseCut,cohort,
        memberCut,batchPhase,batchShard,cutoff,members,viewRows,viewBlocked>>
Respond ==
    /\ batchPhase = "reading" /\ members # {}
    /\ \A r \in members : key[r] \notin viewBlocked /\ collected[r] # -1
    /\ state' = [r \in Requests |-> IF r \in members THEN "done" ELSE state[r]]
    /\ result' = [r \in Requests |-> IF r \in members THEN collected[r] ELSE result[r]]
    /\ responseCut' = [r \in Requests |-> IF r \in members THEN Len(history) ELSE responseCut[r]]
    /\ cohort' = [r \in Requests |-> IF r \in members THEN members ELSE cohort[r]]
    /\ memberCut' = [r \in Requests |-> IF r \in members THEN cutoff ELSE memberCut[r]]
    /\ batchPhase' = "idle" /\ members' = {}
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,acknowledged,
        key,ticket,issued,invokeCut,minimum,batchShard,cutoff,viewRows,viewBlocked,collected>>
Abandon ==
    /\ batchPhase # "idle"
    /\ state' = [r \in Requests |-> IF r \in members THEN "pending" ELSE state[r]]
    /\ batchPhase' = "idle" /\ members' = {}
    /\ UNCHANGED <<phase,locks,tableX,intents,participants,yes,durable,rows,
        logicalRows,history,commitAt,observed,consistent,applied,acknowledged,
        key,ticket,issued,invokeCut,minimum,result,responseCut,cohort,memberCut,
        batchShard,cutoff,viewRows,viewBlocked,collected>>
Next ==
    \/ \E t \in Transactions,k \in Keys : StrictRead(t,k) \/ Write(t,k)
    \/ \E t \in Transactions,p \in Shards : ClaimTableX(t,p) \/ Prepare(t,p) \/ Finish(t,p)
    \/ \E t \in Transactions : Freeze(t) \/ Commit(t) \/ LocalCommit(t) \/ Abort(t) \/ Acknowledge(t)
    \/ \E r \in Requests,k \in Keys : Invoke(r,k)
    \/ \E p \in Shards : Capture(p)
    \/ \E r \in Requests : Admit(r) \/ Collect(r)
    \/ FreezeBatch \/ Respond \/ Abandon
TypeOK ==
    /\ phase \in [Transactions -> {"active","frozen","committed","aborted"}]
    /\ locks \in [Transactions -> [Keys -> {"none","read","write"}]]
    /\ tableX \in [Transactions -> SUBSET Shards]
    /\ intents \in [Transactions -> SUBSET Keys]
    /\ participants \in [Transactions -> SUBSET Shards] /\ yes \in [Transactions -> SUBSET Shards]
    /\ durable \in [Transactions -> SUBSET Keys]
    /\ rows \in [Keys -> Values] /\ logicalRows \in [Keys -> Values]
    /\ history \in Seq([Keys -> Values]) /\ Len(history) \in 1..(Cardinality(Transactions)+1)
    /\ commitAt \in [Transactions -> 0..(Cardinality(Transactions)+1)]
    /\ observed \in [Transactions -> [Keys -> Values \cup {-1}]]
    /\ consistent \in [Transactions -> BOOLEAN] /\ applied \in [Transactions -> SUBSET Keys]
    /\ acknowledged \subseteq Transactions
    /\ state \in [Requests -> {"new","pending","running","done"}]
    /\ key \in [Requests -> Keys] /\ ticket \in [Requests -> 0..Cardinality(Requests)]
    /\ issued \in 0..Cardinality(Requests) /\ cutoff \in 0..Cardinality(Requests)
    /\ invokeCut \in [Requests -> 0..Len(history)]
    /\ minimum \in [Requests -> [ack : 1..Len(history), seen : 1..Len(history)]]
    /\ result \in [Requests -> Values \cup {-1}]
    /\ responseCut \in [Requests -> 0..Len(history)]
    /\ cohort \in [Requests -> SUBSET Requests] /\ memberCut \in [Requests -> 0..Cardinality(Requests)]
    /\ batchPhase \in {"idle","collecting","reading"} /\ batchShard \in Shards
    /\ members \subseteq Requests /\ viewRows \in [Keys -> Values] /\ viewBlocked \subseteq Keys
    /\ collected \in [Requests -> Values \cup {-1}]
CompatibleClaims == \A t \in Transactions,k \in Keys :
    /\ (locks[t][k] = "write" => ~OtherClaims(t,k))
    /\ (locks[t][k] # "none" => ~OtherTableX(t,Owner(k)))
PreparedClaimsDurable == \A t \in Transactions : \A p \in yes[t] :
    Claimed(t,p) \subseteq durable[t]
DecidedWritesProtected == \A t \in Transactions : \A k \in intents[t] \ applied[t] :
    phase[t] = "committed" => k \in durable[t] /\ locks[t][k] = "write"
UnprotectedRowsMatchLogicalState == \A k \in Keys : ~DurableWriter(k) => rows[k] = logicalRows[k]
StrictReadsMatchCommitOrder == \A t \in Transactions : phase[t] = "committed" => consistent[t]
AcknowledgedWritesApplied == \A t \in acknowledged :
    /\ phase[t] = "committed" /\ applied[t] = intents[t]
    /\ \A k \in Keys : locks[t][k] = "none"
FixedBatchMembership ==
    /\ members = {r \in Requests : state[r] = "running"}
    /\ Cardinality(members) <= MaxBatch
    /\ \A r \in members : Owner(key[r]) = batchShard /\ ticket[r] <= cutoff
    /\ \A r \in Requests : state[r] = "done" =>
        \A other \in cohort[r] : Owner(key[other]) = Owner(key[r]) /\ ticket[other] <= memberCut[r]
CommittedBatchWitness == \A r \in Requests : state[r] = "done" =>
    \E cut \in 1..responseCut[r] : \A other \in cohort[r] :
        invokeCut[other] <= cut /\ result[other] = history[cut][key[other]]
ReadAfterAcknowledgement == \A r \in Requests : state[r] = "done" =>
    Version(result[r]) >= minimum[r].ack
ReadAfterObservedCommit == \A r \in Requests : state[r] = "done" =>
    Version(result[r]) >= minimum[r].seen
NoDirtyPublicRead == \A r \in Requests : state[r] = "done" =>
    result[r] = 0 \/ (commitAt[result[r]] > 0 /\ commitAt[result[r]] <= responseCut[r])
NoPrematureResponse == \A r \in Requests : (state[r] = "done") <=> (result[r] # -1)
Spec == Init /\ [][Next]_vars
=============================================================================
