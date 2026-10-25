---------------------------- MODULE ReadBatch ----------------------------
EXTENDS Integers, FiniteSets, TLC
\* Versions stand for acknowledged writes to two keys. A successful barrier
\* returns at least the committed version at its start (ReplicaEpoch's contract).
\* New invocations must never join an older captured barrier.
Requests == 1..3
Keys == 1..2
VARIABLES versions, state, key, minimum, ticket, issued, active, cutoff,
          snapshot, members, result
vars == <<versions,state,key,minimum,ticket,issued,active,cutoff,snapshot,members,result>>
Init ==
    /\ versions = [k \in Keys |-> 0]
    /\ state = [r \in Requests |-> "new"]
    /\ key = [r \in Requests |-> 1]
    /\ minimum = [r \in Requests |-> 0]
    /\ ticket = [r \in Requests |-> 0]
    /\ issued = 0 /\ active = FALSE /\ cutoff = 0
    /\ snapshot = [k \in Keys |-> 0] /\ members = {}
    /\ result = [r \in Requests |-> -1]
Invoke(request,k) ==
    /\ state[request] = "new"
    /\ state' = [state EXCEPT ![request] = "pending"]
    /\ key' = [key EXCEPT ![request] = k]
    /\ minimum' = [minimum EXCEPT ![request] = versions[k]]
    /\ ticket' = [ticket EXCEPT ![request] = issued+1]
    /\ issued' = issued+1
    /\ UNCHANGED <<versions,active,cutoff,snapshot,members,result>>
Capture ==
    /\ ~active /\ \E request \in Requests : state[request] = "pending"
    /\ active' = TRUE /\ cutoff' = issued /\ snapshot' = versions
    /\ UNCHANGED <<versions,state,key,minimum,ticket,issued,members,result>>
Admit(request) ==
    /\ active /\ state[request] = "pending"
    /\ ticket[request] <= cutoff
    /\ Cardinality(members) < 2
    /\ members' = members \cup {request}
    /\ state' = [state EXCEPT ![request] = "running"]
    /\ UNCHANGED <<versions,key,minimum,ticket,issued,active,cutoff,snapshot,result>>
Complete ==
    /\ active /\ members # {}
    /\ result' = [r \in Requests |-> IF r \in members THEN snapshot[key[r]] ELSE result[r]]
    /\ state' = [r \in Requests |-> IF r \in members THEN "done" ELSE state[r]]
    /\ active' = FALSE /\ members' = {}
    /\ UNCHANGED <<versions,key,minimum,ticket,issued,cutoff,snapshot>>
Abandon ==
    /\ active
    /\ state' = [r \in Requests |-> IF r \in members THEN "pending" ELSE state[r]]
    /\ active' = FALSE /\ members' = {}
    /\ UNCHANGED <<versions,key,minimum,ticket,issued,cutoff,snapshot,result>>
Write(k) ==
    /\ versions[k] < 2
    /\ versions' = [versions EXCEPT ![k] = @+1]
    /\ UNCHANGED <<state,key,minimum,ticket,issued,active,cutoff,snapshot,members,result>>
Next ==
    \/ \E r \in Requests, k \in Keys : Invoke(r,k)
    \/ Capture \/ Complete \/ Abandon
    \/ \E r \in Requests : Admit(r)
    \/ \E k \in Keys : Write(k)
TypeOK ==
    /\ versions \in [Keys -> 0..2] /\ snapshot \in [Keys -> 0..2]
    /\ state \in [Requests -> {"new","pending","running","done"}]
    /\ key \in [Requests -> Keys] /\ minimum \in [Requests -> 0..2]
    /\ ticket \in [Requests -> 0..3] /\ issued \in 0..3 /\ cutoff \in 0..3
    /\ active \in BOOLEAN /\ members \subseteq Requests
    /\ result \in [Requests -> -1..2]
Membership ==
    /\ members = {r \in Requests : state[r] = "running"}
    /\ Cardinality(members) <= 2
    /\ (~active => members = {})
NoStaleResponse ==
    \A r \in Requests : state[r] = "done" =>
        /\ ticket[r] > 0 /\ result[r] >= minimum[r]
        /\ result[r] <= versions[key[r]]
NoPrematureResponse ==
    \A r \in Requests : state[r] # "done" => result[r] = -1
Spec == Init /\ [][Next]_vars
=============================================================================
