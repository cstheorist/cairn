--------------------------- MODULE VolatileOpen ---------------------------
EXTENDS Integers, FiniteSets, TLC
\* Optional volatile OPEN protocol: unprepared state is temporary. Durable prepare
\* copies it before voting YES. Transaction IDs bind to the home incarnation;
\* enlistment tokens bind each participant incarnation and cannot be rebound.
\* Scopes are authenticated objects; history records issuance, not server RAM.
Parts == {1,2}
Home == 1
Keys == {1,2}
Epochs == 0..1
VARIABLES incarnation, idEpoch, used, rootOpen, enlisted, binding, frozen,
          present, tokenEpoch, temporaryWrites, acknowledgedWrites,
          scopeMembers, scopeEpoch, scopeHistory, clientHasScope, storedScope,
          vote, durableWrites, coordinatorRecorded, decisions, userAborted
vars == <<incarnation,idEpoch,used,rootOpen,enlisted,binding,frozen,
          present,tokenEpoch,temporaryWrites,acknowledgedWrites,
          scopeMembers,scopeEpoch,scopeHistory,clientHasScope,storedScope,
          vote,durableWrites,coordinatorRecorded,decisions,userAborted>>
Init ==
    /\ incarnation = [p \in Parts |-> 0] /\ idEpoch = -1
    /\ used = [p \in Parts |-> FALSE] /\ rootOpen = FALSE
    /\ enlisted = {} /\ binding = [p \in Parts |-> -1] /\ frozen = FALSE
    /\ present = [p \in Parts |-> FALSE] /\ tokenEpoch = [p \in Parts |-> -1]
    /\ temporaryWrites = [p \in Parts |-> {}]
    /\ acknowledgedWrites = [p \in Parts |-> {}]
    /\ scopeMembers = {} /\ scopeEpoch = [p \in Parts |-> -1]
    /\ scopeHistory = {} /\ clientHasScope = FALSE
    /\ storedScope = [p \in Parts |-> FALSE]
    /\ vote = [p \in Parts |-> "none"] /\ durableWrites = [p \in Parts |-> {}]
    /\ coordinatorRecorded = FALSE /\ decisions = {} /\ userAborted = FALSE
Begin ==
    /\ ~used[Home] /\ ~coordinatorRecorded
    /\ idEpoch = -1 \/ idEpoch = incarnation[Home]
    /\ idEpoch' = IF idEpoch = -1 THEN incarnation[Home] ELSE idEpoch
    /\ used' = [used EXCEPT ![Home] = TRUE] /\ rootOpen' = TRUE
    /\ enlisted' = {Home} /\ binding' = [binding EXCEPT ![Home] = incarnation[Home]]
    /\ present' = [present EXCEPT ![Home] = TRUE]
    /\ tokenEpoch' = [tokenEpoch EXCEPT ![Home] = incarnation[Home]]
    /\ UNCHANGED <<incarnation,frozen,temporaryWrites,acknowledgedWrites,
        scopeMembers,scopeEpoch,scopeHistory,clientHasScope,storedScope,
        vote,durableWrites,coordinatorRecorded,decisions,userAborted>>
Enlist(p) ==
    /\ rootOpen /\ ~frozen /\ p # Home
    /\ p \notin enlisted
    /\ enlisted' = enlisted \cup {p}
    /\ binding' = [binding EXCEPT ![p] = incarnation[p]]
    /\ UNCHANGED <<incarnation,idEpoch,used,rootOpen,frozen,present,tokenEpoch,
        temporaryWrites,acknowledgedWrites,scopeMembers,scopeEpoch,scopeHistory,
        clientHasScope,storedScope,vote,durableWrites,coordinatorRecorded,decisions,userAborted>>
Join(p) ==
    /\ p # Home /\ p \in enlisted /\ ~used[p] /\ vote[p] = "none"
    /\ binding[p] = incarnation[p]
    /\ used' = [used EXCEPT ![p] = TRUE]
    /\ present' = [present EXCEPT ![p] = TRUE]
    /\ tokenEpoch' = [tokenEpoch EXCEPT ![p] = incarnation[p]]
    /\ UNCHANGED <<incarnation,idEpoch,rootOpen,enlisted,binding,frozen,
        temporaryWrites,acknowledgedWrites,scopeMembers,scopeEpoch,scopeHistory,
        clientHasScope,storedScope,vote,durableWrites,coordinatorRecorded,decisions,userAborted>>
Stage(p,k) ==
    /\ present[p] /\ tokenEpoch[p] = incarnation[p] /\ vote[p] = "none"
    /\ temporaryWrites' = [temporaryWrites EXCEPT ![p] = @ \cup {k}]
    /\ acknowledgedWrites' = [acknowledgedWrites EXCEPT ![p] = @ \cup {k}]
    /\ UNCHANGED <<incarnation,idEpoch,used,rootOpen,enlisted,binding,frozen,present,tokenEpoch,
        scopeMembers,scopeEpoch,scopeHistory,clientHasScope,storedScope,
        vote,durableWrites,coordinatorRecorded,decisions,userAborted>>
Freeze ==
    /\ rootOpen /\ ~frozen /\ idEpoch = incarnation[Home]
    /\ frozen' = TRUE /\ scopeMembers' = enlisted /\ scopeEpoch' = binding
    /\ scopeHistory' = scopeHistory \cup {<<enlisted,binding>>}
    /\ clientHasScope' = TRUE
    /\ UNCHANGED <<incarnation,idEpoch,used,rootOpen,enlisted,binding,present,tokenEpoch,
        temporaryWrites,acknowledgedWrites,storedScope,vote,durableWrites,
        coordinatorRecorded,decisions,userAborted>>
ScopeAvailable == scopeMembers # {} /\ (clientHasScope \/ (rootOpen /\ frozen) \/ \E p \in Parts : storedScope[p])
ValidOpen(p) == present[p] /\ tokenEpoch[p] = incarnation[p] /\ scopeEpoch[p] = incarnation[p]
Prepare(p) ==
    /\ ScopeAvailable /\ p \in scopeMembers /\ vote[p] = "none"
    /\ vote' = [vote EXCEPT ![p] = IF ValidOpen(p) THEN "yes" ELSE "no"]
    /\ durableWrites' = [durableWrites EXCEPT ![p] = IF ValidOpen(p) THEN temporaryWrites[p] ELSE {}]
    /\ storedScope' = [storedScope EXCEPT ![p] = TRUE]
    /\ coordinatorRecorded' = (coordinatorRecorded \/ p = Home)
    /\ present' = [present EXCEPT ![p] = FALSE]
    /\ temporaryWrites' = [temporaryWrites EXCEPT ![p] = {}]
    /\ rootOpen' = IF p = Home THEN FALSE ELSE rootOpen
    /\ UNCHANGED <<incarnation,idEpoch,used,enlisted,binding,frozen,tokenEpoch,
        acknowledgedWrites,scopeMembers,scopeEpoch,scopeHistory,clientHasScope,
        decisions,userAborted>>
RecoverMissingHome ==
    /\ ScopeAvailable /\ ~coordinatorRecorded /\ vote[Home] = "none"
    /\ vote' = [vote EXCEPT ![Home] = "no"]
    /\ storedScope' = [storedScope EXCEPT ![Home] = TRUE]
    /\ coordinatorRecorded' = TRUE /\ rootOpen' = FALSE
    /\ present' = [present EXCEPT ![Home] = FALSE]
    /\ temporaryWrites' = [temporaryWrites EXCEPT ![Home] = {}]
    /\ UNCHANGED <<incarnation,idEpoch,used,enlisted,binding,frozen,tokenEpoch,
        acknowledgedWrites,scopeMembers,scopeEpoch,scopeHistory,clientHasScope,
        durableWrites,decisions,userAborted>>
Expire(p) ==
    /\ present[p] /\ vote[p] = "none"
    /\ present' = [present EXCEPT ![p] = FALSE]
    /\ temporaryWrites' = [temporaryWrites EXCEPT ![p] = {}]
    /\ rootOpen' = IF p = Home THEN FALSE ELSE rootOpen
    /\ UNCHANGED <<incarnation,idEpoch,used,enlisted,binding,frozen,tokenEpoch,
        acknowledgedWrites,scopeMembers,scopeEpoch,scopeHistory,clientHasScope,
        storedScope,vote,durableWrites,coordinatorRecorded,decisions,userAborted>>
Restart(p) ==
    /\ incarnation[p] < 1
    /\ incarnation' = [incarnation EXCEPT ![p] = @+1]
    /\ used' = [used EXCEPT ![p] = FALSE]
    /\ present' = [present EXCEPT ![p] = FALSE]
    /\ temporaryWrites' = [temporaryWrites EXCEPT ![p] = {}]
    /\ rootOpen' = IF p = Home THEN FALSE ELSE rootOpen
    /\ frozen' = IF p = Home THEN FALSE ELSE frozen
    /\ UNCHANGED <<idEpoch,enlisted,binding,tokenEpoch,acknowledgedWrites,
        scopeMembers,scopeEpoch,scopeHistory,clientHasScope,storedScope,
        vote,durableWrites,coordinatorRecorded,decisions,userAborted>>
LoseClient ==
    /\ clientHasScope /\ clientHasScope' = FALSE
    /\ UNCHANGED <<incarnation,idEpoch,used,rootOpen,enlisted,binding,frozen,
        present,tokenEpoch,temporaryWrites,acknowledgedWrites,scopeMembers,
        scopeEpoch,scopeHistory,storedScope,vote,durableWrites,
        coordinatorRecorded,decisions,userAborted>>
AbortUnprepared ==
    /\ rootOpen /\ scopeHistory = {}
    /\ userAborted' = TRUE /\ rootOpen' = FALSE
    /\ present' = [p \in Parts |-> FALSE]
    /\ temporaryWrites' = [p \in Parts |-> {}]
    /\ UNCHANGED <<incarnation,idEpoch,used,enlisted,binding,frozen,tokenEpoch,
        acknowledgedWrites,scopeMembers,scopeEpoch,scopeHistory,clientHasScope,
        storedScope,vote,durableWrites,coordinatorRecorded,decisions>>
IssueCommit ==
    /\ ScopeAvailable /\ coordinatorRecorded
    /\ \A p \in scopeMembers : vote[p] = "yes"
    /\ decisions' = decisions \cup {"commit"}
    /\ UNCHANGED <<incarnation,idEpoch,used,rootOpen,enlisted,binding,frozen,
        present,tokenEpoch,temporaryWrites,acknowledgedWrites,scopeMembers,
        scopeEpoch,scopeHistory,clientHasScope,storedScope,vote,durableWrites,
        coordinatorRecorded,userAborted>>
IssueAbort ==
    /\ ScopeAvailable /\ coordinatorRecorded
    /\ \E p \in scopeMembers : vote[p] = "no"
    /\ decisions' = decisions \cup {"abort"}
    /\ UNCHANGED <<incarnation,idEpoch,used,rootOpen,enlisted,binding,frozen,
        present,tokenEpoch,temporaryWrites,acknowledgedWrites,scopeMembers,
        scopeEpoch,scopeHistory,clientHasScope,storedScope,vote,durableWrites,
        coordinatorRecorded,userAborted>>
Next ==
    \/ Begin \/ Freeze \/ LoseClient \/ AbortUnprepared \/ RecoverMissingHome
    \/ IssueCommit \/ IssueAbort
    \/ \E p \in Parts : Enlist(p) \/ Join(p) \/ Prepare(p) \/ Expire(p) \/ Restart(p)
    \/ \E p \in Parts,k \in Keys : Stage(p,k)
TypeOK ==
    /\ incarnation \in [Parts -> Epochs] /\ idEpoch \in {-1,0,1}
    /\ used \in [Parts -> BOOLEAN] /\ rootOpen \in BOOLEAN
    /\ enlisted \subseteq Parts /\ binding \in [Parts -> {-1,0,1}] /\ frozen \in BOOLEAN
    /\ present \in [Parts -> BOOLEAN] /\ tokenEpoch \in [Parts -> {-1,0,1}]
    /\ temporaryWrites \in [Parts -> SUBSET Keys] /\ acknowledgedWrites \in [Parts -> SUBSET Keys]
    /\ scopeMembers \subseteq Parts /\ scopeEpoch \in [Parts -> {-1,0,1}]
    /\ scopeHistory \subseteq (SUBSET Parts) \X [Parts -> {-1,0,1}]
    /\ clientHasScope \in BOOLEAN /\ storedScope \in [Parts -> BOOLEAN]
    /\ vote \in [Parts -> {"none","yes","no"}] /\ durableWrites \in [Parts -> SUBSET Keys]
    /\ coordinatorRecorded \in BOOLEAN /\ decisions \subseteq {"commit","abort"} /\ userAborted \in BOOLEAN
ActiveHomeIncarnation == rootOpen => idEpoch = incarnation[Home]
UniqueScope == Cardinality(scopeHistory) <= 1
PreparedKeepsAcknowledgedWrites ==
    \A p \in Parts : vote[p] = "yes" => acknowledgedWrites[p] \subseteq durableWrites[p]
PreparedHomeHasCoordinator == vote[Home] = "yes" => coordinatorRecorded
UniqueDecision == Cardinality(decisions) <= 1
CommitKeepsAcknowledgedWrites == "commit" \in decisions =>
    \A p \in scopeMembers : vote[p] = "yes" /\ acknowledgedWrites[p] \subseteq durableWrites[p]
NoCommitAfterUnpreparedAbort == userAborted => "commit" \notin decisions
Spec == Init /\ [][Next]_vars
=============================================================================
