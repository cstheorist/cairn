------------------------- MODULE TwoPhaseCommit -------------------------
EXTENDS Integers, FiniteSets, TLC
\* Logical durable services: each storage/coordinator service is a quorum group.
\* Receipts abstract unforgeable authenticators issued AFTER durable commits.
\* Network/replica behavior is modeled separately by ReplicaEpoch.
CONSTANT ParticipantCount
ASSUME ParticipantCount \in Nat /\ ParticipantCount > 0
Participants == 1..ParticipantCount
Services == Participants \cup {0}
Phases == {"absent", "open", "prepared", "applied_commit", "applied_abort", "done_commit", "done_abort"}

VARIABLES online, enlisted, phase, decision, locks, rows, preparedReceipts,
          appliedReceipts, releasedReceipts, complete, cleaned, acknowledged
vars == <<online, enlisted, phase, decision, locks, rows, preparedReceipts,
          appliedReceipts, releasedReceipts, complete, cleaned, acknowledged>>

Init ==
    /\ online = Services /\ enlisted = {}
    /\ phase = [p \in Participants |-> "absent"]
    /\ decision = "open" /\ locks = {}
    /\ rows = [p \in Participants |-> 0]
    /\ preparedReceipts = {} /\ appliedReceipts = {} /\ releasedReceipts = {}
    /\ complete = FALSE /\ cleaned = FALSE /\ acknowledged = FALSE

Enlist(p) ==
    /\ 0 \in online /\ decision = "open"
    /\ enlisted' = enlisted \cup {p}
    /\ UNCHANGED <<online, phase, decision, locks, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, cleaned, acknowledged>>
Start(p) ==
    \* Atomic composition of coordinator creation/enlistment and first operation.
    /\ 0 \in online /\ p \in online /\ decision = "open"
    /\ enlisted = {} /\ phase[p] = "absent"
    /\ enlisted' = {p}
    /\ phase' = [phase EXCEPT ![p] = "open"] /\ locks' = locks \cup {p}
    /\ UNCHANGED <<online, decision, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, cleaned, acknowledged>>
Stage(p) ==
    /\ p \in online /\ p \in enlisted
    /\ phase[p] \in {"absent", "open"}
    /\ phase' = [phase EXCEPT ![p] = "open"] /\ locks' = locks \cup {p}
    /\ UNCHANGED <<online, enlisted, decision, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, cleaned, acknowledged>>
Prepare(p) ==
    /\ p \in online /\ phase[p] = "open"
    /\ phase' = [phase EXCEPT ![p] = "prepared"]
    /\ preparedReceipts' = preparedReceipts \cup {p}
    /\ UNCHANGED <<online, enlisted, decision, locks, rows, appliedReceipts,
                   releasedReceipts, complete, cleaned, acknowledged>>
DecideCommit ==
    /\ 0 \in online /\ decision = "open"
    /\ enlisted # {} /\ preparedReceipts = enlisted
    /\ decision' = "commit"
    /\ UNCHANGED <<online, enlisted, phase, locks, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, cleaned, acknowledged>>
DecideAbort ==
    /\ 0 \in online /\ decision = "open"
    /\ decision' = "abort"
    /\ UNCHANGED <<online, enlisted, phase, locks, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, cleaned, acknowledged>>
LocalCommit(p) ==
    \* Only a co-located coordinator and sole participant can combine these
    \* durable changes in one synchronous WAL transaction.
    /\ 0 \in online /\ p \in online /\ decision = "open"
    /\ enlisted = {p} /\ phase[p] = "open"
    /\ phase' = [phase EXCEPT ![p] = "done_commit"]
    /\ decision' = "commit" /\ rows' = [rows EXCEPT ![p] = 1]
    /\ locks' = locks \ {p}
    /\ preparedReceipts' = {p} /\ appliedReceipts' = {p} /\ releasedReceipts' = {p}
    /\ complete' = TRUE /\ cleaned' = TRUE
    /\ UNCHANGED <<online, enlisted, acknowledged>>
ApplyCommit(p) ==
    /\ p \in online /\ decision = "commit" /\ phase[p] = "prepared"
    /\ phase' = [phase EXCEPT ![p] = "applied_commit"]
    /\ rows' = [rows EXCEPT ![p] = 1]
    /\ appliedReceipts' = appliedReceipts \cup {p}
    /\ UNCHANGED <<online, enlisted, decision, locks, preparedReceipts,
                   releasedReceipts, complete, cleaned, acknowledged>>
ApplyAbort(p) ==
    /\ p \in online /\ p \in enlisted /\ decision = "abort"
    /\ phase[p] \in {"absent", "open", "prepared"}
    /\ phase' = [phase EXCEPT ![p] = "applied_abort"]
    /\ appliedReceipts' = appliedReceipts \cup {p}
    /\ UNCHANGED <<online, enlisted, decision, locks, rows, preparedReceipts,
                   releasedReceipts, complete, cleaned, acknowledged>>
Complete ==
    /\ 0 \in online /\ decision # "open" /\ appliedReceipts = enlisted
    /\ complete' = TRUE
    /\ UNCHANGED <<online, enlisted, phase, decision, locks, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, cleaned, acknowledged>>
Release(p) ==
    /\ p \in online /\ complete
    /\ phase[p] \in {"applied_commit", "applied_abort"}
    /\ phase' = [phase EXCEPT ![p] = IF decision = "commit" THEN "done_commit" ELSE "done_abort"]
    /\ locks' = locks \ {p} /\ releasedReceipts' = releasedReceipts \cup {p}
    /\ UNCHANGED <<online, enlisted, decision, rows, preparedReceipts,
                   appliedReceipts, complete, cleaned, acknowledged>>
Clean ==
    /\ 0 \in online /\ complete /\ releasedReceipts = enlisted
    /\ cleaned' = TRUE
    /\ UNCHANGED <<online, enlisted, phase, decision, locks, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, acknowledged>>
Acknowledge ==
    /\ decision = "commit" /\ cleaned
    /\ acknowledged' = TRUE
    /\ UNCHANGED <<online, enlisted, phase, decision, locks, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, cleaned>>
ToggleService(service) ==
    /\ online' = IF service \in online THEN online \ {service} ELSE online \cup {service}
    /\ UNCHANGED <<enlisted, phase, decision, locks, rows, preparedReceipts,
                   appliedReceipts, releasedReceipts, complete, cleaned, acknowledged>>

Next ==
    \/ \E p \in Participants : Enlist(p) \/ Start(p) \/ Stage(p) \/ Prepare(p) \/ ApplyCommit(p) \/ ApplyAbort(p) \/ Release(p) \/ LocalCommit(p)
    \/ DecideCommit \/ DecideAbort \/ Complete \/ Clean \/ Acknowledge
    \/ \E service \in Services : ToggleService(service)
TypeOK ==
    /\ online \subseteq Services /\ enlisted \subseteq Participants
    /\ phase \in [Participants -> Phases] /\ decision \in {"open", "commit", "abort"}
    /\ locks \subseteq Participants /\ rows \in [Participants -> {0, 1}]
    /\ preparedReceipts \subseteq Participants /\ appliedReceipts \subseteq Participants
    /\ releasedReceipts \subseteq Participants
    /\ complete \in BOOLEAN /\ cleaned \in BOOLEAN /\ acknowledged \in BOOLEAN
CommitRequiresPrepare == decision = "commit" => enlisted # {} /\ preparedReceipts = enlisted
NoEarlyCompletion == complete => appliedReceipts = enlisted
NoEarlyRelease == releasedReceipts # {} => complete /\ appliedReceipts = enlisted
NoPartialVisibleRows == decision # "open" =>
    \A p, q \in enlisted \ locks : rows[p] = rows[q]
AbortKeepsOriginalRows == decision = "abort" => \A p \in Participants : rows[p] = 0
AcknowledgedCommitIsApplied == acknowledged =>
    /\ decision = "commit" /\ cleaned /\ locks = {}
    /\ \A p \in enlisted : rows[p] = 1
TombstonesFenceLateOperations == \A p \in releasedReceipts : phase[p] \in {"done_commit", "done_abort"}

Spec == Init /\ [][Next]_vars
=============================================================================
