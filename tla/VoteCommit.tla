---------------------------- MODULE VoteCommit ----------------------------
EXTENDS Integers, FiniteSets, TLC
\* Optional CAIRN_VOTE_COMMIT protocol; legacy TwoPhaseCommit stays available.
\* Durable YES/NO votes are exclusive. Unanimous YES certifies commit; any NO
\* certifies abort. The coordinator's vote atomically freezes membership.
Participants == {1,2}
Home == 1
Services == {0,1,2}
Phases == {"absent","open","prepared","voted_abort",
           "applied_commit","applied_abort","done_commit","done_abort"}
VARIABLES online, members, frozen, frozenMembers, phase, yes, no,
          certificates, rows, locks, complete, acknowledged
vars == <<online,members,frozen,frozenMembers,phase,yes,no,
          certificates,rows,locks,complete,acknowledged>>
Init ==
    /\ online = Services /\ members = {Home}
    /\ frozen = FALSE /\ frozenMembers = {}
    /\ phase = [p \in Participants |-> IF p = Home THEN "open" ELSE "absent"]
    /\ yes = {} /\ no = {} /\ certificates = {}
    /\ rows = [p \in Participants |-> 0] /\ locks = {Home}
    /\ complete = FALSE /\ acknowledged = FALSE
Enlist(p) ==
    /\ 0 \in online /\ p \in online /\ ~frozen /\ p \notin members
    /\ members' = members \cup {p}
    /\ phase' = [phase EXCEPT ![p] = "open"] /\ locks' = locks \cup {p}
    /\ UNCHANGED <<online,frozen,frozenMembers,yes,no,certificates,rows,complete,acknowledged>>
Prepare(p) ==
    /\ p \in online /\ p \in members /\ phase[p] = "open"
    /\ p \notin no
    /\ phase' = [phase EXCEPT ![p] = "prepared"] /\ yes' = yes \cup {p}
    /\ frozen' = IF p = Home THEN TRUE ELSE frozen
    /\ frozenMembers' = IF p = Home THEN members ELSE frozenMembers
    /\ UNCHANGED <<online,members,no,certificates,rows,locks,complete,acknowledged>>
VoteAbort(p) ==
    /\ p \in online /\ p \in members /\ phase[p] = "open"
    /\ p \notin yes
    /\ p = Home \/ frozen
    /\ phase' = [phase EXCEPT ![p] = "voted_abort"] /\ no' = no \cup {p}
    /\ frozen' = IF p = Home THEN TRUE ELSE frozen
    /\ frozenMembers' = IF p = Home THEN members ELSE frozenMembers
    /\ UNCHANGED <<online,members,yes,certificates,rows,locks,complete,acknowledged>>
IssueCommit ==
    /\ 0 \in online /\ frozen /\ members # {} /\ yes = members
    /\ certificates' = certificates \cup {"commit"}
    /\ UNCHANGED <<online,members,frozen,frozenMembers,phase,yes,no,rows,locks,complete,acknowledged>>
IssueAbort ==
    /\ 0 \in online /\ frozen /\ no # {}
    /\ certificates' = certificates \cup {"abort"}
    /\ UNCHANGED <<online,members,frozen,frozenMembers,phase,yes,no,rows,locks,complete,acknowledged>>
ApplyCommit(p) ==
    /\ p \in online /\ "commit" \in certificates /\ phase[p] = "prepared"
    /\ phase' = [phase EXCEPT ![p] = "applied_commit"]
    /\ rows' = [rows EXCEPT ![p] = 1]
    /\ UNCHANGED <<online,members,frozen,frozenMembers,yes,no,certificates,locks,complete,acknowledged>>
ApplyAbort(p) ==
    /\ p \in online /\ p \in members /\ "abort" \in certificates
    /\ phase[p] \in {"open","prepared","voted_abort"}
    /\ phase' = [phase EXCEPT ![p] = "applied_abort"]
    /\ UNCHANGED <<online,members,frozen,frozenMembers,yes,no,certificates,rows,locks,complete,acknowledged>>
AllApplied ==
    \/ "commit" \in certificates /\ \A p \in members : phase[p] \in {"applied_commit","done_commit"}
    \/ "abort" \in certificates /\ \A p \in members : phase[p] \in {"applied_abort","done_abort"}
Complete ==
    /\ 0 \in online /\ AllApplied /\ complete' = TRUE
    /\ UNCHANGED <<online,members,frozen,frozenMembers,phase,yes,no,certificates,rows,locks,acknowledged>>
Release(p) ==
    /\ p \in online /\ complete
    /\ phase[p] \in {"applied_commit","applied_abort"}
    /\ phase' = [phase EXCEPT ![p] = IF @ = "applied_commit" THEN "done_commit" ELSE "done_abort"]
    /\ locks' = locks \ {p}
    /\ UNCHANGED <<online,members,frozen,frozenMembers,yes,no,certificates,rows,complete,acknowledged>>
Acknowledge ==
    /\ "commit" \in certificates /\ complete
    /\ \A p \in members : phase[p] = "done_commit"
    /\ acknowledged' = TRUE
    /\ UNCHANGED <<online,members,frozen,frozenMembers,phase,yes,no,certificates,rows,locks,complete>>
Toggle(service) ==
    /\ online' = IF service \in online THEN online \ {service} ELSE online \cup {service}
    /\ UNCHANGED <<members,frozen,frozenMembers,phase,yes,no,certificates,rows,locks,complete,acknowledged>>
Next ==
    \/ \E p \in Participants : Enlist(p) \/ Prepare(p) \/ VoteAbort(p) \/ ApplyCommit(p) \/ ApplyAbort(p) \/ Release(p)
    \/ IssueCommit \/ IssueAbort \/ Complete \/ Acknowledge
    \/ \E service \in Services : Toggle(service)
TypeOK ==
    /\ online \subseteq Services /\ members \subseteq Participants /\ Home \in members
    /\ frozen \in BOOLEAN /\ frozenMembers \subseteq Participants
    /\ phase \in [Participants -> Phases] /\ yes \subseteq members /\ no \subseteq members
    /\ certificates \subseteq {"commit","abort"}
    /\ rows \in [Participants -> {0,1}] /\ locks \subseteq members
    /\ complete \in BOOLEAN /\ acknowledged \in BOOLEAN
FrozenMembership == frozen => members = frozenMembers
ExclusiveVotes == yes \intersect no = {}
UniqueDecision == Cardinality(certificates) <= 1
CommitRequiresUnanimity == "commit" \in certificates => frozen /\ yes = members /\ no = {}
AbortRequiresNo == "abort" \in certificates => frozen /\ no # {}
CompletionRequiresApplication == complete => AllApplied
ReleaseRequiresCompletion == (members \ locks) # {} => complete
NoPartialVisibleRows == \A p,q \in members \ locks : rows[p] = rows[q]
AbortPreservesRows == "abort" \in certificates => \A p \in Participants : rows[p] = 0
AcknowledgedCommitIsApplied == acknowledged =>
    /\ certificates = {"commit"} /\ locks = {}
    /\ \A p \in members : phase[p] = "done_commit" /\ rows[p] = 1
Spec == Init /\ [][Next]_vars
=============================================================================
