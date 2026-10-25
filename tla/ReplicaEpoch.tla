-------------------------- MODULE ReplicaEpoch --------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
\* Abstract quorum/epoch contract relied upon by the gateway, not a model of
\* Tarantool's full Raft implementation. Commit is an atomic quorum operation;
\* in-flight uncommitted WAL records and membership changes are out of scope.
CONSTANTS NodeCount, OperationCount, MaxEpoch
ASSUME /\ NodeCount \in Nat /\ NodeCount > 0
       /\ OperationCount \in Nat /\ OperationCount > 0
       /\ MaxEpoch \in Nat /\ MaxEpoch > 0
Nodes == 1..NodeCount
Operations == 1..OperationCount
Quorums == {q \in SUBSET Nodes : Cardinality(q) > NodeCount \div 2}
Edges == {<<a, b>> : a \in Nodes, b \in Nodes} \cap
         {edge \in Nodes \X Nodes : edge[1] < edge[2]}

VARIABLES online, links, epoch, elected, promised, votedFor, leaderEpoch,
          logs, acknowledged, unsafeCommit, unsafeRead
vars == <<online, links, epoch, elected, promised, votedFor, leaderEpoch,
          logs, acknowledged, unsafeCommit, unsafeRead>>

Elements(sequence) == {sequence[i] : i \in 1..Len(sequence)}
Prefix(a, b) == Len(a) <= Len(b) /\ \A i \in 1..Len(a) : a[i] = b[i]
Connected(a, b) == a = b \/
    (IF a < b THEN <<a, b>> \in links ELSE <<b, a>> \in links)
Reachable(node, quorum) == quorum \subseteq online /\
    \A peer \in quorum : Connected(node, peer)
CanServe(node, requestEpoch, quorum) ==
    /\ node \in online /\ node \in quorum
    /\ leaderEpoch[node] = requestEpoch /\ requestEpoch > 0
    /\ Reachable(node, quorum)
    /\ \A peer \in quorum : promised[peer] = requestEpoch /\ votedFor[peer] = node

Init ==
    /\ online = Nodes /\ links = Edges
    /\ epoch = 0 /\ elected = 0
    /\ promised = [n \in Nodes |-> 0]
    /\ votedFor = [n \in Nodes |-> 0]
    /\ leaderEpoch = [n \in Nodes |-> 0]
    /\ logs = [n \in Nodes |-> <<>>]
    /\ acknowledged = {}
    /\ unsafeCommit = FALSE /\ unsafeRead = FALSE

Elect(node, quorum) ==
    /\ epoch < MaxEpoch /\ node \in online /\ node \in quorum
    /\ Reachable(node, quorum)
    /\ \A peer \in quorum : Len(logs[node]) >= Len(logs[peer])
    /\ epoch' = epoch + 1 /\ elected' = node
    /\ promised' = [n \in Nodes |-> IF n \in quorum THEN epoch + 1 ELSE promised[n]]
    /\ votedFor' = [n \in Nodes |-> IF n \in quorum THEN node ELSE votedFor[n]]
    /\ leaderEpoch' = [leaderEpoch EXCEPT ![node] = epoch + 1]
    /\ UNCHANGED <<online, links, logs, acknowledged, unsafeCommit, unsafeRead>>

Commit(node, requestEpoch, op, quorum) ==
    /\ CanServe(node, requestEpoch, quorum)
    /\ op \notin acknowledged
    /\ logs' = [n \in Nodes |->
         IF n \in quorum THEN Append(logs[node], op) ELSE logs[n]]
    /\ acknowledged' = acknowledged \cup {op}
    /\ unsafeCommit' = unsafeCommit \/ requestEpoch # epoch
    /\ UNCHANGED <<online, links, epoch, elected, promised, votedFor, leaderEpoch, unsafeRead>>

StrongRead(node, requestEpoch, quorum) ==
    /\ CanServe(node, requestEpoch, quorum)
    /\ unsafeRead' = unsafeRead \/ requestEpoch # epoch \/
        ~acknowledged \subseteq Elements(logs[node])
    /\ UNCHANGED <<online, links, epoch, elected, promised, votedFor, leaderEpoch,
                   logs, acknowledged, unsafeCommit>>

CatchUp(node, peer) ==
    /\ node \in online /\ peer \in online /\ Connected(node, peer)
    /\ Prefix(logs[node], logs[peer])
    /\ logs' = [logs EXCEPT ![node] = logs[peer]]
    /\ UNCHANGED <<online, links, epoch, elected, promised, votedFor, leaderEpoch,
                   acknowledged, unsafeCommit, unsafeRead>>

Crash(node) ==
    /\ node \in online /\ online' = online \ {node}
    /\ UNCHANGED <<links, epoch, elected, promised, votedFor, leaderEpoch,
                   logs, acknowledged, unsafeCommit, unsafeRead>>
Restart(node) ==
    /\ node \notin online /\ online' = online \cup {node}
    /\ UNCHANGED <<links, epoch, elected, promised, votedFor, leaderEpoch,
                   logs, acknowledged, unsafeCommit, unsafeRead>>
Toggle(edge) ==
    /\ links' = IF edge \in links THEN links \ {edge} ELSE links \cup {edge}
    /\ UNCHANGED <<online, epoch, elected, promised, votedFor, leaderEpoch,
                   logs, acknowledged, unsafeCommit, unsafeRead>>

Next ==
    \/ \E node \in Nodes, quorum \in Quorums : Elect(node, quorum)
    \/ \E node \in Nodes, term \in 1..MaxEpoch, op \in Operations, quorum \in Quorums : Commit(node, term, op, quorum)
    \/ \E node \in Nodes, term \in 1..MaxEpoch, quorum \in Quorums : StrongRead(node, term, quorum)
    \/ \E node \in Nodes, peer \in Nodes : CatchUp(node, peer)
    \/ \E node \in Nodes : Crash(node) \/ Restart(node)
    \/ \E edge \in Edges : Toggle(edge)

TypeOK ==
    /\ online \subseteq Nodes /\ links \subseteq Edges
    /\ epoch \in 0..MaxEpoch /\ elected \in 0..NodeCount
    /\ promised \in [Nodes -> 0..MaxEpoch]
    /\ votedFor \in [Nodes -> 0..NodeCount]
    /\ leaderEpoch \in [Nodes -> 0..MaxEpoch]
    /\ logs \in [Nodes -> Seq(Operations)]
    /\ acknowledged \subseteq Operations
    /\ unsafeCommit \in BOOLEAN /\ unsafeRead \in BOOLEAN
LogAgreement == \A a, b \in Nodes : Prefix(logs[a], logs[b]) \/ Prefix(logs[b], logs[a])
DurableAcknowledgement == \A op \in acknowledged :
    Cardinality({node \in Nodes : op \in Elements(logs[node])}) > NodeCount \div 2
LeaderCompleteness == epoch > 0 => acknowledged \subseteq Elements(logs[elected])
NoStaleCommit == ~unsafeCommit
NoStaleStrongRead == ~unsafeRead

Spec == Init /\ [][Next]_vars
=============================================================================
