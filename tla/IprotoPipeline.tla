-------------------------- MODULE IprotoPipeline --------------------------
EXTENDS Integers, FiniteSets, TLC
\* Two non-idempotent calls, two slots, and two captured pool generations.
\* Costs 1/2 and caps 2 calls/3 bytes scale the runtime's count/encoded-byte
\* budgets. Pending includes queued, sent and caller-abandoned calls. Frames
\* carry ghost origin identities so a wrong SYNC association is observable.
CONSTANTS Requests, Connections, NoFrame, MaxCount, MaxBytes
Generations == 1..2
Physical == Connections \X Generations
Syncs == 1..3
Calls == Physical \X Syncs \X Requests \X (1..2)
TerminalKinds == {"ok","db","fence"}
Kinds == {"ok","db","fence","chunk","bad"}

VARIABLES cost, generation, opened, retired, nextSync, phase, attempts, retries,
          pending, issued, sent, executed, answered, frame, matched,
          retryCause, rejections, failures, invalidations
vars == <<cost,generation,opened,retired,nextSync,phase,attempts,retries,
          pending,issued,sent,executed,answered,frame,matched,
          retryCause,rejections,failures,invalidations>>

Bytes(entries) == Cardinality({c \in entries : cost[c[3]] = 1}) +
                 2 * Cardinality({c \in entries : cost[c[3]] = 2})
Entries(p) == {c \in pending : c[1] = p}
NextGeneration(p) == IF p[2] = generation /\ generation < 3 THEN generation+1 ELSE generation
Unanswered(c) == ~\E reply \in answered : reply[1] = c
\* Retirement resolves every still-live waiter, but a cancelled caller gets
\* no result. A matched fence is the sole request allowed a safe retry.
Resolve(entries, target, outcome) == [r \in Requests |->
    IF \E c \in entries : c[3] = r
    THEN IF phase[r] = "abandoned" THEN "cancelled"
         ELSE IF target # NoFrame /\ target[3] = r THEN outcome ELSE "uncertain"
    ELSE phase[r]]

Init ==
    /\ cost \in [Requests -> {1,2}]
    /\ generation = 1 /\ opened = Connections \X {1} /\ retired = {}
    /\ nextSync = [p \in Physical |-> 1]
    /\ phase = [r \in Requests |-> "new"]
    /\ attempts = [r \in Requests |-> 0] /\ retries = [r \in Requests |-> 0]
    /\ pending = {} /\ issued = {} /\ sent = {} /\ executed = {}
    /\ answered = {} /\ frame = NoFrame /\ matched = {}
    /\ retryCause = [r \in Requests |-> "none"]
    /\ rejections = {} /\ failures = {} /\ invalidations = {}

Admit(r,s,p) ==
    /\ p \in opened \ retired /\ p[2] = generation /\ phase[r] = "new" /\ attempts[r] < 2
    /\ s = nextSync[p]
    /\ Cardinality(Entries(p)) < MaxCount
    /\ Bytes(Entries(p)) + cost[r] <= MaxBytes
    /\ LET call == <<p,s,r,attempts[r]+1>> IN
        /\ pending' = pending \cup {call} /\ issued' = issued \cup {call}
    /\ phase' = [phase EXCEPT ![r] = "pending"]
    /\ attempts' = [attempts EXCEPT ![r] = @+1]
    /\ nextSync' = [nextSync EXCEPT ![p] = @+1]
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,retries,sent,executed,answered,
                    frame,matched,retryCause,rejections,failures>>
Reject(r,p) ==
    /\ p \in opened \ retired /\ p[2] = generation /\ phase[r] = "new"
    /\ Cardinality(Entries(p)) >= MaxCount \/
        Bytes(Entries(p)) + cost[r] > MaxBytes
    /\ rejections' = rejections \cup {<<r,attempts[r]+1>>}
    /\ phase' = [phase EXCEPT ![r] = "limit"]
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,attempts,retries,pending,
                    issued,sent,executed,answered,frame,matched,retryCause,failures>>
Send(c) ==
    /\ c \in pending /\ c \notin sent /\ c[1] \notin retired
    /\ sent' = sent \cup {c}
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,phase,attempts,retries,
                    pending,issued,executed,answered,frame,matched,retryCause,rejections,failures>>
Execute(c) ==
    \* A sent mutation may execute even after the connection dies. Nothing
    \* infers rollback from transport loss. A pre-execution fence cannot execute.
    /\ c \in sent /\ c \notin executed /\ Unanswered(c)
    /\ executed' = executed \cup {c}
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,phase,attempts,retries,
                    pending,issued,sent,answered,frame,matched,retryCause,rejections,failures>>
MakeFrame(c,kind) ==
    /\ frame = NoFrame /\ c \in sent /\ Unanswered(c)
    /\ kind \in {"ok","db"} => c \in executed
    /\ kind = "fence" => c \notin executed
    /\ frame' = <<c,kind>>
    /\ answered' = IF kind \in TerminalKinds THEN answered \cup {<<c,kind>>} ELSE answered
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,phase,attempts,retries,
                    pending,issued,sent,executed,matched,retryCause,rejections,failures>>
DuplicateFrame(reply) ==
    /\ frame = NoFrame /\ reply \in answered
    /\ frame' = reply
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,phase,attempts,retries,
                    pending,issued,sent,executed,answered,matched,retryCause,rejections,failures>>
UnknownSync(p) ==
    /\ frame = NoFrame /\ p \in opened \ retired
    /\ frame' = <<<<p,4,0,0>>,"ok">>
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,phase,attempts,retries,
                    pending,issued,sent,executed,answered,matched,retryCause,rejections,failures>>
Deliver(target) ==
    /\ frame # NoFrame /\ frame[2] \in TerminalKinds
    /\ frame[1][1] \in opened \ retired
    /\ target \in pending
    /\ target[1] = frame[1][1]
    /\ target[2] = frame[1][2]
    /\ LET p == target[1]
           fence == frame[2] \notin {"ok","db"} IN
        /\ pending' = pending \ (IF fence THEN Entries(p) ELSE {target})
        /\ phase' = Resolve(IF fence THEN Entries(p) ELSE {target}, target,
                            IF fence THEN "fenced" ELSE frame[2])
        /\ retired' = IF fence THEN retired \cup {p} ELSE retired
        /\ generation' = IF fence THEN NextGeneration(p) ELSE generation
        /\ invalidations' = IF fence THEN invalidations \cup {<<p[2],generation,NextGeneration(p)>>} ELSE invalidations
    /\ matched' = matched \cup {<<target,frame[1],frame[2]>>}
    /\ frame' = NoFrame
    /\ UNCHANGED <<opened,cost,nextSync,attempts,retries,issued,sent,
                    executed,answered,retryCause,rejections,failures>>
FatalFrame ==
    /\ frame # NoFrame /\ frame[1][1] \in opened \ retired
    /\ frame[2] \notin TerminalKinds \/
        ~\E c \in pending : c[1] = frame[1][1] /\ c[2] = frame[1][2]
    /\ LET p == frame[1][1] IN
        /\ pending' = pending \ Entries(p)
        /\ phase' = Resolve(Entries(p), NoFrame, "uncertain")
        /\ retired' = retired \cup {p} \* fatal protocol frame
        /\ failures' = failures \cup {p}
        /\ generation' = NextGeneration(p)
        /\ invalidations' = invalidations \cup {<<p[2],generation,NextGeneration(p)>>}
    /\ frame' = NoFrame
    /\ UNCHANGED <<opened,cost,nextSync,attempts,retries,issued,sent,
                    executed,answered,matched,retryCause,rejections>>
TransportFailure(p) ==
    \* EOF, partial/malformed/oversized frame and deadline expiration all
    \* retire only this physical connection, including abandoned/queued work.
    /\ p \in opened \ retired
    /\ pending' = pending \ Entries(p)
    /\ phase' = Resolve(Entries(p), NoFrame, "uncertain")
    /\ retired' = retired \cup {p}
    /\ failures' = failures \cup {p}
    /\ generation' = NextGeneration(p)
    /\ invalidations' = invalidations \cup {<<p[2],generation,NextGeneration(p)>>}
    /\ UNCHANGED <<opened,cost,nextSync,attempts,retries,issued,sent,
                    executed,answered,frame,matched,retryCause,rejections>>
DropOldFrame ==
    /\ frame # NoFrame /\ frame[1][1] \in retired
    /\ frame' = NoFrame
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,phase,attempts,retries,
                    pending,issued,sent,executed,answered,matched,retryCause,rejections,failures>>
Open(connection) ==
    /\ generation \in Generations
    /\ <<connection,generation>> \notin opened
    /\ opened' = opened \cup {<<connection,generation>>}
    /\ UNCHANGED <<invalidations,cost,generation,retired,nextSync,phase,attempts,retries,pending,
                    issued,sent,executed,answered,frame,matched,retryCause,rejections,failures>>
Abandon(r) ==
    /\ phase[r] = "pending"
    /\ phase' = [phase EXCEPT ![r] = "abandoned"]
    /\ pending' = pending \* retain ownership after caller cancellation
    \* No capacity, SYNC or queued frame is released by dropping the caller.
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,attempts,retries,
                    issued,sent,executed,answered,frame,matched,retryCause,rejections,failures>>
Retry(r) ==
    /\ phase[r] = "fenced"
    /\ retries[r] = 0
    /\ retryCause' = [retryCause EXCEPT ![r] = phase[r]]
    /\ retries' = [retries EXCEPT ![r] = @+1]
    /\ phase' = [phase EXCEPT ![r] = "new"]
    /\ UNCHANGED <<invalidations,opened,cost,generation,retired,nextSync,attempts,pending,
                    issued,sent,executed,answered,frame,matched,rejections,failures>>

Next ==
    \/ \E r \in Requests,s \in Syncs,p \in Physical : Admit(r,s,p)
    \/ \E r \in Requests : Abandon(r) \/ Retry(r) \/ (\E p \in Physical : Reject(r,p))
    \/ \E c \in pending : Send(c)
    \/ \E c \in sent : Execute(c) \/ (\E kind \in Kinds : MakeFrame(c,kind))
    \/ \E target \in pending : Deliver(target)
    \/ \E reply \in answered : DuplicateFrame(reply)
    \/ \E p \in Physical : UnknownSync(p) \/ TransportFailure(p)
    \/ \E connection \in Connections : Open(connection)
    \/ FatalFrame \/ DropOldFrame

TypeOK ==
    /\ cost \in [Requests -> {1,2}] /\ generation \in 1..3
    /\ opened \subseteq Physical /\ retired \subseteq opened
    /\ nextSync \in [Physical -> 1..4]
    /\ phase \in [Requests -> {"new","pending","abandoned","ok","db","fenced","uncertain","cancelled","limit"}]
    /\ attempts \in [Requests -> 0..2] /\ retries \in [Requests -> 0..2]
    /\ pending \subseteq Calls /\ issued \subseteq Calls /\ sent \subseteq issued /\ executed \subseteq sent
    /\ answered \subseteq Calls \X TerminalKinds
    /\ frame \in {NoFrame} \cup ((Calls \cup {<<p,4,0,0>> : p \in Physical}) \X Kinds)
    /\ matched \subseteq Calls \X (Calls \cup {<<p,4,0,0>> : p \in Physical}) \X Kinds
    /\ retryCause \in [Requests -> {"none","fenced","uncertain"}]
    /\ rejections \subseteq Requests \X (1..2) /\ failures \subseteq Physical
    /\ invalidations \subseteq Generations \X (1..3) \X (1..3)
CountBound == \A p \in Physical : Cardinality(Entries(p)) <= MaxCount
ByteBound == \A p \in Physical : Bytes(Entries(p)) <= MaxBytes
NoSyncReuse == \A a,b \in issued : a[1] = b[1] /\ a[2] = b[2] => a = b
GenerationIsolation == \A m \in matched : m[1][1] = m[2][1]
OldFailureCannotInvalidateNewPool == \A event \in invalidations : event[1] # event[2] => event[3] = event[2]
ResponseAssociation == \A m \in matched : m[1] = m[2]
NoNonfinalCompletion == \A m \in matched : m[3] \in {"ok","db","fence"}
RetiredHasNoPending == \A g \in retired : Entries(g) = {}
FailureRetiresGeneration == failures \subseteq retired
PendingOwnership ==
    /\ pending \subseteq issued
    /\ \A r \in Requests :
        (phase[r] \in {"pending","abandoned"}) = (\E c \in pending : c[3] = r)
FenceRetryAtMostOnce == \A r \in Requests : retries[r] <= 1
NoMutationReplayAfterUncertainty == \A r \in Requests :
    /\ retries[r] > 0 => retryCause[r] = "fenced"
    /\ Cardinality({c \in executed : c[3] = r}) <= 1
NoDuplicateMutationEffect == \A r \in Requests : Cardinality({c \in executed : c[3] = r}) <= 1
FencePrecedesExecution == \A m \in matched : m[3] = "fence" => m[2] \notin executed
RejectedBeforeAdmission == \A rejection \in rejections :
    ~\E c \in issued : c[3] = rejection[1] /\ c[4] = rejection[2]
Spec == Init /\ [][Next]_vars
Symmetry == Permutations(Requests) \cup Permutations(Connections)
=============================================================================
