#!/usr/bin/env python3
"""Ensure bounded models detect early acknowledgements and weak commit guards."""
import argparse
import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'tla/out/mutations'
OUTPUT.mkdir(parents=True, exist_ok=True)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--model', choices=['ShardStore','ReplicaEpoch','TwoPhaseCommit','PredicateLocks','ReadBatch','VoteCommit','LocalFinish','VolatileOpen','IprotoPipeline','OpenPointRead'])
options = parser.parse_args()
if options.model in (None, 'ShardStore'):
    with tempfile.TemporaryDirectory(prefix="cairn-tla-") as temporary:
        directory = pathlib.Path(temporary)
        source = (ROOT / "tla/ShardStore.tla").read_text()
        guard = 'phase[op] = "stored"'
        assert source.count(guard) == 1
        (directory / "ShardStore.tla").write_text(source.replace(guard, 'phase[op] \\in {"sent", "stored"}'))
        shutil.copyfile(ROOT / "tla/ShardStore.cfg", directory / "ShardStore.cfg")
        result = subprocess.run([
            "docker", "run", "--rm", "-v", f"{directory}:/model", "-v", f"{ROOT / '.tools/tla2tools.jar'}:/tla2tools.jar:ro",
            "-w", "/model", "eclipse-temurin:21-jre", "java", "-cp", "/tla2tools.jar", "tlc2.TLC",
            "-workers", "1", "-metadir", "/tmp/states", "-config", "ShardStore.cfg", "ShardStore.tla",
        ], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (OUTPUT / '00-ShardStore-NoFalseAcknowledgement.out').write_text(result.stdout)
        if result.returncode == 0 or "Invariant NoFalseAcknowledgement is violated" not in result.stdout:
            raise SystemExit("expected an early-acknowledgement counterexample, got:\n" + result.stdout)
        print("mutation check passed: TLC rejected acknowledgement before durable commit")

CASES = [
    ("VolatileOpen", '/\\ idEpoch = -1 \\/ idEpoch = incarnation[Home]', '/\\ TRUE', "ActiveHomeIncarnation"),
    ("VolatileOpen", '/\\ p \\notin enlisted', '/\\ TRUE', "PreparedKeepsAcknowledgedWrites"),
    ("VolatileOpen", '/\\ p # Home /\\ p \\in enlisted /\\ ~used[p] /\\ vote[p] = "none"', '/\\ p # Home /\\ p \\in enlisted /\\ vote[p] = "none"', "PreparedKeepsAcknowledgedWrites"),
    ("VolatileOpen", 'ValidOpen(p) == present[p] /\\ tokenEpoch[p] = incarnation[p] /\\ scopeEpoch[p] = incarnation[p]', 'ValidOpen(p) == TRUE', "PreparedKeepsAcknowledgedWrites"),
    ("VolatileOpen", "/\\ coordinatorRecorded' = (coordinatorRecorded \\/ p = Home)", "/\\ coordinatorRecorded' = coordinatorRecorded", "PreparedHomeHasCoordinator"),
    ("LocalFinish", '/\\ locks[Other(t)][k] # "write"', '/\\ TRUE', "CompatibleLocks"),
    ("LocalFinish", '/\\ k \\in online /\\ phase[t] \\in {"committed","aborted"}', '/\\ k \\in online /\\ phase[t] \\in {"active","committed","aborted"}', "PreparedWritesRemainLocked"),
    ("LocalFinish", "/\\ rows' = IF phase[t] = \"committed\" /\\ k \\in intents[t]\n        THEN [rows EXCEPT ![k] = t] ELSE rows", "/\\ rows' = rows", "UnlockedRowsMatchLogicalState"),
    ("VoteCommit", '/\\ ~frozen', '/\\ TRUE', "FrozenMembership"),
    ("VoteCommit", '/\\ 0 \\in online /\\ frozen /\\ members # {} /\\ yes = members', '/\\ 0 \\in online /\\ frozen /\\ members # {} /\\ yes # {}', "CommitRequiresUnanimity"),
    ("VoteCommit", '/\\ 0 \\in online /\\ frozen /\\ no # {}', '/\\ 0 \\in online /\\ frozen', "AbortRequiresNo"),
    ("VoteCommit", '/\\ p \\in online /\\ complete', '/\\ p \\in online', "ReleaseRequiresCompletion"),
    ("ReadBatch", '/\\ ticket[request] <= cutoff', '/\\ TRUE', "NoStaleResponse"),
    ("ReplicaEpoch", "Cardinality(q) > NodeCount \\div 2", "Cardinality(q) >= NodeCount \\div 2", "DurableAcknowledgement"),
    ("TwoPhaseCommit", '/\\ enlisted # {} /\\ preparedReceipts = enlisted', '/\\ enlisted # {}', "CommitRequiresPrepare"),
    ("TwoPhaseCommit", '/\\ enlisted = {p} /\\ phase[p] = "open"', '/\\ p \\in enlisted /\\ phase[p] = "open"', "CommitRequiresPrepare"),
    ("PredicateLocks", 'IF @ \\in {"write","scan_write"} THEN "scan_write" ELSE "scan"', '"scan"', "RepeatableReadsAndNoPhantoms"),
    ("IprotoPipeline", '/\\ target[2] = frame[1][2]', '/\\ TRUE', "ResponseAssociation"),
    ("IprotoPipeline", '/\\ s = nextSync[p]', '/\\ (s = nextSync[p] \\/ \\E old \\in pending : old[1] = p /\\ old[2] = s /\\ phase[old[3]] = "abandoned")', "NoSyncReuse"),
    ("IprotoPipeline", 'TerminalKinds == {"ok","db","fence"}', 'TerminalKinds == {"ok","db","fence","chunk"}', "NoNonfinalCompletion"),
    ("IprotoPipeline", '/\\ phase[r] = "fenced"', '/\\ phase[r] \\in {"fenced","uncertain"}', "NoDuplicateMutationEffect"),
    ("IprotoPipeline", '/\\ target[1] = frame[1][1]', '/\\ target[1][1] = frame[1][1][1]', "GenerationIsolation"),
    ("IprotoPipeline", '/\\ Cardinality(Entries(p)) < MaxCount', '/\\ TRUE', "CountBound"),
    ("IprotoPipeline", '/\\ Bytes(Entries(p)) + cost[r] <= MaxBytes', '/\\ TRUE', "ByteBound"),
    ("IprotoPipeline", '/\\ pending\' = pending \\ Entries(p)\n    /\\ phase\' = Resolve(Entries(p), NoFrame, "uncertain")', '/\\ pending\' = pending\n    /\\ phase\' = Resolve(Entries(p), NoFrame, "uncertain")', "RetiredHasNoPending"),
    ("IprotoPipeline", '/\\ retired\' = retired \\cup {p} \\* fatal protocol frame', '/\\ retired\' = retired \\* fatal protocol frame', "FailureRetiresGeneration"),
    ("IprotoPipeline", 'IF p[2] = generation /\\ generation < 3 THEN generation+1 ELSE generation', 'IF generation < 3 THEN generation+1 ELSE generation', "OldFailureCannotInvalidateNewPool"),
    ("IprotoPipeline", '/\\ retries[r] = 0', '/\\ retries[r] < 2', "FenceRetryAtMostOnce"),
    ("IprotoPipeline", '/\\ pending\' = pending \\* retain ownership after caller cancellation', '/\\ pending\' = {c \\in pending : c[3] # r}', "PendingOwnership"),
    ("OpenPointRead", 'Blocked(k) == DurableWriter(k) \\/ TableWriter(k)', 'Blocked(k) == TableWriter(k)', "ReadAfterObservedCommit"),
    ("OpenPointRead", '/\\ durable\' = [durable EXCEPT ![t] = @ \\cup Claimed(t,p)]', '/\\ durable\' = durable', "ReadAfterObservedCommit"),
    ("OpenPointRead", '/\\ rows\' = IF phase[t] = "committed" THEN\n        [k \\in Keys |-> IF k \\in intents[t] /\\ Owner(k) = p THEN t ELSE rows[k]] ELSE rows', '/\\ rows\' = rows', "ReadAfterAcknowledgement"),
    ("OpenPointRead", '/\\ collected\' = [collected EXCEPT ![r] = viewRows[key[r]]]', '/\\ collected\' = [collected EXCEPT ![r] = rows[key[r]]]', "CommittedBatchWitness"),
    ("OpenPointRead", '/\\ batchPhase = "collecting" /\\ ticket[r] <= cutoff', '/\\ batchPhase \\in {"collecting","reading"}', "ReadAfterAcknowledgement"),
    # Reachability controls do not weaken transition effects. The first limits
    # view capture to an existing OPEN write claim, so a read followed by a new
    # writer cannot masquerade as evidence that reads bypass OPEN claims.
    ("OpenPointRead", 'Spec == Init /\\ [][Next]_vars', 'OpenReadIsReachable == ~(\\E r \\in Requests : state[r] = "done")\nOpenWitnessNext == Next /\\ ((batchPhase = "collecting" /\\ batchPhase\' = "reading") => (\\A r \\in members : \\E t \\in Transactions : locks[t][key[r]] = "write" /\\ key[r] \\notin durable[t]))\nSpec == Init /\\ [][OpenWitnessNext]_vars', "OpenReadIsReachable"),
    ("OpenPointRead", 'Spec == Init /\\ [][Next]_vars', 'PartialFinishReadIsReachable == ~(\\E r \\in Requests,t \\in Transactions : state[r] = "done" /\\ phase[t] = "committed" /\\ applied[t] # {} /\\ applied[t] # intents[t] /\\ key[r] \\in applied[t] /\\ result[r] = t)\nSpec == Init /\\ [][Next]_vars', "PartialFinishReadIsReachable"),
]
for number, (model, guard, mutation, invariant) in enumerate(CASES, start=1):
    if options.model is not None and model != options.model: continue
    with tempfile.TemporaryDirectory(prefix="cairn-tla-") as temporary:
        directory = pathlib.Path(temporary)
        source = (ROOT / f"tla/{model}.tla").read_text()
        assert source.count(guard) == 1, (model, guard)
        mutated_source = source.replace(guard, mutation)
        distinct_batch = model == 'OpenPointRead' and invariant == 'CommittedBatchWitness'
        if distinct_batch:
            # Require the torn-view control to expose two different keys,
            # rather than the shorter duplicate-key old/new counterexample.
            scenario = 'DistinctBatchKeys == \\A r,s \\in Requests : (r # s /\\ state[r] # "new" /\\ state[s] # "new") => key[r] # key[s]\n'
            mutated_source = mutated_source.replace('Spec ==', scenario + 'Spec ==', 1)
        (directory / f"{model}.tla").write_text(mutated_source)
        config_name = 'IprotoPipelineCount' if model == 'IprotoPipeline' and invariant == 'CountBound' else model
        if model == 'OpenPointRead' and invariant in ('ReadAfterAcknowledgement', 'ReadAfterObservedCommit', 'CommittedBatchWitness'):
            config_name = 'OpenPointReadBatch'
        configuration = (ROOT / f"tla/{config_name}.cfg").read_text()
        if model in ('IprotoPipeline', 'OpenPointRead'):
            # Check the named consequence rather than stopping at a different,
            # earlier invariant. In particular, unknown replay must exhibit two
            # server-side effects, not merely the forbidden retry admission.
            configuration = configuration.split('INVARIANTS', 1)[0] + f'INVARIANT {invariant}\n'
        if distinct_batch:
            configuration += 'CONSTRAINT DistinctBatchKeys\n'
        (directory / f"{model}.cfg").write_text(configuration)
        result = subprocess.run([
            "docker", "run", "--rm", "-v", f"{directory}:/model", "-v", f"{ROOT / '.tools/tla2tools.jar'}:/tla2tools.jar:ro",
            "-w", "/model", "eclipse-temurin:21-jre", "java", "-cp", "/tla2tools.jar", "tlc2.TLC",
            "-workers", "1", "-metadir", "/tmp/states", "-config", f"{model}.cfg", f"{model}.tla",
        ], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (OUTPUT / f'{number:02}-{model}-{invariant}.out').write_text(result.stdout)
        if result.returncode == 0 or f"Invariant {invariant} is violated" not in result.stdout:
            raise SystemExit(f"expected {model}/{invariant} counterexample, got:\n" + result.stdout)
        print(f"mutation check passed: {model} rejected violation of {invariant}", flush=True)
