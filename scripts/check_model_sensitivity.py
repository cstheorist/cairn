#!/usr/bin/env python3
"""Ensure bounded models detect early acknowledgements and weak commit guards."""
import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'tla/out/mutations'
OUTPUT.mkdir(parents=True, exist_ok=True)
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
]
for number, (model, guard, mutation, invariant) in enumerate(CASES, start=1):
    with tempfile.TemporaryDirectory(prefix="cairn-tla-") as temporary:
        directory = pathlib.Path(temporary)
        source = (ROOT / f"tla/{model}.tla").read_text()
        assert source.count(guard) == 1, (model, guard)
        (directory / f"{model}.tla").write_text(source.replace(guard, mutation))
        shutil.copyfile(ROOT / f"tla/{model}.cfg", directory / f"{model}.cfg")
        result = subprocess.run([
            "docker", "run", "--rm", "-v", f"{directory}:/model", "-v", f"{ROOT / '.tools/tla2tools.jar'}:/tla2tools.jar:ro",
            "-w", "/model", "eclipse-temurin:21-jre", "java", "-cp", "/tla2tools.jar", "tlc2.TLC",
            "-workers", "1", "-metadir", "/tmp/states", "-config", f"{model}.cfg", f"{model}.tla",
        ], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (OUTPUT / f'{number:02}-{model}-{invariant}.out').write_text(result.stdout)
        if result.returncode == 0 or f"Invariant {invariant} is violated" not in result.stdout:
            raise SystemExit(f"expected {model}/{invariant} counterexample, got:\n" + result.stdout)
        print(f"mutation check passed: {model} rejected violation of {invariant}", flush=True)
