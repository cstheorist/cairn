#!/usr/bin/env python3
"""Gateway/coordinator crash tests against only the isolated cairn-ha project."""
import json
import subprocess
from check_replication import COMPOSE, ROOT, node, run

def phase(name):
    run(*COMPOSE, "--profile", "test", "run", "--no-deps", "--rm", "tests", "python", "/tests/transaction_recovery.py", name)

def main():
    held = None
    stopped = None
    try:
        held = subprocess.Popen([*COMPOSE, "--profile", "test", "run", "--no-deps", "--rm", "--name", "cairn-ha-held-client", "tests",
                                 "python", "/tests/transaction_recovery.py", "hold_open"], cwd=ROOT, stdout=subprocess.PIPE, text=True)
        while True:
            line = held.stdout.readline()
            if not line: raise AssertionError("held transaction did not become ready")
            print(line, end="", flush=True)
            if "OPEN_READY" in line: break
        run(*COMPOSE, "kill", "-s", "SIGKILL", "gateway")
        held.wait(timeout=10)
        held = None
        run(*COMPOSE, "up", "-d", "--no-deps", "--wait", "gateway")
        phase("check_open")
        phase("prepare")
        run(*COMPOSE, "stop", "gateway")
        phase("decide_partial")
        state = json.loads((ROOT / "artifacts/transaction-recovery-commit.json").read_text())
        stopped = node(state["coordinator"])
        run(*COMPOSE, "kill", "-s", "SIGKILL", stopped)
        run(*COMPOSE, "up", "-d", "--no-deps", "--wait", "gateway")
        phase("check_commit")
    finally:
        if stopped: run(*COMPOSE, "up", "-d", "--wait", stopped)
        run(*COMPOSE, "up", "-d", "--no-deps", "--wait", "gateway")
        if held and held.poll() is None:
            subprocess.run(["docker", "stop", "cairn-ha-held-client"], check=False)
            held.wait(timeout=10)

if __name__ == "__main__": main()
