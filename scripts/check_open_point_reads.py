#!/usr/bin/env python3
"""Validate committed point visibility, then fault only task-owned HA leaders."""
import json
import os
import subprocess

from check_replication import node
from check_votes import COMPOSE, ROOT, run


def phase(*arguments, recovery="1"):
    run("--profile", "test", "run", "--no-deps", "--rm", "tests", "python",
        "/tests/open_point_reads.py", *arguments, recovery=recovery)


def main():
    os.environ["CAIRN_VOTE_COMMIT"] = "3"
    stopped = None
    try:
        run("up", "-d", "--build", "--wait", "gateway", recovery="0")
        phase(recovery="0")
        for case in ("home_open", "remote_open", "prepared", "local"):
            run("up", "-d", "--no-deps", "--wait", "gateway", recovery="0")
            phase("seed", case, recovery="0")
            state = json.loads((ROOT / "artifacts/open-point-reads-fault.json").read_text())
            stopped = node(state["dead"])
            subprocess.run([*COMPOSE, "kill", "-s", "SIGKILL", stopped], cwd=ROOT, check=True)
            run("up", "-d", "--no-deps", "--wait", "gateway")
            phase("check", case)
            run("up", "-d", "--no-deps", "--wait", stopped)
            stopped = None
    finally:
        if stopped:
            run("up", "-d", "--no-deps", "--wait", stopped)
        run("up", "-d", "--no-deps", "--wait", "gateway")


if __name__ == "__main__":
    main()
