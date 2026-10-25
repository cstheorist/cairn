#!/usr/bin/env python3
"""Faults only the isolated cairn-ha Compose cluster; always restores test nodes."""
import json
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "compose.replication.yaml")]


def run(*arguments, timeout=None):
    subprocess.run(list(arguments), cwd=ROOT, check=True, timeout=timeout)


def test(stage):
    run(*COMPOSE, "--profile", "test", "run", "--no-deps", "--rm", "tests", "python", "/tests/replication.py", stage)


def state():
    return json.loads((ROOT / "artifacts/replication-state.json").read_text())


def node(value):
    assert re.fullmatch(r"s[01][abc]", value), value
    return value


def check_fenced(isolated):
    # These net.box options are supported by Tarantool 3.3.5:
    # https://github.com/tarantool/tarantool/blob/3.3.5/src/box/lua/net_box.lua
    lua = """
local c
local ok, err = pcall(function()
    c = require('net.box').connect('operator:local-operator@127.0.0.1:3301',
        {connect_timeout = 3, wait_connected = 3, fetch_schema = false})
    assert(c:is_connected(), 'isolated-node probe could not connect')
    assert(c:eval('return box.info.ro', {}, {timeout = 3}) == true,
        'isolated old leader is not fenced read-only')
end)
if c then c:close() end
if not ok then io.stderr:write(tostring(err), '\\n') end
-- net.box background fibers must not keep this one-shot probe alive.
os.exit(ok and 0 or 1)
"""
    run(*COMPOSE, "exec", "-T", isolated, "env", "-u", "TT_INSTANCE_NAME", "-u", "TT_APP_NAME",
        "tarantool", "-e", lua, timeout=15)


def main():
    (ROOT / "artifacts").mkdir(exist_ok=True)
    stopped = []
    isolated = None
    try:
        test("seed")
        initial = node(state()["initial"][0]["node"])
        stopped.append(initial)
        run(*COMPOSE, "kill", "-s", "SIGKILL", initial)
        test("failover")
        current = node(state()["after_failover"]["node"])
        follower = next("s0" + letter for letter in "abc" if "s0" + letter not in (initial, current))
        stopped.append(follower)
        run(*COMPOSE, "kill", "-s", "SIGKILL", follower)
        test("no_quorum")
        run(*COMPOSE, "up", "-d", "--wait", *stopped)
        stopped.clear()
        test("restored")
        isolated = node(state()["before_partition"]["node"])
        run("docker", "network", "disconnect", "cairn-ha_default", f"cairn-ha-{isolated}-1")
        test("partition")
        # Query the isolated old leader over its local socket namespace, not the
        # disconnected cluster network; it must have fenced itself read-only.
        check_fenced(isolated)
        run("docker", "network", "connect", "--alias", isolated, "cairn-ha_default", f"cairn-ha-{isolated}-1")
        isolated = None
        test("healed")
    finally:
        if isolated:
            run("docker", "network", "connect", "--alias", isolated, "cairn-ha_default", f"cairn-ha-{isolated}-1")
        if stopped:
            run(*COMPOSE, "up", "-d", *stopped)


if __name__ == "__main__": main()
