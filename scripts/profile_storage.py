#!/usr/bin/env python3
"""Scoped jit.p diagnostics for the six local Cairn HA storage containers.

No sampling occurs unless the explicit `start` subcommand is invoked. Keep
profiling separate from timed comparison runs. This helper never restarts a
service, changes database tuples, installs packages, or retries an EVAL.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess


ROOT = Path(__file__).resolve().parents[1]
TOOL_ROOT = ROOT / ".tools" / "profile-storage"
SERVICES = tuple(f"s{shard}{letter}" for shard in (0, 1) for letter in "abc")
KEY = "_cairn_storage_cpu_profile_lease"
MODE = "vf3i10m0"
SECONDS = 20
INSPECT_FORMAT = ('{"id":{{json .Id}},"name":{{json .Name}},'
                  '"running":{{json .State.Running}},'
                  '"project":{{json (index .Config.Labels "com.docker.compose.project")}},'
                  '"service":{{json (index .Config.Labels "com.docker.compose.service")}}}')


def command(arguments, *, input_text=None, timeout=15):
    result = subprocess.run(arguments, input=input_text, capture_output=True,
                            text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip()
                           or f"command failed with status {result.returncode}")
    return result.stdout


def container(node):
    if node not in SERVICES:
        raise ValueError("unsupported storage service")
    name = f"cairn-ha-{node}-1"
    info = json.loads(command(["docker", "inspect", "--type", "container",
                               "--format", INSPECT_FORMAT, name]))
    if (info["name"] != "/" + name or not info["running"]
            or info["project"] != "cairn-ha" or info["service"] != node):
        raise RuntimeError("container identity/project/service/running guard failed")
    return info


def evaluate(node, source, *, expected_container_id=None):
    # All EVALs stay inside an identity-checked, task-owned container and use its
    # existing native operator endpoint. Never evaluate code in another service.
    info = container(node)
    if expected_container_id is not None and info["id"] != expected_container_id:
        raise RuntimeError("container changed before operator EVAL")
    helper = """
local c
local ok, result = pcall(function()
    c = require('net.box').connect('operator:local-operator@127.0.0.1:3301',
        {wait_connected=3, connect_timeout=3, fetch_schema=false})
    assert(c:is_connected(), 'operator connection failed')
    return c:eval([===[%s]===], {}, {timeout=5})
end)
if c then c:close() end
if not ok then io.stderr:write(tostring(result), '\\n'); os.exit(1) end
print(require('json').encode(result))
-- Exit only this client helper, never the storage server.
os.exit(0)
""" % source
    return json.loads(command(["docker", "exec", "-i", info["id"],
                               "env", "-u", "TT_INSTANCE_NAME", "-u", "TT_APP_NAME",
                               "tarantool", "-"], input_text=helper))


def manifest_path(node, lease):
    if node not in SERVICES:
        raise ValueError("unsupported storage service")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", lease):
        raise ValueError("lease must contain 1..64 lowercase ASCII letters/digits/hyphens")
    return TOOL_ROOT / "leases" / f"{lease}-{node}.json"


def write_manifest(path, data, *, exclusive=False):
    # Ownership tokens never live beside published scripts or collected output.
    flags = os.O_WRONLY | os.O_CREAT | (os.O_EXCL if exclusive else os.O_TRUNC)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w") as output:
        os.fchmod(output.fileno(), 0o600)
        output.write(json.dumps(data, indent=2) + "\n")


def load_manifest(node, lease):
    path = manifest_path(node, lease)
    data = json.loads(path.read_text())
    info = container(node)
    if data["container_id"] != info["id"] or data["node"] != node or data["lease"] != lease:
        raise RuntimeError("saved lease belongs to a different container incarnation")
    return path, data


SNAPSHOT = """
local function snapshot(lease)
    return {lease=lease.id, path=lease.path, active=lease.active,
        started=lease.started, stopped=lease.stopped, stop_ok=lease.stop_ok,
        error=lease.error, mode=lease.mode, seconds=lease.seconds,
        pid=box.info.pid, version=box.info.version, arch=jit.arch}
end
"""


def start(node, lease):
    path = manifest_path(node, lease)
    if path.exists():
        raise RuntimeError("lease already has a manifest; use status/stop/collect, never replay start")
    info = container(node)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    data = {"node": node, "lease": lease, "token": secrets.token_hex(16),
            "container_id": info["id"], "mode": MODE, "seconds": SECONDS,
            "remote_path": f"/var/lib/tarantool/cairn-cpu-{lease}.txt",
            "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "state": "starting"}
    # Persist identity before the non-idempotent diagnostic EVAL. Even an RPC
    # timeout leaves enough information for an owned stop/status request.
    try:
        write_manifest(path, data, exclusive=True)
    except FileExistsError:
        raise RuntimeError("lease already has a manifest; never replay start") from None
    source = SNAPSHOT + """
local key, id, token, path = %s, %s, %s, %s
local previous = rawget(_G, key)
assert(not previous or (not previous.active and previous.stop_ok == true),
    'an active or failed profiling lease already exists')
local existing = io.open(path, 'r')
if existing then existing:close(); error('profile path already exists') end
local profiler, fiber, clock = require('jit.p'), require('fiber'), require('clock')
local lease = {id=id, token=token, path=path, active=false, mode=%s, seconds=20}
lease.stop = function()
    assert(rawget(_G, key) == lease, 'profiling lease was replaced')
    if not lease.active then return snapshot(lease) end
    local ok, err = pcall(profiler.stop)
    -- A failed stop leaves activity uncertain; never report it as inactive.
    lease.stop_ok = ok
    if not ok then lease.error=tostring(err); error(err) end
    lease.active, lease.stopped = false, clock.monotonic()
    return snapshot(lease)
end
profiler.start(lease.mode, lease.path)
lease.active, lease.started = true, clock.monotonic()
rawset(_G, key, lease)
local ok, err = pcall(function()
    fiber.create(function()
        fiber.sleep(20)
        -- A later lease must never be stopped by this lease's old timer.
        if rawget(_G, key) == lease and lease.active then
            local stopped, stop_error = pcall(lease.stop)
            if not stopped then require('log').error('Cairn CPU lease stop: %%s', tostring(stop_error)) end
        end
    end)
end)
if not ok then lease.stop(); error(err) end
return snapshot(lease)
""" % tuple(json.dumps(value) for value in
             (KEY, lease, data["token"], data["remote_path"], MODE))
    try:
        data["server"] = evaluate(node, source, expected_container_id=data["container_id"])
        data["state"] = "active"
    except Exception as error:
        data["state"] = "start_failed_or_uncertain"
        data["error"] = str(error)
        write_manifest(path, data)
        raise
    write_manifest(path, data)
    return data["server"]


def owned_status(node, lease, *, stop=False):
    path, data = load_manifest(node, lease)
    source = SNAPSHOT + """
local lease = rawget(_G, %s)
assert(lease and lease.id == %s and lease.token == %s, 'owned profiling lease not found')
%s
return snapshot(lease)
""" % (json.dumps(KEY), json.dumps(lease), json.dumps(data["token"]),
       "lease.stop()" if stop else "")
    status = evaluate(node, source, expected_container_id=data["container_id"])
    data["server"] = status
    data["state"] = "active" if status["active"] else "stopped"
    write_manifest(path, data)
    return status


def collect(node, lease):
    status = owned_status(node, lease)
    if status["active"] or status.get("stop_ok") is not True:
        raise RuntimeError("profile has not stopped successfully; do not collect an open/incomplete file")
    path, data = load_manifest(node, lease)
    destination = ROOT / "artifacts" / "storage-profiles" / lease / f"{node}.jit.txt"
    metadata = destination.with_suffix(".json")
    if destination.exists() or metadata.exists():
        raise RuntimeError("local profile already exists; refusing overwrite")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Reserve exclusively before Docker copies into our new file. A failed copy
    # leaves the reservation for inspection rather than allowing an overwrite.
    try:
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise RuntimeError("local profile already exists; refusing overwrite") from None
    os.close(descriptor)
    try:
        descriptor = os.open(metadata, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise RuntimeError("local provenance already exists; refusing overwrite") from None
    os.close(descriptor)
    command(["docker", "cp", f"{data['container_id']}:{data['remote_path']}", str(destination)])
    data["local_path"] = str(destination.relative_to(ROOT))
    data["sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
    data["state"] = "collected"
    write_manifest(path, data)
    # Keep a reviewable provenance record beside the text; the ownership token
    # is an internal control value and is omitted from this evidence copy.
    evidence = {key: data[key] for key in
                ("node", "lease", "container_id", "mode", "seconds", "remote_path",
                 "started_utc", "state", "server", "local_path", "sha256")
                if key in data}
    metadata.write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def probe(node):
    return evaluate(node, """
local p_ok, p = pcall(require, 'jit.p')
local low_ok, low = pcall(require, 'jit.profile')
local misc_ok, misc = pcall(require, 'misc')
local lease = rawget(_G, '_cairn_storage_cpu_profile_lease')
return {version=box.info.version, pid=box.info.pid, read_only=box.info.ro, arch=jit.arch,
    native_ping={cairn.ping()},
    jit_version=jit.version, jit_p_available=p_ok, jit_profile_available=low_ok,
    jit_p_start_type=p_ok and type(p.start) or tostring(p),
    jit_profile_start_type=low_ok and type(low.start) or tostring(low),
    sysprof_available=misc_ok and misc.sysprof and misc.sysprof.available or false,
    own_lease_active=lease and lease.active or false}
""")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("probe", "start", "status", "stop", "collect"))
    parser.add_argument("--node", required=True, choices=SERVICES)
    parser.add_argument("--lease", help="unique diagnostic ID; required except for probe")
    options = parser.parse_args()
    if options.action != "probe" and not options.lease:
        parser.error("--lease is required for this action")
    if options.action == "probe":
        result = probe(options.node)
    elif options.action == "start":
        result = start(options.node, options.lease)
    elif options.action == "collect":
        result = collect(options.node, options.lease)
    else:
        result = owned_status(options.node, options.lease, stop=options.action == "stop")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
