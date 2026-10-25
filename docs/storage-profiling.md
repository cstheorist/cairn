# Storage CPU profiling

Use a separate diagnostic run to distinguish Lua/metadata work from native
transport, replication and WAL CPU. Keep throughput comparisons unprofiled.
The helper targets only the six local `cairn-ha` storage containers; sampling
starts only with its explicit `start` command.

The [2026-10-25 sanitized evidence](../bench/results/storage-profile-2026-10-25/summary.json)
is one transfer diagnostic, with no speed or comparative performance claim.
Raw profiles remain private because native records contain addresses, registers
and user stacks. The published copies contain aggregates, sanitized Lua text
and provenance hashes.

## Supported runtime and scope

The observed storage image ran Tarantool `3.3.5-0-gacc0d4d806`, LuaJIT
`2.1.0-beta3`, ARM64, with Tarantool as PID 1. Its `jit.p` and `jit.profile`
modules are available. `misc.sysprof.available` is false on this ARM64 build;
Tarantool's pinned LuaJIT source disables sysprof for this architecture.

Stock `jit.p` provides `start(mode, filename)` and `stop()`. The helper uses
`vf3i10m0`: VM states, three Lua stack levels, a 10 ms CPU sampling interval and
no percentage cutoff. The pinned POSIX implementation uses
`ITIMER_PROF`/`SIGPROF`; it does not measure wall time or blocked WAL latency.
JIT inlining and delayed callbacks during native execution limit attribution.
Stock `jit.p` avoids a custom profiling callback: an error from a low-level
`jit.profile` callback terminates this runtime's process.

Coordinate exclusive use of the Lua profiler. The ownership guard detects this
helper's leases; it cannot prove that an unrelated profiler is inactive. Do not
change WAL settings, flush JIT traces, disable JIT, restart services or alter
workload semantics to collect a profile.

## Lua lease helper

Run from the repository root with Python 3; the helper needs only the standard
library and the Docker CLI. It uses the existing development operator endpoint
inside the selected Cairn container. This is tooling for the local development
Compose deployment, not arbitrary remote servers.

```sh
python3 -B scripts/profile_storage.py probe --node s0a
```

Probe all six nodes (`s0a`, `s0b`, `s0c`, `s1a`, `s1b`, `s1c`) and select the
current writable leader for each shard using `read_only` and `native_ping`.
Leadership can change, and the benchmark harness can recreate containers; check
again after its `loaded and initialized` line. Never assume the leaders are the
same as those in this evidence.

In a coordinated diagnostic window, use a fresh lease ID per node/capture:

```sh
PROFILE_LEASE=transfer-$(date -u +%Y%m%dt%H%M%Sz)
PROFILE_NODE=s0a # replace with the actual current leader
python3 -B scripts/profile_storage.py start --node "$PROFILE_NODE" --lease "$PROFILE_LEASE"
python3 -B scripts/profile_storage.py status --node "$PROFILE_NODE" --lease "$PROFILE_LEASE"
# Optional early stop, bound to this lease's saved ownership token.
python3 -B scripts/profile_storage.py stop --node "$PROFILE_NODE" --lease "$PROFILE_LEASE"
# After the automatic stop, or a successful explicit stop:
python3 -B scripts/profile_storage.py collect --node "$PROFILE_NODE" --lease "$PROFILE_LEASE"
```

Each start installs a server-side fiber that stops stock `jit.p` after 20
seconds. The timer checks its original lease object so it cannot stop a later
replacement lease. This deadline depends on the server event loop running; it
is not a hard watchdog for a wedged server. No `os.exit` runs in the storage
server. The short-lived native client exits explicitly after closing net.box.

Before EVAL, the helper checks the exact container name, running state and
Compose project/service labels. Exec/copy uses that immutable container ID;
owned operations reject a replacement incarnation. Native connect/wait is
bounded to 3 seconds, EVAL to 5 seconds and each Docker subprocess to 15 seconds.
An EVAL is never retried. The manifest is saved before start, so a timeout is
resolved with owned `status`/`stop`, never by replaying start or using a new ID.

Ownership tokens live in mode-0600 manifests under ignored
`.tools/profile-storage/leases/`, independent of the script directory. Initial
creation is exclusive, including concurrent starts. Keep those manifests
private until cleanup is complete. A process/container replacement invalidates
the saved ownership; do not bypass that guard or stop a replacement server's
profiler.

Collection requires a successful stop and writes
`artifacts/storage-profiles/<lease>/<node>.jit.txt` plus token-free provenance
JSON. Existing remote paths and local output are refused. Local collection
reserves its destination exclusively; a failed copy leaves a partial/reserved
file for inspection rather than silently overwriting it on another attempt.
Collect a lease before starting its replacement on the same node: the server
keeps only its latest lease record. Completed remote files are retained; this
helper does not delete arbitrary files or erase diagnostic evidence.

## Reproducing the workload

Start the following in one terminal. After `loaded and initialized`, probe the
six nodes and start the two leaders' Lua leases and native captures concurrently
from a second terminal or a log-triggered supervisor. The diagnostic used the
same lease ID on the two distinct nodes.

```sh
BENCH_WORKLOADS=transfer BENCH_CLIENTS=16 BENCH_SECONDS=20 \
BENCH_WARMUP=2 BENCH_REPEATS=1 CAIRN_PROFILE_RPC=1 \
python3 -u scripts/run_comparison.py --databases cairn --no-build \
  --cairn-commit volatile --cairn-connections 8 \
  --cairn-cpu-layout storage --cairn-pipeline
```

`--no-build` skips the driver's image build; the current harness still calls
Compose with `--build` for service setup. All Rust/native build stages were
cached in this diagnostic. Do that setup before profiling and record any
recreation. Do not run other tests, builds, fault injections or comparative
measurements concurrently.

The driver prints that line before each worker's two-second warmup. These
20-second captures consequently overlap warmup and most of the measured
interval, rather than matching the complete measured 20 seconds exactly. Record
actual start/stop times and avoid treating the benchmark rate as a rate for an
identical profiling window. The harness leaves its selected service environment
in place; use ordinary unprofiled settings, including `CAIRN_PROFILE_RPC=0`, for
subsequent performance comparisons.

## Native Linux perf capture

The optional [collector recipe](../bench/perf.Dockerfile) contains packaged ARM64
Debian tools, without a database or source build. Prepare it before the
profiling window:

```sh
docker build --platform linux/arm64 -f bench/perf.Dockerfile -t cairn-perf-collector:local .
COLLECTOR_ID=$(docker image inspect --format '{{.Id}}' cairn-perf-collector:local)
```

The base digest is pinned; Debian package repositories can change. This recipe
is not a claim of bit-for-bit image reproducibility. Record the actual immutable
image ID, package inventory, perf build options and tool hashes from
`/opt/collector-evidence/`. The diagnostic used image
`sha256:cc31e75f891c0cf0c90c6e3708e9129f28d962699cd4dd038b0b025d4b17ad60`,
perf `6.1.190`, libdw `0.188-2.1`, libunwind `1.6.2-3` and binutils `2.40-2`.
DWARF, libelf, libunwind and libdw unwind support were enabled. The image ID
identifies that local build; it is not a public registry reference.

Get the target ID through the helper's identity guard, not an unchecked name.
The following template captures one leader; launch an equivalent capture for
the other actual leader in parallel. Use a fresh output directory and retain
both exit status and stderr.

```sh
TARGET_NODE=s0a # replace with the verified current leader
TARGET_ID=$(python3 -B -c 'import runpy,sys; h=runpy.run_path("scripts/profile_storage.py"); print(h["container"](sys.argv[1])["id"])' "$TARGET_NODE")
PROFILE_DIR="$PWD/artifacts/storage-profiles/$PROFILE_LEASE/$TARGET_NODE-native"
umask 077
mkdir -p "$PWD/artifacts/storage-profiles/$PROFILE_LEASE"
mkdir "$PROFILE_DIR" || exit 1

CAPTURE_STATUS=0
docker run --rm --name "cairn-profile-$PROFILE_LEASE-$TARGET_NODE" \
  --label "cairn.profile.lease=$PROFILE_LEASE" --cidfile "$PROFILE_DIR/collector.cid" \
  --pid="container:$TARGET_ID" --network=none --cap-add=PERFMON \
  --mount "type=bind,src=$PROFILE_DIR,dst=/capture" --env LC_ALL=C \
  --entrypoint perf "$COLLECTOR_ID" \
  record -e cpu-clock -F 99 --call-graph dwarf,8192 -p 1 \
  -o /capture/storage-perf.data -- /bin/sleep 20 \
  > "$PROFILE_DIR/capture.log" 2>&1 || CAPTURE_STATUS=$?
printf '%s\n' "$CAPTURE_STATUS" > "$PROFILE_DIR/capture.exit"
```

`cpu-clock` avoids assumptions about a virtualized ARM PMU. `-p 1` captures the
Tarantool process's current threads in its shared PID namespace. Sleep bounds
the recording interval. `--rm` removes the temporary collector on normal exit;
it does not stop or reconfigure storage.

Use the default seccomp policy and only `PERFMON`. Docker 28.0.4's default
capability-aware policy permits `perf_event_open` with that capability; other
Docker versions, custom seccomp/LSM policy, permissions or perf/kernel versions
can differ. If capture is unavailable, preserve the error and finish the Lua
capture. Do not expand to `SYS_ADMIN`, `SYSLOG`, privileged mode or unconfined
seccomp. The observed perf 6.1/kernel 6.10 combination recorded and decoded
successfully; that does not establish compatibility for every environment.

For an early native stop, use the collector ID saved in its private `cidfile`.
First inspect that exact ID and require its `cairn.profile.lease` label to equal
this lease, then send `docker kill --signal INT` to that collector ID only.
Never signal the storage container or PID 1 through a joined namespace. After
completion, confirm no collector with this lease label remains, and run helper
`probe` on all six nodes to confirm no owned Lua lease remains active. If a
lease needs early cleanup, use only its saved token/incarnation-bound `stop`.

```sh
COLLECTOR_CONTAINER_ID=$(cat "$PROFILE_DIR/collector.cid")
OWNED_LEASE=$(docker inspect --format '{{ index .Config.Labels "cairn.profile.lease" }}' "$COLLECTOR_CONTAINER_ID")
if [ "$OWNED_LEASE" = "$PROFILE_LEASE" ]; then
  docker kill --signal INT "$COLLECTOR_CONTAINER_ID"
else
  echo 'Collector ownership check failed; refusing signal' >&2
  exit 1
fi
```

## Exact symbols and offline decode

After capture, while the same checked storage container is alive, save its
`/proc/1/maps`, executable and mapped shared libraries. Keep those files private.
Recheck the immutable target ID through the identity guard before copying.
Preserve each mapped file's SHA-256 and ELF build ID. For this image, the mapped
files were `/usr/bin/tarantool` and the ARM64 loader, libc, libdl, libgcc_s,
libm, libpthread and librt under `/usr/lib/aarch64-linux-gnu/`.

Copy exact target files with `docker cp -L` into the corresponding path below
`$PROFILE_DIR/symfs/`; for example:

```sh
mkdir -p "$PROFILE_DIR/symfs/usr/bin"
docker cp -L "$TARGET_ID:/usr/bin/tarantool" "$PROFILE_DIR/symfs/usr/bin/tarantool"
# Copy each mapped, non-deleted /lib or /usr/lib DSO to its matching symfs path.
```

Do not substitute the collector's libc or LuaJIT. The captured Tarantool
executable has `.symtab` and `.debug_info`; its hash and build ID are in the
published provenance. Initial decoding through `/proc/1/root` did not resolve
user symbols correctly. Exact exported symfs decoding resolved them; only that
decode supports the published user attribution.

Decode existing data without joining the target namespace or granting
capabilities:

```sh
docker run --rm --network=none --cap-drop=ALL --read-only \
  --mount "type=bind,src=$PROFILE_DIR,dst=/capture,readonly" \
  --env LC_ALL=C --entrypoint perf "$COLLECTOR_ID" \
  report --stdio --header --no-children --sort comm,dso,symbol -n \
  --symfs /capture/symfs -i /capture/storage-perf.data \
  > "$PROFILE_DIR/offline.report.txt" 2> "$PROFILE_DIR/offline.report.stderr"
```

The same container invocation can run `script --symfs /capture/symfs -i
/capture/storage-perf.data` or `buildid-list -i /capture/storage-perf.data`.
`perf script` output includes addresses and captured user-stack information;
retain it privately. DWARF stacks can stop at LuaJIT/fiber boundaries. Kernel
symbols were restricted, so kernel leaves remain unresolved. Do not infer
kernel names from guessed symbols or grant additional privileges to remove
that limitation.

## Observed CPU attribution and limits

The two captures had 1,156 and 1,161 native samples, with zero lost samples.
The following percentages use all captured native CPU samples per node:

| Thread | s0a | s1b |
| --- | ---: | ---: |
| Transaction thread | 60.2% | 59.9% |
| Replication relays | 21.5% | 21.4% |
| IPROTO | 9.3% | 9.0% |
| WAL | 9.1% | 9.7% |

Kernel-leaf samples account for 43.3%/44.7%. Their first resolved user caller is
`evpipe_write` in 252/257 samples (21.8%/22.1% of all samples), and `sio_writev`
in 136/146 (11.8%/12.6%). These are caller contexts, not named kernel routines
or exclusive native function self time. They justify investigating
notification, request/phase count and transport overhead; they do not prove
that every notification is cbus or that it can be removed.

Native stacks containing GC helpers occur in 131/121 samples (11.3%/10.4%).
Allocation, string/table, tuple/index and MessagePack work are visible. Native
crypto routines are present, but the narrow named crypto-stack category is
26/25 samples (2.2%/2.2%); wrapper/allocation work can appear elsewhere. Stack
presence categories overlap and must not be summed as independent costs.

The separate Lua VM profile reports 58%/61% C state, 24%/22% compiled state,
10%/9% GC, 7%/7% interpreted state and 1%/0% JIT. These are rounded values.
Within the compiled subgroup, rows matching `hmac_new|sha256|certificate` sum
to 31%/30%; weighting by that subgroup gives about 7.4%/6.6% of **Lua VM
samples**, not 30% of total CPU and not a whole-process native CPU fraction.
Preparation, finishing, claims and certificate construction appear throughout
the Lua call sites. Reducing repeated metadata/tuple allocations and examining
authenticated certificate setup are candidates for separately validated work;
profiling does not authorize weakening durable claims or authentication.

The 19 full one-second RPC counter dumps inside the capture show prepare calls
averaging about 2.09 ms and home finish-last calls about 2.64 ms. Those are
concurrent end-to-end RPC durations, including queue/network/server time, not
CPU costs or additive SQL critical-path time.

The profiled driver completed 26,429 transfers at 1,321 ops/s, with 138
serialization retries and successful per-key/sum checks. This one instrumented
run is diagnostic only. No profiler-overhead control, cgroup CPU delta,
syscall-duration or off-CPU capture was collected. WAL-thread CPU includes
compression, write and notification work; it is not a WAL wait fraction.
Neither these CPU samples nor RPC durations establish that fsync latency or
HMAC dominates throughput.

## Evidence and validation

[The evidence manifest](../bench/results/storage-profile-2026-10-25/manifest.json)
labels every copy as sanitized and retains SHA-256 hashes of the private raw
inputs. Lua sanitization replaces native entry addresses with `[address
removed]`; rounded percentages and source-relative call sites are unchanged.
Native publication contains only aggregate counts. Private lease tokens,
container IDs, network addresses, host paths, perf.data, register/user-stack
dumps and exact executable/library copies are excluded. Original source and
result hashes validate; all 154 recorded source-member hashes matched the
private source archive. Public aggregate files have their own hashes.

The helper's offline checks are part of `make check` and require no Docker,
network, profiler or Python third-party packages:

```sh
python3 -B tests/profile_storage.py
```

Fifteen checks cover identity guards, local bounded EVAL, no replay, private and
exclusive manifest creation, simultaneous starts, token/incarnation-bound
stop, collection completion and overwrite races. The predecessor helper's
20-second Lua leases and native capture were exercised in this diagnostic;
publication ownership/path hardening has offline coverage. No new live capture
was run to prepare these published copies.

## Upstream references

- [Tarantool 3.3.5's pinned LuaJIT](https://github.com/tarantool/tarantool/tree/3.3.5/third_party/luajit)
- [Pinned architecture restrictions](https://github.com/tarantool/luajit/blob/f791ad0bf1f5b4f3fea768def9304336d7253af8/src/lj_arch.h)
- [Stock jit.p options and subgroup output](https://github.com/tarantool/luajit/blob/f791ad0bf1f5b4f3fea768def9304336d7253af8/src/jit/p.lua)
- [Profiling callback behavior](https://github.com/tarantool/luajit/blob/f791ad0bf1f5b4f3fea768def9304336d7253af8/src/lib_jit.c)
- [POSIX profiling timer](https://github.com/tarantool/luajit/blob/f791ad0bf1f5b4f3fea768def9304336d7253af8/src/lj_profile_timer.c)
- [Docker 28.0.4 default seccomp](https://github.com/moby/moby/blob/v28.0.4/profiles/seccomp/default_linux.go)
- [Linux 6.10 perf record options](https://github.com/torvalds/linux/blob/v6.10/tools/perf/Documentation/perf-record.txt)
- [Linux 6.10 perf report options](https://github.com/torvalds/linux/blob/v6.10/tools/perf/Documentation/perf-report.txt)
