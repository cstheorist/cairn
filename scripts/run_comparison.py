#!/usr/bin/env python3
"""Run isolated, resource-matched development benchmarks and retain evidence."""
import argparse
import datetime
import json
import hashlib
import io
import os
from pathlib import Path
import subprocess
import time
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
HA = ["docker","compose","-f","compose.replication.yaml","-f","bench/resources.yaml"]
COMPARISON = ["docker","compose","-f","compose.comparison.yaml"]
FAST = ["gateway","s0a","s0b","s0c","s1a","s1b","s1c"]
YB = ["yb1","yb2","yb3"]
TIDB = ["pd0","pd1","pd2","tikv0","tikv1","tikv2","tidb"]
ARTIFACTS = ROOT/"artifacts"

def source_archive():
    directories=['crates','lua','bench/driver/src','tests','scripts','tla']
    paths={path for directory in directories for path in (ROOT/directory).rglob('*')
           if path.is_file() and not any(part in ['target','states','__pycache__'] for part in path.relative_to(ROOT).parts)
           and '_TTrace_' not in path.name}
    paths.update(ROOT/path for path in ['Cargo.toml','Cargo.lock','rust-toolchain.toml','rustfmt.toml','Dockerfile','Makefile','.dockerignore','.gitignore',
        'compose.yaml','compose.replication.yaml','compose.comparison.yaml','compose.votes.yaml',
        'bench/resources.yaml','bench/resources-storage.yaml','bench/tikv.toml','bench/tikv.Dockerfile','bench/yugabyte-start.sh','bench/yugabyte.Dockerfile',
        'bench/driver/Cargo.toml','bench/driver/Cargo.lock','bench/driver/Dockerfile','bench/driver/.dockerignore'])
    contents={str(path.relative_to(ROOT)):path.read_bytes() for path in sorted(paths)}
    hashes={name:hashlib.sha256(data).hexdigest() for name,data in contents.items()}
    fingerprint=hashlib.sha256(json.dumps(hashes,sort_keys=True).encode()).hexdigest()
    archive=ARTIFACTS/f'source-{fingerprint[:16]}.tar.gz'
    if not archive.exists():
        with tarfile.open(archive,'w:gz') as output:
            for name,data in contents.items():
                info=tarfile.TarInfo(name)
                info.size=len(data)
                info.mode=0o644
                output.addfile(info,io.BytesIO(data))
    return {'source_sha256':hashes,'source_archive':archive.name,
            'source_archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}

def driver_fingerprint():
    directory=ROOT/'bench/driver'
    paths=[directory/'Cargo.toml',directory/'Cargo.lock',directory/'Dockerfile',directory/'.dockerignore',
           *sorted((directory/'src').rglob('*.rs'))]
    hashes={str(path.relative_to(directory)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    return hashlib.sha256(json.dumps(hashes,sort_keys=True).encode()).hexdigest()

def run(arguments, capture=False, env=None):
    result = subprocess.run(arguments,cwd=ROOT,check=True,text=True,
                            stdout=subprocess.PIPE if capture else None,env=env)
    return result.stdout if capture else None

def snapshot(compose, services):
    ids = run([*compose,"ps","-q",*services],True).split()
    if len(ids)!=len(services): raise AssertionError("missing benchmark services")
    records = json.loads(run(["docker","inspect",*ids],True))
    return [{
        "service":record["Config"]["Labels"]["com.docker.compose.service"],
        "image":record["Config"]["Image"],"image_id":record["Image"],
        "cpu_limit":record["HostConfig"]["NanoCpus"]/1e9,
        "memory_limit_bytes":record["HostConfig"]["Memory"],
        "oom_killed":record["State"]["OOMKilled"],
        "state":record["State"]["Status"],"restarts":record["RestartCount"],
        "started_at":record["State"]["StartedAt"],
    } for record in records]

def fast_metadata(cpu_layout='standard', requested_read_batch_us=None):
    nodes = []
    for node in FAST[1:]:
        code = ("local c=require('net.box').connect('reader:local-reader@127.0.0.1:3301'); "
                "print(require('json').encode({c:call('cairn.ping')})); c:close()")
        data = json.loads(run([*HA,"exec","-T",node,"env","-u","TT_INSTANCE_NAME","-u","TT_APP_NAME","tarantool","-e",code],True))
        if data[4] is not True or data[5]!=2: raise AssertionError((node,data))
        nodes.append({"node":node,"ping":data})
    for shard in (0,1):
        if sum(item["ping"][3] for item in nodes if item["ping"][0]==shard)!=1:
            raise AssertionError("expected one leader per shard")
    environment=json.loads(run(['docker','inspect','cairn-ha-gateway-1','--format','{{json .Config.Env}}'],True))
    settings={entry.split('=',1)[0]:entry.split('=',1)[1] for entry in environment
              if entry.split('=',1)[0] in ['CAIRN_VOTE_COMMIT','CAIRN_RECOVERY','TOKIO_WORKER_THREADS','CAIRN_LOG_ERRORS','CAIRN_PROFILE_RPC','CAIRN_STORAGE_CONNECTIONS','CAIRN_STORAGE_PIPELINE','CAIRN_READ_BATCH_US']}
    result = {"nodes":nodes,"gateway_settings":settings,"cpu_layout":cpu_layout,"wal_mode":"fsync","replica_count":3,"quorum":2,
              "reads":"linearizable","transaction_isolation":"strict two-phase locking"}
    if requested_read_batch_us is not None:
        actual = settings.get('CAIRN_READ_BATCH_US')
        if actual is None or read_batch_microseconds(actual) != requested_read_batch_us:
            raise AssertionError(f'CAIRN_READ_BATCH_US requested {requested_read_batch_us}, actual {actual!r}')
        result['requested_read_batch_us'] = requested_read_batch_us
    return result

def yb_metadata():
    raw = run([*COMPARISON,"exec","-T","yb1","/home/yugabyte/bin/yb-admin",
               "--master_addresses","yb1:7100,yb2:7100,yb3:7100","get_universe_config"],True)
    config = json.loads(raw)
    def contains_rf3(value):
        if isinstance(value,dict):
            return any((key in ("num_replicas","numReplicas") and item==3) or contains_rf3(item)
                       for key,item in value.items())
        if isinstance(value,list): return any(contains_rf3(item) for item in value)
        return False
    if not contains_rf3(config): raise AssertionError("YugabyteDB RF3 not configured")
    flags = {}
    for node in YB:
        text = run([*COMPARISON,"exec","-T",node,"curl","-fsS",f"http://{node}:9000/varz?raw"],True)
        if "--durable_wal_write=true" not in text: raise AssertionError(f"{node}: durable WAL not enabled")
        flags[node] = [line for line in text.splitlines()
                       if any(name in line for name in ["durable_wal_write=","never_fsync=","enable_follower_reads=","enable_wait_queues=","yb_enable_read_committed_isolation="])]
    return {"universe_config":config,"durability_flags":flags,"replica_count":3,"quorum":2,
            "transaction_isolation":os.environ['YB_ISOLATION'].replace('-',' '),"conflict_policy":"fail on conflict"}

def tidb_metadata():
    # The PD HTTP endpoint is reached from the benchmark network without
    # publishing a host port. Python's standard library is the only dependency.
    code = ("import json,urllib.request; "
            "print(json.dumps({k:json.load(urllib.request.urlopen('http://pd0:2379/pd/api/v1/'+v)) "
            "for k,v in [('replication','config/replicate'),('stores','stores'),('missing_peers','regions/check/miss-peer'),('pending_peers','regions/check/pending-peer')]}))")
    for attempt in range(120):
        result = json.loads(run(["docker","run","--rm","--network","cairn-comparison_default",
                                "python:3.12-slim","python","-c",code],True))
        if result["missing_peers"]["count"]==0 and result["pending_peers"]["count"]==0: break
        if attempt%10==0: print("waiting for TiKV replica placement",flush=True)
        time.sleep(2)
    else: raise AssertionError("TiKV regions did not reach full replica placement")
    if result["replication"]["max-replicas"]!=3: raise AssertionError("TiDB RF3 not configured")
    if len(result["stores"]["stores"])!=3: raise AssertionError("expected three TiKV stores")
    run(["docker","cp","cairn-comparison-tikv0-1:/cairn-build-evidence.txt",str(ARTIFACTS/"tikv-build-evidence.txt")])
    result["tikv_debug_stripping"]=(ARTIFACTS/"tikv-build-evidence.txt").read_text()
    return {**result,"sync_log":True,"reads":"leader/strong, no stale-read setting",
            "transaction_isolation":"repeatable read with pessimistic writes"}

def wait_tidb():
    code = ("import urllib.request; urllib.request.urlopen('http://tidb:10080/status',timeout=3).read()")
    for attempt in range(120):
        result = subprocess.run(["docker","run","--rm","--network","cairn-comparison_default",
                                 "python:3.12-slim","python","-c",code],cwd=ROOT,
                                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        if result.returncode==0: return
        if attempt%10==0: print("waiting for TiDB startup",flush=True)
        time.sleep(1)
    raise RuntimeError("TiDB did not become ready")

def measure(database):
    environment = dict(os.environ,DB_LABEL=database,DB_KIND="mysql" if database=="tidb" else "pg")
    environment["DB_URL"] = {
        "cairn":"host=gateway port=5432 user=cairn dbname=cairn password=local-postgres",
        "yugabyte":"host=yb1 port=5433 user=yugabyte dbname=yugabyte",
        "tidb":"mysql://root@tidb:4000/test",
    }[database]
    run([*COMPARISON,"--profile","load","run","--no-deps","--rm","load"],env=environment)

def read_batch_microseconds(value):
    try:
        micros = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError('read batch window must be an integer from 0 to 1000') from error
    if not 0 <= micros <= 1000:
        raise argparse.ArgumentTypeError('read batch window must be from 0 to 1000 microseconds')
    return micros


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--databases",default="cairn,yugabyte,tidb")
    parser.add_argument("--no-build",action="store_true")
    parser.add_argument("--cairn-commit",choices=['legacy','votes','local-finish','volatile'],default='legacy')
    parser.add_argument('--yugabyte-isolation',choices=['repeatable-read','serializable','read-committed'],default='repeatable-read')
    parser.add_argument('--cairn-connections',type=int,choices=range(1,65),default=8)
    parser.add_argument('--cairn-cpu-layout',choices=['standard','storage'],default='standard')
    parser.add_argument('--cairn-pipeline',action='store_true')
    parser.add_argument('--cairn-read-batch-us',type=read_batch_microseconds,default=50,metavar='US',
                        help='public point-read collection window, 0..1000 microseconds (default: 50)')
    options = parser.parse_args()
    os.environ['YB_ISOLATION']=options.yugabyte_isolation
    os.environ['CAIRN_STORAGE_CONNECTIONS']=str(options.cairn_connections)
    os.environ['CAIRN_STORAGE_PIPELINE']='1' if options.cairn_pipeline else '0'
    os.environ['CAIRN_READ_BATCH_US']=str(options.cairn_read_batch_us)
    if options.cairn_cpu_layout=='storage':
        HA.extend(['-f','bench/resources-storage.yaml'])
    if options.cairn_commit!='legacy' or options.cairn_connections!=8:
        HA.extend(['-f','compose.votes.yaml'])
        os.environ['CAIRN_VOTE_COMMIT']={'legacy':'0','votes':'1','local-finish':'2','volatile':'3'}[options.cairn_commit]
    databases = options.databases.split(",")
    if len(set(databases))!=len(databases) or any(name not in {"cairn","yugabyte","tidb"} for name in databases):
        parser.error("databases must be distinct names from cairn,yugabyte,tidb")
    ARTIFACTS.mkdir(exist_ok=True)
    os.environ['BENCH_SOURCE_SHA256']=driver_fingerprint()
    if not options.no_build: run([*COMPARISON,"--profile","load","build","load"])
    image=json.loads(run(['docker','image','inspect','cairn-comparison-load'],True))[0]
    if (image['Config'].get('Labels') or {}).get('cairn.benchmark.source-sha256')!=os.environ['BENCH_SOURCE_SHA256']:
        raise AssertionError('load image does not match driver sources; rerun without --no-build')
    evidence_path=ARTIFACTS/"comparison-environment.json"
    previous=json.loads(evidence_path.read_text()) if evidence_path.exists() else {}
    evidence = {"started_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "docker":json.loads(run(["docker","info","--format","{{json .}}"],True)),
                "resource_budget":{"database_cpus":6,"database_memory_bytes":6*1024**3,
                                   "client_cpus":4,"client_memory_bytes":768*1024**2},
                "load_image":run(["docker","image","inspect","cairn-comparison-load","--format","{{.Id}}"],True).strip(),
                "databases":previous.get("databases",{})}
    evidence['load_binary_sha256']=run(['docker','run','--rm','--network','none','--entrypoint','sha256sum',
        evidence['load_image'],'/usr/local/bin/cairn-benchmark'],True).split()[0]
    # Retain only neutral host characteristics; Docker info also has unrelated
    # runtime/plugin details that do not describe this experiment.
    evidence["docker"] = {key:evidence["docker"][key] for key in
                          ["ServerVersion","Architecture","NCPU","MemTotal","KernelVersion","OperatingSystem"]}
    try:
        run([*COMPARISON,"--profile","yugabyte","--profile","tidb","stop",*YB,*TIDB])
        for database in databases:
            evidence["databases"][database]={"status":"preparing","started_utc":datetime.datetime.now(datetime.timezone.utc).isoformat()}
            evidence_path.write_text(json.dumps(evidence,indent=2))
            if database=="cairn":
                run([*HA,"up","-d","--build","--wait","gateway"])
                metadata = fast_metadata(options.cairn_cpu_layout, options.cairn_read_batch_us)
                topology = snapshot(HA,FAST)
            else:
                run([*HA,"stop",*FAST])
                if database=="yugabyte":
                    run([*COMPARISON,"--profile","yugabyte","build","yb1"])
                    run([*COMPARISON,"--profile","yugabyte","up","-d","--wait",*YB])
                    run([*COMPARISON,"exec","-T","yb1","/home/yugabyte/bin/yugabyted","configure",
                         "data_placement","--base_dir=/data","--fault_tolerance=zone","--rf=3"])
                    metadata = yb_metadata()
                    topology = snapshot(COMPARISON,YB)
                else:
                    run([*COMPARISON,"--profile","tidb","build","tikv0"])
                    run([*COMPARISON,"--profile","tidb","up","-d",*TIDB])
                    wait_tidb()
                    metadata = tidb_metadata()
                    topology = snapshot(COMPARISON,TIDB)
            if abs(sum(item["cpu_limit"] for item in topology)-6)>0.001:
                raise AssertionError("CPU budget mismatch")
            if sum(item["memory_limit_bytes"] for item in topology)!=6*1024**3:
                raise AssertionError("memory budget mismatch")
            metadata["containers"] = topology
            metadata["measured_utc"]=datetime.datetime.now(datetime.timezone.utc).isoformat()
            metadata["status"]="running"
            metadata["load_image"]=evidence["load_image"]
            metadata['load_binary_sha256']=evidence['load_binary_sha256']
            metadata.update(source_archive())
            evidence["databases"][database]=metadata
            evidence_path.write_text(json.dumps(evidence,indent=2))
            try:
                measure(database)
            except Exception as error:
                metadata["measurement_error"]=str(error)
                metadata["status"]="failed"
                evidence_path.write_text(json.dumps(evidence,indent=2))
                raise
            metadata["containers_after"] = snapshot(HA if database=="cairn" else COMPARISON,
                                                     FAST if database=="cairn" else YB if database=="yugabyte" else TIDB)
            if database=="cairn":
                metadata["topology_after"]=fast_metadata(options.cairn_cpu_layout, options.cairn_read_batch_us)
            before_by_service={item["service"]:item for item in metadata["containers"]}
            failure = None
            if database=="cairn" and metadata["nodes"]!=metadata["topology_after"]["nodes"]:
                failure="database leadership changed during benchmark"
            if any(item["oom_killed"] for item in metadata["containers_after"]): failure="database OOM"
            if any(before_by_service[after["service"]]["restarts"]!=after["restarts"]
                   or before_by_service[after["service"]]["started_at"]!=after["started_at"] or after["state"]!="running"
                   for after in metadata["containers_after"]): failure="database restarted during benchmark"
            if failure:
                metadata["status"]="failed"
                metadata["measurement_error"]=failure
                evidence_path.write_text(json.dumps(evidence,indent=2))
                raise AssertionError(failure)
            metadata["result_sha256"]=hashlib.sha256((ARTIFACTS/f"{database}-benchmark.json").read_bytes()).hexdigest()
            metadata["status"]="accepted"
            evidence["databases"][database] = metadata
            evidence_path.write_text(json.dumps(evidence,indent=2))
            if database!="cairn": run([*COMPARISON,"stop",*(YB if database=="yugabyte" else TIDB)])
    finally:
        run([*COMPARISON,"--profile","yugabyte","--profile","tidb","stop",*YB,*TIDB])
        run([*HA,"up","-d","--wait","gateway"])

if __name__=="__main__": main()
