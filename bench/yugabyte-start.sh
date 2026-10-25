#!/bin/sh
set -eu
node="$1"
shift
# Fresh joiners need the initial node's metadata API. Persisted RF3 members
# must start together so their existing master quorum can form.
if [ "$#" -gt 0 ]; then
    initialized="$(python3 -c "import json,os; p='/data/conf/yugabyted.conf'; print(int(os.path.exists(p) and json.load(open(p)).get('cluster_member',False)))")"
    if [ "$initialized" != 1 ]; then
        join_node="$(printf '%s' "$1" | sed 's/^--join=//')"
        attempts=0
        until curl -fsS "http://$join_node:9000/api/v1/masters" >/dev/null 2>&1; do
            attempts=$((attempts+1))
            if [ "$attempts" -ge 120 ]; then echo "join endpoint unavailable: $join_node" >&2; exit 1; fi
            sleep 1
        done
    fi
fi
exec /home/yugabyte/bin/yugabyted start \
    --base_dir=/data --advertise_address="$node" --background=false --ui=false \
    --insecure --fault_tolerance=zone --cloud_location="bench.local.$node" \
    --tserver_flags="durable_wal_write=true,memory_limit_hard_bytes=1073741824,ysql_pg_conf_csv={shared_buffers=64MB,max_connections=100},start_cql_proxy=false,callhome_enabled=false,enable_wait_queues=false" \
    --master_flags="durable_wal_write=true,memory_limit_hard_bytes=268435456,callhome_enabled=false" "$@"
