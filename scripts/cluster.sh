#!/bin/sh
# Manage the supplied two-shard deployments without deleting their data.
set -eu
cd "$(dirname "$0")/.."
action=${1:-status}
if [ "$#" -gt 0 ]; then shift; fi
replicas=1
port=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -n) [ "${2:-}" = 2 ] || { echo 'The supplied Compose clusters define two shards.' >&2; exit 2; }; shift 2 ;;
        -r) replicas=${2:?missing replica count}; shift 2 ;;
        -p) port=${2:?missing PostgreSQL port}; shift 2 ;;
        *) echo 'Usage: scripts/cluster.sh start|stop|status|logs [-n 2] [-r 1|3] [-p port]' >&2; exit 2 ;;
    esac
done
case "$replicas" in
    1) configuration=compose.yaml; default_port=55432 ;;
    3) configuration=compose.replication.yaml; default_port=55433 ;;
    *) echo 'Replica count must be 1 or 3.' >&2; exit 2 ;;
esac
if [ -n "$port" ]; then
    case "$port" in *[!0-9]*|'') echo 'Port must be an integer.' >&2; exit 2 ;; esac
    [ "$port" -ge 1 ] && [ "$port" -le 65535 ] || { echo 'Port must be from 1 to 65535.' >&2; exit 2; }
fi
export CAIRN_PG_PORT=${port:-$default_port}
case "$action" in
    start) exec docker compose -f "$configuration" up -d --build --wait gateway ;;
    stop) exec docker compose -f "$configuration" stop ;;
    status) exec docker compose -f "$configuration" ps ;;
    logs) exec docker compose -f "$configuration" logs --tail 100 ;;
    *) echo 'Action must be start, stop, status or logs.' >&2; exit 2 ;;
esac
