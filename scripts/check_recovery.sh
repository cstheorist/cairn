#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p artifacts
docker compose --profile test run --rm tests python /tests/recovery.py seed
# Always restore the stopped development shard, even when an assertion fails.
trap 'docker compose up -d shard1' EXIT
docker compose kill -s SIGKILL shard1
docker compose --profile test run --no-deps --rm tests python /tests/recovery.py unavailable
docker compose up -d --wait shard1
docker compose --profile test run --no-deps --rm tests python /tests/recovery.py recover
