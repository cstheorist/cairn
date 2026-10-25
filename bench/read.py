"""Closed-loop read benchmark using independent psycopg and native iproto clients.

The Python client, Docker networking, and this machine are part of each measurement.
These are smoke measurements, not claims about competing databases.
"""
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import json
import os
import platform
import random
import threading
import time
import uuid

import psycopg
import tarantool


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def run(mode, args, table):
    barrier = threading.Barrier(args.clients + 1)
    start_event = threading.Event()
    end_at = [0.0]

    def worker(number):
        rng = random.Random(42 + number)
        if mode == "sql":
            connection = psycopg.connect(os.environ["CAIRN_DSN"], autocommit=True, prepare_threshold=0)
            cursor = connection.cursor(binary=True)
            def get(key):
                cursor.execute(f"SELECT id, value FROM {table} WHERE id = %s", (key,))
                return cursor.fetchone()
            close = connection.close
        else:
            connections = [tarantool.Connection(f"shard{shard}", 3301, user="reader", password="local-reader") for shard in range(2)]
            def get(key):
                return connections[key % 2].select(table, [key]).data[0]
            def close():
                for connection in connections: connection.close()
        samples = []
        count = 0
        try:
            for _ in range(100):
                key = rng.randrange(args.rows)
                assert get(key)[0] == key
            barrier.wait(timeout=30)
            start_event.wait(timeout=30)
            while time.perf_counter() < end_at[0]:
                key = rng.randrange(args.rows)
                begin = time.perf_counter_ns()
                row = get(key)
                elapsed = time.perf_counter_ns() - begin
                assert row[0] == key and row[1] == "x" * args.payload
                samples.append(elapsed / 1000)
                count += 1
        finally:
            close()
        return count, samples

    with ThreadPoolExecutor(max_workers=args.clients) as executor:
        tasks = [executor.submit(worker, index) for index in range(args.clients)]
        barrier.wait(timeout=30)
        started = time.perf_counter()
        end_at[0] = started + args.seconds
        start_event.set()
        results = [task.result() for task in tasks]
        duration = time.perf_counter() - started
    samples = [sample for _, values in results for sample in values]
    return {"mode": mode, "operations": sum(count for count, _ in results), "elapsed_s": duration,
            "ops_per_s": sum(count for count, _ in results) / duration,
            "latency_us": {"p50": percentile(samples, .5), "p95": percentile(samples, .95), "p99": percentile(samples, .99)}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--payload", type=int, default=128)
    parser.add_argument("--output", default="/artifacts/read-benchmark.json")
    args = parser.parse_args()
    assert 1 <= args.clients <= 128 and args.rows > 0 and args.seconds > 0 and 0 <= args.payload <= 65536
    table = "bench_" + uuid.uuid4().hex[:10]
    with psycopg.connect(os.environ["CAIRN_DSN"], autocommit=True, prepare_threshold=0) as connection:
        connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        for key in range(args.rows):
            connection.execute(f"INSERT INTO {table} VALUES (%s, %s)", (key, "x" * args.payload))
    report = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), "server": "cairn 0.1.0", "tarantool": "3.3.5",
              "workload": "uniform random point reads, warm cache, closed loop, no think time", "rows": args.rows,
              "payload_bytes": args.payload, "clients": args.clients, "target_s": args.seconds,
              "python": platform.python_version(), "platform": platform.platform(), "shards": 2,
              "replicas_per_shard": 1, "wal_mode": "fsync", "client_libraries": {"psycopg": psycopg.__version__, "tarantool": tarantool.__version__},
              "results": [run(mode, args, table) for mode in ["sql", "native"]],
              "limitations": ["Python/client overhead included", "single-host Docker deployment", "no replication or failover", "not a YugabyteDB/TiDB comparison", "latencies omit queueing in an open-loop arrival process", "read-only timing; writes not benchmarked"]}
    with open(args.output, "w") as output:
        json.dump(report, output, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__": main()
