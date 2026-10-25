"""Real replica-set tests. Fault injection is performed by scripts/check_replication.py."""
from contextlib import closing
import json
import os
import sys
import time
import uuid

import psycopg
import tarantool

DSN = os.environ["CAIRN_DSN"]
STATE = "/artifacts/replication-state.json"


def connect():
    return psycopg.connect(DSN, autocommit=True, prepare_threshold=0)


def native(node, user="reader"):
    return tarantool.Connection(node, 3301, user=user, password=f"local-{user}", socket_timeout=2, connection_timeout=1)


def status(node):
    with closing(native(node)) as connection:
        data = connection.call("cairn.ping").data
        return {"node": node, "shard": data[0], "count": data[1], "term": data[2], "writable": data[3]}


def leader(shard, excluded=()):
    for _ in range(100):
        candidates = []
        for letter in "abc":
            node = f"s{shard}{letter}"
            if node in excluded: continue
            try:
                info = status(node)
                if info["writable"]: candidates.append(info)
            except (tarantool.Error, OSError):
                pass
        if len(candidates) == 1: return candidates[0]
        time.sleep(.1)
    raise AssertionError(f"no unique writable leader for shard {shard}: {candidates}")


def read(connection, table, key):
    return connection.execute(f"SELECT * FROM {table} WHERE id = %s", (key,)).fetchone()


def read_after_election(connection, table, key):
    for attempt in range(60):
        try:
            return read(connection, table, key)
        except psycopg.Error as error:
            if error.sqlstate not in ("08006", "57P03", "XX000") or attempt == 59: raise
            time.sleep(.1)


def replicated_value(table, key, expected, excluded=()):
    for letter in "abc":
        node = f"s{key % 2}{letter}"
        if node in excluded: continue
        for attempt in range(60):
            try:
                with closing(native(node)) as connection:
                    actual = connection.select(table, [key]).data
                if actual == expected: break
            except (tarantool.Error, OSError): pass
            if attempt == 59: raise AssertionError((node, actual, expected))
            time.sleep(.1)


def main(mode):
    if mode == "seed":
        leaders = [leader(shard) for shard in range(2)]
        table = "replica_" + uuid.uuid4().hex[:10]
        with connect() as connection:
            connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
            for key in (0, 1):
                connection.execute(f"INSERT INTO {table} VALUES (%s, %s)", (key, f"ack-{key}"))
                assert read(connection, table, key) == (key, f"ack-{key}")
                replicated_value(table, key, [[key, f"ack-{key}"]])
        for info in leaders:
            with closing(native(info["node"], "operator")) as connection:
                assert connection.eval("local name=...; return box.space[name].is_sync, box.space._cairn_catalog.is_sync", table).data == [True, True]
            with closing(native(info["node"], "gateway")) as connection:
                try:
                    connection.call("cairn.insert", table, [10 + info["shard"], "stale-epoch"], info["term"] - 1)
                except tarantool.DatabaseError as error:
                    assert "[CAIRN:FENCED]" in str(error)
                else: raise AssertionError("stale leadership epoch accepted a write")
            with closing(native(info["node"])) as connection:
                assert connection.call("cairn.get", table, info["shard"], info["term"]).data == [[info["shard"], f"ack-{info['shard']}"]]
        with open(STATE, "w") as output: json.dump({"table": table, "initial": leaders}, output)
        print("replica seed: both shards, synchronous spaces, all six native replicas, strong native read and stale-epoch rejection passed", flush=True)
        return
    with open(STATE) as source: state = json.load(source)
    table = state["table"]
    with connect() as connection:
        if mode == "failover":
            old = state["initial"][0]
            current = leader(0, [old["node"]])
            assert current["term"] > old["term"]
            assert read_after_election(connection, table, 0) == (0, "ack-0")
            assert read(connection, table, 1) == (1, "ack-1")
            connection.execute(f"INSERT INTO {table} VALUES (2, 'after-failover')")
            assert read(connection, table, 2) == (2, "after-failover")
            state["after_failover"] = current
            with open(STATE, "w") as output: json.dump(state, output)
            print("leader death: automatic election and gateway rediscovery preserve acknowledged rows; new quorum writes succeed", flush=True)
        elif mode == "no_quorum":
            for sql in [f"INSERT INTO {table} VALUES (4, 'must-not-commit')", f"SELECT * FROM {table} WHERE id = 0"]:
                try: connection.execute(sql)
                except psycopg.Error as error:
                    assert error.sqlstate in ("08006", "57P03", "XX000"), error
                else: raise AssertionError("operation succeeded without a majority: " + sql)
            assert read(connection, table, 1) == (1, "ack-1")
            connection.execute(f"INSERT INTO {table} VALUES (3, 'independent-shard')")
            assert read(connection, table, 3) == (3, "independent-shard")
            print("quorum loss: both SQL writes and strong reads fail closed; the other shard stays available", flush=True)
        elif mode == "restored":
            leader(0)
            assert read_after_election(connection, table, 0) == (0, "ack-0")
            assert read(connection, table, 2) == (2, "after-failover")
            assert read(connection, table, 4) is None
            for key, value in [(0, "ack-0"), (2, "after-failover")]: replicated_value(table, key, [[key, value]])
            state["before_partition"] = leader(0)
            with open(STATE, "w") as output: json.dump(state, output)
            print("quorum restore: acknowledged data survives, failed minority write absent, replicas converge", flush=True)
        elif mode == "partition":
            old = state["before_partition"]
            current = leader(0, [old["node"]])
            assert current["term"] > old["term"]
            assert read_after_election(connection, table, 0) == (0, "ack-0")
            connection.execute(f"INSERT INTO {table} VALUES (6, 'majority-partition')")
            assert read(connection, table, 6) == (6, "majority-partition")
            print("network partition: connected majority elects and commits while old leader is isolated", flush=True)
        elif mode == "healed":
            leader(0)
            assert read_after_election(connection, table, 6) == (6, "majority-partition")
            replicated_value(table, 6, [[6, "majority-partition"]])
            print("partition heal: isolated old leader rejoins and converges without losing acknowledged writes", flush=True)
        else: raise ValueError(mode)


if __name__ == "__main__": main(sys.argv[1] if len(sys.argv) > 1 else "seed")
