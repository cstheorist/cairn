"""Phases coordinated by scripts/check_transaction_recovery.py."""
from contextlib import closing
import json
import sys
import time
import uuid
import psycopg
from replication import connect, leader, native
from transaction_protocol import begin, call

BASE = "/artifacts/transaction-recovery-"

def save(kind, state):
    with open(BASE + kind + ".json", "w") as output: json.dump(state, output)

def load(kind):
    with open(BASE + kind + ".json") as source: return json.load(source)

def fixture():
    table = "txrecover_" + uuid.uuid4().hex[:8]
    with connect() as connection:
        connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        for key in (0, 1): connection.execute(f"INSERT INTO {table} VALUES (%s, 'original')", (key,))
    return table

def wait_outcome(state, expected):
    deadline = time.monotonic() + 50
    connection = None
    try:
        while True:
            try:
                if connection is None: connection = connect()
                values = [connection.execute(f"SELECT value FROM {state['table']} WHERE id = %s", (key,), prepare=False).fetchone()[0] for key in (0, 1)]
                if values == [expected, expected]:
                    record = call(0, "tx_status", state["id"])[0]
                    if record[4] and record[5]: return record
            except psycopg.Error as error:
                if error.sqlstate not in (None, "40001", "08006", "57P03"): raise
                if error.sqlstate in (None, "08006"):
                    if connection is not None: connection.close()
                    connection = None
            if time.monotonic() >= deadline: raise AssertionError("transaction recovery did not converge")
            time.sleep(.1)
    finally:
        if connection is not None: connection.close()

def main(mode):
    if mode == "hold_open":
        table = fixture()
        connection = connect()
        connection.execute("BEGIN")
        for key in (0, 1): connection.execute(f"UPDATE {table} SET value = 'orphan' WHERE id = %s", (key,))
        with closing(native(leader(0)["node"], "operator")) as operator:
            identifier = operator.eval("local name=...; for _, row in box.space._cairn_intents:pairs() do if row[2] == name then return row[1] end end", table).data[0]
        save("open", {"table": table, "id": identifier})
        print("OPEN_READY", flush=True)
        try:
            while True:
                connection.execute(f"SELECT * FROM {table} WHERE id = 0").fetchone()
                time.sleep(.1)
        except psycopg.Error: pass
        finally: connection.close()
    elif mode == "check_open":
        record = wait_outcome(load("open"), "original")
        assert record[1] == "ABORT", record
        print("gateway crash: expired OPEN transaction aborted durably and released locks on both shards", flush=True)
    elif mode == "prepare":
        table = fixture()
        identifier, tokens = begin()
        for shard in range(2): call(shard, "tx_mutate", identifier, table, shard, "update", [["=", 2, "decided"]], tokens[shard])
        prepared = [call(shard, "tx_prepare", identifier, tokens[shard])[0] for shard in range(2)]
        save("commit", {"table": table, "id": identifier, "prepared": prepared})
        print("durably prepared both shards", flush=True)
    elif mode == "decide_partial":
        state = load("commit")
        record = call(0, "tx_decide", state["id"], "COMMIT", state["prepared"])[0]
        call(0, "tx_apply", state["id"], "COMMIT", record[2], record[6])
        state["record"] = record
        state["coordinator"] = leader(0)["node"]
        save("commit", state)
        print("durable COMMIT decision with only shard 0 applied; gateway is stopped", flush=True)
    elif mode == "check_commit":
        state = load("commit")
        record = wait_outcome(state, "decided")
        assert record[1] == "COMMIT", record
        assert call(0, "tx_decide", state["id"], "ABORT", [])[0][1] == "COMMIT"
        with connect() as connection:
            connection.execute(f"UPDATE {state['table']} SET value = 'later' WHERE id = 0")
            call(0, "tx_apply", state["id"], "COMMIT", record[2], record[6])
            assert connection.execute(f"SELECT value FROM {state['table']} WHERE id = 0").fetchone() == ("later",)
        print("coordinator leader crash: durable decision recovered, both shards committed, locks released, stale apply receipt did not replay writes", flush=True)
    else: raise ValueError(mode)

if __name__ == "__main__": main(sys.argv[1])
