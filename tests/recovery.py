"""Phases used by scripts/check_recovery.sh; only the host script controls Docker."""
import json
import os
import sys
import time
import uuid
from contextlib import closing

import psycopg
import tarantool

DSN = os.environ["CAIRN_DSN"]
STATE = "/artifacts/recovery.json"


def connect():
    return psycopg.connect(DSN, autocommit=True, prepare_threshold=0)


def read(connection, table, key):
    return connection.execute(f"SELECT * FROM {table} WHERE id = %s", (key,)).fetchone()


def wait_read(connection, table, key):
    # Read-only retries reconnect each of the pool's stale sockets after restart.
    for attempt in range(40):
        try:
            return read(connection, table, key)
        except psycopg.Error as error:
            if error.sqlstate != "08006" or attempt == 39:
                raise
            time.sleep(0.05)


def main(mode):
    if mode == "seed":
        table = "recovery_" + uuid.uuid4().hex[:10]
        partial = "partial_" + uuid.uuid4().hex[:10]
        with connect() as connection:
            connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT)")
            for key in [0, 1]:
                connection.execute(f"INSERT INTO {table} VALUES (%s, %s)", (key, f"acknowledged-{key}"))
        with open(STATE, "w") as output:
            json.dump({"table": table, "partial": partial}, output)
        print("seeded acknowledged writes on both shards", flush=True)
        return
    with open(STATE) as source:
        state = json.load(source)
    table, partial = state["table"], state["partial"]
    with connect() as connection:
        if mode == "unavailable":
            assert read(connection, table, 0) == (0, "acknowledged-0")
            try:
                read(connection, table, 1)
            except psycopg.Error as error:
                assert error.sqlstate == "08006", error
            else:
                raise AssertionError("offline shard returned success")
            try:
                connection.execute(f"CREATE TABLE IF NOT EXISTS {partial} (id BIGINT PRIMARY KEY, value TEXT)")
            except psycopg.Error as error:
                assert error.sqlstate == "08006", error
            else:
                raise AssertionError("DDL succeeded despite missing shard")
            print("unavailable shard fails closed; the other shard remains usable; partial DDL reported failure")
        elif mode == "recover":
            for key in [0, 1]:
                assert wait_read(connection, table, key) == (key, f"acknowledged-{key}")
                with closing(tarantool.Connection(f"shard{key}", 3301, user="reader", password="local-reader")) as native:
                    assert native.select(table, [key]).data == [[key, f"acknowledged-{key}"]]
            try:
                connection.execute(f"SELECT * FROM {partial} WHERE id = 0")
            except psycopg.Error as error:
                assert error.sqlstate == "42P01", error
            else:
                raise AssertionError("incomplete DDL became visible")
            connection.execute(f"CREATE TABLE IF NOT EXISTS {partial} (id BIGINT PRIMARY KEY, value TEXT)")
            connection.execute(f"INSERT INTO {partial} VALUES (1, 'repaired')")
            assert read(connection, partial, 1) == (1, "repaired")
            print("acknowledged writes survived SIGKILL/WAL recovery; SQL and native reads agree; partial DDL repair passed")
        else:
            raise ValueError(mode)


if __name__ == "__main__":
    main(sys.argv[1])
