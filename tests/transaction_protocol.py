"""Independent client checks of durable 2PC receipts before SQL session integration."""
from contextlib import closing
import time
import uuid
import psycopg
import tarantool
from replication import connect, leader, native


def call(shard, function, *arguments):
    for attempt in range(8):
        info = leader(shard)
        with closing(native(info["node"], "gateway")) as connection:
            try:
                return connection.call("cairn." + function, *arguments, info["term"]).data
            except tarantool.DatabaseError as error:
                # Recovery may concurrently resolve this participant. Error 97
                # confirms MVCC rollback; uncertain transport outcomes never retry.
                if (function not in {"tx_apply", "tx_complete", "tx_release", "tx_clean"}
                        or error.code != 97 or attempt == 7):
                    raise
        # Allow an in-flight resolution to finish across several 250ms recovery
        # intervals; seven capped waits total 2.25 seconds before the last try.
        time.sleep(min(.05 * 2**attempt, .5))


def begin():
    identifier = str(uuid.uuid4())
    call(0, "tx_begin", identifier)
    tokens = [call(0, "tx_enlist", identifier, shard)[0] for shard in range(2)]
    return identifier, tokens


def resolve(identifier, decision, prepared):
    record = call(0, "tx_decide", identifier, decision, prepared)[0]
    members, token = record[2], record[6]
    receipts = [call(shard, "tx_apply", identifier, decision, members, token)[0] for shard in members]
    complete = call(0, "tx_complete", identifier, receipts)[0]
    released = [call(shard, "tx_release", identifier, decision, members, complete[7])[0] for shard in members]
    call(0, "tx_clean", identifier, released)


def main():
    table = "txprotocol_" + uuid.uuid4().hex[:8]
    with connect() as connection:
        connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        for key in (0, 1): connection.execute(f"INSERT INTO {table} VALUES (%s, 'before')", (key,))
        identifier, tokens = begin()
        try: call(1, "tx_begin", identifier)
        except tarantool.DatabaseError: pass
        else: raise AssertionError("a different shard accepted the same coordinator identity")
        for shard in range(2):
            assert call(shard, "tx_mutate", identifier, table, shard, "update", [["=", 2, "after"]], tokens[shard]) == [True]
            assert call(shard, "tx_read", identifier, table, shard, tokens[shard]) == [[shard, "after"]]
            with closing(native(leader(shard)["node"])) as direct:
                assert direct.select(table, [shard]).data == [[shard, "before"]]
        prepared = [call(shard, "tx_prepare", identifier, tokens[shard])[0] for shard in range(2)]
        try: call(0, "tx_decide", identifier, "COMMIT", [prepared[0], "forged"])
        except tarantool.DatabaseError: pass
        else: raise AssertionError("forged prepare receipt authorized COMMIT")
        record = call(0, "tx_decide", identifier, "COMMIT", prepared)[0]
        members, token = record[2], record[6]
        first = call(0, "tx_apply", identifier, "COMMIT", members, token)[0]
        try: call(0, "tx_release", identifier, "COMMIT", members, token)
        except tarantool.DatabaseError: pass
        else: raise AssertionError("decision certificate released locks before global completion")
        for key in (0, 1):
            try:
                row = connection.execute(f"SELECT * FROM {table} WHERE id = {key}").fetchone()
                assert row == (key, 'after'), row
            except psycopg.Error as error: assert error.sqlstate == "40001", error
        second = call(1, "tx_apply", identifier, "COMMIT", members, token)[0]
        complete = call(0, "tx_complete", identifier, [first, second])[0]
        released = [call(shard, "tx_release", identifier, "COMMIT", members, complete[7])[0] for shard in members]
        call(0, "tx_clean", identifier, released)
        for key in (0, 1): assert connection.execute(f"SELECT * FROM {table} WHERE id = {key}").fetchone() == (key, "after")
        # An abort after prepare must leave native user tuples untouched.
        identifier, tokens = begin()
        for shard in range(2): call(shard, "tx_mutate", identifier, table, shard, "delete", None, tokens[shard])
        for shard in range(2): assert call(shard, "tx_read", identifier, table, shard, tokens[shard]) == [[]]
        prepared = [call(shard, "tx_prepare", identifier, tokens[shard])[0] for shard in range(2)]
        resolve(identifier, "ABORT", [])
        for key in (0, 1): assert connection.execute(f"SELECT * FROM {table} WHERE id = {key}").fetchone() == (key, "after")
    print("2PC protocol: staged own reads, invisible uncommitted tuples, signed prepare/apply/release receipts, no partial SQL visibility, commit and abort passed", flush=True)


if __name__ == "__main__": main()
