"""Committed public points through volatile OPEN row claims.

Run the regular suite with gateway recovery disabled for deterministic native
phase assertions. `seed CASE` / `check CASE` provide external leader-fault hooks;
CASE is home_open, remote_open, prepared, or local. The external driver kills
the node recorded in /artifacts/open-point-reads-fault.json between those calls.
No fault is injected by this file.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import sys
import threading
import time
import uuid

import psycopg
import tarantool
from replication import connect, leader, native
from transaction_protocol import call
from transaction_start import identifier
from volatile_open import epoch, freeze, identity, prepare
from vote_protocol import finish as finish_barrier


STATE = Path("/artifacts/open-point-reads-fault.json")


def create(connection, keys=(0, 1, 2, 3)):
    table = "open_points_" + uuid.uuid4().hex[:9]
    connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
    for key in keys:
        connection.execute(f"INSERT INTO {table} VALUES (%s, 'before')", (key,))
    return table


def point(connection, table, key):
    return connection.execute(f"SELECT * FROM {table} WHERE id=%s", (key,)).fetchone()


def public(shard, function, *arguments):
    info = leader(shard)
    with closing(native(info["node"])) as reader:
        return reader.call("cairn." + function, *arguments, info["term"]).data


def visible(connection, table, key, expected):
    assert point(connection, table, key) == expected
    row = [] if expected is None else list(expected)
    assert public(key % 2, "get", table, key) == [row]
    assert public(key % 2, "get_many", [[table, key]]) == [[[table, key, row]]]


def conflicts(operation):
    try:
        operation()
    except psycopg.Error as error:
        assert error.sqlstate == "40001", error
    except tarantool.DatabaseError as error:
        assert "[CAIRN:CONFLICT]" in str(error), error
    else:
        raise AssertionError("read or write bypassed a strict claim")


def blocked(connection, table, key):
    conflicts(lambda: point(connection, table, key))
    conflicts(lambda: public(key % 2, "get", table, key))
    conflicts(lambda: public(key % 2, "get_many", [[table, key]]))


def resolution_call(shard, function, *arguments):
    assert function in ("tx_vote_abort", "tx_decide", "tx_finish_vote")
    for attempt in range(8):
        try:
            return call(shard, function, *arguments)
        except tarantool.DatabaseError as error:
            # Recovery may race these idempotent phases. Error97 is a confirmed
            # native rollback; transport uncertainty is never replayed here.
            if error.code != 97 or attempt == 7:
                raise
            time.sleep(min(.05 * (attempt + 1), .25))


def start_open(table, home=0):
    tx = identity(home)
    token, result = call(home, "tx_open_start", tx, "cairn.tx_mutate", [table, home, "update", [["=", 2, "after"]]])
    assert result == [True]
    return tx, {home: token}


def join_open(tx, table, tokens, shard):
    home = int(tx[:4], 16)
    tokens[shard] = call(home, "tx_open_enlist", tx, shard, format(epoch(shard), "016x"))[0]
    assert call(shard, "tx_mutate", tx, table, shard, "update", [["=", 2, "after"]], tokens[shard]) == [True]


def strict_sql_conflicts(table, key):
    with connect() as other:
        other.execute("BEGIN")
        conflicts(lambda: point(other, table, key))
        other.execute("ROLLBACK")
        conflicts(lambda: other.execute(f"UPDATE {table} SET value='other' WHERE id=%s", (key,)))


def open_crud(connection):
    for home in (0, 1):
        for outcome in ("abort", "local"):
            table = create(connection)
            tx, tokens = start_open(table, home)
            deleted, inserted = home + 2, home + 4
            call(home, "tx_mutate", tx, table, deleted, "delete", None, tokens[home])
            call(home, "tx_mutate", tx, table, inserted, "insert", [inserted, "inserted"], tokens[home])
            visible(connection, table, home, (home, "before"))
            visible(connection, table, deleted, (deleted, "before"))
            visible(connection, table, inserted, None)
            expected = [[table, deleted, [deleted, "before"]], [table, inserted, []], [table, home, [home, "before"]]]
            assert public(home, "get_many", [[table, key] for key in (deleted, inserted, home)]) == [expected]
            assert call(home, "tx_read", tx, table, home, tokens[home]) == [[home, "after"]]
            assert call(home, "tx_read", tx, table, deleted, tokens[home]) == [[]]
            assert call(home, "tx_read", tx, table, inserted, tokens[home]) == [[inserted, "inserted"]]
            strict_sql_conflicts(table, home)
            conflicts(lambda: connection.execute(f"SELECT * FROM {table} ORDER BY id"))
            if outcome == "abort":
                # The unchanged scan/write upgrade creates temporary table-X.
                call(home, "tx_scan", tx, table, None, 1024, tokens[home])
                blocked(connection, table, home)
                blocked(connection, table, home + 6)  # Also excludes absent/unwritten points.
                call(home, "tx_open_abort", tx, tokens[home], False)
                visible(connection, table, home, (home, "before"))
                visible(connection, table, deleted, (deleted, "before"))
                visible(connection, table, inserted, None)
            else:
                result = call(home, "tx_open_local", tx)[0]
                assert result[1] == "COMMIT" and result[4] and result[5]
                visible(connection, table, home, (home, "after"))
                visible(connection, table, deleted, None)
                visible(connection, table, inserted, (inserted, "inserted"))
                connection.execute(f"UPDATE {table} SET value='later' WHERE id=%s", (home,))
                call(home, "tx_open_local", tx)
                visible(connection, table, home, (home, "later"))
            # Unlike public point success, a competing write proves unlock.
            connection.execute(f"UPDATE {table} SET value=value WHERE id=%s", (home,))


def durable_modes(connection):
    for mode, start in [(0, "tx_start"), (1, "tx_start_voted"), (2, "tx_start_finished")]:
        for outcome in ("COMMIT", "ABORT"):
            table = create(connection, (0, 1))
            tx = identifier(0)
            token, _ = call(0, start, tx, "cairn.tx_mutate", [table, 0, "update", [["=", 2, "after"]]])
            tokens = {0: token, 1: call(0, "tx_enlist", tx, 1)[0]}
            call(1, "tx_mutate", tx, table, 1, "update", [["=", 2, "after"]], tokens[1])
            for key in (0, 1):
                blocked(connection, table, key)  # Durable OPEN claims are still strict.
            receipts = [call(shard, "tx_prepare", tx, tokens[shard])[0] for shard in (0, 1)] if outcome == "COMMIT" else []
            record = call(0, "tx_decide", tx, outcome, receipts)[0]
            if mode == 2:
                released = [call(0, "tx_finish_vote", tx, outcome, record[2], record[6], record[12])[0]]
                visible(connection, table, 0, (0, "after" if outcome == "COMMIT" else "before"))
                blocked(connection, table, 1)
                released.append(call(1, "tx_finish_vote", tx, outcome, record[2], record[6], record[12])[0])
                call(0, "tx_clean", tx, released)
            else:
                call(0, "tx_apply", tx, outcome, record[2], record[6])
                for key in (0, 1):
                    blocked(connection, table, key)  # Applying never grants public access.
                finish_barrier(0, record)
            for key in (0, 1):
                visible(connection, table, key, (key, "after" if outcome == "COMMIT" else "before"))


def volatile_prepare_and_finish(connection):
    for home in (0, 1):
        remote = 1 - home
        table = create(connection, (0, 1))
        tx, tokens = start_open(table, home)
        join_open(tx, table, tokens, remote)
        scope = freeze(tx)
        for key in (0, 1):
            visible(connection, table, key, (key, "before"))
        remote_yes = prepare(tx, tokens, scope, remote)
        blocked(connection, table, remote)
        visible(connection, table, home, (home, "before"))
        home_yes = prepare(tx, tokens, scope, home)
        proofs = [home_yes, remote_yes] if home == 0 else [remote_yes, home_yes]
        for key in (0, 1):
            blocked(connection, table, key)
        # Coordinator OPEN is not evidence that a durable claim can be ignored.
        assert call(home, "tx_status", tx)[0][1] == "OPEN"
        released = call(remote, "tx_open_finish", tx, scope[14], scope[15], proofs)[0]
        visible(connection, table, remote, (remote, "after"))
        blocked(connection, table, home)
        result = call(home, "tx_open_finish_last", tx, scope[14], scope[15], proofs, [released])[0]
        assert result[1] == "COMMIT" and result[4] and result[5]
        for key in (0, 1):
            visible(connection, table, key, (key, "after"))


def abort_prepared_scope(tx, scope, no_member):
    home = int(tx[:4], 16)
    vote = resolution_call(no_member, "tx_vote_abort", tx, scope[2], scope[10])[0]
    assert vote[0] == "NO", vote
    record = resolution_call(home, "tx_decide", tx, "ABORT", [no_member, vote[1]])[0]
    assert record[1] == "ABORT", record
    released = [resolution_call(shard, "tx_finish_vote", tx, "ABORT", record[2], record[6], record[12])[0] for shard in record[2]]
    call(home, "tx_clean", tx, released)


def volatile_prepared_abort(connection):
    table = create(connection, (0, 1))
    tx, tokens = start_open(table)
    join_open(tx, table, tokens, 1)
    scope = freeze(tx)
    prepare(tx, tokens, scope, 0)
    blocked(connection, table, 0)
    visible(connection, table, 1, (1, "before"))
    abort_prepared_scope(tx, scope, 1)
    for key in (0, 1):
        visible(connection, table, key, (key, "before"))
        connection.execute(f"UPDATE {table} SET value=value WHERE id=%s", (key,))


def ack_before_batched_reads(connection):
    table = create(connection, (0, 2))
    first, tokens = start_open(table)
    call(0, "tx_mutate", first, table, 2, "update", [["=", 2, "after"]], tokens[0])
    assert call(0, "tx_open_local", first)[0][1] == "COMMIT"
    pending, tokens = start_open(table)
    call(0, "tx_mutate", pending, table, 0, "update", [["=", 2, "private"]], tokens[0])
    call(0, "tx_mutate", pending, table, 2, "update", [["=", 2, "private"]], tokens[0])
    barrier = threading.Barrier(8)
    def reader(_):
        with connect() as other:
            barrier.wait()
            assert point(other, table, 0) == (0, "after")
            assert point(other, table, 2) == (2, "after")
    with ThreadPoolExecutor(max_workers=8) as readers:
        list(readers.map(reader, range(8)))
    requests = [[table, key] for key in (2, 0, 2)]
    assert public(0, "get_many", requests) == [[[table, key, [key, "after"]] for key in (2, 0, 2)]]
    call(0, "tx_open_abort", pending, tokens[0], False)


def local_batch_snapshot(connection):
    table = create(connection, (0, 2))
    tx, tokens = start_open(table)
    call(0, "tx_mutate", tx, table, 2, "update", [["=", 2, "after"]], tokens[0])
    barrier = threading.Barrier(2)
    def reader():
        barrier.wait()
        successful = 0
        for _ in range(32):
            try:
                rows = public(0, "get_many", [[table, 0], [table, 2]])[0]
            except tarantool.DatabaseError as error:
                assert "[CAIRN:CONFLICT]" in str(error) or error.code == 97, error
                continue
            assert [row[2][1] for row in rows] in [["before", "before"], ["after", "after"]], rows
            successful += 1
        return successful
    with ThreadPoolExecutor(max_workers=1) as readers:
        future = readers.submit(reader)
        barrier.wait()
        assert call(0, "tx_open_local", tx)[0][1] == "COMMIT"
        assert future.result() > 0, "concurrent batch loop produced no successful read"
    rows = public(0, "get_many", [[table, 0], [table, 2]])[0]
    assert [row[2] for row in rows] == [[0, "after"], [2, "after"]], rows
    visible(connection, table, 0, (0, "after"))
    visible(connection, table, 2, (2, "after"))


def fault_phase(phase, case):
    assert phase in ("seed", "check"), phase
    assert case in ("home_open", "remote_open", "prepared", "local"), case
    if phase == "seed":
        with connect() as connection:
            table = create(connection, (0, 1))
            tx, tokens = start_open(table)
            scope = None
            if case != "local":
                join_open(tx, table, tokens, 1)
                scope = freeze(tx)
                if case in ("remote_open", "prepared"):
                    prepare(tx, tokens, scope, 0)
                if case == "prepared":
                    prepare(tx, tokens, scope, 1)
            else:
                assert call(0, "tx_open_local", tx)[0][1] == "COMMIT"
            shard = 1 if case == "remote_open" else 0
            STATE.write_text(json.dumps({"id": tx, "table": table, "tokens": tokens, "scope": scope,
                                         "shard": shard, "epoch": epoch(shard), "dead": leader(shard)["node"]}))
        print(f"{case}: point-read fault boundary seeded", flush=True)
        return
    state = json.loads(STATE.read_text())
    tx, table, scope = state["id"], state["table"], state["scope"]
    tokens = {int(shard): token for shard, token in state["tokens"].items()}
    assert epoch(state["shard"]) > state["epoch"], "leader reused an old volatile incarnation"
    with connect() as connection:
        if case in ("home_open", "remote_open"):
            visible(connection, table, state["shard"], (state["shard"], "before"))
            try:
                prepare(tx, tokens, scope, state["shard"])
            except tarantool.DatabaseError as error:
                assert "[CAIRN:ABORTED]" in str(error), error
            else:
                raise AssertionError("lost OPEN image prepared after failover")
            abort_prepared_scope(tx, scope, state["shard"])
            for key in (0, 1):
                visible(connection, table, key, (key, "before"))
        elif case == "prepared":
            deadline = time.monotonic() + 15
            for key in (0, 1):
                while True:
                    try:
                        row = point(connection, table, key)
                    except psycopg.Error as error:
                        assert error.sqlstate in ("40001", "08006", "57P03", "XX000"), error
                    else:
                        assert row == (key, "after"), "prepared failover exposed the old committed row"
                        break
                    assert time.monotonic() < deadline, "prepared outcome recovery did not finish"
                    time.sleep(.05)
                visible(connection, table, key, (key, "after"))
        else:
            visible(connection, table, 0, (0, "after"))  # Invocation follows a durable commit ACK.
            call(0, "tx_open_local", tx)
            visible(connection, table, 0, (0, "after"))
    print(f"{case}: committed point visibility and claim/failover fencing passed", flush=True)


def main():
    with connect() as connection:
        open_crud(connection)
        durable_modes(connection)
        volatile_prepare_and_finish(connection)
        volatile_prepared_abort(connection)
        ack_before_batched_reads(connection)
        local_batch_snapshot(connection)
    print("public points: volatile OPEN committed images, strict durable/predicate claims, modes0/1/2/3, abort/local/partial finish, ACK freshness and atomic batches passed", flush=True)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        fault_phase(sys.argv[1], sys.argv[2])
