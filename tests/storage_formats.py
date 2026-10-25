"""Native mutation validation survives reuse of immutable per-table formats."""
from contextlib import closing
import uuid

import tarantool

from replication import connect, leader, native
from transaction_protocol import call
from transaction_start import identifier
from volatile_open import identity as open_identifier


def rejects(operation):
    try:
        operation()
    except tarantool.DatabaseError:
        return
    raise AssertionError("native mutation accepted an invalid tuple")


def check_handle_sets(connection, suffix):
    table = "format_handles_" + suffix
    connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
    for shard, key in ((0, -(2**63)), (1, 2**63 - 1)):
        assert call(shard, "insert", table, [key, "before"]) == [[key, "before"]]
        rejects(lambda: call(1 - shard, "get", table, key))
        durable, opened = identifier(shard), open_identifier(shard)
        _, result = call(shard, "tx_start", durable, "cairn.tx_read", [table, key])
        assert result == [[key, "before"]], result
        token, result = call(shard, "tx_open_start", opened, "cairn.tx_read", [table, key])
        assert result == [[key, "before"]], result
        # Separate durable/open handle sets must still respect each other's claims.
        changes = [["=", 2, "open"]]
        rejects(lambda: call(shard, "tx_mutate", opened, table, key, "update", changes, token))
        call(shard, "tx_commit_local", durable)
        assert call(shard, "tx_mutate", opened, table, key, "update", changes, token) == [True]
        assert call(shard, "tx_read", opened, table, key, token) == [[key, "open"]]
        with closing(native(leader(shard)["node"])) as direct:
            assert direct.select(table, [key]).data == [[key, "before"]]
        call(shard, "tx_open_local", opened)
        assert call(shard, "get", table, key) == [[key, "open"]]
        # Local OPEN completion must remove its claims from the cached spaces.
        later = identifier(shard)
        _, result = call(shard, "tx_start", later, "cairn.tx_mutate",
                         [table, key, "update", [["=", 2, "durable"]]])
        assert result == [True], result
        call(shard, "tx_commit_local", later)
        assert connection.execute(f"SELECT * FROM {table} WHERE id=%s", (key,)).fetchone() == (key, "durable")


def main():
    suffix = uuid.uuid4().hex[:8]
    cases = [
        ("format_text_" + suffix, "TEXT NOT NULL", "BIGINT", "before", "after",
         [["=", 2, None]], [[["=", 2, None]], [["=", 2, 7]], [["=", 3, "bad"]]]),
        ("format_bool_" + suffix, "BOOLEAN", "BIGINT NOT NULL", False, True,
         [["=", 3, None]], [[["=", 3, None]], [["=", 2, "bad"]], [["=", 3, "bad"]]]),
    ]
    with connect() as connection:
        for table, value_type, counter_type, initial, final, cold_invalid, invalid in cases:
            connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value {value_type}, counter {counter_type})")
            for shard in (0, 1):
                # Seed natively so tx_mutate has not built the table's validator.
                assert call(shard, "insert", table, [shard, initial, 0]) == [[shard, initial, 0]]

            for shard in (0, 1):
                # The first format can be cached even though the write rolls back.
                failed = identifier(shard)
                rejects(lambda: call(shard, "tx_start", failed, "cairn.tx_mutate",
                                     [table, shard, "update", cold_invalid]))
                rejects(lambda: call(shard, "tx_status", failed))
                assert connection.execute(f"SELECT * FROM {table} WHERE id=%s", (shard,)).fetchone() == (shard, initial, 0)

                tx = identifier(shard)
                token, result = call(shard, "tx_start", tx, "cairn.tx_mutate",
                                     [table, shard, "update", [["=", 2, final], ["=", 3, -(2**63)]]])
                assert result == [True], result
                expected = [shard, final, -(2**63)]
                assert call(shard, "tx_read", tx, table, shard, token) == [expected]

                # Reuse the format while validating NULLs and exact signed values.
                nullable_field = 3 if value_type == "TEXT NOT NULL" else 2
                assert call(shard, "tx_mutate", tx, table, shard, "update",
                            [["=", nullable_field, None]], token) == [True]
                expected[nullable_field - 1] = None
                assert call(shard, "tx_read", tx, table, shard, token) == [expected]
                for changes in invalid:
                    rejects(lambda: call(shard, "tx_mutate", tx, table, shard,
                                         "update", changes, token))
                    assert call(shard, "tx_read", tx, table, shard, token) == [expected]

                assert call(shard, "tx_mutate", tx, table, shard, "update",
                            [["=", 2, final], ["=", 3, 2**63 - 1]], token) == [True]
                expected = [shard, final, 2**63 - 1]
                assert call(shard, "tx_read", tx, table, shard, token) == [expected]
                with closing(native(leader(shard)["node"])) as direct:
                    assert direct.select(table, [shard]).data == [[shard, initial, 0]]
                call(shard, "tx_commit_local", tx)
                assert connection.execute(f"SELECT * FROM {table} WHERE id=%s", (shard,)).fetchone() == tuple(expected)

        check_handle_sets(connection, suffix)

    print("storage helpers: format validation, NULLs, signed routing bounds, durable/open claims and committed native visibility passed", flush=True)


if __name__ == "__main__":
    main()
