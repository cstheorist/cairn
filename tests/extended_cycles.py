"""Extended-query transaction dependencies, compared with PostgreSQL 16.

Reference sessions explicitly verify SERIALIZABLE; PostgreSQL's default READ
COMMITTED would legitimately permit the write-skew history tested here.
Run in the existing tests service: python /tests/extended_cycles.py.
"""
from contextlib import contextmanager
import os
import socket
import struct
import time
import unittest
import uuid

import psycopg
from encoding import bind, errors, execute, parse, simple
from integration import frame, receive


CAIRN_DSN = os.environ["CAIRN_DSN"]
ENDPOINTS = [
    ("cairn", CAIRN_DSN),
    ("postgres16", os.environ.get("REFERENCE_DSN", "host=reference port=5432 user=cairn dbname=cairn password=local-postgres sslmode=disable")),
]


def data_rows(messages):
    result = []
    for tag, payload in messages:
        if tag != b"D":
            continue
        count = struct.unpack_from("!H", payload)[0]
        offset = 2
        row = []
        for _ in range(count):
            length = struct.unpack_from("!i", payload, offset)[0]
            offset += 4
            row.append(None if length == -1 else payload[offset:offset + length].decode())
            if length != -1:
                offset += length
        result.append(row)
    return result


def until(raw, terminal):
    messages = []
    while True:
        message = receive(raw)
        messages.append(message)
        if message[0] in terminal:
            return messages


def query(raw, sql, *, sync=False):
    raw.sendall(parse(sql) + bind([]) + execute() + frame(b"S" if sync else b"H"))
    return until(raw, {b"Z"} if sync else {b"C", b"E", b"s"})


@contextmanager
def raw_session(endpoint, dsn):
    options = {"options": "-c default_transaction_isolation=serializable"} if endpoint == "postgres16" else {}
    with psycopg.connect(dsn, autocommit=True, **options) as connection:
        if endpoint == "postgres16":
            assert connection.info.server_version // 10000 == 16
            assert connection.execute("SHOW default_transaction_isolation").fetchone()[0] == "serializable"
        with socket.socket(fileno=os.dup(connection.fileno())) as raw:
            raw.settimeout(5)
            try:
                yield raw
            finally:
                # Raw traffic bypasses libpq's transaction status bookkeeping.
                try:
                    simple(raw, "ROLLBACK")
                except (OSError, EOFError):
                    pass


def create_table(dsn):
    table = "extcycle_" + uuid.uuid4().hex[:12]
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value BIGINT)")
        connection.execute(f"INSERT INTO {table} VALUES (0, 0)")
        connection.execute(f"INSERT INTO {table} VALUES (1, 0)")
    return table


def competing_writer(dsn, table, *, read_other=False):
    """One attempt only; a serialization conflict is an expected known abort."""
    with psycopg.connect(dsn, autocommit=True, prepare_threshold=None) as connection:
        try:
            connection.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            if read_other:
                assert connection.execute(f"SELECT value FROM {table} WHERE id = 1").fetchone() == (0,)
            connection.execute(f"UPDATE {table} SET value = 1 WHERE id = 0")
            connection.execute("COMMIT")
            return None
        except psycopg.DatabaseError as error:
            return error.sqlstate
        finally:
            connection.execute("ROLLBACK")


class ExtendedCycles(unittest.TestCase):
    def assert_success(self, messages, state=None):
        self.assertEqual(errors(messages), [], messages)
        if state is not None:
            self.assertEqual(messages[-1], (b"Z", state), messages)

    def assert_point(self, messages, value="0"):
        self.assert_success(messages)
        self.assertEqual(data_rows(messages), [[value]], messages)

    def write_skew(self, endpoint, dsn, boundary):
        table = create_table(dsn)
        with raw_session(endpoint, dsn) as raw:
            if boundary == "explicit":
                self.assert_success(simple(raw, "BEGIN ISOLATION LEVEL SERIALIZABLE"), b"T")
            self.assert_point(query(raw, f"SELECT value FROM {table} WHERE id = 0"))
            if boundary == "promote":
                self.assert_success(query(raw, "BEGIN", sync=True), b"T")
            elif boundary == "sync":
                raw.sendall(frame(b"S"))
                self.assertEqual(until(raw, {b"Z"}), [(b"Z", b"I")])
            b_error = competing_writer(dsn, table, read_other=True)
            self.assertIn(b_error, (None, "40001"))
            a_write = query(raw, f"UPDATE {table} SET value = 1 WHERE id = 1", sync=True)
            self.assertIn(errors(a_write), ([], ["40001"]))
            a_committed = not errors(a_write)
            if boundary in ("explicit", "promote"):
                finish = simple(raw, "COMMIT")
                self.assertIn(errors(finish), ([], ["40001"]))
                a_committed = a_committed and not errors(finish)
                self.assertEqual(finish[-1], (b"Z", b"I"))
            else:
                self.assertEqual(a_write[-1], (b"Z", b"I"))
            if boundary == "sync":
                self.assertIsNone(b_error)
                self.assertTrue(a_committed)
            else:
                self.assertFalse(b_error is None and a_committed, "both transactions committed the nonserializable history")
                if endpoint == "cairn":
                    self.assertEqual(b_error, "40001", "the initial read must retain its strict claim")
        with psycopg.connect(dsn, autocommit=True) as check:
            rows = check.execute(f"SELECT id, value FROM {table} ORDER BY id").fetchall()
            self.assertEqual(rows, [(0, 1), (1, 1)] if boundary == "sync" else
                             ([(0, 0), (1, 1)] if b_error else [(0, 1), (1, 0)]))

    def test_flush_does_not_discard_the_initial_read_dependency(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.write_skew(endpoint, dsn, "implicit")

    def test_explicit_begin_control(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.write_skew(endpoint, dsn, "explicit")

    def test_sync_separates_independent_transactions(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.write_skew(endpoint, dsn, "sync")

    def test_begin_adopts_prior_read_claims_and_sync_keeps_them(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.write_skew(endpoint, dsn, "promote")

    def test_repeated_point_reads_before_sync_preserve_their_view(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                table = create_table(dsn)
                with raw_session(endpoint, dsn) as raw:
                    self.assert_point(query(raw, f"SELECT value FROM {table} WHERE id = 0"))
                    b_error = competing_writer(dsn, table)
                    self.assertEqual(b_error, "40001" if endpoint == "cairn" else None)
                    self.assert_point(query(raw, f"SELECT value + 0 FROM {table} WHERE id = 0", sync=True))

    def test_coalesced_multiple_executes_and_sync(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                table = create_table(dsn)
                with raw_session(endpoint, dsn) as raw:
                    messages = b""
                    for key in (0, 1):
                        messages += parse(f"SELECT value FROM {table} WHERE id = {key}") + bind([]) + execute()
                    raw.sendall(messages + frame(b"S"))
                    responses = until(raw, {b"Z"})
                    self.assert_success(responses, b"I")
                    self.assertEqual(data_rows(responses), [["0"], ["0"]])
                    self.assertEqual(competing_writer(dsn, table), None)

    def test_single_execute_sync_and_fragmented_sync_remain_usable(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                table = create_table(dsn)
                with raw_session(endpoint, dsn) as raw:
                    self.assert_point(query(raw, f"SELECT value FROM {table} WHERE id = 0", sync=True))
                    raw.sendall(parse(f"SELECT value FROM {table} WHERE id = 1") + bind([]) + execute())
                    for byte in frame(b"S"):
                        raw.sendall(bytes([byte]))
                    responses = until(raw, {b"Z"})
                    self.assert_point(responses)
                    self.assertEqual(responses[-1], (b"Z", b"I"))
                    self.assertIsNone(competing_writer(dsn, table))

    def test_malformed_successor_does_not_overtake_execute_results(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                table = create_table(dsn)
                with raw_session(endpoint, dsn) as raw:
                    raw.sendall(parse(f"SELECT value FROM {table} WHERE id = 0") + bind([]) + execute() + frame(b"S", b"unexpected"))
                    responses = until(raw, {b"Z"})
                    self.assertEqual(data_rows(responses), [["0"]])
                    self.assertEqual(errors(responses), ["08P01"])
                    tags = [tag for tag, _ in responses]
                    self.assertLess(tags.index(b"C"), tags.index(b"E"))
                    self.assertEqual(responses[-1], (b"Z", b"I"))
                    self.assertIsNone(competing_writer(dsn, table))

    def test_exhausted_portal_does_not_drop_its_read_claims(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                table = create_table(dsn)
                with raw_session(endpoint, dsn) as raw:
                    raw.sendall(parse(f"SELECT value FROM {table} WHERE id = 0", b"held") +
                                bind([], statement=b"held", portal=b"held") + execute(b"held") + frame(b"H"))
                    self.assert_point(until(raw, {b"C", b"E"}))
                    raw.sendall(execute(b"held") + frame(b"H"))
                    exhausted = until(raw, {b"C", b"E"})
                    self.assert_success(exhausted)
                    self.assertEqual(exhausted, [(b"C", b"SELECT 0\0")])
                    self.assertEqual(competing_writer(dsn, table), "40001" if endpoint == "cairn" else None)
                    raw.sendall(frame(b"S"))
                    self.assertEqual(until(raw, {b"Z"}), [(b"Z", b"I")])
                    self.assertIsNone(competing_writer(dsn, table))

    def test_begin_adopts_staged_writes_then_read_only_restricts_future_writes(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                table = create_table(dsn)
                with raw_session(endpoint, dsn) as raw:
                    self.assert_success(query(raw, f"INSERT INTO {table} VALUES (2, 7)"))
                    self.assert_success(query(raw, "BEGIN READ ONLY", sync=True), b"T")
                    self.assert_point(query(raw, f"SELECT value FROM {table} WHERE id = 2", sync=True), "7")
                    rejected = query(raw, f"UPDATE {table} SET value = 8 WHERE id = 2", sync=True)
                    self.assertEqual(errors(rejected), ["25006"])
                    self.assertEqual(rejected[-1], (b"Z", b"E"))
                    self.assert_success(simple(raw, "ROLLBACK"), b"I")
                with psycopg.connect(dsn, autocommit=True) as check:
                    self.assertIsNone(check.execute(f"SELECT value FROM {table} WHERE id = 2").fetchone())

    def test_bind_error_rolls_back_prior_write_and_releases_initial_read(self):
        for endpoint, dsn in ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                table = create_table(dsn)
                with raw_session(endpoint, dsn) as raw:
                    self.assert_point(query(raw, f"SELECT value FROM {table} WHERE id = 0"))
                    messages = parse(f"UPDATE {table} SET value = 2 WHERE id = 1") + bind([]) + execute()
                    messages += parse(f"UPDATE {table} SET value = $1 WHERE id = 1") + bind([b"bad-integer"]) + execute()
                    raw.sendall(messages + frame(b"S"))
                    responses = until(raw, {b"Z"})
                    self.assertEqual(errors(responses), ["22P02"])
                    self.assertEqual(responses[-1], (b"Z", b"I"))
                    self.assertIsNone(competing_writer(dsn, table))
                with psycopg.connect(dsn, autocommit=True) as check:
                    self.assertEqual(check.execute(f"SELECT value FROM {table} WHERE id = 1").fetchone(), (0,))

    def test_cairn_execute_alone_and_partial_sync_make_progress_without_releasing_claims(self):
        # Execute-alone output is an existing Cairn behavior, not a PostgreSQL
        # guarantee: protocol clients should send Flush before awaiting output.
        for prefix in (b"", b"S", b"S\0\0\0"):
            with self.subTest(prefix=prefix):
                table = create_table(CAIRN_DSN)
                with raw_session("cairn", CAIRN_DSN) as raw:
                    raw.sendall(parse(f"SELECT value FROM {table} WHERE id = 0") + bind([]) + execute() + prefix)
                    self.assert_point(until(raw, {b"C", b"E"}))
                    self.assertEqual(competing_writer(CAIRN_DSN, table), "40001")
                    raw.sendall(frame(b"S")[len(prefix):])
                    self.assertEqual(until(raw, {b"Z"}), [(b"Z", b"I")])
                    self.assertIsNone(competing_writer(CAIRN_DSN, table))

    def test_cairn_disconnect_releases_a_proven_read_claim(self):
        table = create_table(CAIRN_DSN)
        with raw_session("cairn", CAIRN_DSN) as raw:
            self.assert_point(query(raw, f"SELECT value FROM {table} WHERE id = 0"))
            self.assertEqual(competing_writer(CAIRN_DSN, table), "40001")
            raw.shutdown(socket.SHUT_RDWR)
        deadline = time.monotonic() + 5
        while True:
            error = competing_writer(CAIRN_DSN, table)
            if error is None:
                break
            self.assertEqual(error, "40001")
            self.assertLess(time.monotonic(), deadline, "disconnect left the read claim locked")
            time.sleep(0.02)


if __name__ == "__main__":
    unittest.main(verbosity=2)
