"""Real-client integration tests. Run with docker compose --profile test run --rm tests."""
import os
import socket
import struct
import time
import unittest
import uuid
from contextlib import closing

import psycopg
import tarantool

DSN = os.environ.get("CAIRN_DSN", "host=127.0.0.1 port=55432 user=cairn dbname=cairn password=local-postgres sslmode=disable")
SCHEMA = "(id BIGINT PRIMARY KEY, value TEXT, enabled BOOLEAN NOT NULL)"


def connect():
    return psycopg.connect(DSN, autocommit=True, prepare_threshold=0)


def frame(tag, payload=b""):
    return tag + struct.pack("!I", len(payload) + 4) + payload


def receive(raw):
    def exact(size):
        result = b""
        while len(result) < size:
            chunk = raw.recv(size - len(result))
            if not chunk: raise EOFError("server closed connection")
            result += chunk
        return result
    tag = exact(1)
    length = struct.unpack("!I", exact(4))[0]
    assert 4 <= length <= 1024 * 1024
    return tag, exact(length - 4)


def raw_connect():
    raw = socket.create_connection(("gateway", 5432), timeout=3)
    payload = struct.pack("!I", 196608) + b"user\0cairn\0database\0cairn\0\0"
    raw.sendall(struct.pack("!I", len(payload) + 4) + payload)
    assert receive(raw) == (b"R", struct.pack("!I", 3))
    raw.sendall(frame(b"p", b"local-postgres\0"))
    while receive(raw)[0] != b"Z": pass
    return raw


class ProtocolTests(unittest.TestCase):
    def test_fragmented_and_coalesced_frontend_frames(self):
        with raw_connect() as raw:
            for byte in frame(b"Q", "SELECT '雪'\0".encode()):
                raw.sendall(bytes([byte]))
            responses=[receive(raw) for _ in range(4)]
            self.assertEqual([tag for tag,_ in responses],[b"T",b"D",b"C",b"Z"])
            self.assertEqual(responses[1],(b"D",struct.pack("!Hi",1,3)+"雪".encode()))
            raw.sendall(frame(b"Q",b"SELECT 1\0")*2)
            self.assertEqual([receive(raw)[0] for _ in range(8)],[b"T",b"D",b"C",b"Z"]*2)

    def test_flush_delivers_extended_results_before_sync(self):
        with raw_connect() as raw:
            parse=frame(b"P",b"\0SELECT 1\0"+struct.pack("!H",0))
            bind=frame(b"B",b"\0\0"+struct.pack("!HHH",0,0,0))
            raw.sendall(parse+bind+frame(b"D",b"P\0")+frame(b"E",b"\0"+struct.pack("!I",0))+frame(b"H"))
            self.assertEqual([receive(raw)[0] for _ in range(5)],[b"1",b"2",b"T",b"D",b"C"])
            raw.sendall(frame(b"S"))
            self.assertEqual(receive(raw),(b"Z",b"I"))

    def test_error_discards_pipeline_until_sync(self):
        table = "pipeline_" + uuid.uuid4().hex[:8]
        with connect() as connection:
            connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY)")
        with raw_connect() as raw:
            invalid = frame(b"P", b"bad\0SAVEPOINT unsupported\0" + struct.pack("!H", 0))
            parse = frame(b"P", b"\0" + f"INSERT INTO {table} VALUES (1)".encode() + b"\0" + struct.pack("!H", 0))
            bind = frame(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
            execute = frame(b"E", b"\0" + struct.pack("!I", 0))
            raw.sendall(invalid + parse + bind + execute + frame(b"S"))
            self.assertEqual(receive(raw)[0], b"E")
            self.assertEqual(receive(raw), (b"Z", b"I"))
            raw.sendall(frame(b"Q", b"SELECT 1\0"))
            self.assertEqual([receive(raw)[0] for _ in range(4)], [b"T", b"D", b"C", b"Z"])
        with connect() as connection:
            self.assertIsNone(connection.execute(f"SELECT * FROM {table} WHERE id = 1").fetchone())

    def test_exhausted_portal_does_not_replay_rows(self):
        with raw_connect() as raw:
            parse = frame(b"P", b"\0SELECT 1\0" + struct.pack("!H", 0))
            bind = frame(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
            describe = frame(b"D", b"P\0")
            execute = frame(b"E", b"\0" + struct.pack("!I", 0))
            raw.sendall(parse + bind + describe + execute + execute + frame(b"S"))
            responses = [receive(raw) for _ in range(7)]
            self.assertEqual([tag for tag, _ in responses], [b"1", b"2", b"T", b"D", b"C", b"C", b"Z"])
            self.assertEqual(responses[5], (b"C", b"SELECT 0\0"))


class DatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table = "it_" + uuid.uuid4().hex[:12]
        with connect() as connection:
            connection.execute(f"CREATE TABLE {cls.table} {SCHEMA}")

    def setUp(self):
        self.connection = connect()

    def tearDown(self):
        self.connection.close()

    def sql_error(self, sql, expected="0A000", params=None):
        with self.assertRaises(psycopg.Error) as error:
            self.connection.execute(sql, params)
        self.assertEqual(error.exception.sqlstate, expected, str(error.exception))
        self.assertEqual(self.connection.execute("SELECT 1").fetchone(), (1,), "session must recover after error")

    def test_signed_key_boundaries_and_both_shards(self):
        keys = [-(2**63), -(2**63) + 1, -1, 0, 1, 2**63 - 2, 2**63 - 1]
        for key in keys:
            with self.subTest(key=key):
                text = f"key={key}; quotes ' and unicode 雪"
                self.connection.execute(f"INSERT INTO {self.table} VALUES (%s, %s, %s)", (key, text, True))
                row = self.connection.execute(f"SELECT * FROM {self.table} WHERE id = %s", (key,)).fetchone()
                self.assertEqual(row, (key, text, True))
                owner = ((key & ((1 << 64) - 1)) ^ (1 << 63)) % 2
                for shard in range(2):
                    with closing(tarantool.Connection(f"shard{shard}", 3301, user="reader", password="local-reader")) as native:
                        result = native.select(self.table, [key]).data
                        self.assertEqual(result, [[key, text, True]] if owner == shard else [])

    def test_update_delete_and_missing_rows(self):
        key = 1001
        self.connection.execute(f"INSERT INTO {self.table} VALUES (%s, %s, %s)", (key, "before", False))
        changed = self.connection.execute(f"UPDATE {self.table} SET value = %s, enabled = %s WHERE id = %s", ("after", True, key))
        self.assertEqual(changed.rowcount, 1)
        self.assertEqual(self.connection.execute(f"SELECT value AS message, enabled FROM {self.table} WHERE id = %s", (key,)).fetchone(), ("after", True))
        self.assertEqual(self.connection.execute(f"DELETE FROM {self.table} WHERE id = %s", (key,)).rowcount, 1)
        self.assertIsNone(self.connection.execute(f"SELECT * FROM {self.table} WHERE id = %s", (key,)).fetchone())
        self.assertEqual(self.connection.execute(f"DELETE FROM {self.table} WHERE id = %s", (key,)).rowcount, 0)
        self.assertEqual(self.connection.execute(f"UPDATE {self.table} SET value = 'missing' WHERE id = %s", (key,)).rowcount, 0)

    def test_binary_parameters_and_results(self):
        key = 2002
        self.connection.execute(f"INSERT INTO {self.table} (enabled, id, value) VALUES (%b, %b, %b)", (False, key, "binary 雪"))
        with self.connection.cursor(binary=True) as cursor:
            cursor.execute(f"SELECT * FROM {self.table} WHERE id = %b", (key,))
            self.assertEqual(cursor.fetchone(), (key, "binary 雪", False))
            self.assertEqual([column.type_code for column in cursor.description], [20, 25, 16])

    def test_null_and_omitted_columns(self):
        self.connection.execute(f"INSERT INTO {self.table} (id, enabled) VALUES (%s, %s)", (3003, True))
        self.assertEqual(self.connection.execute(f"SELECT * FROM {self.table} WHERE id = %s", (3003,)).fetchone(), (3003, None, True))
        self.sql_error(f"INSERT INTO {self.table} VALUES (%s, %s, %s)", "23502", (3004, None, None))

    def test_duplicate_key_is_not_overwrite(self):
        self.connection.execute(f"INSERT INTO {self.table} VALUES (4004, 'original', true)")
        self.sql_error(f"INSERT INTO {self.table} VALUES (4004, 'overwrite', true)", "23505")
        self.assertEqual(self.connection.execute(f"SELECT value FROM {self.table} WHERE id = 4004").fetchone(), ("original",))

    def test_reject_unsupported_clauses_and_transactions(self):
        for sql in [
            "SAVEPOINT unsupported",
            f"SELECT ROW_NUMBER() OVER () FROM {self.table}",
            f"SELECT DISTINCT ON (id) * FROM {self.table}",
            f"SELECT * FROM {self.table} WHERE id = 0 FOR UPDATE",
            f"DELETE FROM {self.table} WHERE id = 0 ORDER BY id",
            f"INSERT INTO {self.table} VALUES (5005, 'a', true) ON CONFLICT DO NOTHING",
            f"UPDATE {self.table} SET value = (SELECT 'unsupported') WHERE id = 0",
            f"CREATE TABLE bad_{uuid.uuid4().hex[:8]} (id BIGINT PRIMARY KEY, v TEXT DEFAULT 'hidden')",
        ]:
            with self.subTest(sql=sql): self.sql_error(sql)

    def test_simple_query_multi_statement_rejected_before_mutation(self):
        # ClientCursor uses the simple-query protocol with client-side parameters.
        with psycopg.ClientCursor(self.connection) as cursor:
            with self.assertRaises(psycopg.Error) as error:
                cursor.execute(f"INSERT INTO {self.table} VALUES (6006, 'must not commit', true); SELECT 1")
            self.assertEqual(error.exception.sqlstate, "0A000")
        self.assertIsNone(self.connection.execute(f"SELECT * FROM {self.table} WHERE id = 6006").fetchone())

    def test_injection_is_data(self):
        value = "'); DELETE FROM arbitrary; -- 雪"
        self.connection.execute(f"INSERT INTO {self.table} VALUES (%s, %s, %s)", (7007, value, True))
        self.assertEqual(self.connection.execute(f"SELECT value FROM {self.table} WHERE id = %s", (7007,)).fetchone(), (value,))

    def test_idempotent_ddl_requires_same_schema(self):
        self.connection.execute(f"CREATE TABLE IF NOT EXISTS {self.table} {SCHEMA}")
        self.sql_error(f"CREATE TABLE IF NOT EXISTS {self.table} (id BIGINT PRIMARY KEY, other TEXT)", "XX000")
        self.sql_error(f"CREATE TABLE {self.table} {SCHEMA}", "XX000")

    def test_native_reader_cannot_write_or_eval(self):
        with closing(tarantool.Connection("shard0", 3301, user="reader", password="local-reader")) as native:
            with self.assertRaises(tarantool.DatabaseError):
                native.insert(self.table, [8008, "bypass", True])
            with self.assertRaises(tarantool.DatabaseError):
                native.eval("return box.space._cairn_catalog:select{}")

    def test_storage_rejects_wrong_shard(self):
        # The gateway service account only has execute grants, not direct space writes.
        with closing(tarantool.Connection("shard1", 3301, user="gateway", password="local-gateway")) as native:
            with self.assertRaises(tarantool.DatabaseError):
                native.call("cairn.insert", self.table, [9008, "wrong shard", True])
            with self.assertRaises(tarantool.DatabaseError):
                native.insert(self.table, [9009, "bypass", True])

    def test_authentication_rejects_bad_password(self):
        with self.assertRaises(psycopg.OperationalError):
            psycopg.connect(DSN.replace("local-postgres", "wrong-password"), autocommit=True)

    def test_truncated_and_oversized_startup_frames(self):
        host = "gateway"
        for length in [0, 3, 1024 * 1024 + 1, 0xFFFFFFFF]:
            with self.subTest(length=length), socket.create_connection((host, 5432), timeout=3) as raw:
                raw.sendall(struct.pack("!I", length))
                self.assertEqual(raw.recv(1), b"E")
        with socket.create_connection((host, 5432), timeout=3) as raw:
            raw.sendall(struct.pack("!I", 4))
            self.assertEqual(raw.recv(1), b"E")


if __name__ == "__main__":
    for attempt in range(30):
        try:
            with connect(): pass
            break
        except psycopg.OperationalError:
            if attempt == 29: raise
            time.sleep(0.5)
    unittest.main(verbosity=2)
