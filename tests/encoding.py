"""Raw Bind encoding and error recovery, compared with PostgreSQL 16.

Run with docker compose -f compose.replication.yaml --profile test run --no-deps
--rm tests python /tests/encoding.py while gateway and reference are running.
"""
from contextlib import contextmanager
import os
import socket
import struct
import unittest
import uuid

import psycopg
from integration import frame, receive


DSNS = [
    os.environ["CAIRN_DSN"],
    os.environ.get("REFERENCE_DSN", "host=reference port=5432 user=cairn dbname=cairn password=local-postgres sslmode=disable"),
]
INVALID_TEXT = [
    b"\0", b"\0leading", b"middle\0suffix", b"trailing\0", b"two\0\0zeros",
    b"\xff", b"\x80", b"truncated\xe9\x9b", b"\xc0\x80",
    b"\xed\xa0\x80", b"\xf4\x90\x80\x80",
]


@contextmanager
def raw_connection(dsn):
    # Let libpq handle the reference's SCRAM authentication, then send Bind
    # frames directly. Text-format libpq parameters use C strings, and Psycopg's
    # normal text adapter rejects NUL locally, so neither can exercise these bytes.
    with psycopg.connect(dsn, autocommit=True) as connection:
        with socket.socket(fileno=os.dup(connection.fileno())) as raw:
            raw.settimeout(5)
            yield raw


def parse(query, statement=b""):
    return frame(b"P", statement + b"\0" + query.encode() + b"\0" + struct.pack("!H", 0))


def bind(parameters, input_format=0, result_format=0, statement=b"", portal=b""):
    payload = portal + b"\0" + statement + b"\0"
    payload += struct.pack("!HH", 1, input_format) + struct.pack("!H", len(parameters))
    for value in parameters:
        payload += struct.pack("!i", -1 if value is None else len(value))
        if value is not None:
            payload += value
    return frame(b"B", payload + struct.pack("!HH", 1, result_format))


def execute(portal=b""):
    return frame(b"E", portal + b"\0" + struct.pack("!I", 0))


def exchange(raw, messages):
    raw.sendall(messages + frame(b"S"))
    responses = []
    while True:
        response = receive(raw)
        responses.append(response)
        if response[0] == b"Z":
            return responses


def simple(raw, query):
    raw.sendall(frame(b"Q", query.encode() + b"\0"))
    responses = []
    while True:
        response = receive(raw)
        responses.append(response)
        if response[0] == b"Z":
            return responses


def errors(responses):
    return [
        dict((field[:1], field[1:]) for field in payload.split(b"\0") if field)[b"C"].decode()
        for tag, payload in responses if tag == b"E"
    ]


class Encoding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table = "encoding_" + uuid.uuid4().hex[:8]
        for index, dsn in enumerate(DSNS):
            with psycopg.connect(dsn, autocommit=True) as connection:
                if index == 1:
                    assert connection.info.server_version // 10000 == 16, "PostgreSQL 16 reference required"
                connection.execute(f"CREATE TABLE {cls.table} (id BIGINT PRIMARY KEY, value TEXT)")

    def assert_rejected(self, responses, state=b"I", sqlstate="22021"):
        self.assertEqual(errors(responses), [sqlstate], responses)
        self.assertEqual(responses[-1], (b"Z", state))

    def assert_usable(self, raw):
        responses = simple(raw, "SELECT 1")
        self.assertEqual(errors(responses), [])
        self.assertEqual(responses[-1], (b"Z", b"I"))

    def assert_empty(self, raw):
        responses = simple(raw, f"SELECT id FROM {self.table}")
        self.assertEqual(errors(responses), [])
        self.assertFalse(any(tag == b"D" for tag, _ in responses), responses)

    def test_valid_utf8_empty_and_null_round_trip(self):
        values = [b"", b"ascii", "雪🦀".encode(), "\u0001\u007f\U0010ffff".encode(), None]
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for input_format in (0, 1):
                    for result_format in (0, 1):
                        for value in values:
                            with self.subTest(endpoint=endpoint, input_format=input_format, result_format=result_format, value=value):
                                responses = exchange(raw, parse("SELECT CAST($1 AS TEXT)")
                                                     + bind([value], input_format, result_format) + execute())
                                self.assertEqual(errors(responses), [])
                                expected = struct.pack("!Hi", 1, -1 if value is None else len(value))
                                if value is not None:
                                    expected += value
                                self.assertEqual([payload for tag, payload in responses if tag == b"D"], [expected])

    def test_invalid_bytes_reject_at_bind_and_recover_at_sync(self):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for input_format in (0, 1):
                    for value in INVALID_TEXT:
                        with self.subTest(endpoint=endpoint, input_format=input_format, value=value):
                            responses = exchange(raw, parse("SELECT CAST($1 AS TEXT)")
                                                 + bind([value], input_format) + execute()
                                                 + parse("SELECT 999") + bind([]) + execute())
                            self.assert_rejected(responses)
                            self.assertEqual([tag for tag, _ in responses], [b"1", b"E", b"Z"])
                            self.assert_usable(raw)

    def test_text_format_checks_encoding_before_scalar_conversion(self):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for kind, value in [("BIGINT", b"1\0tail"), ("INTEGER", b"1\0"),
                                    ("SMALLINT", b"\xff"), ("BOOLEAN", b"t\0tail")]:
                    with self.subTest(endpoint=endpoint, kind=kind, value=value):
                        responses = exchange(raw, parse(f"SELECT CAST($1 AS {kind})") + bind([value]) + execute())
                        self.assert_rejected(responses)
                        self.assert_usable(raw)
                # Binary scalar representations legitimately contain zero bytes.
                responses = exchange(raw, parse("SELECT CAST($1 AS BIGINT)")
                                     + bind([struct.pack("!q", 0)], 1, 1) + execute())
                self.assertEqual(errors(responses), [])
                self.assertEqual([payload for tag, payload in responses if tag == b"D"], [struct.pack("!Hiq", 1, 8, 0)])

    def test_binary_boolean_accepts_any_nonzero_byte(self):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for result_format in (0, 1):
                    for byte in (0, 1, 2, 127, 128, 255):
                        with self.subTest(endpoint=endpoint, result_format=result_format, byte=byte):
                            responses = exchange(raw, parse("SELECT CAST($1 AS BOOLEAN)")
                                                 + bind([bytes([byte])], 1, result_format) + execute())
                            self.assertEqual(errors(responses), [])
                            expected = bytes([int(byte != 0)]) if result_format else b"t" if byte else b"f"
                            self.assertEqual([payload for tag, payload in responses if tag == b"D"],
                                             [struct.pack("!Hi", 1, 1) + expected])

    def test_binary_scalar_lengths_distinguish_truncation_and_excess(self):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for kind, width in [("BOOLEAN", 1), ("SMALLINT", 2), ("INTEGER", 4), ("BIGINT", 8)]:
                    for length in range(width + 2):
                        if length == width:
                            continue
                        with self.subTest(endpoint=endpoint, kind=kind, length=length):
                            responses = exchange(raw, parse(f"SELECT CAST($1 AS {kind})")
                                                 + bind([b"\0" * length], 1) + execute()
                                                 + parse("SELECT 999") + bind([]) + execute())
                            self.assert_rejected(responses, sqlstate="08P01" if length < width else "22P03")
                            self.assertEqual([tag for tag, _ in responses], [b"1", b"E", b"Z"])
                            self.assert_usable(raw)

    def test_binary_integer_signed_boundaries_and_null(self):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for kind, width in [("SMALLINT", 2), ("INTEGER", 4), ("BIGINT", 8)]:
                    minimum, maximum = -(1 << (width * 8 - 1)), (1 << (width * 8 - 1)) - 1
                    for result_format in (0, 1):
                        for value in [minimum, -1, 0, 1, maximum, None]:
                            with self.subTest(endpoint=endpoint, kind=kind, result_format=result_format, value=value):
                                parameter = None if value is None else value.to_bytes(width, "big", signed=True)
                                responses = exchange(raw, parse(f"SELECT CAST($1 AS {kind})")
                                                     + bind([parameter], 1, result_format) + execute())
                                self.assertEqual(errors(responses), [])
                                result = parameter if result_format or value is None else str(value).encode()
                                expected = struct.pack("!Hi", 1, -1 if result is None else len(result))
                                if result is not None:
                                    expected += result
                                self.assertEqual([payload for tag, payload in responses if tag == b"D"], [expected])

    def test_bad_bind_rolls_back_prior_implicit_pipeline_mutations(self):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for input_format, value in [(0, b"bad\0text"), (1, b"bad\0text"), (0, b"\xff"), (1, b"\xff")]:
                    with self.subTest(endpoint=endpoint, input_format=input_format, value=value):
                        pipeline = b""
                        for key in (0, 1):
                            pipeline += parse(f"INSERT INTO {self.table} VALUES ({key}, 'staged')") + bind([]) + execute()
                        pipeline += parse(f"INSERT INTO {self.table} VALUES (2, $1)") + bind([value], input_format) + execute()
                        pipeline += parse(f"INSERT INTO {self.table} VALUES (3, 'ignored')") + bind([]) + execute()
                        responses = exchange(raw, pipeline)
                        self.assert_rejected(responses)
                        self.assertEqual([tag for tag, _ in responses], [b"1", b"2", b"C", b"1", b"2", b"C", b"1", b"E", b"Z"])
                        self.assert_empty(raw)
                        self.assert_usable(raw)

    def test_bad_bind_aborts_explicit_transaction_until_rollback(self):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for input_format, value in [(0, b"bad\0text"), (1, b"bad\0text"), (0, b"\xff"), (1, b"\xff")]:
                    with self.subTest(endpoint=endpoint, input_format=input_format, value=value):
                        self.assertEqual(simple(raw, "BEGIN")[-1], (b"Z", b"T"))
                        for key in (0, 1):
                            responses = simple(raw, f"INSERT INTO {self.table} VALUES ({key}, 'staged')")
                            self.assertEqual(errors(responses), [])
                        responses = exchange(raw, parse(f"INSERT INTO {self.table} VALUES (2, $1)")
                                             + bind([value], input_format) + execute())
                        self.assert_rejected(responses, b"E")
                        self.assertEqual(errors(simple(raw, "SELECT 1")), ["25P02"])
                        self.assertEqual(simple(raw, "ROLLBACK")[-1], (b"Z", b"I"))
                        self.assert_empty(raw)
                        self.assert_usable(raw)


if __name__ == "__main__":
    unittest.main(verbosity=2)
