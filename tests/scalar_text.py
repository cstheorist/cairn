"""SELECT-only text scalar input comparisons against PostgreSQL 16.

Run with docker compose -f compose.replication.yaml --profile test run --no-deps
--rm tests python /tests/scalar_text.py while gateway and reference are running.
"""
import struct
import unittest

import psycopg
from encoding import DSNS, bind, errors, exchange, execute, parse, raw_connection, simple


INTEGER_TYPES = [("SMALLINT", 2), ("INTEGER", 4), ("BIGINT", 8)]
ASCII_SPACE = " \t\n\r\v\f"


def spelling(magnitude, radix):
    if radix == 2:
        return "0b" + format(magnitude, "b")
    if radix == 8:
        return "0o" + format(magnitude, "o")
    if radix == 16:
        return "0x" + format(magnitude, "x")
    return str(magnitude)


def expected_row(kind, value):
    if value is None:
        return struct.pack("!Hi", 1, -1)
    width = dict(INTEGER_TYPES).get(kind, 1)
    data = bytes([int(value)]) if kind == "BOOLEAN" else value.to_bytes(width, "big", signed=True)
    return struct.pack("!Hi", 1, len(data)) + data


class ScalarText(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with psycopg.connect(DSNS[-1], autocommit=True) as reference:
            assert reference.info.server_version // 10000 == 16, "PostgreSQL 16 reference required"

    def check_cases(self, cases):
        for endpoint, dsn in enumerate(DSNS):
            with raw_connection(dsn) as raw:
                for nested in (False, True):
                    for kind, text, value, sqlstate in cases:
                        label = text if text is None or len(text) < 80 else f"{text[:20]!r}... ({len(text)} characters)"
                        with self.subTest(endpoint=endpoint, nested=nested, kind=kind, text=label):
                            source = "CAST($1 AS TEXT)" if nested else "$1"
                            query = f"SELECT CAST({source} AS {kind})"
                            parameter = None if text is None else text.encode()
                            messages = parse(query) + bind([parameter], 0, 1) + execute()
                            if sqlstate:
                                messages += parse("SELECT 999") + bind([]) + execute()
                            responses = exchange(raw, messages)
                            self.assertEqual(errors(responses), [sqlstate] if sqlstate else [])
                            self.assertEqual(responses[-1], (b"Z", b"I"))
                            rows = [payload for tag, payload in responses if tag == b"D"]
                            if sqlstate:
                                self.assertEqual(rows, [])
                                # PostgreSQL may fold the nested cast while
                                # planning at Bind; Cairn evaluates it at Execute.
                                allowed = [[b"1", b"E", b"Z"]]
                                if nested:
                                    allowed.append([b"1", b"2", b"E", b"Z"])
                                self.assertIn([tag for tag, _ in responses], allowed)
                                recovered = simple(raw, "SELECT 1")
                                self.assertEqual(errors(recovered), [])
                                self.assertEqual(recovered[-1], (b"Z", b"I"))
                            else:
                                self.assertEqual(rows, [expected_row(kind, value)])

    def test_integer_sign_radix_separator_and_ascii_whitespace(self):
        valid = [("42", 42), ("+00042", 42), ("-0", 0), ("052", 52),
                 ("0x2a", 42), ("+0X2A", 42), ("-0x2a", -42), ("0o52", 42),
                 ("0B10_1010", 42), ("1_000", 1000), ("0_42", 42),
                 ("0x_2a", 42), ("0o_52", 42), ("0b_101010", 42)]
        cases = []
        for kind, _ in INTEGER_TYPES:
            cases += [(kind, text, value, None) for text, value in valid]
            cases += [(kind, space + "+42" + space, 42, None) for space in ASCII_SPACE]
            cases.append((kind, ASCII_SPACE + "-42" + ASCII_SPACE, -42, None))
        self.check_cases(cases)

    def test_signed_bounds_in_every_radix(self):
        cases = []
        for kind, width in INTEGER_TYPES:
            limit = 1 << (width * 8 - 1)
            for magnitude in (limit - 1, limit, limit + 1):
                for radix in (2, 8, 10, 16):
                    for sign in ("+", "-"):
                        value = -magnitude if sign == "-" else magnitude
                        state = None if -limit <= value < limit else "22003"
                        cases.append((kind, sign + spelling(magnitude, radix), value, state))
        self.check_cases(cases)

    def test_malformed_input_and_overflow_error_precedence(self):
        malformed = ["", ASCII_SPACE, "+", "-", "+ 1", "- 1", "++1", "--1", "_42", "42_", "4__2",
                     "0x", "0x_", "0x__1", "0b2", "0o8", "0xg", "1.0", "1e2", "12 34", "123abc", "４２"]
        cases = []
        for kind, width in INTEGER_TYPES:
            cases += [(kind, text, None, "22P02") for text in malformed]
            limit = 1 << (width * 8 - 1)
            for radix in (2, 8, 10, 16):
                boundary = spelling(limit, radix)
                cases += [(kind, boundary + suffix, None, state)
                          for suffix, state in [("g", "22P02"), ("_", "22P02"), ("0g", "22003"), ("_0g", "22003")]]
        self.check_cases(cases)

    def test_boolean_unique_prefixes_case_and_ascii_whitespace(self):
        cases = []
        for word, value, minimum in [("true", True, 1), ("false", False, 1), ("yes", True, 1),
                                     ("no", False, 1), ("on", True, 2), ("off", False, 2), ("1", True, 1), ("0", False, 1)]:
            for length in range(minimum, len(word) + 1):
                prefix = word[:length]
                cases += [("BOOLEAN", text, value, None)
                          for text in [prefix, prefix.upper(), ASCII_SPACE + prefix + ASCII_SPACE]]
        cases += [("BOOLEAN", text, None, "22P02") for text in
                  ["", ASCII_SPACE, "o", "O", "00", "01", "10", "2", "+1", "-0", "truex", "yesno", "onn", "ofx"]]
        self.check_cases(cases)

    def test_unicode_whitespace_is_not_scalar_whitespace(self):
        cases = []
        for kind, value in [("SMALLINT", "42"), ("INTEGER", "42"), ("BIGINT", "42"), ("BOOLEAN", "true")]:
            for space in ["\u0085", "\u00a0", "\u1680", "\u2003", "\u2028", "\u2029", "\u202f", "\u3000"]:
                cases += [(kind, text, None, "22P02") for text in [space + value, value + space]]
        self.check_cases(cases)

    def test_nulls_and_bounded_long_inputs(self):
        length = 64 * 1024
        cases = []
        for kind, _ in INTEGER_TYPES:
            cases += [(kind, None, None, None), (kind, "0" * length + "42", 42, None),
                      (kind, "0x" + "0_" * (length // 2) + "2a", 42, None),
                      (kind, " " * length + "+42" + "\v" * length, 42, None),
                      (kind, "0" * length + "_", None, "22P02"), (kind, "9" * length, None, "22003")]
        cases += [("BOOLEAN", None, None, None), ("BOOLEAN", " " * length + "TrU" + "\v" * length, True, None),
                  ("BOOLEAN", "true" + " " * length + "x", None, "22P02")]
        self.check_cases(cases)


if __name__ == "__main__":
    unittest.main(verbosity=2)
