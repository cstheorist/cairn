"""Compare relational results and result type OIDs against PostgreSQL 16."""
import os
import unittest
import uuid
import psycopg
from integration import frame, receive, raw_connect
import struct

DSN = os.environ["CAIRN_DSN"]
REFERENCE = "host=reference port=5432 user=cairn dbname=cairn password=local-postgres sslmode=disable"

class Relational(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        suffix = uuid.uuid4().hex[:8]
        cls.table, cls.other = "query_" + suffix, "joined_" + suffix
        cls.connections = [psycopg.connect(dsn, autocommit=True, prepare_threshold=0) for dsn in [DSN, REFERENCE]]
        data = [(0, "red", 10, True), (1, "blue", 20, False), (2, "red", None, True),
                (3, None, 30, None), (4, "green", 5, False), (5, "blue", -5, True)]
        for connection in cls.connections:
            connection.execute(f"CREATE TABLE {cls.table} (id BIGINT PRIMARY KEY, label TEXT, score BIGINT, enabled BOOLEAN)")
            connection.execute(f"CREATE TABLE {cls.other} (id BIGINT PRIMARY KEY, owner BIGINT, name TEXT)")
            for row in data: connection.execute(f"INSERT INTO {cls.table} VALUES (%s, %s, %s, %s)", row)
            for row in [(10,0,"a"),(11,0,"b"),(12,1,None),(13,7,"outside")]:
                connection.execute(f"INSERT INTO {cls.other} VALUES (%s, %s, %s)", row)

    @classmethod
    def tearDownClass(cls):
        for connection in cls.connections: connection.close()

    def same(self, query, parameters=None):
        results = []
        for connection in self.connections:
            cursor = connection.execute(query, parameters)
            results.append((cursor.fetchall(), [(column.name, column.type_code) for column in cursor.description]))
        self.assertEqual(results[0], results[1], query)

    def test_scans_predicates_order_and_limits(self):
        for query in [
            f"SELECT * FROM {self.table} ORDER BY id",
            f"SELECT id FROM {self.table} WHERE enabled OR (score >= 10 AND label IS NOT NULL) ORDER BY id",
            f"SELECT id, COALESCE(label, 'unknown') AS label, score + 2 AS n FROM {self.table} ORDER BY score ASC NULLS FIRST, id DESC LIMIT 3 OFFSET 1",
            f"SELECT id FROM {self.table} WHERE score BETWEEN 0 AND 20 ORDER BY id",
            f"SELECT score IN (10, NULL) AS matched FROM {self.table} ORDER BY id",
            f"SELECT id FROM {self.table} WHERE score NOT IN (10, NULL) ORDER BY id",
            f"SELECT * FROM {self.table} LIMIT 0",
        ]:
            with self.subTest(query=query): self.same(query)
        self.same(f"SELECT id FROM {self.table} WHERE id IN (%s, %s) ORDER BY id", (1,4))

    def test_grouping_aggregates_and_distinct(self):
        for query in [
            f"SELECT label, COUNT(*) AS n, COUNT(score) AS observed, MIN(score) AS lo, MAX(score) AS hi FROM {self.table} GROUP BY label HAVING COUNT(*) >= 1 ORDER BY label NULLS LAST",
            f"SELECT DISTINCT label FROM {self.table} ORDER BY label NULLS FIRST",
            f"SELECT label, COUNT(*) AS n FROM {self.table} GROUP BY 1 ORDER BY n DESC, label",
            f"SELECT COUNT(*), MIN(score) FROM {self.table} WHERE id = -100",
            f"SELECT COUNT(DISTINCT label) AS n FROM {self.table}",
        ]:
            with self.subTest(query=query): self.same(query)

    def test_joins(self):
        for join in ["INNER", "LEFT", "RIGHT", "FULL"]:
            query = f"SELECT a.id, b.id, b.name FROM {self.table} a {join} JOIN {self.other} b ON a.id = b.owner ORDER BY a.id NULLS FIRST, b.id"
            with self.subTest(join=join): self.same(query)
        self.same(f"SELECT a.id, b.id FROM {self.table} a CROSS JOIN {self.other} b WHERE a.id = 0 AND b.owner = 0 ORDER BY 1,2")
        self.same(f"SELECT a.id, b.name FROM {self.table} a LEFT JOIN {self.other} b ON a.id = b.owner AND b.name IS NOT NULL ORDER BY a.id,b.id")

    def test_expressions_and_types(self):
        self.same("SELECT TRUE AND NULL AS a, FALSE OR NULL AS b, NOT NULL AS c")
        self.same("SELECT 1 + 2 AS a, -5 * 4 AS b, 9 / 2 AS c, 9 % 2 AS d")
        self.same(f"SELECT LOWER(label), UPPER(label), LENGTH(label) FROM {self.table} WHERE id = 1")
        self.same(f"SELECT id + score AS total FROM {self.table} WHERE id = %s", (2,))
        self.same("SELECT CAST(12 AS SMALLINT) + CAST(3 AS SMALLINT) AS n, CAST(1 AS INTEGER) + CAST(2 AS BIGINT) AS wide, ABS(CAST(-7 AS INTEGER)) AS absolute")
        self.same("SELECT COALESCE(NULL, 1, CAST(2 AS BIGINT)) AS wide")
        self.same(f"SELECT * FROM {self.table} WHERE id = NULL")
        self.same(f"SELECT * FROM {self.table} WHERE id = %s", (None,))
        self.same("SELECT CAST(%s AS BIGINT) AS n, CAST(%s AS TEXT) AS label", (123,"parameter"))

    def test_integer_storage_and_binary(self):
        table = "ints_" + uuid.uuid4().hex[:8]
        for connection in self.connections:
            connection.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, small SMALLINT, large BIGINT)")
            for row in [(-2147483648,-32768,-9223372036854775808),(2147483647,32767,9223372036854775807)]:
                connection.execute(f"INSERT INTO {table} VALUES (%s,%s,%s)", row)
        self.same(f"SELECT * FROM {table} ORDER BY id")
        results = []
        for connection in self.connections:
            with connection.cursor(binary=True) as cursor:
                cursor.execute(f"SELECT * FROM {table} ORDER BY id")
                results.append((cursor.fetchall(), [column.type_code for column in cursor.description]))
        self.assertEqual(results[0], results[1])
        for query in [f"INSERT INTO {table} VALUES (1,32768,0)", "SELECT 2147483647 + 1", "SELECT CAST(32767 AS SMALLINT) + CAST(1 AS SMALLINT)", "SELECT ABS(CAST(-2147483648 AS INTEGER))"]:
            states = []
            for connection in self.connections:
                with self.assertRaises(psycopg.Error) as error: connection.execute(query)
                states.append(error.exception.sqlstate)
            self.assertEqual(states, ["22003", "22003"], query)

    def test_errors_and_recovery(self):
        for query in [
            "SELECT 1 / 0", "SELECT 9223372036854775807 + 1",
            f"SELECT id, COUNT(*) FROM {self.table}", f"SELECT missing FROM {self.table}",
            f"SELECT id FROM {self.table} LIMIT -1", f"SELECT id FROM {self.table} OFFSET -1",
        ]:
            states = []
            for connection in self.connections:
                with self.assertRaises(psycopg.Error) as error: connection.execute(query)
                states.append(error.exception.sqlstate)
                self.assertEqual(connection.execute("SELECT 1").fetchone(), (1,))
            self.assertEqual(states[0], states[1], query)

    def test_portal_suspension_and_resume(self):
        with raw_connect() as raw:
            query = f"SELECT id FROM {self.table} ORDER BY id".encode()
            parse = frame(b"P", b"\0" + query + b"\0" + struct.pack("!H", 0))
            bind = frame(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
            describe = frame(b"D", b"P\0")
            execute = lambda count: frame(b"E", b"\0" + struct.pack("!I", count))
            raw.sendall(parse + bind + describe + execute(2) + execute(2) + execute(0) + frame(b"S"))
            responses = []
            while True:
                response = receive(raw); responses.append(response)
                if response[0] == b"Z": break
            self.assertEqual([tag for tag, _ in responses], [b"1",b"2",b"T",b"D",b"D",b"s",b"D",b"D",b"s",b"D",b"D",b"C",b"Z"])
            self.assertEqual(responses[-2], (b"C", b"SELECT 6\0"))

if __name__ == "__main__": unittest.main(verbosity=2)
