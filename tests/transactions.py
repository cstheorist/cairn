"""PostgreSQL transaction semantics over independent replicated Tarantool shards."""
from contextlib import closing
import os
import time
import unittest
import uuid

import psycopg
from psycopg.pq import TransactionStatus
from replication import native, leader

DSN = os.environ["CAIRN_DSN"]


def connect(autocommit=True):
    return psycopg.connect(DSN, autocommit=autocommit, prepare_threshold=0)


class Transactions(unittest.TestCase):
    def setUp(self):
        self.table = "sqltx_" + uuid.uuid4().hex[:8]
        self.connection = connect()
        self.connection.execute(f"CREATE TABLE {self.table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        for key in (0, 1): self.connection.execute(f"INSERT INTO {self.table} VALUES (%s, 'before')", (key,))

    def tearDown(self): self.connection.close()

    def value(self, connection, key):
        row = connection.execute(f"SELECT value FROM {self.table} WHERE id = %s", (key,)).fetchone()
        return row[0] if row else None

    def test_cross_shard_commit_and_own_reads(self):
        self.connection.execute("BEGIN")
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.INTRANS)
        for key in (0, 1):
            self.connection.execute(f"UPDATE {self.table} SET value = 'committed' WHERE id = %s", (key,))
            self.assertEqual(self.value(self.connection, key), "committed")
            with closing(native(leader(key)["node"])) as direct:
                self.assertEqual(direct.select(self.table, [key]).data, [[key, "before"]])
        self.connection.execute("COMMIT")
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        for key in (0, 1): self.assertEqual(self.value(self.connection, key), "committed")

    def test_rollback_and_delete_then_insert(self):
        self.connection.execute("BEGIN")
        self.connection.execute(f"DELETE FROM {self.table} WHERE id = 0")
        self.assertIsNone(self.value(self.connection, 0))
        self.connection.execute(f"INSERT INTO {self.table} VALUES (0, 'replacement')")
        self.assertEqual(self.value(self.connection, 0), "replacement")
        self.connection.execute(f"UPDATE {self.table} SET value = 'other' WHERE id = 1")
        self.connection.execute("ROLLBACK")
        for key in (0, 1): self.assertEqual(self.value(self.connection, key), "before")

    def test_error_aborts_all_shards_and_commit_becomes_rollback(self):
        self.connection.execute("BEGIN")
        self.connection.execute(f"UPDATE {self.table} SET value = 'uncommitted' WHERE id = 0")
        with self.assertRaises(psycopg.errors.UniqueViolation):
            self.connection.execute(f"INSERT INTO {self.table} VALUES (1, 'duplicate')")
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.INERROR)
        with self.assertRaises(psycopg.errors.InFailedSqlTransaction): self.value(self.connection, 0)
        cursor = self.connection.execute("COMMIT")
        self.assertEqual(cursor.statusmessage, "ROLLBACK")
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        for key in (0, 1): self.assertEqual(self.value(self.connection, key), "before")

    def test_read_only_transaction(self):
        self.connection.execute("BEGIN READ ONLY")
        self.assertEqual(self.value(self.connection, 0), "before")
        with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
            self.connection.execute(f"UPDATE {self.table} SET value = 'forbidden' WHERE id = 1")
        self.connection.execute("ROLLBACK")
        self.assertEqual(self.value(self.connection, 1), "before")

    def test_default_psycopg_transaction_context(self):
        with connect(autocommit=False) as connection:
            for key in (0, 1): connection.execute(f"UPDATE {self.table} SET value = 'context' WHERE id = %s", (key,))
        for key in (0, 1): self.assertEqual(self.value(self.connection, key), "context")

    def test_disconnect_rolls_back_and_releases_locks(self):
        connection = connect()
        connection.execute("BEGIN")
        for key in (0, 1): connection.execute(f"UPDATE {self.table} SET value = 'disconnected' WHERE id = %s", (key,))
        connection.close()
        for _ in range(60):
            try:
                self.assertEqual(self.value(self.connection, 0), "before")
                self.assertEqual(self.value(self.connection, 1), "before")
                break
            except psycopg.errors.SerializationFailure: time.sleep(.05)
        else: self.fail("disconnected transaction retained row locks")

    def test_conflicting_writer_is_rejected_without_deadlock(self):
        self.connection.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        self.assertEqual(self.value(self.connection, 0), "before")
        with connect() as other:
            with self.assertRaises(psycopg.errors.SerializationFailure):
                other.execute(f"UPDATE {self.table} SET value = 'conflict' WHERE id = 0")
            self.connection.execute("COMMIT")
            other.execute(f"UPDATE {self.table} SET value = 'after-lock' WHERE id = 0")
        self.assertEqual(self.value(self.connection, 0), "after-lock")

    def test_failed_first_operation_can_rollback(self):
        self.connection.execute("BEGIN")
        self.connection.execute(f"UPDATE {self.table} SET value='held' WHERE id=0")
        with connect() as other:
            other.execute("BEGIN")
            with self.assertRaises(psycopg.errors.SerializationFailure):
                other.execute(f"UPDATE {self.table} SET value='blocked' WHERE id=0")
            self.assertEqual(other.execute("ROLLBACK").statusmessage,"ROLLBACK")
            self.assertEqual(other.info.transaction_status,TransactionStatus.IDLE)
            self.assertEqual(self.value(other,1),"before")
        self.connection.execute("ROLLBACK")

    def test_extended_pipeline_error_rolls_back_prior_mutations(self):
        with self.assertRaises(psycopg.errors.UniqueViolation):
            with self.connection.pipeline():
                self.connection.execute(f"UPDATE {self.table} SET value = 'pipeline' WHERE id = 0")
                self.connection.execute(f"INSERT INTO {self.table} VALUES (1, 'duplicate')")
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        self.assertEqual(self.value(self.connection, 0), "before")

    def test_cross_shard_write_skew_is_rejected(self):
        with connect() as other:
            self.connection.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            other.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            self.assertEqual(self.value(self.connection, 0), "before")
            self.assertEqual(self.value(other, 1), "before")
            with self.assertRaises(psycopg.errors.SerializationFailure):
                self.connection.execute(f"UPDATE {self.table} SET value = 'skew' WHERE id = 1")
            other.execute(f"UPDATE {self.table} SET value = 'serial' WHERE id = 0")
            other.execute("COMMIT")
            self.connection.execute("ROLLBACK")
        self.assertEqual(self.value(self.connection, 0), "serial")
        self.assertEqual(self.value(self.connection, 1), "before")


if __name__ == "__main__": unittest.main(verbosity=2)
