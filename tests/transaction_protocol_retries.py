"""Unit checks for known MVCC rollback versus uncertain native outcomes."""
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

import tarantool

os.environ.setdefault("CAIRN_DSN", "unused")
import transaction_protocol


class ResolutionRetries(unittest.TestCase):
    def invoke(self, function, outcomes):
        connection = Mock()
        connection.call.side_effect = outcomes
        patches = (
            patch.object(transaction_protocol, "leader", return_value={"node": "s0a", "term": 7}),
            patch.object(transaction_protocol, "native", return_value=connection),
            patch.object(transaction_protocol.time, "sleep"),
        )
        with patches[0], patches[1], patches[2] as sleep:
            try:
                result = transaction_protocol.call(0, function, "transaction", "proof")
            finally:
                self.connection = connection
                self.sleep = sleep
        return result

    def test_known_conflict_retries_each_resolution_phase(self):
        for function in ("tx_apply", "tx_complete", "tx_release", "tx_clean"):
            with self.subTest(function=function):
                result = self.invoke(function, [tarantool.DatabaseError(97, "conflict"),
                                               SimpleNamespace(data=["receipt"])])
                self.assertEqual(result, ["receipt"])
                self.assertEqual(self.connection.call.call_args_list,
                                 [call("cairn." + function, "transaction", "proof", 7)] * 2)
                self.assertEqual(self.connection.close.call_count, 2)
                self.sleep.assert_called_once_with(.01)

    def test_conflict_retries_are_bounded(self):
        error = tarantool.DatabaseError(97, "conflict")
        with self.assertRaises(tarantool.DatabaseError) as raised:
            self.invoke("tx_apply", [error] * 5)
        self.assertIs(raised.exception, error)
        self.assertEqual(self.connection.call.call_count, 4)
        self.assertEqual(self.connection.close.call_count, 4)
        self.assertEqual(self.sleep.call_count, 3)

    def test_non_resolution_conflicts_are_never_retried(self):
        for function in ("tx_mutate", "tx_start", "tx_start_voted", "tx_start_finished",
                         "tx_begin", "tx_enlist", "tx_prepare", "tx_decide", "tx_read"):
            with self.subTest(function=function):
                error = tarantool.DatabaseError(97, "conflict")
                with self.assertRaises(tarantool.DatabaseError) as raised:
                    self.invoke(function, [error, SimpleNamespace(data=["unexpected retry"])])
                self.assertIs(raised.exception, error)
                self.assertEqual(self.connection.call.call_count, 1)
                self.sleep.assert_not_called()

    def test_unknown_database_and_transport_errors_are_never_retried(self):
        errors = [tarantool.DatabaseError(1, "unknown database failure"),
                  tarantool.NetworkError("response lost"), TimeoutError("response deadline"),
                  OSError("connection reset")]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                with self.assertRaises(type(error)) as raised:
                    self.invoke("tx_apply", [error, SimpleNamespace(data=["unexpected retry"])])
                self.assertIs(raised.exception, error)
                self.assertEqual(self.connection.call.call_count, 1)
                self.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
