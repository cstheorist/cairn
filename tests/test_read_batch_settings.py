"""Local-only benchmark configuration/evidence checks; no database or Docker."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


runner = module("run_comparison")
reporter = module("report_comparison")


def metadata_run(actual):
    def run(arguments, capture=False, env=None):
        if "inspect" in arguments:
            settings = [] if actual is None else ["CAIRN_READ_BATCH_US=" + actual]
            return json.dumps(settings)
        node = next(node for node in runner.FAST[1:] if node in arguments)
        return json.dumps([int(node[1]), 2, "node-" + node, node.endswith("a"), True, 2, 1, 1])
    return run


def fixture(directory, actual=None, requested=None):
    evidence = {"databases": {}}
    for database in reporter.DATABASES:
        result = {"status": "complete", "database": database, "rows": 4, "payload_bytes": 128,
                  "seconds_per_run": 10, "warmup_seconds": 2, "protocol": "prepared",
                  "transaction_isolation": "test", "transfer_partitions": {"all_transfers_cross_partitions": True},
                  "results": [{"workload": workload, "repeat": repeat, "clients": 16,
                               "operations_per_second": 100 + repeat, "latency_us": {"p99": 100},
                               "serialization_retries": 0,
                               "correctness": {"all_key_values_verified": True, "verified": True}}
                              for workload in reporter.WORKLOADS for repeat in range(3)]}
        path = directory / (database + "-benchmark.json")
        path.write_text(json.dumps(result))
        container = {"service": "database", "cpu_limit": 6, "memory_limit_bytes": 6 * 1024**3,
                     "state": "running", "oom_killed": False, "restarts": 0, "started_at": "fixed"}
        environment = {"status": "accepted", "result_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                       "load_image": "same-load-image", "transaction_isolation": "test",
                       "containers": [container], "containers_after": [container],
                       "cpu_layout": "standard", "gateway_settings": {}}
        if database == "cairn":
            if actual is not None:
                environment["gateway_settings"]["CAIRN_READ_BATCH_US"] = str(actual)
            if requested is not None:
                environment["requested_read_batch_us"] = requested
        evidence["databases"][database] = environment
    (directory / "comparison-environment.json").write_text(json.dumps(evidence))


class ReadBatchSettings(unittest.TestCase):
    def test_cli_values_reach_gateway_environment_before_any_external_call(self):
        for micros in (0, 50, 1000):
            with self.subTest(micros=micros), tempfile.TemporaryDirectory() as temporary:
                def stop(arguments, capture=False, env=None):
                    self.assertEqual(os.environ["CAIRN_READ_BATCH_US"], str(micros))
                    raise RuntimeError("stopped before Docker")
                with mock.patch.dict(os.environ), mock.patch.object(runner, "run", side_effect=stop), \
                     mock.patch.object(runner, "ARTIFACTS", Path(temporary)), \
                     mock.patch.object(sys, "argv", ["run_comparison.py", "--no-build", "--cairn-read-batch-us", str(micros)]):
                    with self.assertRaisesRegex(RuntimeError, "stopped before Docker"):
                        runner.main()

    def test_cli_rejects_invalid_values_and_keeps_help_compact(self):
        def cli(arguments):
            stdout, stderr = io.StringIO(), io.StringIO()
            with tempfile.TemporaryDirectory() as temporary, mock.patch.dict(os.environ), \
                 mock.patch.object(runner, "run", side_effect=AssertionError("unexpected external command")) as external, \
                 mock.patch.object(runner, "ARTIFACTS", Path(temporary)), \
                 mock.patch.object(sys, "argv", ["run_comparison.py", *arguments]), \
                 contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as exit_result:
                    runner.main()
                external.assert_not_called()
            return exit_result.exception.code, stdout.getvalue(), stderr.getvalue()

        for invalid in ("-1", "1001", "bad", "1.5"):
            code, _, stderr = cli(["--cairn-read-batch-us", invalid])
            self.assertEqual(code, 2)
            self.assertIn("0 to 1000", stderr)
        code, stdout, _ = cli(["--help"])
        self.assertEqual(code, 0)
        self.assertIn("--cairn-read-batch-us US", stdout)
        self.assertNotIn("0,1,2,3", stdout)
        self.assertLess(len(stdout), 5000)

    def test_evidence_records_requested_and_actual_values_and_rejects_mismatch(self):
        for micros in (0, 50, 1000):
            with self.subTest(micros=micros), mock.patch.object(runner, "run", side_effect=metadata_run(str(micros))):
                evidence = runner.fast_metadata("standard", micros)
                self.assertEqual(evidence["requested_read_batch_us"], micros)
                self.assertEqual(evidence["gateway_settings"]["CAIRN_READ_BATCH_US"], str(micros))
        for actual in (None, "50"):
            with self.subTest(actual=actual), mock.patch.object(runner, "run", side_effect=metadata_run(actual)):
                with self.assertRaisesRegex(AssertionError, "requested 0"):
                    runner.fast_metadata("standard", 0)

    def test_legacy_evidence_and_summaries_do_not_gain_an_invented_default(self):
        with mock.patch.object(runner, "run", side_effect=metadata_run(None)):
            evidence = runner.fast_metadata()
            self.assertNotIn("requested_read_batch_us", evidence)
            self.assertNotIn("CAIRN_READ_BATCH_US", evidence["gateway_settings"])
        for actual in (None, 0, 50):
            with self.subTest(actual=actual), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                fixture(directory, actual, actual)
                with mock.patch.object(sys, "argv", ["report_comparison.py", "--directory", str(directory)]), \
                     contextlib.redirect_stdout(io.StringIO()):
                    reporter.main()
                summary = json.loads((directory / "comparison-summary.json").read_text())
                markdown = (directory / "comparison-summary.md").read_text()
                if actual is None:
                    self.assertNotIn("read_batch_us", summary["cairn_settings"])
                    self.assertNotIn("CAIRN_READ_BATCH_US", markdown)
                else:
                    self.assertEqual(summary["cairn_settings"]["read_batch_us"], str(actual))
                    self.assertIn("CAIRN_READ_BATCH_US=" + str(actual), markdown)

    def test_report_rejects_missing_or_mismatched_actual_evidence(self):
        for actual in (None, 50):
            with self.subTest(actual=actual), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                fixture(directory, actual, 0)
                with self.assertRaisesRegex(ValueError, "requested value"):
                    reporter.summarize(directory)


if __name__ == "__main__":
    unittest.main(verbosity=2)
