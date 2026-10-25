"""Offline ownership/no-replay checks; no Docker or profiler operations occur."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("profile_storage", Path(__file__).resolve().parents[1] / "scripts" / "profile_storage.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


class PublishedPaths(unittest.TestCase):
    def test_lease_manifests_are_under_ignored_repository_tools(self):
        repository = Path(__file__).resolve().parents[1]
        self.assertEqual(helper.ROOT, repository)
        self.assertEqual(helper.TOOL_ROOT, repository / ".tools" / "profile-storage")


class ScopedProfiler(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for name, value in (("ROOT", self.root), ("TOOL_ROOT", self.root / "tools")):
            patcher = patch.object(helper, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.info = {"id": "original-container", "name": "/cairn-ha-s0a-1", "running": True,
                     "project": "cairn-ha", "service": "s0a"}
        self.active = {"lease": "test-01", "active": True, "path": "/var/lib/tarantool/cairn-cpu-test-01.txt"}
        self.stopped = dict(self.active, active=False, stop_ok=True)

    def save_lease(self):
        path = helper.manifest_path("s0a", "test-01")
        path.parent.mkdir(parents=True)
        data = {"node": "s0a", "lease": "test-01", "token": "owned-token",
                "container_id": self.info["id"], "remote_path": self.active["path"]}
        helper.write_manifest(path, data)
        return path

    def test_container_guard_rejects_foreign_or_stopped_containers(self):
        for field, bad in (("name", "/foreign"), ("project", "foreign"),
                           ("service", "gateway"), ("running", False)):
            with self.subTest(field=field):
                with patch.object(helper, "command", return_value=json.dumps(dict(self.info, **{field: bad}))):
                    with self.assertRaises(RuntimeError):
                        helper.container("s0a")
        with patch.object(helper, "command") as command:
            with self.assertRaises(ValueError):
                helper.container("gateway")
            command.assert_not_called()

    def test_manifest_names_cannot_escape_private_directory(self):
        for node, lease in (("../scripts", "test-01"), ("s0a", "../test"),
                            ("s0a", "A"), ("s0a", "a" * 65)):
            with self.subTest(node=node, lease=lease):
                with self.assertRaises(ValueError):
                    helper.manifest_path(node, lease)

    def test_manifest_is_private_and_exclusive_creation_retains_owner(self):
        path = helper.manifest_path("s0a", "test-01")
        path.parent.mkdir(parents=True)
        helper.write_manifest(path, {"token": "first"}, exclusive=True)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            helper.write_manifest(path, {"token": "second"}, exclusive=True)
        self.assertEqual(json.loads(path.read_text()), {"token": "first"})

    def test_simultaneous_starts_issue_only_one_eval(self):
        ready = threading.Barrier(2)

        def identity(node):
            ready.wait(timeout=3)
            return self.info

        def attempt():
            try:
                helper.start("s0a", "test-01")
                return "started"
            except RuntimeError:
                return "refused"

        with patch.object(helper, "container", side_effect=identity), \
                patch.object(helper, "evaluate", return_value=self.active) as evaluate:
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(lambda _: attempt(), range(2)))
        self.assertCountEqual(outcomes, ["started", "refused"])
        evaluate.assert_called_once()
        saved = json.loads(helper.manifest_path("s0a", "test-01").read_text())
        self.assertIn(saved["token"], evaluate.call_args.args[1])

    def test_operator_eval_is_bounded_local_and_never_retried(self):
        error = subprocess.TimeoutExpired("docker exec", 15)
        with patch.object(helper, "container", return_value=self.info), \
                patch.object(helper, "command", side_effect=error) as command:
            with self.assertRaises(subprocess.TimeoutExpired):
                helper.evaluate("s0a", "return true")
            command.assert_called_once()
            arguments = command.call_args.args[0]
            self.assertEqual(arguments[:4], ["docker", "exec", "-i", self.info["id"]])
            client = command.call_args.kwargs["input_text"]
            self.assertIn("operator:local-operator@127.0.0.1:3301", client)
            self.assertIn("wait_connected=3, connect_timeout=3, fetch_schema=false", client)
            self.assertIn("{timeout=5}", client)

    def test_subprocess_uses_hard_timeout_without_retry(self):
        with patch.object(helper.subprocess, "run",
                          side_effect=subprocess.TimeoutExpired("docker", 15)) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                helper.command(["docker", "inspect"])
        run.assert_called_once()
        self.assertEqual(run.call_args.kwargs["timeout"], 15)

    def test_collection_race_preserves_existing_output(self):
        self.save_lease()
        destination = self.root / "artifacts/storage-profiles/test-01/s0a.jit.txt"
        real_open = os.open

        def raced_open(path, flags, mode=0o777):
            if Path(path) == destination:
                destination.write_text("other collector output\n")
                raise FileExistsError(str(path))
            return real_open(path, flags, mode)

        with patch.object(helper, "owned_status", return_value=self.stopped), \
                patch.object(helper, "container", return_value=self.info), \
                patch.object(helper.os, "open", side_effect=raced_open), \
                patch.object(helper, "command") as command:
            with self.assertRaises(RuntimeError):
                helper.collect("s0a", "test-01")
            command.assert_not_called()
        self.assertEqual(destination.read_text(), "other collector output\n")

    def test_collection_preserves_existing_provenance_without_text(self):
        self.save_lease()
        metadata = self.root / "artifacts/storage-profiles/test-01/s0a.jit.json"
        metadata.parent.mkdir(parents=True)
        metadata.write_text("previous provenance\n")
        with patch.object(helper, "owned_status", return_value=self.stopped), \
                patch.object(helper, "container", return_value=self.info), \
                patch.object(helper, "command") as command:
            with self.assertRaises(RuntimeError):
                helper.collect("s0a", "test-01")
            command.assert_not_called()
        self.assertEqual(metadata.read_text(), "previous provenance\n")

    def test_eval_rejects_container_replacement_before_command(self):
        with patch.object(helper, "container", return_value=self.info), \
                patch.object(helper, "command") as command:
            with self.assertRaises(RuntimeError):
                helper.evaluate("s0a", "return true", expected_container_id="old-container")
            command.assert_not_called()

    def test_probe_does_not_start_or_stop_a_profiler(self):
        with patch.object(helper, "evaluate", return_value={}) as evaluate:
            helper.probe("s0a")
        source = evaluate.call_args.args[1]
        self.assertNotIn(".start(", source)
        self.assertNotIn(".stop(", source)
        self.assertNotIn("os.exit", source)

    def test_start_persists_ownership_before_eval_and_has_server_lease(self):
        def evaluate(node, source, *, expected_container_id):
            data = json.loads(helper.manifest_path(node, "test-01").read_text())
            self.assertEqual(data["state"], "starting")
            self.assertEqual(data["container_id"], self.info["id"])
            self.assertEqual(expected_container_id, self.info["id"])
            self.assertIn(data["token"], source)
            self.assertIn("require('jit.p')", source)
            self.assertIn('"vf3i10m0"', source)
            self.assertIn("fiber.sleep(20)", source)
            self.assertIn("rawget(_G, key) == lease and lease.active", source)
            self.assertLess(source.index("if not ok then lease.error"),
                            source.index("lease.active, lease.stopped = false"))
            self.assertNotIn("os.exit", source)
            return self.active
        with patch.object(helper, "container", return_value=self.info), \
                patch.object(helper, "evaluate", side_effect=evaluate):
            self.assertEqual(helper.start("s0a", "test-01"), self.active)

    def test_uncertain_start_is_recorded_and_cannot_be_replayed(self):
        with patch.object(helper, "container", return_value=self.info), \
                patch.object(helper, "evaluate", side_effect=subprocess.TimeoutExpired("EVAL", 5)) as evaluate:
            with self.assertRaises(subprocess.TimeoutExpired):
                helper.start("s0a", "test-01")
            data = json.loads(helper.manifest_path("s0a", "test-01").read_text())
            self.assertEqual(data["state"], "start_failed_or_uncertain")
            with self.assertRaises(RuntimeError):
                helper.start("s0a", "test-01")
            evaluate.assert_called_once()

    def test_stop_checks_token_and_container_incarnation(self):
        self.save_lease()
        with patch.object(helper, "container", return_value=self.info), \
                patch.object(helper, "evaluate", return_value=self.stopped) as evaluate:
            helper.owned_status("s0a", "test-01", stop=True)
            source = evaluate.call_args.args[1]
            self.assertIn('lease.id == "test-01" and lease.token == "owned-token"', source)
            self.assertIn("lease.stop()", source)
        with patch.object(helper, "container", return_value=dict(self.info, id="replacement-container")), \
                patch.object(helper, "evaluate") as evaluate:
            with self.assertRaises(RuntimeError):
                helper.owned_status("s0a", "test-01", stop=True)
            evaluate.assert_not_called()

    def test_collection_requires_successful_stop_and_retains_provenance(self):
        self.save_lease()
        for state in (self.active, dict(self.stopped, stop_ok=False)):
            with patch.object(helper, "owned_status", return_value=state), \
                    patch.object(helper, "command") as command:
                with self.assertRaises(RuntimeError):
                    helper.collect("s0a", "test-01")
                command.assert_not_called()
        def copy(arguments):
            self.assertEqual(arguments[:3], ["docker", "cp", self.info["id"] + ":" + self.active["path"]])
            Path(arguments[3]).write_text("profile samples\n")
        with patch.object(helper, "owned_status", return_value=self.stopped), \
                patch.object(helper, "container", return_value=self.info), \
                patch.object(helper, "command", side_effect=copy) as command:
            evidence = helper.collect("s0a", "test-01")
            self.assertEqual(evidence["sha256"], hashlib.sha256(b"profile samples\n").hexdigest())
            self.assertNotIn("token", evidence)
            with self.assertRaises(RuntimeError):
                helper.collect("s0a", "test-01")
            command.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
