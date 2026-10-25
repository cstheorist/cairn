"""Offline checks that failed fencing probes restore the isolated test node."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import call, patch


source = Path(__file__).resolve().parents[1] / "scripts/check_replication.py"
spec = importlib.util.spec_from_file_location("check_replication", source)
check_replication = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_replication)


class FencingProbe(unittest.TestCase):
    def run_cluster_script(self, failure=None):
        probe_commands = []

        def execute(arguments, **options):
            if "tarantool" in arguments:
                probe_commands.append((arguments, options))
                if failure is not None:
                    raise failure
            return subprocess.CompletedProcess(arguments, 0)

        state = {"initial": [{"node": "s0a"}], "after_failover": {"node": "s0b"},
                 "before_partition": {"node": "s0b"}}
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(check_replication, "ROOT", Path(directory)), \
                    patch.object(check_replication, "state", return_value=state), \
                    patch.object(check_replication, "test") as stages, \
                    patch.object(check_replication.subprocess, "run", side_effect=execute) as commands:
                try:
                    check_replication.main()
                finally:
                    self.probes = probe_commands
                    self.commands = commands
                    self.stages = stages

    def assert_reconnected(self):
        reconnect = ["docker", "network", "connect", "--alias", "s0b",
                     "cairn-ha_default", "cairn-ha-s0b-1"]
        self.assertEqual(self.commands.call_args_list[-1],
                         call(reconnect, cwd=self.commands.call_args.kwargs["cwd"],
                              check=True, timeout=None))
        self.assertEqual(sum(item.args[0] == reconnect for item in self.commands.call_args_list), 1)

    def test_probe_timeout_propagates_and_reconnects(self):
        error = subprocess.TimeoutExpired("fencing probe", 15)
        with self.assertRaises(subprocess.TimeoutExpired) as raised:
            self.run_cluster_script(error)
        self.assertIs(raised.exception, error)
        self.assertEqual(self.probes[0][1]["timeout"], 15)
        self.assert_reconnected()
        self.assertNotIn(call("healed"), self.stages.call_args_list)

    def test_failed_fencing_assertion_propagates_and_reconnects(self):
        error = subprocess.CalledProcessError(1, "fencing probe")
        with self.assertRaises(subprocess.CalledProcessError) as raised:
            self.run_cluster_script(error)
        self.assertIs(raised.exception, error)
        self.assert_reconnected()
        self.assertNotIn(call("healed"), self.stages.call_args_list)

    def test_success_retains_bounded_mandatory_fencing_probe(self):
        self.run_cluster_script()
        self.assertEqual(len(self.probes), 1)
        arguments, options = self.probes[0]
        self.assertTrue(options["check"])
        self.assertEqual(options["timeout"], 15)
        lua = arguments[-1]
        self.assertIn("connect_timeout = 3, wait_connected = 3, fetch_schema = false", lua)
        self.assertIn("assert(c:is_connected()", lua)
        self.assertIn("assert(c:eval('return box.info.ro', {}, {timeout = 3}) == true", lua)
        self.assertLess(lua.index("c:close()"), lua.index("os.exit(ok and 0 or 1)"))
        self.assert_reconnected()
        self.stages.assert_called_with("healed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
