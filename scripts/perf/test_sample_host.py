import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("sample_host", Path(__file__).with_name("sample-host.py"))
sampler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sampler)


class SamplerTests(unittest.TestCase):
    def test_process_signal_emits_after_snapshot_before_exit(self):
        process = subprocess.Popen([sys.executable, str(Path(__file__).with_name("sample-host.py")), "30", "basic"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            first = json.loads(process.stdout.readline())
            before = json.loads(process.stdout.readline())
            self.assertEqual(first["kind"], "host")
            self.assertEqual(before["phase"], "before")
            process.send_signal(signal.SIGUSR1)
            output, errors = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, errors)
            self.assertEqual([row["phase"] for line in output.splitlines()
                              if (row := json.loads(line))["kind"] == "network_snapshot"], ["after"])
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate()

    def test_early_requested_stop_keeps_both_network_snapshots(self):
        output = io.StringIO()
        sampler.stop_requested = False
        network = {"qdisc": {"status": 0, "stdout": "[]"}}
        with contextlib.redirect_stdout(output), \
                mock.patch.object(sampler, "network_counters", return_value=network), \
                mock.patch.object(sampler, "process_counters", return_value=[]), \
                mock.patch.object(sampler, "read", return_value=""), \
                mock.patch.object(sampler, "command", return_value={"status": 0, "stdout": ""}), \
                mock.patch.object(sampler.time, "sleep", side_effect=lambda _: sampler.request_stop(None, None)):
            sampler.sample(96, False)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([row["phase"] for row in rows if row["kind"] == "network_snapshot"], ["before", "after"])
        self.assertEqual([row["index"] for row in rows if row["kind"] == "sample"], [0])
        self.assertNotIn("qdisc", next(row for row in rows if row["kind"] == "sample"))

    def test_natural_completion_keeps_both_network_snapshots(self):
        output = io.StringIO()
        sampler.stop_requested = False
        with contextlib.redirect_stdout(output), \
                mock.patch.object(sampler, "network_counters", return_value={}), \
                mock.patch.object(sampler, "process_counters", return_value=[]), \
                mock.patch.object(sampler, "read", return_value=""), \
                mock.patch.object(sampler, "command", return_value={}), \
                mock.patch.object(sampler.time, "sleep"):
            sampler.sample(0, False)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([row["phase"] for row in rows if row["kind"] == "network_snapshot"], ["before", "after"])


if __name__ == "__main__":
    unittest.main()
