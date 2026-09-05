import contextlib
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("probe_runner", Path(__file__).with_name("run-probe.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
SOURCE_SHA = "a" * 40


def arguments(directory, *extra):
    return runner.parse_args(["--topology", str(Path(__file__).with_name("topology.example.json")),
        "--ssh-config", "unused-ssh", "--binary", str(directory / "perfprobe"),
        "--source-sha", SOURCE_SHA, "--output", str(directory / "output"),
        "--protocols", "mpudp", "--paths", "1", "--directions", "upload", "--payloads", "64",
        "--seconds", "2", "--warmup", "1", "--rounds", "1", *extra])


def pair_records(case, worker_id):
    receiver_side = "server" if case["direction"] == "upload" else "client"
    rows, summaries = {}, {}
    expected = {"run_id": worker_id, "protocol": case["protocol"], "direction": case["direction"],
                "flows": case["flows_per_worker"], "seconds": case["seconds"],
                "warmup_seconds": case["warmup_seconds"], "message_bytes": case["message_bytes"]}
    for side in ("client", "server"):
        paths = case["candidate_paths"] if case["protocol"] in runner.MPUDP else 1
        metadata = {"type": "metadata", "side": side, "source_sha": SOURCE_SHA,
                    "path_count": paths, "options": expected.copy()}
        role = "receiver" if side == receiver_side else "sender"
        rows[side] = [metadata]
        size = case["message_bytes"] - 40
        if role == "receiver":
            for i in range(case["warmup_seconds"] + case["seconds"]):
                second = i + 1 - case["warmup_seconds"]
                rows[side].append({"type": "sample", "side": side, "role": role, "second": second,
                                   "steady": second > 0, "verified_bytes": size, "verified_packets": 1})
        summaries[side] = {"type": "summary", **expected, "side": side, "role": role, "path_count": paths,
                           "started_utc": "2026-09-05T00:00:00Z", "verified_bytes": size * case["seconds"],
                           "verified_packets": case["seconds"], "mbps": size * 8 / 1e6}
    for side, opposite in (("client", "server"), ("server", "client")):
        rows[side].extend([copy.deepcopy(summaries[side]), {"type": "remote_summary", "summary": copy.deepcopy(summaries[opposite])}])
    return rows


class MatrixTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def test_native_single_flow_and_parallel_capacity_are_distinct(self):
        args = arguments(self.directory, "--protocols", "kcp", "--paths", "5")
        cases = runner.matrix(args)
        self.assertEqual([(c["layout"], c["active_paths"], c["total_flows"], c["single_flow"]) for c in cases],
                         [("single", 1, 1, True), ("parallel", 5, 5, False)])
        self.assertTrue(all(c["candidate_paths"] == 5 and not c["product_acceptance"] for c in cases))

    def test_mpudp_one_session_spans_selected_carriers(self):
        args = arguments(self.directory, "--paths", "5")
        case, = runner.matrix(args)
        cfg = runner.mpudp_config(args, {"server_addresses": [f"10.206.{i}.2" for i in range(1, 6)]}, case, "client", "private")
        self.assertEqual(case["active_paths"], 5)
        self.assertTrue(case["single_flow"])
        self.assertEqual(len(cfg["carriers"]), 5)
        self.assertNotIn("listen", cfg)
        self.assertEqual(cfg["transport"]["max_udp_payload"], 1200)
        self.assertEqual(cfg["limits"]["max_pending_fec_blocks"], 8192)
        self.assertEqual(cfg["limits"]["receive_queue_capacity"], 4096)

    def test_max_payload_respects_protocol_and_v1_overhead(self):
        args = arguments(self.directory)
        self.assertEqual(runner.payload_size("max", "mpudp", args), 3 * (1200 - 71))
        self.assertEqual(runner.payload_size("max", "udp", args), 1472)
        self.assertEqual(runner.payload_size("max", "kcp", args), 65536)
        args.max_datagram_size = 2048
        self.assertEqual(runner.payload_size("max", "mpudp", args), 2048)

    def test_requested_payload_cannot_exceed_budget(self):
        args = arguments(self.directory)
        args.udp_budget = 400
        with self.assertRaises(ValueError):
            runner.payload_size("1400", "mpudp", args)
        args.udp_budget = 1200
        args.physical_mtu = 1280
        with self.assertRaises(ValueError):
            runner.payload_size("1400", "udp", args)

    def test_formal_window_needs_all_required_dimensions(self):
        args = arguments(self.directory, "--seconds", "300", "--warmup", "20", "--rounds", "3")
        self.assertTrue(all(c["formal_window"] for c in runner.matrix(args)))
        args.rounds = 1
        self.assertFalse(runner.matrix(args)[0]["formal_window"])

    def test_port_and_kcp_budget_collisions_fail_before_ssh(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            arguments(self.directory, "--control-port", "29000")
        args = arguments(self.directory, "--protocols", "kcp-mpudp", "--data-shards", "1", "--udp-budget", "1200")
        with self.assertRaises(ValueError):
            runner.matrix(args)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.args = arguments(self.directory)
        self.case = runner.matrix(self.args)[0]
        self.worker_id = self.case["case_id"] + "-w0"
        self.rows = pair_records(self.case, self.worker_id)

    def verify(self):
        for side in ("client", "server"):
            (self.directory / (side + ".jsonl")).write_text("\n".join(json.dumps(row) for row in self.rows[side]) + "\n")
        return runner.verify_pair(self.directory / "client.jsonl", self.directory / "server.jsonl",
                                  self.case, self.worker_id, SOURCE_SHA)

    def test_upload_uses_matching_remote_receiver_and_both_side_logs(self):
        verified = self.verify()
        self.assertEqual(verified["receiver_side"], "server")
        self.assertEqual(verified["receiver"]["verified_bytes"], 48)

    def test_download_switches_actual_receiver(self):
        self.case["direction"] = "download"
        self.rows = pair_records(self.case, self.worker_id)
        self.assertEqual(self.verify()["receiver_side"], "client")

    def test_mpudp_both_sides_report_negotiated_path_count(self):
        self.case["candidate_paths"] = 5
        self.rows = pair_records(self.case, self.worker_id)
        self.assertEqual(self.verify()["receiver"]["path_count"], 5)
        self.rows["server"][0]["path_count"] = 0
        with self.assertRaisesRegex(ValueError, "path count"):
            self.verify()

    def test_wrong_sha_or_missing_second_cannot_be_measurement(self):
        self.rows["server"][0]["source_sha"] = "b" * 40
        with self.assertRaises(ValueError):
            self.verify()
        self.rows = pair_records(self.case, self.worker_id)
        self.rows["server"].pop(1)
        with self.assertRaises(ValueError):
            self.verify()

    def test_sender_cannot_forge_received_bytes(self):
        self.rows["client"][-1]["summary"]["verified_bytes"] += 9999
        with self.assertRaises(ValueError):
            self.verify()

    def test_headers_or_incorrect_summary_are_rejected(self):
        self.rows["server"][1]["verified_bytes"] = 64
        with self.assertRaises(ValueError):
            self.verify()
        self.rows = pair_records(self.case, self.worker_id)
        self.rows["server"][-2]["mbps"] = float("nan")
        self.rows["client"][-1]["summary"] = copy.deepcopy(self.rows["server"][-2])
        with self.assertRaises(ValueError):
            self.verify()


class PrivacyTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def test_private_profiles_are_not_shareable_checksums(self):
        (self.directory / "summary.json").write_text('{}\n')
        private = self.directory / "case" / ".lab"
        private.mkdir(parents=True)
        (private / "private.pprof").write_text("private-marker")
        runner.checksums(self.directory, "private-marker")
        self.assertNotIn(".lab", (self.directory / "SHA256SUMS").read_text())
        (self.directory / "stderr").write_text("private-marker")
        with self.assertRaises(ValueError):
            runner.checksums(self.directory, "private-marker")

    def test_psk_file_requires_private_mode(self):
        path = self.directory / "secret"
        path.write_text("private-marker\n")
        path.chmod(0o600)
        self.assertEqual(runner.read_secret(path), "private-marker")
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            runner.read_secret(path)

    def test_plan_has_no_remote_side_effect_or_secret(self):
        binary = self.directory / "perfprobe"
        binary.write_bytes(b"fixture-executable")
        argv = ["--topology", str(Path(__file__).with_name("topology.example.json")),
                "--ssh-config", "unused", "--binary", str(binary), "--source-sha", SOURCE_SHA,
                "--output", str(self.directory / "plan"), "--plan"]
        with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(runner.ProbeRunner, "prepare") as prepare, mock.patch.object(runner.calibrate, "run", return_value=SOURCE_SHA):
            runner.main(argv)
        prepare.assert_not_called()
        self.assertTrue((self.directory / "plan" / "matrix.json").is_file())
        plan = json.loads((self.directory / "plan" / "plan-summary.json").read_text())
        self.assertEqual(plan["total_cases"], 696)
        self.assertEqual(plan["minimum_wall_seconds"], 696 * 320)
        self.assertEqual(json.loads((self.directory / "plan" / "manifest.json").read_text())["binary_sha256"], hashlib.sha256(b"fixture-executable").hexdigest())


class FakeProcess:
    def __init__(self, command, stdout, stdin, rows, dead=False):
        self.command = command
        self.stdin = io.BytesIO() if stdin == subprocess.PIPE else None
        self.returncode = 1 if dead else None
        self.terminated = False
        for row in rows:
            stdout.write(json.dumps(row).encode() + b"\n")
        stdout.flush()

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.returncode = self.returncode if self.returncode is not None else 0
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def kill(self):
        self.returncode = -9


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.args = arguments(self.directory)
        self.args.output.mkdir()
        self.args.control_address = "192.0.2.2"
        self.config = json.loads(Path(__file__).with_name("topology.example.json").read_text())
        self.probe = runner.ProbeRunner(self.args, self.config, "b" * 64, "private-marker")
        self.probe.remote_dirs = {self.config[side]: f"/tmp/mpudp-perf-fixture-{side}" for side in ("client", "server")}
        self.case = runner.matrix(self.args)[0]
        self.processes, self.stops, self.config_data, self.commands = [], [], [], []
        self.ss_calls = 0
        self.occupied = self.bad_cleanup = self.dead_client = self.dead_server = False
        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(mock.patch.object(runner.subprocess, "Popen", side_effect=self.popen))
        stack.enter_context(mock.patch.object(self.probe, "remote", side_effect=self.remote))
        stack.enter_context(mock.patch.object(self.probe, "python", side_effect=self.python))
        stack.enter_context(mock.patch.object(self.probe, "stop_unit", side_effect=self.stop))
        stack.enter_context(mock.patch.object(runner.time, "sleep"))
        stack.enter_context(mock.patch.object(runner.calibrate, "host_headroom", return_value={"sample_intervals": 2, "mean_idle_percent": 50}))
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))

    def remote(self, host, command, data=None, timeout=30):
        self.ss_calls += 1
        if self.occupied or self.ss_calls > 2:
            return f"LISTEN 0 10 192.0.2.2:{self.args.control_port} 0.0.0.0:*\n".encode()
        return b""

    def python(self, host, script, *args, data=None, timeout=30):
        if data is not None:
            self.config_data.append((args[0], json.loads(data)))
        return b""

    def stop(self, host, unit):
        self.stops.append((host, unit))
        return {"host": host, "unit": unit, "stopped": not self.bad_cleanup}

    def popen(self, command, *, stdout, stderr, stdin):
        words = shlex.split(command[-1])
        self.commands.append(words)
        label = words[words.index("--unit") + 1]
        if label.endswith("sampler"):
            rows, dead = [{"kind": "host"}], False
        else:
            side = words[words.index("-mode") + 1]
            worker_id = words[words.index("-id") + 1]
            rows = pair_records(self.case, worker_id)[side]
            dead = (self.dead_client and side == "client") or (self.dead_server and side == "server")
        process = FakeProcess(command, stdout, stdin, rows, dead)
        self.processes.append(process)
        return process

    def test_success_preserves_receiver_evidence_and_stops_only_owned_units(self):
        summary = self.probe.case(self.case)
        self.assertEqual(summary["pairs"][0]["receiver_side"], "server")
        self.assertEqual(len(self.processes), 10)
        self.assertEqual(len(self.stops), 10)
        self.assertTrue(all(unit.startswith("mpudp-probe-") for _, unit in self.stops))
        self.assertTrue(all(process.poll() is not None for process in self.processes))
        self.assertEqual(len(self.config_data), 2)
        self.assertTrue(all(cfg["psk"] == "private-marker" for _, cfg in self.config_data))
        self.assertNotIn("private-marker", repr(self.commands))
        self.assertTrue(all("RuntimeMaxSec=108" in cmd and "KillMode=control-group" in cmd for cmd in self.commands))

    def test_occupied_port_stops_no_existing_service(self):
        self.occupied = True
        with self.assertRaises(RuntimeError):
            self.probe.case(self.case)
        self.assertEqual(self.processes, [])
        self.assertEqual(self.stops, [])

    def test_client_failure_still_joins_processes_and_verifies_cleanup(self):
        self.dead_client = True
        with self.assertRaises(RuntimeError):
            self.probe.case(self.case)
        self.assertEqual(len(self.stops), 10)
        self.assertTrue(all(process.poll() is not None for process in self.processes))

    def test_dead_server_cannot_be_replaced_by_stale_listening_port(self):
        self.dead_server = True
        with self.assertRaisesRegex(RuntimeError, "exited during startup"):
            self.probe.case(self.case)
        self.assertEqual(len(self.stops), 9)
        self.assertTrue(all(process.poll() is not None for process in self.processes))

    def test_failed_cleanup_cannot_produce_success_summary(self):
        self.bad_cleanup = True
        with self.assertRaises(RuntimeError):
            self.probe.case(self.case)
        self.assertFalse(self.probe.cleanup_verified)
        self.assertFalse((self.args.output / self.case["case_id"] / "summary.json").exists())


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.args = arguments(self.directory)
        self.args.binary.write_bytes(b"binary")
        self.args.output.mkdir()
        self.args.control_address = "192.0.2.2"
        self.config = json.loads(Path(__file__).with_name("topology.example.json").read_text())
        self.probe = runner.ProbeRunner(self.args, self.config, hashlib.sha256(b"binary").hexdigest(), "private-marker")

    def test_remote_binary_mismatch_retains_owned_directory_for_cleanup(self):
        with mock.patch.object(self.probe.lab, "connect"), mock.patch.object(self.probe, "python", side_effect=[b"/tmp/mpudp-perf-fixture\n", b"wrong-sha\n"]):
            with self.assertRaisesRegex(ValueError, "SHA256"):
                self.probe.prepare()
        self.assertEqual(self.probe.remote_dirs, {self.config["client"]: "/tmp/mpudp-perf-fixture"})
        with mock.patch.object(self.probe, "python", return_value=b"") as command, mock.patch.object(self.probe.lab, "disconnect") as disconnect:
            self.probe.finish()
        command.assert_called_once()
        self.assertEqual(command.call_args.args[-1], "/tmp/mpudp-perf-fixture")
        disconnect.assert_called_once()
        self.assertTrue(json.loads((self.args.output / "workspace-cleanup.json").read_text())[0]["removed"])

    def test_unverified_unit_cleanup_keeps_workspace_and_fails(self):
        self.probe.remote_dirs = {self.config["client"]: "/tmp/mpudp-perf-fixture"}
        self.probe.cleanup_verified = False
        with mock.patch.object(self.probe, "python") as command, mock.patch.object(self.probe.lab, "disconnect") as disconnect:
            with self.assertRaises(RuntimeError):
                self.probe.finish()
        command.assert_not_called()
        disconnect.assert_called_once()
        self.assertFalse(json.loads((self.args.output / "workspace-cleanup.json").read_text())[0]["removed"])

    def test_stop_unit_requires_no_main_process(self):
        unit = "mpudp-probe-fixture-client0"
        with mock.patch.object(self.probe, "python", return_value=b'{"stopped":false,"state":"active","main_pid":"44"}'):
            self.assertFalse(self.probe.stop_unit(self.config["client"], unit)["stopped"])
        with mock.patch.object(self.probe, "python", side_effect=subprocess.TimeoutExpired("ssh", 1)):
            self.assertFalse(self.probe.stop_unit(self.config["client"], unit)["stopped"])


if __name__ == "__main__":
    unittest.main()
