import argparse
import contextlib
import hashlib
import importlib.util
import io
import itertools
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("calibrate", Path(__file__).with_name("calibrate.py"))
calibrate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibrate)
baseline_spec = importlib.util.spec_from_file_location("baseline", Path(__file__).with_name("import-baseline.py"))
baseline = importlib.util.module_from_spec(baseline_spec)
baseline_spec.loader.exec_module(baseline)


def network_fixture():
    return [{"kind": "network_snapshot", "phase": phase,
             "started_unix_ns": index * 10, "finished_unix_ns": index * 10 + 1,
             "qdisc": {"status": 0, "stdout": "[]"}, "links": {"status": 0, "stdout": "[]"},
             "sockets": {"status": 0, "stdout": ""},
             "classes": {"eth0": {"status": 0, "stdout": "[]"}}}
            for index, phase in enumerate(("before", "after"))]


class NetworkSnapshotTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "host.jsonl"

    def write(self, rows):
        self.path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")

    def test_metadata_alone_does_not_allow_load(self):
        self.write([{"kind": "host"}])
        with mock.patch.object(calibrate.time, "monotonic", side_effect=[0, 21]):
            with self.assertRaisesRegex(RuntimeError, "snapshot"):
                calibrate.wait_sampler_ready(mock.Mock(poll=lambda: None), self.path)

    def test_before_snapshot_allows_load_but_does_not_finish_evidence(self):
        self.write(network_fixture()[:1])
        calibrate.wait_sampler_ready(mock.Mock(poll=lambda: None), self.path)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            calibrate.verify_network_snapshots(self.path)

    def test_complete_snapshots_validate_commands_and_order(self):
        rows = network_fixture()
        self.write(rows)
        self.assertEqual(calibrate.verify_network_snapshots(self.path)["after_started_unix_ns"], 10)
        with self.assertRaisesRegex(ValueError, "bracket"):
            calibrate.verify_network_snapshots(self.path, 2, 11)
        rows[1]["qdisc"]["status"] = 1
        self.write(rows)
        with self.assertRaisesRegex(ValueError, "command failed"):
            calibrate.verify_network_snapshots(self.path)
        rows = network_fixture()
        rows[1]["started_unix_ns"] = 0
        self.write(rows)
        with self.assertRaisesRegex(ValueError, "out of order"):
            calibrate.verify_network_snapshots(self.path)


def receiver_summary(**updates):
    summary = {"bytes": 100, "seconds": 2, "bits_per_second": 400, "sender": False}
    summary.update(updates)
    return {"end": {"sum_received": summary}}


class AccountingTests(unittest.TestCase):
    def test_receiver_only(self):
        result = {"end": {"sum_sent": {"bytes": 999999}, "sum_received": {
            "bytes": 100, "seconds": 2, "bits_per_second": 400, "sender": False}}}
        self.assertEqual(calibrate.received(result)["bytes"], 100)
        result["end"].pop("sum_received")
        with self.assertRaises(ValueError):
            calibrate.received(result)

    def test_legacy_udp_receiver(self):
        result = {"end": {"sum": {"bytes": 100, "seconds": 2,
                  "bits_per_second": 400, "sender": False, "lost_packets": 7}}}
        self.assertEqual(calibrate.received(result)["lost_packets"], 7)
        result["end"]["sum"]["sender"] = True
        with self.assertRaises(ValueError):
            calibrate.received(result)

    def test_error_cannot_count_as_measurement(self):
        with self.assertRaises(ValueError):
            calibrate.received({"error": "unable to connect"})

    def test_sender_flag_rejected_even_with_receiver_key(self):
        with self.assertRaises(ValueError):
            calibrate.received(receiver_summary(sender=True))

    def test_invalid_receiver_numbers_are_rejected(self):
        for field, value in (("bytes", -1), ("bytes", float("inf")),
                             ("seconds", 0), ("seconds", -1),
                             ("seconds", float("nan")),
                             ("bits_per_second", -1),
                             ("bits_per_second", float("nan")),
                             ("bits_per_second", float("inf"))):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    calibrate.received(receiver_summary(**{field: value}))

    def test_upload_uses_actual_server_receiver_report(self):
        result = receiver_summary(bytes=9999, sender=True)
        result["server_output_json"] = receiver_summary(bytes=1234)
        selected = calibrate.receiver_result(result, "upload")
        self.assertEqual(calibrate.received(selected)["bytes"], 1234)

    def test_download_uses_actual_client_receiver_report(self):
        result = receiver_summary(bytes=1234)
        result["server_output_json"] = receiver_summary(bytes=9999, sender=True)
        selected = calibrate.receiver_result(result, "download")
        self.assertEqual(calibrate.received(selected)["bytes"], 1234)

    def test_upload_missing_server_report_cannot_fall_back(self):
        with self.assertRaises(ValueError):
            calibrate.receiver_result(receiver_summary(bytes=9999), "upload")


class TopologyTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(Path(__file__).with_name("topology.example.json").read_text())
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "topology.json"

    def parse(self):
        self.path.write_text(json.dumps(self.config))
        return calibrate.topology(self.path)

    def test_valid_topology_keeps_all_five_paths(self):
        self.assertEqual(self.parse()["server_addresses"], self.config["server_addresses"])

    def test_aliases_cannot_inject_ssh_options_or_shell_commands(self):
        for alias in ("-oProxyCommand=touch /tmp/unsafe", "server;false", "$(false)",
                      "server\nother", "root@server"):
            with self.subTest(alias=alias):
                self.config["client"] = alias
                with self.assertRaises(ValueError):
                    self.parse()

    def test_repeated_host_and_path_are_rejected(self):
        self.config["routers"][0] = self.config["server"]
        with self.assertRaises(ValueError):
            self.parse()
        self.config["routers"][0] = "mpudp-wan1"
        self.config["server_addresses"][1] = self.config["server_addresses"][0]
        with self.assertRaises(ValueError):
            self.parse()

    def test_non_ipv4_path_and_unknown_fields_are_rejected(self):
        for address in ("::1", "10.206.1.2;false", "server.example"):
            with self.subTest(address=address):
                self.config["server_addresses"][0] = address
                with self.assertRaises(ValueError):
                    self.parse()
        self.config["server_addresses"][0] = "10.206.1.2"
        self.config["psk"] = "not-a-real-key"
        with self.assertRaises(ValueError):
            self.parse()

    def test_reference_capacity_cannot_be_lowered_by_configuration(self):
        self.config["rate_mbit_per_path"] = 10
        with self.assertRaises(ValueError):
            self.parse()


class HeadroomTests(unittest.TestCase):
    def samples(self, samples):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "host.jsonl"
        rows = [{"kind": "host"}]
        for timestamp, user, idle, swap in samples:
            rows.append({"kind": "sample", "unix_ns": timestamp,
                         "stat": f"cpu {user} 0 0 {idle} 0 0 0 0 0 0\n",
                         "vmstat": f"pswpin {swap}\npswpout 0\n"})
        path.write_text("\n".join(map(json.dumps, rows)))
        return path

    def test_only_complete_steady_intervals_count(self):
        path = self.samples([(0, 0, 0, 0), (10, 10, 90, 100),
                             (20, 100, 100, 102), (30, 110, 190, 200)])
        result = calibrate.host_headroom(path, 10, 20)
        self.assertEqual(result["mean_idle_percent"], 10)
        self.assertEqual(result["swap_pages"], 2)
        self.assertEqual(result["sample_intervals"], 1)

    def test_irregular_intervals_are_weighted_by_cpu_ticks(self):
        path = self.samples([(0, 0, 0, 0), (10, 10, 90, 0), (110, 910, 190, 0)])
        result = calibrate.host_headroom(path)
        self.assertAlmostEqual(result["mean_idle_percent"], 190 / 1100 * 100)
        self.assertEqual(result["min_idle_percent"], 10)

    def test_missing_steady_samples_do_not_imply_idle_headroom(self):
        path = self.samples([(0, 0, 0, 0), (10, 10, 90, 0)])
        result = calibrate.host_headroom(path, 3, 9)
        self.assertIsNone(result["mean_idle_percent"])
        self.assertEqual(result["sample_intervals"], 0)


class FakeProcess:
    def __init__(self, args, *, dead=False, stubborn=False, **kwargs):
        self.args = args
        self.stdin = io.BytesIO() if "stdin" in kwargs else None
        self.returncode = 1 if dead else None
        self.stubborn = stubborn
        self.terminated = self.killed = False
        self.waits = 0

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        if not self.stubborn:
            self.returncode = -15

    def kill(self):
        self.killed = True
        self.returncode = -9

    def wait(self, timeout=None):
        self.waits += 1
        if self.stubborn and self.terminated and not self.killed:
            raise subprocess.TimeoutExpired(self.args, timeout)
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name) / "case"
        self.config = json.loads(Path(__file__).with_name("topology.example.json").read_text())
        args = argparse.Namespace(seconds=1, warmup=0, rounds=1,
                                  diagnostics="basic", ssh_config=Path("unused-ssh-config"))
        self.lab = calibrate.Lab(args, self.config)
        self.processes, self.handles, self.servers, self.clients, self.stops = [], [], [], [], []
        self.preoccupied = self.dead_server = self.bad_endpoint = False
        self.timeout_client = self.fail_cleanup = False
        self.stubborn = False
        self.ss_calls = 0
        patches = contextlib.ExitStack()
        self.addCleanup(patches.close)
        patches.enter_context(mock.patch.object(calibrate.subprocess, "Popen", side_effect=self.popen))
        patches.enter_context(mock.patch.object(calibrate, "run", side_effect=self.remote_run))
        patches.enter_context(mock.patch.object(calibrate.time, "sleep"))
        patches.enter_context(mock.patch.object(calibrate.time, "monotonic", side_effect=itertools.count(0, 3)))
        patches.enter_context(mock.patch.object(calibrate, "host_headroom", return_value={"sample_intervals": 1}))
        patches.enter_context(mock.patch("builtins.print"))

    def popen(self, args, **kwargs):
        command = shlex.split(args[-1])
        server = "iperf3" in command and "-s" in command
        if "sampler" in command[command.index("--unit") + 1]:
            snapshots = network_fixture()
            snapshots[1].update(started_unix_ns=2**63 - 2, finished_unix_ns=2**63 - 1)
            for row in snapshots:
                kwargs["stdout"].write(json.dumps(row) + "\n")
            kwargs["stdout"].flush()
        process = FakeProcess(args, dead=server and self.dead_server,
                              stubborn=self.stubborn, **kwargs)
        self.processes.append(process)
        self.handles.extend(kwargs[key] for key in ("stdout", "stderr") if key in kwargs)
        if server:
            self.servers.append(command)
        return process

    def remote_run(self, args, timeout=30):
        command = shlex.split(args[-1])
        if command[:1] == ["ss"]:
            self.ss_calls += 1
            if not self.preoccupied and not self.servers:
                return ""
            suffix = "1" if self.bad_endpoint else ""
            return "\n".join(f"LISTEN 0 4096 {address}:{15201 + i}{suffix} 0.0.0.0:*"
                             for i, address in enumerate(self.config["server_addresses"]))
        if "iperf3" in command and "-c" in command:
            self.clients.append(command)
            if self.timeout_client:
                raise subprocess.TimeoutExpired(args, timeout)
            self.assertEqual(len(self.servers), 5)
            result = receiver_summary(bytes=11_500_000, seconds=1, bits_per_second=92_000_000)
            return json.dumps({**result, "server_output_json": result})
        if command[:2] == ["python3", "-c"]:
            self.stops.append((args[-2], command[-1]))
            return json.dumps({"state": "active" if self.fail_cleanup else "inactive",
                               "stopped": not self.fail_cleanup})
        self.fail(f"Unexpected remote command: {command}")

    def case(self):
        return self.lab.case(self.output, "tcp", 5, "upload", 1)

    def assert_reaped(self):
        self.assertTrue(all(process.poll() is not None and process.waits for process in self.processes))
        self.assertTrue(all(handle.closed for handle in self.handles))

    def test_existing_listener_rejected_before_new_processes_or_clients(self):
        self.preoccupied = True
        with self.assertRaisesRegex(RuntimeError, "occupied"):
            self.case()
        self.assertEqual(self.processes, [])
        self.assertEqual(self.clients, [])

    def test_dead_owned_listener_cannot_be_replaced_by_stale_socket(self):
        self.dead_server = True
        with self.assertRaisesRegex(RuntimeError, "listener"):
            self.case()
        self.assertEqual(self.clients, [])
        self.assertEqual(len(self.stops), 13)
        self.assert_reaped()

    def test_similar_endpoint_text_does_not_satisfy_readiness(self):
        self.bad_endpoint = True
        with self.assertRaisesRegex(RuntimeError, "ready"):
            self.case()
        self.assertEqual(self.clients, [])
        self.assertLess(self.ss_calls, 10)
        self.assert_reaped()

    def test_five_distinct_servers_precede_all_clients(self):
        summary = self.case()
        endpoints = {(command[command.index("-B") + 1], command[command.index("-p") + 1])
                     for command in self.servers}
        self.assertEqual(len(endpoints), 5)
        self.assertEqual(len(self.clients), 5)
        self.assertEqual(summary["aggregate_receiver_mbit_s"], 460)
        self.assertEqual(len(set(self.stops)), 18)
        self.assertFalse(summary["product_acceptance"])
        self.assert_reaped()

    def test_client_timeout_stops_owned_units_and_reaps_stubborn_processes(self):
        self.timeout_client = self.stubborn = True
        with self.assertRaises(subprocess.TimeoutExpired):
            self.case()
        self.assertGreaterEqual(len(self.stops), 14)
        self.assertTrue(all(process.killed for process in self.processes))
        self.assertFalse((self.output / "summary.json").exists())
        self.assert_reaped()

    def test_unverified_remote_cleanup_fails_case(self):
        self.fail_cleanup = True
        with self.assertRaisesRegex(RuntimeError, "cleanup"):
            self.case()
        cleanup = json.loads((self.output / "cleanup.json").read_text())
        self.assertTrue(all(not entry["stopped"] for entry in cleanup))
        self.assert_reaped()


class BaselineExportTests(unittest.TestCase):
    def original(self):
        return {"seconds": 60, "warmup_seconds": 5, "verified_bytes": 100,
                "corrupt_bytes": 0, "echo_sent": 1, "echo_ok": 1, "adapter_write_drops": 0,
                "mbps": 0.1, "echo_success": 1, "echo_p50_ms": 40, "echo_p95_ms": 40,
                "echo_p99_ms": 40, "max_echo_success_gap_s": 0.2, "ack_no_delay": False,
                "kcp_fec": [0, 0], "errors": [],
                "samples": [{"second": -4, "bytes": 100, "mbps": 0.1}],
                "echo": [{"seq": 0, "scheduled_s": 0, "latency_ms": 40, "ok": True}],
                "kcp_snmp": {key: 0 for key in baseline.KCP_COUNTERS}}

    def test_arbitrary_and_nested_text_is_never_exported(self):
        original = self.original()
        marker = "private-sentinel-do-not-copy"
        original.update(psk=marker, started_utc=marker, errors=[marker])
        original["samples"][0]["payload"] = marker
        original["echo"][0]["payload"] = marker
        original["kcp_snmp"]["UnknownFutureField"] = marker
        exported = baseline.extract_result(original)
        self.assertNotIn(marker, json.dumps(exported))
        self.assertEqual(exported["error_count"], 1)
        self.assertEqual(exported["samples"][0]["bytes"], 100)

    def test_text_in_numeric_measurement_is_rejected(self):
        for value in ("private-sentinel", True, float("nan"), float("inf"), -1):
            with self.subTest(value=value):
                original = self.original()
                original["verified_bytes"] = value
                with self.assertRaises(ValueError):
                    baseline.extract_result(original)

    def test_unbounded_arrays_and_invalid_booleans_are_rejected(self):
        original = self.original()
        original["samples"] *= 3601
        with self.assertRaises(ValueError):
            baseline.extract_result(original)
        original = self.original()
        original["echo"][0]["ok"] = "private-sentinel"
        with self.assertRaises(ValueError):
            baseline.extract_result(original)

    def test_digest_does_not_admit_paths_or_arbitrary_strings(self):
        for value in ("/root/.lab/psk", "a" * 63, "g" * 64, "a" * 64 + "\n"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    baseline.digest(value)

    def test_import_reads_exact_commit_blobs(self):
        with mock.patch.object(baseline.subprocess, "run") as command:
            command.return_value.stdout = b"blob"
            self.assertEqual(baseline.original_file(Path("source"), "REPORT.md"), b"blob")
            args = command.call_args.args[0]
            self.assertEqual(args[-2:], ["show", baseline.LAB_COMMIT + ":REPORT.md"])

    def test_export_links_allowlisted_data_to_raw_checksums(self):
        original = self.original()
        module, sums = b"module", b"sums"
        source_hashes = {name: "a" * 64 for name in ("cmd/labprobe/main.go", "lab.dbf.hcl",
                         "hosts.generated.dbf.hcl", "scripts/shape.sh", "scripts/lab.py")}
        source_hashes.update({"go.mod": hashlib.sha256(module).hexdigest(),
                              "go.sum": hashlib.sha256(sums).hexdigest()})
        provenance = {"mpudp_commit": baseline.MPUDP_COMMIT, "binary_sha256": "b" * 64,
                      "debianform_commit": "c" * 40, "source_sha256": source_hashes,
                      "private": "private-sentinel"}
        files = {"go.mod": module, "go.sum": sums, "REPORT.md": b"private-sentinel",
                 "SHA256SUMS": b"manifest"}
        for run_id in baseline.RUNS:
            base = f"artifacts/{run_id}"
            files[f"{base}/result.json"] = json.dumps(original).encode()
            files[f"{base}/provenance.json"] = json.dumps(provenance).encode()
            files[f"{base}/scenario.json"] = b"private-sentinel"
        with mock.patch.object(baseline, "original_file", side_effect=lambda _, path: files[path]), \
                mock.patch.object(baseline, "verify_dependencies"):
            exported = baseline.export(Path("source"))
        self.assertNotIn("private-sentinel", json.dumps(exported))
        self.assertEqual(len(exported["runs"]), 2)
        self.assertEqual(exported["raw_file_sha256"],
                         {path: hashlib.sha256(content).hexdigest() for path, content in files.items()})
        self.assertTrue(all(".lab" not in path for path in exported["raw_file_sha256"]))


if __name__ == "__main__":
    unittest.main()
