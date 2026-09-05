import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from test_probe_runner import arguments, pair_records, SOURCE_SHA, telemetry_fixture, v2_receive_fixture


spec = importlib.util.spec_from_file_location("probe_report", Path(__file__).with_name("report-probe.py"))
reporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reporter)


def sample(second, role, offset=0, base=100000):
    at = datetime.datetime(2026, 9, 5, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=second + 2 + offset)
    count = base + second * 10
    return {"type": "sample", "role": role, "second": second, "verified_bytes": 1000,
            "verified_packets": 10, "telemetry": {"at_utc": at.isoformat(),
                "process": {"cpu_user_seconds": base + second * .2, "cpu_system_seconds": base + second * .1,
                            "total_alloc_bytes": base + second * 100, "mallocs": count, "gc_count": base + second,
                            "heap_alloc_bytes": 1234, "max_rss_kib": 2048, "goroutines": 10},
                "mpudp_statistics_available": True,
                "mpudp": {"paths": [{"path": "listener" if role == "receiver" else "carrier-1",
                                     "sent_packets": count, "sent_bytes": count * 100,
                                     "received_packets": count, "received_bytes": count * 100,
                                     "send_errors": 0, "receive_oversize_drops": 0}]}}}


def fixture():
    receive = [sample(i, "receiver") for i in range(-1, 6)]
    send = [sample(i, "sender", .1) for i in range(-1, 7)]
    pair = {"worker_id": "test-w0", "receiver_side": "server", "receiver": {
        "started_utc": "2026-09-05T00:00:00Z", "warmup_seconds": 2, "seconds": 5}}
    return receive, send, pair


class AccountingTests(unittest.TestCase):
    def test_v2_receive_reports_counter_deltas_and_decreasing_ending_gauges(self):
        before, after = sample(0, "receiver"), sample(5, "receiver")
        self.assertNotIn("v2_receive", reporter.side_cost(before, after))
        before["telemetry"]["mpudp"]["v2_receive"] = v2_receive_fixture(100, 20)
        after["telemetry"]["mpudp"]["v2_receive"] = v2_receive_fixture(110, 3)
        got = reporter.side_cost(before, after)["v2_receive"]
        self.assertEqual(got["counter_deltas"], {key: 10 for key in reporter.runner.V2_RECEIVE_COUNTERS})
        self.assertEqual(got["end_gauge_snapshot"], {key: 3 for key in reporter.runner.V2_RECEIVE_GAUGES})

    def test_v2_receive_rejects_presence_changes_and_regressing_counters(self):
        for missing in (0, 1):
            before, after = sample(0, "receiver"), sample(5, "receiver")
            (before, after)[1-missing]["telemetry"]["mpudp"]["v2_receive"] = v2_receive_fixture()
            with self.assertRaisesRegex(ValueError, "presence changed"):
                reporter.side_cost(before, after)
        for key in reporter.runner.V2_RECEIVE_COUNTERS:
            before, after = sample(0, "receiver"), sample(5, "receiver")
            before["telemetry"]["mpudp"]["v2_receive"] = v2_receive_fixture(10)
            after["telemetry"]["mpudp"]["v2_receive"] = v2_receive_fixture(10)
            after["telemetry"]["mpudp"]["v2_receive"][key] = 9
            with self.assertRaisesRegex(ValueError, "regressing counter"):
                reporter.side_cost(before, after)

    def test_v2_receive_boundary_values_are_validated_without_archive_wrapper(self):
        for key in reporter.runner.V2_RECEIVE_COUNTERS + reporter.runner.V2_RECEIVE_GAUGES:
            before, after = sample(0, "receiver"), sample(5, "receiver")
            before["telemetry"]["mpudp"]["v2_receive"] = v2_receive_fixture()
            after["telemetry"]["mpudp"]["v2_receive"] = v2_receive_fixture()
            del after["telemetry"]["mpudp"]["v2_receive"][key]
            with self.assertRaisesRegex(ValueError, "v2 receive"):
                reporter.side_cost(before, after)

    def test_large_lifetime_counters_and_drain_do_not_inflate_rates(self):
        receive, send, pair = fixture()
        send[-1]["telemetry"]["mpudp"]["paths"][0]["sent_bytes"] += 99999999
        result = reporter.worker_cost(receive, send, pair)
        self.assertEqual(result["verified_bytes"], 5000)
        self.assertEqual((result["first_bucket"], result["last_bucket"]), (1, 5))
        self.assertAlmostEqual(result["sender"]["cpu_core_percent"], 30)
        self.assertEqual(result["sender"]["allocation_bytes_per_second"], 100)
        self.assertEqual(result["sender"]["socket"]["sent_pps"], 10)
        self.assertEqual(result["approximate_forward_socket_packets_per_verified_original"], 1)
        self.assertEqual(result["approximate_bidirectional_ipv4_l3_bytes_per_verified_byte"], 2.56)
        self.assertAlmostEqual(result["max_boundary_skew_seconds"], .1, places=5)

    def test_sender_indices_are_not_assumed_aligned(self):
        receive, send, pair = fixture()
        for row in send:
            row["second"] += 13
        result = reporter.worker_cost(receive, send, pair)
        self.assertEqual(result["bucket_seconds"], 5)

    def test_no_warmup_excludes_first_unbounded_bucket(self):
        receive, send, pair = fixture()
        pair["receiver"]["started_utc"] = "2026-09-05T00:00:02Z"
        pair["receiver"]["warmup_seconds"] = 0
        result = reporter.worker_cost(receive[2:], send, pair)
        self.assertEqual((result["first_bucket"], result["last_bucket"]), (2, 5))
        self.assertEqual(result["verified_bytes"], 4000)

    def test_rejects_clock_skew_and_delayed_receiver_snapshots(self):
        receive, send, pair = fixture()
        with self.assertRaisesRegex(ValueError, "aligned"):
            reporter.worker_cost(receive, [sample(i, "sender", .5) for i in range(-1, 7)], pair)
        with self.assertRaisesRegex(ValueError, "aligned"):
            reporter.worker_cost([sample(i, "receiver", .5) for i in range(-1, 6)], send, pair)

    def test_rejects_counter_reset_and_path_replacement(self):
        for kind in ("cpu", "path"):
            receive, send, pair = fixture()
            end = send[-2]["telemetry"]
            if kind == "cpu":
                end["process"]["cpu_user_seconds"] = 0
            else:
                end["mpudp"]["paths"][0]["path"] = "replacement"
            with self.assertRaises(ValueError):
                reporter.worker_cost(receive, send, pair)

    def test_duplicate_timestamps_missing_buckets_and_bad_tolerance(self):
        receive, send, pair = fixture()
        with self.assertRaisesRegex(ValueError, "increasing"):
            reporter.worker_cost(receive, [send[0], *send], pair)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            reporter.worker_cost(receive[:3] + receive[4:], send, pair)
        for tolerance in (-1, float("nan"), 1):
            with self.assertRaises(ValueError):
                reporter.worker_cost(receive, send, pair, tolerance)

    def test_unavailable_native_socket_counters_stay_unavailable(self):
        receive, send, pair = fixture()
        for row in receive + send:
            row["telemetry"]["mpudp_statistics_available"] = False
            del row["telemetry"]["mpudp"]
        result = reporter.worker_cost(receive, send, pair)
        self.assertIsNone(result["sender"]["socket"])
        self.assertIsNone(result["approximate_bidirectional_ipv4_l3_bytes_per_verified_byte"])

    def test_zero_business_bytes_do_not_fabricate_efficiency(self):
        receive, send, pair = fixture()
        for row in receive:
            row["verified_packets"] = row["verified_bytes"] = 0
        result = reporter.worker_cost(receive, send, pair)
        self.assertIsNone(result["approximate_forward_socket_packets_per_verified_original"])

    def test_checked_reader_rejects_mutation_unindexed_and_private_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = b'{"schema":1}\n'
            (root / "manifest.json").write_bytes(raw)
            (root / "unindexed.json").write_bytes(raw)
            (root / "SHA256SUMS").write_text(hashlib.sha256(raw).hexdigest() + "  manifest.json\n")
            read = reporter.checked_reader(root)
            self.assertEqual(read("manifest.json"), {"schema": 1})
            with self.assertRaisesRegex(ValueError, "checksum"):
                read("unindexed.json")
            with self.assertRaisesRegex(ValueError, "public artifact"):
                read(".lab/secret.json")
            (root / "manifest.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "checksum"):
                read("manifest.json")


class ArtifactTests(unittest.TestCase):
    def artifacts(self, root, direction="upload"):
        args = arguments(root, "--mpudp-profiles", "v2-aggregation", "--directions", direction,
                         "--warmup", "2", "--seconds", "5")
        case = reporter.runner.matrix(args)[0]
        case_dir = root / case["case_id"]
        case_dir.mkdir()
        rows = pair_records(case, case["case_id"] + "-w0", args)
        for side in ("client", "server"):
            for row in rows[side]:
                if row["type"] == "sample":
                    counter = sample(row["second"], "receiver")["telemetry"]
                    row["telemetry"]["at_utc"] = counter["at_utc"]
                    row["telemetry"]["process"] = counter["process"]
                    row["telemetry"]["mpudp"]["paths"] = counter["mpudp"]["paths"]
            if side == ("client" if direction == "upload" else "server"):
                for second in range(-1, 7):
                    row = sample(second, "sender", .1)
                    telemetry = telemetry_fixture(case)
                    telemetry["at_utc"] = row["telemetry"]["at_utc"]
                    telemetry["process"] = row["telemetry"]["process"]
                    telemetry["mpudp"]["paths"] = row["telemetry"]["mpudp"]["paths"]
                    row["telemetry"] = telemetry
                    rows[side].append(row)
            (case_dir / f"{side}-0.jsonl").write_text("\n".join(json.dumps(row) for row in rows[side]) + "\n")
        manifest = {"source_sha": SOURCE_SHA, "binary_sha256": "b" * 64,
                    "parameters": {key: value for key, value in vars(args).items() if not isinstance(value, Path)}}
        for path, value in (("manifest.json", manifest), ("matrix.json", [case]),
                            (case["case_id"] + "/case.json", case),
                            ("completed.json", {"source_sha": SOURCE_SHA, "binary_sha256": "b" * 64,
                                                "completed_cases": 1, "cleanup_verified": True})):
            (root / path).write_text(json.dumps(value))
        self.index(root)
        return case

    def index(self, root):
        (root / "SHA256SUMS").write_text("\n".join(
            hashlib.sha256(path.read_bytes()).hexdigest() + "  " + str(path.relative_to(root))
            for path in sorted(root.rglob("*")) if path.is_file() and path.name != "SHA256SUMS") + "\n")

    def test_complete_v2_artifact_revalidation_in_both_directions(self):
        for direction in ("upload", "download"):
            with self.subTest(direction=direction), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.artifacts(root, direction)
                report = reporter.report(root)
                self.assertFalse(report["product_acceptance"])
                case = report["cases"][0]
                self.assertEqual(case["workers"][0]["verified_bytes"], 120)
                self.assertEqual(case["receiver_results"][0]["echo_rtt"]["p95_ms"], 1)
                self.assertAlmostEqual(case["workers"][0]["sender"]["cpu_core_percent"], 30)

    def test_complete_v2_artifact_reports_optional_receive_statistics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case = self.artifacts(root)

            def add_receive(value):
                if isinstance(value, dict):
                    if value.get("mpudp_statistics_available"):
                        second = int(reporter.runner.instant(value["at_utc"])) % 60
                        value["mpudp"]["v2_receive"] = v2_receive_fixture(100 + second, 100 - second)
                    else:
                        for child in value.values():
                            add_receive(child)
                elif isinstance(value, list):
                    for child in value:
                        add_receive(child)

            for side in ("client", "server"):
                path = root / case["case_id"] / f"{side}-0.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                add_receive(rows)
                path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
            self.index(root)
            worker = reporter.report(root)["cases"][0]["workers"][0]
            for side in ("sender", "receiver"):
                got = worker[side]["v2_receive"]
                self.assertEqual(got["counter_deltas"], {key: 5 for key in reporter.runner.V2_RECEIVE_COUNTERS})
                self.assertEqual(got["end_gauge_snapshot"], {key: 93 for key in reporter.runner.V2_RECEIVE_GAUGES})

    def test_rejects_incomplete_mislabeled_or_duplicate_artifacts(self):
        for failure in ("cleanup", "sha", "hash_format", "duplicate", "profile", "drain"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                case = self.artifacts(root)
                filename = "completed.json"
                if failure == "hash_format":
                    filename = "manifest.json"
                elif failure == "duplicate":
                    filename = "matrix.json"
                elif failure in ("profile", "drain"):
                    filename = case["case_id"] + "/client-0.jsonl"
                path = root / filename
                value = ([json.loads(line) for line in path.read_text().splitlines()]
                         if path.suffix == ".jsonl" else json.loads(path.read_text()))
                if failure == "cleanup":
                    value["cleanup_verified"] = False
                elif failure == "sha":
                    value["source_sha"] = "c" * 40
                elif failure == "hash_format":
                    value["source_sha"] = "invalid"
                elif failure == "duplicate":
                    value.append(value[0])
                elif failure == "profile":
                    value[0]["config"]["wire_version"] = "v1"
                elif failure == "drain":
                    next(row for row in value if row["type"] == "summary")["local_drain"]["completed_sessions"] = 0
                path.write_text("\n".join(json.dumps(row) for row in value) + "\n" if path.suffix == ".jsonl" else json.dumps(value))
                self.index(root)
                with self.assertRaises(ValueError):
                    reporter.report(root)


if __name__ == "__main__":
    unittest.main()
