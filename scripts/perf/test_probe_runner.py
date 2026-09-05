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

import yaml

from test_calibrate import network_fixture

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


def config_metadata_fixture(case, side, args=None):
    if case["protocol"] not in runner.MPUDP:
        return None
    args = args or arguments(Path("."))
    profile = case.get("mpudp_profile", "v1")
    version = "v1" if profile == "v1" else "v2"
    cfg = {"mpudp_profile": profile, "wire_version": version, "protocol": "datagram",
           "repair": {"enabled": False}, "aggregation": {"enabled": profile == "v2-aggregation"},
           "configured_carriers": case["candidate_paths"] if side == "client" else 0,
           "fec": {"DataShards": args.data_shards, "ParityShards": args.parity_shards},
           "limits": {"MaxDatagramSize": args.max_datagram_size,
                      "MaxPendingFECBlocks": args.pending_blocks, "ReceiveQueueCapacity": args.queue_capacity,
                      "DeliveryQueueCapacity": args.queue_capacity},
           "transport": {"MaxUDPPayload": args.udp_budget},
           "udp_caps": {"send_hard_cap": args.udp_budget, "receive_hard_cap": args.udp_budget},
           "scheduler": {"outbound_path_rates_bps": {}, "inbound_path_rates_bps": {}}}
    if version == "v2":
        group_bytes = min(1048576, args.data_shards * (args.udp_budget - 94))
        cfg["limits"].update(MaxFragmentsPerDatagram=256,
                             MaxDatagramSize=min(args.max_datagram_size, args.v2_max_original_bytes,
                                                 256 * (group_bytes - 24)))
        cfg["transport"].update(MaxReceiveUDPPayload=args.udp_budget, MTUDiscovery="fixed", BudgetStrategy="session")
        cfg["aggregation"].update(max_delay_ns=args.v2_aggregation_max_delay_us * 1000,
                                  max_records=args.v2_aggregation_max_records,
                                  max_queued_datagrams=256, max_queued_bytes=1048576,
                                  max_group_bytes=group_bytes)
        rate_key = "outbound_path_rates_bps" if side == "client" else "inbound_path_rates_bps"
        cfg["scheduler"][rate_key] = {str(index): args.v2_path_rate_bps
                                     for index in range(1, case["candidate_paths"] + 1)}
        cfg["scheduler"]["default_path_rate_bps"] = 100000000
    return cfg


def pair_records(case, worker_id, args=None):
    receiver_side = "server" if case["direction"] == "upload" else "client"
    rows, summaries = {}, {}
    expected = {"run_id": worker_id, "protocol": case["protocol"], "direction": case["direction"],
                "flows": case["flows_per_worker"], "seconds": case["seconds"],
                "warmup_seconds": case["warmup_seconds"], "message_bytes": case["message_bytes"]}
    for side in ("client", "server"):
        paths = case["candidate_paths"] if case["protocol"] in runner.MPUDP else 1
        options = {**expected, "diagnostics": case["diagnostics"], "kcp_mtu": 1400, "kcp_window": 1024,
                   "kcp_ack_no_delay": False, "offered_mbps_per_flow": 0}
        metadata = {"type": "metadata", "side": side, "source_sha": SOURCE_SHA,
                    "path_count": paths, "options": options,
                    "config": config_metadata_fixture(case, side, args),
                    "admission_policy": dict(runner.ADMISSION_POLICY)}
        role = "receiver" if side == receiver_side else "sender"
        rows[side] = [metadata]
        size = case["message_bytes"] - 40
        if role == "receiver":
            for i in range(case["warmup_seconds"] + case["seconds"]):
                second = i + 1 - case["warmup_seconds"]
                rows[side].append({"type": "sample", "side": side, "role": role, "second": second,
                                   "steady": second > 0, "verified_bytes": size, "verified_packets": 1,
                                   "mbps": size * 8 / 1e6, "corrupt_frames": 0, "duplicate_frames": 0,
                                   "too_old_frames": 0, "telemetry": telemetry_fixture(case)})
        summaries[side] = {"type": "summary", **expected, "side": side, "role": role, "path_count": paths,
                           "started_utc": "2026-09-05T00:00:00Z", "verified_bytes": size * case["seconds"],
                           "verified_packets": case["seconds"], "mbps": size * 8 / 1e6,
                           "send_errors": 0, "read_errors": 0, "corrupt_frames": 0, "duplicate_frames": 0,
                           "too_old_frames": 0, "initial": telemetry_fixture(case), "final": telemetry_fixture(case),
                           "worst_5_second_mbps": size * 8 / 1e6 if case["seconds"] >= 5 else None}
        supported = case["flows_per_worker"] if case["protocol"] in runner.MPUDP and case.get("mpudp_profile", "v1") != "v1" else 0
        summaries[side]["local_drain"] = {"scope": runner.LOCAL_DRAIN_SCOPE, "supported_sessions": supported,
                                         "completed_sessions": supported, "failed_sessions": 0, "duration_ns": 1}
        if role == "receiver":
            summaries[side]["samples"] = [{key: value for key, value in row.items() if key not in ("type", "side", "role", "telemetry")}
                                           for row in rows[side] if row["type"] == "sample"]
            scheduled = case["seconds"] * case["flows_per_worker"] * 5
            summaries[side]["echo_rtt"] = {"sent": scheduled, "scheduled": scheduled, "submitted": scheduled,
                "queue_missed": 0, "write_failed": 0, "received": scheduled, "unanswered": 0,
                "on_time": scheduled, "deadline_missed": 0, "deadline_ms": 1000, "over_10000_ms": 0,
                "p50_ms": 1, "p95_ms": 1, "p99_ms": 1, "resolution_ms": 1}
    for side, opposite in (("client", "server"), ("server", "client")):
        rows[side].extend([copy.deepcopy(summaries[side]), {"type": "remote_summary", "summary": copy.deepcopy(summaries[opposite])}])
    return rows


def telemetry_fixture(case):
    value = {"at_utc": "2026-09-05T00:00:00Z", "process": {key: 0 for key in
             ("cpu_user_seconds", "cpu_system_seconds", "max_rss_kib", "heap_alloc_bytes", "total_alloc_bytes",
             "mallocs", "gc_count", "goroutines")}, "mpudp_statistics_available": case["protocol"] in runner.MPUDP,
             "kcp_timeout_retransmits": 0, "adapter_write_drops": 0,
             "mpudp_admission": {key: 0 for key in runner.ADMISSION_COUNTERS}}
    if value["mpudp_statistics_available"]:
        value["mpudp"] = {key: 0 for key in ("ingress_accepted", "ingress_drops", "delivery_accepted", "delivery_drops",
                            "delivered_packets", "delivered_bytes", "sent_datagrams", "sent_datagram_bytes")}
        value["mpudp"].update({"diagnostics_enabled": case["diagnostics"],
            "fec": {key: 0 for key in ("completed_blocks", "recovered_blocks", "recovered_shards", "expired_blocks",
                                      "decoder_full", "late_shards", "duplicate_shards")},
            "paths": [{key: 0 for key in ("sent_packets", "sent_bytes", "send_errors", "received_packets",
                                         "received_bytes", "receive_oversize_drops")}]})
    if case["protocol"] in ("kcp", "kcp-mpudp"):
        value["kcp_snmp"] = {key: 0 for key in ("InSegs", "OutSegs", "RetransSegs", "FastRetransSegs", "EarlyRetransSegs", "LostSegs")}
        value["kcp_sessions"] = [{"flow": flow, "srtt_ms": 0, "srtt_variation_ms": 0, "rto_ms": 0}
                                  for flow in range(case["flows_per_worker"])]
        if case["diagnostics"]:
            value["kcp_correlation"] = [correlation_fixture(case["protocol"], flow)
                                          for flow in range(case["flows_per_worker"])]
    return value


def duration_fixture(count=0):
    return {"count": count, "sum_ns": count * 1000, "max_ns": 1000 if count else 0,
            "buckets": [count, *([0] * 24)]}


def correlation_fixture(protocol, flow):
    packet = protocol == "kcp-mpudp"
    value = {key: 0 for key in runner.KCP_CORRELATION_COUNTERS}
    value.update({"flow": flow, "packet_correlation_available": packet, "retransmit_reason_available": False,
                  "slot_limit": 1024 if packet else 0, "attempts_per_slot": 4 if packet else 0,
                  "boundary": "mpudp_datagram_adapter_call; not_individual_socket_write" if packet else
                  "application_write_only; native_batch_socket_correlation_unavailable"})
    value.update({key: duration_fixture() for key in runner.KCP_CORRELATION_DISTRIBUTIONS})
    return value


class CorrelationTelemetryTests(unittest.TestCase):
    def setUp(self):
        self.case = {"protocol": "kcp-mpudp", "diagnostics": True, "flows_per_worker": 2}

    def verify(self, value, source_sha=SOURCE_SHA):
        runner.verify_telemetry(value, self.case, source_sha)

    def test_each_kcp_mode_has_its_actual_boundary(self):
        for protocol in ("kcp", "kcp-mpudp"):
            self.case["protocol"] = protocol
            value = telemetry_fixture(self.case)
            for trace in value["kcp_correlation"]:
                trace["application_write"] = duration_fixture(1)
            self.verify(value)

    def test_missing_or_duplicate_flow_trace_is_rejected(self):
        for traces in (None, [], [correlation_fixture("kcp-mpudp", 0)],
                       [correlation_fixture("kcp-mpudp", 0), correlation_fixture("kcp-mpudp", 0)]):
            with self.subTest(traces=traces):
                value = telemetry_fixture(self.case)
                value["kcp_correlation"] = traces
                with self.assertRaisesRegex(ValueError, "correlation"):
                    self.verify(value)

    def test_original_product_baseline_still_requires_new_probe_trace(self):
        value = telemetry_fixture(self.case)
        value.pop("mpudp")
        value["mpudp_statistics_available"] = False
        self.verify(value, runner.calibrate.BASELINE_SHA)
        value.pop("kcp_correlation")
        with self.assertRaisesRegex(ValueError, "correlation"):
            self.verify(value, runner.calibrate.BASELINE_SHA)

    def test_disabled_diagnostics_and_non_kcp_cannot_report_trace(self):
        for protocol, enabled in (("kcp", False), ("kcp-mpudp", False), ("tcp", True), ("mpudp", True)):
            with self.subTest(protocol=protocol, diagnostics=enabled):
                self.case.update(protocol=protocol, diagnostics=enabled)
                value = telemetry_fixture(self.case)
                self.verify(value)
                value["kcp_correlation"] = [correlation_fixture("kcp-mpudp", 0)]
                with self.assertRaisesRegex(ValueError, "unexpected KCP correlation"):
                    self.verify(value)

    def test_boundary_bounds_and_unknown_reason_are_bound(self):
        for protocol in ("kcp", "kcp-mpudp"):
            self.case["protocol"] = protocol
            for field, wrong in (("packet_correlation_available", protocol != "kcp-mpudp"),
                                 ("packet_correlation_available", 1), ("retransmit_reason_available", True),
                                 ("retransmit_reason_available", 0), ("boundary", "individual_socket_write"),
                                 ("slot_limit", 999), ("attempts_per_slot", 99)):
                with self.subTest(protocol=protocol, field=field, wrong=wrong):
                    value = telemetry_fixture(self.case)
                    value["kcp_correlation"][0][field] = wrong
                    with self.assertRaisesRegex(ValueError, "boundary"):
                        self.verify(value)

    def test_histograms_require_25_nonnegative_bins_with_matching_count(self):
        for field, wrong in (("buckets", [0] * 24), ("buckets", [0] * 26),
                             ("buckets", [1] + [0] * 24), ("buckets", [-1] + [0] * 24),
                             ("buckets", [True] + [0] * 24), ("count", True), ("sum_ns", 1), ("max_ns", 1)):
            with self.subTest(field=field, wrong=wrong):
                value = telemetry_fixture(self.case)
                value["kcp_correlation"][0]["entry_to_ack"][field] = wrong
                with self.assertRaisesRegex(ValueError, "KCP duration"):
                    self.verify(value)

    def test_missing_or_negative_correlation_fields_are_rejected(self):
        for field in (*runner.KCP_CORRELATION_COUNTERS, *runner.KCP_CORRELATION_DISTRIBUTIONS):
            with self.subTest(field=field):
                value = telemetry_fixture(self.case)
                del value["kcp_correlation"][0][field]
                with self.assertRaises(ValueError):
                    self.verify(value)
        value = telemetry_fixture(self.case)
        value["kcp_correlation"][0]["matched_acks"] = -1
        with self.assertRaises(ValueError):
            self.verify(value)

    def test_duration_maximum_must_fit_its_nonempty_bucket(self):
        value = telemetry_fixture(self.case)
        distribution = duration_fixture(1)
        value["kcp_correlation"][0]["application_write"] = distribution
        distribution.update(sum_ns=2000, max_ns=2000)
        with self.assertRaisesRegex(ValueError, "maximum disagrees"):
            self.verify(value)
        distribution["buckets"] = [0, 1, *([0] * 23)]
        self.verify(value)

    def test_ack_classes_and_exact_timing_cannot_double_count(self):
        value = telemetry_fixture(self.case)
        trace = value["kcp_correlation"][0]
        trace.update(inbound_ack_segments=5, matched_acks=1, unmatched_acks=1, ambiguous_acks=1,
                     incomplete_history_acks=1, duplicate_acks=1)
        trace["entry_to_ack"] = duration_fixture(1)
        # An ACK may be observed before its in-flight adapter call returns.
        self.verify(value)
        for field, wrong in (("inbound_ack_segments", 4), ("matched_acks", 2),
                             ("incomplete_history_acks", 0), ("ack_before_adapter_return", 2)):
            with self.subTest(field=field):
                invalid = copy.deepcopy(value)
                invalid["kcp_correlation"][0][field] = wrong
                with self.assertRaisesRegex(ValueError, "ACK classification"):
                    self.verify(invalid)
        trace["return_to_ack"] = duration_fixture(1)
        self.verify(value)
        trace["ack_before_adapter_return"] = 1
        with self.assertRaisesRegex(ValueError, "ACK classification"):
            self.verify(value)

    def test_native_cannot_fill_unavailable_packet_counters(self):
        self.case["protocol"] = "kcp"
        value = telemetry_fixture(self.case)
        value["kcp_correlation"][0]["outbound_packets"] = 1
        with self.assertRaisesRegex(ValueError, "unavailable packet"):
            self.verify(value)


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

    def test_profiles_preserve_v1_ids_and_do_not_duplicate_native_cases(self):
        args = arguments(self.directory, "--protocols", "mpudp", "kcp-mpudp", "kcp",
                         "--paths", "1", "2", "3", "5")
        legacy = runner.matrix(args)
        args.mpudp_profiles = ["v1", "v2", "v2-aggregation"]
        cases = runner.matrix(args)
        self.assertEqual(len(cases), 31)  # 24 MPUDP variants plus 7 native layouts.
        self.assertEqual([case for case in cases if case.get("mpudp_profile", "v1") == "v1"], legacy)
        self.assertEqual(len({case["case_id"] for case in cases}), len(cases))
        for case in cases:
            if case["protocol"] == "kcp":
                self.assertNotIn("mpudp_profile", case)
            elif case["mpudp_profile"] != "v1":
                self.assertTrue(case["case_id"].endswith("-" + case["mpudp_profile"]))
        self.assertEqual(runner.matrix(args), cases)

    def test_v2_config_has_integer_role_rates_and_datagram_wire_for_both_adapters(self):
        args = arguments(self.directory, "--protocols", "mpudp", "kcp-mpudp", "--paths", "1", "2", "3", "5",
                         "--mpudp-profiles", "v2", "v2-aggregation", "--v2-path-rate-bps", "80000000",
                         "--v2-aggregation-max-delay-us", "500", "--v2-aggregation-max-records", "16")
        topology = {"server_addresses": [f"10.206.{index}.2" for index in range(1, 6)]}
        for case in runner.matrix(args):
            for side in ("client", "server"):
                with self.subTest(case=case["case_id"], side=side):
                    cfg = runner.mpudp_config(args, topology, case, side, "private-marker")
                    encoded = runner.mpudp_config_bytes(cfg)
                    self.assertEqual(yaml.safe_load(encoded), cfg)
                    self.assertEqual(cfg["protocol"], "datagram")
                    self.assertEqual(cfg["wire"], {"version": "v2"})
                    self.assertEqual(cfg["repair"], {"enabled": False})
                    self.assertEqual(cfg["transport"], {"max_udp_payload": 1200, "max_receive_udp_payload": 1200,
                                                        "mtu_discovery": "fixed", "budget_strategy": "session"})
                    self.assertEqual(cfg["aggregation"], {"enabled": case["mpudp_profile"] == "v2-aggregation",
                        "max_delay": "500us", "max_records": 16, "max_queued_datagrams": 256,
                        "max_queued_bytes": 1048576, "max_group_bytes": 3318})
                    key = "outbound_path_rates_bps" if side == "client" else "inbound_path_rates_bps"
                    self.assertEqual(cfg["scheduler"], {key: {index: 80000000
                                                              for index in range(1, case["candidate_paths"] + 1)}})
                    self.assertTrue(all(type(index) is int for index in yaml.safe_load(encoded)["scheduler"][key]))
                    self.assertEqual(runner.mpudp_config_bytes(cfg), encoded)

    def test_v1_serialization_and_configuration_are_unchanged(self):
        args = arguments(self.directory)
        case, = runner.matrix(args)
        cfg = runner.mpudp_config(args, {"server_addresses": ["10.206.1.2"]}, case, "client", "private")
        self.assertEqual(case["case_id"], "mpudp-p1-mpudp-upload-b64-f1-diagoff-r1")
        self.assertEqual(cfg, {"psk": "private", "fec": {"data_shards": 3, "parity_shards": 2},
            "transport": {"max_udp_payload": 1200}, "limits": {"max_datagram_size": 65536,
            "max_pending_fec_blocks": 8192, "receive_queue_capacity": 4096, "delivery_queue_capacity": 4096},
            "carriers": ["10.206.1.2:29000"]})
        self.assertEqual(runner.mpudp_config_bytes(cfg), json.dumps(cfg).encode())

    def test_generated_v2_yaml_passes_actual_strict_go_configuration_parser(self):
        args = arguments(self.directory, "--protocols", "mpudp", "kcp-mpudp", "--paths", "1", "2", "3", "5",
                         "--mpudp-profiles", "v1", "v2", "v2-aggregation")
        topology = {"server_addresses": [f"10.206.{index}.2" for index in range(1, 6)]}
        configs = [runner.mpudp_config_bytes(runner.mpudp_config(args, topology, case, side, "parse-fixture-key")).decode()
                   for case in runner.matrix(args) for side in ("client", "server")]
        source = self.directory / "parse_configs.go"
        source.write_text('''package main
import (
    "encoding/json"
    "fmt"
    "os"
    "github.com/mofelee/mpudp/config"
)
func main() {
    var inputs []string
    if err := json.NewDecoder(os.Stdin).Decode(&inputs); err != nil { panic(err) }
    for index, input := range inputs {
        if _, err := config.Parse([]byte(input)); err != nil {
            fmt.Fprintf(os.Stderr, "config %d: %v\\n", index, err)
            os.Exit(1)
        }
    }
    fmt.Println(len(inputs))
}
''')
        result = subprocess.run(["go", "run", str(source)], input=json.dumps(configs), text=True,
                                capture_output=True, cwd=runner.ROOT, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(int(result.stdout), 48)

    def test_v2_max_original_uses_fragment_capacity_and_explicit_ceiling(self):
        args = arguments(self.directory, "--mpudp-profiles", "v2", "--payloads", "max")
        self.assertEqual(runner.matrix(args)[0]["message_bytes"], 65536)
        args.v2_max_original_bytes = 2048
        self.assertEqual(runner.matrix(args)[0]["message_bytes"], 2048)
        args.v2_max_original_bytes = args.max_datagram_size = 1048576
        args.data_shards, args.udp_budget = 1, 512
        self.assertEqual(runner.matrix(args)[0]["message_bytes"], 256 * (512 - 94 - 24))
        args.max_datagram_size = 4096
        self.assertEqual(runner.matrix(args)[0]["message_bytes"], 4096)
        args.max_datagram_size = 1024
        with self.assertRaisesRegex(ValueError, "original Datagram budget"):
            runner.payload_size("1400", "mpudp", args, "v2")

    def test_v2_profiles_reject_unusable_settings_before_remote_work(self):
        for extra in (("--udp-budget", "511"), ("--v2-max-original-bytes", "1048577"),
                      ("--v2-path-rate-bps", "999"), ("--v2-path-rate-bps", "1000000000001"),
                      ("--v2-aggregation-max-delay-us", "0"), ("--v2-aggregation-max-delay-us", "10001"),
                      ("--v2-aggregation-max-records", "0"), ("--v2-aggregation-max-records", "257"),
                      ("--source-sha", runner.calibrate.BASELINE_SHA),
                      ("--mpudp-profiles", "v2", "v2")):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                arguments(self.directory, "--mpudp-profiles", "v2", *extra)
        args = arguments(self.directory, "--protocols", "kcp-mpudp", "--mpudp-profiles", "v2",
                         "--v2-max-original-bytes", "1024")
        with self.assertRaisesRegex(ValueError, "KCP MTU"):
            runner.matrix(args)

    def test_missing_yaml_dependency_keeps_v1_usable_and_fails_v2_clearly(self):
        args = arguments(self.directory, "--mpudp-profiles", "v1", "v2")
        configs = [runner.mpudp_config(args, {"server_addresses": ["10.206.1.2"]}, case, "client", "private")
                   for case in runner.matrix(args)]
        with mock.patch.dict("sys.modules", {"yaml": None}):
            self.assertEqual(json.loads(runner.mpudp_config_bytes(configs[0])), configs[0])
            with self.assertRaisesRegex(ValueError, "PyYAML"):
                runner.mpudp_config_bytes(configs[1])

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
                                  self.case, self.worker_id, SOURCE_SHA, self.args)

    def exchange_summaries(self):
        for side, opposite in (("client", "server"), ("server", "client")):
            self.rows[side][-1]["summary"] = copy.deepcopy(self.rows[opposite][-2])

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

    def test_requested_diagnostics_kcp_and_rate_are_bound_on_both_sides(self):
        for side in ("client", "server"):
            for key, wrong in (("diagnostics", True), ("kcp_mtu", 1200), ("kcp_window", 64),
                               ("kcp_ack_no_delay", True), ("offered_mbps_per_flow", 10)):
                with self.subTest(side=side, key=key):
                    self.rows = pair_records(self.case, self.worker_id)
                    self.rows[side][0]["options"][key] = wrong
                    with self.assertRaisesRegex(ValueError, "probe options"):
                        self.verify()

    def test_profile_metadata_and_actual_settings_are_bound_on_both_sides(self):
        self.case["candidate_paths"] = 5
        for profile in ("v1", "v2", "v2-aggregation"):
            self.case["mpudp_profile"] = profile
            self.rows = pair_records(self.case, self.worker_id)
            self.verify()
            for side in ("client", "server"):
                for section, field, wrong in ((None, "mpudp_profile", "unknown"), (None, "wire_version", "unknown"),
                        (None, "protocol", "kcp"), ("repair", "enabled", True), ("repair", "enabled", 0),
                        ("fec", "DataShards", 1), ("limits", "MaxDatagramSize", 1),
                        ("udp_caps", "receive_hard_cap", 512), ("transport", "MaxUDPPayload", 512),
                        ("aggregation", "enabled", profile != "v2-aggregation"),
                        ("aggregation", "enabled", int(profile == "v2-aggregation")),
                        (None, "configured_carriers", 99)):
                    with self.subTest(profile=profile, side=side, section=section, field=field):
                        self.rows = pair_records(self.case, self.worker_id)
                        cfg = self.rows[side][0]["config"]
                        (cfg if section is None else cfg[section])[field] = wrong
                        with self.assertRaisesRegex(ValueError, "MPUDP"):
                            self.verify()
                if profile != "v1":
                    for section, field, wrong in (("aggregation", "max_delay_ns", 1),
                            ("aggregation", "max_queued_bytes", 1), ("aggregation", "max_group_bytes", 1),
                            ("transport", "MTUDiscovery", "plpmtud"), ("transport", "BudgetStrategy", "per_carrier"),
                            ("limits", "MaxFragmentsPerDatagram", 1),
                            ("scheduler", "default_path_rate_bps", 1),
                            ("scheduler", "outbound_path_rates_bps" if side == "client" else "inbound_path_rates_bps", {})):
                        with self.subTest(profile=profile, side=side, section=section, field=field):
                            self.rows = pair_records(self.case, self.worker_id)
                            self.rows[side][0]["config"][section][field] = wrong
                            with self.assertRaisesRegex(ValueError, "MPUDP"):
                                self.verify()

    def test_native_protocols_cannot_claim_mpudp_configuration(self):
        self.case["protocol"] = "kcp"
        self.rows = pair_records(self.case, self.worker_id)
        self.verify()
        self.rows["client"][0]["config"] = {"mpudp_profile": "v2"}
        with self.assertRaisesRegex(ValueError, "native protocol"):
            self.verify()

    def test_bounded_admission_policy_and_successful_local_drain_are_required(self):
        self.case["mpudp_profile"] = "v2-aggregation"
        for side in ("client", "server"):
            for key in runner.ADMISSION_POLICY:
                with self.subTest(side=side, policy=key):
                    self.rows = pair_records(self.case, self.worker_id)
                    del self.rows[side][0]["admission_policy"][key]
                    with self.assertRaisesRegex(ValueError, "admission retry policy"):
                        self.verify()
            for key, wrong in (("scope", "remote_ack"), ("supported_sessions", 0), ("completed_sessions", 0),
                               ("failed_sessions", 1), ("duration_ns", -1), ("supported_sessions", True)):
                with self.subTest(side=side, drain=key):
                    self.rows = pair_records(self.case, self.worker_id)
                    self.rows[side][-2]["local_drain"][key] = wrong
                    self.exchange_summaries()
                    with self.assertRaisesRegex(ValueError, "local drain"):
                        self.verify()

    def test_admission_pressure_counters_cannot_be_missing_or_transport_drops(self):
        for key in runner.ADMISSION_COUNTERS:
            value = telemetry_fixture(self.case)
            del value["mpudp_admission"][key]
            with self.subTest(missing=key), self.assertRaisesRegex(ValueError, "MPUDP admission"):
                runner.verify_telemetry(value, self.case, SOURCE_SHA)
        value = telemetry_fixture(self.case)
        value["mpudp_admission"].update(backpressured_packets=1, rejected_attempts=2, retry_attempts=2, wait_ns=1000)
        runner.verify_telemetry(value, self.case, SOURCE_SHA)
        self.assertEqual(value["adapter_write_drops"], 0)
        self.case["protocol"] = "kcp"
        value = telemetry_fixture(self.case)
        value["mpudp_admission"]["rejected_attempts"] = 1
        with self.assertRaisesRegex(ValueError, "native protocol"):
            runner.verify_telemetry(value, self.case, SOURCE_SHA)

    def test_missing_rtt_or_telemetry_cannot_pass(self):
        for field in ("echo_rtt", "initial", "final"):
            self.rows = pair_records(self.case, self.worker_id)
            del self.rows["server"][-2][field]
            self.exchange_summaries()
            with self.assertRaises(ValueError):
                self.verify()
        self.rows = pair_records(self.case, self.worker_id)
        del self.rows["server"][1]["telemetry"]
        with self.assertRaises(ValueError):
            self.verify()

    def test_kcp_requires_per_flow_and_timeout_evidence(self):
        self.case["protocol"] = "kcp"
        self.rows = pair_records(self.case, self.worker_id)
        self.verify()
        self.rows["server"][1]["telemetry"]["kcp_sessions"] = []
        with self.assertRaisesRegex(ValueError, "per-flow KCP"):
            self.verify()

    def test_only_original_baseline_can_lack_mpudp_statistics(self):
        value = telemetry_fixture(self.case)
        value.pop("mpudp")
        value["mpudp_statistics_available"] = False
        runner.verify_telemetry(value, self.case, runner.calibrate.BASELINE_SHA)
        with self.assertRaisesRegex(ValueError, "missing statistics"):
            runner.verify_telemetry(value, self.case, SOURCE_SHA)

    def test_quantiles_cannot_ignore_unanswered_opportunities(self):
        latency = self.rows["server"][-2]["echo_rtt"]
        latency.update(received=1, unanswered=latency["scheduled"] - 1,
                       on_time=1, deadline_missed=latency["scheduled"] - 1)
        self.exchange_summaries()
        with self.assertRaisesRegex(ValueError, "quantile"):
            self.verify()
        latency.update(p50_ms=None, p95_ms=None, p99_ms=None)
        self.exchange_summaries()
        self.verify()

    def test_compact_integrity_and_worst_window_must_match(self):
        for field, wrong in (("corrupt_frames", 1), ("samples", []), ("worst_5_second_mbps", 1)):
            self.rows = pair_records(self.case, self.worker_id)
            self.rows["server"][-2][field] = wrong
            self.exchange_summaries()
            with self.assertRaises(ValueError):
                self.verify()

    def test_near_complete_overlap_bounds_parallel_aggregation(self):
        self.case["seconds"] = 300
        pairs = [{"receiver": {"started_utc": instant}} for instant in
                 ("2026-09-05T00:00:00Z", "2026-09-05T00:00:01Z")]
        start, end, skew = runner.receiver_overlap(pairs, self.case)
        self.assertEqual((end - start) / 1e9, 299)
        self.assertEqual(skew, 1)
        pairs[1]["receiver"]["started_utc"] = "2026-09-05T00:04:59Z"
        with self.assertRaisesRegex(ValueError, "near-complete"):
            runner.receiver_overlap(pairs, self.case)


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

    def test_all_profile_plan_is_deterministic_and_keeps_formal_window_requirements(self):
        binary = self.directory / "perfprobe"
        binary.write_bytes(b"fixture-executable")
        matrices = []
        for name in ("profiles-first", "profiles-second"):
            output = self.directory / name
            argv = ["--topology", str(Path(__file__).with_name("topology.example.json")),
                    "--ssh-config", "unused", "--binary", str(binary), "--source-sha", SOURCE_SHA,
                    "--output", str(output), "--mpudp-profiles", "v1", "v2", "v2-aggregation", "--plan"]
            with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(runner.ProbeRunner, "prepare") as prepare, \
                    mock.patch.object(runner.calibrate, "run", return_value=SOURCE_SHA):
                runner.main(argv)
            prepare.assert_not_called()
            matrices.append((output / "matrix.json").read_bytes())
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["parameters"]["mpudp_profiles"], ["v1", "v2", "v2-aggregation"])
            self.assertEqual(manifest["data_shard_overhead_by_profile"], {"v1": 71, "v2": 94, "v2-aggregation": 94})
            self.assertEqual(manifest["v2_fragment_manifest_overhead"], 24)
            self.assertEqual(manifest["admission_policy"], runner.ADMISSION_POLICY)
            self.assertNotIn("plan-only-redacted-key", (output / "manifest.json").read_text())
            self.assertNotIn("plan-only-redacted-key", (output / "matrix.json").read_text())
            plan = json.loads((output / "plan-summary.json").read_text())
            self.assertEqual(plan["total_cases"], 1080)
            self.assertEqual(plan["minimum_wall_seconds"], 1080 * 320)
            cases = json.loads(matrices[-1])
            self.assertEqual({case["candidate_paths"] for case in cases}, {1, 2, 3, 5})
            self.assertTrue(all(case["formal_window"] and not case["product_acceptance"] for case in cases))
        self.assertEqual(*matrices)


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
        self.missing_after_snapshot = False
        self.sampler_stop_requests = []
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
        if command[:2] == ["systemctl", "kill"]:
            self.sampler_stop_requests.append((host, command))
            return b""
        self.ss_calls += 1
        if self.occupied or self.ss_calls > 2:
            return f"LISTEN 0 10 192.0.2.2:{self.args.control_port} 0.0.0.0:*\n".encode()
        return b""

    def python(self, host, script, *args, data=None, timeout=30):
        if data is not None:
            self.config_data.append((args[0], yaml.safe_load(data)))
        return b""

    def stop(self, host, unit):
        self.stops.append((host, unit))
        return {"host": host, "unit": unit, "stopped": not self.bad_cleanup}

    def popen(self, command, *, stdout, stderr, stdin):
        words = shlex.split(command[-1])
        self.commands.append(words)
        label = words[words.index("--unit") + 1]
        if label.endswith("sampler"):
            snapshots = network_fixture()
            snapshots[1].update(started_unix_ns=2**63 - 2, finished_unix_ns=2**63 - 1)
            rows = [{"kind": "host"}, *snapshots[:1 if self.missing_after_snapshot else 2]]
            dead = False
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
        self.assertEqual(len(self.sampler_stop_requests), 8)
        self.assertEqual(len(summary["network_snapshots"]), 8)
        self.assertTrue(all(unit.startswith("mpudp-probe-") for _, unit in self.stops))
        self.assertTrue(all(process.poll() is not None for process in self.processes))
        self.assertEqual(len(self.config_data), 2)
        self.assertTrue(all(cfg["psk"] == "private-marker" for _, cfg in self.config_data))
        self.assertNotIn("private-marker", repr(self.commands))
        self.assertTrue(all("RuntimeMaxSec=108" in cmd and "KillMode=control-group" in cmd for cmd in self.commands))

    def test_v2_deploys_private_yaml_without_changing_go_probe_flags(self):
        self.case["mpudp_profile"] = "v2-aggregation"
        self.case["case_id"] += "-v2-aggregation"
        summary = self.probe.case(self.case)
        self.assertEqual(summary["pairs"][0]["receiver"]["local_drain"]["completed_sessions"], 1)
        self.assertTrue(all(path.endswith(".yaml") for path, _ in self.config_data))
        self.assertTrue(all(cfg["wire"] == {"version": "v2"} and cfg["aggregation"]["enabled"]
                            for _, cfg in self.config_data))
        self.assertNotIn("private-marker", repr(self.commands))
        for command in self.commands:
            if "-mode" in command:
                self.assertIn("-config", command)
                self.assertNotIn("-mpudp-profile", command)

    def test_occupied_port_stops_no_existing_service(self):
        self.occupied = True
        with self.assertRaises(RuntimeError):
            self.probe.case(self.case)
        self.assertEqual(self.processes, [])
        self.assertEqual(self.stops, [])

    def test_missing_after_snapshot_rejects_summary_after_cleanup(self):
        self.missing_after_snapshot = True
        with self.assertRaisesRegex(ValueError, "snapshot is incomplete"):
            self.probe.case(self.case)
        self.assertEqual(len(self.stops), 10)
        self.assertFalse((self.args.output / self.case["case_id"] / "summary.json").exists())

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
