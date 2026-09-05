#!/usr/bin/env python3
"""Run the receiver-verified perfprobe matrix on the existing reference SSH lab.

Build integration/perf/cmd/perfprobe with -X main.sourceSHA=<full source SHA>,
then supply --binary, --source-sha, --topology, --ssh-config, --psk-file and
--output. Defaults are 20s warmup, 300s steady state and three rounds.
Profiles remain private under output/.lab and are excluded from SHA256SUMS.
The default matrix has 696 cases and at least 61.9 hours of measurement windows,
before setup, collection and cleanup. Use --plan to review the selected matrix;
filter --protocols/--paths/--directions/--payloads for a bounded subset. Reduced
windows are smoke tests and never satisfy the formal measurement gate.
--mpudp-profiles defaults to v1; explicit v2 and v2-aggregation selections add
separate MPUDP cases without repeating native TCP/UDP/KCP baselines.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import importlib.util
import ipaddress
import itertools
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time

spec = importlib.util.spec_from_file_location("probe_calibrate", Path(__file__).with_name("calibrate.py"))
calibrate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibrate)
ROOT = Path(__file__).resolve().parents[2]
PROTOCOLS = ("tcp", "udp", "kcp", "mpudp", "kcp-mpudp")
MPUDP = ("mpudp", "kcp-mpudp")
MPUDP_PROFILES = ("v1", "v2", "v2-aggregation")
DATA_SHARD_OVERHEAD = 71  # v1: prefix 24 + metadata 15 + HMAC-SHA256 32.
V2_DATA_SHARD_OVERHEAD = 94
V2_MANIFEST_OVERHEAD = 24  # One manifest prefix and one fragment descriptor.
V2_MAX_FRAGMENTS = 256
V2_QUEUE_BYTES = 1 << 20
V2_QUEUE_DATAGRAMS = 256
ADMISSION_COUNTERS = ("backpressured_packets", "rejected_attempts", "retry_attempts", "wait_ns",
                      "canceled_packets", "timeout_packets")
V2_RECEIVE_COUNTERS = ("received_fec_bundles", "packet_scratch_rejections", "new_group_rejections",
                       "original_admission_rejections", "decoded_groups", "completed_groups", "expired_groups")
V2_RECEIVE_GAUGES = ("pending_groups", "decoded_pending_groups", "pending_originals",
                     "credit_bytes", "credit_reservations")
ADMISSION_POLICY = {"max_wait_ns": 1000000000, "retry_wait_ns": 100000,
                    "retry_scope": "whole_datagram_resource_limit", "local_drain_limit_ns": 3000000000}
LOCAL_DRAIN_SCOPE = "admitted_mpudp_datagrams_local_socket_attempts"
PROFILE_SUFFIXES = ("cpu", "allocs", "heap", "mutex", "block")
REMOTE_PATH = "/run/current-system/sw/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def read_secret(path):
    if path is None:
        raise ValueError("MPUDP protocols require --psk-file")
    if path.stat().st_mode & 0o077:
        raise ValueError("PSK file must not be accessible to group or other users")
    value = path.read_text().removesuffix("\n").removesuffix("\r")
    if not 1 <= len(value.encode()) <= 4096 or any(c in value for c in "\n\r\x00"):
        raise ValueError("PSK file must contain one nonempty line, at most 4096 bytes")
    return value


def v2_group_bytes(args):
    return min(V2_QUEUE_BYTES, args.data_shards * (args.udp_budget - V2_DATA_SHARD_OVERHEAD))


def datagram_limit(args, profile="v1"):
    if profile == "v1":
        return min(args.max_datagram_size, args.data_shards * (args.udp_budget - DATA_SHARD_OVERHEAD))
    return min(args.max_datagram_size, args.v2_max_original_bytes,
               V2_MAX_FRAGMENTS * (v2_group_bytes(args) - V2_MANIFEST_OVERHEAD))


def payload_size(label, protocol, args, profile="v1"):
    limit = datagram_limit(args, profile)
    if label == "max":
        if protocol == "mpudp":
            return limit
        if protocol == "udp":
            return args.physical_mtu - 28
        return args.stream_max_payload
    size = int(label)
    if protocol == "mpudp" and size > limit:
        raise ValueError(f"requested MPUDP payload exceeds {profile} original Datagram budget")
    if protocol == "udp" and size > args.physical_mtu - 28:
        raise ValueError("requested UDP payload exceeds reference IPv4 path budget")
    return size


def matrix(args):
    result = []
    dimensions = itertools.product(args.protocols, args.paths, args.directions, args.payloads,
                                   args.flows, args.diagnostics, range(1, args.rounds + 1))
    for protocol, paths, direction, label, flows, diagnostics, round_number in dimensions:
        layouts = ("mpudp",) if protocol in MPUDP else ("single", "parallel") if paths > 1 else ("single",)
        profiles = args.mpudp_profiles if protocol in MPUDP else (None,)
        for layout, profile in itertools.product(layouts, profiles):
            workers = paths if layout == "parallel" else 1
            case = {"protocol": protocol, "candidate_paths": paths, "layout": layout,
                    "active_paths": paths if protocol in MPUDP or layout == "parallel" else 1,
                    "workers": workers, "flows_per_worker": flows, "total_flows": flows * workers,
                    "single_flow": flows * workers == 1, "direction": direction,
                    "payload_label": label, "message_bytes": payload_size(label, protocol, args, profile or "v1"),
                    "verification_header_bytes": 40, "diagnostics": diagnostics == "on",
                    "round": round_number, "seconds": args.seconds, "warmup_seconds": args.warmup,
                    "formal_window": args.seconds >= 300 and args.warmup >= 20 and args.rounds >= 3,
                    "product_acceptance": False}
            case["case_id"] = (f"{protocol}-p{paths}-{layout}-{direction}-b{label}-f{flows}-"
                               f"diag{diagnostics}-r{round_number}")
            if profile is not None:
                case.update(mpudp_profile=profile, wire_version="v1" if profile == "v1" else "v2",
                            max_original_bytes=datagram_limit(args, profile))
                if profile != "v1":
                    case["case_id"] += "-" + profile
            if case["message_bytes"] < 64 or case["message_bytes"] * flows > 64 * 1024 * 1024:
                raise ValueError("message and flow sizes exceed probe memory bounds")
            if protocol == "kcp-mpudp" and args.kcp_mtu > datagram_limit(args, profile):
                raise ValueError("KCP MTU exceeds MPUDP negotiated Datagram budget")
            result.append(case)
    if len({c["case_id"] for c in result}) != len(result):
        raise ValueError("matrix dimensions must not contain duplicates")
    return result


def mpudp_config(args, topology, case, side, secret):
    cfg = {"psk": secret, "fec": {"data_shards": args.data_shards, "parity_shards": args.parity_shards},
           "transport": {"max_udp_payload": args.udp_budget},
           "limits": {"max_datagram_size": args.max_datagram_size,
                      "max_pending_fec_blocks": args.pending_blocks,
                      "receive_queue_capacity": args.queue_capacity,
                      "delivery_queue_capacity": args.queue_capacity}}
    profile = case.get("mpudp_profile", "v1")
    if profile != "v1":
        cfg.update(protocol="datagram", wire={"version": "v2"}, repair={"enabled": False})
        cfg["transport"].update(max_receive_udp_payload=args.udp_budget,
                                mtu_discovery="fixed", budget_strategy="session")
        cfg["limits"].update(max_datagram_size=datagram_limit(args, profile),
                             max_fragments_per_datagram=V2_MAX_FRAGMENTS)
        cfg["aggregation"] = {"enabled": profile == "v2-aggregation",
                              "max_delay": f"{args.v2_aggregation_max_delay_us}us",
                              "max_records": args.v2_aggregation_max_records,
                              "max_queued_datagrams": V2_QUEUE_DATAGRAMS,
                              "max_queued_bytes": V2_QUEUE_BYTES,
                              "max_group_bytes": v2_group_bytes(args)}
        rate_key = "outbound_path_rates_bps" if side == "client" else "inbound_path_rates_bps"
        cfg["scheduler"] = {rate_key: {path_id: args.v2_path_rate_bps
                                      for path_id in range(1, case["candidate_paths"] + 1)}}
    if side == "client":
        cfg["carriers"] = [f"{address}:{args.data_port}" for address in topology["server_addresses"][:case["candidate_paths"]]]
    else:
        cfg["listen"] = f"0.0.0.0:{args.data_port}"
    return cfg


def mpudp_config_bytes(config):
    if config.get("wire", {}).get("version") != "v2":
        return json.dumps(config).encode()
    try:
        import yaml
    except ImportError as error:
        raise ValueError("v2 profiles require PyYAML; install PyYAML==6.0.2 before running the matrix") from error
    # JSON quotes integer map keys, which strict v2 scheduler parsing rejects.
    return yaml.safe_dump(config, sort_keys=False, allow_unicode=False).encode()


def records(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream if line.strip()]


def unique_record(rows, kind):
    selected = [row for row in rows if row.get("type") == kind]
    if len(selected) != 1:
        raise ValueError(f"expected exactly one {kind} record")
    return selected[0]


def require_counters(value, names, label):
    if not isinstance(value, dict) or any(type(value.get(name)) is not int or value[name] < 0 for name in names):
        raise ValueError(f"invalid or missing {label} counters")


def finite_nonnegative(value):
    return type(value) in (float, int) and math.isfinite(value) and value >= 0


def instant(value):
    if not isinstance(value, str):
        raise ValueError("missing telemetry timestamp")
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("telemetry timestamp requires timezone")
    return parsed.timestamp()


KCP_CORRELATION_COUNTERS = (
    "malformed_packets", "outbound_packets", "outbound_push_segments", "first_observed_push_segments",
    "repeated_push_segments", "unclassified_push_segments", "outbound_push_payload_bytes", "outbound_header_bytes",
    "outbound_ack_segments", "inbound_packets", "inbound_push_segments", "inbound_ack_segments",
    "incoming_una_advances", "matched_acks", "unmatched_acks", "ambiguous_acks", "incomplete_history_acks",
    "duplicate_acks", "ack_before_adapter_return", "slot_evictions", "attempt_evictions", "adapter_errors")
KCP_CORRELATION_DISTRIBUTIONS = ("application_write", "adapter_call", "entry_to_ack", "return_to_ack")


def verify_duration_distribution(value):
    require_counters(value, ("count", "sum_ns", "max_ns"), "KCP duration")
    buckets = value.get("buckets")
    if (not isinstance(buckets, list) or len(buckets) != 25 or
            any(type(count) is not int or count < 0 for count in buckets) or sum(buckets) != value["count"]):
        raise ValueError("KCP duration histogram bins disagree with count")
    if value["count"] == 0:
        if value["sum_ns"] != 0 or value["max_ns"] != 0:
            raise ValueError("empty KCP duration histogram has nonzero timing")
    elif not value["max_ns"] <= value["sum_ns"] <= value["count"] * value["max_ns"]:
        raise ValueError("KCP duration sum or maximum is inconsistent")
    else:
        micros = (value["max_ns"] + 999) // 1000
        maximum_bin = min(24, (micros - 1).bit_length() if micros > 1 else 0)
        if not buckets[maximum_bin] or any(buckets[maximum_bin + 1:]):
            raise ValueError("KCP duration maximum disagrees with histogram bins")


def verify_kcp_correlation(value, case):
    traces = value.get("kcp_correlation")
    if not case["diagnostics"] or case["protocol"] not in ("kcp", "kcp-mpudp"):
        if traces is not None and traces != []:
            raise ValueError("unexpected KCP correlation while diagnostics are disabled or protocol is not KCP")
        return
    if not isinstance(traces, list) or len(traces) != case["flows_per_worker"]:
        raise ValueError("missing per-flow KCP correlation diagnostics")
    packet_correlation = case["protocol"] == "kcp-mpudp"
    boundary = ("mpudp_datagram_adapter_call; not_individual_socket_write" if packet_correlation else
                "application_write_only; native_batch_socket_correlation_unavailable")
    for index, trace in enumerate(traces):
        require_counters(trace, ("flow", "slot_limit", "attempts_per_slot", *KCP_CORRELATION_COUNTERS), "KCP correlation")
        if (trace["flow"] != index or trace.get("packet_correlation_available") is not packet_correlation or
                trace.get("retransmit_reason_available") is not False or trace.get("boundary") != boundary or
                trace["slot_limit"] != (1024 if packet_correlation else 0) or
                trace["attempts_per_slot"] != (4 if packet_correlation else 0)):
            raise ValueError("KCP correlation flow, bounds or measurement boundary is incorrect")
        for name in KCP_CORRELATION_DISTRIBUTIONS:
            verify_duration_distribution(trace.get(name))
        classified_acks = sum(trace[key] for key in ("matched_acks", "unmatched_acks", "ambiguous_acks",
                                                    "incomplete_history_acks", "duplicate_acks"))
        if (classified_acks != trace["inbound_ack_segments"] or
                trace["entry_to_ack"]["count"] != trace["matched_acks"] or
                trace["return_to_ack"]["count"] + trace["ack_before_adapter_return"] > trace["matched_acks"]):
            raise ValueError("KCP correlation ACK classification or timing count is inconsistent")
        if sum(trace[key] for key in ("first_observed_push_segments", "repeated_push_segments",
                                     "unclassified_push_segments")) != trace["outbound_push_segments"]:
            raise ValueError("KCP correlation PUSH classification is inconsistent")
        if not packet_correlation and (any(trace[key] for key in KCP_CORRELATION_COUNTERS) or
                any(trace[key]["count"] for key in KCP_CORRELATION_DISTRIBUTIONS if key != "application_write")):
            raise ValueError("native KCP reports unavailable packet correlation observations")


def verify_telemetry(value, case, source_sha):
    if not isinstance(value, dict):
        raise ValueError("missing telemetry")
    instant(value.get("at_utc"))
    process = value.get("process")
    require_counters(process, ("max_rss_kib", "heap_alloc_bytes", "total_alloc_bytes", "mallocs", "gc_count", "goroutines"), "process")
    if any(not finite_nonnegative(process.get(key)) for key in ("cpu_user_seconds", "cpu_system_seconds")):
        raise ValueError("missing process CPU telemetry")
    require_counters(value, ("kcp_timeout_retransmits", "adapter_write_drops"), "transport")
    admission = value.get("mpudp_admission")
    require_counters(admission, ADMISSION_COUNTERS, "MPUDP admission")
    if case["protocol"] not in MPUDP and any(admission[key] for key in ADMISSION_COUNTERS):
        raise ValueError("native protocol unexpectedly reports MPUDP admission pressure")
    available = value.get("mpudp_statistics_available")
    if type(available) is not bool:
        raise ValueError("missing MPUDP statistics availability")
    if case["protocol"] in MPUDP and source_sha != calibrate.BASELINE_SHA and not available:
        raise ValueError("current MPUDP build is missing statistics")
    if case["protocol"] not in MPUDP and available:
        raise ValueError("native protocol unexpectedly reports MPUDP statistics")
    mpudp = value.get("mpudp")
    if isinstance(mpudp, dict) and "v2_receive" in mpudp:
        if (not available or case["protocol"] not in MPUDP or
                case.get("mpudp_profile", "v1") not in ("v2", "v2-aggregation")):
            raise ValueError("unexpected v2 receive statistics for this protocol/profile")
        require_counters(mpudp["v2_receive"], V2_RECEIVE_COUNTERS + V2_RECEIVE_GAUGES, "v2 receive")
    if available:
        require_counters(mpudp, ("ingress_accepted", "ingress_drops", "delivery_accepted", "delivery_drops",
                         "delivered_packets", "delivered_bytes", "sent_datagrams", "sent_datagram_bytes"), "MPUDP")
        if mpudp.get("diagnostics_enabled") is not case["diagnostics"]:
            raise ValueError("MPUDP telemetry diagnostics differs from requested mode")
        require_counters(mpudp.get("fec"), ("completed_blocks", "recovered_blocks", "recovered_shards", "expired_blocks",
                         "decoder_full", "late_shards", "duplicate_shards"), "FEC")
        if not isinstance(mpudp.get("paths"), list) or not mpudp["paths"]:
            raise ValueError("missing MPUDP path telemetry")
        for path in mpudp["paths"]:
            require_counters(path, ("sent_packets", "sent_bytes", "send_errors", "received_packets",
                             "received_bytes", "receive_oversize_drops"), "MPUDP path")
    if case["protocol"] in ("kcp", "kcp-mpudp"):
        snmp = value.get("kcp_snmp")
        require_counters(snmp, ("InSegs", "OutSegs", "RetransSegs", "FastRetransSegs", "EarlyRetransSegs", "LostSegs"), "KCP")
        sessions = value.get("kcp_sessions")
        if not isinstance(sessions, list) or len(sessions) != case["flows_per_worker"]:
            raise ValueError("missing per-flow KCP telemetry")
        for index, session in enumerate(sessions):
            require_counters(session, ("flow", "srtt_ms", "srtt_variation_ms", "rto_ms"), "KCP session")
            if session["flow"] != index:
                raise ValueError("KCP flow telemetry differs from requested flows")
        if value["kcp_timeout_retransmits"] != snmp["LostSegs"]:
            raise ValueError("KCP timeout telemetry disagrees with SNMP")
    verify_kcp_correlation(value, case)


def verify_mpudp_config(metadata, case, side, parameters):
    cfg = metadata.get("config")
    if case["protocol"] not in MPUDP:
        if cfg is not None:
            raise ValueError("native protocol unexpectedly reports MPUDP configuration")
        return
    profile = case.get("mpudp_profile", "v1")
    version = "v1" if profile == "v1" else "v2"
    if (not isinstance(cfg, dict) or cfg.get("mpudp_profile") != profile or
            cfg.get("wire_version") != version or cfg.get("protocol") != "datagram" or
            not isinstance(cfg.get("repair"), dict) or cfg["repair"].get("enabled") is not False):
        raise ValueError("MPUDP profile, wire protocol or repair differs from requested configuration")
    for section, expected in (
            ("fec", {"DataShards": parameters.data_shards, "ParityShards": parameters.parity_shards}),
            ("limits", {"MaxDatagramSize": parameters.max_datagram_size if version == "v1" else datagram_limit(parameters, profile),
                        "MaxPendingFECBlocks": parameters.pending_blocks,
                        "ReceiveQueueCapacity": parameters.queue_capacity,
                        "DeliveryQueueCapacity": parameters.queue_capacity}),
            ("transport", {"MaxUDPPayload": parameters.udp_budget}),
            ("udp_caps", {"send_hard_cap": parameters.udp_budget, "receive_hard_cap": parameters.udp_budget})):
        value = cfg.get(section)
        if not isinstance(value, dict) or any(type(value.get(key)) is not type(want) or value[key] != want
                                              for key, want in expected.items()):
            raise ValueError(f"MPUDP {section} differs from requested configuration")
    carriers = case["candidate_paths"] if side == "client" else 0
    if type(cfg.get("configured_carriers")) is not int or cfg["configured_carriers"] != carriers:
        raise ValueError("MPUDP configured Carrier count differs from requested role")
    aggregation = {"enabled": profile == "v2-aggregation"}
    scheduler = {"outbound_path_rates_bps": {}, "inbound_path_rates_bps": {}}
    if version == "v2":
        aggregation.update(max_delay_ns=parameters.v2_aggregation_max_delay_us * 1000,
                           max_records=parameters.v2_aggregation_max_records,
                           max_queued_datagrams=V2_QUEUE_DATAGRAMS, max_queued_bytes=V2_QUEUE_BYTES,
                           max_group_bytes=v2_group_bytes(parameters))
        key = "outbound_path_rates_bps" if side == "client" else "inbound_path_rates_bps"
        scheduler[key] = {str(path_id): parameters.v2_path_rate_bps
                          for path_id in range(1, case["candidate_paths"] + 1)}
        scheduler["default_path_rate_bps"] = 100000000
        transport = cfg["transport"]
        if (transport.get("MTUDiscovery") != "fixed" or transport.get("BudgetStrategy") != "session" or
                type(transport.get("MaxReceiveUDPPayload")) is not int or
                transport["MaxReceiveUDPPayload"] != parameters.udp_budget or
                type(cfg["limits"].get("MaxFragmentsPerDatagram")) is not int or
                cfg["limits"]["MaxFragmentsPerDatagram"] != V2_MAX_FRAGMENTS):
            raise ValueError("MPUDP v2 transport or fragment policy differs from requested configuration")
    for section, expected in (("aggregation", aggregation), ("scheduler", scheduler)):
        # JSON representation also distinguishes booleans from integer values.
        if json.dumps(cfg.get(section), sort_keys=True) != json.dumps(expected, sort_keys=True):
            raise ValueError(f"MPUDP {section} differs from requested configuration")


def verify_latency(value, case):
    fields = ("sent", "scheduled", "submitted", "queue_missed", "write_failed", "received", "unanswered",
              "on_time", "deadline_missed", "deadline_ms", "over_10000_ms", "resolution_ms")
    require_counters(value, fields, "RTT")
    scheduled = case["seconds"] * 5 * case["flows_per_worker"]
    if (value["scheduled"] != scheduled or value["sent"] != value["submitted"] or
            not value["on_time"] <= value["received"] <= value["submitted"] <= scheduled or
            value["submitted"] + value["queue_missed"] + value["write_failed"] > scheduled or
            value["unanswered"] != scheduled - value["received"] or
            value["deadline_missed"] != scheduled - value["on_time"] or
            value["over_10000_ms"] > value["received"] or value["deadline_ms"] != 1000 or value["resolution_ms"] != 1):
        raise ValueError("RTT opportunity accounting is inconsistent")
    previous = 0
    for field, rank in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99)):
        if field not in value:
            raise ValueError("missing RTT quantile")
        quantile = value[field]
        missing = math.ceil(scheduled * rank) > value["received"] - value["over_10000_ms"]
        if (quantile is None) != missing or (quantile is not None and
                (not finite_nonnegative(quantile) or not previous <= quantile <= 10000)):
            raise ValueError("RTT quantile excludes missing opportunities or is invalid")
        if quantile is not None:
            previous = quantile


def receiver_overlap(pairs, case):
    starts = [instant(pair["receiver"]["started_utc"]) + case["warmup_seconds"] for pair in pairs]
    skew = max(starts) - min(starts)
    # At most one second and five percent of a window may be non-overlapping.
    allowed = min(1.0, case["seconds"] * .05)
    if skew > allowed + .000001:
        raise ValueError("native receiver windows do not have near-complete overlap")
    return int(max(starts) * 1e9), int((min(starts) + case["seconds"]) * 1e9), skew


def verify_pair(client_path, server_path, case, worker_id, source_sha, parameters):
    sides = {"client": records(client_path), "server": records(server_path)}
    return verify_pair_records(sides, case, worker_id, source_sha, parameters)


def verify_pair_records(sides, case, worker_id, source_sha, parameters):
    summaries = {}
    receiver_side = "server" if case["direction"] == "upload" else "client"
    for side, rows in sides.items():
        metadata = unique_record(rows, "metadata")
        summary = unique_record(rows, "summary")
        if metadata.get("source_sha") != source_sha or metadata.get("side") != side:
            raise ValueError("remote executable source SHA or side mismatch")
        expected = {"run_id": worker_id, "protocol": case["protocol"], "direction": case["direction"],
                    "flows": case["flows_per_worker"], "seconds": case["seconds"],
                    "warmup_seconds": case["warmup_seconds"], "message_bytes": case["message_bytes"]}
        if any(type(summary.get(k)) is not type(value) or summary[k] != value or
               type(metadata.get("options", {}).get(k)) is not type(value) or metadata["options"][k] != value
               for k, value in expected.items()):
            raise ValueError("probe output does not match requested measurement")
        settings = {"diagnostics": case["diagnostics"], "kcp_mtu": parameters.kcp_mtu,
                    "kcp_window": parameters.kcp_window, "kcp_ack_no_delay": parameters.ack_no_delay,
                    "offered_mbps_per_flow": parameters.rate_mbps}
        options = metadata.get("options", {})
        if any(type(options.get(k)) is not type(v) or options[k] != v for k, v in settings.items() if k != "offered_mbps_per_flow") or \
                not finite_nonnegative(options.get("offered_mbps_per_flow")) or options["offered_mbps_per_flow"] != parameters.rate_mbps:
            raise ValueError("probe options differ from requested diagnostics, KCP or offered rate")
        role = "receiver" if side == receiver_side else "sender"
        if summary.get("role") != role or summary.get("side") != side:
            raise ValueError("sender accounting cannot substitute receiver evidence")
        expected_paths = case["candidate_paths"] if case["protocol"] in MPUDP else 1
        if metadata.get("path_count") != expected_paths or summary.get("path_count") != expected_paths:
            raise ValueError("actual configured path count does not match matrix")
        if json.dumps(metadata.get("admission_policy"), sort_keys=True) != json.dumps(ADMISSION_POLICY, sort_keys=True):
            raise ValueError("MPUDP admission retry policy differs from bounded probe policy")
        verify_mpudp_config(metadata, case, side, parameters)
        drain = summary.get("local_drain")
        require_counters(drain, ("supported_sessions", "completed_sessions", "failed_sessions", "duration_ns"), "local drain")
        expected_supported = case["flows_per_worker"] if case["protocol"] in MPUDP and case.get("mpudp_profile", "v1") != "v1" else 0
        if (drain.get("scope") != LOCAL_DRAIN_SCOPE or drain["supported_sessions"] != expected_supported or
                drain["completed_sessions"] != expected_supported or drain["failed_sessions"] != 0):
            raise ValueError("MPUDP local drain is incomplete or has the wrong completion boundary")
        instant(summary.get("started_utc"))
        require_counters(summary, ("verified_bytes", "verified_packets", "send_errors", "read_errors", "corrupt_frames", "duplicate_frames", "too_old_frames"), "summary")
        for key in ("initial", "final"):
            verify_telemetry(summary.get(key), case, source_sha)
        for row in rows:
            if row.get("type") == "sample":
                verify_telemetry(row.get("telemetry"), case, source_sha)
        summaries[side] = summary
    for side, opposite in (("client", "server"), ("server", "client")):
        if unique_record(sides[side], "remote_summary").get("summary") != summaries[opposite]:
            raise ValueError("exchanged remote summary differs from receiver/sender log")
    receiver = summaries[receiver_side]
    samples = [r for r in sides[receiver_side] if r.get("type") == "sample" and r.get("role") == "receiver"]
    if len(samples) != case["seconds"] + case["warmup_seconds"]:
        raise ValueError("receiver per-second evidence is incomplete")
    for index, sample in enumerate(samples):
        second = index + 1 - case["warmup_seconds"]
        if sample.get("side") != receiver_side or sample.get("second") != second or sample.get("steady") is not (second > 0):
            raise ValueError("receiver sample order or steady-state flag mismatch")
        require_counters(sample, ("verified_bytes", "verified_packets", "corrupt_frames", "duplicate_frames", "too_old_frames"), "receiver sample")
        if sample["verified_bytes"] != sample["verified_packets"] * (case["message_bytes"] - 40):
            raise ValueError("verified bytes include headers or unverified payload")
        if not finite_nonnegative(sample.get("mbps")) or not math.isclose(sample["mbps"], sample["verified_bytes"] * 8 / 1e6, rel_tol=1e-9, abs_tol=1e-12):
            raise ValueError("receiver sample Mbps disagrees with verified bytes")
    steady = samples[case["warmup_seconds"]:]
    verified = sum(r["verified_bytes"] for r in steady)
    mbps = verified * 8 / case["seconds"] / 1e6
    if receiver.get("verified_bytes") != verified or not finite_nonnegative(receiver.get("mbps")) or not math.isclose(receiver["mbps"], mbps, rel_tol=1e-9, abs_tol=1e-12):
        raise ValueError("receiver summary disagrees with per-second verified bytes")
    if receiver.get("verified_packets") != sum(row["verified_packets"] for row in steady):
        raise ValueError("receiver packet count disagrees with per-second evidence")
    for field in ("corrupt_frames", "duplicate_frames", "too_old_frames"):
        if receiver[field] != sum(row[field] for row in steady):
            raise ValueError("receiver integrity summary disagrees with per-second evidence")
    compact_fields = ("second", "steady", "mbps", "verified_bytes", "verified_packets", "corrupt_frames", "duplicate_frames", "too_old_frames")
    if receiver.get("samples") != [{key: row[key] for key in compact_fields} for row in samples]:
        raise ValueError("receiver compact samples disagree with per-second evidence")
    worst = min((sum(row["verified_bytes"] for row in steady[i:i + 5]) * 8 / 5e6 for i in range(len(steady) - 4)), default=None)
    observed = receiver.get("worst_5_second_mbps")
    if "worst_5_second_mbps" not in receiver or (worst is None and observed is not None) or (worst is not None and
            (not finite_nonnegative(observed) or not math.isclose(observed, worst, rel_tol=1e-9, abs_tol=1e-12))):
        raise ValueError("receiver worst five-second throughput disagrees with samples")
    verify_latency(receiver.get("echo_rtt"), case)
    return {"worker_id": worker_id, "receiver_side": receiver_side, "receiver": receiver,
            "sender": summaries["client" if receiver_side == "server" else "server"]}


class ProbeRunner:
    def __init__(self, args, topology, binary_digest, secret):
        self.args, self.topology, self.binary_digest, self.secret = args, topology, binary_digest, secret
        self.lab = calibrate.Lab(args, topology)
        self.remote_dirs = {}
        self.cleanup_verified = True
        self.control_address = args.control_address

    def remote(self, host, command, data=None, timeout=30):
        result = subprocess.run(self.lab.ssh(host, command), input=data, capture_output=True, timeout=timeout)
        if result.returncode:
            # Commands can consume private configuration on stdin. Never echo it.
            raise RuntimeError(f"remote command failed on {host}: status {result.returncode}")
        return result.stdout

    def python(self, host, script, *args, data=None, timeout=30):
        return self.remote(host, [self.lab.python(host), "-c", script, *args], data, timeout)

    def prepare(self):
        self.lab.connect()
        for host in (self.topology["client"], self.topology["server"]):
            raw = self.python(host, "import tempfile; print(tempfile.mkdtemp(prefix='mpudp-perf-',dir='/tmp'))")
            directory = raw.decode().strip()
            if not re.fullmatch(r"/tmp/mpudp-perf-[a-zA-Z0-9_-]+", directory):
                raise ValueError("remote workspace did not have expected private prefix")
            self.remote_dirs[host] = directory
            digest = self.python(host,
                "import hashlib,os,sys; p=sys.argv[1]; b=sys.stdin.buffer.read(); "
                "f=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o700); "
                "s=os.fdopen(f,'wb'); s.write(b); s.close(); "
                "f=open(p,'rb'); print(hashlib.file_digest(f,'sha256').hexdigest()); f.close()",
                directory + "/perfprobe", data=self.args.binary.read_bytes(), timeout=60).decode().strip()
            if digest != self.binary_digest:
                raise ValueError("remote executable SHA256 differs from local binary")
        if self.control_address is None:
            output = calibrate.run(["ssh", "-F", str(self.args.ssh_config), "-G", self.topology["server"]])
            hostnames = [line.split(maxsplit=1)[1] for line in output.splitlines() if line.startswith("hostname ")]
            if len(hostnames) != 1:
                raise ValueError("cannot resolve control address from SSH configuration")
            self.control_address = str(ipaddress.IPv4Address(hostnames[0]))

    def deploy_config(self, host, name, config):
        encoded = mpudp_config_bytes(config)
        extension = ".yaml" if config.get("wire", {}).get("version") == "v2" else ".json"
        path = self.remote_dirs[host] + "/" + name + extension
        self.python(host, "import os,sys; f=os.open(sys.argv[1],os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); "
                    "s=os.fdopen(f,'wb'); s.write(sys.stdin.buffer.read()); s.close()", path,
                    data=encoded)
        return path

    def stop_unit(self, host, unit):
        script = ("import json,subprocess,sys; u=sys.argv[1]; "
                  "subprocess.run(['systemctl','stop',u],capture_output=True,timeout=15); "
                  "r=subprocess.run(['systemctl','show',u,'-p','ActiveState','-p','MainPID'],capture_output=True,text=True,timeout=5); "
                  "v=dict(x.split('=',1) for x in r.stdout.splitlines() if '=' in x); "
                  "print(json.dumps({'state':v.get('ActiveState'),'main_pid':v.get('MainPID'),"
                  "'stopped':r.returncode in (0,4) and v.get('ActiveState') in ('inactive','failed') and v.get('MainPID')=='0'}))")
        try:
            return {"host": host, "unit": unit, **json.loads(self.python(host, script, unit, timeout=25))}
        except (OSError, RuntimeError, ValueError, TypeError, subprocess.TimeoutExpired) as error:
            return {"host": host, "unit": unit, "stopped": False, "error": type(error).__name__}

    def finish(self):
        results = []
        try:
            for host, directory in self.remote_dirs.items():
                removed = False
                if self.cleanup_verified:
                    try:
                        self.python(host, "import shutil,sys; shutil.rmtree(sys.argv[1])", directory)
                        removed = True
                    except (OSError, RuntimeError, subprocess.TimeoutExpired):
                        pass
                results.append({"host": host, "workspace": directory, "removed": removed})
            save(self.args.output / "workspace-cleanup.json", results)
        finally:
            self.lab.disconnect()
        if any(not item["removed"] for item in results):
            raise RuntimeError("remote workspace cleanup could not be verified")

    def command(self, case, side, index, config_path, profile_prefix):
        host = self.topology[side]
        address_index = index if case["layout"] == "parallel" else 0
        address = self.topology["server_addresses"][address_index]
        command = [self.remote_dirs[host] + "/perfprobe", "-mode", side, "-protocol", case["protocol"],
                   "-control", f"{self.control_address}:{self.args.control_port + index}",
                   "-address", f"{address}:{self.args.data_port + index * 64}",
                   "-id", case["case_id"] + f"-w{index}", "-direction", case["direction"],
                   "-flows", case["flows_per_worker"], "-seconds", case["seconds"],
                   "-warmup", case["warmup_seconds"], "-payload", case["message_bytes"],
                   "-kcp-mtu", self.args.kcp_mtu, "-kcp-window", self.args.kcp_window,
                   "-rate-mbps", self.args.rate_mbps]
        if config_path:
            command += ["-config", config_path]
        if case["diagnostics"]:
            command += ["-diagnostics"]
        if self.args.ack_no_delay:
            command += ["-ack-no-delay"]
        if profile_prefix:
            command += ["-profile-prefix", profile_prefix]
        return command

    def case(self, case):
        output = self.args.output / case["case_id"]
        output.mkdir(mode=0o700)
        save(output / "case.json", case)
        duration = case["warmup_seconds"] + case["seconds"] + 90
        token = secrets.token_hex(8)
        units, processes, handles, monitors, servers, clients, profiles, configs = [], [], [], [], [], [], [], []
        hosts = [self.topology["client"], self.topology["server"], *self.topology["routers"], self.topology["hypervisor"]]

        def start(host, label, command, name, data=None):
            unit = f"mpudp-probe-{token}-{label}"
            units.append((host, unit))
            stdout = (output / (name + ".jsonl")).open("wb")
            stderr = (output / (name + ".stderr")).open("wb")
            handles.extend((stdout, stderr))
            command = ["systemd-run", "--quiet", "--pipe", "--wait", "--collect", "--unit", unit,
                       "--property", f"RuntimeMaxSec={duration + 15}", "--property", "KillMode=control-group",
                       "--property", "TimeoutStopSec=3", "--setenv", "PATH=" + REMOTE_PATH, "--", *command]
            process = subprocess.Popen(self.lab.ssh(host, command), stdout=stdout, stderr=stderr,
                                       stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL)
            processes.append(process)
            if data is not None:
                try:
                    process.stdin.write(data)
                finally:
                    process.stdin.close()
            return process

        try:
            required_ports = {self.args.control_port + i for i in range(case["workers"])}
            required_ports.update(self.args.data_port + i * 64 + flow for i in range(case["workers"]) for flow in range(case["flows_per_worker"]))
            for flag in ("-ltn", "-lun"):
                occupied = calibrate.listening_endpoints(self.remote(self.topology["server"], ["ss", "-H", flag]).decode())
                if any(int(endpoint.rsplit(":", 1)[1]) in required_ports for endpoint in occupied):
                    raise RuntimeError("probe control or data port is already occupied")
            for host in hosts:
                name = "host-" + host
                monitor = start(host, "sampler", [self.lab.python(host), "-", duration, self.args.host_diagnostics],
                                name, Path(__file__).with_name("sample-host.py").read_bytes())
                monitors.append(monitor)
                calibrate.wait_sampler_ready(monitor, output / (name + ".jsonl"))
            for index in range(case["workers"]):
                for side in ("server", "client"):
                    host = self.topology[side]
                    config_path = None
                    if case["protocol"] in MPUDP:
                        config_path = self.deploy_config(host, token + "-" + side,
                                                        mpudp_config(self.args, self.topology, case, side, self.secret))
                        configs.append((host, config_path))
                    prefix = self.remote_dirs[host] + f"/{token}-{side}-{index}" if self.args.profiles else None
                    if prefix:
                        profiles.append((host, prefix, f"{side}-{index}"))
                    command = self.command(case, side, index, config_path, prefix)
                    if side == "server":
                        server = start(host, f"server{index}", command, f"server-{index}")
                        servers.append(server)
                        until = time.monotonic() + 20
                        while True:
                            listening = calibrate.listening_endpoints(self.remote(host, ["ss", "-H", "-ltn"]).decode())
                            if server.poll() is not None:
                                raise RuntimeError("probe control listener exited during startup")
                            if f"{self.control_address}:{self.args.control_port + index}" in listening:
                                break
                            if time.monotonic() > until:
                                raise RuntimeError("probe control listener failed during startup")
                            time.sleep(.1)
                    else:
                        clients.append((host, command, index))
            # All listeners and samplers are ready before concurrent business load.
            clients = [start(host, f"client{index}", command, f"client-{index}") for host, command, index in clients]
            until = time.monotonic() + duration
            for process in [*clients, *servers]:
                if process.wait(timeout=max(1, until - time.monotonic())):
                    raise RuntimeError("probe failed; see bounded per-process logs")
            if any(monitor.poll() is not None for monitor in monitors):
                raise RuntimeError("host sampler stopped before measurement completed")
            time.sleep(1.1)
            # SIGUSR1 requests a final network snapshot and normal sampler exit.
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(lambda item: self.remote(item[0],
                    ["systemctl", "kill", "--signal=SIGUSR1", "--kill-whom=main", item[1]]), units[:len(hosts)]))
            for monitor in monitors:
                if monitor.wait(timeout=30):
                    raise RuntimeError("host sampler final snapshot failed")
        finally:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                cleanup = list(executor.map(lambda item: self.stop_unit(*item), units))
            save(output / "cleanup.json", cleanup)
            self.cleanup_verified = self.cleanup_verified and all(row["stopped"] for row in cleanup)
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for handle in handles:
                handle.close()
            if not self.cleanup_verified:
                raise RuntimeError("owned remote process cleanup could not be verified")
        pairs = [verify_pair(output / f"client-{i}.jsonl", output / f"server-{i}.jsonl", case,
                             case["case_id"] + f"-w{i}", self.args.source_sha, self.args) for i in range(case["workers"])]
        start_ns, end_ns, skew = receiver_overlap(pairs, case)
        headroom = {host: calibrate.host_headroom(output / f"host-{host}.jsonl", start_ns, end_ns) for host in hosts}
        network = {host: calibrate.verify_network_snapshots(output / f"host-{host}.jsonl", start_ns, end_ns) for host in hosts}
        if case["formal_window"] and any(row["sample_intervals"] < max(1, int((end_ns - start_ns) / 1e9) - 2) for row in headroom.values()):
            raise ValueError("host steady-state sampling is incomplete")
        for host, prefix, name in profiles:
            (output / ".lab").mkdir(exist_ok=True, mode=0o700)
            private = output / ".lab" / "profiles"
            private.mkdir(exist_ok=True, mode=0o700)
            for suffix in PROFILE_SUFFIXES:
                data = self.python(host, "from pathlib import Path; import sys; p=Path(sys.argv[1]); "
                                   "assert p.stat().st_size<=256*1024*1024; sys.stdout.buffer.write(p.read_bytes())",
                                   prefix + "." + suffix + ".pprof", timeout=60)
                path = private / (name + "." + suffix + ".pprof")
                path.write_bytes(data)
                path.chmod(0o600)
                self.python(host, "from pathlib import Path; import sys; Path(sys.argv[1]).unlink()",
                            prefix + "." + suffix + ".pprof")
        for host, path in configs:
            self.python(host, "from pathlib import Path; import sys; Path(sys.argv[1]).unlink()", path)
        summary = {"case": case, "pairs": pairs, "aggregate_receiver_mbps": sum(p["receiver"]["mbps"] for p in pairs),
                   "overlap_seconds": (end_ns - start_ns) / 1e9, "hosts": headroom,
                   "receiver_start_skew_seconds": skew,
                   "network_snapshots": network, "product_acceptance": False}
        save(output / "summary.json", summary)
        print(json.dumps({"case_id": case["case_id"], "aggregate_receiver_mbps": summary["aggregate_receiver_mbps"],
                          "single_flow": case["single_flow"], "product_acceptance": False}), flush=True)
        return summary


def checksums(output, secret):
    rows = []
    for path in sorted(output.rglob("*")):
        if not path.is_file() or ".lab" in path.relative_to(output).parts or path.name == "SHA256SUMS":
            continue
        if secret:
            needle, previous = secret.encode(), b""
            with path.open("rb") as stream:
                while block := stream.read(1024 * 1024):
                    block = previous + block
                    if needle in block:
                        raise ValueError("private PSK detected in shareable artifacts")
                    previous = block[-len(needle):]
        rows.append(f"{sha256(path)}  {path.relative_to(output)}")
    (output / "SHA256SUMS").write_text("\n".join(rows) + "\n")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--ssh-config", type=Path, required=True)
    parser.add_argument("--hypervisor-python", default="python3")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--binary-sha256")
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--psk-file", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plan", action="store_true", help="write matrix and manifest without SSH or load")
    parser.add_argument("--paths", type=int, nargs="+", choices=(1, 2, 3, 5), default=[1, 2, 3, 5])
    parser.add_argument("--protocols", nargs="+", choices=PROTOCOLS, default=list(PROTOCOLS))
    parser.add_argument("--mpudp-profiles", nargs="+", choices=MPUDP_PROFILES, default=["v1"],
                        help="MPUDP wire/aggregation profiles; native protocols are not duplicated")
    parser.add_argument("--directions", nargs="+", choices=("upload", "download"), default=["upload", "download"])
    parser.add_argument("--payloads", nargs="+", choices=("64", "1200", "1400", "max"), default=["64", "1200", "1400", "max"])
    parser.add_argument("--flows", type=int, nargs="+", default=[1])
    parser.add_argument("--diagnostics", nargs="+", choices=("off", "on"), default=["off"])
    parser.add_argument("--host-diagnostics", choices=("basic", "full"), default="full")
    parser.add_argument("--profiles", action="store_true")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=300)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--control-address")
    parser.add_argument("--control-port", type=int, default=28900)
    parser.add_argument("--data-port", type=int, default=29000)
    parser.add_argument("--physical-mtu", type=int, default=1500)
    parser.add_argument("--udp-budget", type=int, default=1200)
    parser.add_argument("--data-shards", type=int, default=3)
    parser.add_argument("--parity-shards", type=int, default=2)
    parser.add_argument("--max-datagram-size", type=int, default=65536)
    parser.add_argument("--v2-max-original-bytes", type=int, default=65536,
                        help="additional v2 original Datagram ceiling (64..1048576)")
    parser.add_argument("--v2-path-rate-bps", type=int, default=100000000,
                        help="explicit configured rate for each selected v2 path; not a measured rate")
    parser.add_argument("--v2-aggregation-max-delay-us", type=int, default=250)
    parser.add_argument("--v2-aggregation-max-records", type=int, default=32)
    parser.add_argument("--stream-max-payload", type=int, default=65536)
    parser.add_argument("--queue-capacity", type=int, default=4096)
    parser.add_argument("--pending-blocks", type=int, default=8192)
    parser.add_argument("--kcp-mtu", type=int, default=1400)
    parser.add_argument("--kcp-window", type=int, default=1024)
    parser.add_argument("--ack-no-delay", action="store_true")
    parser.add_argument("--rate-mbps", type=float, default=0)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", args.source_sha):
        parser.error("--source-sha must be a full lowercase Git SHA")
    if args.binary_sha256 and not re.fullmatch(r"[0-9a-f]{64}", args.binary_sha256):
        parser.error("--binary-sha256 must be a lowercase SHA256")
    if not 1 <= args.rounds <= 20 or not 1 <= args.seconds <= 3600 or not 0 <= args.warmup <= 300:
        parser.error("rounds, seconds or warmup outside bounded range")
    if not args.flows or any(not 1 <= flows <= 64 for flows in args.flows):
        parser.error("each flow count must be 1..64")
    if not 1 <= args.data_shards <= 255 or not 1 <= args.parity_shards <= 255 or args.data_shards + args.parity_shards > 256:
        parser.error("FEC shard counts outside GF(2^8) range")
    if not 92 <= args.physical_mtu <= 65535 or not 72 <= args.udp_budget <= args.physical_mtu - 28:
        parser.error("UDP budget must fit the stated IPv4 physical MTU")
    if not 64 <= args.max_datagram_size <= 16777216 or not 64 <= args.stream_max_payload <= 16777216 or not 1 <= args.queue_capacity <= 65536 or not 1 <= args.pending_blocks <= 65536:
        parser.error("message size or queue capacity outside bounded range")
    if (not 64 <= args.v2_max_original_bytes <= V2_QUEUE_BYTES or
            not 1000 <= args.v2_path_rate_bps <= 1000000000000 or
            not 1 <= args.v2_aggregation_max_delay_us <= 10000 or
            not 1 <= args.v2_aggregation_max_records <= 256):
        parser.error("v2 original, path rate or aggregation setting outside bounded range")
    if len(set(args.mpudp_profiles)) != len(args.mpudp_profiles):
        parser.error("MPUDP profiles must not contain duplicates")
    if any(protocol in MPUDP for protocol in args.protocols) and any(profile != "v1" for profile in args.mpudp_profiles):
        if args.udp_budget < 512:
            parser.error("v2 requires UDP send and receive budgets of at least 512 bytes")
        if args.source_sha == calibrate.BASELINE_SHA:
            parser.error("the original product baseline supports only the v1 MPUDP profile")
    if not 64 <= args.kcp_mtu <= min(1500, args.physical_mtu - 28) or not 1 <= args.kcp_window <= 65536:
        parser.error("KCP settings outside implementation/path bounds")
    if not math.isfinite(args.rate_mbps) or not 0 <= args.rate_mbps <= 1000000:
        parser.error("offered rate must be finite and within 0..1000000 Mbit/s")
    control_ports = set(range(args.control_port, args.control_port + 5))
    data_ports = set(range(args.data_port, args.data_port + 5 * 64))
    if min(control_ports | data_ports) < 1024 or max(control_ports | data_ports) > 65535 or control_ports & data_ports:
        parser.error("control and data ranges must be separate unprivileged ports")
    if args.control_address:
        args.control_address = str(ipaddress.IPv4Address(args.control_address))
    return args


def main(argv=None):
    args = parse_args(argv)
    topology = calibrate.topology(args.topology)
    cases = matrix(args)
    for case in cases:
        if case.get("wire_version") == "v2":
            for side in ("client", "server"):
                mpudp_config_bytes(mpudp_config(args, topology, case, side, "plan-only-redacted-key"))
    secret = read_secret(args.psk_file) if any(p in MPUDP for p in args.protocols) and not args.plan else None
    digest = sha256(args.binary)
    if args.binary_sha256 and digest != args.binary_sha256:
        raise ValueError("local executable SHA256 differs from --binary-sha256")
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    parameters = {key: value for key, value in vars(args).items() if not isinstance(value, Path) and key != "binary_sha256"}
    save(args.output / "manifest.json", {"schema": 1, "kind": "receiver-verified-probe-matrix",
        "source_sha": args.source_sha, "baseline_sha": calibrate.BASELINE_SHA, "binary_sha256": digest,
        "run_id": args.output.name, "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "topology": topology, "parameters": parameters, "data_shard_overhead": DATA_SHARD_OVERHEAD,
        "data_shard_overhead_by_profile": {"v1": DATA_SHARD_OVERHEAD,
            "v2": V2_DATA_SHARD_OVERHEAD, "v2-aggregation": V2_DATA_SHARD_OVERHEAD},
        "v2_fragment_manifest_overhead": V2_MANIFEST_OVERHEAD,
        "v2_aggregation_queue": {"max_datagrams": V2_QUEUE_DATAGRAMS, "max_bytes": V2_QUEUE_BYTES},
        "admission_policy": ADMISSION_POLICY, "local_drain_scope": LOCAL_DRAIN_SCOPE,
        "runner_source_sha": calibrate.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
        "runner_dirty": bool(calibrate.run(["git", "-C", str(ROOT), "status", "--porcelain"])),
        "runner_sha256": {path.name: sha256(path) for path in sorted(Path(__file__).parent.glob("*.py"))}})
    save(args.output / "matrix.json", cases)
    plan_summary = {"type": "plan_summary", "total_cases": len(cases),
                    "minimum_wall_seconds": len(cases) * (args.warmup + args.seconds),
                    "excludes_setup_collection_cleanup": True, "product_acceptance": False}
    save(args.output / "plan-summary.json", plan_summary)
    print(json.dumps(plan_summary), flush=True)
    if args.plan:
        checksums(args.output, secret)
        return
    runner = ProbeRunner(args, topology, digest, secret)
    measured = False
    try:
        runner.prepare()
        for case in cases:
            runner.case(case)
        measured = True
    except BaseException as error:
        save(args.output / "failure.json", {"error": type(error).__name__, "completed": False})
        raise
    finally:
        try:
            runner.finish()
            if measured:
                save(args.output / "completed.json", {"completed_cases": len(cases), "source_sha": args.source_sha,
                     "binary_sha256": digest, "cleanup_verified": True, "product_acceptance": False})
        except BaseException as error:
            save(args.output / "failure.json", {"error": type(error).__name__, "completed": False})
            raise
        finally:
            checksums(args.output, secret)


if __name__ == "__main__":
    def terminate(_signal, _frame):
        raise KeyboardInterrupt("probe matrix terminated")
    signal.signal(signal.SIGTERM, terminate)
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"probe matrix failed: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)
