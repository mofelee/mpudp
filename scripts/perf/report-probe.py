#!/usr/bin/env python3
"""Revalidate a completed probe matrix and report sampled steady-window costs."""

import argparse
import bisect
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
from types import SimpleNamespace


spec = importlib.util.spec_from_file_location("probe_runner", Path(__file__).with_name("run-probe.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

CPU = ("cpu_user_seconds", "cpu_system_seconds")
ALLOC = ("total_alloc_bytes", "mallocs", "gc_count")
SOCKET = ("sent_packets", "sent_bytes", "send_errors", "received_packets", "received_bytes",
          "receive_oversize_drops")


def delta(before, after, names):
    result = {}
    for key in names:
        a, b = before.get(key), after.get(key)
        if not runner.finite_nonnegative(a) or not runner.finite_nonnegative(b) or b < a:
            raise ValueError(f"missing or regressing counter: {key}")
        result[key] = b - a
    return result


def samples(rows, role):
    selected = [row for row in rows if row.get("type") == "sample" and row.get("role") == role]
    stamps = [runner.instant(row["telemetry"]["at_utc"]) for row in selected]
    if any(b <= a for a, b in zip(stamps, stamps[1:])):
        raise ValueError("sample timestamps are not strictly increasing")
    return selected, stamps


def aligned_samples(receiver_rows, sender_rows, receiver, tolerance):
    if not math.isfinite(tolerance) or not 0 <= tolerance <= .5:
        raise ValueError("alignment tolerance must be between zero and 0.5 seconds")
    receive, _ = samples(receiver_rows, "receiver")
    send, stamps = samples(sender_rows, "sender")
    nominal_start = runner.instant(receiver["started_utc"]) + receiver["warmup_seconds"]
    matched = []
    for row in receive:
        second = row["second"]
        if not 0 <= second <= receiver["seconds"] or not stamps:
            continue
        at = runner.instant(row["telemetry"]["at_utc"])
        if abs(at - (nominal_start + second)) > tolerance:
            continue
        index = bisect.bisect_left(stamps, at)
        candidates = [i for i in (index - 1, index) if 0 <= i < len(stamps)]
        index = min(candidates, key=lambda i: abs(stamps[i] - at))
        if abs(stamps[index] - at) <= tolerance:
            matched.append((row, send[index]))
    if len(matched) < 2 or matched[0][1] is matched[-1][1]:
        raise ValueError("fewer than two aligned steady-window sample boundaries")
    first, last = matched[0], matched[-1]
    # Whole receiver buckets between selected boundaries are the business-byte authority.
    buckets = [row for row in receive if first[0]["second"] < row["second"] <= last[0]["second"]]
    if len(buckets) != last[0]["second"] - first[0]["second"]:
        raise ValueError("selected receiver bucket interval is incomplete")
    return first, last, buckets


def side_cost(first, last):
    a, b = first["telemetry"], last["telemetry"]
    elapsed = runner.instant(b["at_utc"]) - runner.instant(a["at_utc"])
    if elapsed <= 0:
        raise ValueError("nonpositive telemetry interval")
    cpu = delta(a["process"], b["process"], CPU)
    allocation = delta(a["process"], b["process"], ALLOC)
    result = {"start_utc": a["at_utc"], "end_utc": b["at_utc"], "seconds": elapsed,
              "cpu_core_percent": sum(cpu.values()) / elapsed * 100,
              "allocation_bytes_per_second": allocation["total_alloc_bytes"] / elapsed,
              "mallocs_per_second": allocation["mallocs"] / elapsed,
              "gc_count": allocation["gc_count"], "end_heap_bytes": b["process"]["heap_alloc_bytes"],
              "process_lifetime_max_rss_kib": b["process"]["max_rss_kib"], "socket": None}
    if "mpudp_admission" in a or "mpudp_admission" in b:
        result["mpudp_admission"] = delta(a.get("mpudp_admission", {}), b.get("mpudp_admission", {}),
                                            runner.ADMISSION_COUNTERS)
    if a.get("mpudp_statistics_available") != b.get("mpudp_statistics_available"):
        raise ValueError("MPUDP statistics availability changed inside interval")
    if a.get("mpudp_statistics_available"):
        paths = []
        maps = []
        for value in (a, b):
            entries = value["mpudp"]["paths"]
            by_name = {entry["path"]: entry for entry in entries}
            if len(by_name) != len(entries):
                raise ValueError("duplicate raw socket metric identity")
            maps.append(by_name)
        if maps[0].keys() != maps[1].keys():
            raise ValueError("raw socket metric identities changed inside interval")
        for name in maps[0]:
            change = delta(maps[0][name], maps[1][name], SOCKET)
            paths.append({"path": name, **change, "sent_pps": change["sent_packets"] / elapsed,
                          "received_pps": change["received_packets"] / elapsed})
        total = {key: sum(path[key] for path in paths) for key in SOCKET}
        result["socket"] = {**total, "paths": paths, "sent_pps": total["sent_packets"] / elapsed,
                            "received_pps": total["received_packets"] / elapsed,
                            "sent_udp_mbps": total["sent_bytes"] * 8 / elapsed / 1e6,
                            "estimated_sent_ipv4_l3_bytes": total["sent_bytes"] + 28 * total["sent_packets"],
                            "estimated_sent_ipv4_l3_mbps":
                                (total["sent_bytes"] + 28 * total["sent_packets"]) * 8 / elapsed / 1e6}
    return result


def worker_cost(receiver_rows, sender_rows, pair, tolerance=.25):
    first, last, buckets = aligned_samples(receiver_rows, sender_rows, pair["receiver"], tolerance)
    verified = sum(row["verified_bytes"] for row in buckets)
    originals = sum(row["verified_packets"] for row in buckets)
    receiver, sender = side_cost(first[0], last[0]), side_cost(first[1], last[1])
    result = {"worker_id": pair["worker_id"], "receiver_side": pair["receiver_side"],
              "first_bucket": buckets[0]["second"], "last_bucket": buckets[-1]["second"],
              "bucket_seconds": len(buckets), "verified_bytes": verified, "verified_packets": originals,
              "receiver_verified_mbps": verified * 8 / len(buckets) / 1e6,
              "max_boundary_skew_seconds": max(abs(runner.instant(r["telemetry"]["at_utc"]) -
                                                       runner.instant(s["telemetry"]["at_utc"]))
                                                for r, s in (first, last)),
              "receiver": receiver, "sender": sender,
              "approximate_bidirectional_ipv4_l3_bytes_per_verified_byte": None,
              "approximate_forward_socket_packets_per_verified_original": None}
    if receiver["socket"] is not None and sender["socket"] is not None:
        # Count each endpoint's sends once; received bytes would double-count traffic.
        cost = sum(side["socket"]["estimated_sent_ipv4_l3_bytes"] for side in (receiver, sender))
        if verified:
            result["approximate_bidirectional_ipv4_l3_bytes_per_verified_byte"] = cost / verified
        if originals:
            result["approximate_forward_socket_packets_per_verified_original"] = sender["socket"]["sent_packets"] / originals
    return result


def checked_reader(root):
    index = {}
    for line in (root / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match or match[2] in index:
            raise ValueError("invalid or duplicate checksum entry")
        index[match[2]] = match[1]

    def read(relative, jsonl=False):
        path = root / relative
        if not path.resolve().is_relative_to(root.resolve()) or ".lab" in Path(relative).parts:
            raise ValueError("report input is outside the public artifact tree")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != index.get(relative):
            raise ValueError(f"missing or mismatched artifact checksum: {relative}")
        return [json.loads(line) for line in raw.splitlines() if line.strip()] if jsonl else json.loads(raw)
    return read


def report(root, tolerance=.25):
    read = checked_reader(root)
    manifest, cases, completed = read("manifest.json"), read("matrix.json"), read("completed.json")
    if (not isinstance(manifest.get("source_sha"), str) or
            not re.fullmatch(r"[0-9a-f]{40}", manifest["source_sha"]) or
            not isinstance(manifest.get("binary_sha256"), str) or
            not re.fullmatch(r"[0-9a-f]{64}", manifest["binary_sha256"])):
        raise ValueError("invalid source or executable hash")
    if not cases or len({case["case_id"] for case in cases}) != len(cases):
        raise ValueError("matrix cases must be nonempty and unique")
    if (completed.get("completed_cases") != len(cases) or completed.get("cleanup_verified") is not True or
            any(completed.get(key) != manifest.get(key) for key in ("source_sha", "binary_sha256"))):
        raise ValueError("matrix completion, cleanup or executable identity does not match manifest")
    result = {"schema": 1, "source_sha": manifest["source_sha"], "binary_sha256": manifest["binary_sha256"],
              "product_acceptance": False, "alignment_tolerance_seconds": tolerance,
              "measurement_boundary": "per-second telemetry endpoints near receiver bucket boundaries; no interpolation",
              "clock_assumption": "endpoint wall clocks synchronized within the stated alignment tolerance",
              "unavailable": ["exact padding/control/FEC byte split", "native socket PPS",
                              "wire retransmission causes", "physical shaper accounting confirmation"], "cases": []}
    for case in cases:
        name = case["case_id"]
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name) or read(name + "/case.json") != case:
            raise ValueError("case identity differs from matrix")
        workers, pairs = [], []
        for i in range(case["workers"]):
            paths = {side: f"{name}/{side}-{i}.jsonl" for side in ("client", "server")}
            rows = {side: read(path, jsonl=True) for side, path in paths.items()}
            pair = runner.verify_pair_records(rows, case, name + f"-w{i}", manifest["source_sha"],
                                              SimpleNamespace(**manifest["parameters"]))
            receiver = pair["receiver_side"]
            workers.append(worker_cost(rows[receiver], rows["client" if receiver == "server" else "server"], pair, tolerance))
            pairs.append(pair)
        runner.receiver_overlap(pairs, case)
        result["cases"].append({"case": case, "aggregate_receiver_mbps": sum(p["receiver"]["mbps"] for p in pairs),
                                "receiver_results": [{"worst_5_second_mbps": p["receiver"]["worst_5_second_mbps"],
                                    "echo_rtt": p["receiver"]["echo_rtt"],
                                    **{k: p["receiver"][k] for k in ("corrupt_frames", "duplicate_frames", "too_old_frames")}}
                                                     for p in pairs], "workers": workers})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--alignment-tolerance-seconds", type=float, default=.25)
    args = parser.parse_args()
    print(json.dumps(report(args.directory, args.alignment_tolerance_seconds), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
