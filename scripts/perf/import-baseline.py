#!/usr/bin/env python3
"""Export allowlisted v0.1 measurements from the exact original lab commit."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import tempfile

LAB_COMMIT = "9480e5551612d6541f6aaf0d3f9b36ad63f4fcc3"
MPUDP_COMMIT = "934a6325f25be3be0c587d5eab57bd6a8380e7e9"
RUNS = {"20260905T044234Z-healthy-download": "download",
        "20260905T044358Z-healthy-upload": "upload"}
DEPENDENCIES = {"github.com/mofelee/mpudp": "v0.1.0",
                "github.com/xtaci/kcp-go/v5": "v5.6.72"}
KCP_COUNTERS = (
    "BytesSent", "BytesReceived", "MaxConn", "ActiveOpens", "PassiveOpens", "CurrEstab",
    "InErrs", "InCsumErrors", "KCPInErrors", "InPkts", "OutPkts", "InSegs", "OutSegs",
    "InBytes", "OutBytes", "RetransSegs", "FastRetransSegs", "EarlyRetransSegs", "LostSegs",
    "RepeatSegs", "FECFullShardSet", "FECRecovered", "FECErrs", "FECParityShards",
    "FECShardSet", "FECShardMin", "RingBufferSndQueue", "RingBufferRcvQueue",
    "RingBufferSndBuffer", "OOBPackets")


def numeric(value, integer=False, minimum=0):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("measurement must be numeric")
    if not math.isfinite(value) or value < minimum or integer and not isinstance(value, int):
        raise ValueError("invalid numeric measurement")
    return value


def boolean(value):
    if not isinstance(value, bool):
        raise ValueError("measurement must be boolean")
    return value


def digest(value, length=64):
    if not isinstance(value, str) or not re.fullmatch(f"[0-9a-f]{{{length}}}", value):
        raise ValueError("invalid source digest")
    return value


def extract_result(result):
    counts = ("seconds", "warmup_seconds", "verified_bytes", "corrupt_bytes",
              "echo_sent", "echo_ok", "adapter_write_drops")
    rates = ("mbps", "echo_success", "echo_p50_ms", "echo_p95_ms", "echo_p99_ms",
             "max_echo_success_gap_s")
    selected = {key: numeric(result[key], integer=True) for key in counts}
    selected.update({key: numeric(result[key]) for key in rates})
    selected["ack_no_delay"] = boolean(result["ack_no_delay"])
    if result["kcp_fec"] != [0, 0]:
        raise ValueError("original KCP FEC configuration changed")
    selected["kcp_fec"] = [0, 0]
    for key, bound in (("samples", 3600), ("echo", 18000), ("errors", 18000)):
        if not isinstance(result[key], list) or len(result[key]) > bound:
            raise ValueError("measurement array outside export bound")
    selected["error_count"] = len(result["errors"])
    selected["samples"] = [{"second": numeric(row["second"], True, -300),
                            "bytes": numeric(row["bytes"], True),
                            "mbps": numeric(row["mbps"])} for row in result["samples"]]
    selected["echo"] = [{"seq": numeric(row["seq"], True),
                         "scheduled_s": numeric(row["scheduled_s"]),
                         "latency_ms": numeric(row["latency_ms"], minimum=-1),
                         "ok": boolean(row["ok"])} for row in result["echo"]]
    selected["kcp_snmp"] = {key: numeric(result["kcp_snmp"][key], True) for key in KCP_COUNTERS}
    return selected


def original_file(source, relative):
    return subprocess.run(["git", "-C", str(source), "show", f"{LAB_COMMIT}:{relative}"],
                          check=True, capture_output=True, timeout=30).stdout


def verify_dependencies(module):
    with tempfile.TemporaryDirectory(prefix="mpudp-baseline-mod-") as directory:
        path = Path(directory) / "go.mod"
        path.write_bytes(module)
        result = subprocess.run(["go", "mod", "edit", "-json", str(path)],
                                check=True, capture_output=True, timeout=30)
    parsed = json.loads(result.stdout)
    versions = {item["Path"]: item["Version"] for item in parsed["Require"]}
    if any(versions.get(name) != version for name, version in DEPENDENCIES.items()):
        raise ValueError("baseline dependency versions changed")


def export(source):
    hashes = {}

    def load(relative):
        content = original_file(source, relative)
        hashes[relative] = hashlib.sha256(content).hexdigest()
        return content

    verify_dependencies(load("go.mod"))
    load("go.sum")
    load("REPORT.md")
    load("SHA256SUMS")
    runs = []
    for run_id, direction in RUNS.items():
        base = f"artifacts/{run_id}"
        result = json.loads(load(f"{base}/result.json"))
        provenance = json.loads(load(f"{base}/provenance.json"))
        load(f"{base}/scenario.json")
        if provenance["mpudp_commit"] != MPUDP_COMMIT:
            raise ValueError("baseline source commit changed")
        for name in ("go.mod", "go.sum"):
            if provenance["source_sha256"][name] != hashes[name]:
                raise ValueError("dependency source digest mismatch")
        runs.append({"run_id": run_id, "direction": direction,
                     "binary_sha256": digest(provenance["binary_sha256"]),
                     "debianform_commit": digest(provenance["debianform_commit"], 40),
                     "source_sha256": {name: digest(provenance["source_sha256"][name])
                                       for name in ("go.mod", "go.sum", "cmd/labprobe/main.go",
                                                    "lab.dbf.hcl", "hosts.generated.dbf.hcl",
                                                    "scripts/shape.sh", "scripts/lab.py")},
                     "measurements": extract_result(result)})
    return {"schema": 1, "kind": "original-v0.1-numeric-baseline",
            "source_repository": "https://github.com/mofelee/mpudp-test",
            "source_visibility": "private", "source_commit": LAB_COMMIT,
            "mpudp_commit": MPUDP_COMMIT, "dependencies": DEPENDENCIES,
            "configuration": {"paths": 5, "mpudp_fec": [3, 2], "kcp_fec": [0, 0],
                              "mpudp_udp_payload_budget": 1200, "kcp_mtu": 1400,
                              "kcp_window": 1024, "kcp_nodelay": [1, 10, 2, 1],
                              "ack_no_delay": False, "independent_business_sessions": 2,
                              "rate_mbit_per_path_per_direction": 100,
                              "delay_ms_per_path_per_direction": 20},
            "limitations": ["Original measurements are 60 seconds with 5 seconds of warmup; not formal performance acceptance.",
                            "Bulk and echo use separate MPUDP/KCP sessions sharing the five paths.",
                            "KCP counters cover the client process, including warmup and final drain; server sender counters are absent.",
                            "Download counts verified client receive bytes; upload counts server-verified bytes acknowledged to the client.",
                            "Reported retransmission categories do not identify real loss versus queue delay or premature timeout.",
                            "Underlying five-path simultaneous capacity and host headroom were not established by this baseline.",
                            "Raw originals require private repository access; this artifact exports only allowlisted numeric and boolean measurements."],
            "raw_file_sha256": hashes, "runs": runs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = export(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
