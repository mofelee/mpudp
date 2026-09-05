#!/usr/bin/env python3
"""Concurrent native capacity calibration over an existing five-path SSH lab."""
import argparse
import concurrent.futures
import datetime
import hashlib
import ipaddress
import json
import math
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
BASELINE_SHA = "934a6325f25be3be0c587d5eab57bd6a8380e7e9"


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def run(args, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"command {args[0]} exited {result.returncode}: {result.stderr[-1000:]}")
    return result.stdout.strip()


def topology(path):
    data = json.loads(path.read_text())
    fields = {"client", "server", "hypervisor", "routers", "server_addresses",
              "rate_mbit_per_path", "one_way_delay_ms", "metering_layer"}
    if set(data) != fields:
        raise ValueError("topology fields must match topology.example.json")
    hosts = [data["client"], data["server"], data["hypervisor"], *data["routers"]]
    if len(data["routers"]) != 5 or len(set(hosts)) != 8:
        raise ValueError("expected separate client, server, hypervisor and five routers")
    if any(not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", h) for h in hosts):
        raise ValueError("hosts must be plain SSH aliases")
    addresses = data["server_addresses"]
    if len(addresses) != 5 or len(set(addresses)) != 5:
        raise ValueError("expected five different server IPv4 addresses")
    for address in addresses:
        ipaddress.IPv4Address(address)
    if data["rate_mbit_per_path"] != 100 or data["one_way_delay_ms"] != 20:
        raise ValueError("reference calibration requires 100 Mbit/s and 20 ms per direction")
    return data


def received(result):
    if result.get("error"):
        raise ValueError("iperf reported an error")
    end = result["end"]
    # Newer iperf versions provide sum_received for UDP too. Older ones put
    # the receiving summary in sum; sender values must never substitute it.
    summary = end.get("sum_received")
    if summary is None:
        summary = end.get("sum", {})
        if summary.get("sender") is not False:
            raise ValueError("missing receiver summary")
    if summary.get("sender") is True:
        raise ValueError("summary is from the sender")
    for key in ("bytes", "seconds", "bits_per_second"):
        value = summary[key]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise ValueError("invalid receiver accounting")
    if summary["seconds"] == 0 or not isinstance(summary["bytes"], int):
        raise ValueError("invalid receiver interval or byte count")
    return {"bytes": summary["bytes"], "seconds": summary["seconds"],
            "mbit_s": summary["bits_per_second"] / 1e6,
            "lost_packets": summary.get("lost_packets"),
            "packets": summary.get("packets"),
            "jitter_ms": summary.get("jitter_ms")}


def receiver_result(result, direction):
    if result.get("error"):
        raise ValueError("iperf reported an error")
    if direction == "upload":
        if "server_output_json" not in result:
            raise ValueError("missing server receiver report")
        return result["server_output_json"]
    if direction != "download":
        raise ValueError("unknown direction")
    return result


def listening_endpoints(output):
    return {fields[3] for line in output.splitlines() if len(fields := line.split()) >= 5}


def wait_sampler_ready(process, path, timeout=20):
    deadline = time.monotonic() + timeout
    while True:
        rows = [json.loads(line) for line in path.read_bytes().splitlines(keepends=True) if line.endswith(b"\n")]
        if process.poll() is not None:
            raise RuntimeError("host sampler exited before load")
        if any(row.get("kind") == "network_snapshot" and row.get("phase") == "before" for row in rows):
            return
        if time.monotonic() > deadline:
            raise RuntimeError("host sampler network snapshot failed to become ready")
        time.sleep(.1)


def verify_network_snapshots(path, start_ns=None, end_ns=None):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    result = {}
    for phase in ("before", "after"):
        snapshots = [row for row in rows if row.get("kind") == "network_snapshot" and row.get("phase") == phase]
        if len(snapshots) != 1:
            raise ValueError("host network before/after snapshot is incomplete")
        snapshot = snapshots[0]
        commands = [snapshot.get(key, {}) for key in ("qdisc", "sockets", "links")]
        classes = snapshot.get("classes")
        if not isinstance(classes, dict) or not classes:
            raise ValueError("host network class snapshot is incomplete")
        commands.extend(classes.values())
        if any(command.get("status") != 0 or not isinstance(command.get("stdout"), str) for command in commands):
            raise ValueError("host network snapshot command failed")
        for command in [snapshot["qdisc"], snapshot["links"], *classes.values()]:
            if not isinstance(json.loads(command["stdout"]), list):
                raise ValueError("host network snapshot JSON is invalid")
        started, finished = snapshot.get("started_unix_ns"), snapshot.get("finished_unix_ns")
        if type(started) is not int or type(finished) is not int or not 0 <= started <= finished:
            raise ValueError("host network snapshot interval is invalid")
        result[phase + "_started_unix_ns"] = started
        result[phase + "_finished_unix_ns"] = finished
    if result["before_finished_unix_ns"] > result["after_started_unix_ns"]:
        raise ValueError("host network snapshots are out of order")
    if (start_ns is not None and result["before_finished_unix_ns"] > start_ns) or \
            (end_ns is not None and result["after_started_unix_ns"] < end_ns):
        raise ValueError("host network snapshots do not bracket the measurement")
    return result


def host_headroom(path, start_ns=None, end_ns=None):
    samples = [json.loads(line) for line in path.read_text().splitlines()]
    samples = [s for s in samples if s.get("kind") == "sample"]
    idle, swap, total_idle, total_ticks = [], 0, 0, 0
    for previous, current in zip(samples, samples[1:]):
        if start_ns is not None and previous["unix_ns"] < start_ns:
            continue
        if end_ns is not None and current["unix_ns"] > end_ns:
            continue
        before = list(map(int, previous["stat"].splitlines()[0].split()[1:9]))
        after = list(map(int, current["stat"].splitlines()[0].split()[1:9]))
        delta = [a - b for a, b in zip(after, before)]
        total = sum(delta)
        if total > 0:
            idle.append(100 * delta[3] / total)
            total_idle += delta[3]
            total_ticks += total
        a = dict(line.split() for line in previous["vmstat"].splitlines())
        b = dict(line.split() for line in current["vmstat"].splitlines())
        swap += sum(max(0, int(b[k]) - int(a[k])) for k in ("pswpin", "pswpout"))
    return {"mean_idle_percent": 100 * total_idle / total_ticks if total_ticks else None,
            "min_idle_percent": min(idle) if idle else None,
            "swap_pages": swap, "sample_intervals": len(idle)}


class Lab:
    def __init__(self, args, config):
        self.args, self.config = args, config
        self.control_dir = None

    def connect(self):
        self.control_dir = tempfile.TemporaryDirectory(prefix="mpudp-perf-ssh-")
        for host in [self.config["client"], self.config["server"], *self.config["routers"], self.config["hypervisor"]]:
            run(self.ssh(host, ["true"]))

    def disconnect(self):
        if self.control_dir is None:
            return
        for host in [self.config["client"], self.config["server"], *self.config["routers"], self.config["hypervisor"]]:
            command = self.ssh(host, ["true"])
            subprocess.run([*command[:-2], "-O", "exit", host], capture_output=True, timeout=15)
        self.control_dir.cleanup()

    def ssh(self, host, command):
        args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
        if host != self.config["hypervisor"]:
            args += ["-F", str(self.args.ssh_config)]
        if self.control_dir is not None:
            args += ["-o", "ControlMaster=auto", "-o", "ControlPersist=60",
                     "-o", "ControlPath=" + str(Path(self.control_dir.name) / host)]
        return [*args, host, shlex.join(list(map(str, command)))]

    def python(self, host):
        if host == self.config["hypervisor"]:
            return getattr(self.args, "hypervisor_python", "python3")
        return "python3"

    def case(self, output, protocol, paths, direction, round_number):
        output.mkdir()
        seconds, warmup = self.args.seconds, self.args.warmup
        duration = seconds + warmup + 15
        config = self.config
        save(output / "case.json", {"protocol": protocol, "paths": paths,
             "direction": direction, "round": round_number, "seconds": seconds,
             "warmup": warmup, "flows_per_path": 1, "concurrent_native_flows": paths,
             "is_mpudp_single_flow": False, "udp_payload": 1400,
             "udp_offered_mbit_per_path": 100, "diagnostics": self.args.diagnostics})
        processes, handles, monitors, servers, units = [], [], [], [], []
        token = secrets.token_hex(8)

        def owned(host, label, command):
            unit = f"mpudp-perf-{token}-{label}"
            units.append((host, unit))
            return self.ssh(host, ["systemd-run", "--quiet", "--pipe", "--wait", "--collect",
                                  "--unit", unit, "--property", f"RuntimeMaxSec={duration + 10}",
                                  "--property", "KillMode=control-group", "--property", "TimeoutStopSec=3",
                                  "--setenv", "PATH=/run/current-system/sw/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                                  "--", *command])

        try:
            occupied = listening_endpoints(run(self.ssh(config["server"], ["ss", "-H", "-ltn"])))
            reserved_ports = {str(15201 + i) for i in range(paths)}
            if any(endpoint.rsplit(":", 1)[-1] in reserved_ports for endpoint in occupied):
                raise RuntimeError("calibration port already occupied")
            for host in [config["client"], config["server"], *config["routers"], config["hypervisor"]]:
                handle = (output / f"host-{host}.jsonl").open("w")
                errors = (output / f"host-{host}.stderr").open("w")
                handles.extend([handle, errors])
                process = subprocess.Popen(owned(host, "sampler", [self.python(host), "-", duration, self.args.diagnostics]),
                                           stdin=subprocess.PIPE, stdout=handle, stderr=errors)
                processes.append(process)
                monitors.append(process)
                process.stdin.write((ROOT / "scripts/perf/sample-host.py").read_bytes())
                process.stdin.close()
                wait_sampler_ready(process, output / f"host-{host}.jsonl")
            for index, address in enumerate(config["server_addresses"][:paths]):
                port = 15201 + index
                handle = (output / f"server-{index + 1}.json").open("w")
                errors = (output / f"server-{index + 1}.stderr").open("w")
                handles.extend([handle, errors])
                command = ["timeout", duration, "iperf3", "-s", "-1", "-B", address,
                           "-p", port, "-J", "--forceflush"]
                process = subprocess.Popen(owned(config["server"], f"server{index}", command), stdout=handle, stderr=errors)
                processes.append(process)
                servers.append(process)
            deadline = time.monotonic() + 10
            while True:
                listening = listening_endpoints(run(self.ssh(config["server"], ["ss", "-H", "-ltn"])))
                if any(p.poll() is not None for p in servers):
                    raise RuntimeError("calibration listener exited during startup")
                if all(f"{address}:{15201 + index}" in listening
                       for index, address in enumerate(config["server_addresses"][:paths])):
                    break
                if any(p.poll() is not None for p in servers) or time.monotonic() >= deadline:
                    raise RuntimeError("calibration listeners did not become ready")
                time.sleep(.2)

            def client(index):
                command = ["iperf3", "-c", config["server_addresses"][index], "-p", 15201 + index,
                           "-t", seconds, "-O", warmup, "-i", 1, "-J", "--get-server-output"]
                if protocol == "udp":
                    command += ["-u", "-b", "100M", "-l", 1400, "--dont-fragment"]
                if direction == "download":
                    command += ["-R"]
                started = time.time_ns()
                raw = run(owned(config["client"], f"client{index}", command), timeout=duration)
                result = json.loads(raw)
                save(output / f"client-{index + 1}.json", result)
                return {"path": index + 1, "started_unix_ns": started,
                        "finished_unix_ns": time.time_ns(), **received(receiver_result(result, direction))}

            with concurrent.futures.ThreadPoolExecutor(max_workers=paths) as executor:
                rows = list(executor.map(client, range(paths)))
            for process in servers:
                if process.wait(timeout=10):
                    raise RuntimeError("iperf server failed")
            # The monitor is deliberately bounded and includes recovery after load.
            for process in monitors:
                if process.wait(timeout=duration + 5):
                    raise RuntimeError("host sampler failed")
            for handle in handles:
                handle.flush()
            # Restrict to the overlap after every client has warmed up. SSH
            # startup uncertainty is excluded with one additional second.
            start_ns = max(r["started_unix_ns"] for r in rows) + (warmup + 1) * 1_000_000_000
            end_ns = min(r["started_unix_ns"] for r in rows) + (warmup + seconds) * 1_000_000_000
            network = {host: verify_network_snapshots(output / f"host-{host}.jsonl", start_ns, end_ns)
                       for host in [config["client"], config["server"], *config["routers"], config["hypervisor"]]}
            headroom = host_headroom(output / f"host-{config['hypervisor']}.jsonl", start_ns, end_ns)
            summary = {"paths": rows, "aggregate_receiver_mbit_s": sum(r["mbit_s"] for r in rows),
                       "hypervisor": headroom, "network_snapshots": network,
                       "capacity_90_percent_each": all(r["mbit_s"] >= 90 for r in rows),
                       "formal_window": seconds >= 300 and self.args.rounds >= 3,
                       "product_acceptance": False}
            save(output / "summary.json", summary)
            print(json.dumps({"case": output.name, **summary}), flush=True)
            return summary
        finally:
            def stop_owned(entry):
                host, unit = entry
                script = ("import json,subprocess,sys; u=sys.argv[1]; "
                          "subprocess.run(['systemctl','stop',u],capture_output=True,timeout=10); "
                          "r=subprocess.run(['systemctl','show',u,'-p','ActiveState','--value'],"
                          "capture_output=True,text=True,timeout=5); "
                          "s=r.stdout.strip(); print(json.dumps({'state':s,'status':r.returncode,"
                          "'stopped':r.returncode in (0,4) and s in ('inactive','failed')}))")
                try:
                    result = json.loads(run(self.ssh(host, [self.python(host), "-c", script, unit]), timeout=20))
                    return {"host": host, "unit": unit, **result}
                except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
                    return {"host": host, "unit": unit, "stopped": False, "error": type(error).__name__}
            grouped = {}
            for entry in units:
                grouped.setdefault(entry[0], []).append(entry)
            def stop_host(entries):
                return [stop_owned(entry) for entry in entries]
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                cleanup = [entry for group in executor.map(stop_host, grouped.values()) for entry in group]
            save(output / "cleanup.json", cleanup)
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for handle in handles:
                handle.close()
            if any(not entry["stopped"] for entry in cleanup):
                raise RuntimeError("remote cleanup could not be verified; see cleanup.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--ssh-config", type=Path, required=True)
    parser.add_argument("--hypervisor-python", default="python3")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--paths", type=int, nargs="+", choices=(1, 2, 3, 5), default=[1, 2, 3, 5])
    parser.add_argument("--protocols", nargs="+", choices=("tcp", "udp"), default=["tcp", "udp"])
    parser.add_argument("--directions", nargs="+", choices=("upload", "download"), default=["upload", "download"])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=300)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--diagnostics", choices=("basic", "full"), default="basic")
    args = parser.parse_args()
    if not 1 <= args.rounds <= 20 or not 1 <= args.seconds <= 3600 or not 0 <= args.warmup <= 300:
        parser.error("rounds, seconds, or warmup outside bounded range")
    config = topology(args.topology)
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {"schema": 1, "kind": "native-capacity-calibration", "baseline_sha": BASELINE_SHA,
                "source_sha": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
                "dirty": bool(run(["git", "-C", str(ROOT), "status", "--porcelain"])),
                "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "run_id": args.output.name, "topology": config,
                "parameters": {k: v for k, v in vars(args).items() if not isinstance(v, Path)},
                "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in sorted((ROOT / "scripts/perf").glob("*.py"))}}
    save(args.output / "manifest.json", metadata)
    lab = Lab(args, config)
    try:
        lab.connect()
        versions = {host: run(lab.ssh(host, ["iperf3", "--version"]))
                    for host in (config["client"], config["server"])}
        save(args.output / "versions.json", versions)
        for protocol in args.protocols:
            for paths in args.paths:
                for direction in args.directions:
                    for repeat in range(1, args.rounds + 1):
                        name = f"{protocol}-{paths}path-{direction}-r{repeat}"
                        print(f"Starting {name}", file=sys.stderr, flush=True)
                        lab.case(args.output / name, protocol, paths, direction, repeat)
    finally:
        lab.disconnect()
        lines = [f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(args.output)}"
                 for p in sorted(args.output.rglob("*")) if p.is_file() and p.name != "SHA256SUMS"]
        (args.output / "SHA256SUMS").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
