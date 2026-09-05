#!/usr/bin/env python3
"""Bounded, payload-free Linux host samples, suitable for streaming over SSH."""
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

stop_requested = False


def request_stop(_signal, _frame):
    global stop_requested
    stop_requested = True


def read(path):
    try:
        return Path(path).read_text()
    except OSError:
        return None


def command(args):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=3)
        return {"status": result.returncode, "stdout": result.stdout[:262144]}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"error": type(error).__name__}


def process_counters():
    rows = []
    for path in Path("/proc").glob("[0-9]*/stat"):
        raw = read(path)
        if raw is None:
            continue
        prefix, _, rest = raw.rpartition(")")
        name = prefix.partition("(")[2]
        if "qemu-system" not in name and name not in ("iperf3", "perfprobe", "labprobe"):
            continue
        fields = rest.split()
        if len(fields) < 22:
            continue
        rows.append({"pid": int(path.parent.name), "comm": name,
                     "user_ticks": int(fields[11]), "system_ticks": int(fields[12]),
                     "threads": int(fields[17]), "start_ticks": int(fields[19]),
                     "rss_pages": int(fields[21])})
        if len(rows) == 1024:
            break
    return rows


def network_counters():
    return {"qdisc": command(["tc", "-j", "-s", "qdisc", "show"]),
            "classes": {dev.name: command(["tc", "-j", "-s", "class", "show", "dev", dev.name])
                        for dev in Path("/sys/class/net").iterdir()},
            "sockets": command(["ss", "-u", "-a", "-n", "-m"]),
            "links": command(["ip", "-j", "-s", "link"])}


def network_snapshot(phase):
    record = {"kind": "network_snapshot", "phase": phase, "started_unix_ns": time.time_ns()}
    record.update(network_counters())
    record["finished_unix_ns"] = time.time_ns()
    print(json.dumps(record), flush=True)


def sample(seconds, full):
    print(json.dumps({"kind": "host", "uname": command(["uname", "-a"]),
                      "cpu": command(["lscpu", "-J"]),
                      "links": command(["ip", "-j", "-s", "link"]),
                      "routes": command(["ip", "-j", "route"])}), flush=True)
    network_snapshot("before")
    start = time.monotonic()
    for index in range(seconds + 1):
        if stop_requested:
            break
        record = {"kind": "sample", "index": index, "unix_ns": time.time_ns(),
                  "elapsed": time.monotonic() - start, "processes": process_counters()}
        for name in ("stat", "meminfo", "vmstat", "softirqs", "net/snmp", "net/netstat",
                     "pressure/cpu", "pressure/memory", "pressure/io"):
            record[name] = read("/proc/" + name)
        if full:
            record.update(network_counters())
        print(json.dumps(record), flush=True)
        time.sleep(max(0, start + index + 1 - time.monotonic()))
    network_snapshot("after")


if __name__ == "__main__":
    signal.signal(signal.SIGUSR1, request_stop)
    sample(int(sys.argv[1]), len(sys.argv) > 2 and sys.argv[2] == "full")
