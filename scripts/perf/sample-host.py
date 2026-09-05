#!/usr/bin/env python3
"""Bounded, payload-free Linux host samples, suitable for streaming over SSH."""
import json
from pathlib import Path
import subprocess
import sys
import time


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


def sample(seconds, full):
    print(json.dumps({"kind": "host", "uname": command(["uname", "-a"]),
                      "cpu": command(["lscpu", "-J"]),
                      "links": command(["ip", "-j", "-s", "link"]),
                      "routes": command(["ip", "-j", "route"])}), flush=True)
    start = time.monotonic()
    for index in range(seconds + 1):
        record = {"kind": "sample", "index": index, "unix_ns": time.time_ns(),
                  "elapsed": time.monotonic() - start}
        for name in ("stat", "meminfo", "vmstat", "softirqs", "net/snmp", "net/netstat",
                     "pressure/cpu", "pressure/memory", "pressure/io"):
            record[name] = read("/proc/" + name)
        if full or index in (0, seconds):
            record["qdisc"] = command(["tc", "-j", "-s", "qdisc", "show"])
            record["classes"] = {dev.name: command(["tc", "-j", "-s", "class", "show", "dev", dev.name])
                                 for dev in Path("/sys/class/net").iterdir()}
            record["sockets"] = command(["ss", "-u", "-a", "-n", "-m"])
            record["links"] = command(["ip", "-j", "-s", "link"])
        print(json.dumps(record), flush=True)
        time.sleep(max(0, start + index + 1 - time.monotonic()))


if __name__ == "__main__":
    sample(int(sys.argv[1]), len(sys.argv) > 2 and sys.argv[2] == "full")
