#!/usr/bin/env python3
"""Count a ready-barrier RX fixture's receive syscalls in private tracefs.

Run only after the shared CPU hold is released. The fixture must emit one
ready JSON line containing receiver_pid, receiver_fd and sender_pid, then
wait for a newline on stdin after warmup. Counts cover measurement and teardown.
This helper has not yet been exercised on the target kernel.
"""

import argparse
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time
import uuid


SYSCALLS = ("recvfrom", "recvmsg", "recvmmsg")


def write_control(path, value):
    with path.open("w", encoding="ascii") as handle:
        handle.write(value + "\n")


def task_states(pid):
    result = {}
    for task in (Path("/proc") / str(pid) / "task").iterdir():
        try:
            status = (task / "status").read_text()
        except FileNotFoundError:
            continue
        state = re.search(r"^State:\s+(\S)", status, re.MULTILINE)
        if state is None:
            raise RuntimeError("receiver task lacks State")
        result[int(task.name)] = state.group(1)
    return result


def stop_receiver(pid):
    os.kill(pid, signal.SIGSTOP)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        states = task_states(pid)
        if states and all(state in ("T", "t") for state in states.values()):
            return sorted(states)
        time.sleep(0.01)
    raise RuntimeError("receiver threads did not stop within 3 seconds")


def read_until(proc, timeout, first_line=False):
    deadline = time.monotonic() + timeout
    output = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(proc.stdout, selectors.EVENT_READ)
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("fixture output timed out")
            if not selector.select(min(left, 1)):
                continue
            part = os.read(proc.stdout.fileno(), 65536)
            if not part:
                if first_line:
                    raise RuntimeError("fixture exited before ready JSON")
                return bytes(output)
            output.extend(part)
            if len(output) > 1048576:
                raise RuntimeError("fixture stdout exceeded 1 MiB")
            if first_line and b"\n" in output:
                line, extra = bytes(output).split(b"\n", 1)
                if extra:
                    raise RuntimeError("fixture wrote data past its ready barrier")
                return line


def parse_hist(raw, fields):
    rows = []
    for line in raw.splitlines():
        if not line.lstrip().startswith("{"):
            continue
        row = {}
        for name in (*fields, "hitcount"):
            match = re.search(r"\b" + name + r":\s*(-?\d+)", line)
            if match is None:
                raise RuntimeError("unrecognized histogram row: " + line)
            row[name] = int(match.group(1))
        rows.append(row)
    drops = re.findall(r"\bDropped:\s*(\d+)", raw)
    totals = re.findall(r"\bHits:\s*(\d+)", raw)
    if len(drops) != 1 or len(totals) != 1:
        raise RuntimeError("histogram lacks a unique Hits/Dropped summary")
    if int(drops[0]):
        raise RuntimeError("histogram dropped entries")
    if sum(row["hitcount"] for row in rows) != int(totals[0]):
        raise RuntimeError("histogram rows disagree with Hits total")
    return {"rows": rows, "hits": int(totals[0]), "dropped": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--trace-root", default="/sys/kernel/tracing")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command or not 0 < args.timeout <= 3600:
        parser.error("command and timeout in (0, 3600] are required")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    instance = Path(args.trace_root) / "instances" / (
        "mpudp-rx-" + str(os.getpid()) + "-" + uuid.uuid4().hex[:8]
    )
    proc = None
    receiver_pid = None
    receiver_stopped = False
    created = False
    triggers = []
    errors = []
    report = {
        "command": command,
        "kernel": os.uname().release,
        "scope": "receiver-only syscall entries from ready release through exit",
        "includes": ["timed measurement", "teardown", "EAGAIN retries"],
        "excludes": ["warmup before ready barrier", "sender syscalls"],
        "histogram_size_per_event": 4096,
        "instance": str(instance),
        "valid": False,
    }
    try:
        instance.mkdir()
        created = True
        write_control(instance / "tracing_on", "0")
        write_control(instance / "current_tracer", "nop")
        with (output / "fixture.stderr").open("wb") as stderr:
            proc = subprocess.Popen(
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=stderr, start_new_session=True, bufsize=0,
            )
            ready_raw = read_until(proc, min(args.timeout, 20), first_line=True)
            (output / "fixture.ready.json").write_bytes(ready_raw + b"\n")
            ready = json.loads(ready_raw)
            receiver_pid = int(ready["receiver_pid"])
            receiver_fd = int(ready["receiver_fd"])
            sender_pid = int(ready["sender_pid"])
            if receiver_pid <= 1 or receiver_fd < 0 or receiver_pid == sender_pid:
                raise RuntimeError("invalid receiver/sender ready metadata")
            if os.getpgid(receiver_pid) != proc.pid:
                raise RuntimeError("receiver is outside the fixture process group")
            receiver_stopped = True
            tids = stop_receiver(receiver_pid)
            report["ready"] = ready
            report["seed_receiver_tids"] = tids
            report["receiver_socket"] = os.readlink(
                Path("/proc") / str(receiver_pid) / "fd" / str(receiver_fd)
            )
            if not report["receiver_socket"].startswith("socket:["):
                raise RuntimeError("declared receiver FD is not a socket")
            write_control(instance / "options" / "event-fork", "1")
            write_control(instance / "set_event_pid", " ".join(map(str, tids)))
            for name in SYSCALLS:
                for phase, fields in (("enter", "common_pid,fd"),
                                      ("exit", "common_pid,ret")):
                    event = "sys_" + phase + "_" + name
                    path = instance / "events" / "syscalls" / event
                    trigger = "hist:keys=" + fields + ":size=4096"
                    write_control(path / "trigger", trigger + ":pause")
                    triggers.append((event, path, trigger, fields.split(",")))
                    (output / (event + ".format")).write_text((path / "format").read_text())
            for _, path, trigger, _ in triggers:
                write_control(path / "trigger", trigger + ":continue")
            write_control(instance / "tracing_on", "1")
            os.kill(receiver_pid, signal.SIGCONT)
            receiver_stopped = False
            proc.stdin.write(b"\n")
            proc.stdin.flush()
            proc.stdin.close()
            result_raw = read_until(proc, args.timeout)
            report["fixture_exit_code"] = proc.wait(timeout=3)
            for _, path, trigger, _ in triggers:
                write_control(path / "trigger", trigger + ":pause")
            write_control(instance / "tracing_on", "0")
            (output / "fixture.stdout").write_bytes(result_raw)
            report["events"] = {}
            for event, path, _, fields in triggers:
                raw = (path / "hist").read_text()
                (output / (event + ".hist")).write_text(raw)
                report["events"][event] = parse_hist(raw, fields)
            enter_rows = [
                row for name in SYSCALLS
                for row in report["events"]["sys_enter_" + name]["rows"]
            ]
            report["unexpected_receive_fds"] = sorted({
                row["fd"] for row in enter_rows if row["fd"] != receiver_fd
            })
            report["receive_syscall_entries"] = sum(row["hitcount"] for row in enter_rows)
            report["by_syscall"] = {}
            for name in SYSCALLS:
                entries = report["events"]["sys_enter_" + name]["hits"]
                exit_hist = report["events"]["sys_exit_" + name]
                rows = exit_hist["rows"]
                report["by_syscall"][name] = {
                    "entries": entries,
                    "exits": exit_hist["hits"],
                    "eagain_exits": sum(r["hitcount"] for r in rows if r["ret"] == -11),
                    "successful_datagrams": sum(
                        r["hitcount"] * (r["ret"] if name == "recvmmsg" else 1)
                        for r in rows if r["ret"] >= (1 if name == "recvmmsg" else 0)
                    ),
                }
                if entries != exit_hist["hits"]:
                    errors.append(name + " entry/exit totals differ")
            if report["unexpected_receive_fds"]:
                errors.append("receiver made receive calls on undeclared socket FDs")
            if not report["receive_syscall_entries"]:
                errors.append("no receiver receive syscalls were recorded")
            if report["fixture_exit_code"]:
                errors.append("fixture exited unsuccessfully")
            summaries = [json.loads(line) for line in result_raw.splitlines() if line]
            if len(summaries) != 1 or summaries[0].get("kind") != "rx_summary":
                errors.append("fixture did not emit exactly one RX summary")
            else:
                summary = summaries[0]
                report["fixture_summary"] = summary
                traced_packets = sum(value["successful_datagrams"]
                                     for value in report["by_syscall"].values())
                if traced_packets != summary["received_packets"]:
                    errors.append("traced successful datagrams disagree with fixture packets")
                expected = "recvmmsg" if summary["mode"] == "batch" else "recvfrom"
                if any(value["entries"] for name, value in report["by_syscall"].items()
                       if name != expected):
                    errors.append("receive syscall kind disagrees with fixture mode")
                if report["by_syscall"][expected]["entries"] < summary["receive_calls"]:
                    errors.append("fewer kernel entries than receive API calls")
    except BaseException as exc:
        errors.append(type(exc).__name__ + ": " + str(exc))
    finally:
        if receiver_stopped and receiver_pid is not None:
            try:
                os.kill(receiver_pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        if proc is not None and proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=3)
            except (ProcessLookupError, subprocess.TimeoutExpired) as exc:
                errors.append("fixture cleanup: " + str(exc))
        if created:
            controls = [(instance / "tracing_on", "0")]
            controls.extend((path / "trigger", "!" + trigger)
                            for _, path, trigger, _ in reversed(triggers))
            controls.extend(((instance / "set_event_pid", ""),
                             (instance / "options" / "event-fork", "0")))
            for path, value in controls:
                try:
                    write_control(path, value)
                except OSError as exc:
                    errors.append("tracefs cleanup: " + str(exc))
            try:
                instance.rmdir()
            except OSError as exc:
                errors.append("tracefs instance removal: " + str(exc))
        report["errors"] = errors
        report["valid"] = not errors
        report["instance_removed"] = not instance.exists()
        (output / "syscall-counts.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["valid"] else 1


def interrupt(signum, _frame):
    raise InterruptedError("received signal " + str(signum))


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    sys.exit(main())
