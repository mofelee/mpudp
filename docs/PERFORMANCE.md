# Performance Evidence

Issue [#16](https://github.com/mofelee/mpudp/issues/16) tracks the single-flow
multipath performance work. The baseline tooling is delivered incrementally in
[#17](https://github.com/mofelee/mpudp/issues/17). Tool availability does not mean
the performance targets have passed.

Initial evidence: [native five-path capacity smoke](performance/native-capacity-smoke-20260905.md).
Updated diagnostic evidence: [five-path counters, profiles and complete network snapshots](performance/diagnostics-20260905.md).
FEC correctness comparison: [fixed receive window and same-workload smoke](performance/fec-replay-window-20260905.md).
V2 deadline indexing: [profiles, microbenchmarks and same-workload failure comparison](performance/v2-deadlines-20260905.md).

## Reference Environment

The reference lab has two endpoints and five independent forwarding VMs, each
with a separate LAN and WAN bridge. Each router shapes both egress directions at
100 Mbit/s, with 20 ms one-way netem delay, producing about 40 ms RTT. Management
traffic uses a separate network. Native TCP/UDP capacity tests use five separate
server sockets; a single iperf3 server cannot service five simultaneous tests.

The existing seven-VM deployment and original measurements are in the private
`mofelee/mpudp-test` repository, commit
`9480e5551612d6541f6aaf0d3f9b36ad63f4fcc3`. Its MPUDP source is the immutable v0.1.0
baseline `934a6325f25be3be0c587d5eab57bd6a8380e7e9`, with kcp-go v5.6.72. New runs
must record their own source, binaries, dependencies, hardware and guest kernels;
the old deployment is not evidence that those properties remain unchanged.

The runner consumes an existing lab through SSH. It neither provisions machines
nor changes routes, MTUs, qdiscs, firewalls, or existing services. The caller must
verify the topology and shaping before measurement. Requirements on every node:
Linux, Python 3, iproute2 and systemd; endpoints additionally need iperf3 supporting
`--get-server-output` and `--dont-fragment`. SSH aliases must permit running
transient systemd units. The hypervisor alias uses the normal SSH configuration;
the seven guest aliases use the separately supplied SSH configuration.
The runner establishes private SSH control sockets sequentially, so starting
the parallel load does not exceed a jump host's unauthenticated connection limit.
Control sockets are closed and removed after the run.

On a NixOS hypervisor without Python in the system profile, a separately resolved
Python store path can be supplied with `--hypervisor-python /nix/store/.../bin/python3`.
For example, `ssh ks nix build --no-link --print-out-paths nixpkgs#python3` resolves
that runtime without modifying the system profile. Record the resolved path in
the manifest; do not assume it is portable between hosts.

## Native Capacity Calibration

From the repository root, with the existing seven-VM lab:

```bash
python3 scripts/perf/calibrate.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --output integration/perf/artifacts/calibration-20260905 \
  --paths 1 2 3 5 --rounds 3 --warmup 20 --seconds 300
```

This measures TCP and UDP, upload and download, for every selected path count.
One connection is used per native path; their aggregate is explicitly labeled
as concurrent native capacity, never an MPUDP single-flow result. Output paths
must be new, preventing accidental overwrite of evidence. The five-path-only
matrix takes approximately one hour; all path counts take approximately four.
For a tool smoke test, use `--paths 5 --rounds 1 --seconds 15 --warmup 2`.
Short runs remain diagnostic evidence and cannot pass the formal measurement
window requirement.

The runner binds one temporary server per selected data IP on ports 15201 through
15205. It rejects occupied ports and uses uniquely named transient units with
bounded lifetimes. On success or failure it stops only those units and records
their final state in `cleanup.json`. Existing VMs, networks and services remain
owned by the deployment. A failed cleanup prevents a successful runner exit.

Each case preserves raw iperf JSON (one-second intervals and both endpoint
reports), host JSONL samples, command errors, case parameters, a summary and
cleanup proof. `manifest.json` binds the run to the git SHA, dirty-tree marker,
script hashes, topology and fixed measurement parameters. `SHA256SUMS` covers
every artifact. Receiver reports are selected explicitly: the remote server for
upload and the client for download. UDP is offered at 100 Mbit/s per path with
1400-byte payloads and DF; its offered rate excludes IP/UDP headers, so some
shaper loss is expected at that offered load.

The host collector samples CPU, memory, swap, softirq, pressure and UDP/TCP kernel
counters every second. It also records bounded numeric CPU/RSS counters for QEMU,
iperf3 and benchmark processes, keyed by PID and process start time; command lines
are excluded. It captures link/qdisc/socket state before and after the
load; `--diagnostics full` captures these expensive commands every sample and
must be compared with the default `basic` mode. HTB classes are collected per
interface. Both modes write explicit `network_snapshot` records; measurement
starts after the initial snapshot, and a successful run requires a complete final
snapshot. The protocol runner requests that final snapshot with SIGUSR1 before
stopping its owned sampler units. Failed commands, malformed output or snapshots
that do not surround the measured interval fail verification.
Hypervisor CPU summaries use tick-weighted deltas within the common
steady interval, excluding warmup and recovery. Clock synchronization and startup
uncertainty still need inspection in the raw timestamps. A mean idle value alone
does not prove headroom: inspect endpoints, routers, per-CPU/softirq use, swap,
pressure, sampler gaps and per-path capacity together.

The per-path 90 Mbit/s diagnostic flag is only a native-capacity screen. It does
not freeze an encapsulation budget or certify the host, and every calibration
summary explicitly has `product_acceptance: false`.

## Protocol Comparison Runner

The [isolated probe](../integration/perf/README.md) implements native TCP/UDP,
direct KCP, MPUDP Datagram and KCP-over-MPUDP. Build from a committed checkout,
then pass a private file containing one raw PSK line (mode 0600):

```bash
go build -C integration/perf -trimpath \
  -ldflags "-X main.sourceSHA=$(git rev-parse HEAD)" \
  -o bin/perfprobe ./cmd/perfprobe
python3 scripts/perf/run-probe.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --binary integration/perf/bin/perfprobe --source-sha "$(git rev-parse HEAD)" \
  --psk-file /private/mpudp-perf.psk \
  --output integration/perf/artifacts/protocol-baseline
```

Add `--hypervisor-python` as needed, as for capacity calibration. Use `--plan`
to produce the exact matrix without SSH or network load. The default matrix is
696 cases, at least 61.9 hours of warmup and measurement alone. It covers five
protocols, 1/2/3/5 candidate paths, both directions, 64/1200/1400/maximum messages
and three independent rounds. Native protocols distinguish a single flow on one
path from concurrent independent native paths. MPUDP single-flow cases use one
Session over all selected Carriers. Add `--flows 1 2` for the separate multi-flow
comparison and `--diagnostics off on` for instrumentation overhead comparisons;
these increase the matrix size. A formal baseline must include those comparisons.

For a deployment smoke test, select `--paths 5 --payloads 1400 --rounds 1
--warmup 1 --seconds 5`. A smoke run only verifies the tool and traffic workflow.
The runner records all resolved dimensions and never declares product acceptance.
The [26-case protocol smoke report](performance/protocol-smoke-20260905.md)
records the first real-lab replay and its measurement limits.

The runner deploys the exact hashed executable into a new private remote workspace
and verifies the bytes on disk. It sends secret configuration through SSH stdin
into mode-0600 files. Original services and configurations are preserved. Every
case has bounded, individually owned systemd units, receiver-verified per-second
records, both hosts' exchanged summaries, host samples and cleanup proof. Full
verification binds both hosts' diagnostic, KCP and offered-rate options and
requires RTT opportunity accounting, per-second telemetry, integrity counts and
the independently recomputed worst-five-second throughput. Native parallel
receiver start times may differ by at most one second or 5% of the steady window,
whichever is smaller. Their full-window rates are summed; the actual skew and
common overlap are reported, and host headroom uses that common interval. Full
host diagnostics are enabled by default; `--host-diagnostics basic` allows a
collector overhead comparison. Optional `--profiles` stores local private
profiles under each case's `.lab/profiles`, excluded from the shareable checksum
manifest until separately inspected. The generated configuration matches the
original 4096-entry delivery/receive queues and 8192 pending FEC block bound.

## Counting And Capacity

Product throughput is receiver-validated, unique application bytes divided by a
fixed steady interval. Padding, MPUDP/FEC/KCP headers, parity, acknowledgments,
repair and retransmissions must be reported separately. Record single-flow and
multi-flow results separately, including each direction, the 1/2/3/5-path curve,
P50/P95/P99, worst rolling five-second throughput, CPU, memory and wire rate.
Formal healthy cases require at least three independent rounds of 300 seconds
after fixed warmup.

Do not substitute IP MTU for a complete UDP payload budget. With no tunnel or IP
options, IPv4 MTU 1500 allows UDP payload 1472; IPv6 allows 1452. The v1 MPUDP DATA
overhead is 71 bytes inside that UDP payload. A 1400-byte Datagram encoded with
RS(5,3) becomes five 467-byte shards, each carrying those 71 bytes plus UDP/IP.
At a genuine IPv4 L3 aggregate limit of 500 Mbit/s, a 1376-byte KCP segment has an
illustrative ceiling of about 243 Mbit/s. It cannot reach the epic's 250 Mbit/s
RS threshold without improving packing and packet processing.

HTB's charged skb lengths and any link-layer/tunnel overhead must be established
before freezing the efficient capacity ceiling. The old lab used no explicit
HTB overhead/stab configuration. The example topology therefore identifies its
accounting as requiring calibration; it does not silently claim a verified L3
meter. Neither reducing the ceiling nor changing this accounting can be used to
bypass the unchanged product thresholds.

## Remaining Acceptance

The complete #17 matrix also requires direct KCP, pure MPUDP Datagram and
KCP-over-MPUDP, small/1200/1400/maximum negotiated Datagrams, diagnostics on/off,
sender and receiver queue/ACK timing, CPU/alloc/mutex/block profiles, and a
causal explanation of the old RTO-heavy retransmissions. Native KCP product
mode, repair, smux and authenticated MTU epochs belong to later child issues.

MTU comparisons must include homogeneous/heterogeneous actual path MTUs,
conservative/optimal fixed budgets, size-dependent blackholes with filtered
ICMP, increases/decreases and one-way changes. Record actual packet-size
distributions, confirmed per-path budgets, padding, probe bytes and epoch
migration times when those features exist. Raising a configured limit alone
does not demonstrate an MTU improvement.

Shared artifacts must exclude PSKs, private keys, raw business packets and
complete authentication tags. The collector emits counters and metadata only;
never copy the deployment's `.lab/` directory into public evidence. A private
repository URL alone is not public final acceptance evidence.

## Original Measurements

[The public numeric baseline](performance/original-v0.1-baseline.json) retains the
original healthy upload/download per-second samples, request latencies, KCP
counters, source hashes and binary hashes. It can be reproduced from access to
the original private repository:

```bash
python3 scripts/perf/import-baseline.py --source /root/mpudp-test \
  --output docs/performance/original-v0.1-baseline.json
```

The export reads the fixed git commit, validates dependency pins and permits only
specific numeric/boolean fields and validated digests. It never exports secret
configuration files or arbitrary error strings. The original upload has 103922
retransmits across 390568 output segments (26.608%). Of those retransmits, 103257
are counted in the RTO branch, 530 in fast retransmit, and 135 in early retransmit.
This establishes the dominant retransmission category, not whether packets were
lost in the network, dropped inside MPUDP, queued too long or timed out early.
