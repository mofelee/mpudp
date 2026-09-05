# V2 Datagram Measurement

[中文版](v2-measurement.zh-CN.md)

The first [controlled diagnostic report](v2-diagnostics-20260905.md) records
severe v2 upload underperformance; functional support is not throughput acceptance.
The [deadline-index comparison](v2-deadlines-20260905.md) publishes the supporting
profiles and microbenchmarks; the revised network run still fails upload delivery.
The subsequent [receive-credit comparison](v2-credit-20260905.md) fixes a
deterministic reservation stall but records continuing network failure.
The [receive-state diagnostic](v2-receive-20260905.md) adds live credit/group
evidence and fresh profiles of the remaining upload failure.
The [negotiated-path comparison](v2-paths-20260905.md) removes the observed
short-run upload stall and publishes matching profiles and local benchmarks;
formal throughput and capacity acceptance remain open.
The [allocation-cost comparison](v2-allocations-20260905.md) records reduced
v2 sender allocations, mixed throughput changes and separate transport timing;
it does not establish a causal throughput improvement.
The [directional-authenticator comparison](v2-authenticator-20260905.md) records
lower v2 sender/receiver allocation costs with prepaid HMAC state, alongside
mixed throughput and host-pressure limits.
The [prepaid-send workspace comparison](v2-send-workspace-20260905.md) protects
accepted-original progress at full byte credit and records the same-workload
allocation, throughput and host-pressure evidence.

The runner can compare the public Linux v2 Datagram runtime with v1. It retains
the receiver-verified business-byte accounting and one Session per business flow.
These tools do not mark product acceptance complete. The three 300-second rounds,
capacity calibration, host headroom and fault/MTU matrices remain separate gates
in [the performance contract](../PERFORMANCE.md).

## Profiles And Prerequisites

`--mpudp-profiles v1 v2 v2-aggregation` expands only `mpudp` and `kcp-mpudp` cases.
Native protocols run once for each selected native layout. The default is `v1`,
whose case IDs remain unchanged. V2 cases have distinct profile suffixes.

V2 requires Python's `PyYAML==6.0.2` to preserve integer path IDs in generated
YAML. Install it in the runner's Python environment before measurement. Both
endpoints run the same probe binary. The runner records source and binary hashes,
nonsecret configuration metadata, and exact directional hard caps and path rates.
Private PSK and SSH files stay outside published artifacts.

The supported v2 profile uses `protocol: datagram`, fixed/session MTU budgets,
repair disabled and explicit directional path rates. `kcp-mpudp` is the retained
experimental KCP-over-Datagram-FEC stack; it is not the native product KCP mode.
Pinned kcp-go still limits `--kcp-mtu` to 1500 bytes, independent of application
message size. Native `kcp` measures direct kcp-go over one UDP path per process.

The runner defaults to 100,000,000 configured bits/s per v2 path, 250 microseconds
maximum aggregation delay, 32 records, 256 queued originals and 1 MiB queue bytes.
`--v2-path-rate-bps`, `--v2-aggregation-max-delay-us`,
`--v2-aggregation-max-records` and `--v2-max-original-bytes` expose bounded
overrides. These are configured limits, not measured rates or delays. V2 maximum
original size also respects manifest fragment capacity; it does not use the v1
single-block payload formula.

## Controlled Diagnostic

Build from a clean, committed source checkout:

```sh
python3 -m pip install PyYAML==6.0.2
go build -C integration/perf -trimpath \
  -ldflags "-X main.sourceSHA=$(git rev-parse HEAD)" \
  -o /tmp/mpudp-perfprobe ./cmd/perfprobe
python3 scripts/perf/run-probe.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --binary /tmp/mpudp-perfprobe --source-sha "$(git rev-parse HEAD)" \
  --psk-file /private/mpudp-perf.psk --output /tmp/mpudp-v2-diagnostic \
  --protocols mpudp --mpudp-profiles v1 v2 v2-aggregation \
  --paths 5 --directions upload download --payloads 1400 \
  --flows 1 --rounds 1 --seconds 15 --warmup 3 --host-diagnostics basic
```

Use the lab's existing `--hypervisor-python` path where required. Add `--plan` to
write the matrix without SSH or traffic. The runner creates and cleans owned
transient processes and temporary probe workspaces; it does not reconfigure VMs
or the hypervisor. Repeat separately with 64-byte messages and a low offered rate
to compare latency. The existing 1 ms RTT bins cannot resolve a 250 microsecond
aggregation change precisely. RTT includes all scheduled opportunities, including
unanswered requests, and shares the same Session as bulk traffic.

## Checked Steady-Window Report

```sh
python3 scripts/perf/report-probe.py /tmp/mpudp-v2-diagnostic
```

The report verifies indexed input checksums, completion/source/binary identity,
endpoint metadata, receiver byte accounting, RTT and exchanged summaries. Keep
derived output outside the original artifact tree so its checksum index remains
unchanged.

CPU, allocations and socket PPS use cumulative per-second sample deltas. Sender
and receiver sample indices do not imply a common clock. The report selects
timestamp-matched endpoints within the receiver's nominal steady window, with a
default 250 ms maximum endpoint skew and receiver sampling delay. It reports the
actual timestamps, elapsed intervals and selected receiver buckets. Insufficient
alignment fails the report; no counter interpolation is used. With no warmup, the
first bucket has no preceding sampled boundary and is excluded from these cost
calculations. Full-window receiver throughput and RTT remain separately reported.
Endpoint wall clocks must be synchronized for this comparison; timestamp matching
alone cannot prove synchronization. Maximum RSS is a process-lifetime high-water
mark, not a steady-window memory delta. CPU percentage counts cores, so 100% means
one occupied core.

Raw MPUDP socket counters include data, control, parity, tails and echo traffic.
IPv4 L3 estimates add 28 bytes per sent UDP datagram. Bidirectional cost counts
each endpoint's sends once, divided by the selected receiver's unique business
bytes. These ratios are approximate because endpoint intervals are only matched
within the reported tolerance. They do not prove exact HTB accounting or split
padding, authentication, control and parity bytes. Native socket PPS is unavailable.
Detailed v2 FEC and authenticated listener-path metrics remain unavailable.

For packing arithmetic, v2 uses `S = U - 94`, protected logical bytes
`L = 4 + 20*m + A`, padding `k*S - L`, and IPv4 bytes per full group
`(k+r)*(94+S+28)`. Here `A` includes original bytes, including the probe's 40-byte
verifier per message; only verified body bytes count as business throughput.
Actual tails and both directions' control traffic must be included in measurements.
Packing capacity is an arithmetic bound and cannot substitute for measured throughput.
