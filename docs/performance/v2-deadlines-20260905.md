# V2 Receive Deadline Comparison, 2026-09-05

[中文版](v2-deadlines-20260905.zh-CN.md)

Admission-ordered deadline indexes remove a measured backlog-dependent CPU
cost. The same-workload network comparison still fails v2 upload delivery and
does not establish throughput acceptance or a causal network speedup.

```text
before source: c7d3ab00baf74c84f4864d877f3f5332bcc2f205
before probe SHA-256: c0f2823cfd5a0ae26ce886480c57c430b53add580e437ea6abfc9b0976a732fb
after source: ed2d6484170562f02474f3c0ebf31b6d115c1f5d
after probe SHA-256: 3571cb10eba5612cb760e6f4b3f6f49da62ba0ec5e1783696692269e628f08f2
after run: mpudp-deadline-diagnostics-ed2d648
```

## Profile And Implementation

A separate baseline v2-aggregation upload profile attributes 13.84 seconds,
54.62% cumulative CPU, to `sessionv2.Controller.NextDeadline`. Its flat cost
is 8.56 seconds, 33.78%; the nested original-deadline lookup accounts for
1.96 seconds, 7.73% cumulative. These overlapping percentages must not be added.
The dispatcher queries deadlines before each wait, so repeated scans compete
with packet processing as incomplete groups accumulate.

Both receive owners enforce nondecreasing time and fixed timeouts. Intrusive
admission-order lists therefore provide minimum-deadline lookup and arbitrary
removal without traversing pending entries. Expiry visits only due entries.
An exact decoded-group count avoids retry scans when no decoded group awaits
storage. Entries leave immediately on completion, error, expiry or Close;
there are no stale heap entries. Configured limits and deadlines are unchanged.
Original link storage is charged an additional 16 bytes; group links are covered
by the existing structure-size charge. Seeded scan-oracle and ownership tests
cover equal timestamps, arbitrary IDs/removal, rollback and exact expiry.

Local Linux/amd64 microbenchmarks used an Intel Xeon E3-1245 v5, 200 ms benchtime,
and real pending entries. Every reported operation has zero allocations.
Controller values are one measurement; original values are the median of three
with one CPU. The benchmark archive preserves exact fixtures, logs and commands.
These local after measurements used uncommitted baseline-plus-patch sources
that were later incorporated into the fix, not the exact `ed2d648` revision.
Only the revised network comparison below ran that committed revision.

| Operation, 8192 pending entries | Before | After |
| --- | ---: | ---: |
| Controller next deadline | 321.636 us | 568.5 ns |
| Controller expiry, none due | 307.076 us | 46.06 ns |
| Original next deadline | 299.637 us | 41.07 ns |
| Original expiry, none due | 366.115 us | 68.91 ns |

The baseline profile is a separate diagnostic, not a case in either six-case
throughput table. CPU, mutex and block collection starts after connection setup
and covers warmup, steady load, local drain and result exchange. `alloc_space`
also includes earlier startup allocations; heap in-use is a finalization
snapshot. None is a steady-window allocation rate. No after-change CPU profile
was collected for this increment.

## Controlled Network Comparison

Both runs use the existing five 100 Mbit/s paths per direction, 20 ms one-way
netem delay, one business flow in one Session, RS(3+2), a 1200-byte UDP budget,
and 1400-byte originals with 1360 verified body bytes. Each case has three
warmup seconds and 15 steady seconds, one round, with timing diagnostics and
profiles disabled. The source/runner trees were clean. Local tests and
benchmarks were paused during the revised run. No VM or hypervisor settings
changed. See the [baseline report](v2-diagnostics-20260905.md) for its full data.

| Direction/profile | Before Mbit/s | After Mbit/s | After worst 5s | After RTT P95/P99 ms |
| --- | ---: | ---: | ---: | --- |
| Upload v1 | 65.971 | 68.564 | 66.179 | 121 / 124 |
| Upload v2 | 0.178 | 0.944 | 0.000 | unavailable |
| Upload v2 aggregation | 0.309 | 1.569 | 0.000 | unavailable |
| Download v1 | 62.960 | 63.814 | 61.400 | 66 / 79 |
| Download v2 | 25.090 | 26.353 | 26.129 | 55 / 60 |
| Download v2 aggregation | 75.654 | 71.438 | 68.601 | 57 / 63 |

V2 upload still has long zero-delivery intervals. Without aggregation only
1/75 scheduled RTT opportunities completes on time; with aggregation 3/75
complete on time, with 50 queue misses and 13 failed writes. Null quantiles
must not be interpreted as low latency. Other rows have 75/75 on-time replies.
All cases have zero corrupt and duplicate business frames and pass local tail
drain; a local socket attempt does not prove remote delivery.

CPU and socket costs below use timestamp-aligned sample deltas, with a 250 ms
tolerance. Download v1 has only nine selected receiver buckets, and download
v2 aggregation has 13; the remaining rows have 15. Headline throughput and RTT
above still cover each full 15-second receiver window. Maximum boundary skew
is 249.91 ms. Different sample coverage limits direct cost comparisons.

| Direction/profile | Cost buckets, s | Sender CPU % | Receiver CPU % | Forward socket PPS | IPv4 bytes / verified byte |
| --- | ---: | ---: | ---: | ---: | ---: |
| Upload v1 | 15 | 125.20 | 124.58 | 31,637 | 2.091 |
| Upload v2 | 15 | 120.97 | 138.65 | 19,073 | 198.665 |
| Upload v2 aggregation | 15 | 121.83 | 134.78 | 19,972 | 125.073 |
| Download v1 | 9 | 123.84 | 114.40 | 28,914 | 2.077 |
| Download v2 | 15 | 122.31 | 82.46 | 12,136 | 4.533 |
| Download v2 aggregation | 13 | 119.81 | 103.82 | 16,322 | 2.223 |

CPU counts occupied cores. The approximate IPv4 ratio counts both endpoints'
sends once, adding 28 bytes per UDP packet; it includes unsuccessful delivery,
parity, padding and control. It does not confirm physical shaper accounting.
This short, sequential comparison cannot establish spare host capacity or
separate implementation effects from run-to-run variation.
Hypervisor mean idle ranges from 7.51% to 18.96%; its download cases also record
49, 1 and 4 swap-page changes for v1, v2 and aggregation respectively. Bracketing
snapshots show no data-interface link error/drop or qdisc-drop increase, but
do not exclude per-socket queue loss. Snapshot intervals include setup, warmup
and drain; they are not steady-window counter deltas.

## Remaining Credit Pressure

A deterministic local reproducer independently shows that obsolete decode
reservations can prevent original admission. With 31 incomplete groups and one
decoded group, retained credit is 475216 of 476216 bytes. The remaining 1000
bytes cannot admit a 1400-byte original plus its range/link storage. There is
no externally held delivery lease; 9998 simulated retries make no progress.
Expiry of the older groups at ten seconds then permits delivery.

The deadline change preserves this ownership behavior. The group reservation
still includes discarded shards and reconstruction workspace while original
reassembly needs separate backing. This is consistent with the observed
waveform, but the remote records do not expose exact live credit occupancy
and cannot prove the same internal state. A separate accounting fix and new
measurements are required. The >=250 Mbit/s and >=90% capacity targets, three
300-second rounds, native KCP, repair, mux and fault/MTU gates remain open.

## Replay And Public Artifacts

Follow [the measurement guide](v2-measurement.md), using the after source/binary
above and the same six-case parameters. Recompute the report with
`python3 scripts/perf/report-probe.py <extracted-run-directory>`.

- [Revised raw records and checked report](v2-deadlines-20260905.tar.gz):
  144 original public files, original `SHA256SUMS`, derived report/audit and
  `PUBLIC_SHA256SUMS`. Verify both indexes after extraction.
- [Baseline profile evidence](v2-upload-profile-20260905.tar.gz): ten explicitly
  selected CPU/allocs/heap/mutex/block profiles, decoded protobuf validation,
  top reports, original public run records and checksums. It excludes private
  configurations and the source `.lab` directory.
- [Microbenchmark evidence](v2-deadline-benchmarks-20260905/README.md): exact
  original diagnostic fixtures and logs, plus maintained benchmark references.

Publication uses an explicit public-file selection, known-PSK/private-material
scans and deterministic archive reconstruction. Lab addresses and host metadata
remain present. All 60 owned transient units stopped and both endpoint
workspaces were removed. No existing VM, network or hypervisor was modified.

```text
revised archive SHA-256: 3822f5743e629443174bd7dc1a27a5628a64b25a204911fd6a0191eb3089aee7
profile archive SHA-256: 1d9a16e714b3bfaa3066ed8af70b7fbb297131d167c5b9b825c5b0b43596daaa
```
