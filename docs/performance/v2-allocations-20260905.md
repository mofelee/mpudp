# V2 Allocation Costs, 2026-09-05

[中文版](v2-allocations-20260905.zh-CN.md)

Scalar completion polling and single-buffer FEC encoding reduce the observed
v2 sender allocation cost per verified business byte. Network goodput changes
are mixed: upload improves in this round, while aggregation download falls
from 90.939 to 84.387 Mbit/s. This short comparison does not establish a causal
throughput improvement or satisfy the formal performance requirements.

```text
before source: 28f786b6abb52c8ed4cff94c5b2ea539be279faf
measured after source: 5e19e44a6ea0d9e6a084129ebfe2075e4881ae00
after probe SHA-256: 4fb8daa691211be40a5d7407ca89d0c7034e0f1d1f4f264c0638362da6d3a272
diagnostic run: mpudp-alloc-diagnostics-5e19e44
separate timing run: mpudp-alloc-timing-5e19e44
```

The measured source remains the executable identity when this report is
published from a later documentation or merge commit.

## Runtime Changes

`Controller.Completion` returns the scalar send-completion frontier. Fence
polling reads it under the existing owner lock, avoiding allocation of path
statistics from `Snapshot`. Cancellation, closure, failure-frontier filtering
and completion notifications retain their existing behavior.

For a nil destination, `AppendFECBundle` now validates the complete bundle and
allocates one final packet buffer, writing the body directly after the envelope
prefix. Nonnil destinations retain a separate body so caller aliases remain
safe. Wire bytes, authenticated boundaries, caller ownership and the conservative
two-packet credit reservation are preserved. The changes add no shared pool or
retained packet cache and do not raise the credit ceiling.

## Same-Workload Diagnostics

The baseline is the [negotiated-path diagnostic](v2-paths-20260905.md). Both runs
use five 100 Mbit/s paths per direction, 20 ms one-way delay, one Session/flow,
RS(3+2), a 1200-byte UDP budget and 1400-byte originals with 1360 verified
business bytes. Each case has three warmup seconds and 15 steady seconds, one
round, basic host sampling, and profiles/timing diagnostics disabled. Topology,
workload parameters and runner script hashes match. Source and runner trees
were recorded clean; local test/benchmark load was paused during measurement.

| Direction/profile | Before Mbit/s | After Mbit/s | After worst 5s | After RTT P95/P99 ms |
| --- | ---: | ---: | ---: | --- |
| Upload v1 | 65.347 | 68.381 | 60.745 | 68 / 87 |
| Upload v2 | 40.635 | 45.402 | 42.752 | 84 / 128 |
| Upload v2 aggregation | 92.689 | 97.377 | 93.651 | 73 / 123 |
| Download v1 | 59.638 | 63.514 | 58.989 | 67 / 176 |
| Download v2 | 39.985 | 42.419 | 40.439 | 71 / 83 |
| Download v2 aggregation | 90.939 | 84.387 | 77.953 | 72 / 105 |

Every new case has positive goodput in all 15 receiver buckets, 75/75 RTT
opportunities on time, no RTT queue misses or failed writes, and no corrupt,
duplicate or too-old business frames. Local drains pass; local socket attempts
alone do not establish delivery. V1 variation and a single short round prevent
a statistical or isolated capacity claim.

All cost windows cover receiver buckets 1..15. Maximum boundary skew is
226.94 ms, within the 250 ms tolerance; it measures timestamp separation, not
clock offset. CPU percent is relative to one core. Socket PPS includes protocol
traffic. The approximate IPv4 ratio counts each endpoint's sends once and adds
28 bytes per UDP packet; it is not exact physical shaper accounting.

| Direction/profile | Sender CPU % | Receiver CPU % | Sender socket PPS | Receiver UDP ingress Mbit/s | IPv4 bytes / verified byte |
| --- | ---: | ---: | ---: | ---: | ---: |
| Upload v1 | 124.63 | 123.11 | 31,461 | 135.366 | 2.085 |
| Upload v2 | 120.13 | 128.14 | 20,888 | 200.393 | 4.525 |
| Upload v2 aggregation | 121.04 | 128.99 | 20,656 | 198.168 | 2.086 |
| Download v1 | 124.11 | 116.46 | 29,163 | 125.770 | 2.081 |
| Download v2 | 120.06 | 115.28 | 19,520 | 187.365 | 4.528 |
| Download v2 aggregation | 120.45 | 115.47 | 18,325 | 176.049 | 2.136 |

V1 download records 560 receiver dispatcher ingress drops. Other receiver
windows, including every v2 case, record zero. Sender ingress drops and all
adapter/delivery drops are zero. Endpoint UDP error deltas and data-interface
link/qdisc drop/error deltas are zero in their respective windows; nonzero
management-interface drops remain in the evidence. These distinct counters do
not establish a universal absence of packet loss.

Hypervisor mean idle ranges from 8.60% to 12.70%. It records four swap pages
during v1 upload and 28 during v2 upload; other host/case swap counts are zero.
Host summaries use 14 complete interior intervals. The baseline also had low
headroom and recorded 567/2 hypervisor swap pages during v2 upload/download.
Neither run establishes isolated network capacity or spare host capacity.

## Allocation Accounting

Each endpoint total is the exact difference of `total_alloc_bytes` at its two
selected telemetry boundaries, not a rounded rate multiplied by 15. Divide by
that endpoint's actual timestamp interval for B/s. For B/verified B, divide by
the sum of receiver-verified business bytes in the complete aligned receiver
buckets. Sender and receiver boundaries differ slightly; this is a window cost
ratio, not attribution of every allocation to a particular business byte.
Startup allocations before the selected window are excluded. Heap end gauges,
process-lifetime maximum RSS and cumulative allocation profiles are different
measurements.

MB/s below means decimal allocation bytes/s divided by 1,000,000. Each entry
shows before / after; both sender and receiver use the same verified-byte
denominator for that build and case.

| Direction/profile | Sender B/verified B | Receiver B/verified B | Sender allocation MB/s | Receiver allocation MB/s |
| --- | ---: | ---: | ---: | ---: |
| Upload v1 | 5.749 / 5.728 | 8.002 / 8.011 | 46.963 / 48.959 | 65.364 / 68.472 |
| Upload v2 | 22.189 / 16.609 | 28.768 / 28.743 | 112.706 / 94.262 | 146.115 / 163.126 |
| Upload v2 aggregation | 10.287 / 8.285 | 15.948 / 15.876 | 119.184 / 100.849 | 184.767 / 193.240 |
| Download v1 | 5.782 / 5.778 | 7.052 / 7.070 | 43.105 / 45.870 | 52.572 / 56.131 |
| Download v2 | 22.243 / 16.619 | 27.034 / 27.014 | 111.171 / 88.097 | 135.116 / 143.241 |
| Download v2 aggregation | 10.326 / 8.456 | 15.194 / 15.333 | 117.382 / 89.202 | 172.731 / 161.767 |

V2 sender B/verified B falls in every case. Receiver B/verified B stays close to
the baseline; the larger upload allocation rates accompany more received work.
The allocation changes cannot be assigned separate network effects from this
combined-build comparison. V1 provides an unchanged implementation control,
not an estimate of measurement variance from repeated trials.

The following new-build totals and rates retain the distinction between bytes
and allocation objects. The archive's `report.json` includes exact timestamps,
verified-byte denominators, rates, GC counts, end heap and maximum RSS; original
endpoint JSONL retains every cumulative snapshot for independent subtraction.

| Direction/profile | Sender allocation bytes | Receiver allocation bytes | Sender mallocs/s | Receiver mallocs/s |
| --- | ---: | ---: | ---: | ---: |
| Upload v1 | 734376648 | 1027132440 | 223884 | 1011913 |
| Upload v2 | 1413921672 | 2446820688 | 518759 | 828598 |
| Upload v2 aggregation | 1512744760 | 2898597832 | 532573 | 857616 |
| Download v1 | 688039904 | 841995224 | 213984 | 740745 |
| Download v2 | 1321777784 | 2148561576 | 485163 | 481204 |
| Download v2 aggregation | 1337966792 | 2426124096 | 472314 | 484712 |

## Receive Progress

These are receiver event deltas over the aligned window and current ending
gauges. Bundles count authenticated handler attempts, including retries;
completed groups have admitted fragments to original reassembly and need not
immediately produce business delivery.

| V2 direction/profile | Bundles | Completed groups | New-group / original / scratch rejects | Expired groups | End groups / originals | End credit bytes |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Upload | 313308 | 62660 | 0 / 0 / 0 | 0 | 0 / 0 | 1066416 |
| Upload aggregation | 309580 | 61913 | 0 / 0 / 0 | 0 | 18 / 1 | 1359500 |
| Download | 292777 | 58555 | 0 / 0 / 0 | 0 | 2 / 0 | 172841 |
| Download aggregation | 275039 | 55007 | 0 / 0 / 0 | 0 | 1 / 1 | 161295 |

Every sampled v2 endpoint, including warmup, has zero rejection/expiry counters
and zero decoded-pending groups. Upload receiver pending groups range 0..2
without aggregation and 0..18 with aggregation; observed credit ranges are
1066416..1181216 and 1069216..1359500 bytes. Download group maxima are six and
17. These are observed sample extrema, not guaranteed high-water bounds or
zero-backlog claims. Credit covers aggregate Peer ownership, not process RSS.
All seven counters and five gauges remain in the original per-second records.

## Isolated Microbenchmarks

The benchmark archive preserves three logs with 180 observations, exact source
patches, fixtures and the original scalar checksum package. Scalar source is
`302a07ed6f8d48010bc0a7114780db0f1c365ee1`; single-buffer source is
`192153a909f48eb195d7b804b30aad5ac15d700d`. Both derive from the before source;
the audit reconstructs their integration at the measured after source. These
are isolated benchmarks, not measurements of the combined executable.

Snapshot and Completion are compared within one executable, with unchanged
Snapshot code. Five-path polling falls from 1808 B/op and four objects/op to
zero. Median ns/op for zero / 1057 synthetic pending groups is 1782 / 2007 for
Snapshot and 1.710 / 1.868 for Completion. The fixture uses real negotiated,
prepaid controllers; pending groups are synthetic metadata. Owner locking,
wakeups, cancellation, close and failure-frontier handling are excluded.

For UDP1200 nil-destination encoding, bytes/op fall from 3000 to 1848 and
allocations/op from ten to nine. Median time is 5601 versus 5625 ns/op: no CPU
speedup is established. Reused nonnil destination allocation behavior is
unchanged. Shapes include 512, 1200, maximum packet size and sixteen records
under a 1472-byte budget; the latter encodes 1388 bytes. The optimized maximum
nil case reports 66105 B/op once and 66104 B/op four times; all observations,
medians and ranges are retained.

The buffer baseline contains only the two identical fixture files added to the
before source; the optimized run then applies the encoder change. Producer
commands and sequencing are recorded, and patches match the named commits.
Logs capture Linux/amd64, CPU model and concurrency suffix six, but not compiler
versions or temporary test executable hashes. Five 200 ms repetitions are not
confidence intervals. No benchmark was rerun by the auditor.

## Separate Transport Timing

The timing archive contains two additional uploads at the same after source
and binary, with timing diagnostics enabled and profiles disabled. They are
excluded from the before/after throughput table. The archived reader and exact
on/off JSON were independently reproduced from pinned reporter scripts and
audited public inputs. The off JSON also retains the v2 download controls.

All timing upload windows select receiver buckets 1..15. On-run goodput is
46.075 / 99.223 Mbit/s for v2 / aggregation; the slightly higher values than
the off run do not establish negative instrumentation overhead or significance.
Each sender path sends 1200-byte UDP payloads at approximately 4245 / 4157 PPS.
Payload bytes exclude IP/UDP headers. Sender/receiver durations and boundary
skew are recorded independently, without interpolation or overlap rescaling.

Times below are microseconds; P99 values are histogram bucket bounds.

| Upload profile | Per-path socket-write mean range | Socket-write P99 | Transport write-queue mean range | Write-queue P99 |
| --- | ---: | --- | ---: | --- |
| v2 | 17.870..22.612 | (64,128] or (128,256] | 0.102..0.118 | [0,1] |
| v2 aggregation | 17.074..20.905 | (64,128] or (128,256] | 0.104..0.114 | [0,1] |

SocketWrite measures socket write-call wall time, including returned errors;
WriteQueue measures transport write-mutex acquisition. Neither measures v2
owner-lock wait or pacing-timer lateness. Summed sender socket-write wall time
is 6.324 / 5.996 seconds; summed transport queue time is 0.03404 / 0.03412
seconds. These sums are not CPU time or measured owner-lock occupancy. Summing
five path means gives an estimated sequential write service cost of about
99.34 / 96.15 microseconds, not a measured group-duration distribution; path
percentiles cannot be added into group percentiles.

Configured `carrier-N` and `listener` counter slots can aggregate sockets and
persist across rebuilds or Session churn. Stable names do not prove an unchanged
socket generation. The listener slot aggregates listener socket writes, without
a per-remote-path breakdown. It sends 378 reverse/control datagrams per case; socket-write
means are 63.901 / 68.096 microseconds and P99 bounds are (512,1024]. Selected
write errors are zero. Exact per-path packets, bytes, rates, count/total deltas
and all 24 non-cumulative buckets are in `analysis/timing-on.json`.
Fields are independently sampled; count/bucket agreement here does not create
an atomic snapshot guarantee. Lifetime `max_ns` cannot yield an interval
maximum. Empty diagnostics-off histograms mean unavailable, not zero latency.
The small transport queue wait does not rule out owner-lock contention.

## Replay And Evidence

Use the [measurement guide](v2-measurement.md) at the measured source with the
recorded parameters. Run the timing uploads separately with diagnostics on and
profiles off. Microbenchmark reproduction commands, fixture patches and their
source associations are included in the benchmark README:

```sh
go test ./internal/sessionv2 -run '^$' -bench '^BenchmarkCompletionPolling$' -benchmem -benchtime=200ms -count=5
go test ./internal/wirev2 -run '^$' -bench '^BenchmarkFECBundleAppend$' -benchtime=200ms -count=5
```

- [Diagnostic archive](v2-allocations-20260905.tar.gz): 144 original public
  records, regenerated report/audit and both checksum indexes.
- [Benchmark archive](v2-allocations-benchmarks-20260905.tar.gz): eight original
  inputs, production/fixture reconstruction, 180 observations and audit.
- [Timing archive](v2-allocations-timing-20260905.tar.gz): 52 original public
  records, regenerated report/audit, timing reader, exact on/off JSON and
  detailed interpretation; no profiles.

Verify `PUBLIC_SHA256SUMS` and the original `SHA256SUMS` after extraction;
the scalar original checksum index resides under `completion/`. All archives
use explicit member inventories, deterministic metadata and secret marker
scans. Private configurations, keys and `.lab` are excluded. Lab addresses and
host metadata remain public evidence. The off analysis names the diagnostic
archive hash above, whose raw records are published separately.

Recorded cleanup covers 60 diagnostic and 20 timing owned units and two removed
endpoint workspaces per run. Independent read-only endpoint rechecks confirm
all 12 diagnostic and four timing units per endpoint inactive with MainPID 0,
and each owned workspace absent. Router/hypervisor cleanup uses recorded proof.

```text
diagnostic archive bytes: 1045477
diagnostic archive SHA-256: dd014cbe812916d1282cf30485cac73ae8e9600128d2a2d5f0d75234b7017bd6
benchmark archive bytes: 18465
benchmark archive SHA-256: e9685bfbaa90357e8aa80482802b024a66f783d8583160981e3fe864d95689eb
timing archive bytes: 381463
timing archive SHA-256: 660d1e5b813e026c5930b3ec9db1211cd06e6ce4d48f83e14e49635ef3fdec11
```

The >=250 Mbit/s, >=90% capacity, three 300-second rounds, native KCP, repair,
mux and fault/MTU requirements remain open. This evidence supports the bounded
allocation reductions and further sender-cost investigation, not performance
acceptance or a predicted throughput uplift.
