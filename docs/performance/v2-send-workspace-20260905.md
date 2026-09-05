# V2 Prepaid Send Workspace, 2026-09-05

[中文版](v2-send-workspace-20260905.zh-CN.md)

The controller now prepays one FEC output and one packet assembly so accepted
originals can progress even when Session or Peer byte credit is exhausted.
The same-workload diagnostic records lower sender allocation bytes per verified
byte in all four v2 cases. Throughput remains mixed: aggregation download falls
from 95.791 to 91.913 Mbit/s. This short run does not establish a causal speedup
or formal performance acceptance.

```text
before source: f6bc4322dfe7b22f5cc29cf07df737ebf829e032
measured after source: cbcb4076a01bc397aa518a04c0b654a8b1dfd1a5
after probe SHA-256: a565df9376e7734d5cc4a6e7a0f4f4e6667a26214c78390cc0318896b42216ca
diagnostic run: mpudp-send-workspace-diagnostics-cbcb407
```

The measured source identifies the executable even if a later documentation
or merge commit publishes this report.

## Change And Progress Guarantee

Previously, accepted original payloads could consume the credit needed for a
later FEC output or packet assembly reservation. Retrying could not release that
credit while the same originals remained queued. Two dedicated initial claims
now cover one output's shard backing and owner metadata, plus the existing
conservative allowance of twice the complete UDP packet budget for assembly.
At UDP1200, the assembly claim is 2400 bytes. Both stay charged between sends,
so original admission has less uncommitted credit but cannot consume this floor.

Installation binds the claims without reserving again. Sizing uses the local
offer; a smaller negotiated packet budget remains within that allowance.
The output workspace permits one live group, returns its slot after clearing
output storage, and retains its lease while a live output survives workspace
Close. Standalone aggregation queues retain their existing `Seal` behavior.
The controller still sends synchronously; this change adds no workers,
concurrent packets, async completion, pacing policy or wire-format change.

The [ownership design](../design/v2-send-workspace.md) describes the bounds and
tests. Full-credit Session and Peer regressions exercise successive partial
groups of an accepted original, including unrelated admission taking newly
freed payload credit during emission. Rollback, negotiated smaller budgets,
copied handles and Close ownership are covered separately. This is a local
memory progress guarantee while paths and the serialized driver remain usable;
the diagnostic below does not deliberately saturate the ledger or prove remote
delivery under arbitrary faults.

## Same-Workload Diagnostics

The baseline is the [directional-authenticator run](v2-authenticator-20260905.md).
Both use five 100 Mbit/s paths per direction, 20 ms one-way delay, one
Session/flow, RS(3+2), UDP1200 and 1400-byte originals containing 1360 verified
business bytes. Each case has three warmup seconds and 15 steady seconds,
one round, basic host sampling, and profiles/timing diagnostics disabled.
Topology, workload and runner script hashes match; source/runner trees were
recorded clean. Local tests, builds and bulk processing were paused during traffic.

| Direction/profile | Before Mbit/s | After Mbit/s | After worst 5s | After RTT P95/P99 ms |
| --- | ---: | ---: | ---: | --- |
| Upload v1 | 71.736 | 71.259 | 69.308 | 110 / 125 |
| Upload v2 | 44.430 | 47.913 | 47.339 | 80 / 111 |
| Upload v2 aggregation | 99.665 | 103.234 | 101.221 | 75 / 107 |
| Download v1 | 62.061 | 67.245 | 64.895 | 87 / 109 |
| Download v2 | 45.581 | 46.285 | 43.942 | 73 / 107 |
| Download v2 aggregation | 95.791 | 91.913 | 83.839 | 72 / 138 |

Every steady receiver bucket has positive goodput, with no corrupt, duplicate
or too-old business frames. All six cases receive 75/75 RTT responses on time;
deadline misses, unanswered requests, queue misses and RTT write failures are
zero. Local drains pass. V1 is an unchanged implementation control, not a
repeated-trial variance estimate.

All cost windows cover receiver buckets 1..15. Endpoint durations range
14.995463..15.004536 seconds; maximum boundary skew is 238.35 ms, within the
250 ms tolerance. Timestamp separation is not a measured clock offset.
CPU percent is relative to one core. Socket PPS includes protocol traffic;
the approximate IPv4 ratio adds 28 bytes to each endpoint's sent UDP packet
and is not exact physical shaper accounting.

| Direction/profile | Sender CPU % | Receiver CPU % | Sender socket PPS | Receiver UDP ingress Mbit/s | IPv4 bytes / verified byte |
| --- | ---: | ---: | ---: | ---: | ---: |
| Upload v1 | 124.42 | 124.63 | 32,739 | 141.129 | 2.082 |
| Upload v2 | 121.08 | 125.98 | 22,050 | 211.580 | 4.526 |
| Upload v2 aggregation | 120.70 | 126.73 | 21,736 | 208.664 | 2.070 |
| Download v1 | 124.44 | 117.97 | 30,941 | 133.114 | 2.085 |
| Download v2 | 120.69 | 117.89 | 21,308 | 204.439 | 4.528 |
| Download v2 aggregation | 120.17 | 116.12 | 19,772 | 189.086 | 2.116 |

## Allocation Costs

Each entry is before / after. Allocation totals are exact differences of each
endpoint's cumulative `total_alloc_bytes` at the selected boundaries. Divide
by that endpoint's actual elapsed time for B/s, or by the selected receiver
buckets' verified bytes for B/verified B. Startup is excluded. Endpoint
boundaries differ slightly, so these are window cost ratios rather than causal
per-byte attribution. MB/s below means decimal allocation bytes/s divided by
1,000,000. End heap and process-lifetime maximum RSS are separate gauges.

| Direction/profile | Sender B/verified B | Receiver B/verified B | Sender allocation MB/s | Receiver allocation MB/s |
| --- | ---: | ---: | ---: | ---: |
| Upload v1 | 5.790486 / 5.719122 | 8.059666 / 8.022446 | 51.936 / 50.942 | 72.262 / 71.437 |
| Upload v2 | 14.549383 / 14.271964 | 26.745336 / 26.737652 | 80.803 / 85.477 | 148.546 / 160.131 |
| Upload v2 aggregation | 7.278251 / 7.157164 | 14.916748 / 14.884038 | 90.674 / 92.386 | 185.853 / 192.077 |
| Download v1 | 5.765799 / 5.789216 | 7.059426 / 7.079943 | 44.728 / 48.662 | 54.774 / 59.521 |
| Download v2 | 14.481747 / 14.276863 | 25.006328 / 25.005833 | 82.503 / 82.601 | 142.514 / 144.676 |
| Download v2 aggregation | 7.332727 / 7.296430 | 14.190828 / 14.253474 | 87.783 / 83.829 | 169.868 / 163.767 |

Sender B/verified B decreases in all four v2 cases, while allocation MB/s can
increase when more verified work is delivered. Receiver B/verified B is nearly
unchanged in nonaggregated cases and mixed with aggregation. The standing
credit floor is an ownership bound; it does not preallocate all Go objects or
eliminate the group's independent output and encoded-packet allocations.

Exact new-build totals and denominators permit direct ratio checks:

| Direction/profile | Verified business bytes | Sender allocation bytes | Receiver allocation bytes | Sender / receiver mallocs/s |
| --- | ---: | ---: | ---: | ---: |
| Upload v1 | 133610480 | 764134592 | 1071882840 | 232783 / 1055649 |
| Upload v2 | 89836160 | 1282138440 | 2402007944 | 318160 / 719726 |
| Upload v2 aggregation | 193563360 | 1385364784 | 2881004392 | 334410 / 751251 |
| Download v1 | 126084240 | 729928896 | 892669200 | 226969 / 785147 |
| Download v2 | 86784320 | 1239007888 | 2170114224 | 307764 / 375345 |
| Download v2 aggregation | 172336480 | 1257441120 | 2456393512 | 304087 / 383376 |

## Pressure And Host Limits

Receiver deltas use the aligned cost window; ending gauges are current
snapshots. Bundles count authenticated handler attempts, including retries.
Completed groups have admitted fragments into original reassembly and need
not immediately yield business delivery.

| V2 direction/profile | Bundles | Completed groups | New-group / original / scratch rejects | Expired groups | End groups / originals | End credit bytes |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Upload v2 | 330635 | 66123 | 0 / 0 / 0 | 0 | 10 / 0 | 1231318 |
| Upload v2 aggregation | 326092 | 65218 | 0 / 0 / 0 | 0 | 0 / 1 | 1089082 |
| Download v2 | 319431 | 63886 | 0 / 0 / 0 | 0 | 1 / 0 | 172993 |
| Download v2 aggregation | 295415 | 59083 | 0 / 0 / 0 | 0 | 0 / 0 | 158143 |

Every sampled v2 endpoint, including warmup, has zero rejection/expiry counters
and zero decoded-pending groups. Receiver pending-group sample maxima are
24 / 3 / 74 / 25 in table order. Credit includes standing send claims and all
other Peer obligations; it is not RSS or group-specific attribution. Sample
maxima are not guaranteed high-water bounds.

All aligned endpoint admission backpressure/retry/wait/timeout/cancellation
counters are zero. The baseline aggregation upload recorded six backpressured
packets and six retries. This single unsaturated run does not establish a
general absence of admission pressure.

Aligned dispatcher ingress, adapter and delivery drop counters, socket write
errors/oversize drops and endpoint host UDP error deltas are zero. The baseline
v1 upload recorded 4683 dispatcher drops and overlapping UDP `InErrors=112`,
`RcvbufErrors=112`. New-run data-interface link/qdisc drop/error deltas are zero;
management-interface drops remain in the evidence. Different counter scopes
and endpoint boundaries do not establish an exact packet-loss identity.

Hypervisor mean idle is only 8.18%..12.05%. Hypervisor swap activity is ten pages
in v2 upload, 512 in v2 download and one in aggregation download; other host/case
swap counts are zero. Host summaries use 14 complete interior intervals and
cover all processes; network snapshots bracket setup through drain. These
limits prevent interpreting the run as isolated network capacity.

## Replay And Publication

Use the [measurement guide](v2-measurement.md) at the measured source with the
recorded six-case parameters. The [public archive](v2-send-workspace-20260905.tar.gz)
contains 144 original public records, regenerated report/audit and the original
and public checksum indexes. Original JSONL and `report.json` retain exact
allocation totals, timestamps, selected receiver-byte denominators and rates.
No new microbenchmarks or profiles were captured for this change.

The audit checks source/binary/runner identities, the exact public inventory,
all original checksums, endpoint/host reports, receive counters/gauges and
cleanup. It scans decompressed members for private credentials and probe-frame
material, then verifies deterministic archive bytes and exact member roundtrip.
Verify `PUBLIC_SHA256SUMS` after extraction; `SHA256SUMS` verifies original
records. Private configurations, keys, `.lab`, executable payloads and private
profiles are excluded. Lab addresses and host metadata remain public.

Recorded cleanup verifies all 60 owned units stopped and both endpoint
workspaces removed. Independent read-only checks confirm all 12 named units
per endpoint inactive with MainPID 0 and owned workspaces absent. Router and
hypervisor cleanup is verified from the recorded evidence.

```text
diagnostic archive bytes: 1044261
diagnostic archive SHA-256: fce383a7a80348631a62aadc4d6a587ee8cc19903fa288d39412c760d49c91db
```

The >=250 Mbit/s, >=90% capacity, three 300-second rounds, native KCP, repair,
mux and fault/MTU acceptance gates remain open.
