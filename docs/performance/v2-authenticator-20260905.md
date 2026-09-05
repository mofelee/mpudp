# V2 Directional Authenticator, 2026-09-05

[中文版](v2-authenticator-20260905.zh-CN.md)

Prepaid directional HMAC state lowers observed sender and receiver allocation
bytes per verified business byte in all four v2 cases. Throughput remains
mixed: nonaggregated upload decreases from 45.402 to 44.430 Mbit/s, while the
other v2 cases increase. This single short round does not establish a causal
throughput improvement, spare host capacity or formal performance acceptance.

```text
before source: 5e19e44a6ea0d9e6a084129ebfe2075e4881ae00
measured after source: f6bc4322dfe7b22f5cc29cf07df737ebf829e032
after probe SHA-256: 6c4f927c63d61e603de9e27e6cebad3e6e380c125ed641ab8cb0207e94bc075f
diagnostic run: mpudp-auth-diagnostics-f6bc432
```

The measured source identifies the executable even if this report is published
from a later documentation or merge commit.

## Change And Scope

Each controller owns separate send/receive authenticators, serialized by its
existing owner. FEC encoding uses the send owner; established-packet
authentication uses the receive owner. Each operation resets the keyed hash
state and clears scratch arrays. Stateless wire functions retain local state
for concurrent callers. Wire bytes, exact authenticated prefix/body boundaries,
constant-time tag comparison and borrowed-input/caller-output ownership remain
unchanged. Successful authentication still does not authorize admission.

Two dedicated 4096-byte allowances are added to initial control credit before
hash construction, independently of codec and receive-scratch storage. Including
the two controller pointers, the standing claim increases by 8208 bytes on the
measured amd64 build; the Peer byte ceiling is unchanged. Constructor failure
and Close dispose the new owners before releasing the bound control lease.
Close clears owned key/scratch arrays and drops the hash reference; the standard
hash API cannot explicitly erase opaque internal keyed state.

The allowance was reviewed against native Go 1.24/1.26 HMAC-SHA256 storage,
including native FIPS code. It is not RSS or an alternate-provider heap bound.
The captured microbenchmarks use native Go 1.26.4; they do not establish
BoringCrypto allocation behavior or an explicitly enabled FIPS-mode timing run.

## Same-Workload Diagnostics

The baseline is the [allocation-cost run](v2-allocations-20260905.md). Both use
five 100 Mbit/s paths per direction, 20 ms one-way delay, one Session/flow,
RS(3+2), a 1200-byte UDP budget and 1400-byte originals containing 1360 verified
business bytes. Each case has three warmup seconds and 15 steady seconds, one
round, basic host sampling, and profiles/timing diagnostics disabled. Topology,
workload parameters and runner script hashes match. Source/runner trees were
recorded clean and local test/benchmark load was paused during traffic.

| Direction/profile | Before Mbit/s | After Mbit/s | After worst 5s | After RTT P95/P99 ms |
| --- | ---: | ---: | ---: | --- |
| Upload v1 | 68.381 | 71.736 | 63.637 | 102 / unavailable |
| Upload v2 | 45.402 | 44.430 | 43.692 | 91 / 99 |
| Upload v2 aggregation | 97.377 | 99.665 | 92.365 | 80 / 87 |
| Download v1 | 63.514 | 62.061 | 56.361 | 68 / 109 |
| Download v2 | 42.419 | 45.581 | 42.419 | 73 / 96 |
| Download v2 aggregation | 84.387 | 95.791 | 94.021 | 76 / 126 |

All cases have positive goodput in every steady receiver bucket and no corrupt,
duplicate or too-old business frames. V1 upload has 74/75 on-time RTT responses,
one deadline miss/unanswered request and unavailable P99. All other cases have
75/75 on time. RTT queue misses and write failures are zero. Local drains pass;
local socket completion alone does not prove business delivery.

Every cost window covers receiver buckets 1..15. Endpoint durations range
14.996209..15.004501 seconds; maximum sender/receiver boundary skew is
231.31 ms, within the 250 ms tolerance. Timestamp separation is not a measured
clock offset. CPU percent is relative to one core. Socket PPS includes protocol
traffic. The approximate IPv4 ratio counts each endpoint's sends once and adds
28 bytes per UDP packet; it is not exact physical shaper accounting.

| Direction/profile | Sender CPU % | Receiver CPU % | Sender socket PPS | Receiver UDP ingress Mbit/s | IPv4 bytes / verified byte |
| --- | ---: | ---: | ---: | ---: | ---: |
| Upload v1 | 126.17 | 130.27 | 33,363 | 143.296 | 2.107 |
| Upload v2 | 120.44 | 124.03 | 20,486 | 196.282 | 4.535 |
| Upload v2 aggregation | 121.50 | 125.93 | 21,011 | 202.089 | 2.073 |
| Download v1 | 124.22 | 114.00 | 28,434 | 122.917 | 2.076 |
| Download v2 | 121.36 | 115.58 | 20,927 | 201.307 | 4.516 |
| Download v2 aggregation | 120.79 | 118.54 | 20,371 | 195.685 | 2.092 |

## Allocation Costs

Totals are exact differences of each endpoint's cumulative `total_alloc_bytes`
at the selected telemetry boundaries. Divide by that endpoint's actual elapsed
time for B/s, or by the selected receiver buckets' verified business bytes for
B/verified B. Startup before the window is excluded. Boundaries differ slightly
between endpoints; these are window cost ratios, not per-byte causal
attribution. End heap and process-lifetime maximum RSS are separate gauges.

Each table entry is before / after. Ratios are shown to six decimals; MB/s means
decimal allocation bytes/s divided by 1,000,000. Original endpoint snapshots,
precise timestamps, verified-byte denominators and allocation/malloc rates are
preserved in the diagnostic archive's JSONL and regenerated `report.json`.

| Direction/profile | Sender B/verified B | Receiver B/verified B | Sender allocation MB/s | Receiver allocation MB/s |
| --- | ---: | ---: | ---: | ---: |
| Upload v1 | 5.727741 / 5.790486 | 8.011079 / 8.059666 | 48.959 / 51.936 | 68.472 / 72.262 |
| Upload v2 | 16.609392 / 14.549383 | 28.742896 / 26.745336 | 94.262 / 80.803 | 163.126 / 148.546 |
| Upload v2 aggregation | 8.285257 / 7.278251 | 15.875532 / 14.916748 | 100.849 / 90.674 | 193.240 / 185.853 |
| Download v1 | 5.777556 / 5.765799 | 7.070338 / 7.059426 | 45.870 / 44.728 | 56.131 / 54.774 |
| Download v2 | 16.618711 / 14.481747 | 27.013862 / 25.006328 | 88.097 / 82.503 | 143.241 / 142.514 |
| Download v2 aggregation | 8.456023 / 7.332727 | 15.333237 / 14.190828 | 89.202 / 87.783 | 161.767 / 169.868 |

For example, aggregation download receiver allocation MB/s increases while
B/verified B decreases, because this run delivers more verified work. V1 is an
unchanged implementation control, not a repeated-trial variance estimate.

New-build exact totals and denominators permit direct checking of the ratios:

| Direction/profile | Verified business bytes | Sender allocation bytes | Receiver allocation bytes | Sender / receiver mallocs/s |
| --- | ---: | ---: | ---: | ---: |
| Upload v1 | 134505360 | 778851368 | 1084068248 | 237504 / 1066668 |
| Upload v2 | 83305440 | 1212042736 | 2228032008 | 344743 / 667997 |
| Upload v2 aggregation | 186872160 | 1360102472 | 2787524832 | 373634 / 727446 |
| Download v1 | 116364320 | 670933312 | 821465256 | 208775 / 723659 |
| Download v2 | 85465120 | 1237684224 | 2137168824 | 352380 / 369816 |
| Download v2 aggregation | 179608400 | 1317019304 | 2548791920 | 362220 / 397173 |

## Receive Pressure And Host Limits

Receiver event deltas below use the aligned window; ending gauges are current
snapshots. Bundles count authenticated handler attempts, including retries;
completed groups have admitted fragments into original reassembly and need not
immediately yield business delivery.

| V2 direction/profile | Bundles | Completed groups | New-group / original / scratch rejects | Expired groups | End groups / originals | End credit bytes |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Upload | 306630 | 61326 | 0 / 0 / 0 | 0 | 0 / 0 | 1081624 |
| Upload aggregation | 315741 | 63146 | 0 / 0 / 0 | 0 | 1 / 1 | 1099778 |
| Download | 314461 | 62894 | 0 / 0 / 0 | 0 | 0 / 0 | 149949 |
| Download aggregation | 305757 | 61154 | 0 / 0 / 0 | 0 | 0 / 1 | 196813 |

Every sampled v2 endpoint, including warmup, has zero rejection/expiry counters
and zero decoded-pending groups. Receiver pending-group sample maxima are
5 / 4 / 31 / 8 in table order. Credit includes the additional standing HMAC
claim and all other Peer obligations; it is not process RSS or group-specific
attribution. Sample extrema are not guaranteed high-water bounds.

Aggregation upload records six sender backpressured packets, six rejected
admission attempts and six retries, with 4.543567 ms aggregate wait and no
timeout/cancellation. Other sampled sender admission counters are zero. These
are distinct from receiver admission rejection counters.

V1 upload records 4683 receiver dispatcher ingress drops and server host UDP
`InErrors=112`, `RcvbufErrors=112`; those overlapping kernel counters must not
be added. Other endpoint UDP error deltas and all v2 dispatcher ingress drops
are zero. The baseline had 560 v1 download dispatcher drops. Raw socket write
errors/oversize drops, adapter/delivery drops and sender dispatcher drops are
zero. Data-interface link/qdisc drop/error deltas are zero; nonzero management
interface drops remain in the evidence. Different counter scopes and
sender/receiver boundaries cannot establish an exact packet-loss identity.

Hypervisor mean idle is only 7.91%..11.62%. Swap activity is seven pages during
aggregation upload, six during v1 download, 178 during v2 download and two
during aggregation download; other host/case swap counts are zero. Host
summaries use 14 complete interior intervals and cover all host processes;
network snapshots bracket setup through drain. These observations and the v1
upload loss prevent interpreting the run as isolated network capacity.

## Isolated Native Benchmarks

The benchmark archive preserves 12 original files and 200 observations: 15
stateless cases on each source and ten directional/construction cases, each
repeated five times for 200 ms. Fixtures and production patches were reconstructed
against the exact sources. The baseline adds only the envelope-authentication
benchmark; the existing FEC benchmark is unchanged. The refactored stateless and
warmed owner cases use the same optimized test executable.

| UDP1200 operation | Original stateless | Refactored stateless | Warmed directional owner |
| --- | --- | --- | --- |
| Nil-destination encoding B/op, allocs/op | 1848, 9 | 1848, 9 | 1280, 1 |
| Authentication B/op, allocs/op | 544, 7 | 544, 7 | 0, 0 |
| Encoding median ns/op | 5628 | 5201 | 4539 |
| Authentication median ns/op | 5029 | 5110 | 3697 |

Cold owner construction plus Close separately records 848 B/op and nine
allocations/op. Stateless object counts match across all shapes. Bytes also
match except original maximum-size `reused_prefix_0`: 66105 B/op three times
and 66104 twice, versus refactored 66104 five times. All observations, medians
and ranges are retained. Warmed authentication uses valid FEC bundle bodies;
the stateless authentication fixture uses an equal-length opaque body.

Captured binary hashes and read-only build metadata were independently
rechecked: Go 1.26.4, Linux/amd64, CGO_ENABLED=1, GOAMD64=v1, empty GOEXPERIMENT.
Recorded GOMAXPROCS is unset; every benchmark suffix reports runtime value six.
The binaries contain no VCS source attestation, so source association and
command sequencing remain producer-recorded, not a reproducible-build proof.
Exact build/run commands and hashes are included. Executable binaries are not
published. No benchmarks were rerun by the auditor. Short local timing samples
exclude owner locks, transport, pacing and completion workflows and do not
predict network throughput or provider-independent allocation behavior.

## Replay And Publication

Use the [measurement guide](v2-measurement.md) at the measured source with the
recorded six-case parameters. The benchmark archive README and original replay
note provide exact commands and source/fixture reconstruction instructions.

- [Diagnostic archive](v2-authenticator-20260905.tar.gz): 144 original public
  records, regenerated report/audit and original/public checksum indexes.
- [Benchmark archive](v2-authenticator-benchmarks-20260905.tar.gz): 12 original
  inputs, exact fixture sources, source/binary metadata audit and 200 observations.

Both archives use explicit member inventories, secret-marker scans and
deterministic metadata. Verify `PUBLIC_SHA256SUMS` after extraction. Diagnostic
`SHA256SUMS` verifies original records; original benchmark checksum files retain
their capture paths, with their verification recorded in `audit.json`. Private
configurations, keys, `.lab`, executable payloads and private profiles are
excluded; lab addresses and host metadata remain.

Recorded cleanup verifies 60 owned units stopped and two endpoint workspaces
removed. Independent read-only rechecks confirm all 12 named units per endpoint
inactive with MainPID 0 and owned workspaces absent. Router/hypervisor cleanup
is verified from the recorded evidence.

```text
diagnostic archive bytes: 1040770
diagnostic archive SHA-256: 9264ef449625bbec32d02d986142b0b6e35e58868c811a78c18de96b9a37c195
benchmark archive bytes: 29193
benchmark archive SHA-256: b00904608c9f0b34561e00209f05ea78f3e1e4859f1b268f895b808e8785b3f7
```

The >=250 Mbit/s, >=90% capacity, three 300-second rounds, native KCP, repair,
mux and fault/MTU gates remain open. This report establishes the captured
allocation evidence and its limits, not formal performance acceptance.
