# Bounded Authentication Cache, 2026-09-05

This #19 increment reuses HMAC working state while preserving the v1 wire
format, validation order, immutable key ownership and packet ownership. Warm
codec operations remove 512 bytes and six allocations per packet in the local
microbenchmark. The network comparison remains a short diagnostic run on a
shared host with unproven processing headroom.

```text
before source: 60098fe6e1c7d79bda3754ce3147d995991ecda2
before binary SHA-256: d819731eceb9fc60548f13b51603e531deb691f89c7a92f600a47c1edd3d3a48
after source: eee4883837ae14461931bae6fb76176852591f25
after binary SHA-256: 0951d94f37024aba08c5897bc3288df21c9f8453c9d0e3a176681bb4a749346b
after run: mpudp-auth-diagnostics-eee4883
source checkout and runner: clean
```

The [before run](fec-replay-window-20260905.md) already contains the #18 replay
window. Its matrix, workload parameters, topology and runner script hashes
match this run. The historical `baseline_sha` field in each raw manifest points
to the imported original baseline; the comparison here uses `60098fe`.

## Ownership And Retention

Each normalized configuration owns a copied PSK and retains at most four idle
HMAC states. Listener Sessions share their configuration. An operation borrows
one state exclusively and resets a cached state before reuse; an empty cache
creates a fresh HMAC without waiting. Returned states clear private digest
scratch and are discarded when the cache is full. The four-state limit bounds
retained storage; transient state follows existing caller concurrency.

Encoding writes the tag into the caller's output packet. Decoding compares
private digest scratch before returning the state and preserves the existing
payload alias into the input datagram. The cache retains no references to
caller-owned key or packet buffers. It introduces no goroutine, admission queue,
channel close or secure-memory-erasure guarantee. Closing one listener Session
leaves the shared cache usable by the remaining Sessions.

## Codec Measurements

Go 1.26.4, linux/amd64, Intel Xeon E3-1245 v5. Values are medians of three
200 ms runs with GOMAXPROCS 1, except the explicitly parallel rows use 4.
Other agent CPU work was paused during timing. These small samples measure
codec costs and do not establish network throughput improvements.

| Shard bytes / operation | Before ns/op | After ns/op | Before B/op / allocs | After B/op / allocs |
| --- | ---: | ---: | ---: | ---: |
| 480 / encode fresh | 3265 | 2495 | 1088 / 7 | 576 / 1 |
| 480 / encode existing capacity | 3133 | 2165 | 512 / 6 | 0 / 0 |
| 480 / decode | 3014 | 2307 | 512 / 6 | 0 / 0 |
| 480 / decode parallel, 4 | 1019 | 580.9 | 512 / 6 | rounded 0 / 0 |
| 1129 / encode fresh | 5132 | 4165 | 1792 / 7 | 1280 / 1 |
| 1129 / encode existing capacity | 4691 | 3927 | 512 / 6 | 0 / 0 |
| 1129 / decode | 4885 | 3903 | 512 / 6 | 0 / 0 |
| 1129 / decode parallel, 4 | 1208 | 997.7 | 512 / 6 | rounded 0 / 0 |

At 480 shard bytes, forced cache misses measured 3226 ns/op and 1104 B/op with
seven allocations, including a fresh output packet and the benchmark's cache
eviction operation. This preserves the stateless allocation count with 16 extra
bytes. Warm results do not promise zero allocation on first reuse or at arbitrary
concurrency.

The benchmark implementation was committed as `b558b0f` and rebased to
`eee4883`. All seven measured implementation/benchmark source hashes match both
commits. The extracted baseline codec matches `60098fe` and uses the identical
`codec_benchmark_test.go` harness. The archive's `audit.json` records these
source hashes and binds the two final raw logs:

```text
codec-baseline-60098fe.txt
8ad493cea99ae69cb1f2511e93b8be2fd2a2a8a6645c8d4bd2818638bf3899b1
codec-auth-cache-reset-on-acquire.txt
7dea5f32a0a2f604e6d76496e092f1cc36cb6f5be126a7a221a1c37b505fca36
```

## Network Comparison

Both runs use five independent 100 Mbit/s paths, 20 ms one-way netem delay,
one business flow, 1400-byte messages and 1360 receiver-verified body bytes per
message. MPUDP uses RS(3+2), a 1200-byte complete UDP payload budget, 4096-entry
queues and 8192 pending blocks. KCP uses MTU 1400, window 1024, no KCP FEC,
nodelay `(1,10,2,1)` and delayed ACKs. KCP-over-MPUDP is an experimental probe
stack. Physical MTU is 1500 and offered rate is unlimited.

Every case has three seconds of warmup and 15 seconds of steady load, one round.
CPU, heap, allocation, mutex and block profiles are enabled in both timing
diagnostics modes in both runs. Timing off therefore remains instrumented.

| Protocol | Direction | Timing | Before Mbit/s | After Mbit/s | Before worst 5 s | After worst 5 s |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| MPUDP Datagram | Upload | Off | 69.953 | 64.134 | 66.877 | 59.383 |
| MPUDP Datagram | Upload | On | 58.217 | 62.590 | 55.669 | 58.887 |
| MPUDP Datagram | Download | Off | 60.050 | 61.017 | 56.532 | 57.468 |
| MPUDP Datagram | Download | On | 61.145 | 57.969 | 59.152 | 56.519 |
| KCP-over-MPUDP | Upload | Off | 25.603 | 29.815 | 24.160 | 27.751 |
| KCP-over-MPUDP | Upload | On | 26.970 | 31.407 | 25.065 | 27.104 |
| KCP-over-MPUDP | Download | Off | 25.926 | 28.582 | 23.296 | 26.206 |
| KCP-over-MPUDP | Download | On | 25.118 | 27.753 | 21.116 | 26.047 |

After-run host mean idle ranges from 6.44% to 9.57%, and its minimum sampled
idle reaches 3.54%. KCP upload/on records four swap pages and KCP download/off
two; the remaining case windows record zero. The shared host and sequential
single-round comparison prevent assigning these throughput differences solely
to the cache. Formal acceptance remains false, and no new native-capacity or
KVM polling experiment is included.

For the timing-on KCP cases, runtime `TotalAlloc` initial/final deltas also
decrease. These are cumulative process allocation counters over the wider
initial/final interval, including warmup and teardown. Each run transferred a
different amount of data; these totals are not allocation-per-byte estimates.

| Direction / process role | Before allocated MiB | After allocated MiB |
| --- | ---: | ---: |
| Upload / sender client | 961.021 | 776.976 |
| Upload / receiver server | 1320.387 | 1193.077 |
| Download / sender server | 915.885 | 712.922 |
| Download / receiver client | 976.826 | 782.529 |

## KCP Counter Scope

The following timing-on sender values preserve the raw initial/final snapshots.
Trace PUSH and ACK counts are observed at the MPUDP Datagram adapter call.
Other commands are derived as `outbound_header_bytes / 24 - PUSH - ACK` and
are zero in these snapshots; malformed trace packets are also zero.

| Direction / snapshot | KCP OutSegs | Trace PUSH | Trace ACK | Other | LostSegs | RetransSegs | Adapter errors | Adapter write drops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Upload / initial | 4 | 4 | 0 | 0 | 2 | 2 | 2 | 2 |
| Upload / final | 125368 | 124645 | 85 | 0 | 27778 | 27884 | 8 | 3 |
| Download / initial | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Download / final | 114687 | 113217 | 92 | 0 | 24195 | 24195 | 24 | 1 |

Upload's `OutSegs` delta is 125364 versus 124726 traced segments, a difference
of 638. Download's delta is 114687 versus 113309, a difference of 1378. The
corresponding before-run differences were 768 and 731. These are observations
across separately sampled counters at different pipeline stages. They are not
exact counts of pre-adapter drops and do not establish their cause. The pinned
KCP postprocessing queue and shutdown behavior require separate instrumentation
to attribute missing output.

Upload records 27882 retransmissions during this initial/final interval:
27776 `LostSegs`, 105 fast and one early retransmission. Download records 24195,
all `LostSegs`. In pinned kcp-go v5.6.72, `LostSegs` identifies the RTO retransmit
branch rather than proven network loss. Adapter error deltas are six and 24;
recoverable errors counted by `adapter_write_drops` increase by one in each
direction and are already included in adapter errors. The snapshots include
warmup and shutdown around the steady window. No exact RTO cause is established.

## FEC And Evidence Audit

All eight cases preserve zero final completed-cache evictions, decoder-full
and `TooOldShards`. Final pending blocks/shards/bytes are zero. Receiver pending
block lifetime peaks range from 124 to 439. Some upload cases retain ingress
drops and expired blocks; this cache increment does not remove those bottlenecks.
All receiver integrity counters and final delivery-drop counters remain zero.

All 128 before/after network snapshots surround their measurement windows.
Both-side source/options, exchanged summaries, unique receiver body bytes,
per-second throughput, worst-five-second accounting, RTT and telemetry were
independently revalidated. Cleanup records show all 80 owned transient units
inactive with MainPID 0 and both remote workspaces removed. The seven existing
VMs, bridges and unrelated services were preserved.

All 80 profiles were gzip-decoded and parsed against the pprof protobuf schema:
16 each of CPU, allocations, heap, mutex and block; 22189 sample records,
22367 string-table entries and 1605 unique strings. Strings are printable ASCII;
the only sample labels are 1870 numeric `bytes` labels. No string labels, unknown
fields, known-PSK matches, private-key markers or probe-frame markers were found.
Allocation profiles contain stack/count/size samples, not heap object-content
dumps. `alloc_space` is a sampling-adjusted cumulative allocation estimate;
profile percentages are not independent additive shares.

[Audited raw records, profiles and benchmarks](auth-cache-20260905.tar.gz)
contain 272 original files: the 189 indexed protocol records and their original
`SHA256SUMS`, 80 explicitly selected profiles and two final benchmark logs.
Lab addresses and system metadata remain present. No other `.lab` content or
private PSK files are included; profiles are exported to a public `profiles/`
path without changing their bytes. The original checksum index is preserved.

The archive has 274 regular members under `auth-cache-20260905/`:

```text
diagnostics/           190 original files, including SHA256SUMS
profiles/<case>/        80 selected .pprof files
benchmarks/              2 original benchmark logs
audit.json               source attribution, allowlist, hashes and outcomes
SHA256SUMS               273 entries; excludes itself
```

Two deterministic builds were byte-identical. Extraction verified all 273
publication checksum entries and all 189 entries in the original index.
Archive size is 2643085 bytes; SHA-256:

```text
39d1a6e7ca5763b1c03fe70d83afb0ff8c3320e4febec89bd5e8fcc38cb49ead
```

## Replay

Use the exact after source with a private PSK outside artifact directories and
fresh output paths. The SSH configuration and hypervisor Python executable are
local prerequisites. All workload flags below match the recorded run.

```bash
go build -C integration/perf -trimpath \
  -ldflags '-X main.sourceSHA=eee4883837ae14461931bae6fb76176852591f25' \
  -o /tmp/mpudp-perfprobe-eee4883 ./cmd/perfprobe
python3 scripts/perf/run-probe.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --hypervisor-python /nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3 \
  --binary /tmp/mpudp-perfprobe-eee4883 \
  --source-sha eee4883837ae14461931bae6fb76176852591f25 \
  --psk-file /private/mpudp-perf.psk --output /tmp/mpudp-auth-replay \
  --paths 5 --protocols mpudp kcp-mpudp --payloads 1400 \
  --rounds 1 --seconds 15 --warmup 3 --diagnostics off on \
  --profiles --host-diagnostics basic
go test ./internal/wire -run '^$' -bench '^BenchmarkCodecStateless$' \
  -benchmem -benchtime=200ms -count=3 -cpu=1,4
go test ./internal/wire -run '^$' -bench '^BenchmarkAuthenticator(Codec|CacheMiss)$' \
  -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

Running `BenchmarkCodecStateless` on the after checkout compares the retained
stateless path. The recorded before log used the exact extracted `60098fe`
codec plus the same benchmark harness. After extracting the evidence archive,
run `sha256sum --check SHA256SUMS` in `auth-cache-20260905/`; the original index
also remains usable inside `diagnostics/`.
