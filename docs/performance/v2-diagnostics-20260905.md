# V2 Datagram Diagnostics, 2026-09-05

[中文版](v2-diagnostics-20260905.zh-CN.md)

The first controlled v2 comparison exposes a severe upload failure under load.
Download with aggregation improves over this run's v1 comparison, but neither
direction meets the performance contract. All acceptance flags remain false.

```text
source: c7d3ab00baf74c84f4864d877f3f5332bcc2f205
probe SHA-256: c0f2823cfd5a0ae26ce886480c57c430b53add580e437ea6abfc9b0976a732fb
run: mpudp-v2-diagnostics-c7d3ab0
source and runner: clean
```

All six cases use the existing five 100 Mbit/s paths per direction, 20 ms one-way
netem delay, one business flow in one Session, RS(3+2), a 1200-byte UDP budget and
1400-byte original messages. Only each message's 1360 verified body bytes count.
Each case has three seconds of warmup and 15 steady seconds, one round. Timing
diagnostics and profiles are disabled. V2 uses fixed/session budgets, repair off,
100 Mbit/s configured rates per path, and either aggregation off or a bounded
250us/32-record aggregation queue. V1 retains its existing direct write path.

## Results

| Direction | Profile | Mbit/s | Worst 5 seconds | RTT P95/P99 ms | Host mean idle % |
| --- | --- | ---: | ---: | --- | ---: |
| Upload | v1 | 65.971 | 65.291 | 85 / 179 | 7.43 |
| Upload | v2 | 0.178 | 0.000 | unavailable | 10.05 |
| Upload | v2 aggregation | 0.309 | 0.000 | unavailable | 10.10 |
| Download | v1 | 62.960 | 60.029 | 101 / 156 | 8.56 |
| Download | v2 | 25.090 | 23.973 | 58 / 67 | 18.67 |
| Download | v2 aggregation | 75.654 | 69.882 | 62 / 102 | 12.68 |

V2 upload has zero on-time replies among 75 scheduled RTT opportunities in each
case; missing quantiles must not be interpreted as low latency. All other rows
have 75/75 on-time replies. The echoes are also 1400 bytes and use 1 ms bins, so
this is not a dedicated low-rate small-packet latency comparison.

The report matches per-second endpoint snapshots to the receiver's full 15-second
steady bucket interval. Maximum timestamp skew is 247.73 ms, within the stated
250 ms tolerance; ratios below are approximate. CPU counts occupied cores, so
100% is one core. Socket PPS includes all raw protocol traffic.

| Direction/profile | Sender CPU % | Receiver CPU % | Forward socket PPS | IPv4 L3 bytes / verified body byte |
| --- | ---: | ---: | ---: | ---: |
| Upload v1 | 124.14 | 121.32 | 30,647 | 2.105 |
| Upload v2 | 121.84 | 135.69 | 20,091 | 1106.846 |
| Upload v2 aggregation | 122.57 | 134.02 | 19,390 | 616.684 |
| Download v1 | 124.30 | 115.89 | 28,937 | 2.083 |
| Download v2 | 122.48 | 83.01 | 11,539 | 4.528 |
| Download v2 aggregation | 121.43 | 111.32 | 16,880 | 2.195 |

The byte ratio counts both endpoints' sends once, adding 28 IPv4/UDP bytes per
packet. It includes control, parity, padding, echoes and unsuccessful business
delivery, and does not establish the shaper's exact accounting. The exceptionally
high upload cost is a failure signal, not useful FEC protection overhead.

## Failure Evidence

V2 upload's initial-to-final counters record 281,648 ingress drops without
aggregation and 289,350 with it. These wider intervals include warmup and drain,
and must not be presented as steady-window counts. The v2 receive path delivers
warmup traffic, then long runs of zero verified steady bytes, with short recovery
near group expiry. The sender reports no admission pressure or send errors;
successful local sends therefore do not establish useful remote delivery.

Both v2 download rows have zero initial-to-final ingress drops. Aggregation lowers
forward packets per verified original to about 2.427, compared with v1's roughly
five. This confirms useful packing in that case, but it is still far below the
250 Mbit/s requirement. Pending-group scans, receive service cost, allocation and
credit retention require investigation. This report does not identify a sole
root cause, prove spare host capacity or claim a causal speedup from one run.

No corrupted or duplicate business frames were counted. Local tail drains passed
on both endpoints; these prove local shard attempts, not remote receipt. The
three-round 300-second performance, native KCP and fault/MTU gates remain open.

## Replay And Artifacts

Use [the measurement guide](v2-measurement.md) with the exact source/binary above,
five paths, `--protocols mpudp --mpudp-profiles v1 v2 v2-aggregation`, both
directions, `--payloads 1400 --flows 1 --rounds 1 --seconds 15 --warmup 3`, and
`--host-diagnostics basic`. The lab used the existing hypervisor Python at
`/nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3`.

[Audited raw records and derived report](v2-diagnostics-20260905.tar.gz) include
144 original public files, the original checksum index, `report.json`, `audit.json`
and a `PUBLIC_SHA256SUMS` index. After extraction, verify both checksum indexes
inside `mpudp-v2-diagnostics-c7d3ab0/`. The audit revalidates all six cases and
host snapshots, scans the known PSK and private-material markers, and verifies
deterministic archive reconstruction. No configuration, PSK, private key, profile
or `.lab` directory is included. Lab addresses and host metadata remain present.

All 60 owned transient units stopped, and both endpoint workspaces were removed.
No VM or hypervisor configuration changed. The later exploratory profile run is
separate and is not part of this archive or the tables above.

```text
archive SHA-256: b1033dcdb48d6b88e0c0254c5e21c12fd5faf6be258129bb4b3f0237259edffd
```
