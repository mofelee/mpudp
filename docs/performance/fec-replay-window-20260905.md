# FEC Replay Window Comparison, 2026-09-05

Issue [#18](https://github.com/mofelee/mpudp/issues/18) fixes late shards
reopening completed blocks after cache eviction. This report separates the
deterministic correctness result from the healthy-network smoke comparison.
Neither the short network runs nor this state fix establish #16's performance
acceptance.

```text
before: dd868367148a3eeaa071acfade3f3966ecd5a913
after:  60098fe6e1c7d79bda3754ce3147d995991ecda2
after probe SHA-256: d819731eceb9fc60548f13b51603e531deb691f89c7a92f600a47c1edd3d3a48
after run ID: mpudp-replay-diagnostics-60098fe
```

Both source checkouts and runners were clean. The manifests' historical
`baseline_sha=934a6325...` identifies the original imported v0.1 baseline;
the actual before/after comparator here is `dd86836` versus `60098fe`.

## State-Retention Result

The same deterministic RS(3+2) workload completes 32 known blocks, then releases
their two late parity shards. Each Datagram is 1200 bytes and the pending
capacity is 16 blocks. The legacy completed-cache capacities are varied
independently. These results were replayed on both exact source commits:

| Implementation | Legacy cache capacity | Reopened pending | Pending bytes | Decoder-full | Next new block |
| --- | ---: | ---: | ---: | ---: | --- |
| Before | 8 | 16 | 12800 | 17 | Rejected |
| Before | 16 | 16 | 12800 | 1 | Rejected |
| Before | 32 | 0 | 0 | 0 | Accepted |
| After, fixed window | 8 / 16 / 32 | 0 | 0 | 0 | Accepted |

The fixed window identifies all 64 late shards in this workload. A second
regression completes 65,568 blocks before releasing all parity: `late=131072`,
`too_old=64`, `pending=0`, `decoder_full=0`, and the next block completes.
Raw test output is included in the archive. Reproduce on the corresponding
source checkout:

```bash
go test ./internal/fec -run TestDelayedParityCapacityDiagnostics -count=1 -v
go test ./internal/fec \
  -run 'TestDelayedParityWindowDiagnostics|TestReplayWindowHighBlockRateDelayedParityDoesNotReopen' \
  -count=1 -v
```

Production decoders use a fixed 65,536-ID, 8 KiB bitmap per Session receive
direction. Completed bits survive time-based sweeps. Previously admitted
pending blocks retain their original deadline even after the receive floor
passes them; later shards cannot reopen them after completion or expiry.
Never-admitted IDs outside the window are dropped without retained state or
Endpoint learning. Session recreation does not inherit this history. The
[FEC contract](../FEC.md#解码超时与去重) documents these finite reordering and
lifetime limits; v1 wire bytes are unchanged.

## Same-Workload Network Comparison

The matrix is byte-for-byte identical to the
[before-run matrix](diagnostics-20260905.md). All runner script hashes, topology
and workload parameters match. There are five 100 Mbit/s paths, one Session and
one business flow per case, three seconds of warmup and 15 seconds of steady
measurement. A 1400-byte application frame contributes 1360 receiver-verified
body bytes. Both timing modes include CPU/heap/alloc/mutex/block profiles.

| Protocol | Direction | Timing | Before Mbit/s | After Mbit/s | After worst 5 seconds Mbit/s |
| --- | --- | --- | ---: | ---: | ---: |
| MPUDP Datagram | Upload | Off | 67.929 | 69.953 | 66.877 |
| MPUDP Datagram | Upload | On | 63.392 | 58.217 | 55.669 |
| MPUDP Datagram | Download | Off | 58.112 | 60.050 | 56.532 |
| MPUDP Datagram | Download | On | 60.885 | 61.145 | 59.152 |
| KCP-over-MPUDP | Upload | Off | 29.205 | 25.603 | 24.160 |
| KCP-over-MPUDP | Upload | On | 28.104 | 26.970 | 25.065 |
| KCP-over-MPUDP | Download | Off | 26.422 | 25.926 | 23.296 |
| KCP-over-MPUDP | Download | On | 24.783 | 25.118 | 21.116 |

There is no consistent throughput improvement in this single-round comparison.
The shared host's after-run mean idle ranged from 6.86% to 9.62%; host headroom
remains unproven. These short differences cannot establish a causal speedup,
regression, timing overhead or profile overhead.

Each receiver completed more than 100,000 blocks. After-run receiver pending
peaks were 115-416 blocks, with zero decoder-full and zero `TooOldShards`.
`CompletedCapacityEvictions` is now zero because production no longer uses
the old cache; this counter change alone is not evidence of improved capacity.
Both endpoints recorded zero delivery drops. Receiver ingress drops remain in
MPUDP upload timing off/on (764/1995) and KCP upload timing off (528). The fix
does not explain or resolve all observed queue drops or KCP retransmissions.
Counters cover the initial/final interval including warmup; pending peaks cover
the process lifetime. Throughput and worst-five-second values use only the
receiver's steady window.

## Replay And Evidence

From the exact after source, with the existing lab and a new output directory:

```bash
go build -C integration/perf -trimpath \
  -ldflags '-X main.sourceSHA=60098fe6e1c7d79bda3754ce3147d995991ecda2' \
  -o /tmp/mpudp-perfprobe-replay-window ./cmd/perfprobe
python3 scripts/perf/run-probe.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --hypervisor-python /nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3 \
  --binary /tmp/mpudp-perfprobe-replay-window \
  --source-sha 60098fe6e1c7d79bda3754ce3147d995991ecda2 \
  --psk-file /private/mpudp-perf.psk --output /tmp/mpudp-replay-window-replay \
  --paths 5 --protocols mpudp kcp-mpudp --payloads 1400 \
  --rounds 1 --seconds 15 --warmup 3 --diagnostics off on \
  --profiles --host-diagnostics basic
```

The SSH configuration, hypervisor Python path and raw private PSK are local
prerequisites. The key stays outside the output directory. Formal performance,
future batch/repair lifetime, and MTU epoch regressions remain open.

All eight cases passed the runner's receiver, per-second accounting, RTT and
telemetry validation. All 128 required network snapshots surround measurement;
80 owned transient units ended inactive with MainPID 0, and both temporary
remote workspaces were removed. Existing lab VMs, bridges and services remain.

The [audited archive](fec-replay-window-20260905.tar.gz) contains 272 original
files: 190 protocol records including their original checksum index, 80
explicitly selected profiles, and the two deterministic test outputs. Each
profile was gzip-decoded and protobuf-parsed. The audit found no known PSK
matches or private-key/probe-frame markers; lab addresses and machine metadata
remain included. Other private `.lab` files are excluded.

The archive adds `audit.json` with source/hash mappings and a top-level
`SHA256SUMS`. All 273 new-index entries and 189 original-index entries passed
after extraction. Two independently generated archives were byte-identical.
Run `sha256sum --check SHA256SUMS` inside `fec-replay-window-20260905/` after
extraction. Archive SHA-256:

```text
0b497faabcdfa6dda05cb36321801a8ba9ee95c45c2fd476feb96c0fd660c556
```
