# Five-Path Diagnostics, 2026-09-05

Run IDs: `mpudp-diagnostics-dd86836` and `mpudp-capacity-polling-dd86836`.
These runs validate complete before/after network snapshots, bounded KCP adapter
correlation, authenticated listener path counters and profile collection. They
are short diagnostic runs, not the three-round, 300-second acceptance matrix.

```text
source: dd868367148a3eeaa071acfade3f3966ecd5a913
probe binary SHA-256: a7d7f52b5e48fa14aa65ef2932d2fccb91922e49f511c44faa37949645648581
source checkout and runner: clean
```

The existing seven-VM lab retains five independent 100 Mbit/s paths in each
direction and 20 ms one-way netem delay. Every protocol case uses one business
flow in one MPUDP Session across five Carriers. Each message is 1400 bytes;
1360 unique, receiver-verified body bytes count toward throughput. MPUDP uses
RS(3+2), a 1200-byte complete UDP payload budget, 4096-entry queues and 8192
pending blocks. KCP uses no KCP FEC, MTU 1400, window 1024, nodelay `(1,10,2,1)`
and delayed ACKs. KCP-over-MPUDP remains an experimental probe stack.

## Throughput And Host Capacity

Each protocol case has three seconds of warmup and 15 seconds of steady load.
CPU, heap, allocation, mutex and block profiles are enabled in **both** timing
diagnostics modes. The off/on rows therefore compare optional MPUDP/KCP timing
with profiling already enabled. Their single short runs on a shared host cannot
establish either timing or profiling overhead.

| Protocol | Direction | Timing | Mbit/s | Worst 5 seconds Mbit/s | Host mean idle % |
| --- | --- | --- | ---: | ---: | ---: |
| MPUDP Datagram | Upload | Off | 67.929 | 64.934 | 7.97 |
| MPUDP Datagram | Upload | On | 63.392 | 61.341 | 8.79 |
| MPUDP Datagram | Download | Off | 58.112 | 55.157 | 9.61 |
| MPUDP Datagram | Download | On | 60.885 | 58.282 | 8.71 |
| KCP-over-MPUDP | Upload | Off | 29.205 | 26.634 | 6.23 |
| KCP-over-MPUDP | Upload | On | 28.104 | 24.154 | 6.54 |
| KCP-over-MPUDP | Download | Off | 26.422 | 24.410 | 6.29 |
| KCP-over-MPUDP | Download | On | 24.783 | 22.974 | 6.95 |

A separate five-path TCP upload calibration used three seconds of warmup and
30 seconds of steady load. Receiver aggregate throughput was **474.006 Mbit/s**;
host mean idle was **4.903%**, with two swap pages. This is five native flows,
not an MPUDP single-flow result. The Xeon E3-1245 v5 host has four cores/eight
threads and also runs the controller and unrelated VMs. Available processing
headroom remains unproven; the shaper's exact packet accounting is not frozen.

Read-only KVM polling snapshots surround that calibration with a wider
98.78-second interval. The summed polling elapsed counter increased by 73.49
seconds and domain CPU by 225.22 seconds. These enclosing-interval values are
not steady-window CPU percentages or proof of a removable bottleneck. VM,
host, kernel and polling settings were not changed.

## FEC And Retransmission Findings

Receiver completed-cache capacity evictions ranged from 93,866 to 115,073.
Decoder-full remained zero on both endpoints in all eight cases, and receiver
pending-block lifetime peaks ranged from 122 to 505. Together with the
deterministic delayed-parity regression, this supports fixing old IDs reopening
pending state in #18. It does not identify that defect as the main bottleneck
in these healthy runs.

For KCP upload with timing enabled, initial-to-final counters record 4,405
receiver ingress drops, zero delivery drops, and maximum observed ingress queue
delay of 166.332 ms. The sender records 27,590 retransmitted segments out of
118,423 outgoing segments; 27,538 are timeout retransmissions. KCP download
with timing enabled still records 24,231 retransmissions out of 107,861 outgoing
segments, with zero observed ingress or delivery drops on either endpoint.
These counters cover the probe's initial/final interval, including warmup;
they are not restricted to the steady throughput window.

The correlation records match bounded adapter `(sn, ts)` attempts and ACKs.
Their boundary is the MPUDP Datagram adapter call, not an individual socket
write. Missing or evicted history cannot be used as an exact ACK match. The
pinned KCP implementation also has a postprocessing-channel drop before the
adapter that its exported counters do not measure. These limitations prevent
attributing every timeout to network loss, application delay or a false timeout.
The main source of the original high retransmission rate remains unresolved.

## Profile-Supported Follow-Up

The timing-enabled KCP upload profiles identify concrete #19 investigation
targets. Percentages below are sampled profile attribution, not independent
additive shares; cumulative entries include their callees. Allocation totals
use `alloc_space`, Go's sampling-adjusted estimate of cumulative allocated bytes.

| Profile | Observation |
| --- | --- |
| Client CPU | Syscalls 47.1% flat; SHA256 7.6% flat; `writeConnected` 50.8% cumulative |
| Client allocations | About 1,040 MiB estimated total; authenticated encoding 46.1% cumulative; FEC encoding 18.0%; `context.AfterFunc` 13.2% cumulative; transport write 20.0% cumulative |
| Server allocations | About 1,414 MiB estimated total; listener read 27.7% cumulative; address cloning 15.0% flat; FEC decoder 23.4% cumulative; HMAC creation 19.8% cumulative |
| Server allocations | `snapshotSessions` 1.9% flat; this alone does not justify calling the dispatcher the primary bottleneck |

Profiles include process setup, warmup and shutdown around the measured load.
Before/after optimization comparisons must preserve the workload and use a new
source/binary hash. Replaying a profile after extracting the archive:

```bash
go tool pprof -top -sample_index=alloc_space /tmp/mpudp-perfprobe-dd86836 \
  profiles/kcp-mpudp-p5-mpudp-upload-b1400-f1-diagon-r1/client-0.allocs.pprof
```

## Replay And Evidence

Build from the exact clean source and use fresh output directories:

```bash
go build -C integration/perf -trimpath \
  -ldflags '-X main.sourceSHA=dd868367148a3eeaa071acfade3f3966ecd5a913' \
  -o /tmp/mpudp-perfprobe-dd86836 ./cmd/perfprobe
python3 scripts/perf/run-probe.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --hypervisor-python /nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3 \
  --binary /tmp/mpudp-perfprobe-dd86836 \
  --source-sha dd868367148a3eeaa071acfade3f3966ecd5a913 \
  --psk-file /private/mpudp-perf.psk --output /tmp/mpudp-diagnostics-replay \
  --paths 5 --protocols mpudp kcp-mpudp --payloads 1400 \
  --rounds 1 --seconds 15 --warmup 3 --diagnostics off on \
  --profiles --host-diagnostics basic
python3 scripts/perf/calibrate.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --hypervisor-python /nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3 \
  --output /tmp/mpudp-capacity-replay --paths 5 --protocols tcp \
  --directions upload --rounds 1 --seconds 30 --warmup 3
```

SSH configuration, the hypervisor Python path and the private raw PSK file are
local prerequisites. The key must remain outside artifact directories.

All 144 initial/final network snapshots completed and surround their respective
measurement windows. The calibration's 18 and protocol run's 80 owned transient
units ended inactive with MainPID 0; both protocol workspaces were removed.
Existing VMs, bridges and services remain intact.

The publication allowlist contains 312 original files: 37 calibration records,
190 protocol records including their original checksum indexes, 80 explicitly
selected profiles, and five read-only polling records/scripts. Profiles were
gzip-decoded and protobuf-parsed before publication. Neither known PSK matched,
and no private-key or probe-frame markers were found in the audited records.
Lab addresses and machine metadata remain present. Other `.lab` files are
excluded.

[Raw records and profiles](diagnostics-20260905.tar.gz) preserve the original
indexes and add a top-level `SHA256SUMS` covering all original files and
`audit.json`. After extraction, run `sha256sum --check SHA256SUMS` inside
`diagnostics-20260905/`; the nested indexes also remain usable in their own
directories. Formal acceptance flags remain false.

Archive SHA-256:

```text
8f90ebd5adee15810c762c78643c55961e619c369faea0c47a1d8bf6ecab3c81
```
