# Isolated Linux RX Prototype, 2026-09-05

[Curated raw evidence](rx-prototype-20260905.tar.gz), SHA-256:
`2ed200088f5e1800ee7a59339b5e9fd557767c09348579980b60fb8ac1542eb8`.
The archive contains synthetic fixture source, raw samples, hardware metadata,
and receiver-only trace histograms; no product configuration or `.lab` workspace.

Fixture source: `f9782a7831e6f7411f36378b7503a23c565e9fc4` on
`perf/19-rx-fixture`, based on `main` at
`3cf520e712e82d9a44a999a7b45a7ce994c1ce9b`.
Fixture code is confined to `integration/perf`. Production transport and its
dependency graph are unchanged. Existing perf-module x/net v0.47.0 becomes a
direct requirement.

The fixture preserves owned payload copies and local/remote address snapshots,
checks packet sequence/length/body/source, and retains a fixed 32-packet ring.
Its sender runs in a separate process. Each sender burst is fully queued before
the receiver begins draining it. These are controlled loopback drain measurements,
not production ingress, FEC/authentication, network capacity, or application
throughput/latency measurements. Connected Carrier and IPv6 are not covered.

## Environment And Reproduction

Go 1.26.4, Linux amd64, kernel `6.12.94+deb13-cloud-amd64`, Intel Xeon
E3-1245 v5, GOMAXPROCS 6. Hardware details are retained in `raw/lscpu.json`.
Payload 551 bytes, requested SO_RCVBUF 256 KiB, observed kernel value 425984.
All cases warm 1024 packets. Other agent CPU work was held during timing.

```sh
cd integration/perf
go build -ldflags '-X main.sourceSHA=f9782a7831e6f7411f36378b7503a23c565e9fc4' -o /tmp/mpudp-rxprobe-f9782a7 ./cmd/rxprobe
/tmp/mpudp-rxprobe-f9782a7 -mode scalar -batch 1 -packets 262144 -payload 551 -burst 32
/tmp/mpudp-rxprobe-f9782a7 -mode batch -batch 8 -packets 262144 -payload 551 -burst 32
```

Burst-32 samples include scalar, batch1, batch8 and batch32. Orders were
scalar/1/8/32, 32/8/1/scalar, then 8/scalar/32/1. Sparse burst-1 samples use
16384 packets, with orders scalar/8/32, 32/8/scalar, then 8/scalar/32.
Raw JSONL includes each sample's order and all output fields.

## Untraced Results

Medians of three samples; no confidence-interval/significance claim is made.
Active drain PPS includes read, copy, snapshots and collection/validation, but
excludes sender preparation and pipe coordination. Wall PPS includes coordination.
Receiver CPU and allocation totals include coordination and collection but exclude
the separate sender process. Trace runs below are excluded from these medians.

| Prequeued burst | Receive mode | Packets/API call | Active drain PPS | Wall PPS | Receiver CPU ns/packet | B/packet | Allocs/packet |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | scalar | 1 | 335721 | 105232 | 8181 | 744.1 | 7.000 |
| 32 | batch1 | 1 | 292819 | 99296 | 8741 | 744.3 | 7.002 |
| 32 | batch8 | 8 | 534161 | 120248 | 6879 | 740.5 | 7.002 |
| 32 | batch32 | 32 | 570572 | 114702 | 6997 | 741.3 | 7.002 |
| 1 | scalar | 1 | 245837 | 34781 | 24818 | 745.4 | 7.003 |
| 1 | batch8 | 1 | 204011 | 35079 | 24887 | 746.0 | 7.005 |
| 1 | batch32 | 1 | 167638 | 32052 | 27097 | 746.5 | 7.005 |

Batch8 improves prequeued drain PPS by about 59% and reduces receiver CPU per
packet by about 16%. Sparse drain PPS instead drops about 17% (batch8) and 32%
(batch32). Batch1 also loses to scalar receive. This supports evaluating batching
where real RX occupancy is sufficient; it does not justify unconditional adoption,
an optimal batch-size claim, or a production throughput target. Payload/address
ownership costs remain, so allocations stay approximately seven per packet.

## Separate Receiver-Only Syscall Proof

The optional helper uses only a uniquely owned tracefs instance. After fixture
warmup it stops the receiver, seeds every existing receiver TID, enables event-fork
tracking for new threads, and releases the ready barrier. Histograms count kernel
entries/exits by TID and FD/return value. Sender threads are excluded. Raw event
formats and histograms are retained under `trace/`.

For 4096 verified packets at burst 32:

| Receive mode | recvfrom entries | recvmmsg entries | Successful datagrams |
| --- | ---: | ---: | ---: |
| scalar | 4096 | 0 | 4096 |
| batch8 | 0 | 512 | 4096 |
| batch32 | 0 | 128 | 4096 |

Each trace reports valid=true, zero histogram drops, zero unexpected receive FDs,
zero EAGAIN, matching entry/exit totals, and successful datagrams equal to the
fixture count. New receiver TIDs not present in the initial seed were captured.
Every owned tracefs instance was removed, and no fixture processes remained.
Counts cover barrier release through receiver exit; warmup is excluded and the
fixture schedules no receive after the measured phase. API summaries correctly
leave `syscall_count_available` false; syscall proof is a separate artifact.

The first tracing setup attempt failed before measurement because O_TRUNC on
`trigger` unregisters existing histogram triggers before a resume command.
Cleanup succeeded. The final helper at `846f33eb865b4c9ac809499581c186a0dfb5099e`
uses append mode for trigger commands and was then validated by all three
successful traces. This helper-only correction does not change the measured
receiver source or binary.

## Verification And Binding

Focused unit tests, race tests, vet, and `go mod tidy -diff` passed. Coverage includes
owned copies/address independence across buffer reuse, exact-limit/+1/truncation,
Close unblocking, independent sender PID, single-packet/tail bursts, integrity
rejection, and failed/stalled sender and ready-timeout cleanup. Root statically
reviewed the fixture and found no blocker.

```text
98b6adb49997a2a18b353a1704ba4736a18f4126d6cd2febe4a3fe81c8f17318  measured binary
5c6c2127aa94a5225aaf502a118e07d266b097848ae0ca0dd75ff2fd384efbcc  raw/mpudp-rx-f9782a7-timing.jsonl
ce10e3091ce5a72d5b8af644edf94adcf53183f06ccca7c5df45e1fbba18406a  raw/mpudp-rx-f9782a7-sparse.jsonl
ef045daea9babf6388c5aff5851ae782dba5cbc2431f47da0a507144596049e9  source/main_linux.go
8640ef70df0903aad3dcae1c6d6918271069b98323e5c9c76b8fe78d472eba73  source/main_linux_test.go
94c5744240a59d10c52ecd7eb8c053a3440e9f9cce8bf3f46362d0887cbccb0d  source/rx-tracefs-count.py
```

`SHA256SUMS` binds every evidence file. The measured binary is retained locally
at `/tmp/mpudp-rxprobe-f9782a7`; source and its exact commit reproduce it.
