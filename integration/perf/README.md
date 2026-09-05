# Performance Probe

This Linux benchmark module isolates the experimental kcp-go adapter from the
MPUDP library dependency graph. It can also build against the v0.1.0 regression
source by changing the local `replace` target in a disposable copy of this
module. It detects the optional MPUDP statistics API at runtime; absent counters
are explicitly marked unavailable.

Build and verify:

```sh
cd integration/perf
go test -race ./...
go build -ldflags "-X main.sourceSHA=$(git rev-parse HEAD)" -o perfprobe ./cmd/perfprobe
```

One-shot native example, with the server command running on the receiving host:

```sh
./perfprobe -mode server -protocol tcp -control 0.0.0.0:18999 -address 0.0.0.0:19000
./perfprobe -protocol tcp -control 192.0.2.1:18999 -address 192.0.2.1:19000 -direction upload -flows 1 -warmup 10 -seconds 300 -id example
```

For `mpudp` or `kcp-mpudp`, pass `-config` on each host. MPUDP data sockets use
that configuration; `-address` only applies to native TCP, UDP, and KCP. The
client always initiates the control connection and data flows, including for
downloads. The server obtains measurement options from the client and exits
after one run. Control retries and startup/measurement deadlines are bounded.
Control messages are for an isolated benchmark network and are not authenticated;
do not expose the control listener to an untrusted network.

Each native process uses one destination path. Multiple native flows share that
path; UDP uses consecutive ports beginning at `-address`, while TCP and KCP use
one listener port. Run independent native processes concurrently to calibrate
multiple physical paths. Each MPUDP flow uses one Session across all configured
Carriers. `-flows 1` therefore measures one application flow and one MPUDP
Session, including RTT probes carried inside that same Session. Path counts are
configured counts, not evidence that every path was live or fully utilized.

`-payload` is the application message size, including a 40-byte synthetic verifier
header. The receiver validates the run, flow, body pattern, and CRC32C, then
deduplicates sequence numbers using a fixed 65,536-packet window. It counts only
the remaining body bytes. Duplicate, corrupted, and packets reordered beyond
the deduplication window do not contribute to effective throughput. Retained
memory is bounded by flow count, message size, and the requested measurement
duration. Native UDP supports messages through 65,507 bytes; MPUDP Datagram
messages must fit the configured local `max_datagram_size` and negotiated peer
limit. Larger reliable-stream messages are segmented by TCP or KCP.

`-kcp-mtu` is the complete KCP UDP payload or MPUDP Datagram size, independently
of `-payload`. The pinned kcp-go version caps it at 1,500 bytes; the probe rejects
higher requests instead of reporting a silently clamped value. The KCP setup is
stream mode, a default 1,024-segment send/receive window, `nodelay=1`, a 10 ms
interval, fast resend 2, congestion control disabled, no KCP FEC, and delayed ACKs
unless `-ack-no-delay` is set. Transient MPUDP send failures become counted
adapter drops so KCP can recover from them.

The receiver owns the monotonic warmup and steady-state windows. Counts are
assigned at validation time to exact one-second buckets, excluding packets
outside those windows. Each host emits JSONL metadata, per-second samples, its
local summary, and a `remote_summary` containing the opposite host's summary.
Receiver summaries are the throughput authority for both upload and download.
The worst-five-second value is null for runs shorter than five steady seconds.
Echo RTT follows the receiver's steady-state clock with exactly five scheduled
opportunities per second per flow. A one-slot probe queue records submission
misses under blocked writes. Replies share the bulk sender's write mutex, so
stream head-of-line blocking remains in the measurement without a second
Session. Independent bounded probe IDs prevent bulk sequence advances from
discarding slow valid replies. RTT includes delay from each scheduled instant,
and records unanswered requests and one-second deadline misses. Quantiles use
all scheduled opportunities, including unanswered requests; ranks reaching
unanswered requests or the >10-second histogram overflow bucket are null. A
one-second drain gives the last opportunity its full deadline while throughput
accounting remains restricted to the steady window.

Native UDP and direct KCP sockets require verified Linux IPv4/IPv6 PMTU discovery
with fragmentation disabled. The regular tests verify socket options and an
IPv6 oversize rejection; `MPUDP_PERF_PRIVILEGED_TESTS=1 go test -race ./...` also
verifies IPv4 `EMSGSIZE` and non-delivery in an isolated network namespace with
MTU 1280. It requires `unshare`, `ip`, and network-namespace privileges and never
changes host interfaces. Optional rate control caps each unpaced burst at
64 KiB or one message when that message is larger; rates through 10 Mbit/s pace
each message independently. Unrepresentable pacing intervals are rejected.

Per-second telemetry includes process CPU, allocation/heap counters, maximum RSS,
optional MPUDP statistics, process-wide KCP SNMP counters, and per-flow KCP
SRTT/RTO. In this pinned kcp-go version, `LostSegs` counts timeout retransmits; it
does not prove physical loss. Snapshot deltas distinguish timeout, fast, and
early retransmits. Sender queue and ACK correlation, host softirq/swap, and
socket/qdisc drops require the external harness and additional diagnostic
evidence; retransmit causes must not be inferred from a single counter.

`-diagnostics` enables optional MPUDP timing and packet-size histograms on both
hosts, plus optional probe-side KCP diagnostics. Per-flow `kcp_correlation`
snapshots contain application write duration for both KCP stacks. For
KCP-over-MPUDP they also validate concatenated 24-byte KCP headers at the existing
Datagram adapter, recording PUSH/ACK/header-byte counts and matching ACK `(sn,ts)`
to at most four attempts in each of 1,024 fixed sequence slots. Only counters and
fixed histograms are exported; payload and identifiers are never exported.
Duplicate ACKs, ambiguous timestamps, unmatched ACKs, and slot/attempt evictions
remain explicit. An attempt eviction or recreation of an older evicted sequence
marks that sequence's history incomplete; subsequent ACKs cannot contribute an
exact timing match, even if the retained timestamps appear unique. They increment
`incomplete_history_acks`. Timing buckets are <=1us, <=2us, ... <=8388608us, then overflow.

`adapter_call` ends when the whole MPUDP `WritePacket` call returns, after its
shard socket attempts. It is not a timestamp of an individual shard socket
write. `entry_to_ack` ends when the adapter receives the ACK Datagram, before
KCP input processing; `return_to_ack` is available only when that ACK follows
the adapter return. ACKs arriving while remaining shards are still being sent
increment `ack_before_adapter_return`. These metrics do not separate network
transit, FEC/delivery queue time, KCP ACK scheduling, or individual socket queues.

Repeated PUSH headers are retransmission candidates with unknown cause; they do
not prove network loss. In pinned kcp-go v5.6.72, the bounded postprocessing
channel can discard KCP output before it reaches this adapter without a
dedicated SNMP counter. Those attempts are invisible here. Fast, early, and
timeout causes remain available only as aggregate SNMP deltas. Cumulative UNA
advances are counted separately, without inventing a timestamped ACK match.
Native KCP packet correlation stays explicitly unavailable: wrapping its UDP
socket would bypass or disable the library's Linux batch-I/O path. No KCP source
or product wire format is changed. Run diagnostics off/on controls to quantify
the overhead before interpreting performance differences.

`-profile-prefix /private/run/profile` enables CPU, alloc, heap, mutex, and
block profiles only on the host where that argument is supplied. Profile files
are created with mode 0600 and never overwritten. JSON metadata includes only
nonsecret configuration fields and dependency/build information. Probe traffic
contains synthetic data; configuration files, PSKs, and private keys must remain
outside shared artifacts.

See [the performance contract](../../docs/PERFORMANCE.md) for topology,
calibration, formal repetitions, MTU scenarios, artifact requirements, and
remaining issue #17 acceptance gates. A successful loopback test is functional
evidence and is not a throughput acceptance result.

## Isolated Linux RX Comparison

`cmd/rxprobe` compares native scalar UDP receive with the existing perf module's
`x/net/ipv4.ReadBatch`. It does not change production transport or dependencies.
The initial fixture covers one unconnected IPv4 loopback socket, with a separate
sender process and at most 32 prequeued datagrams per burst. Every receive uses
an owned payload copy and independent local/remote address snapshots. Sequence,
length, body and sender endpoint checks fail the run on any integrity mismatch.

```sh
go test -race ./cmd/rxprobe
go build -ldflags "-X main.sourceSHA=$(git rev-parse HEAD)" -o /tmp/mpudp-rxprobe ./cmd/rxprobe
/tmp/mpudp-rxprobe -mode scalar -payload 551 -burst 32
/tmp/mpudp-rxprobe -mode batch -batch 1 -payload 551 -burst 32
/tmp/mpudp-rxprobe -mode batch -batch 8 -payload 551 -burst 32
/tmp/mpudp-rxprobe -mode batch -batch 32 -payload 551 -burst 32
```

Repeat and alternate scalar/batch order during a quiet CPU window. Defaults are
1,024 warmup and 262,144 measured packets. `-burst 1` checks sparse traffic;
`-packets 37 -burst 8` checks a partial final burst. Packet count, payload, burst
size and timeout are bounded. The child is joined on success, receive failure,
sender failure and cancellation. A receive error fails the whole fixture; this
is not a proposed production policy for partially returned batches.

Each JSON summary reports packet counts, receive API calls and occupancy,
allocation totals, receiver user/system CPU, and two rates. `active_receive_pps`
includes read, owned copies, address snapshots and bounded collection/validation,
but excludes sender preparation and pipe coordination. `wall_pps` includes that
coordination. Receiver CPU and allocation deltas cover the whole measured phase,
including coordination and collection, but exclude child CPU and allocation.
The reported socket receive buffer is the actual `SO_RCVBUF` value after a
256 KiB request, including Linux's accounting multiplier/cap behavior.

Prequeued batches measure idealized drain cost. They do not measure live batch
occupancy, production authentication/FEC, Peer ingress drops, connected Carrier
receive, IPv6, or application throughput/latency. In particular, Carrier scalar
`Read` currently avoids source-address parsing that `ReadBatch` would add.
Those surfaces require separate evidence before adopting receive batching.

### Receiver Syscall Counts

Normal summaries explicitly report `syscall_count_available: false`: API call
counts are not kernel-entry counts. In pinned `x/net v0.47.0`, `ReadBatch` calls
`internal/socket.Conn.recvMsgs`, then `syscaller.recvmmsg` through `RawConn.Read`.
Each callback performs one `recvmmsg`, but `EAGAIN` and netpoll retries can make
one API call perform multiple syscalls. Scalar `UDPConn.ReadFrom` likewise may
retry `recvfrom`. A full batch therefore establishes packets/API-call only.

The optional stdlib-only Python helper records actual kernel entries in a
uniquely owned tracefs instance, without installed tracing binaries or global
tracer changes. It requires root-equivalent tracefs access and the receive
syscall tracepoints. Run it separately from performance timing:

```sh
python3 scripts/rx-tracefs-count.py --output /tmp/rx-scalar-count -- \
  /tmp/mpudp-rxprobe -mode scalar -packets 4096 -ready
python3 scripts/rx-tracefs-count.py --output /tmp/rx-batch-count -- \
  /tmp/mpudp-rxprobe -mode batch -batch 32 -packets 4096 -ready
```

The ready barrier occurs after warmup. The helper stops the receiver, seeds all
its thread IDs, enables event-fork tracking for new threads, then releases the
barrier. Sender threads remain excluded. Histogram entry/exit counts include
errors and retries from barrier release through receiver exit; no additional UDP
reads are scheduled after the measured phase. Successful datagrams must match
the fixture count, histogram drops must be zero, and entry/exit totals and
socket FDs must agree. Failure leaves `valid: false` with its reason. Cleanup
removes the owned instance on both success and failure. Trace runs perturb CPU
and timing and must never be used as performance samples.
