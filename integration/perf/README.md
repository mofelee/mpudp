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
hosts. `-profile-prefix /private/run/profile` enables CPU, alloc, heap, mutex, and
block profiles only on the host where that argument is supplied. Profile files
are created with mode 0600 and never overwritten. JSON metadata includes only
nonsecret configuration fields and dependency/build information. Probe traffic
contains synthetic data; configuration files, PSKs, and private keys must remain
outside shared artifacts.

See [the performance contract](../../docs/PERFORMANCE.md) for topology,
calibration, formal repetitions, MTU scenarios, artifact requirements, and
remaining issue #17 acceptance gates. A successful loopback test is functional
evidence and is not a throughput acceptance result.
