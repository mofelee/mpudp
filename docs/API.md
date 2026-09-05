# MPUDP Public API

[简体中文](API.zh-CN.md)

The public package is `github.com/mofelee/mpudp`; strict configuration lives
in `github.com/mofelee/mpudp/config`. The data plane exposes complete Datagram
Sessions, without exposing shards, Carriers or upper-layer adapters.

## Startup And Roles

```go
cfg, err := config.Parse(yamlBytes)
if err != nil {
    // errors.Is(err, mpudp.ErrInvalidConfig)
}
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
peer, err := mpudp.NewPeerContext(ctx, cfg)
if err != nil {
    // Partially initialized resources have already been released.
}
defer peer.Close()

if cfg.InitiatorEnabled() {
    outbound, err := peer.NewSession()
    // Handshake is asynchronous; WritePacket returns ErrNotReady initially.
    _, _ = outbound, err
}
if cfg.ListenerEnabled() {
    listener, err := peer.Listener()
    if err == nil {
        inbound, err := listener.Accept(ctx)
        _, _ = inbound, err
    }
}
```

`NewPeer` validates configuration before runtime side effects, binds `listen`
for listener/dual roles, then starts one Peer dispatcher. Initiator-only Peers
do not bind a listener. `NewSession` opens one long-lived UDP socket per
configured Carrier and begins authenticated bootstrap. Failed construction
closes sockets opened by that call and removes partial Session state.
V2 reserves temporary startup credit before Carrier sockets open. DNS and
socket startup run outside the shared Peer mutex; cancellation or failure
releases the temporary reservation and partial socket resources.

Existing v1 Datagram remains supported. Linux also supports explicit v2
Datagram with `transport.mtu_discovery: fixed`,
`transport.budget_strategy: session`, repair disabled, and aggregation either
enabled or disabled. KCP, repair, PLPMTUD, per-Carrier budget, and v2 on other
platforms remain unavailable. Valid unsupported selections return a nil Peer
and `ErrProtocolUnavailable` before accessing runtime context, randomness,
socket/timer dependencies or starting goroutines. Invalid configurations return
`ErrInvalidConfig` first. There is no automatic downgrade or KCP adapter.

`NewPeerContext` allows cancellation of listener binding, Carrier startup and
runtime operations. Callers must still call `Close` to synchronously release
sockets and join dispatcher/receive work. The CLI creates a Peer from `-config`,
creates one outbound Session for initiator/dual mode, and runs until SIGINT or
SIGTERM with controlled cleanup.

Go callers start with `config.Default()` for v1 or `config.DefaultV2(protocol)`
for v2, then provide roles, FEC and PSK. The latter returns configuration, not
resources; only the supported subset above can start. `Validate` does not fill
numeric zeros in Go literals. `Clone` deep-copies directional path budgets and
rate maps. Empty `Protocol` and `Wire.Version` preserve legacy Go literals as
Datagram/v1 without rewriting the object; explicit empty YAML strings reject.
Use `EffectiveProtocol()` and `EffectiveWireVersion()` to inspect defaults.
KCP requires explicit v2 and FEC 0/0, but its runtime remains unavailable.

`NewSession` requires initiator/dual mode; `Listener` requires listener/dual.
Wrong roles return `ErrModeUnavailable`. One dual Peer's `max_sessions` covers
outbound and inbound Sessions together.

## Datagram Sessions

```go
type Session interface {
    WritePacket(payload []byte) error
    ReadPacket() ([]byte, error)
    Close() error
}
```

Each successful read returns a whole original Datagram. Empty Datagrams return
a nonnil zero-length slice. Delivery order is unspecified. Duplicate/late FEC
shards cannot deliver an original twice within one live receive direction.
The bounded 65536-ID window rejects previously unadmitted IDs below its floor;
already admitted pending work keeps its original deadline. Recreated Sessions
do not inherit history. See [FEC](FEC.md#解码超时与去重) for v1 replay boundaries.
V2 keeps independent Completed/Expired histories for original DatagramIDs and
encoding GroupIDs, and only delivers complete originals. A returned slice is
caller-owned and survives later Session closure unchanged.

`NewSession` does not wait for readiness. `WritePacket` returns `ErrNotReady`
until handshake and required v2 path/encoding setup complete. Oversized whole
Datagrams return `ErrMessageTooLarge` before retaining payload, consuming an ID
or sending a prefix. Limits include configured and negotiated receiver bounds
and effective FEC fragment capacity.

V1 and v2 with aggregation disabled complete the original's local encoding and
socket attempts before `WritePacket` returns. This is not remote delivery
confirmation. With v2 aggregation enabled, nil means the complete payload was
copied, metadata/bytes reserved, and its DatagramID committed to the bounded
queue. The caller may reuse its slice. Full admission returns `ErrResourceLimit`
without a partial prefix or retained background waiter. Concurrent admissions
order IDs at queue commit, not call start or socket send.

Capacity, descriptor count, the oldest admission's `aggregation.max_delay`, or
an explicit Flush seals a group. Later writes do not extend that deadline.
Operating-system scheduling and shared dispatcher work can add latency;
`max_delay` is not a hard end-to-end timing guarantee.

V2 Datagram Sessions implement this optional interface; v1 and the existing
three-method `Session` remain compatible:

```go
type DatagramSession interface {
    Session
    Flush(context.Context) error
    CloseGracefully(context.Context) error
}

datagram, ok := current.(mpudp.DatagramSession)
if ok {
    err := datagram.Flush(ctx)
    _ = err
}
```

`Flush(ctx)` captures the already committed admission frontier, seals its tail,
and waits for every original shard through that frontier to finish its local
socket attempt or failure. It excludes later writes and does not wait for
repair, remote ACKs or application reads. Context cancellation stops only that
wait; accepted Datagrams remain owned and may still send. The first asynchronous
send failure stays in the Session for subsequent applicable Flush/graceful
close calls, independently of the lossy `Peer.Errors` channel. Nil context
returns `ErrInvalidConfig`.

`CloseGracefully(ctx)` stops new admission, flushes accepted work, and closes
after success, failure or context expiry. Repair is unavailable, so this does
not wait for remote repair obligations. Repeated calls return the first call's
completed result. Ordinary `Close` may discard unsent
accepted work. Neither method turns local completion into a delivery guarantee.

`ReadPacket` blocks until a complete Datagram arrives or the Session closes.
It has no context argument: close the Session or Peer to cancel a read. Queued
data is not returned after closure. Send failures retain `errors.Is` causes;
one failure can match both a send result and a path-MTU cause.

## Listener Admission

```go
type Listener interface {
    Accept(ctx context.Context) (Session, error)
    Close() error
}
```

`Accept` waits for an authenticated compatible admitted Session, context
cancellation/expiry, or Listener closure. V2 verifies FINISH, promotes reserved
credits and installs components before READY. Pending-accept slots are reserved
during handshake and released only when public Accept takes the Session.
Retries do not enqueue duplicate public Sessions. Nil context is invalid.
`Listener.Close` ends accepted and queued inbound Sessions and wakes Accept
callers; outbound Sessions on a dual Peer remain independent.

## Bounds And Deadlines

One dispatcher and one reusable timer drive each Peer. V1 deadlines cover
HELLO retry, keepalive, Endpoint expiry and FEC sweep. V2 drives handshake and
control retries, aggregation tails and group/original receive deadlines. Each
UDP Carrier can have a receive loop. Transport callbacks enqueue owned packets
without blocking or starting per-packet workers.

| Resource | Capacity | Full Policy |
|---|---:|---|
| Peer packet/recoverable-error ingress | `limits.receive_queue_capacity` | Drop newest event |
| Listener terminal failure latch | 1 | Retain first failure |
| V1 Listener accept | `limits.receive_queue_capacity` | Close/release newest Session |
| V2 Listener accept | `limits.max_pending_accepts` | Reject admission without reserved capacity |
| Session delivery | `limits.delivery_queue_capacity` | Drop newest Datagram and release ownership |

Slow application consumption cannot create unbounded queues or block transport
callbacks. Dropping a Datagram does not enable retransmission or stream
semantics. A separate listener failure latch cannot be hidden by full ingress.
Oversized packets and recoverable transport errors do not alone close a live
socket.

V1 completion history is a fixed 65536-ID/8 KiB bitmap independent of pending
capacity and Endpoint TTL. V2's independent terminal windows, queue backing,
FEC output, receive state and pending deliveries use Session/Peer credits.
Disposal clears storage before returning credit; global byte pressure can
reject admission before the configured Session count is reached.
Credits measure reserved obligations and Peer/Session-owned storage, not
process RSS. Go allocator/GC retention and shared codec lookup tables are
outside these ownership counters.

The current v2 dispatcher is serial and uses bounded synchronous socket
attempts with a 20ms context per attempt. Encoding or sending for one Session
can delay another. There is no per-packet goroutine or unbounded wait queue.
`max_send_workers` and path-queue settings do not promise an implemented
parallel worker pool. Current path selection/rate limits are not the complete
#22 scheduler or fast health detector, and do not establish #16 throughput
acceptance. Repair, MTU probing/migration, KCP and smux remain unavailable.

## Closure And Concurrency

Peer lifecycle/configuration methods, Session reads/writes/Close, and Listener
Accept/Close support concurrent callers. Datagram boundaries remain distinct;
physical send and delivery order are unspecified. Peer, Listener and Session
Close are idempotent and share their first close result.

Closure prevents new work, cancels runtime operations, attempts bounded CLOSE
when possible, releases queued/receive/FEC/path storage, closes sockets and
wakes waiters. V1 uses a bounded one-second close context; v2 socket attempts
use 20ms contexts. Close joins owned background network activity. Authenticated
remote CLOSE also releases the public Session and its initiator Carriers.
V2 Session and Listener closure cancel their associated in-flight sends.

`Peer.Errors()` is a capacity-one diagnostic channel. Producers never block
and drop the newest diagnostic when full. Text exposes stable operation
categories, while `errors.Is`/`errors.As` retain causes. The channel is not
closed by `Peer.Close`; select with a lifecycle context.

<a id="sessionid-与诊断"></a>

## Diagnostics

`Peer.Statistics()` returns a JSON-compatible bounded snapshot. Counters and
independent high-water marks last for the Peer lifetime and remain readable
after Close. `CapturedAt` supports interval rates; independently sampled fields
are not one atomic transaction. Differences inside one snapshot do not prove
packet loss.

Detailed FEC/Endpoint metrics below describe v1. V2 reuses basic Peer/transport
ingress, delivery, admission and socket counters, but does not yet provide
equivalent internal FEC/path coverage. Missing evidence must not be inferred
from zero counters. V2 `SentDatagrams` counts admitted originals.

V2 additionally exposes the optional `v2_receive` object, omitted for v1.
`received_fec_bundles` counts authenticated, route-accepted FEC handler attempts,
including body or resource rejection. `packet_scratch_rejections`,
`new_group_rejections` and `original_admission_rejections` count resource-limit
failures at those stages; every failed original retry counts as another attempt.
An implementation that prepays packet scratch during Session admission keeps
the packet-scratch rejection counter zero. `decoded_groups` counts successful
FEC decoding, and `completed_groups` counts atomic admission of all decoded
fragments into original reassembly, which need not deliver a complete original.
`expired_groups` counts terminal expiry/error events and excludes Close.
These counters survive Session disposal and Peer closure.

`pending_groups`, `decoded_pending_groups` and `pending_originals` are current
gauges across live Sessions; decoded-pending groups are included in pending
groups. `credit_bytes` and `credit_reservations` sample all live Peer ledger
usage, including pending handshakes, inbound/outbound work and retained application
deliveries; they are not receive-only credit. Credit
bytes describe charged obligations, not heap usage or RSS. These gauges can
decrease and must not be treated as cumulative counters. Sampling acquires the
v2 owner mutex once and reads scalar controller state plus one credit snapshot;
receive events do not copy paths or take additional credit snapshots.
`CapturedAt` is recorded before collection, including any wait for that mutex;
it does not promise that every field was read at exactly that instant.

The performance runner accepts older v2 archives without `v2_receive`. When
present, it requires all named counters/gauges to be nonnegative integers and
rejects the object for v1 or native protocols. The interval report emits
`counter_deltas` and the separate `end_gauge_snapshot`; counter regression or
object presence changes between selected boundaries are rejected. Decreasing
gauges are valid and remain ending snapshots rather than deltas.

V1 tracks delivery/ingress overflow, completed and recovered blocks, missing
data shards, timeout/full/duplicate events, known late shards and `TooOldShards`.
Late or too-old shards do not independently measure network loss.
`CompletedCapacityEvictions` supports the old decoder comparison and stays zero
for the production bitmap window. Current `PendingBlocks`, `PendingShards` and
`PendingBytes` fall on completion, expiry and closure; bytes count retained
shard payload only, excluding map/codec/temporary allocation overhead. Their
high-water marks are independent Peer-wide maxima, not sums of Session peaks.

The reproducible delayed-parity comparison is:

```sh
go test ./internal/fec -run 'TestDelayedParity(Capacity|Window)Diagnostics' -v
go test ./internal/fec -run TestReplayWindowHighBlockRateDelayedParityDoesNotReopen -v
go test ./internal/fec -run '^$' -bench BenchmarkDelayedParityCapacity -benchmem
```

For RS(3+2), 32 completed IDs and 16 pending slots, old cache capacities 8/16/32
reopen 16/16/0 blocks and reject a new block in the first two cases. The fixed
window reopens none and admits the new block. After 65568 completions followed
by all parity, it records 131072 late and 64 too-old shards with no reopened
pending work. These are correctness regressions, not throughput measurements.

`Paths` aggregates configured `carrier-N` sockets across Sessions and the shared
listener without exposing addresses, SessionIDs, PSKs or payload. Bytes measure
complete UDP payload, excluding IP/UDP/L2. Receive oversize counts distinguish
kernel-truncated reads. Sent bytes count socket write results; sent packets
require complete writes. `SendErrors` includes socket errors/short writes, not
later kernel/qdisc loss or earlier validation/deadline failures.

V1 `ListenerPaths` tracks authenticated accepted Endpoints in at most 256
anonymous lifetime slots, then one overflow row. Socket generation and local/
remote endpoints define identity, not SessionID; slots persist across expiry.
Authentication/protocol/admission failures create no slots. Accepted duplicates
count, while CLOSE uses only an existing source row. Its scope differs from
the raw listener socket row, so totals need not match.

`SetDiagnosticsEnabled(true)` enables optional ingress-queue and send latency,
socket-write-lock wait and socket-call time, plus fixed packet-size histograms.
Latency uses 24 independent buckets from `<=1us` through `<=4194304us`, then
overflow; `TotalNS`/`MaxNS` are nanoseconds. Disabling retains existing samples.
These counters do not replace kernel/qdisc, reliable-transport or MTU/epoch
instrumentation. Local diagnostic overhead benchmarks do not establish
end-to-end performance.

`SessionID` is `[16]byte` and generated with `crypto/rand.Reader`; zero values
are retried boundedly and entropy failure cannot return a usable ID. Config
cannot force an ID. Default Peer/Listener/Session formatting contains roles,
counts, state and a short ID hash, never secrets, full IDs or packet contents.

## Stable Errors

Use `errors.Is`:

| Sentinel | Meaning |
|---|---|
| `ErrInvalidConfig` | Invalid YAML/configuration or nil context |
| `ErrProtocolUnavailable` | Valid protocol/feature/platform combination is unimplemented |
| `ErrMessageTooLarge` | Original exceeds effective configured/negotiated bounds |
| `ErrClosed` | Peer, Listener or Session closed |
| `ErrAuthentication` | Authentication failed |
| `ErrHandshakeIncompatible` | Protocol, FEC or transport capabilities conflict |
| `ErrNotReady` | Handshake or required v2 context is not ready |
| `ErrModeUnavailable` | Requested bootstrap role is disabled |
| `ErrResourceLimit` | Admission/count/queue or Session/Peer byte capacity exhausted |
| `ErrNoAvailablePaths` | No available path before sending |
| `ErrPartialSend` | Some original FEC shard attempts failed |
| `ErrAllSendsFailed` | All original FEC shard attempts failed |
| `ErrPathMTUExceeded` | Packet exceeds path MTU; may also match a send-result error |

Malformed or unauthenticated packets are dropped in the dispatcher without
creating a public Session or escaping through ReadPacket/Accept. Public errors
and retained causes do not expose PSKs, authentication tags or payloads.
