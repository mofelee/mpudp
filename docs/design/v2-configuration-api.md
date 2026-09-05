# V2 Configuration And Proposed API Contract

Status: concrete proposal for #20 review. Strict `protocol` and `wire.version`
recognition, protocol-specific FEC validation, shared v2 transport/resource
settings, directional path budgets/rates, receive deadlines, aggregation,
repair, KCP tuning and mux configuration validation are implemented.
`config.DefaultV2(protocol)` supplies the recognized v2 defaults for Go callers;
`config.Default()` preserves v1 defaults. A valid v2 configuration still returns
`ErrProtocolUnavailable` from Peer construction before runtime side effects.
New runtime APIs and data-plane behavior below remain proposals; the runnable
data plane remains v1 Datagram. The
maintained [configuration](../CONFIGURATION.md) and [API](../API.md) describe
the implemented parser/runtime boundary. Wire values are assigned in the
[registry](v2-registry.md); behavior and capacity are in the
[joint design](v2-joint-contract.md).

## Defaults And Validation

Normalize the selected protocol/wire version before protocol-specific defaults.
Do not change Mode: it still means initiator/listener/dual. YAML remains strict
about unknown fields, duplicates, null, scalar types, trailing documents and
the existing 1 MiB file limit. Explicit zero means zero, not omitted/default.
Check duration conversion and all products before sockets or allocation.
Explicit v2-only transport/resource/scheduler fields reject on v1, including
zero values or empty maps. Static path budgets must be absent outside
fixed/per_carrier mode. Path-rate keys and values must be YAML integers;
normalized duplicate keys such as `1` and `0x1` reject. Rate maps may be sparse:
unlisted valid PathIDs use 100000000 bits/s. Outbound IDs fit the configured
Carrier count; inbound IDs fit the static reverse profile when present,
otherwise `limits.max_endpoints_per_session`. Each role's maps are independent.

Only YAML omission applies defaults. Direct Go v2 literals must contain all
required invariants; `Validate` does not rewrite zeros. YAML receive hard cap
defaults to the final local send cap after overrides. PLPMTUD defaults apply
only when that discovery strategy is selected. Shared configured maxima are
validated against Session/Peer ceilings; reducing a Session ceiling may also
require explicitly reducing dependent maxima rather than silently clamping
their defaults. `Clone` copies both directional profile slices and rate maps.
Protocol-specific defaults populate only the selected protocol. An explicit
false boolean is preserved, including KCP congestion control and fast/early
retransmit policy; changing the retransmit threshold never enables it. Within
the selected protocol, all configured tuning values are range checked even
when the optional feature is disabled. Configuration defaults for disabled
features do not imply offered wire capabilities or resource allocation.

The bare `enabled: false` exception applies only to `aggregation`, `repair`
and `stream_mux`, which define that field. It permits neutral sections on v1
or the unrelated v2 protocol; empty mappings and additional tuning fields
reject there. `kcp` has no top-level `enabled` field: any explicit KCP section
requires v2 KCP. Omitted `limits.max_stream_retained_bytes` in KCP is computed
after the configured mux frame size as `262144 + MaxFrameSize`; an explicit
value is never replaced.

| Field | Configuration Default | Legal Range / Rule |
|---|---|---|
| protocol | datagram | datagram or kcp |
| wire.version | v1 | v1 or v2; v1 permits existing Datagram only |
| fec.data_shards, parity_shards | Required in Datagram; both 0 in KCP | Datagram each1..255, sum<=256 and codec-supported; KCP explicit positive values conflict |
| aggregation.enabled | false | v2 Datagram only when true |
| aggregation.max_delay | 250us | 1us..10ms; checked even when disabled in Datagram |
| aggregation.max_records | 32 | 1..256 fragment descriptors/group |
| aggregation.max_queued_datagrams | 256 | 1..65536 whole admitted Datagrams |
| aggregation.max_queued_bytes | 1048576 | 1..Peer/Session retained-byte limits; insufficient whole-packet reservation rejects admission |
| aggregation.max_group_bytes | 1048576 | 24..16777216; effective ceiling also k*epoch.ShardBytes |
| repair.enabled | false | v2 Datagram+FEC only |
| repair.max_age | 5s | 100ms..60s; fixed from sender admission |
| repair.max_attempts | 3 | 1..16 additional repair rounds, excluding first send |
| repair.max_cached_blocks | 1024 | 1..65536, also bounded by GroupID span |
| repair.max_cached_bytes | 8388608 | 1..Session/Peer retained ceilings |
| repair.max_outstanding_datagram_span | 65536 | 1..65536, not greater than peer window |
| repair.max_outstanding_group_span | 65536 | 1..65536, not greater than peer window |
| transport.max_udp_payload | 1200 | v1:72..65507; v2:512..65507; complete local send hard cap |
| transport.max_receive_udp_payload | local max_udp_payload | v2:512..65507; advertised local receive hard cap |
| transport.mtu_discovery | fixed | fixed or plpmtud |
| transport.budget_strategy | session | session or per_carrier |
| transport.outbound_path_budgets | absent | fixed/per_carrier initiator c2s budgets, indices1..len(carriers) |
| transport.inbound_path_budgets | absent | fixed/per_carrier listener s2c budgets, contiguous indices1..N; N is supported MaxPaths |
| transport.plpmtud.base_udp_payload | 512 | exactly512 in this profile |
| transport.plpmtud.probe_interval | 1s | 100ms..60s |
| transport.plpmtud.max_outstanding_per_path | 1 | exactly1 |
| transport.max_retained_epochs | 2 | 1..8 OLD contexts excluding current, negotiated minimum; pinned pending contexts count |
| transport.max_epoch_age | 5s | 100ms..60s, negotiated minimum |
| transport.max_migrations | 2 | 1..2 attempts/original group; replacement groups per attempt<=256 |
| kcp.fast_retransmit.enabled | true | local sender policy; false disables both fast and early, retains RTO |
| kcp.fast_retransmit.threshold | 2 | 1..255; does not implicitly enable a disabled policy |
| kcp.update_interval | 10ms | 10ms..100ms |
| kcp.send_window_segments | 1024 | 32..65535 and owned-byte reservation |
| kcp.receive_window_segments | 1024 | 32..65535 and owned-byte reservation |
| kcp.congestion_control | true | false requires explicit operator choice, never injected from benchmark settings |
| stream_mux.enabled | false | KCP only; true selects smux-wire2/admission-v1 |
| stream_mux.max_frame_size | 16384 | 128..65535; fixed by negotiated profile and byte accounting |
| stream_mux.max_pending_opens | 128 | 1..128 |
| stream_mux.open_timeout | 5s | 100ms..5s, shortened by caller context |
| stream_mux.max_control_record_bytes | 256 | exactly256 |
| stream_mux.max_queued_control_bytes | 32768 | 256..32768; also Session/Peer charged |

Each static entry is `{path_id: 1, max_udp_payload: 1200}`. The initiator's
outbound profile must cover its configured Carrier indices exactly. A listener
need not configure Carriers: its explicit contiguous inbound profile1..N
defines supported MaxPaths=N and rejects HELLO BootstrapPathID/count outside
that profile. Dual mode has independent outbound/inbound arrays and may have
different topology counts; neither silently supplies the other. Omit the
profile for a role never used; reject duplicate, missing or noncontiguous
indices. Values are 512..local send hard cap, capped by peer receive capability.
Never infer reverse safety from forward configuration or interface MTU.

PLPMTUD-only fields are rejected in fixed mode. An explicit protocol-specific
section in an unrelated protocol is rejected, apart from the bare neutral
`enabled: false` form. Shared limits below may be supplied in either mode:
they are validated but do not create inactive-mode state. v1 rejects enabled
v2 features and keeps existing defaults and synchronous WritePacket behavior.
There is no implicit wire downgrade or automatic protocol conversion.

## Scheduler And Repair Profile

These fixed v2 profile constants avoid adding unbounded downstream knobs.
They are local scheduling behavior, not additional wire fields. Revisit them
with the required #22/#23 RTT/loss matrix; changing a local timer never
changes the frozen meaning of feedback or releases a still-owned buffer.

| Setting | Proposed Value / Bound |
|---|---|
| scheduler.outbound_path_rates_bps | Optional PathID map for initiator c2s; omitted entries100000000 bits/s |
| scheduler.inbound_path_rates_bps | Optional PathID map for listener s2c; omitted entries100000000 bits/s |
| Explicit path rates | 1000..1000000000000 bits/s; no unverified capacity estimator in the first profile |
| Pacing charge | Full outer UDP+IP bytes,28 extra IPv4 or48 IPv6 in the no-options profile; all traffic classes |
| Token bucket burst | Two complete configured-hard-cap packets, including IP/UDP |
| Path queue residence limit | 100ms; expiry is a send failure/loss for that mode, not unlimited queuing |
| Health PING interval | 200ms when healthy, one outstanding/path |
| Health response deadline | max(200ms,3*SRTT), capped2s; bootstrap validation seeds SRTT |
| Suspect / inactive | First miss marks suspect; three consecutive misses stop DATA assignment |
| Recovery probes | Backoff200ms to2s; three consecutive successes over at least400ms before full weight |
| Reference40ms RTT failure bound | At most800ms probe detection plus100ms queue residence, targeting<=1s stop assignment |
| Control scheduling reserve | Up to5% of each path's paced capacity, borrowable when unused; never uncharged |
| Repair rate cap | At most25% of each configured path rate, one-packet repair burst, also within total pacing |
| Completion feedback coalescing | At most5ms from first pending completion, or packet/range capacity |
| Incomplete-group feedback grace | max(20ms,1.25*SRTT), capped1s, after first distinct shard |
| Fast repair evidence | Three distinct newer completion ACKs plus actual-send/reordering grace; a numeric ID gap alone is insufficient |
| Whole/tail sender timeout | max(200ms,SRTT+4*RTTVar), capped2s, bounded by original repair age |
| Feedback repeat | No more than once per RTO per object without new shard evidence; no timer extension |

Explicit rates are operator knowledge, not bandwidth claims. Health timers
are independent of the old endpoint_ttl and keepalive_interval, which remain
state cleanup/v1 compatibility settings. At240ms RTT, the longer deadline
reduces false failures; the <=1s target is specifically the reference40ms
case, and other profiles must report measured convergence separately.
At most one deferred feedback slot per bounded group/original plus one
bounded aggregate range buffer exists; an ACK storm cannot allocate per-ACK
workers or timers. A mode's normal initial/parity packets share the main
pacer; repair/control quotas cannot increase its total configured line rate.

## Ownership Limits

These are ceilings, not reservations for every configured maximum at once.
Global admission can reject before the configured Session/stream count is
reached when byte credits are exhausted. Existing v1 limits remain unchanged.

| Field | V2 Default | Bounds / Meaning |
|---|---:|---|
| limits.max_sessions | 1024 | 1..65536 live Sessions Peer-wide |
| limits.max_pending_handshakes | 256 | 1..4096, separate from established Sessions |
| limits.max_pending_accepts | 256 | 1..65536 global business connections/streams waiting for application Accept |
| limits.max_peer_retained_bytes | 268435456 | 1 MiB..1 GiB, all Sessions and modes |
| limits.max_session_retained_bytes | 16777216 | 1 MiB..Peer ceiling |
| limits.max_datagram_size | 65536 | 1..16777216, also effective fragment/budget limit |
| limits.max_datagram_reassemblies | 1024 | 1..65536 and Datagram ID span |
| limits.max_fragments_per_datagram | 256 | 1..4096 |
| limits.max_migration_transaction_bytes | 8388608 | 1..8388608, also Session/Peer ceilings; all aliases and replacement storage charged |
| limits.max_streams_per_session | 128 | 1..4096 business streams, excluding one charged reserved control stream |
| limits.max_peer_streams | 4096 | 1..65536 business streams across all Sessions |
| limits.max_stream_retained_bytes | 262144+configured MaxFrameSize in KCP (278528 at default) | Positive and at most Session ceiling; enabled mux requires at least262144+configured MaxFrameSize before negotiation |
| limits.max_path_queued_packets | 256 | 1..4096 per active path |
| limits.max_path_queued_bytes | 1048576 | 512..Session ceiling per path, with global charge |
| limits.max_send_workers | 8 | 1..32 Peer-wide; not a goroutine per queued send |
| timers.datagram_reassembly_timeout | 10s | 100ms..60s, starts on first admitted original fragment |
| timers.group_decode_timeout | 10s | 100ms..60s, starts on first admitted shard |

For repair, both receive deadlines must be at least the selected repair age;
this is an opportunity window, not a guarantee against arbitrary network
delay. Expiry records a terminal expired bit distinct from completed. Late
fragments cannot recreate the expired original ID while it is in the window,
and IDs below the window floor drop without allocation. DATAGRAM_EXPIRED
reports only known expired IDs; unknown status requests create no state.
Completion ACK is never emitted for an expired original.

Before admitting a Datagram, cap its effective size by configured size,
peer advertised size and `MaxFragments*(k*ShardBytes-24)` with checked
arithmetic. The packer starts a fresh group when necessary to keep the whole
Datagram within its fragment-count reservation. MTU migration can still hit
its configured attempts/count/deadline; Datagram then fails boundedly with a
classified event. Native KCP acknowledged bytes cannot take this drop path.

Credit accounting includes copied application bytes, FEC work/output, queued
wire packets, repair originals, migration transactions, KCP windows, outer
packet assemblies, smux receive buffers and pending accepts. Moving ownership
can avoid double charging the same backing allocation; simultaneous copies
are separately charged. Advertised receive windows are limited to credits
already reserved, and cannot all independently equal the Session byte cap.
The global reservation lock/ledger must atomically cover Session plus Peer.

For smux, reserve the control stream's 262144+MaxFrameSize separately and keep
business windows from consuming that shared receive-bucket floor. Business
stream reservation includes the same initial-window/accepted-frame bound.
The library's MaxStreamBuffer value does not reduce its initial 262144-byte
peer window. Config validation checks the necessary control plus one initial
business window and queued-control capacity against the Session ceiling.
This lower bound is not a backend allocation formula or an admission proof.
KCP send/receive window segment counts are validated as configured ceilings;
there is no invented `segments * 1500` memory estimate. Before advertising
receive capabilities or creating the backend, runtime admission must reserve
the actual KCP buffers, independently owned control storage, negotiated stream
credits and any simultaneously retained queue copies under Session and Peer
ceilings. Datagram sizes also require the selected encoding budget and actual
retained-byte reservation. Teardown clears actual owned buffers before returning leases;
a count decrement alone is not proof of byte reclamation.

## Compatibility Matrix

| Local / Remote Selection | Required Result |
|---|---|
| Omitted new fields / omitted new fields | Existing v1 Datagram, explicit positive FEC, fixed session budget |
| v1 / v2 | No data state; authenticated mismatch where understood, otherwise bounded handshake timeout; no automatic downgrade |
| v2 Datagram / v2 KCP | ErrHandshakeIncompatible before FEC/KCP creation |
| KCP plus explicit FEC positive values or repair.enabled=true | Startup ErrInvalidConfig |
| Datagram plus stream_mux.enabled=true or kcp options | Startup ErrInvalidConfig |
| repair enabled on one side, disabled on the other | Authenticated ErrHandshakeIncompatible |
| mux profile differs | Authenticated ErrHandshakeIncompatible |
| fixed vs plpmtud or session vs per_carrier | Authenticated ErrHandshakeIncompatible in this single-profile offer |
| fixed/per_carrier with complete static directional profiles | Allowed when both implement/select capability; publication is not PLPMTUD evidence |
| fixed/per_carrier with missing reverse profile | Startup ErrInvalidConfig on the sending role |
| PLPMTUD/KCP without packet-piece capability | Startup rejection locally, or authenticated capability mismatch remotely |
| Different local fast/early policy or pacing rates | Allowed; these do not alter reliable stream framing |
| Different receive/resource hard caps | Allowed with directional minima and successful global reservation |
| Any explicit invalid zero, overflow or unknown field | ErrInvalidConfig before sockets/threads/state |

## Public API Choices

Session stays `WritePacket([]byte) error`, `ReadPacket() ([]byte,error)`,
`Close() error`. v2 aggregation is explicit admission semantics; the optional
DatagramFlusher interface and exact fences are in joint section 6. Ordinary
Close is local bounded discard; CloseGracefully has a caller deadline.

For reliable mode add these separate entry points, retaining existing role
errors and rejecting Datagram API entry points in KCP mode:

```go
type Stream interface {
    net.Conn
    CloseWrite() error
    CloseGracefully(context.Context) error
}
type ReliableSession interface {
    OpenStream(context.Context) (Stream, error)
    AcceptStream(context.Context) (Stream, error)
    Close() error
}
// Peer.DialStream(ctx) and Peer.AcceptStream(ctx): one business connection.
// Peer.DialReliableSession(ctx) / Peer.AcceptReliableSession(ctx): explicit
// long-lived mux Session ownership, available only with stream_mux.enabled.
```

Without mux, each DialStream/AcceptStream owns one independent reliable
Session. With mux, those convenience methods return ErrProtocolUnavailable;
callers explicitly own a ReliableSession and call its OpenStream/AcceptStream.
Do not add an implicit global Session pool, Carrier pooling or silently
create extra KCP Sessions to mask head-of-line blocking.

Successful Open/Dial means remote READY and owned receive credits, not merely
local SYN submission. Accept returns only admitted business streams; the
reserved control stream never escapes. Cancellation removes the waiter and
cancels only that pending operation; late grants/READY follow the registry's
idempotent cancellation rules. Context waits create no goroutine per request.

Write follows net.Conn partial `(n,error)` semantics and accepts bytes into
bounded reliable storage; it does not promise peer consumption. CloseWrite
orders FIN after previously accepted bytes and preserves reading. EOF follows
all preceding bytes. CloseGracefully waits for the profile's drain/FIN
condition within context. Close/abort discards owned bytes, wakes all waiters
and reports an explicit abort cause to the peer's affected stream; a stream
close does not close sibling streams. Session Close ends all owned streams.
Deadline errors keep net.Error and standard context/deadline matching.

Existing ErrResourceLimit, ErrNoAvailablePaths, ErrMessageTooLarge,
ErrHandshakeIncompatible and send/MTU errors keep their identities. Add only
ErrProtocolUnavailable, ErrProtocolViolation, ErrIDExhausted and
ErrStreamAborted for new distinctions. Invalid authentication remains a local
counter/drop; diagnostics contain neither key material nor application bytes.

## Remaining Evidence

The definitions above choose behavior and defaults so implementation can
proceed after review. Missing work is concrete: byte-exact codec/KDF vectors,
strict-parser tests, handshake/reflection/replay tests, grants/cancellation
under global pressure, lifecycle/deadline and leak tests, then exact-SHA
network/MTU/capacity acceptance. Dependency experiments are narrow evidence,
not completion of #20, #21, #24, #25, #13 or #14.
