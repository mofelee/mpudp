# MPUDP Configuration Reference

[简体中文](CONFIGURATION.zh-CN.md)

Configuration is one YAML document. Strict `yaml.v3` decoding rejects unknown
fields, duplicate keys, wrong types, additional documents and numeric overflow
with `config.ErrInvalidConfig`, the same sentinel as `mpudp.ErrInvalidConfig`.
Only omitted optional fields receive defaults. Explicit zero is validated as
zero. Integers must be YAML integers: floats, exponent-form floats, numeric
strings and booleans reject; valid hexadecimal/octal/binary/underscore integer
notation is decoded before range checks. Durations are Go duration strings.

The file limit is 1 MiB (`config.MaxConfigBytes`). `Parse` checks input length;
`Decode` reads at most the limit plus one byte. Explicit YAML null, including
`~` and empty mapping values, is invalid everywhere.

Go callers start with `config.Default()` for v1 or
`config.DefaultV2(config.ProtocolDatagram)` / `DefaultV2(config.ProtocolKCP)`
for v2, then set roles, FEC and PSK. Helpers return configuration without
creating resources. `Validate` and `NewPeer` do not silently fill numeric zeros
in direct Go literals. Empty Go `Protocol`/`Wire.Version` preserve legacy
Datagram/v1 semantics without modifying the object; explicit empty YAML
strings reject. `EffectiveProtocol()` / `EffectiveWireVersion()` expose this
compatibility behavior.

## Roles

At least one of `carriers` and `listen` is required:

| Configuration | Mode | Meaning |
|---|---|---|
| `carriers` only | initiator | Creates outbound Sessions |
| `listen` only | listener | Accepts inbound Sessions |
| Both | dual | Supports both roles independently |

Each Carrier is a remote UDP `host:port`, not a local bind address. Hosts must
be nonempty and not unspecified addresses; DNS names, IPv4 and bracketed IPv6
are supported. Ports are 1..65535. Normalized duplicate remotes reject; at
most 256 Carriers are allowed. `listen` is local and permits an omitted host,
such as `:9000`. Validation performs no DNS lookup or socket creation.

`peer.id` and `session_id` are unknown fields. Runtime SessionIDs use 16 random
bytes and are not configured or tied to a UDP tuple.

## Protocol Availability

`protocol: datagram|kcp` is independent of roles and defaults to `datagram`.
`wire.version: v1|v2` defaults to `v1`. Invalid types, case variants, empty
strings and null reject. `config.Default()` explicitly chooses Datagram/v1.

| Selection | Parse / Validate | NewPeer / NewPeerContext |
|---|---|---|
| Omitted fields, or Datagram/v1 | Existing positive FEC/resource rules | Existing v1 Datagram |
| KCP with omitted version or v1 | `ErrInvalidConfig` | `ErrInvalidConfig` |
| Datagram/v2 | Positive k/r required; UDP caps at least 512 | Linux fixed/session, repair off; aggregation optional |
| KCP/v2 | Omitted FEC becomes 0/0; explicit nonzero/negative FEC rejects | Valid configuration returns `ErrProtocolUnavailable` |

Linux v2 supports authenticated handshake, fixed Session budgets, equal-size
FEC groups, original-Datagram reassembly and optional aggregation. KCP,
`repair.enabled: true`, `mtu_discovery: plpmtud`,
`budget_strategy: per_carrier`, and v2 on non-Linux platforms remain unavailable.
After validation, valid unsupported selections return `ErrProtocolUnavailable`
before touching runtime context, randomness, sockets or timers. There is no
automatic v1 fallback. Linux sockets must support required PMTU enforcement
and destination-address-bound replies; inability to configure them fails startup.

The parser also recognizes the shared and future protocol settings below.
Successful parsing does not allocate their maximum resources or activate an
unsupported feature. V2 uses a fixed Peer-wide `max_send_workers` pool for
established control and DATA sends, with a 20ms invocation context outside the
protocol mutex. One admitted packet per path remains owned through completion;
the effective packet budget must fit `max_path_queued_bytes`. Waiting descriptors
are bounded at group level, and invocation must begin within 100ms of queueing.
Protocol/encoding and bounded bootstrap emission remain serialized. Inbound
sends share the listener socket write lock. The worker pool does not implement
the remaining #22 scheduling/health policy.
Operator-supplied rates are not measured bandwidth or #16 performance evidence.

## Shared V2 Settings

These fields require explicit v2. V1 rejects them even when explicitly zero or
empty; omitted new fields preserve v1 defaults. The
[joint configuration design](design/v2-configuration-api.md) distinguishes the
implemented subset from future protocol contracts.

| Field | V2 Default | Range |
|---|---:|---|
| `transport.max_receive_udp_payload` | Final `max_udp_payload` | 512..65507 |
| `transport.mtu_discovery` | `fixed` | `fixed` or `plpmtud` |
| `transport.budget_strategy` | `session` | `session` or `per_carrier` |
| `transport.max_retained_epochs` | 2 | 1..8 |
| `transport.max_epoch_age` | `5s` | `100ms`..`60s` |
| `transport.max_migrations` | 2 | 1..2; does not enable migration |
| `transport.plpmtud.base_udp_payload` | 512 | Exactly 512 |
| `transport.plpmtud.probe_interval` | `1s` | `100ms`..`60s` |
| `transport.plpmtud.max_outstanding_per_path` | 1 | Exactly 1 |
| `limits.max_pending_handshakes` | 256 | 1..4096 |
| `limits.max_pending_accepts` | 256 | 1..65536 |
| `limits.max_peer_retained_bytes` | 268435456 | 1 MiB..1 GiB |
| `limits.max_session_retained_bytes` | 16777216 | 1 MiB..Peer ceiling |
| `limits.max_datagram_reassemblies` | 1024 | 1..65536 |
| `limits.max_fragments_per_datagram` | 256 | 1..4096 |
| `limits.max_migration_transaction_bytes` | 8388608 | 1..8 MiB, at most Session ceiling |
| `limits.max_streams_per_session` | 128 | 1..4096 |
| `limits.max_peer_streams` | 4096 | 1..65536 |
| `limits.max_stream_retained_bytes` | KCP: 262144 + configured MaxFrameSize (278528 by default) | Positive, at most Session; mux requires this initial window |
| `limits.max_path_queued_packets` | 256 | 1..4096 |
| `limits.max_path_queued_bytes` | 1048576 | 512..Session ceiling |
| `limits.max_send_workers` | 8 | 1..32 |
| `timers.datagram_reassembly_timeout` | `10s` | `100ms`..`60s` |
| `timers.group_decode_timeout` | `10s` | `100ms`..`60s` |

`transport.plpmtud` is valid only in PLPMTUD mode; even an empty block rejects in
fixed mode. `outbound_path_budgets` / `inbound_path_budgets` are recognized only
for fixed/per_carrier, with entries such as
`{path_id: 1, max_udp_payload: 1200}`. Outbound entries must cover all Carrier
indices; listener inbound entries independently cover contiguous 1..N within
the Endpoint limit. The roles do not fill each other's lists. Unused-role
lists must be omitted. Duplicate/missing/reindexed paths reject. Budgets are
512..local send hard cap. This policy remains unavailable in the runtime.

`scheduler.outbound_path_rates_bps` / `inbound_path_rates_bps` are optional
PathID maps, for example `{1: 100000000, 2: 50000000}`. Rates are
1000..1000000000000 bit/s; omitted valid paths use 100000000. Keys and values
must be YAML integers; normalized duplicate keys such as `1` and `0x1` reject.
Outbound keys fit Carrier count; inbound keys fit a static reverse profile or
`max_endpoints_per_session`. `Clone()` deep-copies both lists and both maps.

Byte limits are ceilings, not simultaneous reservations of every maximum.
Lowering a Session ceiling may require explicit reductions of dormant default
migration/path/stream limits; parsing never silently clips them. Send and
receive UDP hard caps are independent; reverse receive capability does not
prove local path MTU.

## Protocol Settings

Defaults populate only the selected protocol. Datagram aggregation/repair and
KCP mux default off. KCP fast/early retransmit and congestion control default
on. Booleans must be YAML booleans; numbers, strings and implicit `yes`/`no`
reject. Explicit false is never replaced by a default.

| Field | Default | Range / Rule |
|---|---:|---|
| `aggregation.enabled` | false | V2 Datagram |
| `aggregation.max_delay` | `250us` | `1us`..`10ms` |
| `aggregation.max_records` | 32 | 1..256 descriptors |
| `aggregation.max_queued_datagrams` | 256 | 1..65536 originals |
| `aggregation.max_queued_bytes` | 1048576 | 1..Session ceiling |
| `aggregation.max_group_bytes` | 1048576 | 24..16777216; also bounded by k*ShardBytes |
| `repair.enabled` | false | V2 Datagram with positive FEC; runtime unavailable when true |
| `repair.max_age` | `5s` | `100ms`..`60s` |
| `repair.max_attempts` | 3 | 1..16 |
| `repair.max_cached_blocks` | 1024 | 1..65536, at most group span |
| `repair.max_cached_bytes` | 8388608 | 1..Session ceiling |
| `repair.max_outstanding_datagram_span` | 65536 | 1..65536, also bounded by peer receive span |
| `repair.max_outstanding_group_span` | 65536 | 1..65536, also bounded by peer receive span |
| `kcp.fast_retransmit.enabled` | true | False disables fast/early, retaining RTO |
| `kcp.fast_retransmit.threshold` | 2 | 1..255; does not enable a disabled policy |
| `kcp.update_interval` | `10ms` | `10ms`..`100ms` |
| `kcp.send_window_segments` | 1024 | 32..65535 |
| `kcp.receive_window_segments` | 1024 | 32..65535 |
| `kcp.congestion_control` | true | False requires explicit configuration |
| `stream_mux.enabled` | false | V2 KCP |
| `stream_mux.max_frame_size` | 16384 | 128..65535 |
| `stream_mux.max_pending_opens` | 128 | 1..128 |
| `stream_mux.open_timeout` | `5s` | `100ms`..`5s` |
| `stream_mux.max_control_record_bytes` | 256 | Exactly 256 |
| `stream_mux.max_queued_control_bytes` | 32768 | 256..32768 and Session/Peer charged |

Selected-protocol tuning remains range-checked when its optional feature is
disabled. On another protocol or v1, only bare neutral declarations such as
`aggregation: {enabled: false}`, `repair: {enabled: false}` and
`stream_mux: {enabled: false}` are allowed; empty maps or extra fields reject.
There is no top-level `kcp.enabled`: any KCP section requires KCP/v2.

Repair requires both receive timeouts to cover `repair.max_age`. Mux requires
stream bytes of at least `262144 + MaxFrameSize`, with independent control and
business initial windows plus queued control fitting the Session. Omitted
stream bytes use the final configured frame size; explicit values are preserved.
These are validation necessities, not allocated backend memory. KCP/mux
implementations must reserve actual buffers and simultaneous copies before
advertising receive credit; `window_segments * 1500` is not an accounting
formula. Both runtimes remain unavailable.

## Aggregation And Ownership

With aggregation disabled, `WritePacket` waits for the original's local socket
attempts. Enabled aggregation copies the whole original and reserves its queue
ID/bytes before returning success. Capacity failure returns `ErrResourceLimit`
without a partial prefix. `max_records` counts fragment descriptors;
`max_queued_datagrams` counts whole originals, including empty Datagrams.
The oldest admission fixes `max_delay`; capacity, descriptor count, expiry or
`DatagramSession.Flush(ctx)` seals a group. This is not a hard timing guarantee
under operating-system scheduling or shared dispatcher load.

Flush waits only for local shard attempts through its captured admission
frontier, not remote reads/ACKs. Cancellation does not retract accepted work.
`CloseGracefully(ctx)` stops writes, drains within the context and closes;
ordinary Close may discard unsent work. See the [public API](API.md).

Fixed Peer ingress/listener/accept storage is deducted from the global byte
budget first. Handshake reserves the future Session, pending accept and actual
initial component storage. Installation consumes those dedicated leases without
charging twice. Subsequent payload, FEC/group/reassembly and queued deliveries
remain Session/Peer charged; dropped deliveries release ownership. Session
count is a ceiling, not a guarantee under byte pressure. Runtime construction
or admission can return `ErrResourceLimit` when initial storage does not fit.
These ceilings bound ownership and reservations, not process RSS: Go
allocator/GC retention and shared codec lookup tables are outside the counters.

## Examples

This minimal example preserves v1:

```yaml
carriers:
  - "192.0.2.11:4000"
  - "[2001:db8::11]:4000"
fec: {data_shards: 3, parity_shards: 2}
psk: "development-only-example-key"
transport:
  max_udp_payload: 1200
```

For the supported Linux v2 path, add:

```yaml
protocol: datagram
wire: {version: v2}
aggregation: {enabled: true}
```

The omitted discovery/budget policies default to fixed/session; repair remains
off. Datagram k and r must both be positive, with sum at most 256, using the
pinned Reed-Solomon GF(2^8) profile.

<a id="psk-管理"></a>

## PSK Management

`psk` must be a nonempty string, at most 4096 UTF-8 bytes. There is no `psk_file`,
environment expansion or shell interpolation: `${NAME}` is literal key text.
PSK provides HMAC-SHA-256 integrity, not payload encryption.

Example keys are development-only. Production keys should be independently
generated high-entropy secrets, provided through protected mode-0600 configuration
or `config.NewSecret`. Never include keys in logs, arguments or artifacts.
Environment variables are not inherently protected from process/crash tools.
Secret/config formatting and YAML output use `[REDACTED]`; `Secret.Bytes()`
explicitly returns a copy.

## UDP Payload Budget

For v1, distinguish:

| Term | Meaning |
|---|---|
| Path MTU | Maximum complete IP packet size |
| UDP payload | Bytes after UDP header, including complete MPUDP packet |
| Shard capacity | Negotiated payload minus 71-byte DATA_SHARD overhead |
| Original limit | min(k * shard capacity, max_datagram_size) |

| Field | Default | Range | Meaning |
|---|---:|---:|---|
| V1 `transport.max_udp_payload` | 1200 | 72..65507 | Complete local UDP payload |
| V2 `transport.max_udp_payload` | 1200 | 512..65507 | Complete local UDP payload send hard cap |

The payload cap includes prefix, type body, full 32-byte authentication tag
and payload. It is neither IP MTU nor just RS data. V1's 72-byte minimum covers
mandatory PING/PONG. The 1200 default leaves IPv6-minimum-MTU header room but
does not prove smaller tunnels are safe. Operators must choose a safe budget
for every Carrier. Linux DF/PMTU prevents local fragmentation; filtered ICMP
can still cause a silent black hole. PLPMTUD (#13) remains unavailable.

V1 freezes one negotiated minimum after HELLO/HELLO_ACK; PING, PONG, DATA_SHARD
and CLOSE use it. CLOSE is 56 bytes. V2 advertises independent send/receive hard
caps and bootstraps at 512 bytes. It does not send configured larger DATA until
authenticated path-budget/encoding-context exchange completes. Each direction
stays within local send and peer receive hard caps.

For a v2 single-entry FEC bundle, equal shard capacity is `budget - 94`; logical
group bytes are also bounded by min(`aggregation.max_group_bytes`, k*ShardBytes).
An original can span groups within peer size/fragment ceilings. This differs
from v1's 71-byte overhead and is not measured throughput evidence. Dynamic
MTU shrink/migration and per-Carrier layout (#14) remain unavailable.

## Resource And Timer Limits

| Field | Default | Range |
|---|---:|---:|
| `limits.max_datagram_size` | 65536 bytes | 1..16777216 |
| `limits.max_pending_fec_blocks` | 1024 | 1..65536 |
| `limits.receive_queue_capacity` | 256 | 1..65536 |
| `limits.delivery_queue_capacity` | 256 | 1..65536 |
| `limits.max_sessions` | 1024 | 1..65536 |
| `limits.max_endpoints_per_session` | 256 | 1..256 |
| `limits.max_handshake_attempts` | 8 | 1..64 |

`max_datagram_size` is a resource ceiling, also intersected with effective FEC/
peer limits before any payload/ID admission. `max_pending_fec_blocks` bounds
pending v1 blocks/v2 groups; terminal history is independent of pending
capacity. Ingress and delivery queues drop newest items when full. V1 accept
uses receive queue capacity; v2 uses pre-reserved `max_pending_accepts` slots.
Unauthenticated sources cannot create established Session/Endpoint state.
V1 retry/jitter settings do not enable v2 DATA repair.

| V1 Timer | Default | Range |
|---|---:|---:|
| `timers.decode_timeout` | `3s` | `100ms`..`1m` |
| `timers.endpoint_ttl` | `2m` | `5s`..`24h` |
| `timers.keepalive_interval` | `15s` | `1s`..`5m` |
| `timers.handshake_retry_interval` | `1s` | `100ms`..`1m` |

V1 uses one timer for handshake retry, keepalive, Endpoint expiry and FEC sweep;
decode timeout starts with the first shard, while Endpoint TTL follows valid
authenticated activity. V2 uses one timer for its own handshake/control retry,
aggregation and `group_decode_timeout`/`datagram_reassembly_timeout`. Receive
deadlines begin on first admitted shard/original fragment and duplicates do not
refresh them. This is not #22 fast path-health probing. Close cancels deadlines
and waits for owned runtime work.

Dependency versions, licenses and distribution requirements are in the
[dependency audit](DEPENDENCIES.md).
