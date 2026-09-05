# MPUDP V2 Joint Contract Proposal

Status: proposed implementation contract, pending review and deterministic
vectors. Not frozen, implemented, or evidence that any issue is complete.
Prepared 2026-09-05 from issues #20-#25, #13, #14 and the current checkout.
Reviewed main: `f9d65e7268110daa2106f1be92feaa706cd5ca10`.
This document and archived dependency experiments change no runtime behavior.
The numeric assignments and defaults below are concrete proposed choices for
implementation review; publishing them does not advertise v2 support.

## 1. Recommended Decisions

1. Keep omitted `protocol` as Datagram and omitted wire version as v1. New
   features require explicit v2. Do not retry a weaker mode after a timeout.
2. Give each established incarnation fresh directional authentication keys;
   negotiate all mode, framing, repair, mux and MTU capabilities together.
3. Separate stable business Datagram IDs from immutable encoding Group IDs,
   KCP sequence space, transport packet assembly IDs and budget epochs.
4. Use bounded fragment records inside equal-length RS groups. A Datagram may
   span groups, and a group may contain pieces of multiple Datagrams.
5. Keep ordinary repair on the original group, shard and Datagram IDs. Treat
   MTU re-encoding as an explicit, separately negotiated migration that keeps
   the Datagram IDs and original deadline but creates a new immutable group.
6. Make native KCP use no MPUDP FEC, no KCP FEC and no lower-layer DATA ARQ.
   Negotiate bounded outer KCP packet fragmentation before enabling MTU shrink.
7. First realize per-path MTU benefits by bundling equal-length shards from
   different groups on larger paths. This preserves the same RS/path failure
   assumptions and does not invent unequal-length Reed-Solomon recovery.
8. Reserve receive credit before advertising it or acknowledging reliable
   bytes. Every retained buffer is charged to both Session and Peer limits.

## 2. Verified Existing Constraints

- `internal/wire/constants.go`: v1 is version byte 1, prefix 24 bytes,
  HMAC-SHA-256 tag 32 bytes, DATA_SHARD metadata 15 bytes, total DATA overhead
  71 bytes. HELLO is exactly 60 bytes; PING/PONG exactly 72 bytes.
- `internal/wire/codec.go`: exact body length and fixed union validation;
  there is no v1 extension area. New fields must not reinterpret v1 bytes.
- `config/defaults.go:72` and `config/parse.go:110`: `Default()` does not
  supply FEC 3+2. Datagram FEC is currently required explicitly. Preserving
  old configuration means preserving that validation, not inventing a default.
- Strict parsing already rejects unknown fields, duplicate keys, explicit
  null, bad scalar types, trailing YAML documents and files over 1 MiB.
- `peer.go:28`: public `Session` has only `WritePacket`, `ReadPacket`, `Close`.
  `Mode` means initiator/listener/dual and must not become a data protocol enum.
- Current `WritePacket` runs FEC encoding and socket sends before returning;
  it can return partial-send/PMTU errors. Datagram delivery uses drop-newest.
- The #18 receive policy is 65536 IDs per live Session receive direction;
  pending keys can finish below the floor, and Close releases the window.
  A new incarnation must not inherit this state accidentally.
- Production root `go.mod` has Reed-Solomon v1.14.2, not KCP or smux.
  The separate perf module pins kcp-go v5.6.72.
- kcp-go v5.6.72 `sess.go:537` clamps UDPSession MTU to 1500 before calling
  the core. `kcp.go:1080` changes MSS/buffer but does not split queued segments.
- kcp-go v5.6.72 `kcp.go:908` still permits early retransmit when `resend=0`.
  Isolated real-engine tests reproduced early=1, fast=0, RTO=0 on one gap ACK.
  An experimental explicit `SetFastRetransmit(false)` patch suppresses fast
  and early while preserving tested RTO gap/tail/ACK-loss recovery. It passed
  100 focused race runs; no published upstream API was found. See
  [archived KCP evidence](evidence/20260905-kcp/README.md) for source,
  patch and commands.

## 3. Configuration And Compatibility

Candidate hierarchy, shown with an explicit v2 Datagram deployment:

```yaml
protocol: datagram
wire:
  version: v2
carriers: ["192.0.2.10:9000"]
psk: "deployment-secret"
fec:
  data_shards: 3
  parity_shards: 2
aggregation:
  enabled: true
  max_delay: 250us
  max_records: 32
  max_queued_datagrams: 256
  max_queued_bytes: 1048576
  max_group_bytes: 1048576
repair:
  enabled: false
  max_age: 5s
  max_attempts: 3
  max_cached_blocks: 1024
  max_cached_bytes: 8388608
  max_outstanding_datagram_span: 65536
  max_outstanding_group_span: 65536
transport:
  max_udp_payload: 1472
  mtu_discovery: fixed
  budget_strategy: session
limits:
  max_sessions: 1024
  max_pending_handshakes: 256
  max_pending_accepts: 256
  max_peer_retained_bytes: 268435456
  max_session_retained_bytes: 16777216
  max_datagram_reassemblies: 1024
  max_fragments_per_datagram: 256
  max_streams_per_session: 128
  max_peer_streams: 4096
  max_stream_retained_bytes: 278528
stream_mux:
  enabled: false
```

For KCP, `protocol: kcp`, `wire.version: v2`, omitted FEC means disabled with
zero parameters, and repair must be disabled. Candidate KCP-only settings:

```yaml
kcp:
  fast_retransmit:
    enabled: true
    threshold: 2
  update_interval: 10ms
  send_window_segments: 1024
  receive_window_segments: 1024
  congestion_control: true
stream_mux:
  enabled: false
```

These are the proposed defaults, except this example explicitly selects
aggregation and 1472 bytes: omitted aggregation remains disabled and omitted
max_udp_payload remains the existing 1200 bytes. They are not performance
tuning claims. PLPMTUD-only defaults are base_udp_payload=512,
probe_interval=1s and max_outstanding_per_path=1; reject an explicit plpmtud
section in fixed mode. Default old-epoch receive grace is two old epochs and
5s; hard bounds, pending-group context pins and full validation rules are in
the [configuration/API contract](v2-configuration-api.md) and registry.
The strict normalizer selects protocol before applying defaults. Datagram
retains explicit positive k/r; KCP normalizes omitted FEC to zero. Explicit
nonzero FEC, an enabled FEC flag, or repair in KCP is a conflict. Initially do
not add an unrelated no-FEC Datagram protocol. Datagram rejects mux and
KCP-specific settings. Local fast retransmit policy is frozen per sender but
need not equal the peer's setting: it does not change reliable wire framing.
An unavailable fast/early-off implementation is a startup error.

`max_udp_payload` remains a complete UDP payload hard cap. Fixed mode retains
the old safe-session-budget interpretation. Set the v2 minimum to 512 bytes;
v1 keeps its existing 72-byte minimum. Every bootstrap handshake packet is
exactly 512 bytes. Both modes start at the confirmed 512-byte bootstrap
budget; fixed mode publishes its configured known-safe budget after handshake,
while PLPMTUD requires size-probe evidence before increasing it.

MTU mode and budget scope are orthogonal. `fixed` + `session` keeps the existing
shared safe budget. `fixed` + `per_carrier` is allowed with explicit known-safe
per-direction static path budgets, negotiated capability and local/peer hard
caps. `transport.outbound_path_budgets` maps configured initiator Carriers;
`transport.inbound_path_budgets` independently defines the listener's contiguous
PathID profile and supported path count. Dual roles may have different inbound
and outbound topology. The listener cannot assume the
initiator's forward cap is safe in reverse. Reject missing or ambiguous static
profiles; never infer their values from interface MTUs or successful sends.
`plpmtud` + `session` uses the minimum confirmed active-path budget;
`plpmtud` + `per_carrier` uses each directed path's confirmed budget.
Dynamic KCP requires the KCP packet fragmentation capability.
Configuration cannot enable an
unimplemented or unnegotiated capability. Fields irrelevant to a selected
mode should be rejected when explicitly supplied, except documented neutral
`enabled: false` values. Explicit invalid zeros never mean "use defaults".

| Local / Remote | Expected Result |
|---|---|
| Existing v1 configs / existing v1 configs | Existing Datagram behavior and fixed Session budget |
| v2 Datagram / v2 Datagram, matching required features | Negotiated v2 batch/repair/MTU profile |
| v2 KCP / v2 KCP, matching mux/framing requirements | One reliable KCP sequence space per Session |
| v1 / v2 | Version incompatibility; bounded timeout against an old silent peer |
| v2 Datagram / v2 KCP | Authenticated mode rejection before data state |
| repair or mux required on one side only | Authenticated capability mismatch, no weakening |
| fixed/session / adaptive/per-carrier required | Reject unless the initiator explicitly offered an acceptable profile |

Initially prefer one wire version and one data protocol per Peer. A dual
version listener is a separate explicit option, not necessary for v2 delivery.
An explicit operator fallback is permitted only to an equivalent v1 Datagram
configuration: never KCP, repair, mux or an altered protection guarantee.

## 4. Handshake, Authentication And Freshness

Propose HELLO -> CHALLENGE -> FINISH -> READY. HELLO and CHALLENGE use the
configured PSK with a v2 handshake domain. HELLO contains SessionID, a random
128-bit client nonce, required/offered capabilities, receive hard caps and
bounded resource advertisements. CHALLENGE adds a fresh random 128-bit server
nonce and the exact selected contract. FINISH proves the full transcript and
return path; READY confirms installation. Capabilities cannot change later.

After authentication of HELLO, the listener may reserve a bounded pending
handshake record and provisional accounting leases. It must not construct
FEC/KCP/smux or public accepted state
until FINISH validates. Repeated HELLO for the same pending transcript reuses
that pending challenge; expired or closed incarnations get a fresh challenge.
An already established SessionID rejects incompatible new incarnations.

Derive directional HMAC keys using HKDF-SHA-256 from the PSK, both nonces,
SessionID, selected version and canonical transcript hash. Use different
labels for initiator-to-listener and listener-to-initiator. The complete
header, type, lengths and body remain authenticated with a full 32-byte tag.
This is integrity and peer possession of a PSK, not encryption or identity
among multiple peers sharing that PSK.

Old FINISH/DATA must not establish or affect a fresh incarnation with the same
SessionID. Avoid a deterministic, time-bucket-only challenge that could permit
captured FINISH to recreate a closed incarnation within the bucket. A stateless
cookie variant needs its own replay proof before replacing bounded challenges.

Structural parsing may reject a malformed or unsupported envelope before MAC
verification; it may not allocate protocol state, emit amplifying responses,
learn endpoints, refresh health or process feedback. Read-only Session lookup
to find a derived MAC key is allowed. Authenticate before semantic mutation.
Unknown-version responses cannot be trusted as permission to downgrade.

Handshake bodies use bounded canonical TLVs: u16 type, u16 length,
strictly increasing types, no duplicates, no nesting initially. A high bit
marks required extensions; unknown required extensions are incompatible.
Optional unknown extensions are skipped but remain in the signed transcript.
The registry fixes handshake length512, TLV count16, TLV bytes372 and pending
lifetime10s. Every handshake type fits that same initial budget without
fragmentation or response amplification. Established controls use their
fixed registry layouts, not an unspecified TLV extension area.

## 5. Identifier And Envelope Proposal

All MPUDP multi-byte protocol integers are big-endian. Keep the v1 prefix
positions for demultiplexing, with a separate v2 parser. The proposed exact
type/TLV numbers, handshake, key derivation and control layouts are in the
[v2 registry](v2-registry.md). Embedded KCP/smux preserve their own byte order.

| Field | Proposed Width / Meaning |
|---|---|
| magic, version, type, body length, SessionID | Existing 24-byte prefix shape, version=2 |
| sender path ID | u32, authenticated logical path handle, never an address |
| path generation | u64, no wrap; rebinding invalidates previous path knowledge |
| current transmit budget epoch | u32, no wrap; not the encoding group's epoch |
| typed body | Exact known layout selected by type/capability |
| tag | 32-byte HMAC over prefix, route fields and body |

The 16-byte routing fields apply to established packets; handshake has its own
fixed schema. The prefix body length includes the 16 routing bytes plus the
typed body, excluding the 24-byte prefix and 32-byte tag. Thus the exact total
packet size is `24 + body_length + 32`; established packets require
`body_length >= 16`, and overhead around their typed body is 72 bytes.
Reject truncated packets, trailing bytes and checked-length overflow.
Direction is bound by the MAC key and state, not inferred from an IP.
Do not add one blanket packet anti-replay window that would silently discard
the already admitted old FEC shards which #18 allows to complete.

| Identity | Scope / Lifetime |
|---|---|
| DatagramID u64 | Original business Datagram, one incarnation and direction; unchanged by packing/migration |
| GroupID u64 | One immutable encoding group; monotonically allocated across all encoding epochs |
| EncodingEpoch u32 | Frozen layout/budget context for a group; never changes its shard bytes |
| PathID + Generation + Direction | Confirmed reachability/PLPMTU and scheduling scope |
| PathBudgetEpoch u32 | Monotonic authenticated budget publication for one directed path |
| KCP conv/sn/ACK space | Owned by one KCP engine per reliable Session; not Datagram IDs |
| KCP packet assembly ID u64 | One immutable emitted KCP packet during outer fragmentation; not KCP delivery dedup |
| smux StreamID | Owned by the negotiated smux profile within that reliable Session |

All IDs have checked exhaustion and never wrap. Group lookup rejects metadata
changes for an already seen GroupID; changing epoch is not permission to reuse
that ID. Receiver Datagram dedup is independent of GroupID/epoch. Limit the
number and bytes of admitted groups, reassemblies and retired epoch records.

## 6. FEC Packing And Reassembly

Proposed FEC_BUNDLE body: record count u16, reserved zero u16, then shard
records. A record is 18 bytes of metadata plus its epoch's exact `ShardBytes`:

| Record Field | Bytes |
|---|---:|
| GroupID | 8 |
| EncodingEpoch | 4 |
| LogicalBytes | 4 |
| ShardIndex | 1 |
| Flags, zero | 1 |

The admitted EncodingEpoch provides LayoutID, k/r and exact ShardBytes for
every group, including tails. Those values are not inferred from route budget
epochs. Every record occupies exactly 18+ShardBytes bytes, so mixed-epoch
bundles remain unambiguous after authentication and read-only context lookup.
An unknown/unacknowledged epoch rejects the entire bundle before allocation.
Every group uses equal-length RS shards and immutable logical length.
Validate n=k+r<=256, index<n, canonical full-shard zero padding
and all integer products before allocation. A bundle has at most 16 records
and cannot contain two shards of the same group on one path in the protected
five-path/RS(3+2) profile. Each record is authenticated by the enclosing MAC.

The logical bytes recovered from k shards are a u16 manifest version, u16
record count, followed by 20-byte fragment descriptors and their concatenated
payloads. Descriptor: DatagramID u64, total length u32, offset u32, fragment
length u32. Require sorted canonical descriptors, bounded record count,
offset+length<=total, consistent total length and rejection of conflicting
overlap. Empty Datagram is exactly one zero-length descriptor at offset zero.
Metadata and data are FEC-protected together; do not rely on one unprotected
manifest packet. No application delivery occurs from unverified bytes.

With U as complete UDP payload cap, E=72, bundle prefix B=4, record header
R=18, m fragment descriptors and application byte sum A:

```text
C = U - E - B - R
L = 4 + 20*m + A
S = admitted_encoding_context.ShardBytes <= C
L <= k*S
padding = k*S - L
one-shard UDP payload = E + B + R + S
IPv4 L3 group bytes = (k+r) * (E+B+R+S+28)
IPv6 L3 group bytes = (k+r) * (E+B+R+S+48)
payload efficiency = A / L3_group_bytes
```

A greedy bounded packer may cut a Datagram into fragments so that a stream of
1400-byte inputs can actually fill groups. Packing only whole 1400-byte
records can waste most of a third slot once manifest overhead is included.
All arithmetic includes manifests, padding, ACKs and outer authentication.
Repair/probe/control overhead is added separately, never counted as goodput.

For the fixed Session budget profile, epoch ShardBytes=U-94. Tail groups use
the same S and zero-pad all k data shards through k*S; no variable record
length or out-of-band short-tail exception exists. Low-rate padding costs
are an explicit measurement requirement. The group manifest permits at most
one descriptor per DatagramID per group; a spanning Datagram continues in
another group. Adjacent fragments of one original group can therefore be
merged canonically when verifying a migration transaction.

Worked full-load IPv4 example, five independently shaped 100 Mbit/s paths,
RS(3+2), sequential 1400-byte input Datagrams, one shard/group/path:

| Complete UDP budget U | Fixed S | k*S | Useful bytes, conservative full group | IPv4 L3 bytes/group | Conservative codec ceiling | Long-stream estimate |
|---:|---:|---:|---:|---:|---:|---:|
| 1472 | 1378 | 4134 | 4050 | 7500 | 270.00 Mbit/s | 270.14 Mbit/s |
| 1200 | 1106 | 3318 | 3234 | 6140 | 263.36 Mbit/s | 264.46 Mbit/s |

A full group crosses at most four 1400-byte Datagram fragments, hence
`A>=k*S-4-4*20`. Over a long stream, one fragment split per group gives
`m approximately 1+A/1400`, so `A approximately (k*S-24)*70/71`. These are
codec/packing ceilings before ACK/probe/control bytes, startup/tail padding,
loss, pacing inefficiency or CPU costs. The actual capacity calculation must
use measured descriptor counts, padding and all control traffic; it cannot
count duplicate delivery as unique application goodput. With repair disabled,
DATA completion ACK cost is exactly zero; KCP-over-FEC application ACK
Datagrams still consume their own reverse-direction RS groups and padding.
Report both directions' accounting separately for independent link shapers.

The acceptance remains measured RS single-flow goodput >=250 Mbit/s and >=90%
of the reproducible efficient capacity bound, with the same protection and
five-path assumptions. Passing arithmetic or lowering a model ceiling does
not satisfy it. At U=1472, 250 Mbit/s already requires about 92.54% of the
270.14 Mbit/s no-control estimate; U=1200 requires about 94.53%. Tail padding
or control costs that prevent the threshold require a layout/implementation
change and renewed vectors, not reduced acceptance. IPv6 replaces the 28-byte
outer cost with 48; each path's safe U must also respect its actual IPv6 MTU.

First implementation delivers only complete original Datagrams after enough
groups have reconstructed all fragments. It may deliver one finished Datagram
while another Datagram sharing a group still awaits other fragments; it does
not deliver directly from partial systematic shard sets. This is a clear
failure/latency policy while preserving any-k-shard group recovery.

Flush when capacity, max record count or max delay is reached. Tail batches,
empty writes, cancellation and Close consume finite time. Original payload
copies, encoded bytes, queued shards and reassembly storage all consume the
same resource ledger. A fragmented Datagram's deadline is fixed at first
receiver admission; later fragments or migrations do not refresh it.

### Public Write Semantics

Select explicit v2 aggregation admission semantics. With aggregation disabled,
WritePacket retains synchronous encoding/socket-attempt completion; with
aggregation enabled it returns nil only after copying the whole Datagram,
reserving its metadata/byte ownership and assigning an ID in the bounded queue.
The caller may reuse its slice after return. A rejected admission does not
send a prefix, consume an ID or retain payload. Full queues return the proposed
ErrResourceLimit immediately; they do not create an unbounded wait queue.

Concurrent admissions linearize at queue commit. DatagramID order follows that
commit order, not call-start order or physical send order. The oldest queued
byte starts the 250us max-delay timer; later writes do not extend it. Seal on
capacity, descriptor limit, that deadline or Flush. max_records means fragment
descriptors per group; max_queued_datagrams counts whole admitted Datagrams,
including an empty Datagram. A group is bounded by the smaller of configured
max_group_bytes and k*(current_payload_budget-94), with checked arithmetic.
Reject oversized whole Datagrams against limits.max_datagram_size before
copying, even though a legal Datagram may span several groups.

Keep Session's three required methods unchanged. Add this optional interface,
implemented by v2 Datagram sessions; v1 behavior is unchanged:

```go
type DatagramSession interface {
    Session
    Flush(context.Context) error
    CloseGracefully(context.Context) error
}
```

Flush captures the committed admission frontier, seals its tail, and waits
for every original shard through that frontier to finish its local socket
attempt or terminal send failure. It does not wait for repair/remote ACK or
application consumption, and excludes writes committed after the fence.
Concurrent fences share bounded sequence bookkeeping. A cancelled Flush stops
only that wait; already admitted Datagrams remain owned and may send later.
No new background waiter is retained after the call returns.

The first asynchronous send failure is a sticky Session latch returned by
subsequent Flush/CloseGracefully; Peer.Errors remains the existing lossy bounded
diagnostic channel and is not the sole delivery-error record. Statistics count
every classified send failure even when diagnostics overflow. A successful
admission is a local queue result, never a delivery guarantee.

Close stops admissions, cancels workers and discards queued/unsent work within
bounded local cleanup. CloseGracefully stops admissions, flushes all accepted
work and waits for repair obligations only until their original deadline or
the supplied context, then releases remaining state and returns the first
error (context error takes precedence if its deadline caused abandonment).
It is idempotent and wakes concurrent Flush/ReadPacket callers. Empty writes
follow the same queue, ID and fence rules and produce one zero-length record.
Budget changes may repack unsealed bytes into a new epoch; once a GroupID is
assigned and encoded, only the negotiated immutable-group migration applies.

## 7. Repair And Dedup Span Contract

Repair is negotiated on/off and fixed per Datagram Session. Disabled means no
DATA completion ACK, missing-shard feedback or retransmission. Health/MTU
probes remain available because they are not DATA repair.

Proposed authenticated feedback types:

- DATAGRAM_COMPLETE: canonical bounded ranges of original Datagram IDs.
- GROUP_MISSING: GroupID, EncodingEpoch, feedback sequence and bounded present
  shard bitmap (up to 256 bits); receiver has fewer than k distinct shards.
- DATAGRAM_STATUS_REQUEST: bounded original-ID ranges, to recover lost ACKs
  without retaining an unbounded GroupID-to-Datagram mapping at the receiver.

Completion ACK means full authenticated Datagram reassembly and dedup marking,
not application consumption. If the Datagram delivery queue drops newest, an
already issued completion ACK does not promise that the application read it.
Do not free all repair state on a group-only ACK while its original Datagrams
remain incomplete. Retain enough original bytes and immutable group metadata
until all relevant Datagram ACKs, the original deadline or an explicit abort.

Normal repair resends original bytes under the same GroupID/DatagramID/shard
index. With m authenticated distinct shards, send at most k-m useful missing
shards per feedback decision; repeated feedback must not repeat that decision
unboundedly. Track actual send timestamps and distinct newer ACK evidence,
not just numeric ID gaps. Apply an RTT/variance/reordering wait before fast
repair, with a sender timeout for whole-group and tail loss. Use original
age and attempt limits, byte quotas and the same scheduler/pacing as DATA.

Window negotiation must limit outstanding ID *span*, not only item count or
rate times age. For a receive window W, the sender cannot allow its highest
allocated Datagram ID to exceed its oldest repair-eligible original ID by
W or more. The same rule applies to cached Group IDs. On pressure, block
bounded admission or expire old obligations according to Datagram semantics;
never continue promising repair for an ID the receiver may have retired.

First v2 can retain W=65536 while advertising it explicitly. Session-local
Datagram and group windows are separate; already admitted pending assemblies
keep their original deadlines. Epoch migration does not reset either window.
Cap ACK ranges/counts and intersect with bounded state without iterating an
attacker-supplied enormous range. Reject overflow, ACKs for unsent IDs,
conflicting group metadata and feedback from another incarnation/direction.

## 8. MTU State And Migration

Maintain separately: local send hard cap, peer receive hard cap, local receive
hard cap, directed path confirmed PLPMTU, local safety clamp, committed path
budget epoch, group encoding epoch and active scheduling membership.
The peer's receive ceiling is not an instruction to send at that size.

MTU_PROBE and MTU_PROBE_ACK bind random token, exact UDP payload length,
direction, PathID, generation and proposed budget epoch. Pad probes to the
claimed size and validate the actual observed length. Only an authenticated
matching ACK for an outstanding probe is positive size evidence. `Send`
success, interface MTU and unauthenticated ICMP are not confirmation.

Ordinary data/control/repair obey the current local path clamp. A bounded
probe is the sole exception that may exceed confirmed PLPMTU, never either
hard cap. Small health success does not confirm large-packet reachability.
Use bounded repeated evidence and smaller successful probes before classifying
a size blackhole, then backoff and hysteresis for upward recovery.

For a decrease, immediately clamp local sends, then retransmit an authenticated
PATH_BUDGET_UPDATE until its bounded ACK/deadline. While publication is in
flight, old epoch packets may be sent only at the smaller local limit. An
increase requires successful large probe plus budget commit ACK before DATA
uses the larger size. Duplicate identical updates are idempotent; same epoch
with changed contents or unknown future epochs cannot mutate state.

Encoding epochs have their own authenticated announcement and ACK, separate
from directed PATH_BUDGET_UPDATE. A bundle may carry groups from multiple
admitted encoding epochs, so its route header cannot stand in for the groups'
layout context. Send a new encoding epoch's groups only after its context is
acknowledged; bound retained contexts, retry age and outstanding announcements.

Old epoch receive grace is capped by count and age. It permits reception of
already transmitted bytes, not sending old oversized bytes on a shrunken path.
Rebinding or new generation resets reachability and size confirmation; do not
copy a previous generation's PLPMTU just because PathID stayed the same.

For fixed-Session budgeting, all scheduled paths must carry the Session's
chosen group size. Preserve small paths in the active aggregation set rather
than silently excluding them to improve an MTU score. Before #14, use the
minimum confirmed budget of that eligible set.

FEC/repair migration policy:

1. Drain old immutable shards only over currently healthy paths that fit them.
2. If no path fits and migration was negotiated, re-encode retained original
   fragment records into new Group IDs under the new encoding epoch.
3. Keep every Datagram ID, original expiry and delivery dedup state. Bound
   migration attempts (candidate 2), old/new group aliases and retained bytes.
4. Authenticate migration references and reject a conflicting alias. Old/new
   groups may interleave, but original Datagrams are delivered at most once.
5. If migration is unavailable or exhausts limits, Datagram mode drops with a
   classified event. KCP pauses within limits or fails the reliable Session;
   it must not silently discard already acknowledged stream bytes.

Ordinary repair must not invoke this re-encoding rule under a new ID and call
it a retransmission. #20 must define migration framing before #13 implements it.

## 9. Native KCP And Reliable Closure

Use a fixed, reviewed kcp-go core/session through a virtual PacketConn or an
equivalent adapter that retains MPUDP authentication, listener reply socket,
path generations and scheduler. One KCP conv/sequence engine spans all paths.
No new bypass UDP socket, no MPUDP FEC, no KCP FEC, no lower-layer DATA ARQ.
KCP's single RTT/cwnd remains a limitation, not native multipath congestion
control. Keep congestion control enabled by default; tune windows from BDP
within byte reservations rather than copying benchmark `nc=1`.

Map `kcp.fast_retransmit.enabled=false` to an explicit reviewed backend policy
that suppresses both fast and early retransmission. Setting `NoDelay` resend
to zero alone is insufficient. The isolated candidate adds a default-enabled
core/UDPSession setter, guards gap accounting and both retransmit branches,
and leaves RTO and NoDelay tuning independent. Pin an upstream accepted
revision or maintained dependency fork carrying this small patch before
shipping the setting; the experiment is not itself a maintained release.

Outer KCP packet fragmentation is a separately negotiated transport capability:
an assembly ID, total length, offset and fragment length identify immutable
bytes of one KCP output packet. Authenticate and bound fragments; only a
complete packet reaches KCP Input. Lost pieces cause no outer retry and no
KCP ACK; KCP's RTO re-emits its own segments, possibly in a new output packet
with a new assembly ID. Never merge pieces from distinct transmissions.

This permits a queued old KCP segment to cross smaller UDP paths without IP
fragmentation or changing its KCP sequence identity. The maximum inner KCP
packet remains a validated library cap (1500 for the observed UDPSession), not
3387 or an unchecked SetMtu success. After a shrink, use smaller MSS for new
segments if supported, while fragments handle old queued output. If the
capability was not negotiated, choose a stable conservative inner MTU or fail
boundedly when no path can carry old packets. Do not drop confirmed bytes.

Large paths can bundle multiple length-delimited KCP packets/pieces inside one
authenticated outer packet. This is neither FEC nor another ACK domain.

Before KCP Input, bounded outer packet drops are network loss and can recover
through KCP. After KCP acknowledges bytes, the receive pipeline must apply
flow control and retain them until application consumption or explicit abort.
Do not reuse the public Datagram drop-newest delivery queue for a stream.

Candidate public additions, separate from existing Datagram APIs:

```go
type Stream interface {
    net.Conn
    CloseWrite() error
    CloseGracefully(context.Context) error
}
// Peer.DialStream(ctx) and Peer.AcceptStream(ctx) serve one business connection.
// ReliableSession.OpenStream(ctx)/AcceptStream(ctx) expose explicit mux ownership.
```

Mux off means one reliable Session per business connection. Mux on means
streams on one long-lived explicitly owned reliable Session; no hidden Carrier
pool or automatic extra KCP Sessions. Existing Datagram APIs reject KCP mode
with ErrProtocolUnavailable rather than returning a misleading packet adapter.

KCP Close/WaitSnd alone is not reliable FIN. With mux off, negotiate an ordered
record stream over KCP with DATA, FIN(final byte offset), and FIN_ACK. EOF
follows all preceding bytes, CloseWrite preserves reading, and graceful close
waits within a caller deadline for the agreed drain/FIN condition. Ordinary
Close is a bounded local close/abort that wakes waiters; do not promise a
graceful drain from it. Explicit abort may discard bytes and must be visible
as a stream error rather than clean EOF. Concurrent partial Write follows
net.Conn's `(n, error)` semantics. Counters distinguish graceful and abort.

## 10. smux Dependency Gate

Evaluate smux v1.5.57, exact tag commit
`3b4ec04d359934256b3adea7133374e3c93a0622`, using smux wire version 2 for its
per-stream window support. The library tag number is not the smux wire version.
Require the exact mux/framing profile in MPUDP handshake; raw KCP and smux are
not interchangeable payloads.

Runtime tests verified two dependency gaps. Both FINs before a slow reader
caused all 3072 unread bytes per direction to disappear in wire versions 1/2,
for both Read and WriteTo. Separately, 32 remote SYNs created 32 streams and
256 initial buffer-ring slots before any Accept call, without spending any
receive-byte tokens. The default accept backlog is 1024, not a configurable
Peer/session admission limit. Upstream master
`ae956bb8d67bab37a312869b1d38ee3f52a7397a` has identical relevant source.

The isolated tail fix retains stream state until both FINs and receive drain,
checks/removes under the existing session-then-buffer lock order, and makes
graceful EOF deterministic for WriteTo. It passed the reproduction and existing
half-close regressions under 20 race runs. Source and exact evidence are in
[archived smux evidence](evidence/20260905-smux/README.md); the patch is not an
upstream accepted revision or a shipping dependency.

Candidate admission profile, to define jointly with #20:

- Reserve exactly one bidirectional control stream, opened first by the
  initiator (candidate ID 3 for the pinned smux allocation convention). Allow
  that ID once, before business admission. Charge its metadata and byte credits
  to Peer and Session limits, with a separate business-stream count; reserve
  control receive/write capacity so business pressure cannot consume it.
- Stock smux v2 starts each stream with a 262144-byte peer window. Reserve at
  least that initial receive obligation, plus any accepted-frame overshoot,
  even for the control stream, or negotiate and test a backend initial-window
  change. MaxStreamBuffer alone does not establish a smaller initial credit.
  The sum of business obligations must leave the control receive reserve free
  in the Session's shared bucket. A control stream still shares KCP HOL.
- Use bounded, length-delimited OPEN_REQUEST, OPEN_GRANT, OPEN_REFUSE,
  OPEN_READY, OPEN_CANCEL and STREAM_ABORT control records. Bind each to the
  incarnation, direction, request ID, reserved StreamID and original deadline;
  a grant binds its credit profile. Names describe semantics only; record
  numbers/layouts are not frozen. Candidate limits are 128 pending requests,
  256-byte records and 32 KiB queued control bytes, all charged to ownership.
- Reserve a local stream ID without allocating its stream state or sending
  SYN. Send OPEN_REQUEST; the receiver atomically acquires Peer/session count,
  pending-accept capacity and byte credits before OPEN_GRANT. OPEN_REFUSE
  returns a classified Open error and creates no business stream on either
  side. Pressure is not a Session failure and does not close existing streams.
- Send the business SYN only after its grant. A backend preallocation hook
  consumes the matching live grant before newStream/map/channel/ring creation.
  An adapter Accept pump binds that admitted stream to the reserved pending
  accept slot and sends OPEN_READY; expose a successful Open only after READY.
  The ordinary stock OpenStream return does not itself prove remote admission.
- Timeout/cancellation sends OPEN_CANCEL and terminates the caller's wait.
  Late grants trigger cancellation without a SYN; grants expire at their
  original bounded deadline and do not extend through retries. If SYN won the
  race, explicit STREAM_ABORT cancels only that newly admitted stream, frees
  its retained bytes, and reports an error rather than clean EOF. Cancelled
  request/stream IDs are never reused; retained replies/tombstones are bounded.
- Repeated SYN for an active ID cannot consume a second grant. Unknown,
  cancelled, expired or already-retired IDs fail the preallocation hook and
  allocate no stream. Duplicate control messages are idempotent. A late SYN
  cannot revive a released lease. Release count/byte leases once, after full
  drain or explicit abort, including pending accepts and Session teardown.

An isolated control-stream prototype plus two small candidate backend hooks
proved one-business-stream admission, refusal before business allocation,
ungranted-SYN rejection, sibling bidirectional traffic after refusal and credit
reuse after drain. Its five-byte request/reply fixture is not the production
wire profile. It does not yet prove cancellation/late-grant races, OPEN_READY,
global multi-Session leases, initial-credit reservation or control starvation.
The candidate APIs are a local pre-SYN admission callback and a remote
pre-newStream grant check; both preserve stock behavior when unset. A remote
hook alone cannot communicate refusal: stock FIN means half-close, and stock
OpenStream has no acceptance ACK.

The tag does provide CloseWrite and deadlines. Its fixed 30-second internal
open/close timeout still needs context/deadline integration. Freeze the
maintained dependency and exact admission profile only after the remaining
gates pass. These control payloads require an explicit negotiated MPUDP profile
even though they do not add smux wire commands.

smux cannot remove head-of-line blocking in the underlying single KCP stream.
Test simultaneous large and small streams in that same Session and report
P95/P99, timeout rate and HOL. Closing one stream must not close sibling streams.

## 11. Resource, Scheduler And Error Contract

Provisional defaults from section 3 are ceilings, not promises that every
maximum can be allocated simultaneously. Account copied application bytes,
FEC scratch/shards, repair copies, migration copies, KCP queued/unacked bytes,
outer fragment assemblies, mux buffers and queued accepts against owned-byte
credits. Count-bounded metadata adds a known upper bound; do not label this
ledger as exact process RSS.

Reserve enough receive credit before advertising KCP/mux windows. If advertised
credit cannot be reserved globally, reject admission or reduce the offered
window before handshake, never acknowledge and then drop data. Release credit
exactly once on consumption, ACK/expiry or Close. Keep a small separately
bounded control reserve so DATA pressure cannot indefinitely starve close or
credit return. Apply limits to authenticated peers too.

Suggested further hard bounds: 256 packets and 1 MiB per path send queue,
16 records per wire bundle, 26 feedback ranges at the512-byte floor, one active
MTU probe per directed path, two old epochs by default, two migration attempts
per original obligation,
bounded pending handshake/accept age. Bound concurrent goroutines/worker count;
closing must cancel timers, pending Open/Accept, writes and credit waiters.

Pacing accounts complete outer packets, including FEC, ACK, repair, probes,
padding and packet bundling. Define configured link capacity explicitly as
IPv4/IPv6 L3 bytes unless another accounting mode is selected; do not compare
UDP-only tokens with L3-shaped benchmark capacity. Control gets bounded
priority, not uncharged bandwidth. Scheduler eligibility includes actual
packet size, healthy path generation and current local safety clamp.

Health and size reachability are separate. Use authenticated bounded probes,
single-direction failure cases and recovery probation. A reference 40 ms RTT
profile must meet #22's <=1 second stop-assignment goal without relying on
Endpoint TTL. Expose queue delay, path state transitions and probe reasons.

Keep existing role, ErrResourceLimit, ErrNoAvailablePaths, send and MTU errors.
New classes are ErrProtocolUnavailable, ErrProtocolViolation, ErrIDExhausted
and ErrStreamAborted. Authenticated mode/capability mismatch continues to
match ErrHandshakeIncompatible. All causes exclude packet contents, PSKs,
complete identities and raw addresses in standard diagnostics. Deadline errors
retain net.Error/standard deadline matching. Do not abort every usable Session
because one path has MTU failure or a temporary blackhole.

## 12. Per-Carrier Layout With A Defensible First Step

Keep equal-length RS groups at a base shard size that small paths can carry.
On larger paths, combine shards from *different* groups in one FEC_BUNDLE.
Keep at most one shard of each RS(3+2) group on a given path in the five-path
profile. Losing a bundled packet erases one shard from each affected group;
each group still recovers from any three of its five shards. No unequal raw
shards are passed to the Reed-Solomon codec, and small paths remain active.

Illustration using the proposed wire sizes: small U=800 gives S=706;
one shard occupies 76+18+706=800 bytes. A large path with U=1800 can carry
two different-group shards in 76+2*(18+706)=1524 bytes instead of two 800-byte
packets. For two groups over three small and two large paths, packet count
falls from ten to eight. IPv4 L3 bytes fall from 8280 to
6*(800+28)+2*(1524+28)=8072, with identical useful group data and path erasure
protection. The intended benefit is lower PPS/header cost; measure CPU and
latency rather than claiming extra physical bandwidth.

Bundling waits remain bounded and should not delay control traffic behind a
large group. Under equal MTUs, measure the extra scheduling/metadata overhead.
For fewer than five paths or multiple shards per path, state the changed path
failure envelope explicitly. A later layered or unequal-protection profile
needs separate information/capacity proofs and an explicit LayoutID. Do not
claim all data survives arbitrary two-path loss when the remaining paths do
not carry enough information to reconstruct it.

## 13. Freeze Gates And Delivery Order

The registry and configuration/API companion choose the proposed fields,
numbers, defaults and lifetime rules. Remaining gates are concrete evidence:

1. Review and encode byte-exact handshake/KDF, mode and initial-budget vectors.
2. Implement strict normalization and v1 compatibility/error matrices.
3. Validate the fixed-shard fragment layout, ownership/fences, low-rate padding
   and >=250 Mbit/s plus >=90% measured-capacity acceptance.
4. Test ACK/expiry ownership, ID spans and whole/tail loss under feedback loss.
5. Test migration roots/aliases, epoch pins, old-route grace and KCP pieces
   together; future tasks must not invent additional wire semantics.
6. Pin a maintained upstream/fork delivery for the reviewed dependency patches;
   validate KCP MTU/window/congestion behavior and smux lifecycle/admission.
7. Exercise global credit/cancellation races and the complete control profile,
   beyond the narrow dependency fixture's tested hook placement.
8. Measure the configured health/pacing/repair profile at
   20/40/80/160/240 ms RTT with the specified fault and cleanup matrices.

Suggested sequencing: #20 first lands reviewed specification, strict config,
versioned codec, authenticated handshake and conformance vectors. #21 and #22
then implement fixed-budget efficient groups and scheduling; #23 repair and
#24 native KCP consume those contracts, followed by #25 mux. #13 implements
budget discovery and the already negotiated migration machinery; #14 adds
per-path bundling/layout optimization. #26 rechecks exact promoted SHA and
resource cleanup across the combined matrix. Defining future capabilities in
#20 must not advertise them as operational before their implementation lands.

Required vectors include all version/mode mismatches; reflected/stale handshake
and feedback; wrong generation/direction/probe length; exact integer maxima and
reserved bits; empty/heterogeneous/fragmented Datagram packing; repeated ACK
ranges; whole/tail loss; a paused low-ID sender crossing the dedup window;
old/new epoch interleaving; all paths shrinking with queued KCP data; slow
consumer, half-close tail and abort; cancelled Open/Accept; and authentication
or resource-limit failures creating no retained state. Add fuzz/race and
IPv4/IPv6 no-IP-fragment integration proof to each implemented contract.

## Sources

- Issues: https://github.com/mofelee/mpudp/issues/20 through /25, plus
  https://github.com/mofelee/mpudp/issues/13 and /14 (read live 2026-09-05).
- Current code: config/{config,defaults,parse}.go,
  internal/wire/{constants,types,codec}.go, peer.go, docs/API.md,
  root and integration/perf go.mod, at the reviewed main SHA above.
- KCP pin: `/root/go/pkg/mod/github.com/xtaci/kcp-go/v5@v5.6.72/{sess,kcp}.go`.
- smux source and isolated runtime fixtures at v1.5.57:
  https://github.com/xtaci/smux/blob/3b4ec04d359934256b3adea7133374e3c93a0622/stream.go
  and https://github.com/xtaci/smux/blob/3b4ec04d359934256b3adea7133374e3c93a0622/session.go.
  Exact source, patches, commands and limitations are in the archived evidence.
