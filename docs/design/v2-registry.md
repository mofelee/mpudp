# Proposed V2 Registry And State Rules

Status: concrete review proposal, not a frozen or implemented protocol.
This supplies the byte assignments for the [joint contract](v2-joint-contract.md).
All integers in this document are unsigned big-endian. Embedded KCP/smux
bytes keep their upstream framing and byte order unchanged.

## Envelope

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | Magic `MPUD` |
| 4 | 1 | Version `2` |
| 5 | 1 | Type from the registry below |
| 6 | 2 | BodyLength B, excluding prefix and tag |
| 8 | 16 | Nonzero SessionID |
| 24 | B | Body |
| 24+B | 32 | HMAC-SHA-256 tag |

Total UDP payload is exactly `56+B`, at most 65507. Reject trailing bytes,
truncation and B>65451. Established bodies begin with PathID u32,
PathGeneration u64, PathBudgetEpoch u32: B=16+typed_body_length. Thus their
overhead around typed data is 72 bytes. Only handshake types omit this route
prefix. Reserved fields and unused flag bits must be zero.

PathIDs are initiator-assigned Carrier indices 1..negotiated MaxPaths, never
addresses. The initiator owns one monotonic generation per Carrier, beginning
at 1; changing it invalidates both directions' health/size evidence. The
direction is the MAC-key role, while budgets remain separate per direction.
No counter may wrap. Session exhaustion requires a fresh incarnation.

## Type Registry

All unlisted values, including zero, are invalid. Type-specific variable
records must consume the exact body length. A receiver parses structurally,
authenticates, checks the negotiated capability and bounds, then mutates.

| Value | Name | Typed Body After Route Fields, Unless Handshake |
|---:|---|---|
| 1 | HELLO | Fixed 456-byte handshake body |
| 2 | CHALLENGE | Fixed 456-byte handshake body |
| 3 | FINISH | Fixed 456-byte handshake body |
| 4 | READY | Fixed 456-byte handshake body |
| 5 | REJECT | Fixed 456-byte handshake body |
| 16 | PATH_JOIN | ClientPathNonce[16], zero[16], zero padding to 440 bytes |
| 17 | PATH_CHALLENGE | ClientPathNonce[16], ServerPathNonce[16], zero padding to 440 |
| 18 | PATH_CONFIRM | Both path nonces, zero padding to 440 |
| 19 | PATH_READY | Both path nonces, zero padding to 440 |
| 20 | PATH_REBIND_HINT | Outstanding health Token[16] |
| 21 | HEALTH_PING | Token[16] |
| 22 | HEALTH_PONG | Echo Token[16] |
| 23 | MTU_PROBE | Token[16], TargetDirection u8, zero u8, ProbeBytes u16, ProposedEpoch u32, zero padding |
| 24 | MTU_PROBE_ACK | Same first 24 bytes as probe, no padding |
| 25 | PATH_BUDGET_UPDATE | TargetDirection u8, Kind u8, PayloadBudget u16, NewEpoch u32 |
| 26 | PATH_BUDGET_ACK | Exact 8-byte update body |
| 27 | ENCODING_CONTEXT | EncodingEpoch u32, LayoutID u16, ProtectionID u16, k u8, r u8, ShardBytes u16, MaxDescriptors u16, zero u16, MaxLogicalBytes u32, zero u32 |
| 28 | ENCODING_CONTEXT_ACK | EncodingEpoch u32, SHA256(context typed body)[32] |
| 32 | FEC_BUNDLE | Count u16, zero u16, Count shard records from joint section 6 |
| 33 | DATAGRAM_COMPLETE | FeedbackSeq u64, Count u16, zero u16, Count inclusive (First u64, Last u64) ranges |
| 34 | GROUP_MISSING | GroupID u64, EncodingEpoch u32, FeedbackSeq u64, BitmapBytes u16, zero u16, present bitmap |
| 35 | DATAGRAM_STATUS_REQUEST | RequestSeq u64, Count u16, zero u16, Count inclusive ID ranges |
| 36 | GROUP_MIGRATE | MigrationID u64, OldGroupID u64, FirstNewGroupID u64, NewEncodingEpoch u32, NewGroupCount u16, zero u16, OriginalLogicalBytes u32, OriginalLogicalDigest[32] |
| 37 | GROUP_MIGRATE_ACK | MigrationID u64, Status u8, zero[3], exact SHA256(migration typed body)[32] |
| 38 | DATAGRAM_EXPIRED | FeedbackSeq u64, Count u16, zero u16, Count inclusive ID ranges |
| 48 | KCP_BUNDLE | Count u16, zero u16, Count KCP piece records below |
| 63 | CLOSE | Reason u16, zero u16, CloseSeq u32 |

Ranges are sorted, disjoint and nonadjacent; canonical adjacent ranges merge.
Count is 1..26 at the 512-byte budget because `72+12+26*16=500`; use the
smaller of this negotiated count and the actual selected path's byte limit.
Never scan every integer in a received range. GROUP_MISSING bitmap uses bit
`index%8` of byte `index/8`, least-significant bit first, and has exactly
ceil((k+r)/8) bytes; unused high bits are zero. Sequence values begin at 1.
Repeated identical feedback is idempotent; the same sequence with altered
contents is invalid. Keep one latest sequence per bounded referenced object,
not an unbounded history. Completion/expired sequences share a direction's
feedback sequence space; a stale sequence cannot undo a newer terminal state.

Every KCP piece record is PacketAssemblyID u64, TotalBytes u16, Offset u16,
PieceBytes u16, Flags u16, followed by PieceBytes immutable packet bytes.
Flags=0. Count=1..16; TotalBytes=24..1500 and PieceBytes>0; checked
Offset+PieceBytes<=TotalBytes. Unfragmented packets use Offset=0 and
PieceBytes=TotalBytes. Distinct emissions get distinct assembly IDs, even
when KCP retransmits the same segment. Assembly retention is at most 3s,
1024 records and the Session/Peer byte ceilings; missing outer pieces never
produce outer DATA retransmission or KCP acknowledgement.

LayoutID=1 means the equal-length RS fragment-manifest layout in joint
section 6; manifest version=1. ProtectionID=1 means any k distinct shards
recover one group, with distinct initial placement on min(k+r,active_paths)
paths. It does not promise recovery from a fixed number of path failures when
multiple shards share a path. Membership/actual assignment belongs to the
scheduler's bounded group state and metrics, not the RS decoding equation.
Larger paths only bundle shards from different groups in this profile.

ENCODING_CONTEXT supplies the exact fixed ShardBytes of every record using
that epoch, including a tail group. MaxLogicalBytes is a ceiling, at most
k*ShardBytes. Tails zero-pad all data shards to that full length. The 18-byte
record header plus context.ShardBytes therefore defines each record boundary
without a length guess, even when one bundle contains several admitted epochs.
Every context, including epoch 1, is explicitly announced and requires its
exact context ACK before DATA uses it. Path/health/budget control may proceed
before a context exists. The sender chooses S no larger than the eligible
paths' safe minimum minus94 for the base layout; the context carries the
exact S, logical ceiling, descriptor limit and protection profile, avoiding
an implicit derivation from asymmetric hard caps. At most one announcement is outstanding;
the original 5s deadline and eight-attempt cap cannot be extended by retries.

One old group may migrate to 1..256 consecutive newly reserved GroupIDs;
checked FirstNewGroupID+NewGroupCount-1 must not overflow or exceed the group
ID span. All replacement groups use the named admitted encoding context and
contain only fragments from that old group's logical input. Fragment ranges
may split to fit smaller shards. Rejoining adjacent ranges per DatagramID
must recover exactly the old canonical manifest and payload, whose SHA256 is
OriginalLogicalDigest. The receiver verifies that whole transaction before
admitting replacement fragments to Datagram reassembly. Already completed
original DatagramIDs still deduplicate through the separate Datagram window.
This is bounded control/data repartition, not unequal RS or one large group
silently using smaller shards. Reserve transaction bytes and at most two
migration attempts per original group; ACK the migration announcement before
replacement groups send. All aliases retain the original sender/receiver
deadlines. Same-ID ordinary repair never uses GROUP_MIGRATE.

OldGroupID is always the original root, never a replacement alias. Create its
receiver lease on the first valid original shard or accepted migration,
whichever arrives first; it lasts the negotiated group decode timeout and
never refreshes across attempts. OriginalLogicalBytes bounds digest rebuilding
and must match a known original length. Reserve all new group slots, retained
contexts and worst-case transaction bytes atomically before accepting. The
transaction ceiling is 8 MiB, also constrained by Session/Peer limits; failure
returns resource pressure without creating partial aliases. At most two
attempts reference one root and one is active at a time.

Migration ACK Status=0 accepted, 1 root already committed to Datagram
reassembly, 2 root terminally expired, 3 resource pressure. Status1 allocates
no replacement transaction and tells the sender to query original Datagram
status rather than retransmit that root. Status2 ends this root's repair
obligation; status3 permits retry only within the original age/attempt limit.
Expired roots and IDs below the group floor cannot be recreated by migration.
All transaction/group/context leases release once on commit, expiry or abort.

A reconstructed ordinary group must atomically transfer all nonterminal
fragments to reserved Datagram ownership before marking its GroupID complete.
If capacity is temporarily unavailable, keep bounded decoded-pending group
ownership under the original deadline and retry on credit return. Do not
mark the group complete and discard an unreserved fragment: later same-ID
repair would otherwise be suppressed. Expiry terminally expires affected
incomplete originals and releases their owned bytes; completion and expiry
bits remain distinct. Already completed originals simply ignore duplicate
fragments, retaining their completion ACK semantics.

## Bootstrap Handshake

Every handshake packet is exactly 512 bytes: prefix24 + body456 + tag32.
Both v2 local send and receive hard caps must be at least 512 before opening
sockets. Handshake transmission never uses an unconfirmed larger local cap.
Do not fragment handshake packets or accept arbitrary padding contents.

| Body Offset | Bytes | Field |
|---:|---:|---|
| 0 | 16 | ClientNonce |
| 16 | 16 | ServerNonce |
| 32 | 32 | TranscriptDigest |
| 64 | 16 | ReturnPathToken |
| 80 | 2 | TLVBytes N, 0..372 |
| 82 | 2 | Flags, zero |
| 84 | N | Canonical TLVs |
| 84+N | 372-N | Zero padding |

HELLO has a fresh nonzero ClientNonce and zero ServerNonce, TranscriptDigest
and ReturnPathToken. CHALLENGE echoes ClientNonce, supplies fresh random
ServerNonce and ReturnPathToken, and carries SHA256(HELLO prefix+body) as
TranscriptDigest. The listener stores the pending transcript and binds the
token to the observed remote tuple, receiving local socket and bootstrap
Carrier. Before CHALLENGE, reserve provisional Session/pending-accept and
receive-credit accounting leases so a valid FINISH can install the selected
contract without competing a second time for capacity. These are bounded
ledger reservations, not FEC/KCP/mux objects; timeout/REJECT/Close releases
them once. Repeated identical HELLO reuses the live challenge without extending
its deadline. A different HELLO under the same pending SessionID is rejected.

Let H be the exact 480 HELLO bytes excluding its tag, C the exact 480
CHALLENGE bytes excluding its tag, and T=SHA256(H||C). FINISH and READY echo
both nonces and token, carry T, TLVBytes=0 and zero remaining bytes. Client
validates the selected contract before FINISH. Listener installs the bounded
Session only after FINISH verifies from the challenged tuple; READY confirms
that installation. Duplicate exact FINISH only resends READY. No endpoint,
FEC/KCP/mux state or public accepted Session is created from HELLO alone.

Use RFC 5869 HKDF-SHA-256, output length 32, with these exact ASCII labels
(no NUL suffix), PSK bytes as IKM, and empty salt for the handshake key:

```text
K_hs  = HKDF(PSK, salt="", info="MPUDP/v2/handshake", L=32)
K_c2s = HKDF(PSK, salt=ClientNonce||ServerNonce,
             info="MPUDP/v2/c2s"||SessionID||T, L=32)
K_s2c = HKDF(PSK, salt=ClientNonce||ServerNonce,
             info="MPUDP/v2/s2c"||SessionID||T, L=32)
```

HELLO/CHALLENGE/REJECT use K_hs; FINISH uses K_c2s; READY uses K_s2c.
Established tags use the sender role's directional key. Tag input is always
the exact prefix+body, including zero padding. Constant-time comparison is
required. Unknown Session/key lookup is read-only before authentication.
After Close, release directional keys and receive windows; a new attempt
uses a fresh SessionID and nonces. Captured FINISH cannot recreate an expired
pending record. Sharing the PSK still shares impersonation authority; none
of this encrypts application data or gives forward secrecy.

Pending lifetime is 10s from first accepted HELLO, eight handshake sends per
attempt, retry interval 1s, and 256 pending records Peer-wide. Retry does not
extend age. Try configured Carriers with bounded concurrent attempt slots
(default 1); each independent attempt gets fresh SessionID/ClientNonce. Only
the first completed attempt may become the public Session; other attempts
are cancelled and their pending records expire or close. An ambiguous attempt
after FINISH retries its exact FINISH until its deadline, not a different
transcript under that SessionID. Already established IDs reject fresh HELLO.

Authenticate before responding to malformed semantics. Bad MAC, unknown
version/type or unknown Session is silently dropped. An authenticated HELLO
with unsupported required capabilities, invalid normalized settings or full
admission capacity may receive one REJECT, no larger than its 512-byte request.
REJECT echoes ClientNonce, sets TranscriptDigest=SHA256(H), ServerNonce/token
zero, and includes only error TLV 0x800d. It creates no path/Session state and
is rate-limited with handshake capacity. A client trusts it only for a live
matching attempt; it never authorizes downgrade. Later fatal errors use CLOSE.

## Handshake TLV Registry

TLV header is Type u16, Length u16. Strictly increasing wire Type, no duplicate,
no nesting; maximum 16 TLVs and 372 total TLV bytes. High bit means required.
Unknown required TLV rejects; unknown optional TLV is skipped but included in
H/C/T exactly. HELLO/CHALLENGE each include exactly the twelve required TLVs
below; inactive mode parameters use their defined neutral zeros on wire.
REJECT contains only 0x800d. FINISH/READY contain none.

| Type | Length | Value And Selection |
|---:|---:|---|
| 0x8001 | 1 | Protocol: 1 Datagram, 2 KCP; exact match |
| 0x8002 | 16 | OfferedCaps u64, RequiredCaps u64; required subset of offered; CHALLENGE OfferedCaps is selected intersection and must cover both required sets |
| 0x8003 | 2 | Datagram LayoutID=1, or 0 for KCP |
| 0x8004 | 2 | k u8,r u8; Datagram exact config match, KCP both zero |
| 0x8005 | 2 | MuxProfile: 0 off, 1 smux-wire2 plus MPUDP admission-v1; exact match |
| 0x8006 | 2 | Discovery u8 (0 fixed,1 PLPMTUD), Scope u8 (0 session,1 per_carrier); exact match |
| 0x8007 | 8 | SendHardCap u16, ReceiveHardCap u16, BootstrapBytes u16=512, MaxInnerKCPBytes u16=1500 for KCP or 0; each endpoint advertises its own caps |
| 0x8008 | 20 | DatagramWindow u32, GroupWindow u32, MaxDatagramBytes u32, MaxFragments u16, MaxDescriptors u16, MaxDatagramAssemblies u32; all zero in KCP |
| 0x8009 | 8 | RepairMaxAgeMS u32, RepairMaxAttempts u16, zero u16; both zero when repair off |
| 0x800a | 28 | MaxBusinessStreams u32, MaxPendingAccepts u32, SessionReceiveBytes u32, StreamReceiveBytes u32, ControlReceiveReserve u32, MaxPendingOpens u32, MaxMuxFrameBytes u32; all zero in Datagram; last field zero when mux off |
| 0x800b | 8 | MaxOldEpochs u16, MaxMigrations u16, EpochGraceMS u32; negotiated minima |
| 0x800c | 4 | MaxPaths u16, BootstrapPathID u16; initiator configured Carrier count and winning configured index, responder ceiling and echoed index; 1<=BootstrapPathID<=effective MaxPaths<=256 |
| 0x800d | 4 | ErrorCode u16, RetryAfterMS u16; REJECT only, retry hint at most 1000 |

Resource values are receive capabilities, not instructions to allocate their
maxima. CHALLENGE advertises responder capabilities; both derive directional
effective minima against local send settings. Required mode/features are not
weakened. k/r, mux profile and MTU strategy are exact selections; queue sizes,
local fast-retransmit policy and local pacing rates are not wire equality
requirements. The twelve mandatory TLVs occupy 149 bytes including headers,
leaving 223 bytes for bounded optional additions; no handshake fragmentation
is needed. All uint32 byte/age values are checked against local integer and
global allocation bounds before use.

Capability bits (LSB=bit0): 0 fragment_manifest, 1 aggregation,
2 datagram_repair, 3 native_kcp, 4 kcp_packet_pieces, 5 smux_admission_v1,
6 plpmtud, 7 per_carrier_budget, 8 group_migration. Bits 9..63 are currently
unknown; an unknown required bit rejects. A sender may use only selected
capabilities actually implemented by both endpoints. False repair and mux
are exact policy requirements, not permission for the peer to enable them.
Aggregation is a local sender policy supported by the selected layout;
the receiver does not have to aggregate its own outbound traffic.
For repair/mux, a disabled local policy clears its OfferedCaps bit and enabled
sets that bit in RequiredCaps; implementation support alone is not an offer
to enable a disabled policy. Effective mux frame size is the smaller endpoint
MaxMuxFrameBytes; both senders clamp frames to it, and receivers reject larger
frames before payload allocation. The initial-window reserve includes that
exact negotiated maximum frame size.

## Paths, Budgets And Epochs

The bootstrap Carrier keeps its configured PathID from authenticated TLV
0x800c, generation1 and budget epoch1; a different winning Carrier is never
renumbered to1. The initiator keeps a fixed mapping from configured indices
to endpoints, and the listener's static reverse profile uses those same
indices. Both directed bootstrap budgets begin at512. Fixed mode then
publishes its configured known-safe budget as epoch2, capped by local/peer
hard limits, before using larger DATA. PLPMTUD needs size-probe evidence for
such an increase. Other configured paths join only after READY.
All four path-join packets are exactly 512 bytes, use derived directional
MACs, repeat PathID and proposed generation, and use budget epoch 0 while
pending. JOIN/CHALLENGE/CONFIRM/READY bind both fresh path nonces and observed
socket/remote tuple. CHALLENGE fills the listener nonce; the other messages
echo it. Only matching CONFIRM/READY commits the new generation and endpoint;
pending validation never refreshes the active endpoint's TTL or health.

One path validation per PathID is pending, at most eight sends and 5s total,
with 250ms retry interval. Identical retries are idempotent without deadline
extension. Retired generation/nonce pairs cannot re-enter pending state;
store a monotonic generation floor plus the bounded live retry record.
The previous active path may drain while validation is pending; committing
the new generation invalidates old health, PLPMTU and budget epochs in both
directions. New packets use its new generation and epoch 1.

This invalidation removes send/health authority, not admitted group state.
Retain at most the configured old-route count (default2, hard maximum8) for
receive-only grace (default5s, maximum60s). A packet from its previously
validated tuple may finish an already admitted group but cannot admit an
unknown old-generation group, learn an endpoint or refresh health/TTL. After
route grace, late old-generation packets drop; repair can carry that same
old immutable group/context on a current validated generation.
Encoding contexts with admitted pending groups remain pinned through those
groups' original deadlines, even beyond general old-context grace. They count
against the old-context limit, excluding the current context. If pinned
contexts fill the limit, block new context admission or expire eligible
Datagram obligations under their original rules; never evict a live context
while still promising its pending decode. Current route/budget generations
and group encoding epochs therefore have independent retention records.

For NAT rebinding detected by the listener, an authenticated health PING from
a changed source receives PATH_REBIND_HINT echoing that outstanding token.
The initiator accepts a hint only for its live PING, current generation and
Carrier, consumes that token, increments generation and performs validation.
Replayed hints cannot keep incrementing generations. DATA from an unvalidated
tuple does not learn/refresh it. Listener replies always use its original
bound socket and local source-address contract.

PATH_BUDGET_UPDATE Kind=0 static configured budget, Kind=1 probe-confirmed
budget. TargetDirection=0 c2s or 1 s2c. Only the sender responsible for that
direction publishes its budget; the route's PathID/generation selects the
path. The matching ACK echoes the exact typed update. Epoch begins at 1,
increments without wrapping, and conflicting equal-epoch values reject.
During an update its packets use the previous committed route epoch, so
an unknown future route epoch is not needed to process the update itself.

A decrease clamps local send size immediately. Publish/ACK commits the new
epoch; no old-epoch receive grace permits an oversized local send. An increase
in PLPMTUD requires matching probe evidence and budget ACK. MTU_PROBE total
UDP length must equal ProbeBytes, with zero padding after its 24-byte prefix;
it alone may exceed current confirmed budget within both hard caps. Its ACK
echoes the token, exact measured length, direction and proposed epoch, using
the reverse path's current safe packet budget. Small health success is not
large-size evidence. No validated 512-byte path means bounded Session failure,
not sending an unnegotiated sub-512 protocol.

For static/per_carrier, configuration supplies each direction's known-safe
path budget; missing listener reverse profiles fail startup. These budgets
need authenticated publication/ACK but are not reported as probe-confirmed.
Interface MTUs or successful writes never fill missing profile values.

## Error Registry

| Code | Meaning | Public Classification |
|---:|---|---|
| 1 | unsupported version/profile/capability | ErrProtocolUnavailable |
| 2 | mode/FEC/mux/MTU policy mismatch | existing ErrHandshakeIncompatible |
| 3 | invalid normalized parameter | ErrInvalidConfig at startup; protocol error remotely |
| 4 | resource/admission exhausted | ErrResourceLimit |
| 5 | handshake/open/operation deadline | context deadline or existing timeout classification |
| 6 | cancelled/explicit abort | context cancellation or ErrStreamAborted |
| 7 | counter/generation exhaustion | ErrIDExhausted |
| 8 | no usable safe path | existing ErrNoAvailablePaths, with MTU cause when relevant |
| 9 | malformed authenticated state transition | ErrProtocolViolation |

No error contains PSK, key bytes, original payload or unbounded peer text.
Wire errors are bounded codes. Invalid authentication is counted locally and
does not elicit a rejection packet. Normal stream admission refusal uses the
control profile below, not Session CLOSE.

## Raw Reliable Record Profile

KCP without mux carries this byte-stream framing; it is distinct from the
smux profile. A record has BodyLength u32, Type u8, Flags u8=0, zero u16,
ByteOffset u64, then payload. BodyLength excludes its own four bytes and
includes the remaining 12-byte header. Check length before allocation.
DATA(Type1) has 1..16384 payload bytes; FIN(Type2) and FIN_ACK(Type3) have
none; ABORT(Type4) has ErrorCode u16 and zero u16. Other types are invalid.
DATA offsets are contiguous from zero with checked uint64 addition. FIN
offset is the final accepted send-byte count; FIN_ACK echoes that offset.
Duplicate exact FIN/FIN_ACK is idempotent; conflicting offsets are errors.

FIN_ACK means all preceding bytes plus FIN entered owned reliable receive
storage, not application consumption. Reader EOF appears only after those
bytes drain. CloseWrite queues FIN after accepted DATA and preserves reading.
CloseGracefully requires both FIN directions, matching FIN_ACK, local send
obligations drained and local unread bytes consumed, within the caller's
context and a maximum 5s close operation. Otherwise it aborts explicitly.
Underlying closure or truncation in a header/body before valid FIN is an
abort/UnexpectedEOF, never clean EOF. No Datagram drop-newest queue sits in
this pipeline. All framing bytes and buffers count against KCP/Session/Peer
credits; KCP may split these records across its own segments.

## Mux Admission Control Registry

MuxProfile=1 reserves one bidirectional smux control stream, first initiator
StreamID 3. It is hidden from business Accept/Open and charged as one Session
and Peer metadata object outside the advertised business count. Its receive
reserve is 262144+MaxFrameSize bytes until a reviewed backend changes the
initial upstream window. Business credit sums leave that reserve available.
MaxFrameSize defaults to 16384; total control reserve therefore 278528 bytes.

Each control record is Length u16 followed by Type u8, Flags u8=0,
RequestID u64, StreamID u32, then type-specific bytes. Length counts the bytes
after Length and is 14..254, so a complete record is at most 256 bytes.
RequestID is monotonic per opener direction, starts at 1 and never wraps.
Session incarnation and sender direction are inherited from the authenticated
KCP stream; these records cannot cross Sessions. StreamID is reserved by the
local pre-SYN callback and never reused after cancellation or refusal.

| Type | Name | Extra Bytes |
|---:|---|---|
| 1 | OPEN_REQUEST | OpenLifetimeMS u32, RequestedReceiveBytes u32 |
| 2 | OPEN_GRANT | GrantID u64, GrantedReceiveBytes u32 |
| 3 | OPEN_REFUSE | ErrorCode u16, zero u16 |
| 4 | OPEN_READY | GrantID u64 |
| 5 | OPEN_CANCEL | GrantID u64, zero when not yet known |
| 6 | STREAM_ABORT | GrantID u64, ErrorCode u16, zero u16 |
| 7 | OPEN_CANCELLED | GrantID u64, zero when no grant existed |
| 8 | STREAM_DRAINED | GrantID u64, SentBytes u64, ReadBytes u64 |

MaxOpenLifetime=5s, MaxPendingOpens=128, total queued control bytes=32768.
The receiver starts its lease deadline on first OPEN_REQUEST; duplicate
requests do not extend it. The requester retains its own original context
deadline. No synchronized clocks or absolute remote timestamps are trusted.
RequestedReceiveBytes is the opener's already reserved receive capacity for
acceptor-to-opener traffic. GrantedReceiveBytes is the acceptor's separately
reserved receive capacity for opener-to-acceptor traffic. Both must meet the
stock initial-window plus frame allowance; neither grants the other endpoint
credit in the reverse direction. The opener reserves its local lease before
OPEN_REQUEST; the acceptor atomically reserves count, pending-accept and byte
leases before GRANT. Refusal/timeout/cancel releases both endpoint leases
once through their own ledgers; remote acknowledgement never double-frees a
local lease.
READY follows preallocation grant consumption and adapter Accept ownership;
only READY permits successful public Open and business writes.

REFUSE returns the classified error before SYN. CANCEL before SYN releases the
grant; a late GRANT triggers CANCEL without sending SYN. CANCEL after SYN
aborts only that stream, clears its unread buffers and yields an explicit
error, not EOF. OPEN_CANCELLED settles the cancellation race; duplicate
messages repeat their prior terminal result. Unknown/expired/cancelled SYN
is rejected by the backend preallocation hook without stream allocation.
Retain at most 128 pending operations and 256 terminal reply entries for 5s;
when the terminal cache is full, reject new OPEN requests until reclamation
rather than evict a live cancellation record. After record expiry, never
re-admit its old ID: keep an ID floor with a 256-ID bounded out-of-order span.
Requests beyond that span return resource pressure instead of moving the
floor past unresolved operations. This also bounds concurrent Open bookkeeping.

Grant IDs are unique monotonic receiver-direction IDs. All repeated records
must match their stored StreamID, request and credit values; conflicts abort
that pending operation. Ordinary capacity pressure cannot close siblings.
Global controller leases remain charged through pending accepts and unread
tails and are released once after drain or explicit abort. Session teardown
must explicitly dispose owned stream buffers before returning byte credits.
The wrapper emits STREAM_DRAINED only after both smux FIN directions and its
own unread tail drain; counters are application bytes accepted/successfully
read by that wrapper. The peer matches ReadBytes to its SentBytes and vice
versa. Mux CloseGracefully waits for this per-stream receipt within context
and the 5s maximum close operation, never waits for the whole shared KCP
WaitSnd()==0 while unrelated sibling traffic continues. Normal pressure or
one stream's close does not terminate the shared Session.
The archived prototype proves the pre-SYN/preallocation hook placement and
sibling isolation; it does not implement this full cancellation registry.
