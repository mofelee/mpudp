# MPUDP v0.1 Wire Protocol

This document is the byte-level interoperability contract for MPUDP v0.1.
Every MPUDP message occupies one complete UDP payload. Multi-byte integers are
unsigned and use network byte order (big endian).

## Packet envelope

Every packet has a 24-byte prefix, one type-specific body, and a full 32-byte
HMAC-SHA-256 tag at the end.

| Offset | Length | Field | v0.1 value or range |
|---:|---:|---|---|
| 0 | 4 | Magic | ASCII `MPUD`, hex `4d 50 55 44` |
| 4 | 1 | Version | `0x01` |
| 5 | 1 | Packet Type | `1..6` as assigned below |
| 6 | 2 | Body Length | Type-specific body bytes; excludes prefix and tag |
| 8 | 16 | Session ID | Any nonzero 16-byte value |
| 24 | B | Body | Exactly `Body Length` bytes |
| 24+B | 32 | Authentication Tag | Full HMAC-SHA-256 output |

The exact UDP payload length is `56 + B`. A decoder MUST reject both truncated
packets and trailing bytes. The largest legal body is 65,451 bytes because the
entire UDP payload is capped at 65,507 bytes.

Packet type assignments are fixed for version 1:

| Value | Name |
|---:|---|
| 1 | `HELLO` |
| 2 | `HELLO_ACK` |
| 3 | `DATA_SHARD` |
| 4 | `PING` |
| 5 | `PONG` |
| 6 | `CLOSE` |

Type 0 and types 7 through 255 are invalid. Version 1 has no flags, reserved
fields, or extension body. An incompatible layout requires a new protocol
version rather than an ambiguous version-1 extension.

## Authentication

The authentication tag is:

```text
HMAC-SHA-256(PSK, packet[0 : 24+BodyLength])
```

The authenticated bytes therefore include Magic, Version, Packet Type, Body
Length, Session ID, and the complete type-specific body. The 32-byte tag itself
is the only excluded field. Implementations MUST compare tags in constant time.

The HMAC key is the exact UTF-8 byte sequence of the configured PSK scalar. It
is not trimmed, normalized, or passed through a protocol-level KDF. An empty
key is invalid. Applications should inject a high-entropy PSK through an
appropriately protected configuration mechanism; the parser and deployment
boundaries are specified in [PSK management](CONFIGURATION.md#psk-管理).

HMAC supplies authentication and integrity only. MPUDP v0.1 does **not**
encrypt payloads and provides no confidentiality.

## HELLO and HELLO_ACK

Both handshake packet types use the same four-byte body and have a total UDP
payload length of 60 bytes.

| Offset | Length | Field | Range |
|---:|---:|---|---|
| 24 | 1 | Data Shards (`k`) | `1..255` |
| 25 | 1 | Parity Shards (`r`) | `1..255` |
| 26 | 2 | Max UDP Payload | `72..65507` |
| 28 | 32 | Authentication Tag | Full HMAC-SHA-256 |

The sum `k+r` is computed in a wider integer and MUST be in `2..256`. A Session
also rejects parameters unsupported by its selected Reed-Solomon codec.

`Max UDP Payload` is the sender's local capability for one complete MPUDP UDP
payload, including all MPUDP metadata and the authentication tag. It is not an
IP MTU or shard-data size. HELLO_ACK advertises the responder's own capability;
it does not echo or replace the initiator's value.

After an authenticated handshake, each direction uses:

```text
negotiated_max_udp_payload = min(local_capability, peer_capability)
```

The packet budget is phase-specific and deterministic:

- HELLO uses the sender's local capability;
- HELLO_ACK uses the negotiated minimum, even though its body advertises the
  responder's local capability;
- established PING, PONG, DATA_SHARD, and CLOSE packets use the frozen
  negotiated budget in their direction.

FEC parameters must match exactly. Once a Session is established, a duplicate
HELLO or HELLO_ACK carrying different FEC or capability values is rejected; it
does not renegotiate a live Session.

## DATA_SHARD

The DATA_SHARD body contains 15 fixed metadata bytes followed by `L` shard
bytes. Its total UDP payload length is `71 + L`, so the fixed DATA_SHARD wire
overhead is exactly 71 bytes.

| Offset | Length | Field | Range |
|---:|---:|---|---|
| 24 | 8 | Packet ID | `0..2^64-1` |
| 32 | 1 | Data Shards (`k`) | `1..255` |
| 33 | 1 | Parity Shards (`r`) | `1..255` |
| 34 | 1 | Shard Index | `0..k+r-1` |
| 35 | 4 | Original Datagram Length | `0..2^32-1`, subject to Session limits |
| 39 | L | Shard Payload | Canonical equal shard length |
| 39+L | 32 | Authentication Tag | Full HMAC-SHA-256 |

As in the handshake, `k+r` MUST NOT exceed 256. Every shard in a block repeats
the same `k`, `r`, original length, and shard length. A decoder groups shards by
Session ID and Packet ID and rejects inconsistent metadata.

For a nonempty original Datagram, the canonical shard length is:

```text
L = ceil(OriginalDatagramLength / k)
  = 1 + (OriginalDatagramLength - 1) / k
```

An empty Datagram uses `OriginalDatagramLength = 0` and exactly one zero byte
in every shard. This convention lets the Reed-Solomon layer represent an empty
Datagram without zero-length codec shards and makes the minimum payload budget
usable by the data plane.

Packet IDs are independently monotonic in the two sending directions. A sender
may use the complete uint64 range, but after sending Packet ID `2^64-1` it MUST
report exhaustion and MUST NOT wrap or reuse an ID.

DATA_SHARD has no ACK, NACK, or retransmission packet in v0.1.

## PING and PONG

PING and PONG have identical 16-byte bodies and total UDP payload lengths of 72
bytes.

| Offset | Length | Field | Rule |
|---:|---:|---|---|
| 24 | 8 | Token | Nonzero uint64 |
| 32 | 8 | Sender Timestamp | Opaque uint64 |
| 40 | 32 | Authentication Tag | Full HMAC-SHA-256 |

PONG echoes Token and Sender Timestamp byte-for-byte. The receiver does not
interpret the peer's timestamp. The sender accepts a PONG only when the Session,
path, Token, and Timestamp match an outstanding PING, and computes RTT from its
locally retained monotonic send time. This avoids trusting or synchronizing the
peer's clock.

## CLOSE

CLOSE has an empty body (`Body Length = 0`) and a total UDP payload length of 56
bytes. It is an authenticated, best-effort indication that the sender abandoned
the Session. It has no reason code, sequence, acknowledgement, or retry. A CLOSE
for an unknown Session is silently discarded.

## UDP payload budget

Protocol constants are:

```text
common prefix             = 24 bytes
authentication tag        = 32 bytes
DATA_SHARD metadata       = 15 bytes
DATA_SHARD fixed overhead = 71 bytes
HELLO / HELLO_ACK         = 60 bytes
PING / PONG               = 72 bytes
CLOSE                     = 56 bytes
minimum max_udp_payload   = max(71 + 1 shard byte, 72) = 72 bytes
maximum max_udp_payload   = 65507 bytes
```

For a negotiated budget:

```text
shard_capacity = negotiated_max_udp_payload - 71
fec_datagram_limit = k * shard_capacity
effective_datagram_limit = min(fec_datagram_limit,
                               configured_resource_limit)
```

All calculations use checked wide integers before conversion. With the default
budget 1200, shard capacity is 1129 bytes and RS(5,3) carries at most 3387
original Datagram bytes. Peers advertising 1200 and 1000 negotiate 1000, giving
a capacity of 929 bytes and an RS(5,3) limit of 2787 bytes.

Every control packet and DATA_SHARD must fit the applicable budget. A sender
checks an upper Datagram before allocating its FEC block, consuming a Packet ID,
or sending any shard. MPUDP does not split a shard into more protocol packets and
does not rely on IP fragmentation.

## Required receive order

An implementation processes an incoming UDP payload in this order:

1. Reject socket truncation and payloads outside the local hard receive limit.
2. Validate the fixed envelope, exact total length, known version/type, and a
   nonzero Session ID without copying the body.
3. Verify the full HMAC-SHA-256 tag.
4. Only after successful authentication, parse and validate the type body,
   capability, FEC counts/index, and canonical shard length.
5. Validate Session existence/state, frozen handshake parameters, and the
   negotiated receive budget.
6. Only then create or refresh Endpoint, timer, Session, or decode state, copy
   shard bytes into bounded storage, or send a response.

Only an authenticated, valid HELLO may create a Session. Other packet types for
an unknown Session are discarded. Invalid packets generate no network response.
No parsing or authentication error may contain the PSK, complete tag, complete
wire packet, or user payload.

The Go decoder returns a DATA_SHARD payload slice that aliases its input UDP
buffer. The runtime must complete authentication and validation first, then
copy the shard into bounded decode storage if it needs to outlive that buffer.

## Golden vectors

The package tests contain complete fixed bytes produced independently with the
public test-only key `mpudp-v0.1-test-key` and Session ID
`000102030405060708090a0b0c0d0e0f`. To keep failure output free of complete
tags and payloads, tests report only these SHA-256 fingerprints:

| Packet | Body values | Complete packet SHA-256 |
|---|---|---|
| HELLO | `k=3, r=2, max=1200` | `ab20eed995e27a638eb78780bd9893e4f189b597aeb9a9d2832474469b50168e` |
| HELLO_ACK | `k=3, r=2, max=1000` | `e9ec593499bca62f13738f99524b8cfc3b0c60ba0fa937adc0395a79c6ddce5c` |
| DATA_SHARD | `packet=0102030405060708, index=4, original=10, data=deadbeef` | `b1f408fac4ab8871eeda6e62e3de79a892663be58a39e2a6c7e7184988a4fcaf` |
| PING | `token=1122334455667788, timestamp=0102030405060708` | `13fb47fdd97023ebd50b910c19b92c08716a6ab1526535f7340dbb20434d583d` |
| PONG | Same echoed values | `f1d6239acf1df26bed55a74c54116c628516bb3393f8d0cfafde5f203e71bdbd` |
| CLOSE | Empty body | `1f6d406c87b59d56c4104c6f4074fccffe975e9b234f9f86cc2111dc52917357` |

These values are protocol fixtures, not production credentials or traffic.

PLPMTUD/adaptive Session budgets ([#13](https://github.com/mofelee/mpudp/issues/13))
and per-Carrier budgets/unequal shard sizing
([#14](https://github.com/mofelee/mpudp/issues/14)) are post-v0.1 protocol work.
Neither may be introduced as a silent change to version 1; any incompatible
field, packet, or sizing semantics require an explicitly reviewed versioned
extension.
