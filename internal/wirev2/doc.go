// Package wirev2 implements the proposed v2 envelope, bootstrap, encoding
// context and FEC bundle wire primitives. The public fixed Datagram runtime
// uses these codecs; this package owns no Peer state or receive loop.
//
// ParseEnvelope performs only bounded structural checks. Its header is
// untrusted and may be used only for read-only key/session lookup before
// Authenticate succeeds. Envelope and AuthenticatedEnvelope borrow their
// input: callers must keep the complete packet immutable and alive until all
// views have been discarded. Copy authenticated bytes before retaining them
// beyond a receive buffer's lifetime. DecodeHandshake returns owned TLV values;
// DecodeFECBundle validates the full bundle before copying owned shard payloads.
//
// Handshake decoding checks canonical wire shape and registered TLV lengths,
// not capability selection, normalized settings, pending-record lifetimes,
// source tuples, admission credits, or replay state. Those checks must complete
// before the caller mutates state. Codec support alone does not enable a mode.
package wirev2
