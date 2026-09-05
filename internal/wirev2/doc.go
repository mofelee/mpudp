// Package wirev2 implements the proposed v2 envelope and bootstrap wire
// primitives. It is not connected to a Peer or a production receive loop.
//
// ParseEnvelope performs only bounded structural checks. Its header is
// untrusted and may be used only for read-only key/session lookup before
// Authenticate succeeds. Envelope and AuthenticatedEnvelope borrow their
// input: callers must keep the complete packet immutable and alive until all
// views have been discarded. Copy authenticated bytes before retaining them
// beyond a receive buffer's lifetime. DecodeHandshake returns owned TLV values.
//
// Handshake decoding checks canonical wire shape and registered TLV lengths,
// not capability selection, normalized settings, pending-record lifetimes,
// source tuples, admission credits, or replay state. Those checks must complete
// before a future runtime mutates state. This package does not enable v2.
package wirev2
