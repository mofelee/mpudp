package session

import "errors"

var (
	// ErrInvalidConfig identifies invalid Session construction parameters.
	ErrInvalidConfig = errors.New("invalid MPUDP session configuration")
	// ErrClosed identifies a terminal locally or remotely closed Session.
	ErrClosed = errors.New("MPUDP session closed")
	// ErrNotEstablished identifies a data operation before handshake completion.
	ErrNotEstablished = errors.New("MPUDP session is not established")
	// ErrHandshakeFailed identifies exhaustion of every bootstrap path.
	ErrHandshakeFailed = errors.New("MPUDP session handshake attempts exhausted")
	// ErrHandshakeIncompatible identifies changed or mismatched authenticated parameters.
	ErrHandshakeIncompatible = errors.New("MPUDP session handshake incompatible")
	// ErrUnexpectedPacket identifies a packet that is not legal in the current role or state.
	ErrUnexpectedPacket = errors.New("unexpected MPUDP packet for session state")
	// ErrUnknownSession identifies a non-HELLO packet for an unknown Session ID.
	ErrUnknownSession = errors.New("unknown MPUDP session")
	// ErrUnknownPath identifies input that did not arrive through a configured Carrier.
	ErrUnknownPath = errors.New("unknown MPUDP session path")
	// ErrInvalidReplyPath identifies missing or unusable receive routing metadata.
	ErrInvalidReplyPath = errors.New("invalid MPUDP reply path")
	// ErrEndpointLimit identifies deterministic rejection of a new Endpoint at capacity.
	ErrEndpointLimit = errors.New("MPUDP session endpoint limit reached")
	// ErrSessionLimit identifies deterministic rejection of a new Session at capacity.
	ErrSessionLimit = errors.New("MPUDP listener session limit reached")
	// ErrProbeMismatch identifies a PONG that does not match the current path probe.
	ErrProbeMismatch = errors.New("MPUDP keepalive probe mismatch")
	// ErrPacketOverBudget identifies an authenticated packet above the frozen receive budget.
	ErrPacketOverBudget = errors.New("MPUDP packet exceeds negotiated receive budget")
)
