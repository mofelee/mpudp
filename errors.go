package mpudp

import (
	"errors"

	"github.com/mofelee/mpudp/config"
)

var (
	// ErrInvalidConfig classifies strict decoding and validation failures.
	ErrInvalidConfig = config.ErrInvalidConfig

	// ErrMessageTooLarge means a Datagram exceeds the applicable upper limit.
	ErrMessageTooLarge = errors.New("MPUDP message too large")

	// ErrClosed means an operation targeted a closed Peer, Listener, or Session.
	ErrClosed = errors.New("MPUDP object closed")

	// ErrAuthentication means packet authentication failed.
	ErrAuthentication = errors.New("MPUDP authentication failed")

	// ErrHandshakeIncompatible means authenticated peers advertised incompatible
	// protocol, FEC, or transport capabilities.
	ErrHandshakeIncompatible = errors.New("MPUDP handshake incompatible")

	// ErrNotReady means the requested data-plane operation has no running
	// transport yet. The issue #2 skeleton returns this without network activity.
	ErrNotReady = errors.New("MPUDP runtime not ready")

	// ErrModeUnavailable means the configuration did not enable the requested
	// initiator or listener role.
	ErrModeUnavailable = errors.New("MPUDP mode unavailable")

	// ErrResourceLimit means an operation would exceed a configured bounded
	// resource such as the maximum number of Sessions.
	ErrResourceLimit = errors.New("MPUDP resource limit reached")
)
