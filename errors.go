package mpudp

import (
	"errors"

	"github.com/mofelee/mpudp/config"
)

var (
	// ErrInvalidConfig classifies strict decoding and validation failures.
	ErrInvalidConfig = config.ErrInvalidConfig

	// ErrProtocolUnavailable means a valid configured protocol or wire version
	// is not implemented by this runtime. Construction creates no runtime state.
	ErrProtocolUnavailable = errors.New("MPUDP protocol unavailable")

	// ErrMessageTooLarge means a Datagram exceeds the applicable upper limit.
	ErrMessageTooLarge = errors.New("MPUDP message too large")

	// ErrClosed means an operation targeted a closed Peer, Listener, or Session.
	ErrClosed = errors.New("MPUDP object closed")

	// ErrAuthentication means packet authentication failed.
	ErrAuthentication = errors.New("MPUDP authentication failed")

	// ErrHandshakeIncompatible means authenticated peers advertised incompatible
	// protocol, FEC, or transport capabilities.
	ErrHandshakeIncompatible = errors.New("MPUDP handshake incompatible")

	// ErrNotReady means an initiator Session has not completed its handshake or
	// has exhausted every bounded handshake attempt.
	ErrNotReady = errors.New("MPUDP runtime not ready")

	// ErrModeUnavailable means the configuration did not enable the requested
	// initiator or listener role.
	ErrModeUnavailable = errors.New("MPUDP mode unavailable")

	// ErrResourceLimit means an operation would exceed a configured bounded
	// resource such as the maximum number of Sessions.
	ErrResourceLimit = errors.New("MPUDP resource limit reached")

	// ErrNoAvailablePaths means no healthy Carrier or Endpoint was available
	// before a Datagram send began.
	ErrNoAvailablePaths = errors.New("MPUDP has no available send path")

	// ErrPartialSend means at least one, but not all, FEC shard sends failed.
	ErrPartialSend = errors.New("MPUDP Datagram partially sent")

	// ErrAllSendsFailed means every FEC shard send for a Datagram failed.
	ErrAllSendsFailed = errors.New("MPUDP Datagram send failed on every shard")

	// ErrPathMTUExceeded means a UDP packet exceeded a path's known MTU.
	ErrPathMTUExceeded = errors.New("MPUDP path MTU exceeded")
)
