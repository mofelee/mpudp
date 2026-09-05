package transport

import "net"

// CaptureSendPath snapshots a native path's socket generation and addresses.
// Capture when creating the route/binding snapshot, and match the returned
// generation and addresses to that binding before dispatch. Capturing a raw
// Carrier later could bind a newer socket to an older authenticated route.
//
// Native snapshots return native=true. They remain bound to that generation,
// but do not keep its socket alive: rebuild or close may reject a later send,
// and retirement may interrupt a send already admitted on the old socket.
// Already captured native paths retain their original binding, even if stale.
// Native addresses must be nonnil *net.UDPAddr or *net.IPAddr values; their
// backing is copied. Other address types return ErrDestinationUnsupported.
//
// Custom ReplyPaths are returned unchanged with native=false, preserving their
// Send implementation without claiming native generation or timestamp support.
// Capture does not invoke a custom path's methods or change the ReplyPath API.
func CaptureSendPath(path ReplyPath) (captured ReplyPath, native bool, err error) {
	if path == nil {
		return nil, false, invalidArgument("nil reply path")
	}
	switch path := path.(type) {
	case *Carrier:
		if path == nil {
			return nil, false, invalidArgument("nil carrier")
		}
		path.mu.RLock()
		defer path.mu.RUnlock()
		if path.closed {
			return nil, false, ErrClosed
		}
		generation := path.current
		if generation == nil || !generation.alive.Load() {
			return nil, false, ErrPathUnavailable
		}
		captured = carrierReplyPath{
			carrier:    path,
			generation: generation.number,
			local:      generation.conn.LocalAddr(),
			remote:     generation.conn.RemoteAddr(),
		}
	case carrierReplyPath, listenerReplyPath:
		captured = path
	default:
		return path, false, nil
	}
	// Clone only supported backing, including paths previously captured by reads.
	switch path := captured.(type) {
	case carrierReplyPath:
		path.local, path.remote, err = captureSendAddresses(path.local, path.remote)
		captured = path
	case listenerReplyPath:
		path.local, path.remote, err = captureSendAddresses(path.local, path.remote)
		captured = path
	}
	if err != nil {
		return nil, false, err
	}
	return captured, true, nil
}

func captureSendAddresses(local, remote net.Addr) (net.Addr, net.Addr, error) {
	for _, address := range [...]net.Addr{local, remote} {
		switch address := address.(type) {
		case *net.UDPAddr:
			if address == nil {
				return nil, nil, ErrDestinationUnsupported
			}
		case *net.IPAddr:
			if address == nil {
				return nil, nil, ErrDestinationUnsupported
			}
		default:
			return nil, nil, ErrDestinationUnsupported
		}
	}
	return cloneAddr(local), cloneAddr(remote), nil
}
