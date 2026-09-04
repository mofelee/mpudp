// Package mpudp exposes the public Datagram-oriented lifecycle API for an
// MPUDP peer.
//
// This initial package skeleton is deliberately side-effect free: NewPeer,
// NewSession, and Listener validate and represent lifecycle state but do not
// open sockets, start goroutines or timers, encode wire packets, perform FEC,
// or begin a handshake. Later implementation loops replace ErrNotReady paths
// without changing the Datagram API.
//
// A Session preserves message boundaries: one successful WritePacket is one
// Datagram, and one successful ReadPacket returns one complete Datagram.
// Session methods, Listener methods, and Peer lifecycle methods are safe for
// concurrent use. Delivery ordering between concurrent calls is unspecified.
package mpudp
