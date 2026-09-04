// Package mpudp exposes the public Datagram-oriented lifecycle API for an
// MPUDP peer.
//
// NewPeer validates configuration, binds an enabled listener, and starts one
// bounded runtime dispatcher. NewSession opens the configured Carrier sockets
// and begins an asynchronous authenticated handshake.
//
// A Session preserves message boundaries: one successful WritePacket is one
// Datagram, and one successful ReadPacket returns one complete Datagram.
// Session methods, Listener methods, and Peer lifecycle methods are safe for
// concurrent use. Delivery ordering between concurrent calls is unspecified;
// Close releases owned sockets and wakes blocked reads and accepts.
package mpudp
