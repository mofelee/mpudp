# Transport Destination Capture

`internal/transport` exposes independent send and receive payload limits for
the directional v2 contract. `CarrierOptions.MaxPayload` and
`ListenerOptions.MaxPayload` remain the send limits. `MaxReceivePayload` is
the receive limit; zero inherits the effective send limit, including its
65,507-byte default. Each explicit limit must be within 1..65,507 bytes.
Receive buffers retain one extra byte to detect and count oversize drops.
The receive limit governs receive statistics and rejection; it does not
constrain sends. Existing callers retain their previous behavior.

`ListenerOptions.RequireDestination` requires native Linux `*net.UDPConn`
support. It enables and verifies `IP_PKTINFO` for IPv4 and
`IPV6_RECVPKTINFO` for IPv6, enabling both on a dual-stack socket. The
bounded ancillary receive buffer accepts both families' metadata. Missing,
malformed, duplicate or truncated destination metadata prevents delivery
to the packet handler. Payload truncation also prevents delivery.

For this mode, `ReceivedPacket.LocalAddr` and `Reply.LocalAddr()` identify
the actual received destination and the socket's bound port, including
when the listener binds a wildcard address. IPv6 link-local destinations
retain their interface zone. Reply paths own the captured source address
and interface metadata and send through the original socket. Later reads,
packet address mutation, reply address getter mutation and
`WithReplyStatistics` do not change the route. Listener close invalidates
these replies using the existing completion barrier.

`Listener.LocalAddr()` continues to describe the bound socket. Unsupported
platforms and injected or wrapped connections return
`ErrDestinationUnsupported` before the constructor takes ownership or
starts a read goroutine. `OpenListener` closes a socket it allocated if
construction fails. The default listener mode retains its native
`ReadFromUDPAddrPort` path and bound-address metadata.

Linux socket tests exercise wildcard IPv4 destinations `127.0.0.1` and
`127.0.0.2` from one client socket, dual-stack IPv4 deliveries, IPv6 `::1`,
retained source-specific replies, oversize receive rejection and concurrent
close. Parser tests cover malformed/truncated control messages and address
validation. This transport capability does not itself activate v2 or
implement handshake admission; the v2 runtime must explicitly require it
when binding a handshake to the receiving local destination.
