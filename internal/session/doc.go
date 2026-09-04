// Package session implements the authenticated MPUDP Session state machine.
//
// It owns handshake negotiation, learned Endpoint lifetime, keepalive probes,
// and direction-specific FEC codecs. UDP socket ownership remains in package
// transport. Callers drive logical deadlines with Session.Advance; the package
// creates no timer goroutine for each Session or Endpoint.
package session
