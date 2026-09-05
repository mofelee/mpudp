// Package creditv2 reserves bounded v2 ownership before protocol admission.
// It is not connected to the runtime and does not authenticate, allocate
// payloads, advertise windows, wait for credits, or clear caller-owned buffers.
// All reservations share one Peer lock, atomically charging Peer and Session.
// Callers release leases only after the corresponding storage or obligation
// is gone. Moving a lease transfers ownership; copying storage needs another
// reservation. Count-bounded ledger metadata is not exact process RSS.
package creditv2
