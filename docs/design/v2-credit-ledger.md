# V2 Ownership Credit Ledger Foundation

`internal/creditv2` implements the shared ownership accounting required by
[configuration ownership limits](v2-configuration-api.md#ownership-limits) and
[joint contract section 11](v2-joint-contract.md#11-resource-scheduler-and-error-contract).
It is not connected to Peer, handshake, FEC, KCP or smux runtime code. #20 and
#25 remain open. Authentication, deadlines, buffer allocation, window
advertisements, control admission and dependency lifecycle remain callers'
responsibilities. This ledger measures reserved obligations and owned bytes,
not exact process RSS or network performance.

## Atomic Admission

`New(Limits)` creates one Peer ledger. All its operations use the same mutex,
so a reservation checks Session and Peer byte/count ceilings before changing
either scope. A failure returns no new handle or partial charge. Limits have
no defaults. Public configuration still owns its stricter minima; the internal
ledger permits small positive byte ceilings and enforces the 1 GiB upper bound.

`BeginHandshake(Claim)` reserves one future established Session slot, one
independently limited pending-handshake slot and the complete selected initial
claim before CHALLENGE. A claim contains owned/reserved bytes and optionally
one business-stream count and one pending-accept count. The caller must include
all selected initial receive obligations before replying. `Session.Promote()`
only converts pending to established after matching FINISH or READY; it never
competes again for capacity. Repeated promotion is harmless while open.

`BeginSession(Claim)` directly creates an established scope for callers that
already have the necessary protocol admission. Empty initial claims return
nil leases but still reserve Session slots. A pending scope may reserve
additional claims before advertising its full selected contract; any failure
requires caller cleanup before refusal. Neither entry point authenticates or
creates protocol state itself.

`Session.Reserve(Claim)` acquires later obligations. A claim can have zero bytes
when it holds a stream or pending-accept count. An entirely empty standalone
claim is rejected. Reserve the control stream's initial window and accepted
frame bound in a separate byte lease with no business count; its held credits
then remain unavailable to business streams. Stream/window/path/repair-specific
sub-limits are still caller responsibilities, not an arbitrary ledger hierarchy.

## Ownership And Teardown

The returned lease owns the accounting obligation, not the actual buffer.
Moving that lease between components in the same Session requires no new
charge. `Lease.Transfer(destination)` moves the same obligation between open
Session destinations of one Peer, checking the destination before debiting
the source. Peer byte/count/reservation totals do not change. A closed source
may transfer surviving ownership to an open destination. Cross-Peer transfer
is rejected. Simultaneously retained copies require separate reservations.

`Session.BindBytes(lease, required)` dedicates a live byte-only lease to one
storage owner after promotion. It atomically verifies the same Session, an
adequate byte charge, and no previous binding, without changing usage. Failed
binding leaves the lease unchanged. Copies cannot bind the same charge twice,
and bound leases cannot transfer to another Session. This permits constructors
to consume dedicated handshake reservations even when all capacity is already
reserved. The returned handle shares Release state with the original: the
handshake owner must dispose of the component before releasing its own handle.

`Lease.MarkAccepted()` returns only the pending-accept slot when an application
takes ownership, keeping bytes and business count charged. It is idempotent
after success; pending-handshake and closed scopes cannot accept. It neither
consumes receive bytes nor proves that buffers were reclaimed.

`Lease.Release()` is idempotent, including through copied handles and after
Close. Callers must first clear the owned storage or end the reserved window
obligation. The Peer, Session and Lease handles refer to shared private state,
so copying a handle cannot create an independent release flag or charge.

`Session.Close()` stops reservations, promotion and acceptance. Its reserved
Session slot and all live byte/count claims remain charged until the last
lease is released or transferred. This includes count-only leases and a
pending scope closed before promotion. An accepted count-only lease retains
its metadata slot until explicit release, even though its claim is now empty.

`Peer.Close()` closes every scope and retires empty scopes immediately in
bounded O(MaxSessions) work. It preserves live claims and returns without
waiting for caller buffers. All future admissions fail; leases can still be
released. Ordinary protocol teardown must cancel its own workers/waiters,
dispose of buffers and release leases. The ledger has no timers, workers,
wait queue, protocol IDs or retained tombstones.

## Metadata Bounds And Snapshots

`MaxSessions` bounds all provisional, established and closing-owned scope
slots together. A map contains only those live scopes. `MaxReservations`
explicitly bounds all live lease records, including count-only leases; it is
an internal construction parameter, not a new public configuration field.
Its hard maximum is 1048576. No memory proportional to configured maxima is
preallocated. Released leases are not retained by the ledger; a released
handle also drops its reference to the owning Session.

`Peer.Snapshot()` distinguishes reserved Session slots, pending handshakes and
established scopes, including closed scopes whose ownership is still live.
Peer and Session usage report bytes, reservations, business streams and pending
accepts. Snapshots are coherent individually under the same lock; separately
obtained snapshots need not describe the same instant. These counters exclude
allocator overhead and caller-retained released handles.

## Verification

Focused tests cover full-capacity promotion, all-or-nothing multi-resource
admission, independent pending counts, destination transfer refusal, separately
charged copies, control credit protection, zero-byte metadata exhaustion,
copied-handle idempotence, maximum byte arithmetic and concurrent reservation,
transfer, acceptance, promotion, release and Close. Accounting checks sum all
live Session charges and compare them with Peer totals and ceilings.

```sh
go test ./internal/creditv2
go test -race ./internal/creditv2
```

No runtime integration, authenticated handshake exchange, smux backend hook,
graceful drain or network acceptance is claimed by this increment.
