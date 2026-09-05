# Prepared Serial Dial Admission

[中文版](v2-prepared-serial-dial.zh-CN.md)

An initiator wrapper and its sockets can exist before a handshake attempt is
installed. `InstallDeferred` therefore cannot own every construction or pending
failure. A prepared serial dial reserves one future Session before construction,
keeps that admission across serial fallback, and hands it to installed disposal
or final pending cleanup. The public runtime must opt in explicitly.

## API

```go
func (e *Engine) PrepareDial(
    now time.Time, policy Policy,
    disposePending func(releaseStorage func()),
) (*PreparedDial, error)

func (e *Engine) BeginPreparedDial(
    now time.Time, prepared *PreparedDial,
    carriers []Carrier, deadline time.Time,
) (DialID, Result, error)

func (e *Engine) AbortPreparedDial(now time.Time, prepared *PreparedDial) error
```

`PreparedDial` is opaque. Copies share one lifecycle and cannot release its
leases directly. All three methods require the existing serialized Engine
caller and nondecreasing clock. `PrepareDial` requires `InstallDeferred` and a
nonnil pending disposer. It validates and copies the policy before publication.
Preparation failure never invokes the disposer, because the wrapper may not
exist yet. A successful preparation emits no packet and has no retry timer.

Call `PrepareDial` before allocating the wrapper, delivery backing or opening
any Carrier. Capture native transport generation and addresses while creating
each authenticated route/binding. Once construction finishes, start with the
complete configured Carrier order and an optional absolute deadline.

Invalid Carrier count/order/binding or an already elapsed deadline leaves the
preparation unconsumed. The caller may correct the request or abort. Valid
adoption consumes it exactly once; identifier, entropy or first-attempt failure
then invokes pending disposal before returning, even without a published DialID.
An adopted handle returns `ErrInvalid` from another start or abort. Use
`CancelDial` or `CloseSession` for the adopted owner.

An unadopted preparation can be aborted repeatedly, including after Engine.Close.
A handle from another Engine is invalid. Engine.Close also aborts all unstarted
preparations, so no prepared owner depends on a later Begin call for retirement.

## Admission And Fallback

Preparation reserves the normal Receive, Initial, handshake-packet and deferred
disposal claims, plus `PreparedDialBytes` (32768 bytes) for its private owner,
policy/handle metadata and maximum 256-Carrier request. The separate
`DeferredDisposalBytes` remains 512 bytes. Neither claim changes Initial indexes
or the maximum of 16 Initial entries. An empty Receive claim is supported when
Initial covers the required receive floor, following the existing policy rules.

With a nonempty Receive claim and N Initial entries, preparation uses N+4
reservations, one pending-handshake slot and one future Session slot. An empty
Receive claim uses N+3 reservations. Byte admission must cover:

```text
Receive.Bytes + sum(Initial.Bytes) + PacketReservationBytes
    + DeferredDisposalBytes + PreparedDialBytes
```

`Snapshot.Prepared` counts unstarted preparations; `Snapshot.Pending` counts
started pending attempts. Their sum is limited by MaxPending, including ordinary
dial and listener admission. PacketBytes includes unstarted packet reservations.
Ledger pending/Session/byte/reservation ceilings also remain enforced.

This API is serial by construction. Before promotion, a failed attempt can reuse
the same still-pending scope, component leases and packet backing for the next
Carrier. Old packets, keys, transcript and attempt references are cleared.
Every attempt gets a fresh SessionID and client nonce, its own retry budget and
original lifetime, constrained by the unchanged caller deadline. A fallback
does not invoke wrapper disposal or require a second Session slot, including
at MaxSessions=1 and exact byte/reservation capacity.

Failure after promotion or partial installation is terminal for the prepared
dial. Promoted scopes and bound or released Initial leases cannot be recycled
as pending admission. Ordinary `BeginDial`, including Concurrent>1 and its
existing fallback behavior, remains on the existing API.

## Disposal Ownership

Pending abort, exhausted fallback, terminal failure and Engine.Close close the
scope and clear/release protocol packet and key state before invoking the pending
disposer. Receive, Initial, both metadata claims and the Session slot remain
owned until `releaseStorage` runs. The callback executes synchronously under the
serialized caller; it must mark/cancel and arrange bounded cleanup without
waiting or reentering Engine. The continuation may run concurrently and is
idempotent across copied invocations.

The adapter must join construction before final storage release. A blocked
Carrier open may return a socket after an earlier cleanup pass inspected the
currently attached carriers. Retain the continuation until construction is
finished and all owned sockets, delivery backing and wrapper state are cleared.
Engine.Close starts this retirement; the adapter joins it outside its owner lock.

Successful installation transfers the same admission to `InstallDeferred` and
disables the pending hook. A failed installer returning a nonnil disposer owns
the full cleanup continuation; a nil disposer means its partial installation is
already cleared, and the pending hook still cleans the preexisting wrapper.
Exactly one path receives the continuation. Old attempt IDs, stale preparation
handles and old Dial cancellation cannot dispose the winning wrapper.

Deterministic tests cover exact-capacity fallback and installation, construction
held across close, admission rollback, shared pending limits, copied and
wrong-Engine handles, synchronous post-adoption failure, promoted installation
failure, concurrent release, component index preservation and metadata bounds.
This prerequisite adds no worker or network measurement.
