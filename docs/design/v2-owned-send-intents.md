# V2 Owned Send Intents

[中文版](v2-owned-send-intents.zh-CN.md)

`Config.OwnedSends` opts into explicit send ownership. The default remains
synchronous `Config.Emit`; this increment enables no public runtime workers and
makes no throughput claim. The controller owns no goroutines, timers or sockets.
Its owner serializes all calls except an intent's worker-side `Release`.

## Admission And Ownership

Owned mode requires `MaxInFlightSends` in1..32, `MaxPathQueuedPackets` in1..4096,
`MaxPathQueuedBytes` from512 through the ledger ceiling, and positive
`MaxQueueResidence` no greater than100ms. The negotiated DATA packet must fit
the path byte ceiling before business admission.

An adapter calls `TakeSend(now)` only when an idle worker can accept work. It
admits at most one packet, with at most `min(MaxInFlightSends, negotiated
MaxPaths)` live packets and one per logical path, shared by DATA and controls.
A path ceiling greater than one does not increase path concurrency here.
Path bytes cover the full packet backing capacity; FEC packets allocate their
exact size. IP overhead counts toward pacing, not retained packet memory.

Waiting FEC descriptors remain unassigned within the one sealed group. They
have no packet allocation or path slot; a placement preference becomes an
assignment only at `TakeSend`. The group has at most256 shard-state entries.
Waiting controls retain bounded frames. These group-level waiting descriptors
are distinct from admitted path packet obligations.

`SendIntent` carries its token, type, path ID, authenticated route and binding,
captured sender, packet, deadline, and native timing capability. Packet and
sender are borrowed until `Release`; access must stop before release, including
through copied wrappers. Release clears packet bytes/private references and is
idempotent across copies. The controller keeps the private owner independently
of mutable public wrappers, so wrapper reassignment cannot redirect ownership.

After I/O returns or before-send cancellation, release the intent and reliably
return one scalar `SendOutcome`. `CompleteSend(now, outcome)` requires that
release and a matching live token. Release alone never returns the global/path
slot or packet-byte obligation. Unknown, duplicate, stale and invalid outcomes
leave live tokens unchanged. Monotonic tokens detect exhaustion.

`TakeSend` errors return no intent. Successful returns can contain an intent
plus earlier waiting failures in `Result`; adapters must register the intent
before handling that result. Queue admission and dispatch never satisfy a DATA
fence or the receive encoding-context ACK gate.

## Observation And Deadlines

`Invoked` means the sender call returned. Native `AttemptKnown` with nonzero
`StartedAt` records the instant before connection Write; zero plus an error is
a native preflight rejection. Custom invocation retains explicitly unknown
timing and its ordinary `Send` success contract. Before-send cancellation
requires an error and no start timestamp.

The owner-supplied `now` advances the controller clock. Worker times must follow
dispatch and finish no later than `now`. Native attempts extend pacing from
`StartedAt`; unknown custom invocations use `FinishedAt` conservatively. The
quantum includes the entire UDP packet plus28 IPv4 or48 IPv6 bytes. Completion
never reduces existing pacing or changes a replacement route's pacing.

Native bindings are captured at bootstrap construction or authenticated join
setup, before dispatch. Socket generation, address tuple and listener source/OOB
remain fixed. IPv4-mapped addresses normalize to the runtime binding format.
A Carrier rebuild cannot move an old intent to a new socket. Custom senders
retain their own `Send` implementation and unknown timing.

`ExpiresAt` limits waiting before invocation only. Adapters check it before
starting Send; executing I/O uses a separate timeout and may finish later.
FEC waiting deadlines originate at group seal and survive placement changes.
Control-frame queue deadlines remain separate from transaction lifetimes.
Expired work is never dispatched, including at the per-step result bound.

`NextDeadlineWithSendCapacity(false)` suppresses dispatch readiness/pacing when
all Peer workers are busy, while preserving queue expiry, retry preparation,
control lifetime, sealing and receive maintenance. Dispatched-only work waits
for completion without a ready-timer loop. `NextDeadline()` preserves normal
ready-work scheduling; legacy synchronous mode ignores the capacity argument.

## Completion, Credit And Close

One group remains live until every shard is terminal, in any completion order.
Only its last outcome advances the original frontier; partial originals retain
their earlier frontier across groups. The first failure preserves the sticky
error and failed-from boundary. Waiting expiry is terminal without invocation.

Control retries have distinct transaction IDs and frame versions. Join phase
changes and context migration preserve lifetime and attempt totals. In-flight
attempts reserve retry budget. A late actual attempt counts against a surviving
transaction; only the current frame version clears dispatch state, schedules
its retry, or opens the ACK gate. Identical overwritten replies receive new
versions. Preflight rejection and waiting expiry consume no actual-attempt count
but remain bounded by retry backoff and the original lifetime.

The six initial indexes stay unchanged. `InitialOutput` prepays one FEC output.
Owned `InitialAssembly` covers bounded simultaneous packets, each with the
conservative twice-local-budget allowance and intent metadata. `InitialControl`
covers fixed token and shard state. Local offered ceilings size these leases
before negotiation; installation binds them without another reservation.
Accepted originals can progress under full Session or Peer byte pressure.

`BeginClose` stops admission/dispatch without blocking or reclaiming any initial
storage. Undispatched work stays inert until finalization. Restricted completions
work after `Scope.Close`, return only token obligations and never advance fences
or schedule work. `FinalizeClose` requires zero pending tokens, then clears owners
and releases all initial leases. `Close` invokes both stages and leaves live
send obligations charged if finalization must wait. Deferred handshake adapters
retain their storage continuation until controller, worker, carrier and delivery
disposal finish.

See [prepaid send workspace](v2-send-workspace.md) and
[deferred handshake disposal](v2-deferred-disposal.md). Tests cover round trips,
full credit, partial originals, reorder/duplicate/invalid outcomes, copied and
reassigned handles, concurrent release, queue limits/expiry, native/custom pacing,
control migration/overwrites, native capture, rollback and scope-close retirement.

```sh
go test -race ./internal/sessionv2
go test -race ./...
go vet ./...
```
