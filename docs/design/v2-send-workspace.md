# V2 Prepaid Send Workspace

[中文版](v2-send-workspace.zh-CN.md)

For the opt-in controller adapter, see [owned send intents](v2-owned-send-intents.md).

The synchronous fixed-budget Datagram controller reserves enough credit for
one sealed FEC group and one packet assembly before handshake promotion.
Without this floor, admitted original payloads could consume every remaining
Session or Peer byte: encoding needed a later output reservation, which could
never succeed while those same originals remained queued. Retrying did not
free the credit needed to consume them.

## Initial Ownership

The existing initial claim indexes remain stable. `InitialOutput` and
`InitialAssembly` are appended, bringing the count to six. Output credit covers
all data/parity backing, the shard-slice array, and the workspace/output owner
metadata. Assembly retains the conservative existing allowance of twice the
complete UDP packet budget. Both are separate from controller/control storage,
receive scratch, original payloads and codecs.

Sizing uses the local fixed budget before negotiation. Installation binds each
dedicated byte-only lease once, without reserving again, and uses an effective
negotiated packet size no larger than that budget. A partially constructed
controller disposes its successfully bound owners; untouched caller leases
remain available for handshake rollback. Insufficient initial credit rejects
admission before business payloads are accepted.

The allowances stay charged between sends. A complete original can consume the
remaining byte credit without blocking its own output or packet assembly.
Partially consumed originals retain their entire copied backing until their
last fragment is sealed. This guarantees local memory progress while paths and
the serialized driver remain usable; it does not guarantee remote delivery or
progress through arbitrary network failures.

## Aggregation API

`RequiredOutputWorkspaceBytes(shards, shardBytes)` computes the output ceiling
without constructing a codec. `Queue.NewPrepaidOutputWorkspace(lease)` binds a
dedicated lease to that queue's frozen dimensions. Failure leaves the supplied
lease unchanged. `Queue.SealWithWorkspace(now, force, workspace)` permits one
live output from the workspace and does not acquire another ledger lease.
A busy workspace fails without consuming original IDs, payloads or cursors.
Codec failures return the workspace slot without committing queue state.

The existing `Queue.Seal` still reserves each output independently and permits
multiple live outputs. The workspace is opt-in; it changes no standalone queue
admission or timing semantics. Its immutable output owns separate shard backing,
and every output gets a fresh shared release state. Copied handles from an older
released output never become live when the workspace is reused.

`Output.Release` clears shard bytes and references before returning its slot.
The standing reservation remains charged for reuse. `OutputWorkspace.Close`
prevents new uses and waits logically for any live output to be released before
returning its lease; it does not block or invalidate that output. The caller
closes both Queue and workspace. Scope/Peer closure alone never revokes their
live leases. Controller.Close disposes its current group, workspace and
assembly allowance synchronously before the handshake owner releases copied
initial handles. This change introduces no send workers or async disposal.

## Verification

Tests cover full Session and Peer byte saturation, successive groups of a
partially consumed original, standing assembly credit after the original copy
is freed, workspace reuse and old copied handles, ordinary multiple outputs,
constructor rollback, codec failure, and Close with a live output. Existing
receive scratch pressure tests also retain outbound queue pressure while a
prepaid send group is in flight.

```sh
go test ./internal/aggregationv2 ./internal/sessionv2
go test -race ./...
go vet ./...
```

The accounting describes owned bytes and reserved obligations, not allocator
overhead, process RSS, a throughput result or a new public queue limit.
