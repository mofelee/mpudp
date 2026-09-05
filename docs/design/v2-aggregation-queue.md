# V2 Aggregation Queue Foundation

`internal/aggregationv2` combines the [prefix packer](fecv2-group-codec.md) and
[ownership ledger](v2-credit-ledger.md) into a bounded queue under one frozen
encoding epoch. It implements the queue portion of the
[v2 admission contract](v2-joint-contract.md#public-write-semantics), with no
runtime activation, sockets, timers, workers or public Flush/graceful-close
fences. #20 and #21 remain open. The owner drives time and sealing explicitly.

## Admission And Bounds

`New(session, limits, epoch)` requires an open established credit scope and
valid immutable FEC parameters. Before allocating its fixed ring, it reserves
the checked product of slot count and `unsafe.Sizeof(entry)`. This accounts
for the typed ring backing, including slots used by empty Datagrams; it is
not an allocator-overhead or exact-RSS estimate. Limits are ceilings, so a
large configured ring can fail against global byte credits at construction.

The charged storage is ring backing, copied payloads, encoded data/parity
backing and the shard-slice array. Fixed codec state and small queue/output
wrapper metadata are not measured byte for byte by the ledger. Returned
queue/output instances are bounded by their successful reservations and
validated codec parameters; the future runtime must also enforce its tighter
per-Session/context limits. This is ownership accounting, not exact process
RSS or an assertion that every allocation is byte-charged.

`Admit(payload, now)` validates the complete original, queue count/retained-byte
limits, fragment bound and ID availability, then reserves and copies all
payload bytes before committing a DatagramID. Failure retains no new copy,
charge or ID. Calls serialize at commit; caller slices are reusable afterward.
Empty Datagrams consume one ring slot and one ID, with pre-reserved metadata.
They remain distinguishable from an empty queue.

Queue bytes and payload leases count the entire retained backing allocation
until the original leaves the queue. Consuming a prefix changes only its
cursor, not its retained-byte charge. `MaxFragmentsPerDatagram` first checks
the fresh-group minimum; planning then declines a short current-group tail
when using it would exceed that original's remaining fragment reservation.
For a 96-byte logical group, a preceding 20-byte original leaves 32 payload
bytes after two descriptors: a following 104-byte original fits a two-fragment
budget across that tail and a fresh group, while 105 bytes must start fresh.

## Sealing And Time

`Ready(now)` reports logical capacity, descriptor count or oldest-admission
deadline readiness. `Seal(now, force)` emits one group if ready, or explicitly
seals an otherwise sparse tail when forced. Empty/not-ready returns nil output
without error. There is no hidden timer or wait queue.

The oldest remaining original retains its admission timestamp across partial
seals. Later arrivals do not extend it. Cancelling the oldest original reveals
the next original's own original deadline. Successful clock-bearing calls
establish a monotonic caller-clock floor; regressions return
`ErrClockRegression` before mutation. Failed resource operations do not update
the clock floor. The caller supplies time; no wall clock is read internally.

Before `fecv2.EncodePrefix`, sealing reserves all data/parity backing bytes
and the shard-slice array. Original copies remain charged during encoding.
Only successful encoding with the planned cursor commits queue consumption
and the next GroupID. Resource or codec failure retains original payloads,
cursors and IDs. The final valid uint64 ID is usable; exhaustion never wraps,
and exhausted GroupIDs stop further admissions without consuming leftovers.

The output's epoch and complete padded shard size never change. Epoch updates,
acknowledgement, migration, packet queues and repair ownership are not part of
this queue. A future runtime must reserve any additional copies separately.

## Cancellation And Ownership

`Cancel(id)` removes only an original's still-queued remainder and compacts
the bounded ring, so cancelled holes cannot accumulate. It does not revoke
already-returned groups, reuse IDs or undo earlier socket work. Cancellation
of a public Flush waiter must not call this method: admitted originals and
waiter lifetime are separate responsibilities.

`Output.View()` borrows immutable shard views. Output handles share one
private release state, so copies cannot double-release the ledger lease.
The caller must finish using every borrowed view before `Output.Release()`
clears its references and releases credit. The queue never retains outputs.

`Queue.Close()` stops admission, clears queued payload/ring references and
releases their leases. Returned outputs remain independently owned, including
after Session/Peer ledger Close. A Session owner must close the queue as well
as its ledger scope; ledger Close preserves owned buffers and does not clean
them up. Neither queue Close nor output release claims socket completion,
remote delivery, original reassembly, repair completion or graceful drain.

## Verification

Deterministic tests cover low-rate and empty tails, full logical/descriptor
capacity, partial cursors and fixed deadlines, the exact fresh-group fragment
boundary, input-copy ownership, separately charged encoded output, failed
whole admissions, ring wrap/cancellation, clock regression, final uint64 IDs,
Close with outstanding output and copied-output release. Concurrent admission
and Close are tested under the race detector. Run:

```sh
go test ./internal/aggregationv2
go test -race ./internal/aggregationv2
```

This foundation makes no throughput, latency, socket-delivery or public
Flush/CloseGracefully claim. Future receive reassembly must reserve its own
storage and retain original deadlines rather than borrowing sender credits.
