# V2 Original Datagram Reassembly

`internal/reassemblyv2` implements original-Datagram ownership after a FEC
group has decoded. It consumes the [group codec](fecv2-group-codec.md),
[credit ledger](v2-credit-ledger.md) and
[terminal receive window](v2-receive-window.md). The Linux fixed/session
Datagram [public runtime](../API.md) connects pending-group decoding to this
receiver and owns its delivery queue. This package remains socket-free;
repair, migration and their full lifecycle acceptance remain pending.

`RequiredInitialBytes(limits)` validates bounds and computes both terminal
bitmap arrays' combined charge without allocation. `New` reserves that charge;
`NewPrepaid(scope, limits, lease)` consumes a dedicated byte-only lease reserved
before handshake completion. Both require an open promoted Session. Prepaid
installation binds the existing lease without another reservation, rejects
invalid inputs without consuming it, and releases it only after Close clears
the bitmaps. Pending originals retain their separate admission-time leases.

The caller authenticates and reconstructs one canonical original group before
calling `AddGroup`. It keeps the decoded group's own charged storage until
admission succeeds. A migration must first verify its whole transaction and
rejoin the original canonical fragment ranges; overlapping replacement pieces
must not be submitted independently.

Admission validates the complete bounded descriptor list, existing original
lengths, exact duplicate bytes, nonoverlapping ranges and fragment counts.
It then reserves every new original's complete payload and fixed-capacity
range table against Session and Peer limits before copying payloads or moving
the terminal window. Any reservation failure rolls back every newly acquired
lease and leaves payloads, IDs, pending state and the caller clock unchanged.
The caller may mark its GroupID complete only after successful admission.
Under pressure it must retain decoded-pending group ownership under that
group's original deadline; dropping those fragments and completing the group
would suppress later repair incorrectly.

Each original owns its total declared byte length plus
`MaxFragments * sizeof(interval)` bytes for the range table. Empty originals
still reserve their table, retain an ID and complete exactly once. The bitmap
storage is separately charged at construction. Metadata outside these tables
has fixed per-object bounds; this accounting is not exact process RSS.
At most MaxDatagrams incomplete originals and MaxFragments accepted ranges
per original exist. Exact duplicates consume no additional range slot.
Admission conservatively counts current pending originals plus all new
originals, before same-group completions free slots. It may therefore report
pressure even if that group's final pending count would fit.

Fragments may arrive in any nonoverlapping order. Whole-original length and
the first admitted timestamp remain immutable. Exact duplicate ranges must
have identical bytes; partial overlap is invalid. Completed and Expired IDs
never recreate reassembly state. Already admitted pending originals remain
eligible until their original timeout even below the advancing ID floor.
Epoch changes do not reset original identity or history.

`Expire` scans only bounded pending state. A final fragment arriving exactly
at the original timeout records Expired and cannot complete the original.
Duplicates do not extend deadlines. Callers schedule expiry explicitly; there
are no timers or goroutines. Caller times must be nondecreasing.

Successful completions transfer immutable payloads and their independent leases
to `Datagram` handles. The range-table charge is conservatively retained with
that handle until release. Copies share release state; the owner finishes all
borrowed reads before `Release`, which clears payloads before returning credit.
Receiver Close clears only its own pending data and bitmaps, preserving already
transferred payloads. The surrounding Session and application queue remain
caller-owned and need their own lifecycle and count bounds.

Validation covers whole-group rollback under byte pressure, conflicting or
overlapping metadata, immutable deadlines, empty and spanning originals,
fragment/pending caps, uint64 exhaustion, Close and copied-handle concurrent
release. Real RS(3+2) decoding exercises all ten three-shard recovery subsets,
reordered groups and duplicate repair-shaped inputs without duplicate original
delivery. These are component integration tests, not socket, repair-timer,
migration-lifecycle or performance acceptance.
