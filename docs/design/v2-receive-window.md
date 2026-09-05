# V2 Terminal Receive Windows

`internal/recvwindow` implements the bounded terminal history required by
[#18](https://github.com/mofelee/mpudp/issues/18) and the
[joint v2 contract](v2-joint-contract.md#7-repair-and-dedup-span-contract).
It is an isolated primitive; v2 runtime admission, Datagram reassembly,
repair and migration integration remain pending.

Each Session receive direction owns separate windows for original DatagramIDs
and encoding GroupIDs. IDs are nonzero monotonic uint64 values in their own
namespace; changing an encoding epoch does not reset either history. `New`
accepts spans 1 through 65536. Two bitmaps retain distinct Completed and Expired
states, using at most 16 KiB per window independently of pending payloads.

The owner serializes all calls with its admission lock, never copies the
Window, and performs receive admission in this order:

1. Authenticate and validate the packet and metadata.
2. Look up already admitted pending work, whose original deadline still
   applies even when its ID is below the advancing floor.
3. Check `State` before allocating for an unknown ID. Terminal and retired IDs
   cannot reopen state.
4. Reserve all initial retained-byte and pending-state obligations, then call
   `Admit` and commit pending ownership under the same lock. Resource rejection
   must not advance the window.
5. After an atomic reassembly handoff or expiry, call `Finish` with Completed
   or Expired and release the corresponding pending ownership.

The window stores terminal history only: Unseen does not prove that an ID has
no pending state. The owner must prove prior admission before `Finish`, since
unadmitted holes and admitted pending work share the same clear bitmap bit.
`Finish` never advances the floor. A late admitted ID below the floor may
finish, but no bit is retained because all future new admissions already
reject that ID. Its terminal result then belongs to the caller's pending
lifecycle; the retired window cannot distinguish Completed from Expired there.

Admission advances the floor only for a higher successfully admitted ID.
Advancing less than a full span clears only crossed bitmap words; a larger
jump clears both fixed bitmaps. Work is bounded by the configured span and
there are no timers or packet goroutines. uint64 exhaustion cannot wrap into
a new incarnation. Close releases both bitmaps and prevents revival; it does
not release caller-owned pending buffers or leases. Nil and zero-value windows
behave as closed windows.

Tests cover distinct terminal results, idempotence, pending completion below
the floor, independent namespaces, non-power-of-two spans, bitmap wrap,
uint64 exhaustion, Close and zero values. A deterministic randomized map
model checks 80000 admissions, terminal states and retained counts. These
checks do not establish runtime duplicate suppression or performance
acceptance, which require the later integrated FEC and repair regressions.
