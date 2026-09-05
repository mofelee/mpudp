# V2 Handshake State Foundation

`internal/handshakev2` implements bounded HELLO, CHALLENGE, FINISH and READY
state over `wirev2`, `negotiationv2` and `creditv2`. The Linux fixed/session
Datagram [public runtime](../API.md) supplies its transport and installer
callbacks; this package itself owns no sockets or workers. KCP and smux are
not activated. Issue #20 remains open for the remaining protocol and network
acceptance matrix; v1 behavior is preserved.

## Ownership And Admission

One owner serializes calls and supplies nondecreasing explicit times. The
engine has no locks, workers, timers or outbound queue. `Emit` borrows its
packet only until return; a queued transport must reserve and copy its own
storage. Callback errors count as sends. Callbacks cannot reenter operations
on the same engine.
`NextDeadline` supplies the earliest retry, original attempt/proof expiry or
rejection-cache expiry for the owner's timer. Exhausted send budgets retain
their original deadline; established initiators with no retry proof have no
handshake timer. The scan allocates no timers, workers or retained queues.

`BeginDial` reserves a future Session slot, initial receive claim and 2048
bytes for four exact 512-byte packets before HELLO. A listener authenticates
and validates HELLO, selects the contract and reserves the same obligations
plus one pending-accept slot before CHALLENGE. Any admission or setup failure
rolls back all those credits. Listener and initiator policies are copied.

The local base receive claim plus prepaid initial claims must cover at least:

- Datagram: both terminal-history bitmaps, charged as
  `16 * (ceil(DatagramWindow / 64) + ceil(GroupWindow / 64))` bytes.
- KCP: the full advertised `SessionReceiveBytes` bucket. This conservative
  reservation includes reliable/control receive capacity; the caller adds any
  backend window overshoot or retained metadata beyond that bucket.

The complete claims must fit the local Peer and Session byte ceilings along
with packet storage. These minima do not reserve future Datagram payloads,
FEC groups, transport copies, new business streams or additional backend
storage. Those owners need separate leases before their admissions. The
foundation does not itself verify a backend's additional storage formula.

`Policy.Initial` partitions prepaid component storage into up to sixteen
positive byte-only claims. `Setup.Initial` supplies matching same-Session lease
handles in the same order. Constructors consume those handles without making
another reservation. The adapter defines the index/cost mapping for its ring,
bitmap and backend storage; the handshake does not import those components.
The engine reserves every initial lease before protocol admission and rolls
all of them back on failure. These bytes count toward the minimum above, so a
listener's base `Receive` can be pending-accept-only and the initiator's base
can be empty. Policy and published handle slices are copied at API boundaries.

`Install` runs after matching FINISH or READY and successful credit promotion.
The listener installs before emitting READY. Installation must use already
reserved initial capacity and return a disposer for its actual storage;
additional mandatory initial capacity must be included in the policy before
CHALLENGE. A nonnil disposer returned with an error is also called. A success
with no disposer or a closed credit scope fails installation.

The engine retains the base and prepaid initial leases. `MarkAccepted` releases only
the listener's pending-accept count. On teardown, the engine closes the scope,
calls the installer disposer, clears packet/key storage and then releases
its leases. Components may release copied prepaid handles after clearing their
storage; engine release is idempotent. Additional adapter leases belong to its disposer. Closing the
shared Peer ledger stops new reservations; `Advance` retires closed scopes.

## Identity And Deadlines

An attempt binds a random SessionID, nonces, exact transcript, configured
PathID, socket incarnation ID and actual local/remote address tuple. An
unspecified wildcard address is not a valid local binding; the transport
must pass the selected packet destination/source. The engine never learns
or replaces a tuple from unauthenticated input.

HELLO, CHALLENGE and REJECT use the handshake key. FINISH and READY use their
sender's directional key. Matching duplicate packets reuse the exact stored
response; an alternate CHALLENGE after FINISH cannot replace keys or the
transcript. Wrong bindings, unknown IDs and reflected packet phases do not
advance state. Malformed input returns a nonfatal packet error. Exact
512-byte shape is required before any HELLO rejection response.

Every attempt has its original ten-second deadline, optionally shortened by
the Dial caller. Retries are at least one second apart and never catch up in
bursts. Each endpoint attempts at most eight handshake sends across phases;
HELLO/CHALLENGE may consume only seven, preserving a FINISH/READY slot. Local
cancellation can additionally send one authenticated CLOSE after keys exist.
Incoming CLOSE must match the incarnation, binding and bootstrap route and
does not generate a CLOSE response.

The listener keeps exact FINISH/READY retry proof until the original deadline,
then releases only that packet lease. Installed receive ownership persists
until Session disposal. Old FINISH/READY cannot recreate removed attempts.
A new HELLO after expiry or close may receive a fresh challenge with fresh
listener nonces and keys; it cannot reuse the old proof.

## Carrier Attempts And Bounds

Dial attempts follow the configured Carrier order, retaining its original
one-based PathID. Serial fallback and bounded concurrent attempts each get
independent SessionIDs, nonces, credits and deadlines. The first successfully
installed READY wins and cancels every sibling. A sibling already installed
remotely is disposed when its authenticated CLOSE arrives. Late responses
cannot establish a second local winner.

There are at most 256 pending attempts and 256 active Dials. Shared credit
ceilings independently bound pending handshakes, future/installed Sessions,
accepts, bytes and leases. Rejections use a fixed 256-entry cache keyed by
SessionID and the exact HELLO digest, with one response per ten seconds.
Duplicates never extend that deadline. Full rejection capacity silently drops
new responses without retaining protocol state.

## Verification

Deterministic tests exercise each lost handshake packet, exact retries,
installation ordering, cancellation, first-winner sibling cleanup, serial
fallback, send errors and budgets, exact deadlines, resource rollback,
receive minima, prepaid installation at full capacity, rejection/pending caps, wrong bindings, transcript/reflection
substitution, entropy/installer failures, reentrancy and complete disposal.
`FuzzHandshakeEvents` varies delivery, loss, duplication, corruption, retries,
time, fallback and cancellation while checking live credit/state invariants.

```sh
go test ./internal/handshakev2
go test -race ./internal/handshakev2
go test ./internal/handshakev2 -run '^$' -fuzz '^FuzzHandshakeEvents$' -fuzztime 10s -parallel 2
```

These are transport-free foundation checks. They do not establish public
Dial/Accept behavior, UDP packet-info handling, socket teardown, backend
admission, platform interoperability or network performance.
