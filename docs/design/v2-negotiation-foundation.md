# V2 Stateless Negotiation Foundation

`internal/negotiationv2` implements the HELLO/CHALLENGE contract validation
and selection subset of [#20](https://github.com/mofelee/mpudp/issues/20).
It consumes the [wire registry](v2-registry.md) through `internal/wirev2`.
This package does not activate v2, open sockets, create attempts or Sessions,
reserve memory, or connect the proposed settings to the public Config API.

## Boundary And API

The caller first authenticates and decodes the wire handshake. `DecodeHello`
and `DecodeChallenge` then copy the twelve required TLVs into value-only
`Advertisement` records and validate their normalized semantics. They
defensively check packet type, TLV ordering, uniqueness, required presence,
registered lengths, reserved bytes, the sixteen-TLV limit, and the 372-byte
TLV budget even when given a caller-constructed `wirev2.Handshake`.
Unknown optional TLVs are ignored for contract selection; unknown required
TLVs reject. The bounded errors never include input payloads.

`Advertisement.TLVs` emits exactly the twelve canonical mandatory TLVs, with
owned value buffers. It does not re-emit optional extensions. The caller
must preserve the original authenticated wire transcript, including those
extensions; reconstruction from the typed contract is not transcript proof.
Session IDs, nonces, transcript matching, attempt deadlines and return-path
validation remain wire and runtime responsibilities.

`Select(hello, listenerProfile)` computes the known capability intersection
and constructs the listener CHALLENGE with its own receive ceilings.
`Accept(hello, challenge)` independently validates the client's view of the
selection and produces the same `Contract`. The client can verify that the
selection is a subset of its offer covering both required sets; it cannot
prove an exact intersection with a listener offer it never received.

## Policy Decisions

Protocol, Datagram layout and FEC k/r, mux profile, MTU discovery and budget
scope must match exactly. Layout 1 is the only current Datagram layout.
Datagram requires positive FEC counts with sum at most 256; KCP requires
zero layout/FEC/Datagram fields and the fixed 1500-byte inner packet limit.
The fixed handshake bootstrap is 512 bytes in both modes. Datagram zeros all
reliable fields. Raw KCP retains pending-accept, Session receive-byte and
stream receive-byte ceilings, but zeros business-stream count, control
reserve, pending-open count and mux frame size.

Unknown offered capability bits do not enter the selected intersection;
unknown required bits reject. Required bits must be offered, and both
endpoints' required sets must survive selection. Active Datagram requires
`fragment_manifest`, active KCP requires `native_kcp`, mux requires
`smux_admission_v1`, and the selected discovery/scope requires its PLPMTUD
or per-carrier capability. KCP requires `kcp_packet_pieces` for PLPMTUD or
per-carrier budgets, where directed path budgets may require fragmentation.

Disabled repair and mux clear their offered capability bits; enabled repair
and mux require them. An enable/disable mismatch rejects. Disabled repair
has zero age and attempts. Other optional implementation capabilities may
remain offered even when inactive in the current mode. `SelectedCaps` keeps
their negotiated intersection; `ActiveCaps` filters it to the current mode
and MTU strategy. Inactive required bits reject; selection cannot silently
discard a requirement. Neither field enables local aggregation or pacing policy.
`MaxMigrations` is zero when `group_migration` is not offered, and 1..2 when
offered. This wire-neutral zero is distinct from the proposed Config default
of two attempts. Selection clears the limit when migration is not selected.

The initiator's configured bootstrap PathID is echoed without renumbering.
Fixed/per-carrier mode requires the listener ceiling to cover the whole
initiator configured path count, including paths that did not win bootstrap.
Other modes select the smaller path count. In every mode, the unchanged
winning index must fit the selected count.

## Directional Limits

`Contract.EffectiveSend(role, localSendLimits)` intersects the explicit local
sender ceilings with the opposite endpoint's advertised receive ceilings.
An endpoint's own advertised receive windows do not clamp its send direction.
Datagram assemblies in `SendLimits` bound outstanding local originals;
their effective ceiling also fits both selected Datagram ID windows.
Mux pending opens also fit the peer pending-accept ceiling. Repair age and
attempts, old epoch count, migration count, grace and mux frame size use
endpoint minima.

For mux, `Streams.SessionBytes` is the total local/peer minimum, including
control traffic. `ControlReceiveReserve` exposes the peer's control-stream
receive floor. `BusinessSessionBytes` is
`min(local sender SessionBytes, peer SessionReceiveBytes - peer control reserve)`;
the per-stream send ceiling also fits this business bound. Peer receive
storage and local sender storage are independent: a 64 KiB local sender
ceiling with a 1 MiB peer Session and 278528-byte peer control reserve still
permits up to 64 KiB business storage. The caller must account for actual
local control bytes under the total sender ceiling, reserve its own control
queue capacity, and enforce aggregate business storage separately. Raw KCP
has zero control reserve and its whole Session budget is business capacity.

The UDP output is `min(local SendHardCap, peer ReceiveHardCap)`, a hard
ceiling, not a proven safe path budget. Both directions still start at 512;
fixed budget publication or PLPMTUD evidence is needed before larger sends.
The package cannot calculate the current encoding epoch's fragment capacity
or prove enough retained bytes for an advertised Datagram maximum.

Reliable advertised receive bytes may be below proposed Config maxima when
admission has reserved a smaller amount. Positive bytes up to 1 GiB are
checked here, with stream bytes no larger than Session bytes. Mux also
requires at least `262144 + MaxFrameBytes` for each initial business window
and the control reserve, and enough Session capacity for both. Overflow is
checked with wider arithmetic. These checks do not prove a reservation:
the future admission ledger must cover the Session, Peer, control stream,
KCP backend and every business stream atomically before activation.

## Verification

Tests consume the independent authenticated HELLO/CHALLENGE wire fixture,
check canonical TLV equality and packet round trips, exercise raw KCP and
mux, asymmetric directional limits, disabled policies, unknown capabilities,
forged selections, original bootstrap indices, numeric bounds and overflow.
Decoder tests cover arbitrary malformed TLV shapes, value ownership,
optional extensions, and bounded payload-free errors. A seeded fuzz target
mutates every registered TLV and checks accepted-value round trips.
