# MPUDP Linux Integration Harness

`scripts/integration` provides the repository-owned Linux network-namespace
harness for issue #8. It creates the topology, runs named scenarios, collects
redacted diagnostics, and removes only resources owned by one unique run ID.
GitHub Actions in issue #9 can call the same commands and scenario manifest.

This loop does not create, inspect, or remove libvirt VMs. A developer may run
the repository scripts inside a disposable Debian VM, but the harness itself
has no dependency on Codex skills, libvirt URIs, host storage pools, or an
external hypervisor.

## Prerequisites

- Linux with network namespace, veth, IPv4/IPv6 forwarding, netfilter NAT, and
  traffic-control support;
- root, or an environment providing the equivalent `CAP_NET_ADMIN` and
  `CAP_SYS_ADMIN` capabilities;
- `bash`, `ip`, `ping`, `tc`, `nft`, `tcpdump`, `sysctl`, `timeout`, `awk`,
  `sed`, `sha256sum`, `realpath`, and Go;
- `conntrack` for tuple diagnostics and the final full #8 gate;
- `ss` and `diff` for public Peer Carrier identity checks.

On Debian/Ubuntu the normal package surface is:

```text
iproute2 iputils-ping nftables conntrack tcpdump
```

The raw NAT smoke does not require the `conntrack` CLI: successful reverse
translation plus per-path nft counters proves that kernel connection tracking
is active. If the binary is absent, setup records `missing` and collect omits
tuple dumps. CI can enforce the full diagnostic prerequisite with:

```bash
sudo env MPUDP_IT_REQUIRE_CONNTRACK=1 scripts/integration/run \
  --case transparent-nat-v4
```

## One-command workflow

The default performs setup, the IPv4 transparent-NAT smoke, teardown, and a
clean-resource audit:

```bash
sudo scripts/integration/run
```

Several base cases can share one topology:

```bash
sudo scripts/integration/run \
  --case transparent-nat-v4 \
  --case transparent-nat-v6 \
  --case path-controls-v4 \
  --case path-controls-v6
```

The public Datagram runtime cases can likewise share one setup:

```bash
sudo scripts/integration/run \
  --case direct-single-carrier \
  --case peer-smoke-v4 --case peer-smoke-v6 \
  --case peer-payload-mtu-v4 --case peer-payload-mtu-v6 \
  --case peer-nat-rebinding-v4 --case peer-nat-rebinding-v6 \
  --case peer-endpoint-expiry-v4 --case peer-endpoint-expiry-v6
```

`run` installs EXIT/HUP/INT/TERM handling. On failure it calls `collect` before
`teardown`; without `--artifacts`, a short sanitized diagnostic summary is
printed and the temporary state is removed. To retain diagnostics outside the
run state:

```bash
sudo scripts/integration/run --artifacts /var/tmp/mpudp-it-artifacts
```

The runner rejects an artifact destination whose per-run output would overlap
the state directory; teardown intentionally removes successful run state.

## Staged workflow

Every phase is non-interactive. `setup` is the only command that writes its
state path to stdout; progress goes to stderr.

```bash
run_id=manual-$(date -u +%Y%m%d%H%M%S)
state=$(sudo scripts/integration/setup --run-id "${run_id}")
trap 'sudo scripts/integration/teardown --state "${state}" --run-id "${run_id}"' EXIT

sudo scripts/integration/run-case --state "${state}" transparent-nat-v4
sudo scripts/integration/collect --state "${state}" --output /var/tmp/mpudp-diagnostics
sudo scripts/integration/teardown --state "${state}" --run-id "${run_id}"
trap - EXIT
sudo scripts/integration/audit --run-id "${run_id}" --state "${state}" --expect-clean
```

`teardown` is idempotent. Supplying both state and run ID adds a consistency
check. A second teardown can use the removed default state path plus the same
run ID without broad name matching. If no validated state exists, teardown is
non-destructive even when similarly named foreign namespaces or links exist.
If a cleanup operation fails, the validated state is retained so the exact
cleanup can be diagnosed and retried.

## Topology

Each run creates seven namespaces:

```text
Alice -- path 1 --> T1 --\
Alice -- path 2 --> T2 ---\
Alice -- path 3 --> T3 ----> Bob UDP :9000
Alice -- path 4 --> T4 ---/
Alice -- path 5 --> T5 --/
```

Each logical path has separate IPv4 and IPv6 veth pairs on both sides of Ti.
This is deliberate: Linux MTU is a link property, so separate links allow an
IPv6 path to run at MTU 1280 without changing its IPv4 counterpart.

| Segment | IPv4 | IPv6 |
|---|---|---|
| Alice to Ti | `10.101.i.2/30 -> 10.101.i.1/30` | `fd42:101:i::2/64 -> fd42:101:i::1/64` |
| Ti to Bob | `10.102.i.1/30 -> 10.102.i.2/30` | `fd42:102:i::1/64 -> fd42:102:i::2/64` |

Alice sends only to Ti port 4000 and has no route to Bob's path addresses. Each
Ti has only forwarding sysctls and two run-specific nft tables. It DNATs port
4000 to Bob port 9000 and masquerades toward Bob. It runs no MPUDP helper and
receives no PSK. Bob's test helper replies with the exact socket bound to port
9000, allowing conntrack to reverse both NAT operations.

The IPv4 and IPv6 nft forward chains count accepted forward/reverse datagrams
and defensively drop fragments visible at that hook. Counter-only rules also
classify MPUDP control and DATA packets by the public `MPUD` discriminator and
one-byte wire type. They do not authenticate or decode packets, and T nodes
still receive no PSK. Because conntrack may reassemble before `forward` and
fragmentation may happen afterward, nft counters are not used as proof of zero
fragmentation. During each NAT smoke, a header-only `tcpdump` observer watches
both wire-facing interfaces inside every Ti. The case requires empty
fragment-filter output and zero capture-kernel drops before asserting zero
on-wire fragments. A separate observer records only timestamp, address,
protocol, and UDP payload length from tcpdump's quiet header output. No pcap or
packet bytes are retained. Netprobe and public Peer events record bounded
metadata and a 12-hex SHA-256 prefix, never packet contents.

## Scenario contract

`integration/scenarios/cases.tsv` is the single case manifest consumed by
`run-case` and intended for #9 matrix generation:

```bash
scripts/integration/run-case --list
```

The final column records whether a case requires a runtime unavailable in this
checkout. All maintained rows are currently runnable (`false`); a future row
marked `true` exits 77 rather than reporting a false pass.

Topology and control cases:

- `transparent-nat-v4` and `transparent-nat-v6` prove five independent
  DNAT+masquerade round trips and same-socket Bob replies;
- `path-controls-v4` and `path-controls-v6` prove one path's controls do not
  mutate another path;
- `nat-rebinding-trigger-v4` and `nat-rebinding-trigger-v6` create two explicit
  Alice source ports and require Bob to observe two distinct NAT Endpoints.

Public `mpudp.Peer` cases use only the exported Datagram API with fixed RS(5,3)
and a test-only PSK:

The probe uses an explicit reply-complete barrier before either Peer may begin
final shutdown. Receiving the first three recoverable shards therefore cannot
race the listener's remaining parity sends or turn an orderly close into a
spurious partial-send result.

- `direct-single-carrier` has an explicit IPv4 contract. For the duration of
  the case it adds host routes between Alice and Bob through T1 plus a
  countered no-SNAT rule ahead of the baseline masquerade rule. Alice targets
  Bob's address and port directly, with exactly one connected Carrier whose
  kernel-assigned source tuple remains stable. Distinct 321-byte and 257-byte
  Datagrams plus an empty Datagram cross in each direction exactly once, with
  boundary and digest assertions. A blocked third read is released only after
  the reply-complete/final barriers by public `Session.Close`; both Peer
  processes must then close cleanly. Direct wire metadata, unchanged baseline
  DNAT/SNAT counters, and the removal of every temporary route/rule prove this
  is not the five-path NAT smoke under another name;
- `transparent-nat-reverse-path` is the canonical five-Carrier IPv4 NAT
  contract. Alice has no route to any Bob address and configures only T1-T5;
  Bob has no route to Alice and configures only its listener. Every T namespace
  remains process-free. Distinct forward/reverse Datagram sizes must cross all
  five DNAT+SNAT mappings, with reverse DATA sourced from Bob's listening port,
  per-path control/DATA counter deltas, and no fragments;
- `endpoint-rebinding-and-expiry` holds one accepted Session while path 1
  changes conntrack zone/source port from 41001 to 41002. The old reverse
  Endpoint is then isolated, paths 2 and 3 lose forward traffic, and a 1-second
  keepalive preserves only the authenticated new/surviving Endpoints across a
  measured 5-second TTL. A unique post-expiry shard size must target 41002 and
  paths 1/4/5, never 41001 or expired paths 2/3, while bidirectional delivery
  and the original Session continue;
- `mtu-budget-no-fragment` is one dual-stack canonical row rather than an
  alias for the foundation MTU cases. It runs a 1200/1000 negotiation in both
  directions, IPv6 at link MTU 1280, an explicit 520-byte IPv4 tunnel budget,
  exact-limit and limit+1 writes, then an intentionally overstated path-1
  budget. Header-only capture proves every safe packet stays within budget and
  produces zero fragments; the bad path reports `ErrPartialSend` plus
  `ErrPathMTUExceeded` while four surviving paths recover the Datagram;
- `peer-smoke-v4` and `peer-smoke-v6` require five connected Carrier sockets,
  stable local/remote socket tuples across the exchange, one 754-byte shard on
  each path for a 2048-byte Datagram, per-path forward/reverse control and DATA
  counter deltas, bidirectional delivery, NAT counters, and zero fragments;
- `peer-payload-mtu-v4` negotiates a 1200/520 capability pair over a 576-byte
  path MTU, while `peer-payload-mtu-v6` negotiates 1200 from a 1450/1200 pair
  over the IPv6 minimum MTU 1280. Both send exact-limit Datagrams in both
  directions, require every observed UDP payload to remain within the
  negotiated budget, and reject exact-limit+1 before any packet or nft counter
  changes;
- the same payload cases deliberately configure budget above path capability.
  Path 1 must return public `ErrPartialSend` plus `ErrPathMTUExceeded` without
  emitting the oversized shard, paths 2-5 must emit full-size shards, and Bob
  must recover the Datagram without an IP fragment;
- `peer-nat-rebinding-v4` and `peer-nat-rebinding-v6` move path 1 from fixed
  source port 41001/conntrack zone 101 to 41002/zone 102. With forward traffic
  dropped on paths 2 and 3, delivery requires the rebound path; Bob must retain
  one accepted Session and Alice's five connected socket tuples must not
  change;
- `peer-endpoint-expiry-v4` and `peer-endpoint-expiry-v6` bootstrap one accepted
  Session, remain quiet with Endpoint TTL 5 seconds and keepalive 30 seconds,
  then require that same Session's write to return `ErrNoAvailablePaths` after
  at least 5 seconds of measured quiet time.

Each case has a manifest hard timeout, capped by `run-case` at 120 seconds. The
public executable always enters GNU `timeout`; its worker is loaded only inside
that bounded child, so inherited environment markers cannot bypass the limit.
On every worker exit, `run-case` stops any still-live recorded helper whose run
ID and process start time match, while preserving the shared topology for the
next staged case.

## Path controls

All controls require state, path 1..5, and address family 4 or 6.

```bash
# One deterministic egress per direction: forward=Alice, reverse=Bob.
sudo scripts/integration/control --state "${state}" netem \
  --path 2 --family 4 --direction forward --delay 50ms --loss 10%

sudo scripts/integration/control --state "${state}" clear-netem \
  --path 2 --family 4 --direction forward

sudo scripts/integration/control --state "${state}" link \
  --path 3 --family 6 --link-state down

sudo scripts/integration/control --state "${state}" mtu \
  --path 5 --family 6 --value 1280
```

`--delay 50ms` means 50ms on that directional egress, not 50ms on each veth
segment. `--direction both` applies once on Alice and once on Bob, affecting the
two traffic directions independently.

Wire-aware tests can drop an exact packet/shard field without hard-coding the
wire layout in this harness:

```bash
sudo scripts/integration/control --state "${state}" drop-match \
  --path 1 --family 4 --offset 24 --hex 0000002a
sudo scripts/integration/control --state "${state}" clear-drop \
  --path 1 --family 4
```

The offset starts at MPUDP byte zero after the UDP header; the match accepts
one to four bytes. The test that invokes it owns the wire offset/value. The
base path-control cases send a real matching datagram, require its nft drop
counter to advance, clear the rule, then require the same exchange to succeed.

The `nat-rebinding-trigger-*` cases are topology-level probes: two fresh Alice
sockets use source ports 41001 then 41002, and Bob must observe two distinct NAT
Endpoints. The public `peer-nat-rebinding-*` cases apply the same deterministic
port change to one live Carrier and prove authenticated Endpoint replacement
without replacing the Session or Alice's Carrier socket. The
`peer-endpoint-expiry-*` cases separately prove TTL removal after a measured
quiet interval.

## Run ownership and cleanup

Run IDs contain 1..48 lowercase letters, digits, or hyphens. `MPUDP_IT_SEED`
accepts 1..128 letters, digits, dots, underscores, colons, pluses, or hyphens
and defaults to the run ID. The exact textual seed is persisted in run state,
included in the case-start event, and copied into failure metadata for replay.
A SHA-256-derived
eight-hex token makes every transient host interface and nft table unique while
remaining within Linux's 15-byte interface-name limit. State is mode 0700 and
records namespaces, transient links, PIDs plus `/proc` start times, capability
status, and metadata-only events.

Cleanup validates the state marker, owner, run ID/token relation, exact seven
namespace names, and interface prefix before deletion. It does not flush a host
nft ruleset, delete wildcard namespaces, or touch unrelated qdiscs/processes.
Recorded PIDs are killed only when start time and `--run-id` still match.
Namespace deletion removes the run's veth/qdisc/table state; an independent
audit then checks exact names, helper command lines, and the state path.

Repeat and bounded parallel verification use the same full lifecycle. On
HUP/INT/TERM, `repeat` terminates and waits for every active child, performs an
exact run-ID teardown/audit for each, and only then removes its logs:

```bash
sudo scripts/integration/repeat --count 20 --parallel 1 \
  --case transparent-nat-v4
sudo scripts/integration/repeat --count 8 --parallel 4 \
  --case transparent-nat-v4
```

## Diagnostic safety

`collect` captures only this run's namespace links, addresses, routes, qdiscs,
sockets, forwarding values, nft counters, optional conntrack tuples, public
Peer metadata events, and header-only UDP and fragment-observer logs. Peer
events contain bounded sizes, timing, error classes, and 12-hex digest prefixes;
UDP logs contain only timestamps, addresses, protocol, and payload lengths.
Collection does not capture host-wide processes, packet bytes, pcap files,
PSKs, authentication tags, Session IDs, or full application payloads. Harness
logs are sanitized again while copying.

## Scope boundary

Issue #8 owns this reusable topology, the public Peer smoke/MTU/rebinding/expiry
cases, diagnostics, hard timeouts, repetition, and exact cleanup. Broader
protocol matrices remain with issue #9, including authentication-rejection and
state-pollution attacks, duplicate-delivery coverage under shard loss/late
arrival, and GitHub-hosted workflow orchestration.

The optional disposable Debian VM development record also remains outside this
repository run. It requires an explicitly selected libvirt URI and a separate
`virsh-test-host` invocation; these harness commands neither create nor delete
VM resources.
