# KCP Fast/Early Policy Experiment

Date: 2026-09-05 UTC. Status: isolated review candidate, no shared mpudp
source edits, no commits, no upstream submission or published dependency.

Archive: [source patch](fast-retransmit.patch),
[runnable fixture source](mpudp_policy_test.go.txt) and
[upstream MIT license](UPSTREAM-LICENSE.txt). The `.go.txt` suffix deliberately
keeps dependency fixtures outside the production Go package/module graph.
The original `/tmp` paths below identify where the experiment ran.

## Result

kcp-go v5.6.72 does not provide a working public API to disable both fast
and early retransmission while preserving RTO. `NoDelay(..., resend=0, ...)`
still early-retransmits a missing segment when a later segment is acknowledged.
Real sender/receiver core engines reproduced that behavior before the patch.

A small explicit policy setter passes focused real-engine tests and 100 race
runs. Default behavior is unchanged. Explicitly disabling the policy prevents
both gap-driven retransmission paths, while RTO recovers a missing first
segment and a missing tail, including a lost ACK of the repaired tail. No
application message is delivered twice and the sender reaches WaitSnd()==0.

## Artifacts And Revisions

- Isolated checkout: `/tmp/mpudp-kcp-policy-20260905`
- Baseline tag: `github.com/xtaci/kcp-go/v5 v5.6.72`
- Baseline source SHA: `78a2a3914a97fe8414b3f63054fad36c0a6e2e7c`
- Upstream master observed during this investigation:
  `f3f1bbd9b9f2c18fde5882c19335ae31c131b077` (2026-05-15, refine README).
  Its relevant fast/early implementation remains the same and no newer
  supported disabling setter was found. This is a source/API observation,
  not a claim that the experimental patch is already maintained upstream.
- Source patch: `/tmp/mpudp-kcp-fast-retransmit-policy-20260905.patch`
- Patch SHA256:
  `a8501eec93f043daebe900ab52dbd11df20aad51f2621c080e3b8dbcf7f8abf3`
- Runnable test: `/tmp/mpudp-kcp-policy-20260905/mpudp_policy_test.go`
- Test SHA256:
  `e12cf0f635949c90b817744385ccf400989a64344a1d28811f17b7994e49ed46`
- Corrected joint draft: `/tmp/mpudp-v2-joint-contract-draft-20260905.md`

The tracked source patch changes only `kcp.go` and `sess.go`: 35 inserted,
6 removed lines, including gofmt alignment changes. The test is untracked
in the isolated checkout and deliberately excluded from that source patch.

## Baseline Evidence

Command, executed after adding the test and before changing dependency source:

```sh
cd /tmp/mpudp-kcp-policy-20260905
go test -run '^TestMPUDP' -count=1 -v .
```

The two baseline-compatible cases passed:

```text
TestMPUDPLegacyResendZeroStillEarlyRetransmits
  fast=0 early=1 rto=0 total=1
TestMPUDPEnabledFastRetransmit
  fast=1 early=0 rto=0 total=1
PASS
ok github.com/xtaci/kcp-go/v5 0.012s
```

The five policy-dependent scenarios explicitly skipped on the unmodified
baseline because the core/UDPSession setter does not exist. They all ran and
passed after patching. Baseline reproduction is not a test expectation that
resend=0 already implements the requested off policy.

## Patched Evidence

The same verbose command passed with no skips:

| Scenario | Fast | Early | RTO | Outcome |
|---|---:|---:|---:|---|
| Legacy default, resend=0, gap ACK | 0 | 1 | 0 | Default unchanged |
| Default enabled, threshold=2, two gap ACKs | 1 | 0 | 0 | Fast threshold unchanged |
| Explicit off, threshold=0 | 0 | 0 | 1 | No pre-RTO output; eventual ordered delivery |
| Explicit off, threshold=2 | 0 | 0 | 1 | No pre-RTO output; eventual ordered delivery |
| Explicit off, tail loss then repair ACK loss | 0 | 0 | 2 | One application delivery, eventual ACK drain |
| Off/on toggle after prior gap evidence | 0 | 1 | 0 | Old evidence cleared; new evidence still works |
| Concurrent UDPSession setters | 0 | 0 | 0 | Mutex serialization and core forwarding |

The patched verbose run finished:

```text
PASS
ok github.com/xtaci/kcp-go/v5 0.016s
```

Focused race command and exact final package output:

```sh
go test -race -run '^TestMPUDP' -count=100 .
```

```text
ok  github.com/xtaci/kcp-go/v5  1.461s
```

## Test Method And Limits

Both peers are actual `KCP` engines. Each output callback copies and parses
all emitted KCP segments using the library's little-endian header framing.
Tests drop selected PUSH segments or receiver-generated ACKs and feed real
ACK bytes back through sender Input. Retransmission cause is measured using
before/after SNMP snapshots, without resetting global counters. Tests are
nonparallel and the focused invocation excludes other test traffic.

The fixture uses a long initial RTO to exclude wall-clock expiry from the gap
experiment, then makes one retained segment's resend deadline due before a
normal full flush. This deterministically tests the production RTO branch;
it does not measure real-time scheduler accuracy or network performance.
`nc=1` is used only to make multiple initial segments outstanding without a
warmup. Congestion control remains enabled in the joint production design,
and its integration behavior still needs coverage. The upstream TestMain
banner mentioning salsa20/FEC is unrelated to these raw-core fixtures, which
do not use KCP FEC, MPUDP FEC, encryption or network sockets.

No full upstream package suite was run: it includes large 1-GiB/6-GiB echo
and other load tests outside this bounded policy experiment. MTU shrink,
outer fragmentation, scheduler behavior, stream FIN, smux admission/half-close
and exact promoted mpudp SHA integration are separate outstanding gates.

## Minimal Backend Approach

Add `SetFastRetransmit(enabled bool)` to the core and locked UDPSession
wrapper. A default-false internal `fastRetransmitDisabled` field preserves
the dependency's existing default. When disabled, suppress gap evidence in
`parse_fastack` and guard both the fast and early flush branches. Leave the
RTO branch, ACK handling, new sends and congestion-control code intact.

The setter does not change NoDelay settings. A subsequent NoDelay call does
not re-enable the policy. Policy transitions clear accumulated gap evidence;
repeating the same setting is idempotent. Core callers still synchronize
access; the UDPSession wrapper takes its existing mutex.

Shipping choices are an upstream accepted revision exposing this behavior or
an explicitly maintained dependency fork carrying the reviewed small patch.
No existing maintained release was verified to have the required setter.
Do not map off to resend=0 or mark regular ACKs as `IKCP_PACKET_FEC`: FEC Input
still calls parse_fastack, while suppressing legitimate RTT/window updates.

## Reproduce From A Fresh Baseline

Use a new empty checkout path; the tested checkout above already has the patch.
From the mpudp repository root, the archived fixture can be replayed with:

```sh
artifact_dir="$(pwd)/docs/design/evidence/20260905-kcp"
git clone --depth 1 --branch v5.6.72 https://github.com/xtaci/kcp-go /tmp/mpudp-kcp-policy-replay
cp "$artifact_dir/mpudp_policy_test.go.txt" /tmp/mpudp-kcp-policy-replay/mpudp_policy_test.go
cd /tmp/mpudp-kcp-policy-replay
git rev-parse HEAD
go test -run '^TestMPUDP' -count=1 -v .
git apply --check "$artifact_dir/fast-retransmit.patch"
git apply "$artifact_dir/fast-retransmit.patch"
go test -run '^TestMPUDP' -count=1 -v .
go test -race -run '^TestMPUDP' -count=100 .
```

## Joint Contract Corrections

MTU mode and budget scope are orthogonal: fixed/session retains the shared
safe budget; fixed/per_carrier accepts explicit known-safe directional path
profiles bounded by local/peer caps; plpmtud/session uses the minimum confirmed
active-path budget; plpmtud/per_carrier uses individual confirmed budgets.
The listener requires a reverse-direction profile. Missing/ambiguous static
profiles are rejected, and values are never inferred from interface MTU,
successful writes or the forward direction's configured cap.

Encoding epoch announcement/ACK is distinct from a directed path-budget
update: one bundle may contain immutable groups from multiple admitted
encoding epochs. Unknown future epochs cannot mutate state. Retention and
announcement retries remain bounded. Freeze gates are engineering decisions
and missing evidence, not requests for additional user authorization.
