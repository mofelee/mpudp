# smux Half-Close And Admission Evidence

Date: 2026-09-05 UTC. Status: isolated dependency experiment and joint design
input. No shared mpudp source edits, commits, upstream messages or submissions.
Neither candidate patch is a maintained upstream release or a frozen profile.

Archive: [tail patch](tail-retention.patch),
[admission hooks](admission-hooks.patch),
[full fixture](mpudp_dependency_test.go.txt),
[baseline fixture](mpudp_baseline_test.go.txt), [baseline output](baseline.log),
[patched output](patched.log), [race output](race.log), and
[upstream MIT license](UPSTREAM-LICENSE.txt). `.go.txt` keeps these external
dependency fixtures outside the production Go module. Original `/tmp` paths
below identify the experiment workspace; replay uses the archived files.

## Findings

1. smux v1.5.57 loses unread data when both CloseWrite FINs complete before
   the application reads. Real net.Pipe sessions lost all 3072 buffered bytes
   in each direction, with smux wire versions 1 and 2, for Read and WriteTo.
   Read returned clean EOF despite missing data; WriteTo sometimes returned
   io.ErrClosedPipe instead. The existing bidirectional upstream test reads
   its payload before both FINs, so it misses this ordering.
2. Remote SYN creates stream state before AcceptStream. Sending 32 SYNs with
   zero Accept calls created 32 streams, 32 queued accepts and 256 initial
   buffer-ring slots, while all 65536 receive-byte tokens remained available.
   The fixed default 1024-entry accept backlog provides a pending queue bound,
   not configurable Peer/per-Session business admission. The SYN handler also
   allocates a stream before trying to enqueue it on that backlog.
3. An isolated tail-retention patch passes the failure reproduction. An
   explicit control-stream admission prototype, using two small dependency
   hooks, refuses a new over-limit stream before business allocation and
   rejects an ungranted SYN without disturbing an existing sibling stream.

## Exact Revisions And Artifacts

- Isolated checkout: `/tmp/mpudp-smux-policy-20260905`
- Pinned tag: `github.com/xtaci/smux v1.5.57`
- Pinned SHA: `3b4ec04d359934256b3adea7133374e3c93a0622`
- Live upstream master: `ae956bb8d67bab37a312869b1d38ee3f52a7397a`,
  2026-05-15 14:27:18 +0800, message `upd`.
- `git diff HEAD FETCH_HEAD -- mux.go stream.go session.go` was empty.
  The latest observed relevant source therefore has both dependency gaps.
- Tail patch: `/tmp/mpudp-smux-tail-retention-20260905.patch`
- Admission API patch: `/tmp/mpudp-smux-admission-hooks-20260905.patch`
- Full tests: `/tmp/mpudp-smux-policy-20260905/mpudp_dependency_test.go`
- Baseline-compatible tests: `/tmp/mpudp-smux-baseline-20260905_test.go`
- Baseline output: `/tmp/mpudp-smux-baseline-20260905.log`
- Patched output: `/tmp/mpudp-smux-patched-20260905.log`
- Final race output: `/tmp/mpudp-smux-race-20260905.log`
- Joint draft: `/tmp/mpudp-v2-joint-contract-draft-20260905.md`

SHA256:

```text
62f2b96c51f56081d707b0b495286d5978dc3753016872925759c9805b6f0077  tail patch
54986b1f9b11f92ac11ad9b0f81f6b9eba10de8dc827ed7988bcbf521bbd03d6  admission hooks
23834d1e3291ab9e21a184b0f4e95c61794bf67d9850514845ab552edcea5b99  full test
0b7a4b87d4dc89e4b783ac9957b1a8af0fcbe1cea3caa05e3f84c7af41a2b30f  baseline test
```

## Commands And Outcomes

Before dependency changes, after adding only baseline-compatible tests:

```sh
go test -run '^TestMPUDP' -count=1 -v .
```

Expected baseline failure, exit 1, package elapsed 0.021s:

```text
v1_Read:    peer=0 want=3072 read=0 error=<nil>
v1_Read:    peer=1 want=3072 read=0 error=<nil>
v1_WriteTo: peer=0 want=3072 read=0 error=io: read/write on closed pipe
v1_WriteTo: peer=1 want=3072 read=0 error=io: read/write on closed pipe
v2_Read:    peer=0 want=3072 read=0 error=<nil>
v2_Read:    peer=1 want=3072 read=0 error=<nil>
v2_WriteTo: peer=0 want=3072 read=0 error=<nil>
v2_WriteTo: peer=1 want=3072 read=0 error=<nil>
Accept calls=0 allocated=32 queued=32 initial_ring_slots=256 receive_byte_tokens=65536
```

After both patches and the additional control-stream prototype test, the same
command passed with all eight direction/reader combinations receiving their
exact 3072-byte payloads. Both sessions removed drained half-closed streams
and returned receive-byte tokens exactly once. The admission case reported:

```text
business_limit=1 refused_open_allocations=0 ungranted_SYN_allocations=0 sibling_echo_bytes=41 grant_after_drain=true
PASS
ok github.com/xtaci/smux 0.009s
```

Here "allocations=0" refers to new business stream/map/ring/channel state;
control request parsing and ordinary Go calls are not claimed heap-allocation
free. The tests check unchanged business state counts and the source hook
precedes newStream, which is the tested admission boundary.

Final focused race and existing behavioral regressions:

```sh
go test -race -run '^(TestMPUDP|TestHalfClose|TestConcurrentClose|TestNumStreamAfterClose|TestStreamRecycleTokens|TestReadStreamAfterSessionClose)' -count=20 .
go vet ./...
git diff --check
```

```text
ok github.com/xtaci/smux 1.918s
```

Vet and diff check passed with no output. No broad upstream suite or throughput
benchmark was run; upstream includes large 8-GiB transfer tests. CPU work was
paused during the coordinated mpudp/auth/network measurements.

## Tail Fix

The 22-insertion/3-deletion patch changes only stream.go. Graceful half-close
cleanup now requires both FIN directions and an empty receive ring. It checks
and removes under the established session-then-buffer lock order, preserving
the stream object and its byte charge while unread data remains. Read and
WriteTo retry cleanup after consumption. A closed stream with received FIN
reports EOF deterministically to WriteTo rather than racing between EOF and
ErrClosedPipe. Full Close retains its existing explicit discard behavior.

The fixture uses actual sessions, stream APIs, ordered wire frames and
net.Pipe. A following NOP header acts as a barrier proving the receiver
completed the preceding FIN handler. No timing sleep or direct internal FIN
injection is needed to reproduce the loss.

## Admission API And Prototype

Two optional API additions preserve existing behavior when unset:

```go
Config.RemoteStreamAdmission func(streamID uint32) bool
Session.OpenStreamWithAdmission(admit func(streamID uint32) error) (*Stream, error)
```

The local callback runs after reserving a fresh ID, before newStream and SYN.
It can send an external admission request and wait for a grant/refusal with
its own bounded deadline. An error returns unchanged without a business
stream. The remote callback validates/consumes a grant before newStream;
false ignores the ungranted SYN without allocating it. It runs under the
Session stream lock, must return promptly, and cannot call Session methods.
Ignoring SYN is not itself a refusal response; normal refusal is carried by
the negotiated control protocol before SYN.

The test reserves the first bidirectional stream, ID 3, for control. A tiny
four-byte requested ID/one-byte reply fixture grants exactly one business
stream and retains its lease until drain. A second Open returns a distinct
limit error before either endpoint creates business state. An injected raw
ungranted SYN also creates none. The existing business stream then echoes 41
bytes in both directions, fully half-closes and drains, releases its lease,
and a subsequent request succeeds. Neither endpoint's Session is aborted.

The five-byte fixture proves ordering and isolation, not a finished protocol.
Stock FIN cannot communicate rejected Open: it means half-close, still allows
the peer to write, and stock OpenStream has no acceptance ACK. Session-wide
failure on ordinary admission pressure would also violate #25 isolation.

## Production Contract Still Required

The joint draft now specifies an explicitly negotiated admission profile on
the reserved control stream, without introducing hidden smux commands:

- Count/byte ownership for the reserved stream and its queues; business
  windows must leave its receive reserve available in the shared bucket.
- Bounded OPEN_REQUEST/GRANT/REFUSE/READY/CANCEL and STREAM_ABORT records,
  bound to incarnation, direction, request ID, StreamID and original deadline.
- Atomic Peer/session/pending-accept/byte reservation before a grant; pre-SYN
  local grant wait, preallocation remote validation, and READY before exposing
  successful Open. An adapter Accept pump can emit READY after ownership is
  established without another library wire command.
- Cancellation before/after SYN, late-grant cancellation without SYN, finite
  grant expiry, idempotent replies, bounded tombstones, no request/stream ID
  reuse, and denial of expired/cancelled/retired SYN without allocation.
- Exactly-once lease release after full drain or explicit abort, including
  pending accepts and Session teardown; ordinary refusal affects no sibling.
- Context-bound Open/Close behavior beyond smux's fixed 30-second internal
  timeout and predictable abort errors for cancellation races.

These are specified decisions/evidence gates, not runtime-proven features of
the small prototype. No cancellation/late-grant race, OPEN_READY, global
multi-Session budget or control-starvation proof is claimed here.

A source observation relevant to credit reservation: smux v2 initializes each
peerWindow to 262144 bytes even when MaxStreamBuffer is smaller. Its receiver
does not pre-reserve per-stream byte capacity on SYN. The production profile
must reserve that initial obligation plus accepted-frame overshoot, or use a
reviewed and negotiated initial-window change; MaxStreamBuffer alone cannot
justify a smaller initial credit claim. This observation was not separately
load-tested in this bounded task.

## Fresh Baseline Replay

Use a new empty checkout path. The active experiment checkout already has both
patches and the full test; the baseline test omits API-dependent cases.
Start these commands in the mpudp repository root:

```sh
artifact_dir="$(pwd)/docs/design/evidence/20260905-smux"
git clone --depth 1 --branch v1.5.57 https://github.com/xtaci/smux /tmp/mpudp-smux-replay
cp "$artifact_dir/mpudp_baseline_test.go.txt" /tmp/mpudp-smux-replay/mpudp_dependency_test.go
cd /tmp/mpudp-smux-replay
git rev-parse HEAD
go test -run '^TestMPUDP' -count=1 -v .
git apply "$artifact_dir/tail-retention.patch"
go test -run '^TestMPUDP' -count=1 -v .
git apply "$artifact_dir/admission-hooks.patch"
cp "$artifact_dir/mpudp_dependency_test.go.txt" ./mpudp_dependency_test.go
go test -run '^TestMPUDP' -count=1 -v .
go test -race -run '^(TestMPUDP|TestHalfClose|TestConcurrentClose|TestNumStreamAfterClose|TestStreamRecycleTokens|TestReadStreamAfterSessionClose)' -count=20 .
```

The first test invocation must fail with the recorded unread-tail loss. The
subsequent invocations must pass. Source patches require review and a pinned
maintained delivery before any mpudp implementation depends on these APIs.
