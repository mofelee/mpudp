# Transport Write-Attempt Time, 2026-09-05

[中文版](transport-attempt-20260905.zh-CN.md)

The optional `transport.SendWithAttempt` helper reports when a native transport
enters its connection write. In this local fixture, requesting the timestamp
adds 78.7-87.9 ns to diagnostics-off median operation time and adds no allocation:
all 90 observations report 264 B/op and 4 allocs/op. This prerequisite is not
activated by runtime callers; it adds no worker, pacing policy or network traffic.

```text
baseline source: 5e19e44a6ea0d9e6a084129ebfe2075e4881ae00
measured candidate source: 0ffd222f3bdbbfb81abe76ba1ce92e18a756220d
candidate tree: 5d574745aea7e6df148545e4925b9058bbed7f9a
```

These identify the measured code, even when a later documentation or merge
commit publishes this report.

## Timestamp Contract

`SendWithAttempt(ctx, path, payload) (time.Time, error)` captures time immediately
before `Write`, `WriteTo` or native `WriteMsgUDPAddrPort`, after transport write
mutex acquisition, deadline/cancellation setup, and applicable source-control
validation/address conversion. This is connection-call time, not dispatch time
or kernel transmission time. A nonzero value does not prove a successful send.

Native preflight rejection returns zero time. Once the connection write begins,
short writes, write failures, cancellation and deadline-reset failures retain
the attempt time. The package helper recognizes exact built-in Carrier and
captured Carrier/listener path types. Unknown custom `ReplyPath` implementations
use their ordinary `Send` and return zero time. This preserves a custom wrapper's
overridden `Send`, even when embedding a Carrier also inherits its timing method.
The `ReplyPath` interface is unchanged.

Ordinary `Send` passes nil timestamp storage; the timestamp plumbing adds no
clock read or allocation to that path. Requesting a timestamp does not enable
diagnostics. Captured generation, remote/source/OOB ownership, write serialization,
deadline cleanup, cancellation callback joining and Close behavior are preserved.

## Local Comparison

The shared fixture sends a 1200-byte packet with `context.Background` through a
deterministic no-op connection. It exercises actual transport locking,
cancellation setup and optional diagnostics. It excludes UDP syscalls, network
load, constructing a fresh caller timeout context per send and retained fake
payload copies. `ns/op` is elapsed benchmark time, not sampled CPU cost or product
throughput; the raw `MB/s` field is not network bandwidth.

Native Linux/amd64, Go 1.26.4 and Intel Xeon E3-1245 v5 were recorded. Available
affinity was CPUs 0-5; each timed process was pinned to CPU 2 with `GOMAXPROCS=1`
and `-test.cpu=1`. Baseline Send, candidate Send and candidate timed-helper runs
were sequential, with five 200 ms observations per case. Parent/sibling tests,
builds and bulk processing were paused during the recorded timing window.

Values below are median ns/op followed by raw minimum-maximum. All samples are
retained, without outlier removal or a statistical significance claim.

| Path / diagnostics | Baseline Send | Candidate Send | Timed helper |
| --- | ---: | ---: | ---: |
| Carrier / off | 706.5 [695.7, 752.8] | 695.8 [684.3, 723.3] | 783.7 [764.1, 845.4] |
| captured Carrier / off | 713.4 [696.1, 743.7] | 714.3 [701.9, 739.2] | 795.2 [785.6, 809.1] |
| listener / off | 801.2 [795.4, 835.5] | 813.7 [799.5, 824.1] | 892.4 [878.3, 901.5] |
| Carrier / on | 2193 [1028, 3000] | 970 [967.8, 991.5] | 1189 [1172, 1205] |
| captured Carrier / on | 956.8 [939.8, 982.8] | 966.9 [955.1, 1004] | 1037 [1004, 1049] |
| listener / on | 1079 [1049, 1131] | 1097 [1068, 1101] | 1164 [1134, 1178] |

Diagnostics-off timed-helper median increments over candidate Send are 87.9 ns,
80.9 ns and 78.7 ns for Carrier, captured Carrier and listener respectively.
Ordinary Send median changes are -1.5%, +0.1% and +1.6% in that order. Every
baseline and candidate observation reports the same 264 B/op and 4 allocs/op.

The baseline diagnostics-on Carrier series is visibly noisy, spanning
1028-3000 ns/op. Its apparent improvement is not attributed to this change.
These short synthetic measurements establish neither network throughput gains
nor a formal performance acceptance result.

## Source And Executables

The baseline contains only the shared legacy benchmark fixture added to its
pinned source. The candidate patch includes both benchmark fixtures and all
production/test changes. Source patch, shared fixture and executable hashes were
captured before benchmark invocation and checked afterward. After committing,
the committed patch and rebuilt candidate executable matched the measured
artifacts byte for byte. Test executable metadata contains no embedded VCS SHA.

| Artifact | SHA-256 |
| --- | --- |
| Candidate source/test patch | `957b2c37ea2d464e77f841203afd84763cd9be56dd26432c64fcf1a60eed191b` |
| Shared legacy fixture | `12174cf15cce5f779bee5320b7e929a0cface596a86f65d3ba91137a4286a336` |
| Candidate timestamp fixture | `24ed51b1fccef5179f293d2b5d2a3f1725343dae4fd46b69a954e8fad840d8eb` |
| Baseline executable, excluded | `3657ada5f86e9d488a38fbb3ecdeff2c064e669e2f5c0e3ffe313617884fc818` |
| Candidate executable, excluded | `edf12d44d841a22e89507a5f7f173d33e0556fffd0985c3fd28e584f93bd1d30` |

Reconstruct the source and compile with the recorded Go environment, using the
extracted archive directory as `evidence_dir` and otherwise unused worktree paths:

```sh
git worktree add --detach /tmp/mpudp-attempt-before 5e19e44a6ea0d9e6a084129ebfe2075e4881ae00
git worktree add --detach /tmp/mpudp-attempt-after 0ffd222f3bdbbfb81abe76ba1ce92e18a756220d
cp "$evidence_dir/send_legacy_benchmark_test.go.txt" /tmp/mpudp-attempt-before/internal/transport/send_legacy_benchmark_test.go
go -C /tmp/mpudp-attempt-before test -c -o "$evidence_dir/baseline.test" ./internal/transport
go -C /tmp/mpudp-attempt-after test -c -o "$evidence_dir/candidate.test" ./internal/transport
```

Alternatively, applying `candidate.patch` to a clean baseline reconstructs the
candidate tree, including both fixtures. The public audit verifies that exact
tree through a temporary Git index without changing the current worktree.
Rebuilt executable hashes may vary with build paths or environment; source-tree
reconstruction is independent of that. On an otherwise idle host, run from the
extracted evidence directory, adjusting CPU affinity to an available CPU:

```sh
env GOMAXPROCS=1 taskset -c 2 ./baseline.test -test.run '^$' -test.bench '^BenchmarkTransportSendLegacy$' -test.benchtime=200ms -test.count=5 -test.cpu=1
env GOMAXPROCS=1 taskset -c 2 ./candidate.test -test.run '^$' -test.bench '^BenchmarkTransportSendLegacy$' -test.benchtime=200ms -test.count=5 -test.cpu=1
env GOMAXPROCS=1 taskset -c 2 ./candidate.test -test.run '^$' -test.bench '^BenchmarkTransportSendAttempt$' -test.benchtime=200ms -test.count=5 -test.cpu=1
```

## Validation And Archive

The producer recorded passing `go test ./internal/transport`, `go test -race
./...` across all 25 root-module packages, `go vet ./...`, formatting and
`git diff --check`. The archive records those test outcomes in the producer
README; it does not contain full test transcripts. Parent and independent code
reviews found no blocking issue. Tests cover timestamp placement after lock and
deadline setup, preflight errors, write/cleanup failures, cancellation, Close,
successive-call reset, adapter fallback and native source-aware replies.

The [public archive](transport-attempt-20260905.tar.gz) contains 24 regular text
files: 19 original records, public notes/audit code, an exact member list, a
source-verification record and a 23-entry public checksum index. Executables,
private files and `.lab` directories are excluded. Their absence is enforced by
the exact member allowlist; binary hashes remain metadata. Host metadata and
temporary source paths remain visible. All actual decompressed members pass
UTF-8, binary-content and credential-marker scans. The archive has normalized
types/modes/owners/timestamps, and rebuilding it produces identical bytes.

After extraction, run `sha256sum -c PUBLIC_SHA256SUMS`. The preserved
`pre-run-SHA256SUMS` also lists excluded binaries and is historical metadata, not
the public verification index. With the repository containing both source
commits, run:

```sh
python3 public-evidence.py audit /path/to/transport-attempt-20260905.tar.gz --repo /path/to/mpudp
python3 compare.py
```

The audit verifies archive membership/checksums, source reconstruction, fixture
identity and all 90 raw observations against the preserved comparison JSON.
It does not require binaries or rerun measurements.

```text
archive bytes: 21732
archive SHA-256: 3e5bdf68be30bf2bb3dc85d656191ea79c4323a96b684a2fc3668abef435d5ce
```
