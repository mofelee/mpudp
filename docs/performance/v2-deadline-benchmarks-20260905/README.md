# Deadline Microbenchmark Evidence

This directory contains an allowlisted recovery of four previously completed
local benchmark runs. Recovery did not rerun benchmarks or correctness tests.
The files contain benchmark source, source changes, command output, run
metadata, and hashes. They contain no session transcript or raw profiles.

## Contents

- `controller/`: exact before and after controller fixtures and complete stdout,
  plus the recorded fixture and production changes.
- `reassembly/`: exact before and after reassembly fixtures and complete stdout,
  plus the recorded fixture and production changes.
- `manifest.json`: original commands, UTC start and completion times, successful
  exit statuses, file sizes, SHA256 hashes, and source provenance.
- `SHA256SUMS`: hashes for every other file in this directory.

The controller baseline source was independently matched to a subsequent
complete source read. The reassembly logs match the separately preserved raw
log files byte for byte. Its baseline fixture was recovered from the original
file addition and the recorded formatting step, then verified by reversing
the recorded fixture change against the after source. The after reassembly
fixture matches commit `ed2d6484170562f02474f3c0ebf31b6d115c1f5d`.

## Source And Reproduction

The production baseline was
`c7d3ab00baf74c84f4864d877f3f5332bcc2f205`. Both after measurements used
uncommitted production edits on that baseline. The included production patches
record those edits; they were subsequently incorporated into the deadline fix.

For the original controller run, the applicable fixture was installed as
`internal/sessionv2/hotpath_probe_test.go`. The only before/after fixture
difference constructs the pending-group expiry links during setup:
direct map insertion becomes `insertGroup`. Timed loops, five-path setup,
admission times, and pending counts are unchanged. The maintained benchmark
was renamed later, so its current name differs from these original logs.

For reassembly, the applicable fixture was installed as
`internal/reassemblyv2/deadline_test.go`. The after fixture adds correctness
checks for the expiry links and their charge. The two benchmark loops and
their real `AddGroup` setup are unchanged; setup and invariant checks execute
before `ResetTimer`. These fixtures use existing helpers from the repository's
other package test files and are not standalone Go programs.

The exact executed commands are in `manifest.json`. Controller measurements
use one sample with a 200 ms benchtime at the recorded default CPU setting.
Reassembly measurements use three samples, a 200 ms benchtime, and one CPU.
The two components therefore have different sampling settings.

## Limits Of The Evidence

These are local component microbenchmarks. They establish the cost of deadline
lookup and no-op expiry at the stated live pending counts. They do not establish
network throughput, formal performance acceptance, or resolution of the
separate retained-credit progress problem.

The after measurements predate the committed revision and its main-branch
synchronization. They must not be described as benchmark runs of the exact
`ed2d6484170562f02474f3c0ebf31b6d115c1f5d` revision. Later changes to benchmark
names and correctness tests do not retroactively change these measured runs.
Use the separate exact-revision network comparison for end-to-end claims.
