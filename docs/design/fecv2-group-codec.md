# V2 Logical Group Codec Foundation

`internal/fecv2` implements the logical manifest and equal-length RS group
format in [joint contract section 6](v2-joint-contract.md#6-fec-packing-and-reassembly).
It is not connected to the v1 or v2 runtime. #21 remains open: queue admission,
timed packing, authenticated bundles, epoch acknowledgements, reassembly,
repair and migration ownership, socket budgets, and performance acceptance
still require implementation and integration.

The codec protects a four-byte manifest prefix, twenty-byte descriptors, and
concatenated fragment bytes together. Descriptors use nonzero increasing
Datagram IDs, with one fragment per ID in a group. A zero-length Datagram is
one descriptor with zero total length and offset. Decoding rejects noncanonical
ordering, duplicates, truncated or trailing bytes, conflicting bounds, and
nonzero padding. No partial systematic-shard delivery is exposed.

`EncodePrefix` greedily fills one group from at most 256 already admitted,
ordered input fragments. It can split the last fragment and returns an explicit
cursor for the caller's unconsumed input. Empty Datagrams consume descriptors;
a context with only 24 logical bytes can carry an empty Datagram but rejects a
nonempty fragment with `ErrNoPayloadCapacity`. All input metadata is checked
before packing, and failure does not advance the cursor. Queue admission,
timers, original ownership/deadlines, flush fences and ID assignment remain
caller responsibilities. Unconsumed input must stay owned until later packing
or terminal cleanup; a cursor never releases that storage by itself.

A deterministic 1000-by-1400-byte stream test verifies reconstructed original
bytes, offsets, descriptor/padding accounting and the documented 1472-byte UDP
capacity model. This proves packing arithmetic only, not network throughput.

Each immutable context fixes data/parity counts and the exact shard length,
including tail groups. Encoding rejects invalid inputs before shard allocation.
Decoding requires at least k authenticated, exact-length indexed shards,
reconstructs with the existing Reed-Solomon dependency, verifies available
parity, and returns fragments only after the whole manifest validates. It
copies input storage; callers own every returned byte and must treat encoded
shards as immutable. Concurrent codec calls are serialized around the RS
backend. This correctness foundation makes no throughput claim.

The API expects callers to authenticate and reserve Session/Peer byte ownership
before calling it, and to enforce acknowledged context and group identity.
It contains no pending maps, ID allocator, timer, goroutine, or network work.
Per-call maxima are bounded by validated parameters; reconstruction scratch
and returned logical storage must both be included in caller reservations.

The literal manifest vector, all 32 presence subsets of RS(3+2), minimum and
256-total-shard contexts, empty payload ownership, exact capacity, malformed
metadata/padding/parity, concurrent calls and canonical round-trip fuzzing are
covered in `group_test.go`. Run:

```sh
go test ./internal/fecv2
go test -race ./internal/fecv2
go test ./internal/fecv2 -run '^$' -fuzz '^FuzzManifest$' -fuzztime=10s -parallel=2
go test ./internal/fecv2 -run '^$' -fuzz '^FuzzPrefixPacking$' -fuzztime=10s -parallel=2
```
