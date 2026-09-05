# Native Listener Receive Allocations, 2026-09-05

This #19 increment saves two allocations and approximately 64 bytes per native
UDP Listener packet in the isolated receiver fixture. Timing shows no
demonstrated improvement. No network throughput, latency, CPU utilization or
capacity gain is claimed; #19 and the broader #16 work remain open.

Exact `*net.UDPConn` sockets use `ReadFromUDPAddrPort`, then construct one owned
remote address after the existing receive validation and handler gates.
Injected or wrapped connections retain their original `ReadFrom` and address
copy rules. Payload ownership, IPv4-mapped address representation, IPv6 zones,
ReplyPath identity and replies from the receiving socket remain unchanged.
Carrier, batching, queues, workers, wire format and dependencies are unchanged.

## Motivation

The [previous diagnostic profile](sendpath-20260905.md) at source
`bf2ea367f2cc647ffbf6a68df527d50b5fa7ecd8` identifies repeated receive address
allocation. Datagram upload with timing off sampled 1259.31 MB allocated on
the server: Listener `readLoop` accounts for 524.69 MB cumulatively (41.66%),
`cloneAddr` for 136.51 MB flat (10.84%), and `UDPConn.ReadFrom` for 37 MB
cumulatively (2.94%). These figures overlap and must not be added. The native
`ReadFrom` address is immediately cloned by the Listener, motivating this
specific change. This earlier profile does not measure the candidate.

## Isolated Measurements

Go 1.26.4, linux/amd64, KVM, Xeon E3-1245 v5, six logical CPUs and GOMAXPROCS 6.
Other agent builds/tests were paused. Each row reports the median of three
500 ms samples, measured sequentially at baseline then candidate. The same
benchmark harness runs the actual `transport.ServePacketConn`; a separate
sender process transmits synthetic 551-byte payloads. The receiver validates
length and sequence and retains independent packets. Receiver pipe coordination
and callback delivery are included; sender CPU and allocations are excluded.

**One operation is one complete burst.** Divide time, bytes and allocations by
the packet count for per-packet values. All allocation counts are identical
across the three samples; minor B/op variation includes runtime overhead.

| Socket | Packets/op | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Native IPv4 | 1 | 46621 | 47882 | 929 | 864 | 14 | 12 |
| Native IPv4 | 32 | 174500 | 175805 | 29706 | 27658 | 448 | 384 |
| Native dual-stack IPv4 | 1 | 46619 | 46561 | 945 | 880 | 14 | 12 |
| Native dual-stack IPv4 | 32 | 167198 | 169355 | 30218 | 28170 | 448 | 384 |
| Injected IPv4 control | 1 | 45662 | 48065 | 928 | 928 | 14 | 14 |
| Injected IPv4 control | 32 | 174575 | 173656 | 29706 | 29706 | 448 | 448 |

The unchanged injected wrapper is the negative control. Native timings are
mixed and control timings vary, so the allocation result does not establish
a speedup. Earlier exploratory samples also showed a wide control spread:
candidate injected burst32 ranged from 172185 to 296345 ns/op. Both exploratory
logs are retained, but their temporary `go test` binaries were not retained or
hashed. The table above uses a fresh repeat through the retained, hashed
binaries below. No statistical significance is asserted.

## Source And Replay

```text
baseline source: 05abfd5c5e7ea04439a0c97d914d1a1b72931d0d
baseline overlay: identical candidate benchmark file only
candidate source: 51fdd0af1b56ce28203d68c2f7866f4570d5ca0b

baseline measured test binary SHA-256:
fff3289d70af884fb77bb47099921bc83c44266f8c8eaebf7a17e1fd1e577893
candidate measured test binary SHA-256:
17d623aab8c5b0b6db2251e784c0b5a9a5c4b21d573f542799457ba4d70b6dd1
shared listener_native_benchmark_test.go SHA-256:
458024f4624f5e2c2df5d850237521bf46a63418fa99e5ac1e80e84072f7e3e1
```

In separate checkouts of the recorded sources, copy only
`internal/transport/listener_native_benchmark_test.go` from the candidate into
the baseline. Build a retained test binary in each checkout, then run them
sequentially with distinct binary/log paths:

```sh
go test -c -o /tmp/native-listener.test ./internal/transport
sha256sum /tmp/native-listener.test
/tmp/native-listener.test -test.run '^$' \
  -test.bench '^BenchmarkNativeListenerReceive$' -test.benchmem \
  -test.benchtime=500ms -test.count=3
```

IPv4 and IPv6 dual-stack loopback are required for the complete benchmark.
Build paths and toolchain settings can affect binary hashes; the archive
records the exact original build commands and paths. Binaries are retained
locally and identified by hashes, but are not included in the archive.

Correctness gates passed: targeted native/injected receive tests, `go test ./...`,
`go test -race ./...`, `go vet ./...`, and `git diff --check`. Tests compare
address representation against original `ReadFrom` on the same UDP4, UDP6 and
dual-stack socket, and exercise empty datagrams, owned snapshots, reply routes,
oversize error ordering and concurrent Close. Actual IPv6 loopback has no
nonempty zone; zone preservation follows the standard address conversion.

## Evidence

The [curated archive](native-listener-20260905.tar.gz) contains four raw benchmark
logs, `audit.json` with exact source/binary/harness/log hashes, and `SHA256SUMS`.
The final logs bind the measurements to the retained binaries; exploratory logs
are explicitly supplementary. Only synthetic benchmark output and audit
metadata are included. No private lab workspace or payload capture is included.

```text
archive SHA-256:
d7f5126cbecba6bbc67ffd4045faae762163c18b763ddfb33f5c72280adf2bd2
```
