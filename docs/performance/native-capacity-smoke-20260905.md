# Native Capacity Smoke, 2026-09-05

Run ID: `mpudp-perf-calibration-smoke3-20260905`.
This is a runner smoke test: one round, 2-second warmup and 15-second steady load.
It is not the three-round, 300-second performance acceptance matrix.

The five native connections start concurrently on different data addresses and
ports. Each router retains its original 100 Mbit/s per-direction HTB and 20 ms
one-way netem configuration. Existing services, VMs and network configuration
were preserved. Raw per-interface classes and qdiscs are in the archive.

| Protocol | Direction | Receiver aggregate Mbit/s | Per-path range Mbit/s | Host mean idle % | Swap pages |
| --- | --- | ---: | ---: | ---: | ---: |
| TCP | Upload | 474.07 | 94.64-95.02 | 3.76 | 0 |
| TCP | Download | 473.82 | 94.52-95.14 | 3.56 | 8 |
| UDP | Upload | 478.59 | 95.31-96.57 | 2.92 | 4 |
| UDP | Download | 481.01 | 95.46-96.73 | 3.37 | 1 |

CPU values are tick-weighted hypervisor samples inside the common steady window.
The host is the shared Xeon E3-1245 v5 (4 cores / 8 threads). It also hosts the
development controller VM and unrelated VMs; the run was not isolated from all
other activity. Capacity is close to the five paths' combined nominal rate, but
processing headroom is not established. Later formal runs must control competing
load and record per-VM/process usage before attributing the low idle time.

UDP offers 100 Mbit/s per path excluding its UDP/IP headers and therefore exceeds
the shaper's full-packet capacity. Its measured packet loss cannot be attributed
to MPUDP. The run makes no product single-flow throughput or latency claim.

## Replay

```bash
python3 scripts/perf/calibrate.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --hypervisor-python /nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3 \
  --output /tmp/mpudp-perf-calibration-smoke3-20260905 \
  --paths 5 --protocols tcp udp --directions upload download \
  --rounds 1 --seconds 15 --warmup 2
```

Use a fresh output directory and the current hypervisor's Python path when
replaying. Each case has five client and five server iperf JSON reports, eight
host JSONL sample streams and `cleanup.json`. All owned units were verified
inactive; the dedicated SSH control sockets were closed after the matrix.

The manifest records the source HEAD and `dirty: true`, because the runner was
under development. Exact Python source hashes are included, so this evidence
does not misrepresent an uncommitted test as final promoted-main acceptance.

[Raw records](native-capacity-smoke-20260905.tar.gz), archive SHA-256:

```text
001ff178d67cbddf8b040767816a4c542e634ef4ddb5a2967d94a5834a3e7743
```

The archive's `SHA256SUMS` checks every extracted record. Files contain counters,
machine/network metadata and source hashes, with no PSK or application payload.
