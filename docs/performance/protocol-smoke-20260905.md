# Protocol Probe Smoke, 2026-09-05

Run ID: `mpudp-probe-smoke-19f1fdb`. All 26 cases completed with one second of
warmup, five seconds of steady measurement, and one round. This validates the
five-protocol runner on the existing seven-VM lab, not formal performance
acceptance. The source checkout was clean:

```text
source: 19f1fdb25c0374b978980d24efc7980c86ae44a9
binary SHA-256: 34c91179fc02dd9fa6848fa08e2dd8b46058d5b8b79820d7171c5cfe62ed60de
```

Each application message is 1400 bytes, of which 1360 receiver-validated unique
body bytes count toward throughput. MPUDP uses RS(3+2), a fixed 1200-byte complete
UDP payload budget, 4096-entry queues and 8192 pending blocks. KCP uses no KCP FEC,
MTU 1400, window 1024, nodelay `(1,10,2,1)` and delayed ACKs. MPUDP diagnostics and
profiles are off. Native UDP and MPUDP Datagram are offered without rate control;
their loss and latency include overload. MPUDP flows each use one Session across
all selected Carriers. Direct KCP remains a probe dependency, not a product mode.

| Protocol | Candidate paths | Actual layout | Upload Mbit/s | Download Mbit/s |
| --- | ---: | --- | ---: | ---: |
| TCP | 1 | One native flow | 92.804 | 92.800 |
| TCP | 5 | One native flow, one path | 92.121 | 92.817 |
| TCP | 5 | Five independent native flows | 449.520 | 456.011 |
| UDP | 1 | One native flow | 94.310 | 94.301 |
| UDP | 5 | One native flow, one path | 94.245 | 76.312 |
| UDP | 5 | Five independent native flows | 412.280 | 457.132 |
| KCP | 1 | One native flow | 88.707 | 72.661 |
| KCP | 5 | One native flow, one path | 84.864 | 88.724 |
| KCP | 5 | Five independent native flows | 281.043 | 294.004 |
| MPUDP Datagram | 1 | One Session, one path | 16.083 | 13.254 |
| MPUDP Datagram | 5 | One Session, five paths | 55.410 | 58.537 |
| KCP-over-MPUDP | 1 | One Session, one path | 22.086 | 24.319 |
| KCP-over-MPUDP | 5 | One Session, five paths | 32.005 | 28.688 |

The runner confirmed configured Carrier counts on both MPUDP endpoints. For the
five-path MPUDP cases, echo P95/P99 is 72/83 ms upload and 70/73 ms download; for
KCP-over-MPUDP it is 413/437 ms upload and 338/361 ms download. Each receiver
schedules 25 echo opportunities inside its own business connection. All 25 met
the one-second deadline in these four cases. Unanswered probes in other cases
remain in quantile accounting; null quantiles must not be read as zero latency.
With only five steady seconds, the worst-five-second rate equals the full-window
rate and does not establish sustained stability.

## Measurement Limits

- Five-path native aggregate rates add independently measured five-second
  windows. Their common overlap was 4.830-4.990 seconds. They are approximate
  concurrent smoke results, not a frozen capacity ceiling or a single-flow rate.
- The shared hypervisor is a Xeon E3-1245 v5, four cores/eight threads. Mean idle
  was 0.78-1.91% for five-path TCP/UDP and 5.37-5.67% for parallel native KCP.
  MPUDP five-path idle was 8.98-9.05%, and KCP-over-MPUDP was 7.49-9.56%.
  Processing headroom and freedom from competing host activity are unproven.
- `host_diagnostics=basic` collected only the initial link/qdisc/socket snapshot
  on all 208 host streams because the sampler was stopped before its planned
  final sample. Per-second CPU/kernel counters exist, but this archive cannot
  establish a before/after qdisc, link or socket drop delta. The runner needs an
  explicit final snapshot before formal measurement.
- One TCP parallel-download sender recorded a read error and a write error.
  The receiver summaries still agree on both hosts; this does not make
  the run error-free. Per-host JSONL retains those diagnostic events.
- This predates the FEC retained-state gauges in `01bc82c`; it has no completed
  capacity-eviction or pending high-water measurements. It also lacks KCP ACK
  correlation and listener per-remote path rows.
- Shaper accounting still requires calibration. No change to the epic's
  throughput thresholds, FEC requirements or fragmentation prohibition follows
  from these measurements. All formal acceptance flags remain false.

## Replay And Evidence

Build from the exact source and use a new output directory:

```bash
go build -C integration/perf -trimpath \
  -ldflags '-X main.sourceSHA=19f1fdb25c0374b978980d24efc7980c86ae44a9' \
  -o /tmp/mpudp-perfprobe-19f1fdb ./cmd/perfprobe
python3 scripts/perf/run-probe.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --hypervisor-python /nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3 \
  --binary /tmp/mpudp-perfprobe-19f1fdb \
  --source-sha 19f1fdb25c0374b978980d24efc7980c86ae44a9 \
  --psk-file /private/mpudp-perf.psk \
  --output /tmp/mpudp-protocol-smoke-replay \
  --paths 1 5 --payloads 1400 --rounds 1 --seconds 5 --warmup 1 \
  --host-diagnostics basic
```

The hypervisor Python path and SSH configuration are local lab dependencies.
Use a private raw PSK file and do not place it in the output directory.

The archive preserves 699 checksummed records: manifests, per-second samples,
both hosts' summaries, machine/network metadata and cleanup proof. All 308 owned
transient units ended inactive with MainPID 0; both private remote workspaces
were removed. Existing VMs, bridges and services were preserved. Host records
include lab addresses but no PSK, private keys or business payload.

[Raw records](protocol-smoke-20260905.tar.gz), archive SHA-256:

```text
c8a8d0c939168fc6c4b119c857857afdbfcfc3702b4a2799209ab9caf32dfc9e
```

After extraction, run `sha256sum --check SHA256SUMS` inside the run directory.
