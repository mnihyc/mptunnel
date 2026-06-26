# Docker Lab

The lab is developer/test infrastructure. It is not compiled into the release `mptunnel` binary, and release archives are built from `--bin mptunnel` only.

Lab goals are measurements, not production behavior. The ~256 MiB RAM and ~1 Gbps clean-link targets are used to identify regressions in manual experiments. The production binary does not contain those thresholds, does not self-limit to them, and does not terminate because a lab target is missed.

The lab test goal is to expose whether the adaptive runtime makes the right choices when paths differ sharply in RTT, bandwidth, jitter, loss, and failure state. A good run shows lower interactive latency than bulk-first scheduling, higher download goodput than any weak single path, fast recovery after blackholing a path, and bounded resource use in the lab process. A poor run is kept as evidence for the next scheduler or transport fix; it is never converted into a production hard stop.

Run the heterogeneous ablation lab from the repository root:

```bash
lab/run-heterogeneous-ablation.sh
```

The script builds the product binary on the host, then runs all network mutation inside Docker containers. It does not change host routes, host DNS, host TUN devices, or host `tc` state.

For repeated manual experiments, use the matrix runner:

```bash
EXPERIMENT_PROFILE=standard lab/run-exhaustive-experiments.sh
```

Profiles:

- `smoke`: one short run for harness validation.
- `standard`: two file sizes and two failover timings.
- `exhaustive`: larger file-size, UDP-count, failover-timing, and repeat matrix.
- `custom`: requires `FILE_MIB_MATRIX`, `UDP_COUNT_MATRIX`, `FAILOVER_AFTER_MATRIX`, and `REPEATS`.

The matrix runner writes per-run JSONL files plus `summary.md` and `summary.json` under `lab/results/exhaustive-<timestamp>/`. It is manual lab tooling only and is not referenced by CI, release, package, or normal build workflows.

## Topology

The lab starts three containers:

- `client`: local SOCKS5 ingress and benchmark driver.
- `server`: mptunnel path listener and direct outbound connector.
- `target`: HTTP download target and UDP echo target.

It creates three simultaneous client/server path networks plus a server/target network:

| Network | Client | Server | Target | Profile |
| --- | --- | --- | --- | --- |
| `path_lowlat` | `172.31.10.10` | `172.31.10.20` | `172.31.10.30` | 20 ms, 30 Mbps, near-clean |
| `path_fat` | `172.31.20.10` | `172.31.20.20` | `172.31.20.30` | 180 ms, 300 Mbps, small loss |
| `path_poor` | `172.31.30.10` | `172.31.30.20` | `172.31.30.30` | 420 ms, 8 Mbps, high jitter/loss |
| `target_net` | none | `172.31.40.20` | `172.31.40.30` | server outbound network |

`lab/configure-netem.sh` applies Linux `tc netem` inside each container namespace. The profiles intentionally create a large latency/throughput discrepancy so the scheduler is exercised against the hard case: a low-latency narrow path, a high-throughput high-RTT path, and an unstable poor-Internet path at the same time.

## Cases

The lab writes JSON Lines to `lab/results/heterogeneous-<timestamp>.jsonl`.

It records:

- Raw direct HTTP downloads over each path network.
- mptunnel SOCKS5 HTTP downloads over each single TCP underlay path.
- mptunnel SOCKS5 HTTP download with all TCP underlay paths configured.
- mptunnel SOCKS5 UDP ASSOCIATE probes over each single UDP underlay path.
- mptunnel SOCKS5 UDP ASSOCIATE probes with all UDP underlay paths configured.
- mptunnel TCP multipath download while the high-bandwidth path is blackholed during transfer.

The HTTP cases record wall time, HTTP status, and goodput Mbps. UDP cases record sent/received datagram counts, loss rate, and latency percentiles.

## Controls

Useful environment variables:

- `FILE_MIB`: HTTP test file size in MiB, default `128`.
- `FILE_MIB_MATRIX`: space-separated matrix for `lab/run-exhaustive-experiments.sh`.
- `CURL_TIMEOUT_SECONDS`: per-download timeout, default `120`.
- `UDP_COUNT`: UDP probe datagram count, default `60`.
- `UDP_COUNT_MATRIX`: space-separated UDP-count matrix for `lab/run-exhaustive-experiments.sh`.
- `UDP_PAYLOAD_BYTES`: UDP probe payload size, default `512`.
- `UDP_TIMEOUT_MS`: per-datagram UDP timeout, default `2500`.
- `FAILOVER_AFTER_SECONDS`: seconds before blackholing the high-bandwidth path, default `2`.
- `FAILOVER_AFTER_MATRIX`: space-separated failover timing matrix for `lab/run-exhaustive-experiments.sh`.
- `REPEATS`: repeat count for each matrix point.
- `PATH_PROBE_INTERVAL_MS`: mptunnel client path-probe interval, default `10000`.
- `PATH_PROBE_TIMEOUT_MS`: mptunnel client path-probe timeout, default `5000`.
- `MPTUNNEL_LAB_LOWLAT_RATE`, `MPTUNNEL_LAB_LOWLAT_DELAY`, `MPTUNNEL_LAB_LOWLAT_JITTER`, `MPTUNNEL_LAB_LOWLAT_LOSS`: low-latency path netem values.
- `MPTUNNEL_LAB_FAT_RATE`, `MPTUNNEL_LAB_FAT_DELAY`, `MPTUNNEL_LAB_FAT_JITTER`, `MPTUNNEL_LAB_FAT_LOSS`: high-bandwidth path netem values.
- `MPTUNNEL_LAB_POOR_RATE`, `MPTUNNEL_LAB_POOR_DELAY`, `MPTUNNEL_LAB_POOR_JITTER`, `MPTUNNEL_LAB_POOR_LOSS`: poor-Internet path netem values.
- `MPTUNNEL_LAB_BLACKHOLE_LOSS`: blackhole loss value for failover tests, default `100%`.
- `KEEP_LAB=1`: keep containers running after the script exits.
- `RESULT_FILE`: explicit JSONL output path.
- `RESULT_ROOT`: output directory for matrix runs.
- `CASE_FILTER`: comma-separated case names or shell globs for targeted reruns, for example `mptunnel_tcp_single_*,mptunnel_tcp_multipath_all`.

Manual netem inspection:

```bash
docker compose -f lab/docker-compose.yml exec client /workspace/lab/configure-netem.sh show
```

Manual cleanup:

```bash
docker compose -f lab/docker-compose.yml down --remove-orphans
```

## Interpreting Results

The deterministic benchmark gates in `lab/benchmarks/` are useful manual regression checks, but they are model results. The Docker lab is the minimum comparison surface before claiming real performance improvement because it compares raw paths, single-path mptunnel, multipath mptunnel, UDP behavior, and a forced path failure under the same emulated network.

For release decisions, compare at least:

- mptunnel multipath versus the best raw direct path.
- mptunnel multipath versus each mptunnel single-path case.
- UDP multipath loss/latency versus each UDP single-path case.
- Failover completion and stall time after blackholing `path_fat`.
- Clean-lab aggregate goodput against the manual ~1 Gbps target.
- Lab RSS or equivalent process-memory samples against the manual ~256 MiB target.

When results are poor, keep them. They are product signals, not harness failures.
