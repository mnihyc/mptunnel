# Docker Lab

The lab is developer/test infrastructure. It is not compiled into the release `mptunnel` binary, and release archives are built from `--bin mptunnel` only.

Lab goals are measurements, not production behavior. The ~256 MiB RAM and ~1 Gbps clean-link targets are used to identify regressions in manual experiments. The production binary does not contain those thresholds, does not self-limit to them, and does not terminate because a lab target is missed.

The lab test goal is to expose whether the adaptive runtime makes the right choices when paths differ sharply in RTT, bandwidth, jitter, loss, and failure state. A good run shows lower interactive latency than bulk-first scheduling, higher download goodput than any weak single path, fast recovery after blackholing a path, and bounded resource use in the lab process. A poor run is kept as evidence for the next scheduler or transport fix; it is never converted into a production hard stop.

Run the heterogeneous ablation lab from the repository root:

```bash
lab/run-heterogeneous-ablation.sh
```

The script builds the product binary on the host, then runs all network mutation inside Docker containers. It does not change host routes, host DNS, host TUN devices, or host `tc` state. Product client launches are intentionally user-like by default: they pass the secret, SOCKS5 listen endpoint, and TCP/UDP path endpoints only. They do not inject path metadata hints, probe timing, resource limits, or other tuning flags unless an explicit lab override is set.

For repeated manual experiments, use the matrix runner:

```bash
EXPERIMENT_PROFILE=standard lab/run-exhaustive-experiments.sh
```

Profiles:

- `smoke`: one short run for harness validation.
- `standard`: sustained-duration, bulk-concurrency, and failover-timing matrix.
- `exhaustive`: longer duration, higher concurrency, failover-timing, and repeat matrix.
- `custom`: requires `LOAD_DURATION_MATRIX`, `BULK_CONNECTIONS_MATRIX`, `FAILOVER_AFTER_MATRIX`, and `REPEATS`.

The matrix runner writes per-run JSONL files plus `summary.md` and `summary.json` under `lab/results/exhaustive-<timestamp>/`. It is manual lab tooling only and is not referenced by CI, release, package, or normal build workflows.

## Topology

The lab starts three containers:

- `client`: local SOCKS5 ingress and benchmark driver.
- `server`: mptunnel path listener and direct outbound connector.
- `target`: HTTP download target and UDP echo target.

Each container is capped at two CPUs in Docker Compose to approximate a modest VPS instead of an unconstrained host.

It creates four simultaneous client/server path networks plus a server/target network:

| Network | Client | Server | Target | Profile |
| --- | --- | --- | --- | --- |
| `path_lowlat` | `172.31.10.10` | `172.31.10.20` | `172.31.10.30` | 20 ms, 80 Mbps, 1% loss |
| `path_balanced` | `172.31.15.10` | `172.31.15.20` | `172.31.15.30` | 80 ms, 200 Mbps, 1% loss |
| `path_fat` | `172.31.20.10` | `172.31.20.20` | `172.31.20.30` | 180 ms, 500 Mbps, 1% loss |
| `path_poor` | `172.31.30.10` | `172.31.30.20` | `172.31.30.30` | 420 ms, 50 Mbps, 10% loss, high jitter |
| `target_net` | none | `172.31.40.20` | `172.31.40.30` | server outbound network |

`lab/configure-netem.sh` applies Linux `tc netem` inside each container namespace. The real-profile suite intentionally keeps low-latency, balanced, high-throughput high-RTT, and unstable poor-Internet paths available at the same time. These profiles emulate plausible Internet paths and stay separate from the controlled 2^3 ablation matrix.

## Cases

The lab writes JSON Lines to `lab/results/heterogeneous-<timestamp>.jsonl`.

It records:

- Raw direct HTTP downloads over each path network.
- Plain VMess-over-TCP, Hysteria2-over-UDP, and kernel MPTCP baselines when the external tools/kernel support are available in the lab containers.
- mptunnel SOCKS5 HTTP downloads over each single TCP underlay path.
- mptunnel SOCKS5 HTTP download with all TCP underlay paths configured.
- mptunnel SOCKS5 sustained TCP uploads over single TCP, single UDP reliable-stream, mixed, multipath, and TUN-mode underlay combinations.
- mptunnel TUN L4 HTTP downloads over TCP, UDP reliable-stream, and mixed underlay paths.
- mptunnel SOCKS5 UDP ASSOCIATE probes over each single UDP underlay path.
- mptunnel SOCKS5 UDP ASSOCIATE probes with all UDP underlay paths configured.
- mptunnel mixed workload while one path is saturated by background `iperf3` traffic.
- mptunnel mixed workload while path states randomly flap between normal, spiked, and blackholed profiles.
- mptunnel TCP multipath download while the high-bandwidth path is blackholed during transfer.
- mptunnel mixed-workload ideal comparisons where one selected path is forced to 0% loss.
- mptunnel controlled matrix download and upload cases over one TCP+UDP path where bandwidth, latency, and loss each toggle between good and poor values.

Download cases use a sparse 1 GiB HTTP object by default and run for a fixed duration, so a valid measurement is usually a partial HTTP body at the end of the test window rather than a completed small file. Upload cases use persistent TCP streams to a lab sink for the same fixed duration, so they measure client-to-server sender behavior instead of repeated short transfers. Mixed cases run several workloads for the same fixed window: a sustained large-object download, repeated small-object HTTP requests for browsing latency, one persistent TCP echo stream with periodic payloads for SSH-like interaction, and duration-driven UDP datagrams for realtime traffic. The harness should not finish early just because one request completed.

The HTTP, upload, and mixed cases record wall time, goodput Mbps, startup timing, max transfer gap, recovery gap, and one-second interval goodput samples during sustained load windows. UDP cases record attempted/received datagram counts, loss rate, and latency percentiles over the same duration-driven window. JSONL rows with `status:"loss"` are retained as valid lossy-network measurements; rows with `status:"fail"` indicate a failed experiment.

## Controls

Useful environment variables:

- `MPTUNNEL_LAB_SECRET`: optional UUID or 32+ byte shared secret for reproducible lab runs. When unset, the lab generates a fresh UUID secret for that run. The release binary itself has no default secret.
- `MPTUNNEL_LAB_OBJECT_MIB`: sparse large HTTP object size in MiB, default `1024`. This is backing data for sustained file-download tests, not the test-length control.
- `MPTUNNEL_LAB_SMALL_OBJECT_KIB`: small HTTP object size in KiB for browsing probes, default `32`.
- `MPTUNNEL_LAB_LARGE_HTTP_PATH`: large-object URL path, default `/large.bin`.
- `MPTUNNEL_LAB_SMALL_HTTP_PATH`: small-object URL path, default `/small.bin`.
- `MPTUNNEL_LAB_LOAD_DURATION_SECONDS`: sustained workload duration for download and mixed probes, default `30`.
- `LOAD_DURATION_MATRIX`: space-separated sustained workload duration matrix for `lab/run-exhaustive-experiments.sh`.
- `MPTUNNEL_LAB_BULK_CONNECTIONS`: parallel pure-download or pure-upload connections for capacity tests, default `2`.
- `BULK_CONNECTIONS_MATRIX`: space-separated bulk connection-count matrix for `lab/run-exhaustive-experiments.sh`.
- `CURL_TIMEOUT_SECONDS`: per-download timeout, default `120`.
- `UDP_PAYLOAD_BYTES`: UDP probe payload size, default `512`.
- `UDP_TIMEOUT_MS`: per-datagram UDP timeout, default `2500`.
- `FAILOVER_AFTER_SECONDS`: seconds before blackholing the high-bandwidth path, default `2`.
- `FAILOVER_AFTER_MATRIX`: space-separated failover timing matrix for `lab/run-exhaustive-experiments.sh`.
- `REPEATS`: repeat count for each matrix point.
- `MPTUNNEL_LAB_DIAGNOSTICS=1`: build an optimized lab binary with the `lab-diagnostics` feature for extra experiment-only instrumentation when that feature is used. Release packaging and CI release jobs do not enable this feature.
- `MPTUNNEL_LAB_LOG`: log level passed to lab-launched mptunnel processes, default `info`.
- `PATH_PROBE_INTERVAL_MS`: optional diagnostic override for the mptunnel client path-probe interval. Unset by default so product launches use built-in defaults.
- `PATH_PROBE_TIMEOUT_MS`: optional diagnostic override for the mptunnel client path-probe timeout. Unset by default so product launches use built-in defaults.
- `MPTUNNEL_LAB_USE_PATH_HINTS=1`: optional diagnostic override that adds RTT/rate/capability query hints to path URIs. Unset by default so product launches use endpoint paths only.
- `MPTUNNEL_LAB_LOWLAT_RATE`, `MPTUNNEL_LAB_LOWLAT_DELAY`, `MPTUNNEL_LAB_LOWLAT_JITTER`, `MPTUNNEL_LAB_LOWLAT_LOSS`: low-latency path netem values. The default loss is `1.00%`.
- `MPTUNNEL_LAB_BALANCED_RATE`, `MPTUNNEL_LAB_BALANCED_DELAY`, `MPTUNNEL_LAB_BALANCED_JITTER`, `MPTUNNEL_LAB_BALANCED_LOSS`: balanced daily-use path netem values. The default loss is `1.00%`.
- `MPTUNNEL_LAB_FAT_RATE`, `MPTUNNEL_LAB_FAT_DELAY`, `MPTUNNEL_LAB_FAT_JITTER`, `MPTUNNEL_LAB_FAT_LOSS`: high-bandwidth path netem values. The default loss is `1.00%`.
- `MPTUNNEL_LAB_POOR_RATE`, `MPTUNNEL_LAB_POOR_DELAY`, `MPTUNNEL_LAB_POOR_JITTER`, `MPTUNNEL_LAB_POOR_LOSS`: poor-Internet path netem values. The default loss is `10.00%`.
- `MPTUNNEL_LAB_IDEAL_LOSS`: loss value for ideal comparison cases, default `0.00%`.
- `MPTUNNEL_LAB_MATRIX_GOOD_RATE`, `MPTUNNEL_LAB_MATRIX_POOR_RATE`: controlled matrix bandwidth values, default `500mbit` and `50mbit`.
- `MPTUNNEL_LAB_MATRIX_GOOD_DELAY`, `MPTUNNEL_LAB_MATRIX_POOR_DELAY`: controlled matrix latency values, default `50ms` and `250ms`.
- `MPTUNNEL_LAB_MATRIX_GOOD_JITTER`, `MPTUNNEL_LAB_MATRIX_POOR_JITTER`: controlled matrix jitter values, default `5ms` and `60ms`.
- `MPTUNNEL_LAB_MATRIX_GOOD_LOSS`, `MPTUNNEL_LAB_MATRIX_POOR_LOSS`: controlled matrix loss values, default `1.00%` and `15.00%`.
- `MPTUNNEL_LAB_BLACKHOLE_LOSS`: blackhole loss value for failover tests, default `100%`.
- `MPTUNNEL_LAB_SATURATE_PROTOCOL`: background saturation protocol, `udp` by default or `tcp`.
- `MPTUNNEL_LAB_SATURATE_LOWLAT_BANDWIDTH`, `MPTUNNEL_LAB_SATURATE_BALANCED_BANDWIDTH`, `MPTUNNEL_LAB_SATURATE_FAT_BANDWIDTH`, `MPTUNNEL_LAB_SATURATE_POOR_BANDWIDTH`: bidirectional background `iperf3` rates for saturated-link cases.
- `MPTUNNEL_LAB_FLAP_MIN_SECONDS`, `MPTUNNEL_LAB_FLAP_MAX_SECONDS`, `MPTUNNEL_LAB_FLAP_MODES`: randomized link-flapping cadence and mode list for unstable-link cases.
- `KEEP_LAB=1`: keep containers running after the script exits.
- `RESULT_FILE`: explicit JSONL output path.
- `RESULT_ROOT`: output directory for matrix runs.
- `CASE_FILTER`: comma-separated case names or shell globs for targeted reruns, for example `mptunnel_tcp_single_*,mptunnel_tcp_multipath_all`.
- `MPTUNNEL_LAB_DIAGNOSTICS=1 MPTUNNEL_LAB_DIAG=1`: build the optimized `lab-diagnostics` binary and emit internal diagnostic lines into the client/server `/tmp/mptunnel-*.log` files, including reliable-stream path open attempts/successes and UDP stream congestion state. Successful download rows also keep bounded client/server log tails when this is enabled. Use this only for investigation; release comparisons should run without diagnostic instrumentation.
- `MPTUNNEL_LAB_DIAGNOSTICS=1 MPTUNNEL_LAB_PERF=1`: build the optimized `lab-diagnostics` binary and emit interval/cumulative per-component timing lines prefixed with `mptunnel_lab_perf`. `MPTUNNEL_LAB_PERF_INTERVAL_MS` controls the flush interval, default `1000`. `MPTUNNEL_LAB_LOG_TAIL_BYTES` and `MPTUNNEL_LAB_LOG_TAIL_LINES` control retained diagnostic log tails.

For a repeatable component/process profiling workflow, use `lab/run-perf-diagnostics.sh` and see `docs/PERF.md`.

Matrix case names use `mptunnel_matrix_bw_{good,poor}_lat_{good,poor}_loss_{good,poor}`. The controlled matrix applies those values only to `path_lowlat` and starts one TCP plus one UDP underlay endpoint on that path, so the eight cells isolate bandwidth, latency, and loss without changing topology or path count. The default loss axis is intentionally harsh: `loss_good` is a realistic non-perfect 1% path and `loss_poor` is a severe 15% path. Ideal 0% loss remains available only in separate ideal comparison cases.

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
- mptunnel multipath versus each mptunnel single-path TCP, UDP reliable-stream, and mixed-carrier case.
- UDP multipath loss/latency versus each UDP single-path case and best-effort UDP-over-TCP case.
- TUN TCP, TUN UDP reliable-stream, and TUN mixed underlay cases.
- Failover completion and stall time after blackholing `path_fat`.
- Sustained interval goodput, first-body time, max read gap, UDP p95, and SSH-like echo success gap during the same fixed-duration window.
- Clean-lab aggregate goodput against the manual ~1 Gbps target.
- Lab RSS or equivalent process-memory samples against the manual ~256 MiB target.

When results are poor, keep them. They are product signals, not harness failures.
