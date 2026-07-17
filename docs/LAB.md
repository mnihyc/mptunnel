# Docker lab

The lab is manual developer infrastructure. It is not compiled into the release
binary, included in packages, or run by CI. Its thresholds and topology never
become production policy.

Run from the repository root:

```bash
lab/run-heterogeneous-ablation.sh
```

The runner builds the product on the host and confines `tc`, routes, interface
changes, blackholes, and TUN work to Docker namespaces. It writes JSONL and
per-case artifacts to a unique invocation directory under `lab/results/`,
which is ignored generated evidence. Named `RESULT_DIR` values remain
available for deliberate cohorts. One lab lock prevents concurrent Compose
runs from corrupting shared topology.

Each invocation retains `run-manifest.json`, an anonymized effective Compose
config, redacted product configs with SHA-256 checksums, and before/after qdisc
and interface-counter snapshots. The case-named client config is the path and
resource contract for its JSON row; the qdisc snapshot is the effective
underlay contract. Publish these side artifacts with any benchmark table.

## Evidence cohorts

Do not combine these cohorts in one performance claim:

- **Shaped**: Docker paths with explicit rate, delay, jitter, and loss.
- **Unconstrained**: Docker paths with netem cleared. This is not the public
  Internet and is not a wire-speed guarantee.
- **Fault**: blackhole, latency-spike, saturation, and deterministic flapping
  experiments.
- **Protocol family**: TCP-only, QUIC-only, and mixed TCP+QUIC reliable streams
  exercise distinct carrier controllers.
- **Work direction**: upload and download have different ownership and host I/O
  paths.
- **Workload**: reliable bulk, interactive/echo, realtime datagram, composite
  mixed workload, and TUN are separate evidence cohorts.
- **Diagnostic**: `lab-diagnostics` and verbose logs establish causality but are
  not release-comparable throughput results.
- **Real Internet**: requires separately recorded endpoints, routes, host
  inventory, time window, and runner. Record it as not run when those inputs do
  not exist.

Protocol-v1/pre-v2 results are historical references only. Current performance
claims require fresh protocol-v2 matched rows.

## Topology

The default Compose topology has client, server, and target containers. The
client exposes local proxy/TUN ingress; the server accepts MPP paths and opens
the target; the target provides HTTP, receiver-confirming TCP upload, TCP echo,
and UDP echo services.

Five simultaneous client/server path networks model different conditions:

| Path | Default profile |
| --- | --- |
| `lowlat` | 20 ms, 80 Mbps, 1% loss |
| `balanced` | 80 ms, 200 Mbps, 1% loss |
| `mildloss` | 160 ms, 100 Mbps, 0.1% loss |
| `fat` | 180 ms, 500 Mbps, 1% loss |
| `poor` | 420 ms, 50 Mbps, 10% loss, high jitter |

`lab/configure-netem.sh` owns these profiles. Equal-path and controlled matrix
variants override them explicitly. The shaped profiles emulate plausible
conditions; they are not measurements of any named ISP or route.

## Selecting cases

`CASE_FILTER` accepts comma-separated shell globs. Keep a focused run bounded:

```bash
CASE_FILTER='direct_cross_continent_high_bandwidth,mptunnel_tcp_single_cross_continent_high_bandwidth,mptunnel_tcp_multipath_all' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=20 \
BUILD_LAB_IMAGES=0 \
lab/run-heterogeneous-ablation.sh
```

Useful families include:

- `direct_*` raw target controls;
- `baseline_vmess_*`, `baseline_hysteria2_*`, and `baseline_mptcp_*` external
  baselines when their dependency/kernel support is available;
- `mptunnel_tcp_single_*` and `mptunnel_tcp_multipath_*`;
- `mptunnel_udp_stream_single_*` and
  `mptunnel_udp_stream_multipath_*`;
- `mptunnel_reliable_mixed_single_*` and
  `mptunnel_reliable_mixed_multipath_*`;
- `mptunnel_mixed_*` composite browsing/bulk/echo/datagram workloads;
- `mptunnel_tun_*` packet-device paths;
- `*_failover_blackhole_*`, `*_latency_spike_*`, `*_saturate_*`, and
  `*_flapping_links` fault cases; and
- `mptunnel_matrix_*` controlled bandwidth/latency/loss ablations.

The runner source is the authoritative case list. Case names are public lab
identifiers, not runtime algorithms or diagnostic event names.

### Windows executable under Wine

The runner can execute the Windows GNU client under Wine inside the same
Docker network namespace as the native client. This preserves the netem,
probe, receiver-accounting, and container-telemetry contract while exercising
the portable Windows socket path. Build the opt-in client image and select the
runtime in one invocation:

```bash
MPTUNNEL_LAB_CLIENT_RUNTIME=wine \
MPTUNNEL_LAB_INSTALL_WINE=1 \
BUILD_LAB_IMAGES=1 \
CASE_FILTER='mptunnel_tcp_single_cross_continent_high_bandwidth,mptunnel_tcp_multipath_equal_fat' \
lab/run-heterogeneous-ablation.sh
```

Every row records the client runtime/version, client and server targets, and
both binary hashes. Wine cases support proxy-based workloads; TUN cases remain
native-only. Wine proves behavior of the Windows executable and portable
fallback, not Wintun, native Windows kernel scheduling, or `SIO_TCP_INFO`.

## Representative release matrix

A bounded release assessment should answer these ten questions before adding
more cases:

1. Does one TCP path match its direct/VMess control for download?
2. Does one TCP path match its control for upload?
3. Does one QUIC path remain competitive with Hysteria2 for download?
4. Does one QUIC path remain competitive with Hysteria2 for upload?
5. Does TCP multipath aggregate above its matched best single path in download?
6. Does TCP multipath aggregate above its matched best single path in upload?
7. Does QUIC multipath aggregate in both directions without excessive traffic?
8. Does mixed TCP+QUIC improve or preserve reliable bulk in both directions?
9. Does composite bulk plus browsing/echo/datagram work preserve latency and
   loss under load?
10. Does a blackholed material path fail over quickly without a long stall,
    duplicate-range accounting error, or unbounded overhead?

Use 100-500 Mbps profiles as the main performance priority. Very-low-rate cases
can expose correctness bugs, but optimizing 1 to 2 Mbps is not a substitute for
fixing a collapse on a normal broadband or high-BDP link.

An experiment may use multiple runner rows to answer one question because the
single-path and external baselines must be adjacent and same-condition.

## Matching rules

Compare rows only when all material inputs match:

- source commit and build profile;
- protocol version and enabled features;
- case topology, netem values, direction, object/load duration, concurrency,
  and failover timing;
- security and diagnostics state;
- container CPU/memory limits and host epoch; and
- upload accounting schema and completion status.

Run the control and candidate adjacently. Repeat a pair when host drift or
random netem loss makes the direction ambiguous. Do not average unmatched
topologies to make a result appear stable.

Record application-flow and carrier-connection counts. Parallel direct or
TCP-proxy downloads normally create independent native TCP congestion states,
while several logical MPP streams may share one carrier. The representative
matrix may keep realistic application concurrency, but it is not a per-carrier
congestion-control comparison when those counts differ. Use a separate
single-flow control before attributing such a difference to MPP scheduling.

For flapping A/B comparisons, preserve `MPTUNNEL_LAB_FLAP_SEED`, ordered modes,
hold bounds, and netem overrides. The seed fixes the intended schedule, not
packet-level random loss, Docker command latency, or application progress.
Require a complete trace and compare actual transition offsets.

## Download accounting

Download goodput uses bytes delivered to the client application during the
fixed measurement window. The large object is intentionally bigger than the
window, so partial HTTP bodies are valid measurements. Record:

- delivered bytes and goodput;
- first response/body time;
- interval goodput;
- maximum read/progress gap;
- recovery gap relative to an explicit fault trigger; and
- client/server/target traffic counters around the case.

Local server reads or carrier writes are diagnostics, not delivered download
bytes.

## Upload accounting

Exact upload ratios require receiver evidence. Current observer rows use
`upload_accounting_source: "target_sink_observer"` and
`upload_accounting_exact: true`. The target holds per-connection totals in
memory, the runner quiesces it, waits for handlers, and reads one finalized
snapshot.

The in-band target ACK stream is a separately labeled lower bound and supplies
delivery intervals/gaps. Client `send()` acceptance is never end-to-end
delivery. Keep these categories separate:

- exact finalized receiver total: eligible for medians and ratios;
- positive incomplete receiver progress: `status: "loss"`, lower-bound only;
- zero receiver progress or invalid observer: `status: "fail"`;
- legacy sender-only rows: unverified historical evidence.

Unexpected target connections make an observer snapshot non-authoritative
because the target does not have MPP stream IDs/offsets with which to
deduplicate retry prefixes.

## Traffic accounting

The runner snapshots endpoint interfaces before and after each case. Interpret
wire overhead against receiver-confirmed unique MPP bytes:

```text
overhead = native retransmission
         + exact MPP range reinjection
         + control/auth/ACK/flow-control frames
         + bounded path proof or capacity measurement
         + encryption/framing/IP/transport headers
```

Do not infer duplicate MPP delivery from aggregate interface bytes alone.
Use exact range diagnostics to distinguish reinjection from new data and native
transport counters to identify TCP/QUIC recovery where available.

Large unexplained endpoint imbalance, traffic growth without delivery growth,
or persistent reinjection in a clean case is a release blocker. Short bounded
control/proof traffic is not aggregation.

Ordinary reinjection should remain within the cumulative allowance funded by
startup credit and unique MPP bytes acknowledged by Data ACK. A critical
path-failure, persistent authoritative Data ACK gap, or
bounded live-tail event may exceed the remaining cumulative allowance, but the
row must show a bounded event quantum, an exact retained unacknowledged range,
no overlapping queued copy, the required alternate output, and no repeated
over-budget stream. Those bytes remain charged against later optional
reinjection.

## Path and scheduling evidence

Protocol-v2 diagnostics should be interpreted at the RFC boundary:

- stream open proves neutral attachment, not path rank;
- `PATH_STATUS` sequence and `Available`/`Backup` value prove peer preference,
  not local health;
- local path-instance state proves reachability/lifecycle;
- snapshots prove metric provenance and demand at decision time;
- data commits prove exact original or reinjected MPP ranges in one stream
  direction;
- request commits correlate physical path instance with `attachment_id`;
- response new-data commits correlate physical path instance, output
  incarnation, and the revalidated response-model generation;
- `STREAM_ACK` proves MPP delivery and releases shared flight in that
  direction, but grants no new offset;
- `STREAM_MAX_DATA` grants offsets in that direction, but proves no delivery;
- additional response-output ownership beyond one bounded startup flight
  requires durable, unambiguous Data ACK coverage of original transmissions;
  the output carrying the contiguous frontier remains governed by shared flow
  control and native carrier credit; and
- TCP receipt/socket evidence and QUIC packet-ACK evidence explain only their
  respective carrier proof state and cannot substitute for Data ACK or for each
  other.

Upload and download must be correlated as independent DSN, Data ACK, and window
spaces even when they use the same `stream_id`. Equal numeric offsets in opposite
directions are unrelated ranges.

Do not require internal module or superseded event names in the lab contract.
Diagnostic schemas may evolve with code; protocol identities,
ranges, directions, and timestamps are the durable correlation keys.

## Diagnostics

Enable optimized lab-only diagnostics explicitly:

```bash
CASE_FILTER='mptunnel_tcp_multipath_all' \
MPTUNNEL_LAB_DIAGNOSTICS=1 \
MPTUNNEL_LAB_PERF=1 \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=20 \
lab/run-heterogeneous-ablation.sh
```

Diagnostics establish a causal sequence and component bottleneck. Re-run the
same matched cases without `lab-diagnostics`, verbose logs, or frequent
management sampling before making a throughput claim. See `docs/PERF.md`.

## Result interpretation

`status: "ok"` means the case contract completed. `status: "loss"` is valid
lossy/incomplete evidence but not an exact throughput ratio. `status: "fail"`
means setup, contract, or zero-progress failure.

Report at least:

- mptunnel single path versus the same-path direct/external baseline;
- multipath versus the matched best mptunnel single path;
- aggregate goodput and per-path material-byte shares;
- first-body/first-delivery and maximum progress gap;
- fault-trigger-to-survivor recovery gap;
- UDP delivery/loss and p50/p95 latency;
- receiver-confirmed upload status;
- endpoint traffic overhead; and
- CPU and peak memory samples.

Nominal path-rate sums are context, not measured available capacity. External
baseline absence must be reported as unavailable, not silently replaced by a
different protocol or old result.

## Current protocol-v2 assessment (2026-07-16)

The latest bounded release matrix is
`lab/results/protocol-v2-layer-ownership-release18-r1-20260716/`. Its current
code and wire contract are protocol v2; older `iteration*` directories are not
part of this assessment.

The matrix proves that the product basically carries traffic and that multiple
carriers can contribute. Equal-fat TCP, QUIC, and mixed download rows reached
158.942, 216.568, and 170.341 Mbps respectively. Their upload rows delivered
106.351, 202.582, and 271.143 Mbps of receiver-observed progress, but those
upload results ended with incomplete positive accounting and are lower bounds,
not exact ratios. The mixed blackhole row resumed reliable bulk with a
0.169-second maximum read gap; its interactive survivor gap was 0.709 seconds.
One of four unreliable datagrams was lost, so the row is not an ideal realtime
failover result.

The two-flow single-TCP row in that short matrix was an outlier and must not be
used as a stable carrier comparison. The separate one-flow matched control in
`lab/results/protocol-v2-single-carrier-matched8-r1-20260716/` measured direct
TCP 17.236 Mbps, VMess TCP 38.180 Mbps, MPP-over-TCP 18.873 Mbps, and
MPP-over-QUIC 66.094 Mbps. Repeated 18-second protocol-v2 TCP rows reached about
110-122 Mbps. This is functional and not MPP-window or CPU limited, but the TCP
startup/ramp is still weaker than the adjacent proxy control.

The retained 300.517/300.619 Mbps protocol-v1 TCP rows are not a reproducible
source-code guard. The checksum-matched historical `7fa7789` executable reached
110.321 Mbps when rerun in the current unseeded 1% loss epoch, while its adjacent
then-current `709d15f` executable reached 121.009 Mbps. The latest protocol-v2
row reached 122.312 Mbps in the same current environment. This A/B rules out the
source migration as the cause of the full historical spread; it does not make
the present TCP result ideal. Random netem loss and the native TCP congestion
ramp remain material experimental variables, so future historical comparison
must include an adjacent old executable or a packet-loss schedule that is
actually reproducible.

Rust tests, strict developer gates, Linux release packaging, Linux musl target
checks, Windows GNU cross-build, and Wine CLI/config startup pass. macOS/MSVC
checks stop at missing platform SDK headers and Android stops at the absent NDK;
Wine cannot exercise Wintun. The current matrix has direct, VMess, and
Hysteria2 controls but no same-condition MPTCP result, and no real-Internet
cohort has been run. The resulting verdict is a controlled Linux/Windows-proxy
release candidate, not a general availability release for all documented
platforms and network conditions.

## Historical pre-v2 results

Directories named `lab/results/iteration*` contain protocol-v1/role-based
experiments. Some captured strong aggregation and useful negative findings, so
they remain regression references. They must always be labeled pre-v2 and
cannot establish current release behavior.

Tracked historical comparisons belong in `docs/BENCHMARKS.md` and the result
manifests. When comparing a current row to one of those records, show both the
matched current control ratio and the historical absolute value. A protocol
change does not justify silently losing the earlier capability; a mismatch
starts a root-cause investigation.

## Iteration rule

Run one bounded experiment for one explicit hypothesis. A poor row should
identify whether the violated boundary is MPP range ownership, path usage,
metric provenance, queue/flight accounting, native carrier control, local I/O,
or lab measurement. Make the smallest general fix at that owner, rerun the
matched case, then run the representative guards.

Do not keep a long run alive after the evidence answers the hypothesis. Do not
repeat a failing case without a changed model, code path, or measurement
question.

Summarize saved results with:

```bash
python3 lab/summarize-results.py lab/results/<result>.jsonl
python3 lab/summarize-results.py --format json lab/results/<result>.jsonl
```
