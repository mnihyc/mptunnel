# Docker lab

The lab is developer performance infrastructure. It is not compiled into the
release binary or included in packages. Generic hosted CI runs only its
contract/unit tests and deterministic benchmark-crate tests; it does not run
the Docker topology. Its thresholds and topology never become production
policy.

## F0 evidence contracts

The hosted release-quality gate validates the versioned impact registry, runs
the declaration, immutable-ledger, evidence-bundle, and host-snapshot unit
tests, and runs deterministic benchmark and observation-trace replay tests:

```bash
python3 lab/validate_performance_declaration.py --check-registry
python3 -m unittest discover --start-directory lab --pattern 'test_*.py'
cargo test --locked --manifest-path lab/benchmarks/Cargo.toml
```

These checks establish schema and deterministic behavior. They are not runtime
performance evidence and cannot accept a candidate, update a champion, or
support a competitive claim.

Runtime performance evidence is collected deliberately on a controlled local
host; there is no generic or self-hosted GitHub workflow that pretends hosted
runner measurements are comparable. Before accepting a candidate, capture an
anonymized host snapshot with `--require-valid`, validate the tracked
declaration with `--phase acceptance`, and run the declared parent and
candidate on the same host and topology. Seal the complete repeated artifacts
with `lab/evidence_bundle.py`. Incomplete coverage, an invalid host, or a
declaration/diff mismatch fails closed.

Run from the repository root:

```bash
lab/run-heterogeneous-ablation.sh
```

With no `CASE_FILTER`, the runner traverses its current registered cases.
Optional external or kernel-dependent baselines that are unavailable are
recorded as `status: "skipped"` and do not make the final status check fail.
Rejected experiments are removed from the registry rather than retained as
permanent skip-only cases.

The runner builds the product on the host and confines `tc`, routes, interface
changes, blackholes, and TUN work to Docker namespaces. It writes JSONL and
per-case artifacts to a unique invocation directory under
`.tmp/lab/results/`,
which is ignored generated evidence. Named `RESULT_DIR` values remain
available for deliberate cohorts. One lab lock prevents concurrent Compose
runs from corrupting shared topology.

Each invocation retains `run-manifest.json`, an anonymized effective Compose
config, redacted product configs with SHA-256 checksums, and before/after qdisc
and interface-counter snapshots. Qdisc snapshots include drop, overlimit, and
backlog counters. The case-named client config is the path and resource
contract for its JSON row; the qdisc snapshot is the effective underlay
contract. Publish these side artifacts with any benchmark table.

## Evidence cohorts

Do not combine these cohorts in one performance claim:

- **Shaped**: Docker paths with explicit rate, delay, jitter, and loss.
- **Unconstrained**: Docker paths with netem cleared. This is not the public
  Internet and is not a wire-speed guarantee.
- **Fault**: blackhole, latency-spike, saturation, and deterministic condition
  handover experiments. Each default handover epoch restores the recorded
  baseline before changing one selected link, so history cannot silently
  accumulate into a different all-link-outage experiment.
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

Pre-v5 results are historical references only. Current performance claims
require fresh protocol-v5 matched rows.

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

`lab/configure-netem.sh` owns these profiles. Its queue limit is derived from
each profile's rate-delay product, including jitter headroom, so unintended
queue overflow cannot silently add loss. `MPTUNNEL_LAB_NETEM_LIMIT_PACKETS`
can override that calculation for an explicit queue experiment. Equal-path and
controlled matrix variants override the profiles explicitly. The shaped
profiles emulate plausible conditions; they are not measurements of any named
ISP or route.

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

### TCP carrier QoS cohort

The adaptive TCP carrier gate has one opt-in, fixed cohort:

```bash
MPTUNNEL_LAB_TCP_CARRIER_QOS_COHORT=1 \
CASE_FILTER='mptunnel_tcp_carrier_qos_*' \
BUILD_LAB_IMAGES=0 \
lab/run-heterogeneous-ablation.sh
```

For download and upload, it runs adjacent `tcp-carriers=1-1` and
`tcp-carriers=1-3` rows with three persistent application flows, a synchronized
post-connect start, and a 30-second load window. The per-flow profile applies a
500 Mbps `fq maxrate` to each native TCP flow; the shared profile applies one
200 Mbps aggregate bottleneck. Both use the fat-path propagation delay, zero
configured loss, BDP-derived aggregate and per-flow queue limits, and saved
`tc -s -d` state. The per-flow limit prevents Linux `fq`'s small default queue
from adding an undocumented loss model on the high-BDP path. The client URI
alone owns the carrier range.

This cohort is a measurement fixture, not evidence that the current runtime
has retained elastic carriers. Accept expansion only from repeated adjacent
pairs: the per-flow profile must establish useful added service, while the
shared bottleneck must preserve goodput and retire an unhelpful candidate
without churn. There is no universal percentage margin.

Xray and Hysteria2 releases, asset URLs, architectures, and SHA-256 digests are
pinned in `lab/baseline-lock.json`; no mutable `latest` URL is resolved. The
runner freezes that lock by SHA-256 for the full invocation. Each external
baseline launch verifies its artifact, Xray is atomically re-extracted from its
verified archive, and each accepted result row records the frozen-lock digest,
executable hash, version output, architecture, and client/server identity
actually observed. `run-manifest.json` embeds the same complete lock. Update the
lock only as an explicit benchmark-cohort change, then run the baseline and
candidate adjacently. The MPTCP baseline uses the lab host's kernel and therefore
requires separate subflow evidence rather than a downloaded-tool identity.

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
CASE_FILTER='mptunnel_tcp_single_cross_continent_high_bandwidth,mptunnel_udp_stream_single_cross_continent_high_bandwidth,mptunnel_udp_stream_multipath_equal_fat' \
lab/run-heterogeneous-ablation.sh
```

Every row records the client runtime/version, client and server targets, and
both binary hashes. Wine cases support TCP and QUIC proxy workloads; TUN cases
remain native-only. On limited Winsock implementations, QUIC uses the explicit
basic-UDP compatibility adapter and records its warning. Wine proves behavior
of the Windows executable and portable fallbacks, not Wintun, native Windows
kernel scheduling, `SIO_TCP_INFO`, or the optimized native QUIC UDP adapter.

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

For condition-handover A/B comparisons, preserve `MPTUNNEL_LAB_FLAP_SEED`,
ordered modes, hold bounds, and netem overrides. The seed fixes the intended
schedule, not packet-level random loss, Docker command latency, or application
progress. Every event is a complete baseline-then-selected-condition epoch;
the default conditions change at most one link. Complete carrier outage and
restoration are separate lifecycle tests.
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

Protocol-v4 diagnostics should be interpreted at the RFC boundary:

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
management sampling before making a throughput claim. See
`docs/PERFORMANCE_DIAGNOSTICS.md`.

## Result interpretation

`status: "ok"` means the case contract completed. `status: "loss"` is valid
lossy/incomplete evidence but not an exact throughput ratio. `status: "fail"`
means setup, contract, or zero-progress failure. `status: "skipped"` means an
optional external or kernel-dependent baseline was unavailable; it is explicit
missing evidence, not a passing comparison and not a product-case failure.

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

## Current release evidence

The compact, release-candidate v0.1.2 evidence is recorded in
[`docs/PERFORMANCE.md`](PERFORMANCE.md). It identifies the exact Core-frozen
no-feature candidate and build inputs, records targeted TCP/QUIC five-path
guards, and discloses the dirty-tree host-validity failure. Product-only edits
after that cohort change the complete binary identity, so the eventual tagged
binary still requires its own guard. The same document preserves the v0.1.1
and v0.1.0 cohorts as historical references without rebinding their competitor
or failover results to protocol v4.

Generated `.tmp/lab/results/` directories are local evidence, not repository
content. They are removed after their durable method, identities, exact rows,
and limitations are recorded. Old protocol-v1 `iteration*` directories are not
current regression evidence and are not retained for a public release.

A historical absolute result must never replace a matched current control. If a
new row materially weakens a retained capability, reproduce it beside its
single-path or external control, identify the owning state transition, and
correct the model before accepting the downgrade.

An isolated throughput change around five percent is ordinary run-to-run noise
unless repeated adjacent pairs and causal evidence establish otherwise. It is
not a pass margin, regression cap, or universal gate. Apply the paired evidence
method in `docs/PERFORMANCE_PLAN.md`; do not convert one convenient percentage
into laboratory or production policy.

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
python3 lab/summarize-results.py .tmp/lab/results/<result>.jsonl
python3 lab/summarize-results.py --format json .tmp/lab/results/<result>.jsonl
```
