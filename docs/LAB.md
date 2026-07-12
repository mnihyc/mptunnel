# Docker Lab

The lab is developer/test infrastructure. It is not compiled into the release `mptunnel` binary, and release archives are built from `--bin mptunnel` only.

Lab goals are measurements, not production behavior. The ~256 MiB RAM and ~1 Gbps clean-link targets are used to identify regressions in manual experiments. The production binary does not contain those thresholds, does not self-limit to them, and does not terminate because a lab target is missed.

The lab test goal is to expose whether the adaptive runtime makes the right choices when paths differ sharply in RTT, bandwidth, jitter, loss, and failure state. A good run shows lower interactive latency than bulk-first scheduling, higher download goodput than any weak single path, fast recovery after blackholing a path, and bounded resource use in the lab process. A poor run is kept as evidence for the next scheduler or transport fix; it is never converted into a production hard stop.

Run the heterogeneous ablation lab from the repository root:

```bash
lab/run-heterogeneous-ablation.sh
```

The script builds the product binary on the host, then runs all network mutation inside Docker containers. It does not change host routes, host DNS, host TUN devices, or host `tc` state. Product launches are intentionally user-like by default: the harness generates the same role-free TOML graph that operators use, validates it inside the container with `mptunnel --config ... --check-config`, then starts the process with `--config`. The generated lab graph is `inbound(socks5|tun) -> outbound(mpp)` for client-side cases and `inbound(mpp) -> outbound(direct)` for the server. It does not inject path metadata hints, probe timing, resource limits, or other tuning fields unless an explicit lab override is set.

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
- `target`: HTTP download target, receiver-confirming TCP upload sink, and UDP echo target.

Each container is capped at two CPUs in Docker Compose to approximate a modest VPS instead of an unconstrained host.

It creates five simultaneous client/server path networks plus a server/target network:

| Network | Client | Server | Target | Profile |
| --- | --- | --- | --- | --- |
| `path_lowlat` | `172.31.10.10` | `172.31.10.20` | `172.31.10.30` | 20 ms, 80 Mbps, 1% loss |
| `path_balanced` | `172.31.15.10` | `172.31.15.20` | `172.31.15.30` | 80 ms, 200 Mbps, 1% loss |
| `path_mildloss` | `172.31.16.10` | `172.31.16.20` | `172.31.16.30` | 160 ms, 100 Mbps, 0.1% loss |
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
- mptunnel mixed workload while a recorded, seed-derived schedule flaps path states between normal, spiked, and blackholed profiles.
- mptunnel TCP multipath download while the high-bandwidth path is blackholed during transfer.
- mptunnel TCP multipath download while the high-bandwidth path is degraded by a latency, jitter, loss, and rate spike during transfer.
- mptunnel mixed workload while the high-bandwidth path is blackholed or degraded by the same latency-spike profile during transfer.
- mptunnel mixed-workload ideal comparisons where one selected path is forced to 0% loss.
- mptunnel controlled matrix download and upload cases over one TCP+UDP path where bandwidth, latency, and loss each toggle between good and poor values.

Download cases use a sparse 1 GiB HTTP object by default and run for a fixed duration, so a valid measurement is usually a partial HTTP body at the end of the test window rather than a completed small file. Upload cases use persistent TCP streams to a lab sink for the same fixed duration. The sink emits cumulative application-byte acknowledgements while receiving and an exact final total after EOF; the probe reads them concurrently with its writes. The sink keeps per-connection receive totals in memory and publishes atomic snapshots on a bounded background cadence, so filesystem serialization is not in the per-receive hot path. After the probe exits, the runner cooperatively quiesces the sink, waits for every handler, and requires a finalized snapshot. Mixed cases run several workloads for the same fixed window: a sustained large-object download, repeated small-object HTTP requests for browsing latency, one persistent TCP echo stream with periodic payloads for SSH-like interaction, and duration-driven UDP datagrams for realtime traffic. The harness should not finish early just because one request completed.

The HTTP, upload, and mixed cases record wall time, goodput Mbps, startup timing, max transfer gap, recovery gap, and interval goodput samples during sustained load windows. Upload primary bytes and aggregate goodput come from the target observer snapshot. In-band target acknowledgements supply separately labelled delivery intervals, first-delivery time, and recovery gaps; local socket acceptance remains a sender diagnostic. UDP cases record attempted/received datagram counts, loss rate, and latency percentiles over the same duration-driven window. JSONL rows with `status:"loss"` are retained as valid lossy-network measurements; rows with `status:"fail"` indicate a failed experiment.

Response-capacity diagnostics keep TCP product ACK-clock calibration separate
from UDP/QUIC carrier metrics. TCP calibration events record the binding-local
`binding_instance_id` as well as session, exact output path/incarnation,
cumulative spent credit, current and resource ceilings, stage authorization,
prior ACK, and earliest/latest sampled send times. The binding ID is required
because concurrent response streams can share session and path identities.

The staged exact-receipt handoff case uses two fixed HTTP requests and rejects
replacement bindings. It first observes two TCP Service bindings, keeps the
already-attached UDP transport scheduler-disabled, then reactivates it and
requires calibration, receipt, proof, retirement, drain, and commit identities
to match. Physical blackhole and reconnection behavior is intentionally left to
the fault cases so transport recovery cannot masquerade as placement evidence.

Mixed response diagnostics emit `response_quic_capacity_calibration` for the
bounded non-product QUIC train, `quic_capacity_receipt` for exact peer receipt,
`quic_capacity_proof` for registry acceptance, `quic_carrier_ack_poll` and
`quic_capacity_ack_poll` for provisional native observations and receipt/native
cleanup ordering,
`quic_capacity_probe_retired` for terminal cleanup, and
`response_service_handoff` for bounded drain and exact whole-flow commit
phases. Include those events in the diagnostic allowlist and correlate by
session, token, path, and exact path instance. For every ordinary carrier
sample, verify `raw_rate_bps ~= sample_bytes * 8_000_000 / sample_elapsed_us`
and `sample_elapsed_us = max(carrier_elapsed_us, 1000)`. `poll_elapsed_us` is a
control comparison and MUST NOT be the denominator. `published_rate_bps` is the
bounded/smoothed model, not the raw observation. For optional UDP Subflow or
capacity admission, only timed non-app-limited bytes and samples may reach the
strict bulk floor. The current QUIC Service has a narrower feed-only exception:
either substantial uniquely owned product `STREAM_ACK` progress or a durable
local carrier ACK-derived DATA estimate may unlock source and emission staging;
the latter may be app-limited. Neither authority publishes optional capacity,
admits another Subflow, or authorizes a handoff, and same-path latency pressure
still prevents graduation. A deliberate capacity epoch is
different: it gates other application writers and requires exact full-train
`PATH_CAPACITY_FINISH`/`PATH_CAPACITY_RECEIPT` ordering.
Packet ACKs remain connection-aggregate and diagnostic; they cannot establish
token ownership. Pre-existing cross-stream work is allowed only because it
lengthens receipt time and lowers the available-rate estimate. The exact target
sample must retain its planning-time frozen three-PTO
bulk-proof deadline through drain and commit; falling RTT must not shorten it,
and durable ACK reachability alone is insufficient.

`PATH_CAPACITY_DATA` owns no stream offset and receives no `STREAM_ACK`, so do
not count it as mixed unique-data aggregation. A committed handoff is the
causal event that changes response Service-family ownership. The calibration
event reports the sample floor, accounting slack, fresh strict window, live
carrier window, train bytes, frame count, and lease. Require
`lease_committed=true` and verify
`train = max(sample floor, carrier window + fresh window)` and separately
require `train <= session limit`, with
`fresh window = sample floor - accounting slack`. A clamped over-limit geometry
is invalid and MUST NOT repeatedly outrank a smaller fitting retry. Token completion requires
exact written and received bytes equal to the frozen whole train, a nonzero
written record count, and a nonzero receipt-derived proof interval; provisional
native ACK bytes are diagnostics only. Verify
`proof_elapsed = max(1000 us, receipt_elapsed)`. Native timing and receipt RTT
remain ordering diagnostics and do not mutate this frozen denominator. The raw
capacity rate uses the whole train:
`rate_bps ~= train_bytes * 8_000_000 / proof_elapsed_us`. The attempt deadline
and proof-validity interval are separate: accepted time is the carrier receipt
time and proof expiry is accepted time plus the frozen validity interval. A
later live congestion window or RTT cannot change the contract. Within one session, eligible
fitting unattempted path instances should appear before a retry. Each exact
session/path/path-instance may start at most twice, every whole train must fit
the one cumulative non-refilling session envelope, and completed, expired,
proven, detached, or closed attempts remain charged. Only an exact failed
provisional enqueue may roll back its reservation.

Under sustained source feed, causal placement proof may include
`phase=drain_started` followed by `phase=committed` for the same session,
binding, exact source instance/incarnation, and exact target
instance/incarnation. Both events report `handoff_mode` and proof authority.
`Diversification` requires a source-family lead of at least two and a no-worse
projected share; `PerformanceOverride` requires a two-fold projected gain even
when families are balanced. TCP product rate is already per-flow goodput, while
carrier-scoped TCP/QUIC rate is divided by projected bulk-flow count. During the
bounded one-shot drain,
fresh `OwnerData` stops only for the selected binding; control, ACK/credit,
correctness-critical repair, and other bindings remain live. Offset-free source
staging remains within the existing bounded source-feed/sender-queue reservoir
while that binding's queued Data front is blocked. `phase=drain_cancelled` means
expiry or revalidation rejected the move and must not be counted as a handoff.
A commit without an exact frontier, or continued fresh OwnerData on the selected
binding between matching drain and commit events, is a failed ownership model.
Shared carrier queue/BIF from other bindings may remain nonzero, but this
binding's exact owner/product flight must be zero and live pending bytes must stay
inside the ranked commit bound. Require one
`server_bulk_output_selected reason=service_handoff` for the committed frame.

Keep three handoff rows separate. The staged `2:0` row disables and blackholes
UDP until two distinct TCP Service bindings emit OwnerData, restores both fat-path
qdiscs, then activates UDP and requires typed train -> receipt -> proof -> drain ->
commit order. The native slow-TCP/fast-QUIC `1:1` row proves
`PerformanceOverride` onto an already-busy carrier without another optional
train. The reversed fast-TCP/slow-QUIC row is a negative control and must not emit
a TCP-to-QUIC override. Persist the resolved netem/resource profile, management
state changes, and both restore-command results with each staged artifact.

Diagnostics alter CPU and trace I/O, so their goodput is not a clean comparison.
After causal acceptance, repeat representative TCP, QUIC, mixed, inverse, and
unconstrained cases with diagnostics and path hints disabled.

A stage-authorizing sample requires a fully spent current stage, every sampled
send before the prior ACK, and its earliest send after stage authorization. A
mixed pre/post-ACK or pre-authorization window is not stage evidence.

Interpret credit growth and rate publication independently. A qualifying
sample may grow the next cumulative ceiling immediately. Separately, all
strict current-stage windows accumulate their bytes and raw ACK-to-ACK elapsed time.
At authorization, the byte/time aggregate enters the rolling five-stage rate
window only when coverage reaches
`min(initial_credit, max(MIN_RATE_SAMPLE_BYTES, ceil(initial_credit / 2)))`,
which is 1 MiB with the default 2 MiB initial credit. Under-covered evidence
still grows or proves the stage, then resets without publishing. Diagnostic
events expose the current sample bytes/elapsed, aggregate bytes/elapsed,
coverage floor, acceptance decision, and aggregate rate. Before three accepted
stage aggregates, the candidate retains its startup rate; from three onward,
publish the rolling median by overwriting the prior product/delivery rate, even
when lower. Do not
report a maximum or upward-only filter as calibrated capacity because ACK
compression can create multi-gigabit sub-millisecond samples. UDP/QUIC product
ACK timing never proves this TCP calibration because the local QUIC ACK
controller owns its delivery-rate and pacing evidence. Calibration must leave
the response Service owner unchanged. A smaller-than-normal response product
frame is valid only when two-pass planning returns the exact active TCP
calibration commit for its residual credit. A Service fallback must use the
normal chunk, and UDP/QUIC framing remains unchanged.

Apply timer granularity once to the completed stage aggregate. Applying the
minimum duration to every raw window makes an identical byte/time trace depend
on how ACKs were partitioned into callbacks. Global ordered-tail,
per-candidate product-flight, and carrier-queue bytes overlap and must use
union-style debt for startup/calibration admission; exact
cumulative spend remains the authoritative credit counter. A proven calibration
identity remains fenced from generic ownership until its exact calibration
flights drain.

Outside any active TCP calibration fence, diagnostics may emit
`server_bulk_output_selected reason=same_family_subflow_reservoir`. This is
ordinary measured ownership, not more calibration: the exact mature Service
already holds at least its derived Service horizon, the selected target is an
admitted same-underlay `Subflow` bound to that Service epoch, neither
path/session has latency pressure, and projected ordered tail remains inside
the existing feed reservoir. TCP uses strict product-ACK evidence; QUIC uses
strict non-app-limited local carrier ACK evidence and native emission credit.
Neither substantial product progress nor the app-limited carrier estimate used
by the weaker QUIC Service-feed predicate can select this reason.
Interpret the event together with per-interface counters; selection count alone
does not prove useful capacity. The v26 matched
diagnostic used the former TCP-specific event name and emitted 147 such 64 KiB
decisions (9,633,792 B), then kept its final 5-second application windows near
95-96 Mbps versus v23's
post-calibration 80 Mbps collapse, but differing Service anchors and diagnostic
CPU make that corroborating causal evidence rather than a clean throughput
claim. The matched endpoint-only, noninstrumented 45-second release pair is the
performance result: v25 delivered 492,276,216 B at 87.263 Mbps and v27 delivered
835,492,784 B at 148.032 Mbps, both with zero failures and
`performance_comparable=true`.

With default limits and a 64 KiB bulk quantum, the derived feed reservoir is
4 MiB. A one-flow QUIC plateau at approximately that much product data per RTT
is not a measured QUIC congestion window. First distinguish source/emission
bootstrap from receive credit: if diagnostics show feed graduation and the
configured source/emission envelope while product flight remains near 4 MiB,
`STREAM_MAX_DATA` is the limiter. Bulk receive credit now uses the configured
receiver-memory window independently of path proof; strict optional-path proof
remains a separate scheduler condition.

The priority trace in `iteration40-service-feed-transition-diag-r1` observed
feed graduation at 1.671 s and strict QUIC proof at 2.187 s while product flight
remained near 4 MiB for the full 9-second row. The matched release pair in
`iteration41-configured-bulk-window-equal-fat-release-r1` then reached
176.625/274.340 Mbps overall/final-three-seconds on five equal paths versus
144.885/180.210 Mbps on one path. This proves a useful window-boundary gain,
not ideal aggregation; retain the separate one-flow scheduler investigation.

Iterations 42-49 close two tempting one-flow bootstrap directions without
shipping them. The bounded product sample reached its full 256 KiB cap and
exact ACK release, but Quinn still classified the finite burst app-limited, so
it created no strict optional-path proof. The typed carrier-only alternative
did produce exact whole-train receipts: iteration 45 sent 7,109,686 bytes and
published 18.190 Mbps over 3.127 s. It still selected no product Subflow.
Draining the Service tail made proof usable, but serialized useful traffic and
regressed the 12-second rows. Iteration 47 completed the entire proof-to-Subflow
transition, then exposed a separate ownership error: carrier-only probing had
grown native QUIC cwnd enough to authorize 7 MiB of product reorder debt,
causing a 2.233 s read gap and only 46.000 Mbps. An initial attempt to cap only
native cwnd missed the carrier pacing input: iteration 48 still assigned about
7.2 MiB and reached 101.949 Mbps. Bounded Service refills in iteration 49
reached only 58.955 Mbps and collapsed to about 1 Mbps late. These instrumented
diagnostics are causal evidence, not release-comparable throughput rows. The
one-flow product/probe policies were therefore removed. Retain only the general
conclusions: an exact startup epoch owner may continue its own non-refilling
lower frontier to the declared cap, and a high-confidence additional
same-underlay QUIC path without durable product progress uses a BBR-style
`2 * delivery-rate BDP` product inflight target instead of carrier pacing or
cwnd. The latter invariant is unit-model verified; no rejected one-flow
mechanism remains enabled to claim a release throughput gain from it.

### Evidence cohorts

Keep these result families separate when comparing changes:

- TCP-only, UDP reliable-stream-only, reliable mixed-carrier, and composite mixed-workload cases exercise different product paths. Do not pool their ratios.
- Shaped one-flow and shaped two-flow cases have different offered capacity and contention. Compare each only with the same topology and netem profile.
- Blackhole, latency-spike, and scheduled-flapping rows are separate fault cohorts. Match the impairment settings, timing, and, for flapping, schedule evidence before comparing them.
- `unconstrained` means Docker paths with netem cleared. It is still a local container experiment, not a real-Internet measurement.
- The shaped real-profile cases emulate Internet conditions. They are not evidence from the public Internet.
- Real-Internet results require a separately recorded endpoint inventory, route/path context, time window, and reproducible runner. When those inputs are unavailable, record the cohort as not run rather than treating an emulated case as a substitute.

### Upload accounting

Standalone probe metric version 2 uses
`upload_accounting_source:"target_sink_ack"`. A Docker runner row promotes a
valid, quiesced target snapshot to metric version 4 with
`upload_accounting_source:"target_sink_observer"`. If quiesce, snapshot, or
schema validation fails, the row retains its in-band v2 accounting and records
the observer error instead of claiming promotion. `bytes`,
`target_confirmed_bytes`, `target_observed_bytes`, `goodput_mbps`, and
`upload_goodput_mbps` then count application bytes observed by the receiving
sink. `observer_elapsed_s` spans runner invocation through finalized target
quiesce and becomes the v4 `time_s` denominator; `probe_elapsed_s` preserves
the earlier client window. The sink is finalized before the case-boundary
target-edge after-snapshot, so target application bytes cannot advance beyond
that counter window. `in_band_target_confirmed_bytes` retains the acknowledgement
lower bound, and `upload_interval_accounting_source:"target_sink_ack"` labels
interval and gap timing only when ACK parsing remained valid.
`local_accepted_bytes`, `local_accepted_goodput_mbps`,
`first_write_s`, `max_write_gap_s`, and `local_recovery_gap_s` describe only
what the client socket or local proxy accepted; they MUST NOT be reported as
end-to-end delivery.

Each stream sends `ACK <cumulative-bytes>` progress and a final
`OK <cumulative-bytes>` after EOF, while the observer stores the same receiving
connection's current total and final state out of band. `ok` requires the
expected connection count, every final marker, and aggregate target/local byte
equality. Positive target progress without complete drain is `loss`, its byte
count is an eventual-delivery lower bound, and
`upload_accounting_lower_bound` is true. Zero target bytes is `fail`.
Unexpected target connections make the observer snapshot non-authoritative:
without an application stream identity and offset, retry prefixes cannot be
deduplicated safely. The default one-second post-window drain is recorded as
`drain_timeout_s` and can be changed explicitly; it never turns unconfirmed
sender buffering into delivery. The outer process timeout is derived from the
load, connection, and configured drain bounds rather than a fixed allowance.

Summaries accept both receiver-confirmed sources and retain legacy upload row
statuses as `unverified`. Exact completed rows, incomplete receiver lower
bounds, and sender-only rows have separate counts and metrics. Lower bounds do
not enter exact medians, best values, or equal-profile ratios. Observer v3 rows
remain receiver-byte evidence but are not exact performance-comparable because
their snapshot I/O and elapsed windows predate the v4 contract.

RFC 6349 motivates measuring a block across the TCP connection rather than
send-buffer admission, but these fixed-duration incomplete cases are not its
completed predetermined-block Transfer Time Ratio procedure. The lab also
mirrors iperf3's distinction between sender and receiver results; iperf3
documents that server output is available only when a test completes, while
this lab's progress acknowledgements retain a bounded receiver-side lower bound
for deliberately harsh, incomplete cases.

References: [RFC 6349 TCP throughput testing](https://www.rfc-editor.org/rfc/rfc6349.html#section-4.1) and [iperf3 server output](https://software.es.net/iperf/invoking.html).

### Traffic accounting

Container telemetry takes case-boundary snapshots of aggregate non-loopback counters. The probe, client edge, and target edge observe different delivery points. Fixed-duration downloads normally stop with a partial final request, so the target may have emitted bytes that the probe has not consumed when the after-snapshot is taken. Endpoint snapshots are sequential, not atomic, and can include additional timing skew. The periodic sampler checks its stop marker between service operations and during its sleep, but an in-flight Docker command remains bounded by that command's timeout.

New rows use traffic metric version 3 and retain several distinct quantities:

- `client_edge_traffic_bytes_approx` and `target_edge_traffic_bytes_approx` are aggregate non-loopback rx+tx deltas at the two container edges.
- `client_vs_probe_payload_excess_*_approx` and `target_vs_probe_payload_excess_*_approx` are signed edge-counter differences relative to probe-visible payload.
- `client_target_endpoint_balance_*_approx` is the signed client-edge counter minus the target-edge counter. All three ratios and percentages use probe payload as a common denominator, so the unrounded ratios preserve the byte identity; independently rounded displayed percentages can differ by `0.001` percentage point.
- `traffic_accounting_identity_residual_bytes_approx` checks the byte identity `client-probe = (target-probe) + (client-target)` and should be zero. This identity is algebraic, not causal.
- `traffic_expansion_estimate_available` and `traffic_expansion_exact_available` are false. Aggregate bidirectional counters cannot separate simultaneous upload/download in-flight bytes, sequential snapshot skew, unrelated interface traffic, or packets lost before an observation point. Expansion requires direction-split, per-interface sender accounting over finite transfers whose endpoint delivery windows are drained.

The same existing case-boundary snapshots retain every non-loopback interface's
IPv4 address when available and its rx/tx byte and packet counters. The telemetry
summary exposes nonnegative before/after deltas under
`services.<service>.interfaces.<interface>` only when that interface exists in
both snapshots. This adds no periodic sampler call. Use these deltas to prove
which shaped paths carried a matched case; they remain bidirectional edge
counters and therefore do not establish transport expansion.

The legacy `traffic_overhead_*_approx` and `tunnel_traffic_bytes_approx` fields remain in JSONL rows for schema compatibility. They are respectively a nonnegative client/probe delivery-window gap and an alias for aggregate client-edge traffic; both also appear on direct controls and must not be interpreted as tunnel expansion. The Markdown summarizer uses only version-3 names and renders a diagnostic median only when every accepted row in that case supplies it.

## Controls

Useful environment variables:

- `MPTUNNEL_LAB_SECRET`: optional UUID or 32+ byte shared secret for reproducible lab runs. When unset, the lab generates a fresh UUID secret for that run. The release binary itself has no default secret.
- `MPTUNNEL_LAB_OBJECT_MIB`: sparse large HTTP object size in MiB, default `1024`. This is backing data for sustained file-download tests, not the test-length control.
- `MPTUNNEL_LAB_SMALL_OBJECT_KIB`: small HTTP object size in KiB for browsing probes, default `32`.
- `MPTUNNEL_LAB_LARGE_HTTP_PATH`: large-object URL path, default `/large.bin`.
- `MPTUNNEL_LAB_SMALL_HTTP_PATH`: small-object URL path, default `/small.bin`.
- `MPTUNNEL_LAB_LOAD_DURATION_SECONDS`: sustained workload duration for download and mixed probes, default `30`.
- `MPTUNNEL_LAB_UPLOAD_DRAIN_TIMEOUT_SECONDS`: bounded time after the upload load window for target progress/final acknowledgements, default `1`. The value is recorded in upload rows.
- `LOAD_DURATION_MATRIX`: space-separated sustained workload duration matrix for `lab/run-exhaustive-experiments.sh`.
- `MPTUNNEL_LAB_BULK_CONNECTIONS`: parallel pure-download or pure-upload connections for capacity tests, default `2`.
- `BULK_CONNECTIONS_MATRIX`: space-separated bulk connection-count matrix for `lab/run-exhaustive-experiments.sh`.
- `CURL_TIMEOUT_SECONDS`: per-download timeout, default `120`.
- `UDP_PAYLOAD_BYTES`: UDP probe payload size, default `512`.
- `UDP_TIMEOUT_MS`: per-datagram UDP timeout, default `2500`.
- `FAILOVER_AFTER_SECONDS`: seconds before blackholing the high-bandwidth path, default `2`.
- `MPTUNNEL_LAB_FAILOVER_FAT_TX_TRIGGER_BYTES`: server fat-path TX delta required before the TCP download blackhole, default `0`. Zero keeps fixed `FAILOVER_AFTER_SECONDS` timing. A positive value waits for both that byte delta and `FAILOVER_AFTER_SECONDS` as a minimum dwell before injecting the fault.
- `MPTUNNEL_LAB_FAILOVER_TRIGGER_TIMEOUT_SECONDS`: maximum wait for a positive fat-path TX trigger, default `60`. Timeout fails the case instead of blackholing a path that did not carry the required traffic.
- `MPTUNNEL_LAB_FAILOVER_TRIGGER_POLL_INTERVAL_SECONDS`: server interface-counter polling interval for the positive trigger, default `0.02`.
- `FAILOVER_AFTER_MATRIX`: space-separated failover timing matrix for `lab/run-exhaustive-experiments.sh`.
- `REPEATS`: repeat count for each matrix point.
- `MPTUNNEL_LAB_DIAGNOSTICS=1`: build an optimized lab binary with the `lab-diagnostics` feature for extra experiment-only instrumentation when that feature is used. Release packaging and CI release jobs do not enable this feature.
- `MPTUNNEL_LAB_LOG`: log level passed to lab-launched mptunnel processes, default `info`.
- `PATH_PROBE_INTERVAL_MS`: optional diagnostic override for the mptunnel client path-probe interval. Unset by default so product launches use built-in defaults.
- `PATH_PROBE_TIMEOUT_MS`: optional diagnostic override for the mptunnel client path-probe timeout. Unset by default so product launches use built-in defaults.
- `MPTUNNEL_LAB_USE_PATH_HINTS=1`: optional diagnostic override that adds RTT/rate/capability query hints to path URIs. Unset by default so product launches use endpoint paths only.
- `MPTUNNEL_LAB_LOWLAT_RATE`, `MPTUNNEL_LAB_LOWLAT_DELAY`, `MPTUNNEL_LAB_LOWLAT_JITTER`, `MPTUNNEL_LAB_LOWLAT_LOSS`: low-latency path netem values. The default loss is `1.00%`.
- `MPTUNNEL_LAB_BALANCED_RATE`, `MPTUNNEL_LAB_BALANCED_DELAY`, `MPTUNNEL_LAB_BALANCED_JITTER`, `MPTUNNEL_LAB_BALANCED_LOSS`: balanced daily-use path netem values. The default loss is `1.00%`.
- `MPTUNNEL_LAB_MILDLOSS_RATE`, `MPTUNNEL_LAB_MILDLOSS_DELAY`, `MPTUNNEL_LAB_MILDLOSS_JITTER`, `MPTUNNEL_LAB_MILDLOSS_LOSS`: lower-loss companion path netem values. Defaults are half the balanced rate, twice the balanced delay, balanced jitter, and `0.10%` loss.
- `MPTUNNEL_LAB_FAT_RATE`, `MPTUNNEL_LAB_FAT_DELAY`, `MPTUNNEL_LAB_FAT_JITTER`, `MPTUNNEL_LAB_FAT_LOSS`: high-bandwidth path netem values. The default loss is `1.00%`.
- `MPTUNNEL_LAB_POOR_RATE`, `MPTUNNEL_LAB_POOR_DELAY`, `MPTUNNEL_LAB_POOR_JITTER`, `MPTUNNEL_LAB_POOR_LOSS`: poor-Internet path netem values. The default loss is `10.00%`.
- `MPTUNNEL_LAB_IDEAL_LOSS`: loss value for ideal comparison cases, default `0.00%`.
- `MPTUNNEL_LAB_MATRIX_GOOD_RATE`, `MPTUNNEL_LAB_MATRIX_POOR_RATE`: controlled matrix bandwidth values, default `500mbit` and `50mbit`.
- `MPTUNNEL_LAB_MATRIX_GOOD_DELAY`, `MPTUNNEL_LAB_MATRIX_POOR_DELAY`: controlled matrix latency values, default `50ms` and `250ms`.
- `MPTUNNEL_LAB_MATRIX_GOOD_JITTER`, `MPTUNNEL_LAB_MATRIX_POOR_JITTER`: controlled matrix jitter values, default `5ms` and `60ms`.
- `MPTUNNEL_LAB_MATRIX_GOOD_LOSS`, `MPTUNNEL_LAB_MATRIX_POOR_LOSS`: controlled matrix loss values, default `1.00%` and `15.00%`.
- `MPTUNNEL_LAB_BLACKHOLE_LOSS`: blackhole loss value for failover tests, default `100%`.
- `MPTUNNEL_LAB_SPIKE_FAT_RATE`, `MPTUNNEL_LAB_SPIKE_FAT_DELAY`, `MPTUNNEL_LAB_SPIKE_FAT_JITTER`, `MPTUNNEL_LAB_SPIKE_FAT_LOSS`: degraded high-bandwidth-path values used by latency-spike cases, default `20mbit`, `900ms`, `250ms`, and `10.00%`.
- `MPTUNNEL_LAB_SATURATE_PROTOCOL`: background saturation protocol, `udp` by default or `tcp`.
- `MPTUNNEL_LAB_SATURATE_LOWLAT_BANDWIDTH`, `MPTUNNEL_LAB_SATURATE_BALANCED_BANDWIDTH`, `MPTUNNEL_LAB_SATURATE_FAT_BANDWIDTH`, `MPTUNNEL_LAB_SATURATE_POOR_BANDWIDTH`: bidirectional background `iperf3` rates for saturated-link cases.
- `MPTUNNEL_LAB_FLAP_MIN_SECONDS`, `MPTUNNEL_LAB_FLAP_MAX_SECONDS`, `MPTUNNEL_LAB_FLAP_MODES`: link-flapping cadence and supported netem mode list for unstable-link cases.
- `MPTUNNEL_LAB_FLAP_SEED`: optional decimal or text seed for the versioned flapping schedule generator. The same seed, ordered mode list, and hold bounds reproduce the same intended mode/hold sequence. Each hold begins after both client and server netem commands finish so slow control commands cannot compress a configured dwell. If omitted, the lab generates and records a 64-bit seed.
- `KEEP_LAB=1`: keep containers running after the script exits.
- `RESULT_FILE`: explicit JSONL output path.
- `RESULT_ROOT`: output directory for matrix runs.
- `CASE_FILTER`: comma-separated case names or shell globs for targeted reruns, for example `mptunnel_tcp_single_*,mptunnel_tcp_multipath_all`.
- `MPTUNNEL_LAB_DIAGNOSTICS=1 MPTUNNEL_LAB_DIAG=1`: build the optimized `lab-diagnostics` binary and emit internal diagnostic lines into the client/server `/tmp/mptunnel-*.log` files, including reliable-stream path open attempts/successes and UDP stream congestion state. Successful download rows also keep bounded client/server log tails when this is enabled. Use this only for investigation; release comparisons should run without diagnostic instrumentation.
- `MPTUNNEL_LAB_DIAG_EVENTS=event_a,event_b`: with diagnostic emission enabled, retain only the exact comma-separated event names. An unset, empty, or `*` value retains all events. Use a narrow allowlist for multi-gigabit lifecycle traces so per-frame candidate and dispatch formatting does not dominate the workload. `sender_service_conformance` explicitly enables the otherwise filtered per-frame conformance counters and assertion. Filtered runs remain instrumented causal evidence, never release-comparable measurements.
- `MPTUNNEL_LAB_DIAGNOSTICS=1 MPTUNNEL_LAB_PERF=1`: build the optimized `lab-diagnostics` binary and emit interval/cumulative per-component timing lines prefixed with `mptunnel_lab_perf`. `MPTUNNEL_LAB_PERF_INTERVAL_MS` controls the flush interval, default `1000`. `MPTUNNEL_LAB_LOG_TAIL_BYTES` and `MPTUNNEL_LAB_LOG_TAIL_LINES` control retained diagnostic log tails.
- `MPTUNNEL_LAB_CONTAINER_STATS=0|1`: enable periodic Docker CPU, memory, and network-counter sampling, default `1`. `MPTUNNEL_LAB_CONTAINER_STATS_INTERVAL_SECONDS` sets the cadence, default `1`. Per-case samples are written as `container-stats-<case>.jsonl`; case-boundary non-loopback counters are written separately and summarized into the result row.
- `MPTUNNEL_LAB_MANAGEMENT_SNAPSHOTS=0|1`: enable periodic release management `/diagnostics` snapshots for the client and server, default `0`. `MPTUNNEL_LAB_MANAGEMENT_SNAPSHOT_INTERVAL_SECONDS` sets the cadence, default `1`, and `MPTUNNEL_LAB_MANAGEMENT_PORT` selects the loopback management port, default `17600`. Per-case records, including interface counters and management errors, are written as `management-snapshots-<case>.jsonl`.

With a positive fat-TX trigger, the runner writes
`failover-trigger-<case>.json` under the result root. The artifact records the
server interface/address/counter, baseline and triggered values, observed and
required delta, minimum wait, timeout, and actual trigger elapsed time. The
runner then writes the client-clock failover marker at the injection boundary.
The probe row's `failover_after_s` records the marker-relative elapsed time it
actually used, and `failover_trigger_source` distinguishes an observed
`marker` from fixed-schedule fallback. These fields are the authoritative fault
boundary for gap calculations; the configured delay alone is not.

The positive byte trigger is a causal diagnostic control for proving that the
fat response path carried a required amount before it was blackholed. Its fault
time depends on the implementation's traffic placement and throughput, so the
result is not release-comparable performance evidence. Use the default zero
trigger and matched fixed timing for release comparisons after the causal case
is established.

The periodic observers are not free. Container statistics invoke Docker control-plane commands and read container network counters; management snapshots additionally use `docker exec` for each client/server sample and run a Python HTTP observer inside the container. Compare performance only when the container-stat setting and cadence match. Treat management-snapshot runs as causal diagnostics, not as performance-comparable release measurements; rerun the same case with management snapshots disabled before claiming a throughput or latency result.

For a repeatable component/process profiling workflow, use `lab/run-perf-diagnostics.sh` and see `docs/PERF.md`.

The flapping result row embeds its resolved seed, schedule/profile digests,
probe-relative start anchor, applied event count, worker/restore outcome, and
trace artifact path under `flapping`. The JSONL trace artifact is authoritative
for per-side netem command exit codes and monotonic application offsets. Compare
flapping throughput only when the generator, schedule profile digest, seed,
applied schedule digest, effective netem overrides, and instrumentation mode
match. A missing marker, compressed completed dwell, command failure, worker
timeout, malformed trace, or restore failure marks the experiment row failed.
The seed controls the impairment schedule, not netem's packet-level
jitter/loss randomness or Docker command latency, so it does not provide
packet-for-packet replay. For strict A/B use, compare the actual application
offsets in addition to the intended schedule digest. The runner rejects
concurrent executions because the Compose lab shares fixed networks and service
names.

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

A throughput comparison is valid only within a matched cohort: direction,
underlay/carrier set, application-flow count, measurement duration and drain,
effective netem profile, topology, and instrumentation mode must all match.
Diagnostics and management snapshots are causal evidence, not release
throughput rows. In particular, iteration 41 is a download cohort and MUST NOT
be compared as a throughput regression or gain against the iteration 55 upload
cohort.

Use only finalized metric-v4 uploads with
`upload_accounting_source:"target_sink_observer"` and
`upload_accounting_exact:true` for exact upload ratios. Incomplete, lossy, or
older upload accounting remains useful as a labelled lower bound, but it cannot
establish an exact aggregation result.

Keep constituent-path baselines separate from aggregation controls. Direct,
VMess, Hysteria2, and single-path mptunnel rows show what one shaped path or
carrier can deliver; they do not prove that multiple paths were aggregated.
Matched kernel MPTCP and a raw five-path aggregate, when available, are
aggregation controls. A final comparison must report these ratios explicitly:

- mptunnel multipath / matched same-carrier mptunnel single-path.
- mptunnel multipath / matched direct baseline.
- mptunnel multipath / matched kernel MPTCP.
- mptunnel multipath / raw five-path aggregate, when that control exists.
- mptunnel multipath / the sum of configured path ceilings (nominal efficiency).

Report overall and final-three-second goodput together with first delivery,
maximum and recovery delivery gaps, client/server CPU and RSS, and
per-interface byte use. Selection events alone do not establish useful path
aggregation; the interface counters must show which paths carried the measured
payload.

For release decisions, compare at least:

- mptunnel multipath versus the best raw direct path.
- mptunnel multipath versus each mptunnel single-path TCP, UDP reliable-stream, and mixed-carrier case.
- UDP multipath loss/latency versus each UDP single-path case and best-effort UDP-over-TCP case.
- TUN TCP, TUN UDP reliable-stream, and TUN mixed underlay cases.
- Failover completion and stall time after blackholing `path_fat`.
- Sustained interval goodput, first-body time, max read gap, UDP p95, and SSH-like echo success gap during the same fixed-duration window.
- Receiver-confirmed upload goodput and delivery gaps; locally accepted upload bytes are diagnostics only.
- Clean-lab aggregate goodput against the manual ~1 Gbps target.
- Lab RSS or equivalent process-memory samples against the manual ~256 MiB target.

Traffic-accounting fields are diagnostic context, not a release gate. A separate finite, drained, direction-split sender-observed experiment is required before making transport-expansion claims.

When results are poor, keep them. They are product signals, not harness failures.
