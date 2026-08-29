# Performance

MPTUNNEL aggregates independent links and keeps active traffic attached when a
carrier changes or disappears. Results depend on path conditions, direction,
workload, host capacity, and the native TCP or QUIC implementation.

## Current evidence publication gate

No current-release time-series figure or ranking is published. Publication
requires at least two matched repetitions with accepted host and source
provenance under identical isolated conditions; single-repetition and
diagnostic artifacts do not update the historical scalar tables below.

## Historical accepted fixed-profile evidence (v0.2.1–v0.2.2)

The fixed-profile tables in this section, through local processing capacity,
are accepted measurements of the exact v0.2.1–v0.2.2 binaries and
configurations used for those runs. They remain useful historical controls,
but they do not characterize v0.4.4 or its defaults. A `default` label in this
section means the default of the measured historical release and profile.

### Test conditions

Measurements used isolated GNU/Linux containers on one host. Rates are bytes
delivered to the receiver divided by full completion time; configured bandwidth
is never reported as throughput. The matched one-link and aggregation rows used
the same objects, two flows, and a 20-second load window. The asymmetric and
mixed-workload rows used 30-second windows. Each cell reports one valid
directional run, not a best-of selection. Repetitions were used only to classify
outliers. Xray-core 26.3.27 used VMess/TCP. Hysteria2 2.10.0 used Brutal with
client bandwidth equal to the shaped directional capacity. The measured
MPTUNNEL releases used their default shared-transport-key profile unless a
transport is named explicitly.

### Matched proxy conditions

Every path was shaped to 500 Mbps. Delay and jitter were applied once in each
direction; the table reports the approximate RTT and the configured
per-direction jitter. Every cell used two parallel downloads for 20 seconds.
MPTUNNEL used the measured release's default TCP+QUIC configuration.

| RTT (ms) | Jitter (ms) | Loss | Xray 26.3.27 VMess/TCP | Hysteria2 2.10.0 Brutal | MPTUNNEL TCP+QUIC |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 40 | 10 | 0.5% | 441.353 | 463.502 | 414.200 |
| 280 | 20 | 10% | 70.833 | 96.288 | 194.504 |

In these historical rows, MPTUNNEL is 10.6% below the fastest baseline on the
ordinary path. Under
280 ms RTT, 20 ms jitter, and 10% loss it delivers 2.02× Hysteria2 and 2.75×
Xray/VMess. Every row has valid host, source, and receiver accounting.

### Link aggregation

Every physical link used the 500 Mbps, 40 ms RTT, 10 ms jitter, and 0.5% loss
profile above.

| System | Transport | Shaped links | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: | ---: |
| Xray | VMess/TCP | 1 | 441.353 | ≥337.788 |
| Hysteria2 | Brutal | 1 | 463.502 | ≥459.287 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 1 | 414.200 | 425.335 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 2 | 771.888 | 621.237 |
| Linux MPTCP | TCP | 5 | 884.667 | 2.572 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 5 | 1,365.876 | 1,383.641 |

The measured historical default scales 1.86×/1.46× from one to two links and
3.30×/3.25× from one to five links for download/upload. Every MPTUNNEL result
completed with exact receiver accounting. The Xray and Hysteria2 upload
sessions did not close inside the completion window, so their
receiver-delivered values are lower bounds and are excluded from ratios.

The Linux kernel MPTCP control had additional subflows confirmed by runtime
`ss -M` evidence. Its upload result completed with exact receiver accounting
but collapsed under the independently jittered and lossy paths; it is reported
rather than replaced and is not used for a product ratio. MPTCP is not an
encrypted proxy.

### Per-flow TCP limits

In the measured releases, a lone TCP endpoint targeted the configured maximum
as regular members; the historical default maximum was three. When several
endpoints were configured, each endpoint contributed a regular primary and
correlated siblings remained ready backups. At collection time, `1-3` and
`3-3` both targeted three members. Live evidence decided which eligible
members received work; readiness did not create a fixed traffic share.

#### Independent 500 Mbps per-flow limits

| Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: |
| Download (Mbps) | 346.354 | 904.757 | 902.027 |
| Upload (Mbps) | 338.889 | 931.537 | 901.967 |

#### Shared 200 Mbps bottleneck

| Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: |
| Download (Mbps) | 157.495 | 164.943 | 171.062 |
| Upload (Mbps) | 157.938 | 153.374 | 165.704 |

The three-carrier forms aggregate independent per-flow capacity and stay near
the same aggregate ceiling when all connections share one bottleneck. No rate
or percentage from these runs is a production threshold.

### Changing link conditions at scale

Ten TCP and ten QUIC links changed bandwidth, latency, jitter, and loss across
five deterministic epochs.

| Rate/link (Mbps) | Transport | Download (Mbps) | Upload (Mbps) |
| ---: | --- | ---: | ---: |
| 30–100 | MPP/TCP+QUIC (default) | 346.911 | 295.621 |
| 300–1,000 | MPP/TCP+QUIC (default) | 1,476.501 | 517.327 |
| 3,000–10,000 | MPP/TCP+QUIC (default) | 2,055.416 | 559.959 |

For these historical runs, configured topology established regular and backup
eligibility. Fresh directional delivery evidence ranked members inside the
eligible tier. Neither source addresses nor fixed bandwidth thresholds
participated in that measured decision.

### Asymmetric links

Link A was 200 Mbps download / 20 Mbps upload. Link B was 20 Mbps download /
200 Mbps upload. A single Xray or Hysteria2 connection remained on Link A in
both directions; it was not moved to the directionally faster endpoint between
measurements. MPTUNNEL received both links in one configuration.

| System | Configured links | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: |
| Xray 26.3.27 VMess/TCP | A | 181.902 | ≥17.638 |
| Hysteria2 2.10.0 Brutal | A | 188.888 | ≥18.762 |
| MPTUNNEL MPP/TCP | A + B | 198.504 | 196.630 |

MPTUNNEL sent 90.7% of download traffic over Link A and 90.7% of upload
traffic over Link B. Independent single-fast-link MPTUNNEL controls delivered
183.436 Mbps download on Link A and 180.222 Mbps upload on Link B; adding both
links did not reduce either direction. Interface accounting, not source
address or configured bandwidth, supplies the path-share evidence.

### Latency and throughput together

The ordinary path was shaped to 80 Mbps, 40 ms RTT, 10 ms jitter, and 0.5%
loss. The adverse high-capacity path was shaped to 500 Mbps, 280 ms RTT, 20 ms
jitter, and 10% loss. Each control used the measured release's default
TCP+QUIC paths while bulk HTTP, short HTTP, persistent TCP echo, and UDP ran
together for 30 seconds.

| Available links | Bulk (Mbps) | TCP p50/p95 (ms) | TCP | HTTP p50/p95 (ms) | HTTP | UDP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Ordinary only | 60.886 | 103 / 217 | 60/60 | 444 / 1,026 | 45/45 | 102/102 |
| Adverse only | 98.512 | 452 / 1,868 | 35/35 | 1,835 / 2,686 | 9/11 | 14/18 |
| Both | 160.002 | 173 / 318 | 60/60 | 376 / 838 | 53/53 | 205/205 |

The two single-link bulk controls sum to 159.398 Mbps; the two-link control
delivered 160.002 Mbps. With both links available, every TCP, HTTP, and UDP
check completed, interactive latency stayed far below the adverse-only
control, and bulk retained the combined capacity. This is a direct control for
latency-aware service and throughput aggregation, not an inference from path
counters alone.

### Disruption recovery

| Condition | Transport | Download (Mbps) | Upload (Mbps) | Receiver gap DL/UL (ms) |
| --- | --- | ---: | ---: | ---: |
| Port hop | MPP/QUIC | 2,818.042 | 2,798.515 | 11 / 24 |
| Blackhole | MPP/TCP+QUIC (default) | 278.488 | — | 636 / — |
| Latency change | MPP/TCP+QUIC (default) | 235.408 | — | 1,489 / — |
| Repeated link changes | MPP/TCP+QUIC (default) | 248.291 | — | 869 / — |
| Blackhole | MPP/TCP | 272.124 | 274.925 | 1,136 / 369 |
| Latency change | MPP/TCP | 253.904 | 221.276 | 315 / 1,625 |

| Default mixed condition | TCP echo | HTTP | Datagrams |
| --- | ---: | ---: | ---: |
| Blackhole | 60/60 | 72/72 | 228/229 |
| Latency change | 60/60 | 93/94 | 241/243 |
| Repeated link changes | 47/47 | 81/83 | 217/219 |

The latency-change row includes a 900 ms one-way, 10% loss epoch.
Persistent TCP echo streams stayed attached in every mixed disruption run.
The accepted historical blackhole row has clean source and host provenance;
every reliable check completed, while one unreliable datagram traversing the
blackholed path was not delivered.
The repeated-change HTTP misses began within deliberate blackholes and reached
their application deadlines before service returned. Datagram counts expose
expected loss during those same unavailable intervals.

| Event | Duration (s) | Continuity result |
| --- | ---: | --- |
| Total carrier outage | 5 | Existing flow recovered 1/1 |
| Server/client restart | — | Post-restart flows 2/2 |

In these measured releases, QUIC used native migration where available and TCP
established a fresh carrier. MPP retained exact logical ranges and resumed on
the replacement. New inbound connections were rejected while no outbound
carrier was available, while existing connections remained until their normal
timeout or recovery.

### Short connections

| Concurrency | Object (KiB) | Duration (s) | Requests | Rejected | Failed | Deadline (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 32 | 30 | 90/90 | 0 | 0 | 3,000 |
| 20 | 1,024 | 60 | 755/755 | 0 | 0 | — |

The first run opened ten requests every three seconds. All 90 requests
completed inside the three-second deadline; the slowest batch took 0.681
seconds. The second kept twenty one-MiB transfers active and replaced each
completed request immediately.

### Local processing capacity

No rate, delay, jitter, or loss was configured. These rows measure the local
container and host path, not a public Internet link.

| System | Transport | Carriers | Download (Gbps) | Upload (Gbps) |
| --- | --- | ---: | ---: | ---: |
| Direct | TCP | 1 | 21.393 | 22.113 |
| Xray 26.3.27 | VMess/TCP | 1 | 8.044 | ≥6.952 |
| Hysteria2 2.10.0 | QUIC | 1 | 2.714 | ≥2.816 |
| MPTUNNEL | MPP/TCP (`1-1`) | 1 | 7.185 | 6.443 |
| MPTUNNEL | MPP/TCP (default) | 3 | 5.584 | 6.328 |
| MPTUNNEL | MPP/QUIC | 1 | 2.867 | 2.796 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 4 | 4.921 | 5.190 |

The measured MPP data plane performed encryption, framing, sequencing,
scheduling, Data ACKs, flow control, and bounded recovery. Extra unshaped
carriers added processing and ordering work without adding link capacity.
Independently shaped links provided the aggregation opportunity measured above.

## Current diagnostic profiles

The optional `internet-five-path-load-coupled-epoch-N` diagnostic reuses the
same seeded five-path schedule but separates the link into a one-class HTB
rate limiter and a finite seeded-netem child. Netem supplies the scheduled
propagation, jitter floor, and exogenous packet effects. Below the scheduled
rate, packets see that floor. Sustained excess offered load consumes the
finite queue, so queue residence adds delay variation and overflow adds loss.
`MPTUNNEL_LAB_INTERNET_LOAD_QUEUE_DELAY` selects the additional full-size
packet queue horizon (default `100ms`); it is an input to diagnosis, not a
product threshold or a result pass/fail cap. Select this opt-in profile through
the heterogeneous runner; the random-Internet matrix keeps its static seeded
profile unless a separate load-coupled cohort is requested. Neither mode is a
Product threshold or an implicit performance cap.

## Current TCP ranged-carrier rotation

The current ranged-TCP lifecycle uses make-before-break replacement. During
planned rotation, a group may overlap at most one temporary authenticated
successor while its predecessor drains; the configured current-member maximum
remains unchanged. The successor starts with only its own readiness and live
delivery evidence. It does not inherit the
predecessor’s rate, RTT, congestion window, flight, ACK, queue, or path-score
evidence. This is the current lifecycle contract, not a fresh performance
claim inferred from the historical tables above.

## High-BDP resource windows

The four 64 MiB defaults are configurable local safety envelopes, not wire or
protocol limits:

- `max_stream_window_bytes` bounds per-direction logical receive credit;
- `max_repair_bytes` bounds retained sender data;
- `max_reorder_bytes` bounds receiver reordering; and
- `max_path_flight_bytes` bounds one path's MPP service flight.

Approximate aggregate BDP bytes as `sum(rate_bps × RTT_seconds) / 8`.
Aggregate admitted work is bounded by the applicable stream, repair, and
reorder envelopes plus the sum of independently applicable per-path flight
envelopes. Each path still needs enough `max_path_flight_bytes` for its own BDP.
Raise the relevant fields coherently on both endpoints only when diagnostics
show a window limit; `max_path_flight_bytes` must not exceed
`max_repair_bytes`. Higher values increase worst-case retained memory, so RAM
availability alone is not a sound automatic sizing signal.

At 64 MiB, one raw BDP is covered up to about 537 ms at 1 Gbps or 53.7 ms at
10 Gbps. At 10 Gbps and 100 ms RTT, a 64 MiB logical window has a rough
window/RTT ceiling of 5.37 Gbps and must be raised for line rate. See the
[reference configuration](../examples/config.reference.toml) and
[operations guide](OPERATIONS.md) for the operator surface.

## Reading the results

Each table or time-series figure establishes only its exact recorded cohort:
binary revision, configuration, carrier mix, direction, workload, duration,
network profile, and accepted host and source provenance. A v0.4.4 time-series
figure is therefore evidence only for its own cohort. It does not update the
historical tables above, establish behavior outside its observation window, or
prove the cause of a rate or latency change by itself.

Interpret samples at the interval recorded by their cohort. The historical
fixed-profile runs used 200 ms delivery samples and one-second management-rate
samples; a derived series may aggregate those samples further. Short zero or
spike buckets can reflect application buffering or ACK release. Diagnose
interruptions with ordered-delivery gaps, native service, Data ACK progress,
queue and flight ownership, interface drops, and the path lifecycle together.

Movement around five percent in the historical repetitions was treated as
ordinary run-to-run variance, not a pass threshold or a current regression
cap. Production contains no fixed Mbps target or fixed percentage threshold.

## Limits

These measurements do not establish:

- v0.4.4 performance from the historical v0.2.1–v0.2.2 tables;
- performance outside any cohort's exact conditions or after its measured
  observation window;
- causality from a throughput or latency trace alone;
- performance on every public route or access technology;
- native Windows, Wintun, macOS Network Extension, or Android `VpnService`
  performance;
- identical host capacity for every packaged target;
- rankings against products not included in the matched tables; or
- an independent security audit of MPP.

The portable runtime is the correctness fallback on every supported platform.
Native host telemetry and VPN integration are used only where they provide a
real platform benefit.
