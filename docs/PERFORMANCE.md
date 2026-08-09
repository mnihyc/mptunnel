# Performance

MPTUNNEL aggregates independent links and keeps active traffic attached when a
carrier changes or disappears. Results depend on path conditions, direction,
workload, host capacity, and the native TCP or QUIC implementation.

## Test conditions

Measurements used isolated GNU/Linux containers on one host. Rates are bytes
delivered to the receiver divided by full completion time; configured bandwidth
is never reported as throughput. The matched one-link and aggregation rows used
the same objects, two flows, and a 20-second load window. The asymmetric and
mixed-workload rows used 30-second windows. Each cell reports one valid
directional run, not a best-of selection. Repetitions were used only to classify
outliers. Xray-core 26.3.27 used VMess/TCP. Hysteria2 2.10.0 used Brutal with
client bandwidth equal to the shaped directional capacity. MPTUNNEL used the
default shared-transport-key profile unless a transport is named explicitly.

## Matched proxy conditions

Every path was shaped to 500 Mbps. Delay and jitter were applied once in each
direction; the table reports the approximate RTT and the configured
per-direction jitter. Every cell used two parallel downloads for 20 seconds.
MPTUNNEL used its default TCP+QUIC configuration.

| RTT (ms) | Jitter (ms) | Loss | Xray 26.3.27 VMess/TCP | Hysteria2 2.10.0 Brutal | MPTUNNEL TCP+QUIC |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 40 | 10 | 0.5% | 441.353 | 463.502 | 414.200 |
| 280 | 20 | 10% | 70.833 | 96.288 | 194.504 |

MPTUNNEL is 10.6% below the fastest baseline on the ordinary path. Under
280 ms RTT, 20 ms jitter, and 10% loss it delivers 2.02× Hysteria2 and 2.75×
Xray/VMess. Every row has valid host, source, and receiver accounting.

## Link aggregation

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

Default MPTUNNEL scales 1.86×/1.46× from one to two links and 3.30×/3.25×
from one to five links for download/upload. Every MPTUNNEL result completed
with exact receiver accounting. The Xray and Hysteria2 upload sessions did not
close inside the completion window, so their receiver-delivered values are
lower bounds and are excluded from ratios.

The Linux kernel MPTCP control had additional subflows confirmed by runtime
`ss -M` evidence. Its upload result completed with exact receiver accounting
but collapsed under the independently jittered and lossy paths; it is reported
rather than replaced and is not used for a product ratio. MPTCP is not an
encrypted proxy.

## Per-flow TCP limits

A lone TCP endpoint maintains the configured maximum as regular members; the
default maximum is three. When several endpoints are configured, each endpoint
contributes a regular primary and correlated siblings remain ready backups.
The range minimum is obsolete in the current runtime: `1-3` and `3-3` both
target three members. Live evidence decides which eligible members receive
work; readiness never creates a fixed traffic share.

### Independent 500 Mbps per-flow limits

| Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: |
| Download (Mbps) | 346.354 | 904.757 | 902.027 |
| Upload (Mbps) | 338.889 | 931.537 | 901.967 |

### Shared 200 Mbps bottleneck

| Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: |
| Download (Mbps) | 157.495 | 164.943 | 171.062 |
| Upload (Mbps) | 157.938 | 153.374 | 165.704 |

The three-carrier forms aggregate independent per-flow capacity and stay near
the same aggregate ceiling when all connections share one bottleneck. No rate
or percentage from these runs is a production threshold.

## Changing link conditions at scale

Ten TCP and ten QUIC links changed bandwidth, latency, jitter, and loss across
five deterministic epochs.

| Rate/link (Mbps) | Transport | Download (Mbps) | Upload (Mbps) |
| ---: | --- | ---: | ---: |
| 30–100 | MPP/TCP+QUIC (default) | 346.911 | 295.621 |
| 300–1,000 | MPP/TCP+QUIC (default) | 1,476.501 | 517.327 |
| 3,000–10,000 | MPP/TCP+QUIC (default) | 2,055.416 | 559.959 |

Configured topology establishes regular and backup eligibility. Fresh
directional delivery evidence ranks members inside the eligible tier. Neither
source addresses nor fixed bandwidth thresholds participate in that decision.

## Asymmetric links

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

## Latency and throughput together

The ordinary path was shaped to 80 Mbps, 40 ms RTT, 10 ms jitter, and 0.5%
loss. The adverse high-capacity path was shaped to 500 Mbps, 280 ms RTT, 20 ms
jitter, and 10% loss. Each control used default TCP+QUIC paths while bulk HTTP,
short HTTP, persistent TCP echo, and UDP ran together for 30 seconds.

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

## Disruption recovery

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
The current blackhole row has clean source and host provenance; every reliable
check completed, while one unreliable datagram traversing the blackholed path
was not delivered.
The repeated-change HTTP misses began within deliberate blackholes and reached
their application deadlines before service returned. Datagram counts expose
expected loss during those same unavailable intervals.

| Event | Duration (s) | Continuity result |
| --- | ---: | --- |
| Total carrier outage | 5 | Existing flow recovered 1/1 |
| Server/client restart | — | Post-restart flows 2/2 |

QUIC uses native migration where available. TCP establishes a fresh carrier;
MPP retains exact logical ranges and resumes on the replacement. New inbound
connections are rejected while no outbound carrier is available, while
existing connections remain until their normal timeout or recovery.

## Short connections

| Concurrency | Object (KiB) | Duration (s) | Requests | Rejected | Failed | Deadline (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 32 | 30 | 90/90 | 0 | 0 | 3,000 |
| 20 | 1,024 | 60 | 755/755 | 0 | 0 | — |

The first run opened ten requests every three seconds. All 90 requests
completed inside the three-second deadline; the slowest batch took 0.681
seconds. The second kept twenty one-MiB transfers active and replaced each
completed request immediately.

## Local processing capacity

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

MPP performs encryption, framing, sequencing, scheduling, Data ACKs, flow
control, and bounded recovery. Extra unshaped carriers add processing and
ordering work without adding link capacity. Independently shaped links provide
the aggregation opportunity measured above.

## TUN-L3 packet service

These diagnostic candidate measurements selected experimental
`forwarding_mode = "l3"`; they are a separate performance family from the
default L4 proxy results above. They used two unprivileged network
namespaces and real kernel TUN interfaces rather than the containerized SOCKS
or local-forwarding Product data plane. Their host load and container inventory
were valid; the uncommitted source snapshot was the sole provenance failure, so
they do not replace an accepted release baseline. The client and server each
had one exact peer route; MPTUNNEL did not install a default route, DNS policy,
NAT, or firewall rules. The TUN MTU was 1,500 bytes. ICMP used 100 small and
100 near-MTU echo requests in each direction. TCP and UDP used four parallel
flows for 15 seconds after omitting three startup seconds.

Upload means client to server; download means server to client. A TCP endpoint
targeted its default three carriers on one physical link. A mixed row used one
TCP link and one independent QUIC link, so its symmetric shaped capacity was
twice that of a TCP-only or QUIC-only row.

| Profile | Per-link rate | RTT | Jitter/direction | Loss/direction |
| --- | ---: | ---: | ---: | ---: |
| Clean | 500 Mbps | 10 ms | 0 ms | 0% |
| Ordinary | 500 Mbps | 40 ms | 10 ms | 0.5% |
| Adverse | 500 Mbps | 280 ms | 20 ms | 10% |
| Asymmetric | TCP 20/200, QUIC 200/20 Mbps upload/download | 40 ms | 5 ms | 0.5% |

### ICMP

Loss and mean RTT are shown as client-originated/server-originated because each
echo exchange traverses both directions. Near-MTU requests use a 1,472-byte
ICMP payload, producing a 1,500-byte inner IPv4 packet.

| Profile | Carrier | Small loss | Small RTT (ms) | Near-MTU loss | Near-MTU RTT (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| Clean | TCP | 0% / 0% | 10.390 / 10.410 | 0% / 0% | 10.442 / 10.437 |
| Clean | QUIC | 0% / 0% | 10.409 / 10.446 | 0% / 0% | 10.553 / 10.498 |
| Clean | TCP+QUIC | 0% / 0% | 10.431 / 10.428 | 0% / 0% | 10.567 / 10.567 |
| Ordinary | TCP | 0% / 0% | 45.819 / 40.059 | 0% / 0% | 45.114 / 51.861 |
| Ordinary | QUIC | 1% / 1% | 40.472 / 41.419 | 1% / 2% | 47.647 / 47.782 |
| Ordinary | TCP+QUIC | 0% / 0% | 50.379 / 41.920 | 0% / 0% | 40.858 / 44.387 |
| Adverse | TCP | 0% / 0% | 620.472 / 541.552 | 0% / 0% | 628.156 / 1,031.686 |
| Adverse | QUIC | 26% / 21% | 292.845 / 290.198 | 23% / 15% | 293.052 / 291.533 |
| Adverse | TCP+QUIC | 21% / 20% | 292.317 / 286.370 | 19% / 23% | 296.650 / 295.620 |
| Asymmetric | TCP+QUIC | 0% / 0% | 40.050 / 44.347 | 0% / 0% | 46.104 / 45.797 |

TCP recovers outer loss and therefore delivered every echo, at the cost of
head-of-line recovery latency on the adverse path. QUIC carries IP packets as
native unreliable datagrams. With 10% independent loss in each direction, an
unfragmented small echo exchange has an approximate 19% round-trip loss
probability; the small-echo measurements were 20–26%. The complete adverse
small and near-MTU range was 15–26%, with near-MTU fragmentation changing the
packet exposure.

### Inner TCP

| Profile | Carrier | Upload (Mbps) | Download (Mbps) |
| --- | --- | ---: | ---: |
| Clean | TCP | 441.509 | 442.685 |
| Clean | QUIC | 389.672 | 401.550 |
| Clean | TCP+QUIC | 605.370 | 628.656 |
| Ordinary | TCP | 415.337 | 370.138 |
| Ordinary | QUIC | 3.618 | 2.517 |
| Ordinary | TCP+QUIC | 333.209 | 418.287 |
| Adverse | TCP | 3.216 | 3.495 |
| Adverse | QUIC | 32.795 | 3.495 |
| Adverse | TCP+QUIC | 37.978 | 6.641 |
| Asymmetric | TCP+QUIC | 35.841 | 174.983 |

An inner TCP connection sent through QUIC datagrams performs its own loss
recovery; it is not the same service as an MPP reliable stream. This makes
inner TCP directly sensitive to random QUIC-datagram loss, although the
non-monotonic ordinary/adverse results show that loss recovery alone does not
explain every measured rate. The asymmetric row selected the fast reliable TCP
direction for download, while upload could not turn the fast but lossy QUIC
direction into equivalent inner TCP goodput.

### Inner UDP

Each cell requested 90% of its aggregate shaped rate: 450 Mbps for a single
link, 900 Mbps for two equal mixed links, and 198 Mbps for the asymmetric pair.
The table reports requested rate, receiver-delivered rate, and receiver loss.
The sender can emit less than the requested rate when carrier congestion or
backpressure intervenes.

| Profile | Carrier | Upload requested / delivered / loss | Download requested / delivered / loss |
| --- | --- | ---: | ---: |
| Clean | TCP | 450 / 444.297 / 0.586% | 450 / 442.034 / 1.504% |
| Clean | QUIC | 450 / 420.399 / 6.581% | 450 / 413.527 / 7.960% |
| Clean | TCP+QUIC | 900 / 864.380 / 3.450% | 900 / 443.569 / 48.037% |
| Ordinary | TCP | 450 / 431.863 / 4.121% | 450 / 425.661 / 4.407% |
| Ordinary | QUIC | 450 / 3.906 / 99.918% | 450 / incomplete |
| Ordinary | TCP+QUIC | 900 / 151.484 / 55.202% | 900 / 354.277 / 54.297% |
| Adverse | TCP | 450 / 15.752 / 86.969% | 450 / 141.945 / 55.723% |
| Adverse | QUIC | 450 / incomplete | 450 / incomplete |
| Adverse | TCP+QUIC | 900 / incomplete | 900 / incomplete |
| Asymmetric | TCP+QUIC | 198 / 41.893 / 70.313% | 198 / incomplete |

The clean QUIC deficit is consistent with requesting 450 Mbps while this run
delivered 413–420 Mbps. Neither netem nor the TUN qdisc recorded drops, but that
does not exclude a bounded internal attachment or QUIC queue. The incomplete
rows are retained failures, not zero-throughput values. Only one of the adverse
mixed row's three planned TCP carriers had established, so that row records
incomplete carrier establishment and command failures rather than a
steady-state aggregate.

The mixed results expose a packet-affinity question requiring a matched proof.
One configured TCP endpoint expands to three carrier members, while one QUIC
endpoint has one member. Packet flow placement currently sees those four
attachments rather than first grouping them by configured endpoint. This can
bias cold placement toward the TCP endpoint and is consistent with the clean
mixed directional imbalance, but per-flow carrier assignment was not captured
to establish causality. An endpoint/member selection model must first prove
that it improves this case without reducing lossy-path performance. No
production timing, queue, congestion, or selection parameter was changed for
these results.

## Current-candidate regression guard

The L3 candidate was also checked against the original default-L4 transport
guard and
the current default TCP+QUIC guard. These diagnostic runs used the exact
optimized candidate binary. The final pure-TCP candidate and current mixed
A-B-A cohorts had acceptable host load, no external containers, and only the
dirty source snapshot as a validity failure. Earlier candidate cohorts also had
external containers; the original `v0.2.2` binary control additionally had high
host load and seven external containers. The controls diagnose this workspace
but do not replace accepted release measurements.

The original guard used two flows for 20 seconds on 500 Mbps paths with 180 ms
one-way delay, 20 ms jitter, and no configured loss.

| Cell | Accepted (Mbps) | Candidate rerun (Mbps) | Diagnostic v0.2.2 control (Mbps) | Finding |
| --- | ---: | ---: | ---: | --- |
| TCP single download | 257.716 | 347.232 | — | No downgrade signal |
| QUIC single download | 298.191 | 276.773–287.217 | — | Variance |
| TCP five-link download | 793.576 | 643.421–755.907 | 631.461–767.816 | Matched ABBA: no candidate downgrade |
| QUIC five-link download | 742.797 | 590.626–666.076 | 651.387 | Open in both binaries |
| TCP single upload | ≥251.097 | 349.733 | — | No downgrade signal |
| QUIC single upload | ≥293.331 | 291.686 | — | Variance |
| TCP five-link upload | 537.303 | 501.532–631.483 | 555.626 | Variance |
| QUIC five-link upload | 749.681 | 616.598–658.535 | 674.170 | Open |

The current default guard used the ordinary profile above, except for its
one-link adverse row. It used two flows for 20 seconds.

| Cell | Accepted (Mbps) | Candidate rerun (Mbps) | Diagnostic v0.2.2 control (Mbps) | Finding |
| --- | ---: | ---: | ---: | --- |
| Adverse one-link download | 194.504 | 172.038 | — | Open |
| Ordinary one-link download | 414.200 | 396.424 | — | Variance |
| Ordinary one-link upload | 425.335 | 426.737 | — | No downgrade signal |
| Ordinary two-link download | 771.888 | 495.171, 775.864 | 651.207 | Variance; closing run restored the accepted level |
| Ordinary two-link upload | 621.237 | 437.808 | 478.086 | Open in both binaries |
| Ordinary five-link download | 1,365.876 | 1,555.464 | — | No downgrade signal |
| Ordinary five-link upload | 1,383.641 | 1,387.474 | — | No downgrade signal |

The closing two-link download rerun rules out a deterministic candidate ceiling
at its first low value; it does not exclude candidate-induced variance. A later
matched ABBA run compared the exact `v0.2.2` and candidate binaries in the TCP
five-link download cell without rebuilding either binary. The controls delivered
`631.461/686.330 Mbps` (mean `658.896 Mbps`) and the candidate delivered
`755.907/704.200 Mbps` (mean `730.053 Mbps`, `10.8%` higher). Every run completed,
used all five paths, and recorded zero shaping drops. This rules out a candidate
downgrade in that bounded comparison; the shared gap from the accepted
`793.576 Mbps` remains run/environment variance rather than replacement release
evidence. Across both guards there is no broad accidental Product downgrade,
but the four remaining open rows do not support a blanket no-regression claim.
No source patch was accepted from an unresolved cell.

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

Delivery samples use 200 ms intervals; management rates use one-second
intervals. Short zero or spike buckets can reflect application buffering or
ACK release. Diagnose interruptions with ordered-delivery gaps, native
service, Data ACK progress, queue and flight ownership, interface drops, and
the path lifecycle together.

Movement around five percent can be ordinary run-to-run variance. It is not a
pass threshold or regression cap. Production contains no fixed Mbps target or
fixed percentage threshold.

## Limits

These measurements do not establish:

- performance on every public route or access technology;
- native Windows, Wintun, macOS Network Extension, or Android `VpnService`
  performance;
- identical host capacity for every packaged target;
- rankings against products not included in the matched tables; or
- an independent security audit of MPP.

The portable runtime is the correctness fallback on every supported platform.
Native host telemetry and VPN integration are used only where they provide a
real platform benefit.
