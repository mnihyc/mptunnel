# Performance

MPTUNNEL aggregates independent links and keeps active traffic attached when a
carrier changes or disappears. Results depend on path conditions, direction,
workload, host capacity, and the native TCP or QUIC implementation.

## Test conditions

Measurements used isolated GNU/Linux containers on one host. Rates are bytes
delivered to the receiver divided by full completion time; configured bandwidth
is never reported as throughput. Matched proxy comparisons used the same
objects, two flows, and a 20-second load window. Each cell reports one valid
directional run, not a best-of selection. Repetitions were used only to classify
outliers. The one-path and local TCP `1-1` MPTUNNEL rows use the default
shared-transport-key profile.

## Matched proxy conditions

Each path was shaped to 500 Mbps with zero jitter. Every cell used two parallel
downloads for 20 seconds. Delay was applied once in each direction, so 20 ms
and 180 ms one-way shaping correspond to approximately 40 ms and 360 ms RTT.
MPTUNNEL used its default TCP+QUIC configuration.

| RTT (ms) | Loss | Xray 26.3.27 VMess/TCP | Hysteria2 2.10.0 | MPTUNNEL TCP+QUIC |
| ---: | ---: | ---: | ---: | ---: |
| 40 | 0% | 461.341 | 461.425 | 439.091 |
| 40 | 10% | 406.613 | 421.454 | 405.129 |
| 360 | 0% | 355.414 | 251.473 | 346.164 |
| 360 | 10% | 25.000 | 71.960 | 225.025 |

MPTUNNEL is 4.8% below the fastest baseline on the clean 40 ms path. Under
360 ms RTT and 10% loss it delivers 3.13× Hysteria2 and 9.00× Xray/VMess.
A health-probe deadline applies only to that probe; complete carrier setup uses
the adaptive path-open budget derived from observed path timing.

The 40 ms/0% and 360 ms/10% rows use clean source `9c2265b`; the other two use
clean source `32f7568`. The only Core change between them assigns full carrier
setup to the adaptive path-open deadline instead of the health-probe deadline;
established-path congestion control and scheduling are unchanged.

## Link aggregation

180 ms one-way delay, 20 ms jitter, 0% loss per path.

| System | Transport | Shaped links | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: | ---: |
| MPTUNNEL | MPP/TCP+QUIC (default) | 1 | 370.207 | 398.793 |
| MPTUNNEL | MPP/TCP | 5 | 841.572 | 562.796 |
| MPTUNNEL | MPP/QUIC | 5 | 623.590 | 730.726 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 5 | 662.573 | 794.876 |

Five links raised default MPTUNNEL goodput by 1.79× download and 1.99× upload.
Every MPTUNNEL row completed with exact receiver accounting.

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
| Download (Mbps) | 345.465 | 901.519 | 744.216 |
| Upload (Mbps) | 338.671 | 873.097 | 890.466 |

### Shared 200 Mbps bottleneck

| Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: |
| Download (Mbps) | 158.931 | 164.476 | 167.164 |
| Upload (Mbps) | 157.099 | 172.327 | 150.939 |

The three-carrier forms aggregate independent per-flow capacity and stay near
the same aggregate ceiling when all connections share one bottleneck. No rate
or percentage from these runs is a production threshold.

## Changing link conditions at scale

Ten TCP and ten QUIC links changed bandwidth, latency, jitter, and loss across
five deterministic epochs.

| Rate/link (Mbps) | Transport | Download (Mbps) | Upload (Mbps) |
| ---: | --- | ---: | ---: |
| 30–100 | MPP/TCP+QUIC (default) | 350.135 | 245.383 |
| 300–1,000 | MPP/TCP+QUIC (default) | 1,346.848 | 726.616 |
| 3,000–10,000 | MPP/TCP+QUIC (default) | 2,000.420 | 597.670 |

Configured topology establishes regular and backup eligibility. Fresh
directional delivery evidence ranks members inside the eligible tier. Neither
source addresses nor fixed bandwidth thresholds participate in that decision.

## Asymmetric links

The two links were 200/20 and 20/200 Mbps, so the faster member reversed with
traffic direction.

| Direction | Single fast link (Mbps) | Multipath (Mbps) | Fast-link share |
| --- | ---: | ---: | ---: |
| Download | 141.161 | 147.748 | 90.1% |
| Upload | 141.258 | 149.680 | 89.4% |

The interface accounting shows that upload and download independently selected
their faster member without using a source-address heuristic.

## Latency and throughput together

The low-latency path was shaped to 80 Mbps, 20 ms one-way delay, and 2 ms
jitter. The high-throughput path was 500 Mbps, 180 ms one-way delay, and 20 ms
jitter. Both used zero loss. Each orientation ran bulk HTTP, short HTTP,
persistent TCP echo, and UDP traffic together for 30 seconds.

| Low-latency transport | High-throughput transport | Bulk (Mbps) | Interactive p50/p95 (ms) | Short HTTP p50/p95 (ms) | Echo / HTTP / UDP |
| --- | --- | ---: | ---: | ---: | ---: |
| TCP | QUIC | 289.061 | 117 / 361 | 218 / 654 | 60/60 / 67/67 / 176/176 |
| QUIC | TCP | 288.886 | 48 / 439 | 105 / 112 | 57/57 / 130/130 / 399/399 |

Bulk exceeded the low-latency path's 80 Mbps ceiling by 3.61× in both
orientations, so the high-throughput path was contributing under load. Median
interactive latency stayed below the high-throughput path's approximately
360 ms RTT. Reversing TCP and QUIC roles preserved both outcomes, ruling out a
fixed transport-family preference. Tail latency includes concurrent bulk
service and ordered-delivery effects, so it does not claim that every
interactive sample remained on one carrier.

## Disruption recovery

| Condition | Transport | Download (Mbps) | Upload (Mbps) | Receiver gap DL/UL (ms) |
| --- | --- | ---: | ---: | ---: |
| Port hop | MPP/QUIC | 2,818.042 | 2,798.515 | 11 / 24 |
| Blackhole | MPP/TCP+QUIC (default) | 204.833 | — | 366 / — |
| Latency change | MPP/TCP+QUIC (default) | 167.651 | — | 3,310 / — |
| Repeated link changes | MPP/TCP+QUIC (default) | 186.452 | — | 1,501 / — |
| Blackhole | MPP/TCP | 272.124 | 274.925 | 1,136 / 369 |
| Latency change | MPP/TCP | 253.904 | 221.276 | 315 / 1,625 |

| Default mixed condition | TCP echo | HTTP | Datagrams |
| --- | ---: | ---: | ---: |
| Blackhole | 60/60 | 108/108 | 240/243 |
| Latency change | 60/60 | 94/94 | 257/259 |
| Repeated link changes | 48/48 | 90/92 | 280/282 |

The latency-change row includes a 900 ms one-way, 10% loss epoch.
Persistent TCP echo streams stayed attached in every mixed disruption run.
The repeated-change HTTP misses began within deliberate blackholes and reached
their application deadlines before service returned. Datagram counts expose
expected loss during those same unavailable intervals.

| Event | Duration (s) | Existing flows | New flows |
| --- | ---: | ---: | ---: |
| Total carrier outage | 5 | 1/1 | Rejected offline |
| Client/server restart | — | 2/2 | — |

QUIC uses native migration where available. TCP establishes a fresh carrier;
MPP retains exact logical ranges and resumes on the replacement. New inbound
connections are rejected while no outbound carrier is available, while
existing connections remain until their normal timeout or recovery.

## Short connections

| Concurrency | Object (KiB) | Duration (s) | Requests | Rejected | Failed | Deadline (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 32 | 30 | 90/90 | 0 | 0 | 3,000 |
| 20 | 1,024 | 60 | 739/739 | 0 | 0 | — |

The first run opened ten requests every three seconds. Every batch completed
inside its deadline. The second kept twenty one-MiB transfers active and
replaced each completed request immediately.

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
