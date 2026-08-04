# Performance

MPTUNNEL aggregates independent links and keeps active traffic attached when a
carrier changes or disappears. Results depend on path conditions, direction,
workload, host capacity, and the native TCP or QUIC implementation.

## Test conditions

Measurements used isolated GNU/Linux containers on one host. Rates are bytes
delivered to the receiver divided by full completion time; configured bandwidth
is never reported as throughput. Matched proxy comparisons used the same
objects, two flows, and a 20-second load window. Each cell reports one valid
comparable directional run, not a best-of selection. Repetitions were used only
to classify outliers.

## One 500 Mbps path

180 ms one-way delay, 20 ms jitter, 1% loss.

| System | Transport | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: |
| Direct | TCP | 207.720 | 201.212 |
| Xray 26.3.27 | VMess/TCP | 218.716 | ≥151.299 |
| Hysteria2 2.10.0 | QUIC | 87.525 | ≥105.615 |
| MPTUNNEL | MPP/TCP | 225.564 | 253.758 |
| MPTUNNEL | MPP/QUIC | 252.859 | 209.684 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 284.982 | 305.017 |

The default delivered 1.37× direct TCP download and 1.52× upload goodput.
MPP/QUIC delivered 2.89× Hysteria2's download. The Xray and Hysteria2
uploads are receiver-confirmed lower bounds and are excluded from ratios.

Multiple TCP carriers on one route can overcome a per-flow limiter, but they
do not create independent link capacity or remove native TCP head-of-line
recovery. The default also has QUIC available when it is the better carrier.

## Five 500 Mbps paths

180 ms one-way delay, 20 ms jitter, 0% loss per path.

| System | Transport | Shaped links | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: | ---: |
| Linux MPTCP | TCP | 5 | 357.424 | 382.493 |
| MPTUNNEL | MPP/TCP | 5 | 641.250 | 526.701 |
| MPTUNNEL | MPP/QUIC | 5 | 602.712 | 748.958 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 5 | 677.370 | 748.829 |

The default delivered 1.90× MPTCP download and 1.96× upload goodput. The
MPTCP topology used one initial path plus four aligned address pairs and
established additional subflows. Every MPTUNNEL row completed with exact
receiver accounting.

## TCP carrier pool

A lone TCP endpoint maintains the configured maximum as regular members; the
default maximum is three. When several endpoints are configured, each endpoint
contributes a regular primary and correlated siblings remain ready backups.
The range minimum is obsolete in the current runtime: `1-3` and `3-3` both
target three members. Live evidence decides which eligible members receive
work; readiness never creates a fixed traffic share.

### Independent 500 Mbps per-flow limits

| Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: |
| Download (Mbps) | 355.923 | 886.246 | 794.061 |
| Upload (Mbps) | 347.045 | 823.774 | 901.910 |

### Shared 200 Mbps bottleneck

| Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: |
| Download (Mbps) | 153.965 | 165.220 | 164.624 |
| Upload (Mbps) | 157.547 | 151.999 | 167.363 |

The three-carrier forms aggregate independent per-flow capacity and stay near
the same aggregate ceiling when all connections share one bottleneck. No rate
or percentage from these runs is a production threshold.

## Twenty varying links

Ten TCP and ten QUIC links changed bandwidth, latency, jitter, and loss across
five deterministic epochs.

| Rate/link (Mbps) | Transport | Download (Mbps) | Upload (Mbps) |
| ---: | --- | ---: | ---: |
| 30–100 | MPP/TCP+QUIC (default) | 356.397 | 247.699 |
| 300–1,000 | MPP/TCP+QUIC (default) | 1,403.046 | 592.790 |
| 3,000–10,000 | MPP/TCP+QUIC (default) | 2,277.788 | 723.070 |

Configured topology establishes regular and backup eligibility. Fresh
directional delivery evidence ranks members inside the eligible tier. Neither
source addresses nor fixed bandwidth thresholds participate in that decision.

## Asymmetric links

The two links were 200/20 and 20/200 Mbps, so the faster member reversed with
traffic direction.

| Direction | Single fast link (Mbps) | Multipath (Mbps) | Fast-link share |
| --- | ---: | ---: | ---: |
| Download | 143.262 | 153.998 | 90.7% |
| Upload | 148.979 | 156.716 | 90.3% |

The interface accounting shows that upload and download independently selected
their faster member without using a source-address heuristic.

## Continuity

| Condition | Transport | Download (Mbps) | Upload (Mbps) | Receiver gap DL/UL (ms) |
| --- | --- | ---: | ---: | ---: |
| Port hop | MPP/QUIC | 2,759.843 | 2,754.965 | 8 / — |
| Blackhole | MPP/TCP+QUIC (default) | 223.127 | — | 371 / — |
| Latency change | MPP/TCP+QUIC (default) | 132.196 | — | 547 / — |
| Repeated link changes | MPP/TCP+QUIC (default) | 159.741 | — | 2,609 / — |
| Blackhole | MPP/TCP | 288.669 | 276.831 | 744 / 899 |
| Latency change | MPP/TCP | 287.350 | 200.169 | 723 / 2,583 |

| Default mixed condition | TCP echo | HTTP | Datagrams |
| --- | ---: | ---: | ---: |
| Blackhole | 60/60 | 93/93 | 231/234 |
| Latency change | 60/60 | 102/102 | 303/304 |
| Repeated link changes | 40/40 | 34/37 | 86/91 |

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
| 20 | 1,024 | 60 | 686/686 | 0 | 0 | — |

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
| MPTUNNEL | MPP/TCP (`1-1`) | 1 | 6.580 | 6.715 |
| MPTUNNEL | MPP/TCP (default) | 3 | 5.550 | 6.393 |
| MPTUNNEL | MPP/QUIC | 1 | 2.825 | 2.721 |
| MPTUNNEL | MPP/TCP+QUIC (default) | 4 | 5.026 | 6.173 |

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
