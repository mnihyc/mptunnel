# Performance

MPTUNNEL is designed to aggregate independent links and preserve active traffic
when a carrier changes or disappears. Results depend on path conditions, host
capacity, direction, workload, and the native TCP or QUIC implementation.

## Test conditions

Measurements used isolated GNU/Linux containers on one host. Rates are bytes
delivered to the receiver divided by the full completion time. Product
comparisons used the same objects, two flows, and a 20-second load window.
Configured bandwidth is never reported as delivered throughput.

## One 500 Mbps path

180 ms one-way delay, 20 ms jitter, 1% loss.

| System | Transport | Download (Mbps) | Upload (Mbps) | Directions |
| --- | --- | ---: | ---: | ---: |
| Direct | TCP | 198.820 | 220.405 | 2/2 |
| Xray 26.3.27 | VMess/TCP | 209.203 | — | 1/2 |
| MPTUNNEL | MPP/TCP | 123.628 | 103.307 | 2/2 |
| Hysteria2 2.10.0 | QUIC | 96.371 | ≥115.109 | 1/2 |
| MPTUNNEL | MPP/QUIC | 240.475 | 180.017 | 2/2 |

MPP/QUIC delivered 2.50× Hysteria2's download goodput. MPP/TCP did not beat
Xray on this single path. Xray upload ended before receiver completion and is
omitted. Hysteria2 upload is the receiver-confirmed lower bound shown above;
neither incomplete upload is used for a ratio.

Single-path TCP still has native head-of-line recovery. MPP adds framing, Data
ACKs, flow control, and relay work without gaining another link in this case.
Its advantage appears when independent capacity or continuity is available.

## Five 500 Mbps paths

180 ms one-way delay, 20 ms jitter, 0% loss per path.

| System | Transport | Paths | Download (Mbps) | Upload (Mbps) | Directions |
| --- | --- | ---: | ---: | ---: | ---: |
| Linux MPTCP | TCP | 5 | 302.101 | 434.432 | 2/2 |
| MPTUNNEL | MPP/TCP | 5 | 732.433 | 508.562 | 2/2 |
| MPTUNNEL | MPP/QUIC | 5 | 629.507 | 707.077 | 2/2 |

MPP/TCP delivered 2.42× MPTCP download goodput and 1.17× its upload goodput.
All MPTUNNEL rows completed with exact receiver accounting. No independent
multipath QUIC product was available for a matched comparison.

## TCP carrier range

A TCP endpoint defaults to `1-3` carriers. It opens above the minimum only when
current demand and completed delivery justify another session, and retires
unused capacity through the normal carrier lifecycle.

### 100 Mbps per TCP flow

| Direction | `1-1` (Mbps) | `1-3` (Mbps) |
| --- | ---: | ---: |
| Download | 75.239 | 111.686 |
| Upload | 78.370 | 121.669 |

### Shared 200 Mbps bottleneck

| Direction | `1-1` (Mbps) | `1-3` (Mbps) |
| --- | ---: | ---: |
| Download | 157.321 | 154.044 |
| Upload | 156.397 | 161.143 |

The per-flow case retained two carriers and gained useful capacity. The shared
bottleneck stayed at its aggregate ceiling; no third carrier was retained. No
Mbps or percentage value from these runs is a production threshold.

### Unshaped request path

| TCP range | Upload (Gbps) | Flows | Failed |
| ---: | ---: | ---: | ---: |
| `1-1` | 6.001 | 2/2 | 0 |
| `1-3` | 6.286 | 2/2 | 0 |

The default range preserves the fixed-carrier local ceiling while retaining
the ability to expand under single-flow TCP shaping.

## 20 links

Ten TCP and ten QUIC links used varied bandwidth, latency, jitter, and loss.

| Rate/link (Mbps) | Download (Mbps) | Upload (Mbps) | Directions |
| ---: | ---: | ---: | ---: |
| 30–100 | 344.534 | 210.378 | 2/2 |
| 300–1,000 | 1,178.811 | 609.004 | 2/2 |
| 3,000–10,000 | 2,261.932 | 670.693 | 2/2 |

Every configured endpoint started one carrier. TCP endpoints retained their
independent `1-3` bounds, but a second carrier was not an eager target.

## Asymmetric links

| Direction | Fast link (Mbps) | Slow link (Mbps) | Fast-link share (%) |
| --- | ---: | ---: | ---: |
| Download | 200 | 20 | 91.5 |
| Upload | 200 | 20 | 86.6 |

Direction-specific delivery measurements, rather than source addresses, allow
the preferred link to differ between upload and download.

## Continuity

| Condition | Download (Mbps) | Upload (Mbps) | TCP echo | HTTP | Datagrams | DL gap (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| QUIC port hop | 2,459.750 | 2,498.275 | — | — | — | 30 |
| TCP+QUIC blackhole | 257.755 | — | 40/40 | — | 151/153 | 777 |
| TCP+QUIC latency | 199.210 | — | 40/40 | — | 145/147 | 1,293 |
| TCP blackhole | 181.261 | 243.518 | — | — | — | — |
| TCP latency | 280.085 | 245.656 | — | — | — | — |
| TCP+QUIC handover | 224.069 | — | 32/32 | 47/47 | 134/134 | 717 |

| Event | Duration (s) | Existing flows | New flows |
| --- | ---: | ---: | ---: |
| Total carrier outage | 5 | 1/1 | 0 |
| Client/server restart | — | 2/2 | — |

Existing reliable traffic remained attached through blackholes, latency
changes, link handover, and a complete five-second carrier outage. New inbound
connections were rejected while no outbound carrier was available. QUIC uses
native connection migration; TCP establishes a fresh carrier and preserves
the MPP stream through exact retained ranges.

## Short connections

| Concurrency | Object (KiB) | Duration (s) | Requests | Rejected | Failed | Deadline (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 32 | 30 | 90/90 | 0 | 0 | 3,000 |
| 20 | 1,024 | 60 | 570/570 | 0 | 0 | — |

The first run opened ten requests every three seconds. The second maintained
twenty concurrent 1 MiB transfers for sixty seconds.

## Local processing capacity

These runs removed configured rate, delay, jitter, and loss. They measure the
local container and host path, not a public Internet link.

| System | Transport | Endpoints | Download (Gbps) | Upload (Gbps) | Directions |
| --- | --- | ---: | ---: | ---: | ---: |
| Direct | TCP | 1 | 15.153 | 23.364 | 2/2 |
| Xray 26.3.27 | VMess/TCP | 1 | 8.261 | ≥7.251 | 1/2 |
| MPTUNNEL | MPP/TCP | 1 | 6.844 | 6.673 | 2/2 |
| MPTUNNEL | MPP/TCP | 5 | 5.453 | 5.999 | 2/2 |

All proxy rows reached their host processing ceiling. MPP/TCP peaked near two
CPU cores while performing encryption, framing, sequencing, scheduling, Data
ACKs, flow control, and relay work. Adding unshaped endpoints adds ordering and
carrier work without adding network capacity. Independently shaped paths
provide the aggregation opportunity shown in the five-path results above. The
remaining local gap is implementation processing cost, not a bandwidth
threshold, congestion controller, or recovery timer.

## Reading the results

Delivery samples use 200 ms intervals; management rates use one-second
intervals. Short zero/spike buckets can reflect application buffering or ACK
release. Diagnose a suspected interruption with ordered-delivery gaps, native
service, Data ACK progress, queue and flight ownership, interface drops, and
the path lifecycle together.

Movement around five percent can be ordinary run-to-run variance. It is not a
pass threshold or a hard regression cap. Production contains no fixed Mbps or
percentage target.

## Limits

These measurements do not establish:

- performance on every public route or access technology;
- native Windows, Wintun, macOS Network Extension, or Android `VpnService`
  performance;
- identical host capacity for every packaged target;
- an external multipath QUIC ranking; or
- an independent security audit of MPP.

The portable Product and MPP implementation is the correctness fallback on
every supported platform. Native host telemetry and VPN integration are used
only where they provide a real platform benefit.
