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
| Direct | TCP | 203.719 | 104.872 | 2/2 |
| Xray 26.3.27 | VMess/TCP | 215.402 | ≥207.296 | 1/2 |
| MPTUNNEL | MPP/TCP | 139.771 | 122.328 | 2/2 |
| Hysteria2 2.10.0 | QUIC | 92.626 | ≥103.035 | 1/2 |
| MPTUNNEL | MPP/QUIC | 244.596 | 229.631 | 2/2 |

MPP/QUIC delivered 2.64× Hysteria2's download goodput. MPP/TCP did not beat
Xray on this single path. The Xray and Hysteria2 uploads are receiver-confirmed
lower bounds; neither incomplete upload is used for a ratio.

Single-path TCP still has native head-of-line recovery. MPP adds framing, Data
ACKs, flow control, and relay work without gaining another link in this case.
Its advantage appears when independent capacity or continuity is available.

## Five 500 Mbps paths

180 ms one-way delay, 20 ms jitter, 0% loss per path.

| System | Transport | Paths | Download (Mbps) | Upload (Mbps) | Directions |
| --- | --- | ---: | ---: | ---: | ---: |
| Linux MPTCP | TCP | 5 | 257.275 | 194.296 | 2/2 |
| MPTUNNEL | MPP/TCP | 5 | 726.106 | 533.922 | 2/2 |
| MPTUNNEL | MPP/QUIC | 5 | 596.944 | 753.061 | 2/2 |

MPP/TCP delivered 2.82× MPTCP download goodput and 2.75× its upload goodput.
The MPTCP topology used one initial path plus four aligned address pairs; all
five carried payload. All MPTUNNEL rows completed with exact receiver
accounting. No independent multipath QUIC product was available for a matched
comparison.

## TCP carrier pool

A TCP endpoint defaults to three ordinary carriers (currently spelled `1-3`).
The maximum is the healthy target; live scheduling evidence decides which
ready carriers receive traffic.

### 100 Mbps per TCP flow

| Direction | `1-1` | `1-3` | `3-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: | ---: |
| Download (Mbps) | 75.139 | 224.123 | 225.158 | 222.027 |
| Upload (Mbps) | 77.518 | 215.666 | 218.047 | 220.080 |

### Shared 200 Mbps bottleneck

| Direction | `1-1` | `1-3` | `3-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: | ---: |
| Download (Mbps) | 154.469 | 159.751 | 165.378 | 151.656 |
| Upload (Mbps) | 157.680 | 165.049 | 162.348 | 171.505 |

The three-carrier forms aggregate the per-flow limit and remain near the same
aggregate ceiling when all carriers share one bottleneck. `1-3` and `3-3` are
the same three-carrier pool in the current runtime. Three explicit one-carrier
endpoints provide the corresponding topology control. No Mbps or percentage
value from these runs is a production threshold.

### Unshaped request path

| Direction | `1-1` | `1-3` | `3-3` | 3 × `1-1` |
| --- | ---: | ---: | ---: | ---: |
| Download (Gbps) | 6.590 | 6.210 | 6.284 | 5.885 |
| Upload (Gbps) | 6.557 | 6.630 | 6.351 | 6.691 |

All four forms reach the local processing ceiling. Ready pool members add no
fixed traffic share, MPP pacing layer, or bandwidth threshold.

## 20 links

Ten TCP and ten QUIC links used varied bandwidth, latency, jitter, and loss.

| Rate/link (Mbps) | Download (Mbps) | Upload (Mbps) | Directions |
| ---: | ---: | ---: | ---: |
| 30–100 | 378.764 | 292.168 | 2/2 |
| 300–1,000 | 1,395.311 | 610.051 | 2/2 |
| 3,000–10,000 | 2,882.347 | 731.499 | 2/2 |

Each configured endpoint remains independently eligible. Live directional
completion evidence, not source addresses or pool membership, decides which
ready carriers receive traffic.

## Asymmetric links

| Direction | Fast link (Mbps) | Slow link (Mbps) | Fast-link share (%) |
| --- | ---: | ---: | ---: |
| Download | 200 | 20 | 90.0 |
| Upload | 200 | 20 | 91.1 |

Direction-specific delivery measurements, rather than source addresses, allow
the preferred link to differ between upload and download.

## Continuity

| Condition | Download (Mbps) | Upload (Mbps) | TCP echo | HTTP | Datagrams | DL gap (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| QUIC port hop | 2,605.402 | 2,662.391 | — | — | — | 8 |
| TCP+QUIC blackhole | 153.510 | — | 60/60 | 98/98 | 252/253 | 532 |
| TCP+QUIC latency | 187.348 | — | 60/60 | 115/115 | 314/315 | 1,995 |
| TCP blackhole | 275.157 | 302.570 | — | — | — | — |
| TCP latency | 280.085 | 245.656 | — | — | — | — |
| TCP+QUIC handover | 221.587 | — | 53/53 | 89/90 | 218/221 | 1,117 |

| Event | Duration (s) | Existing flows | New flows |
| --- | ---: | ---: | ---: |
| Total carrier outage | 5 | 1/1 | 0 |
| Client/server restart | — | 2/2 | — |

Existing reliable traffic remained attached through blackholes, latency
changes, link handover, and a complete five-second carrier outage. New inbound
connections were rejected while no outbound carrier was available. QUIC uses
native connection migration; TCP establishes a fresh carrier and preserves
the MPP stream through exact retained ranges.

The handover HTTP miss began inside the deliberate four-second blackhole and
reached its 2.5-second application deadline before service returned; its
established TCP echo stream remained connected for all 53 exchanges.

## Short connections

| Concurrency | Object (KiB) | Duration (s) | Requests | Rejected | Failed | Deadline (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 32 | 30 | 90/90 | 0 | 0 | 3,000 |
| 20 | 1,024 | 60 | 777/777 | 0 | 0 | — |

The first run opened ten requests every three seconds. The second maintained
twenty concurrent 1 MiB transfers for sixty seconds.

## Local processing capacity

These runs removed configured rate, delay, jitter, and loss. They measure the
local container and host path, not a public Internet link.

| System | Transport | Endpoints | Download (Gbps) | Upload (Gbps) | Directions |
| --- | --- | ---: | ---: | ---: | ---: |
| Direct | TCP | 1 | 23.334 | 23.048 | 2/2 |
| Xray 26.3.27 | VMess/TCP | 1 | 8.525 | ≥7.197 | 1/2 |
| MPTUNNEL | MPP/TCP | 1 | 6.915 | 6.933 | 2/2 |
| MPTUNNEL | MPP/TCP | 5 | 5.726 | 6.011 | 2/2 |

All proxy rows reached their host processing ceiling. MPP/TCP performs
encryption, framing, sequencing, scheduling, Data ACKs, flow control, and relay
work. Adding unshaped endpoints adds ordering and carrier work without adding
network capacity. Independently shaped paths provide the aggregation
opportunity shown in the five-path results above. The remaining local gap is
implementation processing cost, not a bandwidth threshold, congestion
controller, or recovery timer.

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
