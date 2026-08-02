# Performance evidence

This page records retained bounded evidence for MPTUNNEL and MPP v5. It is
deliberately narrower than a claim that one tunnel wins on every network.
Rates depend on path conditions, workload, host capacity, direction, and
native TCP or QUIC behavior.

## Measurement contract

All values are delivered-goodput observations from isolated GNU/Linux
containers on one host. Sustained runs use two flows for 30 seconds; disruption
runs use 20 seconds. Compare products only within the same stated conditions.
Results are capability evidence, not Internet-speed guarantees or SLAs.

## Single-path competitors

These systems ran adjacently on one 500 Mbps path with 180 ms one-way delay,
20 ms jitter, 1% configured loss, the same object and workload duration, and
path hints disabled. Upload values are target-confirmed goodput within the
standard one-second drain and may be lower bounds when delivery remained in
flight at the boundary.

| System | Carrier | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: |
| Direct | TCP | 231.521 | ≥240.939 |
| Xray 26.3.27, VMess | TCP | 219.529 | ≥240.849 |
| MPTUNNEL | MPP/TCP | 151.722 | ≥162.267 |
| Hysteria2 2.10.0 | QUIC | 114.506 | ≥117.541 |
| MPTUNNEL | MPP/QUIC | 212.704 | ≥207.649 |

The matched run does not support a universal single-path TCP win over Xray.
MPP/QUIC's download was ahead of the matched Hysteria2 row. The upload values
are lower bounds and do not establish a final ratio. MPTUNNEL's main
performance purpose is independent-path aggregation and recovery while one
Product flow remains intact.

## Equal-path aggregation

Five equal 500 Mbps paths used 180 ms one-way delay, 20 ms jitter, and no
configured loss.

| MPP carrier | Download (Mbps) | Upload (Mbps) |
| --- | ---: | ---: |
| TCP | 834.364 | 649.766 |
| QUIC | 648.493 | ≥738.113 |

Both directions completed. The QUIC upload is a receiver-confirmed lower
bound at the normal drain boundary.

An earlier same-condition MPP v5 run measured kernel MPTCP at 168.085 Mbps
download and 450.738 Mbps upload, while MPP/TCP measured 875.187 and 617.392
Mbps. The later MPTUNNEL measurement remained in the 834/650 Mbps range.
Because the MPTCP row was not rerun beside the later MPTUNNEL run, this report
does not invent a final ratio from separate invocations.

There is no matched independent multipath QUIC baseline, so no external MPQUIC
ranking is claimed.

## Adaptive TCP carriers

One configured TCP endpoint defaults to a bounded `1-3` carrier range. Capacity
above the minimum is admitted only by the RFC's directional Product validation
and is retained only when complete before/assisted/after evidence proves added
service. Native TCP ACKs, elapsed time, source address, interface identity, and
peer claims cannot grant expansion.

In the fixed 100 Mbps per-native-flow QoS run, `1-1` versus `1-3` measured:

| Direction | `1-1` (Mbps) | `1-3` (Mbps) |
| --- | ---: | ---: |
| Download | 75.246 | 133.130 |
| Upload | 75.675 | 139.136 |

At one shared 200 Mbps bottleneck, adjacent download was 158.424 versus
150.129 Mbps and upload was 158.748 versus 159.831 Mbps. The controller did not
treat a second TCP session as useful aggregate capacity. These paired runs
validate demand-driven expansion and no-gain settlement; they do not encode a
fixed speed or percentage threshold into production.

With 10 TCP and 10 QUIC endpoints configured, every endpoint started one
carrier. Each TCP endpoint retained its independent `1-3` range, but no second
carrier was opened where completed delivery did not prove useful added
service. The configured maximum is never an eager connection target.

## Scale and short connections

Twenty-carrier measurements used five independently seeded
bandwidth, latency, jitter, and loss epochs:

| Mbps/path | Download (Mbps) | Upload (Mbps) | Complete |
| ---: | ---: | ---: | ---: |
| 30–100 | 344.534 | 210.378 | 2/2 |
| 300–1,000 | 1,178.811 | 609.004 | 2/2 |
| 3,000–10,000 | 2,261.932 | 670.693 | 2/2 |

The traces prove schedule execution and flow completion, not an artificial
configured-rate target or universal optimal-path claim. Complementary
200/20 and 20/200 Mbps links placed 91.5% of download bytes on the faster
download direction. A separate exact-accounting upload check placed 86.6% on
the faster upload direction; that value confirms path-use direction, not
comparative throughput.

Short-connection measurements:

| Pattern | Concurrent | KiB | Window (s) | Complete | Reject/incomplete | Max (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 3 s batches | 10 | 32 | 30 | 90/90 | 0/0 | 1.263 |
| Closed loop | 20 | 1,024 | 60 | 570/570 | 0/0 | — |

These checks establish bounded completion and accounting. They are not blended
into the matched competitor speed results.

## Disruption and migration

Retained disruption runs produced:

| Case | ↓/↑ Mbps | Echo | Short | Datagram | Gap (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| QUIC hop ↓ | 2459.750/— | — | — | — | 0.030 |
| QUIC hop ↑ | —/2498.275 | — | — | — | — |
| Mixed blackhole | 257.755/— | 40/40 | — | 151/153 | 0.777 |
| Mixed latency | 199.210/— | 40/40 | — | 145/147 | 1.293 |
| TCP blackhole | 181.261/243.518 | — | — | — | — |
| TCP latency | 280.085/245.656 | — | — | — | — |
| Mixed handover | 224.069/— | 32/32 | 47/47 | 134/134 | 0.717 |

Every reliable flow completed or remained attached. The QUIC upload was
target-confirmed.

The condition-handover run treats each event as a complete epoch: restore
the recorded baseline, then apply one selected condition. This models a link
recovering while another link changes instead of accidentally accumulating an
unbounded total outage. A separate recovery check removes every carrier for
five seconds and proves that the same reliable stream reattaches. During a
total outage, new flows were rejected; separate runs recovered after client
and server process restarts.

Port hopping does not move MPP state between TCP connections. QUIC uses native
connection migration and retains its authenticated connection; TCP selects a
new configured port only for a fresh carrier and replaces a configured-minimum
member at an exact Product-quiescent boundary.

## Interpreting rate traces

The probes retain 200 ms delivery samples, while container and management
collectors retain one-second physical and logical rates. Short zero/spike
delivery buckets can be application buffering or ACK release rather than a
carrier failure. Diagnose a suspected flap with the ordered-delivery gap,
native/interface service, MPP Data ACK progress, queue and flight ownership,
interface drops, and path lifecycle together.

An isolated movement around five percent can be ordinary observation variance,
not a pass threshold or a hard regression cap. Production contains no fixed
Mbps or percentage target.

## Limits

This report does not prove:

- performance on an arbitrary public route or access technology;
- native Windows, Wintun, macOS packet-tunnel, or Android `VpnService`
  performance;
- equivalence between the measured GNU/Linux binary and every packaged target;
- an external MPQUIC comparison;
- exact wire expansion from aggregate endpoint counters; or
- security of the custom MPP protocol.

MPTUNNEL uses portable Product and MPP evidence as its correctness fallback;
native platform telemetry is optional. Packaged targets are build-verified
separately from GNU/Linux performance measurements.
