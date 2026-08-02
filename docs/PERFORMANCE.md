# Performance evidence

This page records the bounded release evidence for MPTUNNEL v0.1.4 and MPP
v5. It is deliberately narrower than a claim that one tunnel wins on every
network. Rates depend on path conditions, workload, host capacity, direction,
and native TCP or QUIC behavior.

## Measurement contract

The local runner builds the release profile without optional features and runs
client, server, and target in isolated Docker namespaces. It records the exact
binary hashes, source state, configuration, qdisc state, interface counters,
per-second container and optional management telemetry, application delivery,
and target-confirmed upload bytes. The standard sustained workload uses two
flows and a 30-second delivery window; disruption rows use a 20-second mixed or
reliable workload.

All values below are GNU/Linux observations on one host. The source trees were
clean for the retained release cohorts, but the formal host gate rejected the
rows because one unrelated Docker container was running. They are therefore
useful matched regression and capability evidence, not publication-grade
cross-host benchmarks, Internet-speed guarantees, or SLAs.

## Single-path competitors

These systems ran adjacently on one 500 Mbps path with 180 ms one-way delay,
20 ms jitter, 1% configured loss, the same object and workload duration, and
path hints disabled. Upload values are target-confirmed goodput within the
standard one-second drain and may be lower bounds when delivery remained in
flight at the boundary.

| System | Carrier | Download | Upload |
| --- | --- | ---: | ---: |
| Direct | TCP | 231.521 Mbps | 240.939 Mbps |
| Xray 26.3.27, VMess | TCP | 219.529 Mbps | 240.849 Mbps |
| MPTUNNEL | MPP/TCP | 151.722 Mbps | 162.267 Mbps |
| Hysteria2 2.10.0 | QUIC | 114.506 Mbps | 117.541 Mbps |
| MPTUNNEL | MPP/QUIC | 212.704 Mbps | 207.649 Mbps |

The cohort does not support a universal single-path TCP win over Xray. It does
show MPP/QUIC ahead of the matched Hysteria2 row in both directions. MPTUNNEL's
main performance purpose is independent-path aggregation and recovery while
one Product flow remains intact.

## Equal-path aggregation

The release Core was guarded on five equal 500 Mbps paths with 180 ms one-way
delay, 20 ms jitter, and no configured loss.

| MPP carrier | Download | Upload | Observation |
| --- | ---: | ---: | --- |
| TCP | 834.364 Mbps | 649.766 Mbps | download and upload completed |
| QUIC | 648.493 Mbps | 738.113 Mbps | download completed; upload is a receiver-confirmed lower bound at the normal drain boundary |

An earlier same-condition MPP v5 cohort measured kernel MPTCP at 168.085 Mbps
download and 450.738 Mbps upload, while MPP/TCP measured 875.187 and 617.392
Mbps. The later release Core remained in the 834/650 Mbps range. Because the
MPTCP row was not rerun beside the final binary, this report does not invent a
final ratio from separate invocations.

The isolated 776.116 Mbps QUIC download peak also did not become a release
target. Back-to-back execution on the same host placed the retained historical
binary at 671.356 Mbps and the current binary at 648.493 Mbps, with maximum
ordered gaps of 0.350 and 0.314 seconds. That 3.4% difference is consistent
with run variation and does not prove a Core regression. There is no retained
independent Multipath QUIC implementation in the lab, so no external MPQUIC
ranking is claimed.

## Adaptive TCP carriers

One configured TCP endpoint defaults to a bounded `1-3` carrier range. Capacity
above the minimum is admitted only by the RFC's directional Product validation
and is retained only when complete before/assisted/after evidence proves added
service. Native TCP ACKs, elapsed time, source address, interface identity, and
peer claims cannot grant expansion.

In the fixed 100 Mbps per-native-flow QoS cohort, `1-1` versus `1-3` measured:

| Direction | `1-1` | `1-3` |
| --- | ---: | ---: |
| Download | 75.246 Mbps | 133.130 Mbps |
| Upload | 75.675 Mbps | 139.136 Mbps |

At one shared 200 Mbps bottleneck, adjacent download was 158.424 versus
150.129 Mbps and upload was 158.748 versus 159.831 Mbps. The controller did not
treat a second TCP session as useful aggregate capacity. These paired cohorts
validate demand-driven expansion and no-gain settlement; they do not encode a
fixed speed or percentage threshold into production.

## Disruption and migration

The final normal-build release gates produced:

| Case | Reliable goodput | Product result |
| --- | ---: | --- |
| QUIC ranged-port migration, unconstrained download | 2459.750 Mbps | complete, 0.030 s maximum read gap |
| QUIC ranged-port migration, unconstrained upload | 2498.275 Mbps | complete and target-confirmed |
| Mixed TCP+QUIC, balanced-path blackhole | 257.755 Mbps | 40/40 echo, 151/153 datagrams, 0.777 s maximum bulk gap |
| Mixed TCP+QUIC, severe fat-path latency change | 199.210 Mbps | 40/40 echo, 145/147 datagrams, 1.293 s maximum bulk gap |
| TCP multipath, balanced-path blackhole | 181.261 / 243.518 Mbps down/up | reliable flow retained |
| TCP multipath, severe fat-path latency change | 280.085 / 245.656 Mbps down/up | reliable flow retained |
| Seeded mixed condition handover | 224.069 Mbps | persistent echo 32/32, small transfers 47/47, datagrams 134/134, 0.717 s maximum bulk gap |

The condition-handover fixture treats each event as a complete epoch: restore
the recorded baseline, then apply one selected condition. This models a link
recovering while another link changes instead of accidentally accumulating an
unbounded total outage. A separate durable integration test removes every
carrier for five seconds and proves that the same reliable stream reattaches;
the packaged acceptance suite separately proves offline new-flow rejection and
client/server process restart recovery.

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
qdisc drops, and path lifecycle together.

An isolated movement around five percent is ordinary observation variance,
not a pass threshold or a hard regression cap. A larger movement is rerun
beside the exact retained binary and accepted or rejected from causal evidence.
No timing, carrier count, or congestion parameter is tuned merely to pass one
fixture.

## Reproduce

Run the maintained local contract from the repository root:

```bash
python3 lab/validate_performance_declaration.py --check-registry
python3 -m unittest discover --start-directory lab --pattern 'test_*.py'
cargo test --locked --manifest-path lab/benchmarks/Cargo.toml

CASE_FILTER='direct_cross_continent_high_bandwidth,baseline_vmess_tcp_single_cross_continent_high_bandwidth,baseline_hysteria2_udp_single_cross_continent_high_bandwidth,mptunnel_tcp_single_cross_continent_high_bandwidth,mptunnel_udp_stream_single_cross_continent_high_bandwidth,mptunnel_tcp_multipath_equal_fat,mptunnel_udp_stream_multipath_equal_fat' \
MPTUNNEL_LAB_OBJECT_MIB=4096 \
MPTUNNEL_LAB_USE_PATH_HINTS=0 \
lab/run-heterogeneous-ablation.sh
```

The runner creates one ignored directory under `.tmp/lab/results/` containing
the JSONL row and all evidence needed to decide comparability. See
[LAB.md](LAB.md) for case selection and matching rules.

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
native platform telemetry is optional. Cross-platform builds and package
contents are separate release gates.
