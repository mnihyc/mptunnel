# Performance evidence for v0.1.0

This report records the release-facing runtime evidence collected on
2026-07-17 and 2026-07-18. It separates the earlier matched-baseline cohort
from the final runtime guard; results are never rebound to a binary that was
not measured. It is intentionally narrower than a claim that `mptunnel` is
faster on every network. Results are valid for the stated binaries, workload,
host, and shaped topology.

## Reference cohort identity

| Item | Value |
| --- | --- |
| Runtime source commit | `c196e22c8ad29ba8358a0b7eeaf2f78c9a58c862` |
| Source state | clean |
| Protocol | MPP v2 |
| Build | Cargo `release`, no optional features |
| Linux client/server target | `x86_64-unknown-linux-gnu` |
| Linux binary SHA-256 | `983f52777c4fb1633407387720e1ef0f2e3a6ec77fe43ea164118beedb7205bf` |
| Wine client target | `x86_64-pc-windows-gnu` |
| Windows PE SHA-256 | `e260f6b4a4fe586471ae9e8a427081cc6bb789a12945c12475ea53dbf2390c7b` |
| Host | Linux 6.12.90, x86_64, 10 CPUs, 34.1 GB RAM |
| Container engine | Docker 29.5.2, Compose 5.1.4 |

This identity belongs only to the adjacent direct, VMess, Hysteria2, MPTCP, and
MPP reference comparisons below. It predates logical-session retention and the
Windows QUIC socket capability adapter, so it is not the final v0.1.0 runtime
identity. An isolated Rust 1.96.0 rebuild reproduced its Linux SHA-256 exactly;
the rows remain valid historical comparisons for that binary.

## Final runtime identity

| Item | Value |
| --- | --- |
| Runtime source commit | `46bc6f84a597fafcfb0d1f4957cf5ecf0464ad72` |
| Source state | clean |
| Protocol | MPP v2 |
| Build | Cargo `release`, no optional features |
| Linux client/server target | `x86_64-unknown-linux-gnu` |
| Linux binary SHA-256 | `cf9ab98d29a62d94e9942021f2a3902a92ec3d9c71ce98f518f08cb56ffcbca1` |
| Wine client target | `x86_64-pc-windows-gnu` |
| Windows PE SHA-256 | `ae0089b2fffd065bf9f64d7b5d576f9537fc331b240fecd185098cc1c35a4659` |
| Wine runtime | Wine 9.0 on the Linux lab host |

The Linux release archives use musl and the Windows release archives use MSVC;
neither packaged target is the exact benchmark binary identified above.

## Method

Every result below used an isolated Docker Compose case and one release-profile
product flow. Network shaping was applied inside container namespaces. The
runner captured:

- a redacted effective configuration and binary identity;
- qdisc state, drops, overlimits, and backlog before and after each case;
- client, server, and target interface counters;
- target-confirmed upload bytes; and
- MPTCP socket/subflow observations for the MPTCP rows.

The bulk object was 4096 MiB, the active load interval was 10 seconds, and
there was one download or upload flow. URI path hints were disabled, so
scheduling used live evidence rather than lab rate/RTT priors. Netem queue
limits were derived above the shaped bandwidth-delay product. The published
cohorts had zero qdisc overlimits; configured loss still appeared as drops.

These are single observations, not medians or confidence intervals. Timing
variation remains possible, so the tables are regression evidence rather than
an SLA.

## Final runtime guard

The final zero-loss high-delay guard used the same 500 Mbps, 180 ms one-way,
20 ms jitter profile, one 10-second reliable flow, and exact target-confirmed
upload accounting. Five-path rows used five equal copies of that profile.

| Client runtime | Reliable carrier | Paths | Download Mbps | Upload Mbps |
| --- | --- | ---: | ---: | ---: |
| Linux native | TCP | 1 | 151.702 | 176.121 |
| Linux native | QUIC | 1 | 254.127 | 250.831 |
| Linux native | QUIC | 5 | 340.048 | 444.420 |
| Windows PE under Wine | TCP portable path | 1 | 159.232 | 172.543 |
| Windows PE under Wine | QUIC basic UDP | 1 | 67.407 | 126.886 |
| Windows PE under Wine | QUIC basic UDP | 5 | 89.043 | 160.167 |

Every row completed, every upload byte accepted by the probe was confirmed by
the target, and no row had a recovery gap. Native QUIC gained 33.8% download
and 77.2% upload over its single path. Basic-UDP QUIC under Wine gained 32.1%
and 26.2%, while remaining substantially slower than native Quinn; the runtime
prints that expected compatibility-path warning.

The matched balanced-path blackhole guard also completed under Wine at
53.130 Mbps download with a 2.119-second recovery gap and 142.683 Mbps upload
with a 1.851-second target-observed gap. Its upload exceeds the earlier
same-condition 134.600 Mbps observation. Download fault rows have varied from
roughly 47 to 79 Mbps on this host, so a single absolute fault goodput is not a
release target; completion and the bounded progress gap are the contract.

These current rows guard the final runtime against a release regression. The
external systems were not rerun in this final abbreviated pass, so baseline
rankings remain claims about the reference cohort below, not a synthetic merge
with the current binary.

## Reference same-condition baselines

The high-delay profile used 500 Mbps, 180 ms one-way delay, 20 ms jitter, and
zero configured loss on each shaped path. Single-path rows used one such path.
Multipath rows configured five equal paths.

| System | Carrier | Paths | Download Mbps | Upload Mbps | Result |
| --- | --- | ---: | ---: | ---: | --- |
| Direct | TCP | 1 | 171.006 | 168.156 | complete |
| VMess, Xray 26.3.27 | TCP | 1 | 172.267 | 168.776 | complete |
| Hysteria2 2.10.0 | QUIC/UDP | 1 | 67.299 | 70.252 | download complete; upload lower bound |
| Linux MPTCP | TCP | 5 | 97.120 | 148.929 | complete |
| MPP | TCP | 1 | 159.142 | 167.440 | complete |
| MPP | QUIC | 1 | 208.399 | 235.062 | complete |
| MPP | TCP | 5 | 246.717 | 268.819 | complete |
| MPP | QUIC | 5 | 326.844 | 430.732 | complete |

The MPP-over-QUIC rows carry the same reliable application stream over QUIC
carrier connections; they are not unreliable datagram measurements.

The MPTCP sampler observed a peak of four additional subflows, five including
the initial subflow. Its result is therefore not a silent single-path fallback.
It is still one Linux MPTCP configuration and must not be generalized to every
path manager, congestion controller, or kernel.

The Hysteria2 upload row is marked `loss`: 96,618,446 of 96,993,280 bytes
accepted locally were confirmed by the target within the original drain
window. Its 70.252 Mbps value is receiver-confirmed goodput and a lower bound,
not a completed upload. A separate 12-second-drain run returned 71.316 Mbps
with 100,859,953 of 101,711,872 bytes confirmed and remained incomplete. The
table retains the matched-cohort value rather than presenting the follow-up as
an equal replacement.

### What the cohort establishes

- Five-path MPP/TCP improved over its single path by 55.0% download and 60.5%
  upload.
- Five-path MPP/QUIC improved over its single path by 56.8% download and 83.2%
  upload.
- In this topology, five-path MPP/TCP measured 2.54 times the MPTCP download
  goodput and 1.81 times its upload goodput.
- Single-path MPP/TCP was 7.6% below VMess download and 0.8% below VMess
  upload, so the evidence does not support claiming a universal single-path
  win.

Those comparisons are arithmetic over this cohort only.

## Reference Linux matrix

The heterogeneous TCP cohort used these one-way profiles:

| Profile | Rate | Delay | Jitter | Configured loss |
| --- | ---: | ---: | ---: | ---: |
| Low latency | 80 Mbps | 20 ms | 2 ms | 1.0% |
| Balanced | 200 Mbps | 80 ms | 10 ms | 1.0% |
| Mild loss | 100 Mbps | 160 ms | 10 ms | 0.1% |
| High bandwidth | 500 Mbps | 180 ms | 20 ms | 0% |
| Poor Internet | 50 Mbps | 420 ms | 120 ms | 10.0% |

`Heterogeneous 5` used all five profiles. `Equal high-bandwidth 5` used
five copies of the high-bandwidth profile.

| TCP reliable case | Download Mbps | Upload Mbps | Completion |
| --- | ---: | ---: | --- |
| Low-latency single | 71.763 | 69.552 | complete |
| High-bandwidth single | 149.381 | 176.894 | complete |
| Heterogeneous 5 | 252.451 | 257.260 | complete |
| Equal high-bandwidth 5 | 238.280 | 260.229 | complete |

The heterogeneous result is evidence that the runtime can use unlike measured
paths without fixed link classes. It does not isolate latency-sensitive small
flow behavior; that requires a separate workload cohort.

## Reference failover

The failover case started one TCP reliable flow on the five heterogeneous
paths, then changed the balanced path to 100% loss after the probe's trigger.
The flow remained complete.

| Client runtime | Download Mbps | Download recovery gap | Upload Mbps | Target-observed upload gap |
| --- | ---: | ---: | ---: | ---: |
| Linux native | 79.018 | 1.301 s | 144.088 | 1.742 s |
| Windows PE under Wine 9.0 | 49.263 | 2.131 s | 177.643 | 1.892 s |

The Linux upload probe itself observed a 1.900-second maximum delivery gap;
the target observer measured 1.742 seconds. Under Wine those values were
3.480 and 1.892 seconds respectively. Target-observed gaps better represent
actual receiver progress, while both are retained to expose buffering and
observation-boundary effects.

These rows prove bounded recovery in this blackhole case. They do not prove a
sub-second failover guarantee or every failure mode.

## Reference Windows executable under Wine

The matched Wine cohort ran the exact Windows PE above as the client in the
same network namespace and used the same native Linux server.

| TCP reliable case | Linux download/upload | Wine download/upload |
| --- | ---: | ---: |
| Low-latency single | 71.763 / 69.552 | 70.863 / 70.829 |
| High-bandwidth single | 149.381 / 176.894 | 159.148 / 165.900 |
| Heterogeneous 5 | 252.451 / 257.260 | 249.068 / 198.977 |
| Equal high-bandwidth 5 | 238.280 / 260.229 | 252.799 / 259.558 |

The Wine low-latency upload value is the exact follow-up with a 12-second drain:
all 162,529,280 accepted bytes were target-confirmed. The original five-second
drain ended before Wine flushed its accepted buffer and was not used in the
table.

Wine 9.0 returned Windows socket error 10045 for native TCP telemetry, and
`mptunnel` printed its portable-fallback performance warning. The
heterogeneous upload result was 22.7% below native Linux, while the equal-path
upload was within 0.3% and aggregated 56.5% above Wine's high-bandwidth single
path. This is useful portable-path evidence, not a native Windows performance
claim. Wine does not exercise the Windows kernel's `SIO_TCP_INFO`, Wintun, or
native network scheduling, and this GNU-target PE is not the MSVC release
artifact.

The final runtime's separate TCP and basic-UDP QUIC results are recorded in
the final guard above. Wine exercises compatibility behavior, not native
Windows UDP acceleration or kernel scheduling.

## Traffic accounting

All completed upload rows use a target sink observer and distinguish bytes
accepted by the local probe from bytes confirmed at the target. The Linux
representative upload rows confirmed every accepted byte.

The current endpoint interface snapshots do **not** provide exact wire
expansion. They combine control traffic, headers, native retransmission,
cross-path reinjection, in-flight bytes, and sequential snapshot skew. This
report therefore makes no exact low-overhead percentage claim from those
counters. A publishable transport-expansion result requires direction-split,
per-interface sender accounting over a finite transfer with a drained delivery
window.

## Reproduction

The same-condition baseline cohort can be selected with:

```bash
export CASE_FILTER='direct_cross_continent_high_bandwidth,direct_upload_cross_continent_high_bandwidth,baseline_vmess_tcp_single_cross_continent_high_bandwidth,baseline_vmess_tcp_single_cross_continent_high_bandwidth_upload,baseline_hysteria2_udp_single_cross_continent_high_bandwidth,baseline_hysteria2_udp_single_cross_continent_high_bandwidth_upload,baseline_mptcp_tcp_multipath_equal_fat,baseline_mptcp_tcp_multipath_equal_fat_upload,mptunnel_tcp_single_cross_continent_high_bandwidth,mptunnel_tcp_single_cross_continent_high_bandwidth_upload,mptunnel_udp_stream_single_cross_continent_high_bandwidth,mptunnel_udp_stream_single_cross_continent_high_bandwidth_upload,mptunnel_tcp_multipath_equal_fat,mptunnel_tcp_multipath_equal_fat_upload,mptunnel_udp_stream_multipath_equal_fat,mptunnel_udp_stream_multipath_equal_fat_upload'
export MPTUNNEL_LAB_FAT_RATE=500mbit
export MPTUNNEL_LAB_FAT_DELAY=180ms
export MPTUNNEL_LAB_FAT_JITTER=20ms
export MPTUNNEL_LAB_FAT_LOSS=0.00%
export MPTUNNEL_LAB_IDEAL_LOSS=0.00%
export MPTUNNEL_LAB_OBJECT_MIB=4096
export MPTUNNEL_LAB_USE_PATH_HINTS=0
lab/run-heterogeneous-ablation.sh
```

The runner records the effective environment, binary hashes, redacted configs,
container identities, qdisc evidence, and result rows in its generated result
directory. Baseline downloads are version- and checksum-pinned in
`lab/baseline-lock.json`.

For a Wine client, build the opt-in image and add:

```bash
export MPTUNNEL_LAB_CLIENT_RUNTIME=wine
export MPTUNNEL_LAB_INSTALL_WINE=1
```

See [the lab contract](LAB.md) for isolation, completeness rules, traffic
accounting, and case selection.

## Scope not proven by this report

- real public-Internet routes or long-duration stability;
- distributions across repeated runs or different hosts;
- native Windows kernel, Wintun, macOS, or Android VPN performance;
- performance of the Linux musl and Windows MSVC release artifacts;
- TUN throughput in this release cohort;
- latency-sensitive and realtime behavior under simultaneous bulk load;
- exact transport wire expansion; or
- security of the custom MPP protocol.

These are separate evidence cohorts. Simulator gates and unit tests can catch
model regressions, but they do not replace end-to-end runtime measurements.
