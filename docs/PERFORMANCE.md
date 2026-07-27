# Performance evidence

## v0.1.2 Core-frozen release-candidate cohort

### Exact measured identity

This protocol-v4 cohort used the exact no-optional-feature release-profile
GNU/Linux executable below for both client and server. Rebuilding was disabled
between these rows. The executable freezes the Core credit fix, but it is not
the eventual tagged binary: later Product-only routing, logging, and doctor
changes do not alter the RFC/Core algorithm, yet they do change the complete
program identity. The tagged executable must receive its own representative
release guard after the whole program is frozen.

| Item | Value |
| --- | --- |
| Version | `mptunnel 0.1.2` |
| Protocol | MPP v4 |
| Carrier presentation | `tcp-tls13-no-alpn+quic-h3-post-data-rfc9297` |
| Build | Cargo `release`, no optional features |
| Client/server target | `x86_64-unknown-linux-gnu` |
| Client/server binary SHA-256 | `d46eaf6a530c57cbe8802d6d9574c5b0afb406c65df5717e724e775e68e2374e` |
| Core-frozen build-input manifest SHA-256 | `12caf8a531e8c7175f45e1f0343f1235c4433977d9d495eb2e5854731a1640f4` |
| Recorded base commit | `692c6f396dbdc0e526208e0f926b57fc1affdb86` |
| Toolchain | Rust/Cargo 1.96.0 |

The working tree was dirty while the release was being assembled. Non-build
documentation and workflow edits changed the whole-tree snapshot between the
QUIC upload rows
(`c39b5af5c1b412a51cc079909aa09116792d9d8f8402accf05e8dd0374c8b019`
and
`ceb20f75832b7d6079f3ae106a4688ec549e712efcdbcc950bafe9dcadf18b40`)
and the representative guards
(`536fbca862253c187bdf072c5341115dd3548bd0b13f6d98be2f9995a242e75b`).
The build-input identity and measured binary stayed unchanged.

These host snapshots fail the formal acceptance rule because the source tree
was dirty. CPU-frequency and thermal telemetry were unavailable; the two QUIC
upload snapshots also saw one unrelated running container. The rows are
therefore descriptive Core-frozen candidate guards, not clean-host or
tagged-binary acceptance evidence. They may establish the stated
protocol-correctness and path-use observations, but they do not establish broad
competitive performance.

### Equal-path representative rows

Each case used five shaped paths at 500 Mbps, 180 ms one-way delay, 20 ms
jitter, and no configured loss. The workload used two concurrent flows for 30
seconds. Upload goodput is receiver-confirmed; a canonical upload has the
normal one-second drain, while a diagnostic completion rerun changes only the
drain to ten seconds and is not throughput-acceptance evidence.

| Carrier and direction | Goodput | Receiver result | Observed carrier use |
| --- | ---: | --- | --- |
| TCP download | 799.384 Mbps | 2/2 requests complete; 2,997,778,416 bytes | all five paths, 13.398%--24.626% each |
| QUIC download | 712.382 Mbps | 2/2 requests complete; 2,672,113,520 bytes | all five paths, 15.225%--22.640% each |
| QUIC upload, canonical drain | 747.305 Mbps lower bound | 2,933,751,030 of 2,964,193,280 locally accepted bytes confirmed; 1.027% pending | all five paths, 9.117%--26.550% each |
| TCP upload, canonical drain | 559.969 Mbps lower bound | 2,198,809,919 of 2,210,201,600 locally accepted bytes confirmed; 0.515% pending | two sustained owners carried 49.596% and 48.758%; the other three carried 1.647% combined |

Download path shares are approximate receive shares and upload path shares are
approximate transmit shares from case-boundary client-interface counters. They
show material path use but are not exact wire-expansion measurements.

The QUIC upload guard follows the shared-stream credit fix: accepting a later
attachment is credit-neutral, and only the logical receive owner grants more
credit. Both streams continued delivering, all five carriers contributed, no
maximum-data violation occurred, and the probe reported no errors. Its runner
status is `loss` only because 30,442,250 bytes remained in the delivery
pipeline when the canonical one-second drain ended; it does not mean the
shaped network configured loss. The two target connection totals were
1,460,929,402 and 1,472,821,628 bytes, and corresponding server receive
counters differed from the client transmit counters by only 709--816 bytes.

With the diagnostic ten-second drain, the same QUIC case returned `ok` at
756.809 Mbps: all 3,017,867,264 locally accepted bytes were confirmed, the two
final connection totals were 1,490,026,496 and 1,527,840,768 bytes, and all
five paths again contributed 13.602%--25.492%. This exact completion supports
the credit/lifecycle diagnosis; its extended drain is not a replacement
acceptance setting.

The canonical TCP upload likewise ended with a bounded delivery tail of
11,391,681 bytes, or 0.515% of local acceptance, and no probe error. Its
ten-second-drain diagnostic returned `ok` at 567.291 Mbps with all
2,230,910,976 locally accepted bytes target-confirmed. Nearly all carrier
traffic still belonged to two paths, at 49.170% and 48.499%. Because this was
two concurrent uploads, it demonstrates sustained aggregate use by two
logical owners; it does **not** demonstrate that one TCP stream was striped
materially across five paths.

These rows establish the corrected credit invariant, exact eventual delivery
in the diagnostic drain, material all-five use for QUIC upload and both
download cases, and honest two-owner TCP upload aggregation in this topology.
They are not adjacent matched comparisons against V2Ray/Xray, Hysteria2,
MPTCP, or Multipath QUIC. No fresh v4 fault row is part of this Core-frozen
guard, so the historical v0.1.1 failover results below are not silently
transferred to v4. Broad competitive and failover claims still require the
clean, repeated, matched cells specified by the performance plan, and tagged
release claims require a guard of the eventual tagged executable.

One observation is never a performance verdict. In particular, an isolated
throughput movement of roughly five percent is ordinary run-to-run noise
unless repeated paired evidence and causal counters prove otherwise. Five
percent is neither a pass margin nor a failure cap, and there is no universal
percentage gate.

## v0.1.1 release gate

The v0.1.1 gate used MPP v3 release builds without optional features. The
native Linux binary SHA-256 was
`8f356f47421ad96e7b9795010573a011ab3215fa3fa713977de79b3d1427c140`;
the Windows GNU PE used by Wine was
`978595cea97666c719ed221f76839246f253af3d30e0497b2907cbd911a2a40f`.
Every row in this section records those exact binaries. Native Windows
execution remains a separate CI and deployment concern; Wine proves the
portable protocol path, not Windows kernel scheduling or `SIO_TCP_INFO`.

The high-bandwidth throughput profile used 500 Mbps, 180 ms one-way delay,
zero configured loss, one reliable flow, and no configured path hints. The
Wine cohort used an 8-second load and 256 MiB object. Upload rows were confirmed
by the target.

| Wine client case | Paths | Download Mbps | Upload Mbps |
| --- | ---: | ---: | ---: |
| TCP | 1 | 140.624 | 168.241 |
| TCP | 5 | 210.973 | 289.208 |
| Mixed TCP+QUIC | 2 | 125.445 | 158.004 |

Five-path Wine TCP improved over its matched single path by 50.0% download and
71.9% upload. The native Linux mixed TCP+QUIC equal-path upload reached 438.729
Mbps with exact accounting and 3.689% endpoint traffic excess.

The mixed latency workload combined one 500 Mbps, 180 ms bulk path with one
80 Mbps, 20 ms latency path. Two native Linux runs retained 339.5--339.8 Mbps
bulk goodput and zero UDP loss. UDP p95 was 73.4--74.1 ms; interactive p95 was
75.4/94.4 ms. These observations are regression evidence, not an SLA.

Balanced-path blackhole recovery remained bounded:

| Client runtime | Download recovery | Destination upload recovery | Upload excess |
| --- | ---: | ---: | ---: |
| Linux native | 0.363 s | 0.778 s | 1.484% |
| Windows PE under Wine 9.0 | 0.278 s | 1.471 s | 5.347% |

The Windows client printed one warning per process that native TCP flight
telemetry was unavailable and the portable Data ACK capacity fallback was in
use. All cases completed. Artifacts were retained locally under the named
`v011-final-*` result cohorts until release evidence was recorded.

## Historical v0.1.0 evidence

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
| Runtime source commit | `1018992ffc5c7e8a857f114027357f27ff360dfd` |
| Source state | clean |
| Protocol | MPP v2 |
| Build | Cargo `release`, no optional features |
| Linux client/server target | `x86_64-unknown-linux-gnu` |
| Linux binary SHA-256 | `c67d921f247ddefb08bbbcdb19cd137f4a43dbbe10a5f716fdf06a091341b701` |
| Wine client target | `x86_64-pc-windows-gnu` |
| Windows PE SHA-256 | `b061eb0f24aa6fcc565d43ec54a3233fe7080088887b462edddb4926cfbcbf44` |
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
| Linux native | TCP | 1 | 149.935 | 176.483 |
| Linux native | QUIC | 1 | 249.664 | 229.762; repeat 259.757 |
| Linux native | QUIC | 5 | 389.138 | 433.475 |
| Windows PE under Wine | TCP portable path | 1 | 155.780 | 167.349 |
| Windows PE under Wine | QUIC basic UDP | 1 | 75.418 | 124.621 |
| Windows PE under Wine | QUIC basic UDP | 5 | 112.579 | 159.564 |

Every row completed, every upload byte accepted by the probe was confirmed by
the target, and no row had a recovery gap. Native QUIC gained 55.9% download;
its upload gain was 66.9% to 88.7% against the two single-path observations.
Those observations bracket the earlier 250.831 Mbps guard and expose timing
variance rather than hiding it. Basic-UDP QUIC under Wine gained 49.3%
download and 28.0% upload, while remaining substantially slower than native
Quinn; the runtime prints that expected compatibility-path warning.

The matched balanced-path blackhole guard also completed under Wine at
51.591 Mbps download with a 2.526-second recovery gap and 153.034 Mbps upload
with a 2.096-second target-observed gap. Download fault rows have varied from
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
- latency-sensitive behavior beyond the recorded mixed workload, including
  application-specific realtime traffic;
- exact transport wire expansion; or
- security of the custom MPP protocol.

These are separate evidence cohorts. Simulator gates and unit tests can catch
model regressions, but they do not replace end-to-end runtime measurements.
