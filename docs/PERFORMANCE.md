# Performance

MPTUNNEL combines the service available from several TCP and/or QUIC
**carriers**—the transport connections between its peers. A single application
transfer can use several carriers, while other applications share the same MPP
session. Carrier sets are configurable: TCP-only, QUIC-only or mixed, with a few
connections or tens.

The network underneath those carriers determines the available capacity. Carriers
on one Internet link share its bandwidth ceiling; parallel connections can still
help use that bandwidth when individual connections are limited. Carriers routed
over independent links can draw on each link's capacity. Both are aggregation;
they give the tunnel different resources to work with.

These experiments show what that means for a transfer: using two links,
continuing through a slowdown, and serving small requests during a large download.
They also compare throughput, latency and CPU cost with Hysteria2, Xray and direct
TCP. Each figure names the carrier set and network conditions used.

<a name="one-connection-two-links"></a>

## Aggregation across independent links

A single download uses **3 TCP carriers on one 200 Mbps link** and **1 QUIC
carrier on a separate 200 Mbps link**. Each link has its own bandwidth limit,
giving a combined capacity of 400 Mbps.

**Before impairment, the application receives 312 Mbps on average over seconds
0–15**, including startup. This exceeds the configured 200 Mbps capacity of either
individual link. The dashed line in the chart marks that configured limit.

## Delivery through restriction and interruption

The same run then restricts the QUIC link to **10 Mbps**, restores its capacity,
and briefly blocks UDP. The TCP link remains available throughout. These phases
measure delivery as service changes:

| Condition | Application download rate | Measurement interval |
| --- | ---: | --- |
| Both links at 200 Mbps | 312 Mbps | 0–15 s, including startup |
| QUIC restricted to 10 Mbps | 159 Mbps | 16–25 s, wholly within the restriction |
| UDP blocked; TCP available | 184 Mbps | 32–33 s, the one full bin inside the block |

The recorded UDP block runs from **31.05 to 33.63 seconds**. Four 64-byte echo requests
start and finish within it, taking **223–234 ms** each; a fifth starts inside and
replies just after the block ends. The application continues receiving the bulk
download during this interruption.

[![Application download rate and echo latency, with separate baseline, QUIC restriction and UDP block periods](assets/performance/independent-paths.svg)](assets/performance/independent-paths.svg)

The short spike after QUIC's capacity is restored is buffered data reaching the
application; it can exceed the link rates momentarily. The longest pause between
body reads across the entire run is **0.79 seconds**, at **24.57–25.36 seconds**,
near restoration from the restriction. It is not a measurement of UDP failover time.

Across the full 40 seconds, download averages **268 Mbps** and all **80 small
echo requests** complete. A separate fixed-schedule probe fetches 100 kB HTTP
responses alongside each transfer:

| Transfer | Whole-run average | HTTP responses completed | Completed late |
| --- | ---: | ---: | ---: |
| Download | 268 Mbps | 80 / 85 | 3 |
| Upload | 213 Mbps | 80 / 85 | 6 |

Five HTTP requests fail during each impaired transfer. A late response takes
more than its 2.5-second budget. All 25 requests before and after each transfer
complete on time. [Both directions and the earlier-version comparisons](PERFORMANCE_DETAILS.md#independent-link-download-during-a-qos-change-and-udp-outage)
are included in the complete results.

<a name="speed-and-responsiveness"></a>

## Throughput and latency on one bottleneck

Here all carriers in a run pass through one **500 Mbps download / 100 Mbps
upload** bandwidth limit. Each system is tested separately on that network.
MPTUNNEL uses **3 TCP + 1 QUIC carriers** in the mixed case and **1 QUIC carrier**
in the QUIC-only case. These are two example configurations; carrier counts and
combinations are configurable.

A download runs for 40 seconds while 64-byte echo requests measure responsiveness.
The link starts with jitter. Recorded commands remove it between **8.08 and
9.22 seconds** across the five runs, separating two measurement periods:

| System | Download, 0–8 s | Echo p95, 0–8 s | Download, 10–40 s | Echo p95, 10–40 s |
| --- | ---: | ---: | ---: | ---: |
| MPTUNNEL · 3 TCP + 1 QUIC | 241 Mbps | 197 ms | 453 Mbps | 823 ms |
| MPTUNNEL · 1 QUIC | 269 Mbps | 248 ms | 367 Mbps | 118 ms |
| Hysteria2 | 49 Mbps | 932 ms | 467 Mbps | 117 ms |
| Xray VMess/TCP | 8 Mbps | 196 ms | 379 Mbps | 122 ms |
| Direct TCP | 139 Mbps | 127 ms | 354 Mbps | 117 ms |

Seconds **0–8 include startup and initial jitter**. Seconds **10–40 follow jitter
removal**; transport recovery may continue into this interval. Hysteria2 delivers
the most data in the later interval. Mixed MPTUNNEL delivers more than its single
QUIC carrier in that interval, with higher echo latency.

[![Per-second application download rate and every echo attempt for all five systems](assets/performance/shared-link-timeline.svg)](assets/performance/shared-link-timeline.svg)

The curves retain startup, the transition and every echo attempt. The shaded
band spans the recorded jitter changes. All echo attempts complete; the sequential
probe makes 69–80 attempts per system because it waits for each reply before
sending the next request.

<details>
<summary>Whole-run comparison: all 40 seconds, including startup and jitter removal</summary>

<a href="assets/performance/shared-link-tradeoffs.svg">
<picture>
  <source media="(max-width: 600px)" srcset="assets/performance/shared-link-tradeoffs-narrow.svg">
  <img src="assets/performance/shared-link-tradeoffs.svg" alt="Whole-run download averages and echo p95 over 40 seconds, including startup and jitter removal">
</picture>
</a>

Over the entire run, mixed MPTUNNEL delivers **406 Mbps** with **797 ms** echo p95;
one QUIC carrier delivers **343 Mbps** with **154 ms** p95. Hysteria2 delivers
**367 Mbps** with **427 ms** p95. Xray and direct TCP both have **122 ms** p95.
These summaries combine the two conditions and their transition; the table and
curves above show how delivery and latency change within the run.

</details>

## CPU and memory on a VPS

Multipath delivery does extra work to schedule, track and recover data across
carriers. On a small VPS, CPU can become a limit before network bandwidth does.
The shared-link experiment above measured the following process CPU costs:

| System | Client CPU seconds / GiB | Server CPU seconds / GiB |
| --- | ---: | ---: |
| MPTUNNEL 1 QUIC | 17.6 | 25.1 |
| MPTUNNEL 3 TCP + 1 QUIC | 13.0 | 22.6 |
| Hysteria2 | 22.1 | 20.5 |
| Xray VMess/TCP | 2.5 | 1.2 |

Lower is better. These values count process CPU time per delivered GiB;
client and server costs are separate. Xray used substantially less CPU in this
comparison. Direct TCP has no tunnel process to measure.

Memory also grows with data in flight. For example, **1 Gbps at 100 ms RTT**
requires roughly **12.5 MB** in flight to fill the link. Recovery and reordering
need additional retained data. The [resource guide](OPERATIONS.md#resource-envelopes)
explains the available limits, and the
[memory measurements](PERFORMANCE_DETAILS.md#cpu-memory-and-traffic) report sampled
process residency for the earlier test set.

## Browsing alongside transfers, and longer runs

In separate 25-second runs without configured jitter or loss, MPTUNNEL's
3 TCP + 1 QUIC set delivered **407 Mbps down** and **444 Mbps up** on a 500 Mbps
link shared by its carriers. All 55 loaded HTTP requests completed in
each direction; seven download-side and one upload-side request exceeded the
2.5-second response budget. These requests each fetched a 100 kB body on a fixed
schedule, so a slow response did not reduce the number of requests offered.

Five-minute transfers used one QUIC carrier on a 500 Mbps link and three TCP
carriers on a separate 200 Mbps link, with changing loss and jitter on QUIC.
Download averaged **226 Mbps**; upload averaged **387 Mbps**.
Combined endpoint traffic was **1.776 bytes per
downloaded byte** and **1.258 bytes per uploaded byte**, including acknowledgements,
control messages and recovery traffic.

The [complete measurements](PERFORMANCE_DETAILS.md) cover both directions,
reversed link capacities, swapped physical links, the loss/recovery scenarios
and comparisons with MPTUNNEL 0.5.2.

## Setup and data

The two main demonstrations use a Linux MPTUNNEL 0.6.0 development build
(`1e8abedf`), with one run per configuration. The release and measurement source
identities are separate; exact measurement hashes are in the data files.

For the shared-link comparison, endpoint containers have four CPU cores and
the router has two. Directional delay is 70/30 ms; initial jitter is 20/5 ms.
Hysteria2 2.10.0 uses Brutal at 500/100 Mbps. Xray 26.3.27 uses VMess over TCP
with `security: auto`, without mux or TLS. MPTUNNEL uses one QUIC carrier and,
in mixed mode, three TCP carriers with native BBR. No random loss is injected.

Download speed counts bytes delivered in order to the application. Upload speed
counts target-confirmed bytes through final settlement. Phase download means use
all complete one-second bins in the named interval, with no smoothing or peak
selection. For the independent-link run, boundary bins that overlap restriction
commands are excluded from the phase table. The UDP interval uses recorded
transition event points; the data does not contain every body-read timestamp
needed to calculate an outage-only maximum delivery gap.

For the shared-link table, all systems use the same intervals: **[0, 8) seconds**
and **[10, 40) seconds** from each probe start. Seconds 8–10 remain visible in the
curves. Echoes belong to the interval in which the request starts; replies after
its end remain included. Response p95 is the nearest-rank 95th percentile of
successful replies. The early window contains 16 echoes per system except
Hysteria2 (15); the later window contains 60 except mixed MPTUNNEL (49). One mixed
reply finishes at 40.17 seconds and remains included. Echo points on the chart
use request start times.

- [Measurements and test conditions](assets/performance/current-observations.json)
- [Independent-link time series](assets/performance/independent-paths-series.json)
- [Shared-link time series](assets/performance/shared-link-series.json)
- [Complete benchmark tables and earlier measurements](PERFORMANCE_DETAILS.md)
- [Earlier raw series](assets/performance/measurements.json)
- [Figure renderer](assets/performance/render.py)

To regenerate the main figures with Python and Matplotlib:

```bash
python3 docs/assets/performance/render.py
```
