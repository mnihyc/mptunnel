# Performance

MPTUNNEL can combine the capacity of independent links within one connection.
Paths sharing a bottleneck share its capacity. Throughput and latency also depend
on direction, packet loss, reordering, transport recovery and available CPU.

The measurements below use optimized Linux builds of MPTUNNEL 0.4.9, with the
standard 20% sender-policy defaults. Each profile was measured once in isolated
containers. [Measurement data](assets/performance/measurements.json) accompanies
the timing plots. These observations describe the stated conditions, rather than
a guaranteed speed on every Internet connection.

## Healthy TCP, QUIC and mixed service

One shared 500 Mbps link has 70 ms downstream and 30 ms upstream delay, without
injected loss or jitter. TCP uses three carriers, QUIC one; mixed uses both on
the same link. Each workload offers traffic for 25 seconds.

| Transport | Download, Mbps | Upload, Mbps | First download body, s | Longest download gap, s | Loaded echo p95, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP | 430.1 | 443.1 | 0.440 | 0.426 | 334 |
| QUIC | 412.5 | 430.5 | 0.410 | 0.101 | 249 |
| TCP+QUIC | 410.3 | 440.9 | 0.473 | 0.509 | 471 |

[![TCP, QUIC and mixed download, echo latency and upload timing](assets/performance/healthy.svg)](assets/performance/healthy.svg)

The tunnel is already running when each application starts. First-body timing
therefore includes application setup over established carriers. Echo is a
concurrent 64-byte TCP exchange; its latency includes loaded-network effects.
The upload workload has no concurrent echo measurement.

## Independent links with QUIC restriction

Three TCP carriers use one 200 Mbps link and one QUIC carrier uses another
200 Mbps link, with 70/30 ms directional delay. During a 40-second workload,
QUIC is restricted to 10 Mbps at nominal seconds 15–25, followed by a UDP
blackhole at seconds 30–33. Upload applies the impairment to its sending direction.

| Phase | Download, Mbps | Upload confirmations, Mbps |
| --- | ---: | ---: |
| Healthy, 5–15 s | 365.5 | 275.8 |
| QUIC restricted, 15–25 s | 185.0 | 173.1 |
| Rate restored, 25–30 s | 348.3 | 277.8 |
| UDP outage, 30–33 s | 224.4 | 216.6 |
| Recovery, 33–40 s | 207.6 | 264.3 |

[![Mixed download, echo latency and upload confirmations during QUIC restriction and outage](assets/performance/independent-links.svg)](assets/performance/independent-links.svg)

The TCP link continues carrying useful ordered traffic while QUIC is restricted.
Whole-workload download is 266.0 Mbps, with a longest body-read gap of 0.530 s and
echo p95 of 353 ms. Upload confirms all 1,330,839,552 accepted bytes in 42.64 s.
The extra elapsed time includes setup and final settlement.

Restoring a link does not immediately restore its full aggregate contribution.
Direction matters: healthy-phase upload here uses less of the combined capacity
than download. The timing plots retain those differences and recovery intervals.

## Loss and recovery

A QUIC download uses a 500 Mbps link with 70/30 ms delay. Downstream random loss
is 20% for the first 20 seconds, then clears; upstream loss is zero.

[![QUIC download and loaded echo timing during loss and after it clears](assets/performance/loss-recovery.svg)](assets/performance/loss-recovery.svg)

Whole-workload download is 393.7 Mbps. First body arrives in 0.725 s, the longest
body-read gap is 0.442 s, and echo p95 is 350 ms. Delivery over seconds 30–40
averages 414.5 Mbps. The figure shows startup and recovery alongside throughput.

## Reordering and other transports

The shared routed link provides 500 Mbps downstream and 100 Mbps upstream,
with 70/30 ms delay. Initial directional jitter is 20/5 ms and clears nominally
at eight seconds. Each product runs a 40-second download with concurrent echo.
Hysteria2 uses version 2.10.0 and Xray VMess/TCP uses version 26.3.27.

| Product | Download, Mbps | First body, s | Longest bulk gap, s | Echo p95 / maximum, ms |
| --- | ---: | ---: | ---: | ---: |
| MPTUNNEL QUIC | 367.5 | 0.530 | 0.147 | 204 / 331 |
| MPTUNNEL TCP+QUIC | 399.6 | 0.576 | 0.320 | 278 / 1436 |
| Hysteria2 | 367.8 | 0.401 | 2.986 | 456 / 739 |
| Xray VMess/TCP | 315.9 | 0.429 | 0.200 | 128 / 223 |

[![Download and echo timing for MPTUNNEL, Hysteria2 and Xray through reordering](assets/performance/reordering.svg)](assets/performance/reordering.svg)

MPTUNNEL QUIC delivers 239.9 Mbps during the first eight seconds and 406.4 Mbps
during seconds 20–40. The latter interval is 447.7 Mbps for mixed MPTUNNEL,
465.3 for Hysteria2 and 453.8 for Xray. Whole averages alone do not describe
startup, pauses or late service.

All actual echoes complete in these four runs. No random loss is configured,
but Hysteria2 incurs 15,941 measured router queue drops. Identical configured
conditions do not imply identical realized loss or a universal product ranking.

## CPU, memory and traffic

Transport work has a material CPU and memory cost. During the reordering
workload, MPTUNNEL QUIC's server/client final lifetime CPU averages are
91.5%/75.3%, with peak RSS of 218,668/52,524 KiB. Xray's corresponding samples are
4.6%/8.5% and 35,796/32,840 KiB. These are loaded process observations; they are
not CPU per delivered byte or evidence about indefinite memory retention.

Healthy mixed download has peak server/client RSS of 213,164/61,980 KiB.
Its sampled combined endpoint egress divided by application bytes is 1.140;
the upload quotient is 1.076. Sampling boundaries differ from the application
window, and framing, acknowledgements, retransmissions and recovery copies are
not individually separated. These quotients are not exact overhead percentages.

High-bandwidth, high-RTT paths also need sufficient flow-control and retention
windows. Approximate bandwidth-delay product as `rate_bps × RTT_seconds / 8`.
A 64 MiB window covers about 537 ms at 1 Gbps. Larger windows can support more
in-flight data but increase memory exposure. See the
[reference configuration](../examples/config.reference.toml) and
[operations guide](OPERATIONS.md) for sizing options.

## Reading the measurements

Download goodput counts ordered application-body delivery. Duration-limited
HTTP downloads intentionally stop before the full 8 GiB object completes.
Upload goodput counts target-sink confirmation arrival, including final settlement;
a catch-up confirmation bin can exceed the physical link rate without implying
that new bytes reached the target at that instantaneous rate.

Plots preserve raw one-second bins and actual echo attempts. Shaded phases are
nominal probe time; collection and shaping commands have separate timestamps.
Boundary bins are therefore approximate intervention windows. Loaded latency,
startup and gaps accompany throughput because each affects ordinary use.

The measurements use one Linux host. Native platform builds and tests are
available through [CI](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml),
but they do not establish identical performance on Windows, macOS or Android.
