# Ordinary mixed-service baseline — 2026-09-08

Status: **comparison baseline, not performance acceptance**. Runtime `d999fea`;
all twelve cells use ordinary binaries, with diagnostics disabled. No candidate
or runtime change was tested in this cohort.

## Practical answer

MPP QUIC and mixed provide substantially more useful bulk service than the
retained raw TCP, VMess and Hysteria2 cells in this particular impaired profile.
That does **not** establish a robust default mixed-mode tunnel:

- QUIC-only beats mixed bulk goodput in both directions here: 69.511 versus
  37.212 Mbps down, and 97.046 versus 66.846 Mbps up.
- Every MPP download mode loses its persistent interactive connection. Raw TCP
  and VMess retain all 80 echo successes, at much lower bulk throughput.
- Mixed is not worse on every measure: it has earlier first download/upload
  delivery than QUIC, better successful echo latency, and smaller upload
  confirmation/write gaps. Its download outage/recovery gap is worse.
- MPP TCP upload, VMess upload and Hysteria2 upload do not settle exactly.
  Hysteria2 download stops delivering at 14.923 seconds and remains silent
  through the observation end.

These are material tradeoffs and failures to retain in a redesign baseline,
not evidence that every observed delay is a code defect. There is no universal
winner, equal-work latency comparison, or release/promotion decision here.

## Frozen basis and measurement contract

The source is `d999fea`, executable
`./.tmp/reflection/bin/ack-support-20260908/mptunnel` at both MPP endpoints.
The existing `./.tmp/reflection/run.py` runs TCP, QUIC, mixed, raw, VMess,
Hysteria2 in that fixed order, first down, then up. No build overlaps the lab.
After TCP upload reaches the existing guard, its owned probe/products are
verified exited before the unchanged remaining five upload cells begin.
Hysteria2 upload also reaches the guard; all owned probes/products are exited
at the end. No favorable repeat or profile change is made.

The routed physical cut is shared: 500 Mbps each way, forward delay/jitter
70/20 ms, reverse 30/5 ms. The **whole impairment is mirrored for upload**.
Loss changes every five seconds:

| Workload seconds | 0–5 | 5–10 | 10–15 | 15–20 | 20–25 | 25–30 | 30–35 | 35 onward |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Forward loss % | 3 | 8 | 5 | 6 | 10 | 3 | 5 | 8 |
| Reverse loss % | 1 | 2 | 0.5 | 3 | 2 | 0.5 | 1 | 2 |
| Forward rate Mbps | 500 | 500 | 500 | 10 | 10 | 500 | 500 | 500 |

UDP blackholing is scheduled at 30–33 seconds in both directions. All 642
service rows match the appropriate rate/loss/delay/jitter/mirror settings.
Actual sampled blackhole activation is 30.003–30.010 seconds; removal is
33.003–33.159 seconds. Every MPP process PID and management start epoch is
stable within its cell. Per-side management snapshots are not atomic:
some client stamps are about one second ahead of the paired server sample
and repeated in the next row. Use producer Unix timestamps for stage intervals,
not a row's elapsed time as a common instantaneous clock.

MPP TCP actually uses three physical TCP carriers; QUIC uses one QUIC carrier;
mixed uses three TCP plus one QUIC carrier on this shared cut. This is not raw
single-TCP equivalence. VMess uses TCP, with no explicit mux, transport-security
or socket congestion-control override in its client configuration. Hysteria2
has configured 500 Mbps up/down bandwidth priors. No independent offload
equivalence measurement or packet-identical random-loss realization is claimed.

Download is one duration-limited HTTP 8 GiB request plus a persistent 64-byte
TCP echo stream (500 ms cadence, 3 s timeout). Every cell receives HTTP 200 and
records one **partial** request, zero complete requests. `bulk_status=ok`
means useful body bytes, not full-object completion. The download maximum gap
includes the terminal no-body interval when it is largest. Echo failure closes
the one connection; later unavailable slots are not repeated timeouts and the
probe does not reconnect. Latency percentiles include successes only.

Upload offers data for 40 seconds, then waits for exact sink acknowledgment
and terminal completion. The probe permits another 50 seconds, but the
existing runner guard acts after about 85 seconds total. Upload rate is an
accepted whole-run rate only when confirmed bytes equal locally accepted bytes
and terminal accounting is exact. In incomplete cases, maximum confirmation
gap covers completed confirmations only, not the unclosed final silence;
the probe discards confirmation bins when ACK accounting is invalid. No bins
are reconstructed from management counters. Upload has no concurrent echo
workload in this existing runner.

This is a current six-system, both-direction **one-profile snapshot**, not the
full acceptance matrix: clean/jitter/loss ablations, independent cuts,
cold/warm short objects, concurrent-flow scenarios, browser use, MPTCP and
statistical/order-reversed repetitions are not included.

## Download outcomes

Rates use exact body bytes divided by measured bulk elapsed time. All runner
cells exit 0; overall workload status is `loss` for all MPP modes and Hysteria2
because echo fails, and `ok` for raw TCP and VMess.

| System | Body bytes | Bulk seconds | Mbps | First body s | Maximum gap s | Echo successes / attempts | Success-only echo p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MPP TCP | 39,109,760 | 40.767263 | 7.675 | 1.017230 | 1.683733 | 16/46 | 2824.781 |
| MPP QUIC | 347,585,060 | 40.003408 | 69.511 | 1.179436 | 4.838137 | 30/75 | 458.283 |
| MPP mixed | 187,819,168 | 40.377782 | 37.212 | 0.545778 | 7.418422 | 42/75 | 290.358 |
| Raw TCP | 41,102,264 | 40.029076 | 8.214 | 0.494862 | 0.391620 | 80/80 | 426.328 |
| VMess | 15,781,966 | 40.066477 | 3.151 | 0.393671 | 0.698836 | 80/80 | 167.880 |
| Hysteria2 | 50,239,119 | 40.943939 | 9.816 | 0.390749 | 26.020518 | 29/74 | 662.686 |

| System | Largest body gap, seconds | Byte counter before → after | Actual echo timeout, seconds | Later unavailable slots |
| --- | --- | ---: | --- | ---: |
| MPP TCP | 35.940190 → 37.623923 | 37,209,216 → 37,274,752 | 22.839143 → 25.840891 | 29 |
| MPP QUIC | 30.130630 → 34.968767 | 234,783,012 → 234,848,548 | 15.294837 → 18.295718 | 44 |
| MPP mixed | 29.956625 → 37.375048 | 187,753,632 → 187,819,168 | 21.359509 → 24.362199 | 32 |
| Raw TCP | 1.625347 → 2.016967 | 1,657,960 → 1,723,496 | none | 0 |
| VMess | 24.056342 → 24.755178 | 8,252,124 → 8,260,234 | none | 0 |
| Hysteria2 | 14.923421 → 40.943939 | 50,239,119 → 50,239,119 | 15.142597 → 18.142818 | 44 |

Hysteria2's 26.020518-second maximum is **terminal silence**, not a successful
recovery: both byte endpoints are 50,239,119. MPP mixed's 7.418422-second
maximum ends in just 65,536 additional body bytes; its final body total is then
unchanged through the rest of the observation. QUIC's 4.838137-second maximum
also overlaps UDP outage/recovery, but substantial bulk service resumes.

## Upload outcomes

`Incomplete` means no accepted whole-run rate; parentheses retain the probe's
partial-confirmation rate only. `After 40 s` is total elapsed minus nominal
offered-load duration, **not an exact EOF-to-completion drain measurement**.

| System | Exact terminal completion | Confirmed / locally accepted bytes | Total s | Accepted Mbps | After 40 s | First confirmation / write s | Max confirmation / write gap s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MPP TCP | **incomplete** | 100,854,413 / 143,917,056 | 85.722026 | — (9.412 partial) | 45.722026 | 0.654453 / 0.105505 | 3.854429 / 3.274218 |
| MPP QUIC | yes | 581,173,248 / 581,173,248 | 47.908974 | 97.046 | 7.908974 | 1.397687 / 0.094674 | 5.293653 / 5.247024 |
| MPP mixed | yes | 432,078,848 / 432,078,848 | 51.710359 | 66.846 | 11.710359 | 0.460686 / 0.133158 | 4.004233 / 3.730152 |
| Raw TCP | yes | 27,394,048 / 27,394,048 | 49.675006 | 4.412 | 9.675006 | 0.153738 / 0.063370 | 0.741362 / 0.623847 |
| VMess | **incomplete** | 29,889,319 / 29,949,952 | 73.779356 | — (3.241 partial) | 33.779356 | 0.228603 / 0.008763 | 0.831383 / 4.880980 |
| Hysteria2 | **incomplete** | 42,126,997 / 63,438,848 | 85.355129 | — (3.948 partial) | 45.355129 | 0.193539 / 0.081127 | 4.670714 / 0.863671 |

TCP and Hysteria2 end at the runner settlement guard. Their subsequent sink
closure/reset is not independent proof of a network failure. VMess ends before
that guard (runner 0, 74.008123 s) with the sink closing before terminal
acknowledgment; it is still incomplete, with the terminal cause unassigned.
The complete upload runner times are QUIC 48.285772 s, mixed 52.171165 s and
raw 50.005484 s. Probe elapsed and runner elapsed are different clocks/scopes.

## Phase evidence and causal limits

Management reliable I/O is an application-socket boundary. For upload,
`S` = client source bytes consumed, `T` = server ordered target-socket bytes
written, `Rs` = server sink-reply bytes read, `Rc` = client reply bytes
delivered locally. **S is not claimed C; T is not mux F; Rc is not response
mux F.** Download's corresponding source/delivery totals are server
`to_peer_bytes` and client `from_peer_bytes`; they also include HTTP headers
and echo bytes, so they are not the exact bulk-body counter.

### Download mixed versus single carriers

Mixed's first body is faster, but its early sustained body service is lower.
The rounded raw-bin means over seconds [0,15) are QUIC 75.748 and mixed
9.206 Mbps; over [33,40), QUIC 128.903 and mixed 0.075 Mbps. These are receiver
arrival rates, not wire rates. Mixed's 536.850-Mbps bin 21 is a buffered
delivery burst during the nominal 10-Mbps QoS period, not a faster physical
link.

The mixed download outage/recovery plateau is independently visible in its
application counters, not just the probe:

| Mixed down service rows | Exact relevant Unix ms | Down source bytes | Down locally delivered bytes | Observation |
| --- | --- | ---: | ---: | --- |
| L31 | server 1788869147293; client 1788869148294 | 249,569,040 | 187,756,592 | Client snapshot is 1.001 s ahead; do not join as atomic |
| L32 | server 1788869148293; client 1788869148294 | 254,865,456 | 187,756,592 | Exact 64 MiB separation |
| L36 | server 1788869152293; client 1788869152294 | 254,865,456 | 187,756,592 | Still no local body progress after outage removal |
| L38 | both 1788869154294 | 254,865,456 | 187,756,592 | Same delivered total |
| L39 | both 1788869155294 | 254,865,456 | 187,822,128 | Only 65,536 bytes released |
| L41 | server 1788869157293; client 1788869157294 | 254,865,456 | 187,822,128 | No further useful bulk delivery |

For server session `65788055440294813`, native output ACK counters continue
through that plateau. From server Unix 1788869147293 to 1788869155294,
TCP wire path/physical instance `0/3` advances 9,802,929→15,187,141 bytes;
`1/1` advances 14,190,435→17,490,211; `2/2` advances
12,677,036→18,090,640. QUIC `0/4` advances
184,376,991→193,585,777. These are stable per-carrier Native ACK domains,
not Product unique-byte/copy ownership.

This excludes total native TCP silence. It does **not** show which carrier
owns or repairs the missing prefix, whether those bytes are useful originals
or losing copies, or whether the final 64 KiB release is a TCP winner. No
exact offset/winner observer is enabled. Sampled mixed down class backlog is
only 124,148–130,204 bytes around rows 34–36 at restored 500 Mbps, but that does
not locate the blocking byte or exclude native pending work, loss recovery,
earlier placement, or local receive service. A small current router queue is
not proof of a healthy entire path.

QUIC download likewise reaches a sampled 4-second body-delivery plateau
at client Unix 1788869105784→1788869109784, with 64 MiB source/delivery
separation, before useful service resumes. TCP-only has much lower bulk
throughput and poor loaded echo latency even without using the UDP port
affected by the blackhole. Thus not every QoS/interactive failure is uniquely
mixed-mode, and mixed's worse outage gap is correlation until exact prefix
service is joined.

### Upload forward and feedback stages

| System / interval | Exact service boundary | S bytes | T bytes | Rs / Rc bytes | Interpretation |
| --- | --- | ---: | ---: | ---: | --- |
| Mixed L21→23, t20.004→22.004 | server 1788869724062→1788869726062 | 271,781,834 fixed | 204,672,970 fixed | Rs 743 fixed; Rc 729→743 | Real target hold plus returned feedback catch-up |
| Mixed L23→25, t22.004→24.004 | client 1788869726061→1788869728061 | 271,781,834→329,712,722 | 204,672,970→262,603,858 | Rs 743→757; Rc 743 fixed | Target progresses 57,930,888 B while delivered replies stay fixed |
| Mixed L29→32, t28.005→31.156 | server 1788869732062→1788869735062 | reaches 338,393,195 | 271,284,331 fixed | Rs 799 fixed; Rc catches 799 | Hold begins before UDP blackhole |
| Mixed L33, t32.156 | server 1788869736062 | 338,393,195 | 272,383,658 | 813 / 799 | Target progresses while blackhole flag still on; winner unknown |
| QUIC L23→26, t22.003→25.003 | server 1788869675332→1788869678332 | 395,540,774 fixed | 328,431,910 fixed | 880 / 880 fixed | QoS forward hold, exact 64 MiB S−T |
| QUIC L32→35, t31.145→34.284 | server 1788869684332→1788869687332 | 471,197,734 fixed | 416,019,784 fixed | Rs 1048 fixed; Rc 1034 fixed | Outage/recovery includes return lag |
| TCP L61→86, t60.153→85.156 | client 1788869599159→1788869624159 | 143,917,056 fixed | 77,654,669→97,315,469 | 2303→2888 at both ends | Slow continuing target drain, not a persistent reply freeze |

The mixed QoS period includes both real forward holds and a returned-feedback
hold; its 463.447-Mbps confirmation bin25 cannot be interpreted as physical
upload service at that rate. Its longest sampled constant T span is 3 seconds,
not a reproduction of the previous 75-second reply hold. During QoS, upload
router class backlogs reach approximately 3.9 MB in both QUIC and mixed cells;
shared 10 Mbps service is a genuine competing bottleneck, not an automatically
removable MPP delay. Q/C alone does not assign any exact prefix's position.

At the last mixed sample (server/client Unix 1788869755061), S=432,078,848,
T=416,774,892 and Rs=Rc=1359; exact probe completion comes later. QUIC's last
sample likewise precedes full target completion. TCP's last sample T is
97,315,469 while the final partial probe confirms 100,854,413 after that
sample. Do not compare sampled end-state with terminal totals as simultaneous
measurements or call final sampled RSS post-load settled ownership.

## Bounded resource and wire context

RSS is KiB. CPU is the sampled `ps %CPU` **process-lifetime average**, not
instantaneous CPU, exclusive actor cost, or accumulated CPU seconds.
Peak/last are within that cell's sampled window. Raw has no tunnel process;
origin/probe resource cost is not included. Different work and observation
durations make normalized cost or statistical efficiency claims invalid.

| Cell | Client RSS peak / last KiB | Server RSS peak / last KiB | Client CPU peak / last % | Server CPU peak / last % |
| --- | ---: | ---: | ---: | ---: |
| MPP TCP down | 36,556 / 36,024 | 47,924 / 41,744 | 76.4 / 49.3 | 3.2 / 2.3 |
| MPP QUIC down | 101,004 / 101,004 | 341,272 / 341,272 | 14.9 / 12.9 | 26.9 / 21.3 |
| MPP mixed down | 130,708 / 119,292 | 284,156 / 284,156 | 29.1 / 20.6 | 36.3 / 18.8 |
| Raw TCP down | n/a | n/a | n/a | n/a |
| VMess down | 34,264 / 34,264 | 34,488 / 34,488 | 4.5 / 0.4 | 4.9 / 0.2 |
| Hysteria2 down | 46,632 / 28,680 | 228,756 / 228,756 | 92.5 / 72.5 | 114.0 / 95.2 |
| MPP TCP up | 92,924 / 92,924 | 27,632 / 26,736 | 6.1 / 2.6 | 2.5 / 1.5 |
| MPP QUIC up | 296,776 / 282,176 | 115,808 / 112,700 | 49.6 / 27.1 | 30.6 / 17.0 |
| MPP mixed up | 370,020 / 369,040 | 156,288 / 143,544 | 115.0 / 61.4 | 46.3 / 17.2 |
| Raw TCP up | n/a | n/a | n/a | n/a |
| VMess up | 33,128 / 33,128 | 32,956 / 32,956 | 3.1 / 0.1 | 4.3 / 0.4 |
| Hysteria2 up | 222,296 / 222,296 | 49,632 / 30,100 | 130.0 / 130.0 | 96.7 / 96.7 |

Router values below are **HTB class first→last sampled deltas**, using the first
two class snapshots in each service row, not summing them with child netem or
root qdisc counters. Forward means bulk-data direction; reverse means its
return direction. They include tunnel/native overhead, duplicates,
retransmissions and control traffic, not repair-only bytes. Drops mix the
configured random loss and possible queue effects. Sampling does not cover an
identical fraction of each final completion; byte ratios are not exact wire
amplification factors.

| Cell | Forward bytes / packets / drops | Reverse bytes / packets / drops | Peak forward class backlog B |
| --- | ---: | ---: | ---: |
| MPP TCP down | 67,321,250 / 46,012 / 1,502 | 2,201,611 / 22,235 / 309 | 202,406 |
| MPP QUIC down | 405,087,827 / 326,075 / 4,799 | 11,753,324 / 63,806 / 680 | 4,420,245 |
| MPP mixed down | 321,259,082 / 263,890 / 3,535 | 16,746,580 / 80,425 / 704 | 12,460,578 |
| Raw TCP down | 47,298,420 / 31,382 / 655 | 845,307 / 9,112 / 126 | 411,808 |
| VMess down | 18,287,440 / 12,213 / 412 | 514,216 / 5,651 / 80 | 63,779 |
| Hysteria2 down | 1,049,014,744 / 1,186,313 / 117,583 | 55,968,889 / 265,225 / 3,519 | 40,736,676 |
| MPP TCP up | 129,044,246 / 88,469 / 3,326 | 4,296,000 / 43,937 / 733 | 281,856 |
| MPP QUIC up | 712,105,764 / 572,278 / 8,286 | 22,267,819 / 109,288 / 1,415 | 4,245,198 |
| MPP mixed up | 569,193,602 / 461,808 / 7,552 | 23,278,513 / 117,197 / 1,574 | 3,928,120 |
| Raw TCP up | 31,991,804 / 21,216 / 669 | 851,821 / 9,197 / 160 | 165,026 |
| VMess up | 34,687,102 / 23,019 / 815 | 968,094 / 10,516 / 171 | 66,616 |
| Hysteria2 up | 2,146,037,101 / 3,079,646 / 274,650 | 159,251,947 / 755,208 / 13,316 | 39,479,876 |

The Hysteria2 cells have much more sampled wire work and larger router queues
than their useful-byte totals: down 1.049 GB and up 2.146 GB forward class deltas,
with about 40 MB peak backlog. Their failure is not an equal-throughput latency
control or proof that all MPP delay is inevitable. Conversely raw/VMess's low
loaded latency occurs at much lower useful and wire throughput. Neither
comparison licenses a blanket congestion-controller or protocol preference.

## Full untrimmed timing series

One-second Mbps bins, indexed from 0. Download arrays are exactly the declared
40-second observation window; a blocking final read may finish after 40 seconds
and contributes to exact body totals but not these 40 bins. Values are rounded
to 3 decimals and cannot reproduce exact bytes by integration. Upload arrays
cover all recorded confirmations through settlement. Buffered arrivals or
confirmations can exceed the nominal link rate in one bin; they are not wire
capacity. All zero bins are retained; trimmed averages are not used.

### Download body-arrival bins

```text
MPP TCP: [0.000,4.700,9.858,4.194,6.816,6.291,3.670,6.816,4.194,5.767,5.243,4.194,2.097,0.117,0.407,9.961,3.146,3.146,112.198,0.000,1.573,1.049,0.524,1.049,1.049,0.524,1.573,0.524,0.524,2.621,3.670,3.146,7.340,67.109,6.816,5.767,0.000,6.816,2.621,5.243]
MPP QUIC: [0.000,14.247,27.429,20.447,29.360,24.213,18.158,17.538,54.098,57.960,183.309,151.422,173.592,172.395,192.050,11.128,0.000,0.000,17.722,8.677,0.000,27.591,0.000,0.000,1.905,115.224,80.925,173.176,110.219,118.837,76.643,0.000,0.000,0.000,2.429,229.651,92.711,0.000,293.012,284.517]
MPP mixed: [0.990,23.211,26.214,17.826,8.389,6.816,38.390,1.980,2.097,1.690,1.980,2.097,2.621,1.573,2.214,3.029,3.146,2.738,4.078,3.263,2.331,536.850,0.021,0.117,0.000,103.439,263.193,55.959,197.849,187.931,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.524,0.000,0.000]
Raw TCP: [1.390,11.874,56.258,49.487,39.942,17.133,13.217,6.302,5.676,4.193,4.147,3.498,4.008,3.684,4.448,2.479,5.097,5.815,7.113,4.981,4.518,4.564,3.800,2.409,3.151,3.522,3.661,3.012,4.564,4.935,4.587,4.610,6.464,4.518,4.286,3.058,3.220,3.174,3.035,2.896]
VMess: [0.978,1.898,2.925,3.309,4.224,3.161,2.837,2.790,1.687,2.383,2.837,2.465,3.875,2.837,2.660,3.421,2.855,3.356,3.226,2.920,2.642,1.946,2.123,2.530,2.383,2.855,2.642,3.810,4.589,6.016,5.951,5.350,3.958,3.291,3.551,4.070,2.855,3.291,2.967,2.725]
Hysteria2: [67.342,75.894,68.629,54.455,22.919,45.613,16.579,0.000,0.000,0.000,6.281,0.000,4.585,0.000,39.616,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000]
```

### Upload sink-confirmation bins

```text
MPP TCP: unavailable (invalid terminal ACK accounting; raw array [])
MPP QUIC: [0.000,198.872,168.296,139.365,247.132,134.838,251.623,126.830,200.937,122.107,218.353,142.217,204.497,212.132,121.127,66.135,0.000,9.097,32.004,13.705,7.296,10.175,0.000,0.000,0.000,1.381,75.564,182.044,214.370,116.348,0.000,0.000,0.000,0.000,263.813,181.336,0.000,73.195,137.349,102.555,13.470,25.618,16.210,16.613,63.710,149.164,190.107,199.801]
MPP mixed: [4.676,19.998,314.049,70.543,112.294,101.328,11.534,3.670,373.153,58.336,23.261,58.004,106.643,186.143,157.578,16.991,0.000,0.524,0.000,0.000,0.000,8.389,0.000,0.000,10.367,463.447,19.614,0.000,16.157,0.000,0.000,0.000,66.489,281.190,51.863,85.817,4.191,1.669,77.325,3.434,9.956,102.151,24.021,47.375,10.461,96.681,0.000,11.106,15.869,118.931,176.871,134.535]
Raw TCP: [3.707,17.329,10.982,7.298,7.877,6.255,4.796,4.078,4.217,3.336,3.614,4.217,2.896,4.124,8.225,5.375,7.965,7.537,5.746,4.193,3.730,4.008,2.108,4.935,3.475,3.174,3.475,2.850,4.356,3.614,3.359,3.197,4.471,2.618,2.803,3.985,3.707,3.197,2.039,5.882,3.105,2.247,3.244,1.807,2.803,3.105,2.456,1.714,2.409,1.512]
VMess: unavailable (invalid terminal ACK accounting; raw array [])
Hysteria2: unavailable (invalid terminal ACK accounting; raw array [])
```

## Disposition and reproducible evidence

The information forecast was to establish a current mixed-versus-single-mode
and external-baseline snapshot before another implementation. It succeeds:
mixed's download recovery and persistent interactive failures are material;
QUIC is the strongest bulk mode in these cells; mixed has some better startup
and upload-gap measures; raw/VMess preserve loaded echo but are much slower;
several uploads fail settlement. It does not identify one causal defect or
predict how many seconds a redesign will remove.

The next architecture decision must preserve this tradeoff and test a concrete
coupling: does extra mixed-carrier placement/recovery/feedback work delay an
actually usable ordered prefix or response compared with the single-carrier
mode? These ordinary counters cannot answer which exact byte wins. Existing
exact-prefix/native and reply-service captures remain separate causal evidence;
their intervals must not be transplanted into these random cells. No new
observer, runtime change, favorable rerun, or performance promotion follows
automatically from this report.

[Full raw cohort and replay inputs](REDESIGN_BASELINE_20260908.raw.tar.gz)
retain result directories
`./.tmp/reflection/results/{tcp,quic,mixed,raw,xray,h2}-combined-{down,up}-redesign-baseline-0908/`,
the three run logs `redesign-baseline-0908-{down,up,up-remaining}.log`,
and the unchanged runner/shaper/probes/configuration basis. The raw
`probe.json` retains every echo attempt and original timing field;
`service.jsonl` retains exact epochs, management and native socket snapshots.
No private key is required for this report.

See [execution method](PERFORMANCE_METHOD_AND_LESSONS.md),
[active decision ledger](CURRENT_CLOSURE_PLAN.md), and
[bounded redesign proposal](MIXED_SERVICE_REDESIGN_20260908.md).
