# Native refill: ordinary return restriction and recovery gates

Recorded: 2026-09-10. Frozen ordinary CONTROL `ba56290` versus working
CANDIDATE `b0baca2`, unchanged native refill reserve. **No performance or release
acceptance.** The return-restriction pair carries more useful bytes in every
phase but worsens median/p95 echo timing in every phase. The candidate's larger
body gap happens before the restriction, not because of the 10 Mbps return cut.
No failed recorded request or multi-second read stall occurs in that pair.
The separate UDP-outage comparison exposes a1.211-second candidate read gap
after restoration, while native TCP and QUIC byte counters continue advancing.
**That material recovery interval required attribution before advancement.**
The subsequent information-only capture identifies a different1.118-second
gap's exact64-byte prefix: repair admission precedes its releasing receipt
by968ms. This narrows that captured interval, not the ordinary pair's cause.

The root-created, tar-listed [recovery archive](NATIVE_REFILL_RECOVERY_20260910.raw.tar.gz)
retains31files: four ordinary cells and one observer cell, five files each,
three driver logs, the runner/shaper and frozen observation overlay.

## Bounded question and fixed conditions

The preceding [ordinary panel](NATIVE_REFILL_ORDINARY_20260910.md) and
[copy attribution](NATIVE_REFILL_COPY_ATTRIBUTION_20260910.md) retain both the
large TCP unsent-residence correction and mixed-mode cost. This next declared
question is whether reduced native unsent debt helps return feedback/fallback,
or changed placement/copies worsen service during restriction and restoration.
The native wake does not depend on a Product ACK or QUIC recovery. It cannot
remove a physical return limit or guarantee an improvement. No new threshold,
deadline, source change, observation guard or native-controller adjustment is
part of these comparisons.

Root runs CONTROL then CANDIDATE, normal management/probe only, no feature
observer or concurrent compiler. The return pair is the existing 40-second
mixed DOWN bulk plus serial64-byte echo workload, one shared500 Mbps DOWN cut
with30 ms delay and a500→10→500 Mbps UP return cut with70 ms delay at15–25s.
Configured random loss, jitter and UDP outage are zero. HTB rate equals ceil,
burst/cburst remain65536 bytes and netem limit8192. All82 actual service rows
verify these values; all class/qdisc drop deltas are zero.

Closed raw inputs are the five files per cell under
`./.tmp/reflection/results/mixed-combined-down-native-refill-return-{control,candidate}-0910/`;
driver log is `./.tmp/reflection/native-refill-return-pair-0910.log`.
Both runners exit zero. The candidate remains the working model with disclosed
cost, not an accepted release. A separate predeclared outage-only pair follows;
its results must not be conflated with return restriction.

## Return pair: full useful service and timing

Both probes report `ok`, HTTP200 and one intentional duration-partial8 GiB
response. This is not full-object completion. All153 actual echo attempts
succeed, with exact64-byte replies, no timeout/disconnect/mismatch and empty
stderr. Slow serial requests reduce attempted count rather than become hidden
failed attempts. Quantiles use sorted index`round((n−1)*rank)`; phase membership
follows request start, not completion.

| Metric | Control | Candidate |
|---|---:|---:|
| Body bytes / elapsed, s | 1,966,165,236 / 40.000038 | 2,085,986,832 / 40.003046 |
| Whole useful goodput, Mbps | 393.233 | 417.166 |
| First body, s | 0.577384 | 0.617250 |
| Maximum body read gap, s | 0.274234 | 0.625170 |
| Gap interval, s | 0.577384–0.851618 | 11.870808–12.495978 |
| Body counters around gap, B | 58,192 / 123,728 | 589,973,716 / 589,985,716 |
| Echo successes / failures | 78 / 0 | 75 / 0 |
| Echo p50 / p95 / max, ms | 266.611 / 541.911 / 1045.462 | 364.202 / 666.994 / 968.753 |
| Worst echo interval, s | 14.537540–15.583002 | 18.584645–19.553397 |
| Maximum successful-echo spacing, s | 1.045478 | 0.993934 |
| Last echo completion, s | 39.758675 | 40.148236 |

Goodput improves6.09%, but first body is39.866ms later, maximum read gap grows
350.936ms and whole echo median/p95 worsen. Worst echo improves. The control's
worst request starts before the return change and completes after it; phase
grouping by start does not make its entire elapsed time pre-restriction.
The candidate's maximum body gap ends at12.496s, well before the15s return cut.
It cannot be reported as a restricted-return stall. Sampled DOWN backlog is
already28.7–34.1MB in nearby12–14s rows; this is queue context, not an exact
blocking-byte or per-stage causal measurement.

| Probe phase: body Mbps; echoes / p50 / p95 / max ms | Control | Candidate |
|---|---|---|
| Startup0–5s | 245.914; 10 / 177.255 / 507.778 / 507.778 | 280.566; 10 / 220.591 / 510.276 / 510.276 |
| Healthy5–15s | 429.366; 20 / 303.943 / 526.408 / 1045.462 | 471.496; 17 / 602.268 / 759.202 / 780.828 |
| Return restricted15–25s | 417.271; 19 / 360.697 / 569.276 / 775.959 | 437.272; 18 / 435.948 / 666.994 / 968.753 |
| Restored25–40s | 402.219; 29 / 231.006 / 455.497 / 465.211 | 413.153; 30 / 309.316 / 466.458 / 488.556 |

Candidate useful means improve in every window, while median and p95 echo are
worse in every window. Thus this is not a restricted-only benefit with unchanged
healthy/restored latency. No average or successful completion waives that cost.
All80 raw body bins follow, from second zero; all153 full-precision attempts
remain in raw JSON. Bins above500Mbps are buffered application reads, not a
claim that the physical link exceeds its configured rate.

```text
Control: 3.121,110.197,193.558,637.386,285.309,554.844,448.266,395.705,348.120,505.467,410.241,376.200,404.704,469.462,380.649,493.355,370.934,505.030,367.050,381.630,420.993,447.959,343.622,396.389,445.749,372.833,432.358,417.842,387.011,374.141,457.262,465.814,450.091,364.342,385.188,350.957,461.794,368.031,336.470,409.155
Candidate: 1.049,145.729,435.063,563.373,257.617,628.408,467.285,447.447,469.502,393.256,424.920,486.142,522.266,443.632,432.104,467.038,473.306,470.951,276.269,196.254,667.473,444.926,436.216,469.904,470.383,433.027,314.232,459.999,411.961,388.384,398.691,348.277,521.650,455.698,461.060,465.612,388.215,381.548,363.274,405.660
```

## Effective restriction, cost and native service

UP is actually10Mbps in rows15–24 in each capture. First restricted row elapsed
is15.001800/15.001641s; last is24.004219/24.002582s; first restored row25 is
25.004333/25.002684s. DOWN remains500Mbps throughout. These elapsed values are
recorded before sequential telemetry commands, not exact simultaneous wire
boundaries. Class differences across rows15→25 cover approximately the
restriction, including sampling/transition margins; they are not a per-packet
causal accounting window.

| Sampled cost | Control | Candidate |
|---|---:|---:|
| Rows / final elapsed, s | 41 / 40.006003 | 41 / 40.004408 |
| Whole DOWN / UP class-byte deltas | 2,393,949,227 / 39,187,459 | 2,394,071,409 / 39,629,557 |
| Rows15→25 DOWN / UP class-byte deltas | 622,769,682 / 10,186,865 | 592,223,838 / 8,727,525 |
| Whole DOWN backlog p50 / max, B | 12,516,690 / 26,749,232 | 18,479,863 / 40,123,103 |
| Whole UP backlog p50 / max, B | 68,374 / 190,507 | 73,644 / 213,793 |
| Rows15–25 UP backlog p50 / max, B | 72,281 / 190,507 | 53,111 / 213,793 |
| Client RSS peak / final, KiB | 94,444 / 94,444 | 109,576 / 81,472 |
| Server RSS peak / final, KiB | 354,388 / 344,980 | 323,264 / 304,112 |
| Last client / server lifetime CPU, % | 76.9 / 195.0 | 83.2 / 184.0 |
| Server TCP NOTSENT p50 / p95 / max, B | 23,840 / 2,507,562 / 19,662,866 | 0 / 312,712 / 569,064 |
| Server TCP sent-unacknowledged estimate p50 / max, B | 8,271,534 / 17,659,412 | 7,199,832 / 16,510,940 |
| Server TCP RTT p50 / p95 / max, ms | 252.855 / 487.876 / 679.984 | 365.221 / 647.850 / 773.728 |
| Server QUIC RTT p50 / p95 / max, ms | 275.794 / 503.353 / 558.181 | 364.657 / 669.354 / 776.725 |
| Server QUIC native flight p50 / max, B | 8,166,758 / 21,669,648 | 15,626,089 / 32,732,436 |

Each role retains three established TCP sockets and one exact QUIC native
epoch. TCP minimum RTT medians remain100.026/100.027ms server-side. Per-output
native acknowledged-byte counters are monotonic within their epochs; common
management rows1→40 show server TCP/QUIC deltas1,066,367,994/1,198,881,991B
versus731,427,477/1,549,958,808B. Client return TCP/QUIC deltas are
11,182,148/681,986B versus13,131,822/3,281,693B. These are delivered native
protocol bytes, not unique Original allocation, safe repair or category-specific
return ACK counts. Different producer sample ages and cancellation/transit tails
prevent treating them as the exact application observation window.

The candidate's smaller local TCP unsent backlog coexists with larger native
RTT, QUIC flight and shared DOWN queue. Restricted UP bytes are lower, but whole
UP bytes slightly higher; no blanket feedback-efficiency or latency conclusion
follows. Count one shaped class per direction, not parent/child/qdisc sums.
TCP sent-unacknowledged estimate is `Send-Q−NOTSENT`, not exact physical packet
flight; RTT quantiles pool socket samples, byte quantiles first sum sockets.
Management QUIC samples are not synchronized per-echo observations. Lifetime
`ps` CPU is not interval CPU, and sampled RSS is not post-teardown ownership.

Warnings are the duration-stop client BrokenPipe/reset and server RemoteClosed,
followed by `H3_NO_ERROR`, not failed recorded requests. There is no feature
counter/range trace here to assign the pre-restriction read gap or each echo's
delay to native queue, recovery copies or route proof. Do not infer the cause
from a displayed state/rate or from this pair's goodput gain alone.

## Separate UDP-outage pair: materially worse late recovery gap

The predeclared outage-only CONTROL→CANDIDATE pair uses the same ordinary
binaries,40-second mixed DOWN/echo load and500/500Mbps, DOWN30/UP70ms profile.
QoS changes, random loss and jitter are off; whole UDP ingress is blocked at
30–33seconds by the existing endpoint rules. This is not the preceding return
restriction and no runtime, native reserve or timer changes occur between them.
Inputs are
`./.tmp/reflection/results/mixed-combined-down-native-refill-outage-{control,candidate}-0910/`,
with driver log `./.tmp/reflection/native-refill-outage-pair-0910.log`.

Both runners exit zero; probes report `ok`, HTTP200 and duration-partial bodies.
All145 actual echo attempts succeed with no mismatch/timeout/disconnection and
empty stderr. Those successes do not waive the materially longer read gap.

| Metric | Control | Candidate |
|---|---:|---:|
| Body bytes / elapsed, s | 1,924,776,478 / 40.001969 | 1,962,418,032 / 40.001386 |
| Whole useful goodput, Mbps | 384.936 | 392.470 |
| First body, s | 0.583353 | 0.580491 |
| Maximum body read gap, s | 0.408427 | 1.210551 |
| Gap interval, s | 32.472965–32.881392 | 37.045635–38.256186 |
| Body counters around gap, B | 1,542,416,446 / 1,542,481,982 | 1,819,841,224 / 1,819,906,760 |
| Echo successes / failures | 74 / 0 | 71 / 0 |
| Echo p50 / p95 / max, ms | 315.087 / 798.955 / 1405.124 | 286.637 / 905.972 / 1416.035 |
| Worst echo interval, s | 34.322268–35.727392 | 31.389694–32.805729 |
| Maximum successful-echo spacing, s | 1.405149 | 1.416058 |
| Last echo completion, s | 40.179584 | 40.109131 |

Whole goodput improves1.96% and median echo improves, but maximum body gap grows
802.124ms and echo p95/max worsen. The candidate's longest gap occurs roughly
four seconds after UDP is unblocked, not entirely inside the physical outage.
Its37th body bin is11.184Mbps, followed by732.430Mbps: the later burst does not
erase the preceding service hold. Restored echo service is substantially worse.

| Probe phase: body Mbps; echoes / p50 / p95 / max ms | Control | Candidate |
|---|---|---|
| Startup0–5s | 306.646; 10 / 180.941 / 445.202 / 445.202 | 280.723; 10 / 230.660 / 294.191 / 294.191 |
| Healthy5–15s | 413.218; 20 / 319.091 / 534.342 / 752.991 | 422.398; 20 / 286.441 / 460.695 / 474.222 |
| Healthy15–25s | 414.950; 20 / 356.241 / 494.310 / 517.227 | 439.037; 20 / 274.885 / 350.925 / 378.592 |
| Pre-outage25–30s | 412.402; 10 / 233.647 / 321.204 / 321.204 | 377.265; 10 / 300.642 / 406.837 / 406.837 |
| Outage30–33s | 185.457; 3 / 905.623 / 984.478 / 984.478 | 294.072; 4 / 1395.225 / 1416.035 / 1416.035 |
| Early restored33–35s | 590.859; 2 / 1278.738 / 1405.124 / 1405.124 | 474.218; 1 / 858.588 / 858.588 / 858.588 |
| Late restored35–40s | 356.535; 9 / 387.188 / 480.300 / 480.300 | 392.861; 6 / 773.642 / 1143.315 / 1143.315 |
| All restored33–40s | 423.485; 11 / 451.426 / 1405.124 / 1405.124 | 416.106; 7 / 858.588 / 1143.315 / 1143.315 |

Phase groups count request starts; an echo can straddle outage or restoration.
For the candidate, requests36.976619–37.981795 and37.981820–38.591999 take
1005.176 and610.179ms and overlap the longest body gap. Later requests still
take611.105 and905.972ms. Thus the adverse interval affects more than one bulk
statistic. All80 raw body bins follow; all145 full-precision attempts remain
in raw records, including the slower restoration sequence.

```text
Outage control: 2.620,102.021,401.937,559.644,467.008,441.170,271.657,596.263,434.554,400.749,420.424,422.525,427.636,348.995,368.207,408.845,199.406,600.358,442.192,437.138,437.090,387.567,391.083,469.878,375.946,439.972,455.498,374.018,285.872,506.650,63.443,395.776,97.151,563.253,618.465,102.760,384.446,612.368,401.368,281.735
Outage candidate: 2.621,184.195,344.982,586.678,285.141,493.591,294.249,487.262,353.556,456.662,394.049,439.740,255.435,623.952,425.479,442.323,457.563,466.291,459.913,420.863,430.451,456.560,436.938,415.967,403.504,321.621,358.306,424.186,424.622,357.589,80.400,514.428,287.388,448.266,500.171,368.509,444.095,11.184,732.430,408.088
```

### Native progress across outage and the later body hold

The `udp_blackhole` flag is true in rows30–32. First set-row elapsed is
30.003538/30.003696s; first clear-row elapsed is33.183269/33.114428s. Rule
updates and sequential telemetry take time, so these are observed sampling
boundaries, not exact synchronized packet timestamps. All82rows retain500Mbps
both directions and zero configured random impairment. Class/qdisc drop deltas
are zero; this does **not** mean the deliberate endpoint UDP drops were zero.

QUIC remains one physical instance and one native-counter epoch in each run.
The following are server native acknowledged-byte totals and their actual
producer `sampled_at_us`, not management retrieval timestamps:

| Row | Control QUIC ACK bytes / producer us | Candidate QUIC ACK bytes / producer us |
|---|---:|---:|
| 31 | 1,031,474,120 / 31,712,705 | 1,069,082,075 / 31,779,443 |
| 32 | 1,031,474,120 / 32,779,560 | 1,069,082,075 / 32,770,121 |
| 33 | 1,031,474,120 / 33,764,950 | 1,069,082,075 / 33,737,133 |
| 34 | 1,031,474,120 / 34,735,347 | 1,069,082,075 / 34,762,537 |
| 35 | 1,031,474,120 / 35,762,672 | 1,069,084,475 / 35,779,399 |
| 36 | 1,031,482,328 / 36,789,583 | 1,082,184,419 / 36,551,455 |
| 37 | 1,047,894,284 / 37,638,638 | 1,085,046,311 / 37,562,214 |
| 38 | 1,069,737,158 / 38,743,166 | 1,099,525,655 / 38,363,535 |
| 39 | 1,100,252,216 / 39,733,212 | 1,113,976,258 / 39,701,226 |

Source verification: `NativeDeliveryTracker::observe` stamps the actual native
poll even if the byte count does not change; server QUIC metrics call the
connection's `tx_metrics` producer. Thus the31–35/31–34plateaus are fresh native
observations, not simply one frozen dashboard row. Candidate native ACKs begin
changing at row35, earlier than control row36. They continue changing during
the candidate's later37–38second body hold. Native ACK progress is protocol
delivery, not a guaranteed missing Product prefix or completed application byte.

Candidate rows37→38, at elapsed37.175409→38.175504s, contain14,479,344more QUIC
native acknowledged bytes and41,784,926more directly sampled TCP `bytes_acked`.
The independently sampled client delivered-application total grows only
3,757,384B. These windows overlap much of the body gap but are not exact
per-request boundaries. They rule out a complete all-carrier native freeze;
they do not identify which received bytes are Original/copies, reordered suffix
or the blocked prefix. Ordinary telemetry has no exact DSN/repair-claim trace.

| Candidate sample row | Aggregate TCP NOTSENT, B | TCP Send-Q−NOTSENT, B | DOWN class backlog, B |
|---|---:|---:|---:|
| 31 | 0 | 2,059,532 | 33,473,462 |
| 32 | 247,130 | 43,580,208 | 41,239,478 |
| 33 | 30,718 | 42,646,438 | 41,036,634 |
| 34 | 82,160 | 43,210,530 | 54,507,238 |
| 35 | 190,408 | 43,430,206 | 43,998,476 |
| 36 | 119,620 | 43,095,934 | 57,257,564 |
| 37 | 279,800 | 43,453,022 | 53,755,145 |
| 38 | 0 | 21,514,822 | 33,602,275 |
| 39 | 0 | 21,061,034 | 43,479,499 |

The native unsent gate is functioning as scoped: little unsent payload remains,
but much already-sent TCP debt and shared queue accumulate after the outage.
At row37 TCP RTT is823–828ms; row38 is1010–1020ms, versus minimum~100ms.
Native QUIC RTT also reaches949ms at row36 and967ms at row38. This is evidence
of substantial native/shared residence, not proof that the gate can bound it,
or that enlarging/removing the reserve would fix ordered recovery. A low queue
at the local handoff does not guarantee low sent flight or fluent Product delivery.

### Outage-pair resource costs

| Sampled cost | Control | Candidate |
|---|---:|---:|
| Rows / final elapsed, s | 41 / 40.267986 | 41 / 40.175709 |
| DOWN / UP class-byte deltas | 2,395,008,304 / 40,328,112 | 2,423,906,954 / 34,042,344 |
| DOWN backlog p50 / max, B | 12,350,082 / 27,572,594 | 12,834,579 / 57,257,564 |
| UP backlog p50 / max, B | 65,609 / 191,083 | 64,783 / 106,476 |
| Client RSS peak / final, KiB | 102,284 / 102,284 | 83,452 / 83,452 |
| Server RSS peak / final, KiB | 349,428 / 322,896 | 340,616 / 329,484 |
| Last client / server lifetime CPU, % | 78.7 / 178.0 | 69.5 / 182.0 |
| Server TCP NOTSENT p50 / max, B | 1,556,876 / 46,450,556 | 0 / 305,625 |
| Server TCP sent-unacknowledged estimate p50 / max, B | 5,900,812 / 14,766,704 | 6,812,398 / 43,580,208 |
| Server TCP RTT p50 / p95 / max, ms | 270.282 / 479.144 / 540.490 | 283.377 / 860.363 / 1019.820 |
| Server QUIC RTT p50 / p95 / max, ms | 258.648 / 507.048 / 542.022 | 260.432 / 777.620 / 966.903 |

Lower return bytes, client CPU/RSS and local NOTSENT coexist with more than
twice the sampled peak shared DOWN queue and much worse native RTT tails.
These tradeoffs remain. Same profile does not mean packet-identical histories;
management and direct socket samples are sequential/cached at different stages.
No queue counter substitutes for exact missing-byte ownership. Warnings are
only the later duration-stop BrokenPipe/RemoteClosed and `H3_NO_ERROR`; no new
protocol failure accompanies the recorded gap.

## Separate information-only outage capture

Input is `./.tmp/reflection/results/mixed-combined-down-native-refill-outage-observer-0910/`.
The root reuses frozen `native-refill-copy-20260910/mptunnel` on working
`b0baca2`, with refill opt-out absent and periodic perf disabled. Only existing
feedback-return, hole-release and recovery events are enabled; no new build,
runtime policy, reserve or native controller change occurs. The frozen overlay
had already been removed from source before the earlier copy capture. This run
is **not** an ordinary performance comparison:80,117client and35,875server
diagnostic events produce41,805,177log bytes, and observation can change timing.

Runner exit0, probe `ok`, HTTP200, one duration-partial response:1,985,969,705B
over40.018863s gives397.006725Mbps. First body is0.582454s. The largest read
gap is1.117740s at36.473416–37.591156s, body counters
1,814,516,553→1,814,582,089. All72actual echoes succeed; p50/p95/max are
469.105/833.917/971.822ms. The worst echo starts32.843173s and ends33.814995s,
not at the maximum body gap. Echoes36.230029–37.081166 and
37.081191–37.992141 take851.137/910.950ms and overlap the later body hold.
Maximum successful-echo spacing is1.370503s; the last reply arrives40.338654s.
Stderr is empty; later RemoteClosed/reset/`H3_NO_ERROR` warnings occur around
duration-stop, not a recorded failed echo or HTTP request.

| Nominal phase | Body Mbps | Echo count / p50 / p95 / max, ms |
|---|---:|---:|
| 0–5s | 287.078 | 10 /271.885 /833.917 /833.917 |
| 5–15s | 450.589 | 19 /370.019 /514.440 /880.205 |
| 15–25s | 438.851 | 17 /531.414 /767.548 /770.193 |
| 25–30s | 441.980 | 10 /495.771 /780.081 /780.081 |
| Outage30–33s | 101.857 | 6 /101.359 /971.822 /971.822 |
| Restored33–35s | 493.597 | 2 /591.629 /614.068 /614.068 |
| Restored35–40s | 410.955 | 8 /612.733 /910.950 /910.950 |

All40raw body bins follow; all72full-precision attempts remain in the archive.

```text
Observer: 2.621,136.581,489.065,536.583,270.540,619.100,344.160,267.640,645.030,413.570,415.335,361.309,444.781,301.593,693.374,361.813,459.445,523.706,407.881,266.130,391.375,659.027,457.559,433.112,428.461,345.607,523.802,427.144,458.790,454.557,86.026,1.168,218.378,506.947,480.248,460.709,222.965,443.165,429.862,498.074
```

### Exact captured prefix and recovery boundary

Independent sender/receiver joins identify bulk stream1 in one stable session.
Client's last preceding release at Unix1789024393996 ends at DSN1,814,516,761;
the next head-closing release at1789024395114 starts exactly there, consumes
frame[1,814,516,761,1,814,516,825), and releases589,632ordered bytes through
1,815,106,393. The1118ms event interval matches the probe's1.117740s gap at
millisecond timestamp precision. The observed stream/body alignment is+208B,
derived from this capture's frontier and body counter, not imported from an
older run. The probe strips HTTP headers before recording body chunks; its
next read is65,536B, not the mux's589,632B release. Final body1,985,969,705+208
also matches a final recorded incoming frame end1,985,969,913. Literal HTTP
headers/payload were not captured, so+208is an observed alignment, not a
reconstructed header. Mux release can exceed local reads at duration-stop.

All138bulk probe/reply-owner joins preserve these exact output correspondences:
client TCP(index/physical/attachment)0/2/0→server wire1/incarnation1(12joins),
1/3/1→wire2/incarnation2(79),2/4/2→wire0/incarnation3(38), and client
UDP0/1/3→server wire0/incarnation4(9). All41management snapshots preserve
physical identities. Client attachment IDs and server incarnation IDs are
different namespaces; equal numeric path labels alone would not establish this.

The following timestamps share Unix base1789024390000ms. Role-local monotonic
origins differ by196ms and are not subtracted across processes.

| Offset, ms | Exact event |
|---:|---|
| 3996 | Last head advance before the missing64B prefix. |
| 4009 | Client admits token130 on selected TCP1/physical3/attachment1; frozen remaining proof budget946,985us. |
| 4079 | Actual server logical owner has required MAX1,881,625,625 applied and admits the reply on TCP wire2/incarnation2. |
| 4146 | Persistent-gap repair of the exact64B head queues, is accepted and dispatched as enqueue24330 on that same server TCP2/incarnation2; queue delay0ms. Original owner is UDP0/incarnation4, assignment age709,764us; target ETA1,018,495us, accepted-copy deadline937,913us. |
| 4956 | Selected-return proof expires28us late; full fanout resumes. |
| 5015 | Old token130 receipt arrives and is ignored,936ms after server reply admission. |
| 5084 | Accepted-copy wake is576us late; second repair24331 is admitted on different TCP wire0/incarnation3, queue delay0ms. |
| 5114 | Client TCP1/physical3/attachment1 delivers the exact64B head,968ms after first repair admission and30ms after the second admission. |

Only two accepted repairs in the full trace overlap this head. The releasing
carrier maps to the first, not the second. Thus most of this observed gap is
**after an actual repair admission**, not waiting for its queue slot, MAX grant,
proof timeout or lost actor wake. First repair was already accepted810ms before
proof expiry; an earlier switch to full fanout cannot be assumed to remove its
post-admission residence. Target ETA already predicts roughly one second of
service, consistent with the observed cost but not a per-byte causal proof.
Admission is not native handoff or transmission. The trace does not divide
those968ms among writer, native transport, network and receiver service, prove
that all queued delay was unavoidable, or authorize a shorter recovery timer.

### Observer profile, native context and resource costs

All41rows verify500/500Mbps,30/70ms delay, zero jitter/configured random loss,
burst65536 and netem limit8192. UDP flags are set at30.003547/31.220771/32.220871s
and first clear at33.220972s. Final sampled elapsed is40.269145s. All
class/qdisc drop deltas are0, excluding the deliberate endpoint UDP filters.

| Sampled cost | Observer |
|---|---:|
| DOWN / UP class-byte deltas | 2,323,582,683 /25,157,600 |
| DOWN backlog p50 / max, B | 24,596,208 /61,128,657 |
| UP backlog p50 / max, B | 37,367 /158,444 |
| Client RSS peak / final, KiB | 81,424 /81,424 |
| Server RSS peak / final, KiB | 299,600 /265,736 |
| Client lifetime CPU peak / final, % | 76.5 /66.2 |
| Server lifetime CPU peak / final, % | 164 /149 |
| Server TCP NOTSENT p50 / max, B | 0 /569,064 |
| Server TCP Send-Q−NOTSENT p50 / max, B | 11,247,868 /34,438,548 |
| Server TCP RTT p50 / p95 / max, ms | 448.322 /750.355 /899.877 |
| Server QUIC RTT p50 / p95 / max, ms | 496.447 /721.356 /747.847 |
| Server QUIC flight p50 / max, B | 10,769,583 /36,238,123 |

Same-epoch native QUIC acknowledged bytes remain906,149,718at fresh producer
samples31,593,819→35,753,151us, then increase2,400B at36,639,951us and continue.
Rows37→38 show QUIC+25,470,984B and directly sampled TCP+43,558,706B. These
sequential telemetry windows are not the exact body-gap window. Stable native
epochs and advancing producer timestamps rule out merely re-reading a frozen
dashboard value, not a slow particular prefix. Low TCP unsent debt coexists
with large already-sent/shared debt and inflated RTT. These sampled queue scopes
cannot be divided by capacity to assert the exact missing byte's residence.

The ordinary candidate's1.211s gap and this diagnostic's1.118s gap are distinct
events with different placement and observations. The observer supplies an
exact late-prefix/repair boundary in its own run, not an attribution retroactively
proven for the ordinary comparator. No periodic accepted-copy/unique-receipt
volume totals are collected here; event counts are not useful-byte throughput.

## Separate ordinary QUIC-only recovery classification

The unchanged working binary also runs the declared sole-QUIC outage classifier,
`./.tmp/reflection/results/quic-combined-down-native-refill-outage-classification-0910/`.
The root-created, tar-listed [QUIC classification archive](NATIVE_REFILL_QUIC_CLASSIFICATION_20260910.raw.tar.gz)
retains all five result files, driver log and runner, seven files total.
There is no TCP carrier/refill intervention in this mode. All41profile rows
retain500/500Mbps,30/70ms and zero configured random impairment. UDP is set
at30.005060s and first clear at33.097232s; sampled end40.098046s. DOWN/UP
class-byte deltas are2,034,104,315/31,957,431B; sampled peak backlogs
8,859,420/92,161B, class drops0 excluding the UDP filters.

Runner exit0 does **not** mean all probes pass: overall status is`loss`, bulk
status`ok`, HTTP200. Body1,905,096,286B/40.000452s gives381.014954Mbps, first
body0.410132s, maximum gap4.369520s at30.146914–34.516434s. All61successful
echoes precede disconnection; successful-only p50/p95/max are
104.814/145.775/297.300ms. One actual echo times out30.511071–33.514212s;
the other13failure records are `unavailable_after_disconnect` after the worker
closes that socket, not13additional independent network timeouts. Restored
echo performance is unavailable and must not be labelled healthy.

Body phase means0–5/5–15/15–25/25–30/30–33/33–35/35–40s are
351.078/442.731/433.835/455.026/22.624/66.786/**448.610Mbps**. The bulk
request survives, resumes34.516434s and returns to substantial throughput;
this is recovery lag, not a persistent QUIC low-rate ceiling in this capture.
All40raw bins and75echo outcome records remain in raw inputs:

```text
QUIC classifier: 9.605,385.398,452.105,452.580,455.700,464.591,457.774,461.394,455.638,452.346,400.299,407.535,426.675,438.533,462.529,455.116,373.881,441.032,447.070,451.303,455.801,442.778,388.382,436.906,446.077,456.096,466.299,459.498,458.781,434.458,67.873,0.000,0.000,0.000,133.572,446.514,447.433,464.337,432.081,452.684
```

The earlier [same-profile baseline classification](RETURN_ROUND_ORDINARY_20260910.md)
is historical context, not a new matched pair: older MPP QUIC/H2/raw gaps are
4.288/3.588/0.100s and35–40s body means453.763/470.592/475.111Mbps. H2 uses
explicit500Mbps priors while MPP discovers dynamically. Both sole-UDP systems
lose their only carrier and censor restored echo after one timeout; raw TCP
is unexposed to UDP filters. Mixed retains TCP and all echoes here, but develops
a later missing-prefix cost. These comparisons neither prove that mixed's
extra latency is unavoidable nor waive it because sole-QUIC outage lasts longer.

## Current disposition

The return pair permitted the already declared separate outage classification,
not a latency pass. The outage pair now exposes a material1.211-second late
recovery hold and worse restored echo service. This required exact-owner
attribution despite all requests succeeding and whole goodput rising.
Existing evidence shows native progress and large already-sent/shared debt,
not a demonstrated native-wake deadlock or later QUIC ACK restart. The separate
observer now localizes its own exact64B prefix to968ms after an accepted repair,
with punctual recovery/proof wakes, rather than delayed recovery admission.
Its downstream service split and the ordinary gap's exact ownership remain
unproven; a queue aggregate or the sole-QUIC result cannot select a fix.
Preserve both ordinary pairs, the diagnostic costs and the earlier healthy
mixed deficit; do not retune the reserve or substitute an average for recovery.
No global competitiveness, Cloudflare, restart-free recovery across all conditions
or release acceptance is claimed.
