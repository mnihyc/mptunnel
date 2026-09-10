# Native TCP refill admission: ordinary comparisons

Recorded: 2026-09-10. Frozen ordinary control `ba56290`; candidate is the
native-refill worktree based on `e722c46`. **The first TCP DOWN pair supports
the bounded mechanism, not release acceptance.** Whole goodput rises from
417.249 to 440.164 Mbps while echo p95 falls from 1269 to 323 ms. Native unsent
bytes and process costs fall substantially. Native RTT and median shared-cut
backlog nevertheless rise: this does not remove all latency or prove optimality.
The mixed DOWN pair is adverse for goodput (417.539 to 394.076 Mbps) despite
better whole echo tails; later phase timings and some resource costs worsen.
Uploads settle exactly in all four cells, but both candidate modes have longer
maximum confirmation gaps; mixed upload adds native queue/RTT/resource costs.
**Promotion is held.** No impairment or release acceptance follows this panel.

## Cause, policy and forecast

[Exact protected-byte observation](TCP_WIRE_FRONTIER_20260910.md) previously
placed at least 1.352786 seconds of one 1.497461-second echo inside the sender's
native unsent FIFO. Repeated successful socket writes had moved bulk beyond
MPP's one-frame priority arbitration. Structural actor availability was not
prompt native transmission eligibility. That evidence concerns an exact first
authenticated echo interval, not aggregate queue divided by a guessed rate.

The candidate combines an exact-socket native writable capability with outer
writer-ready publication and immediate preclaim validation. Blocked Originals
remain weak, unclaimed source notices; one carrier-level native wake returns
through normal arbitration. Input, control, cancellation, source identity and
partial protected-record ownership remain intact. Live capability errors retire
the carrier; unavailable optional acquisition retains structural-only behavior.
The preceding receipt-liveness correction remains present in both binaries.

The disclosed policy is `TCP_NOTSENT_LOWAT L=131072` native bytes, twice the
existing 65536-byte maximum service quantum. Linux's below-half wake leaves an
approximately 65536-byte refill reserve, about 1.049 ms at 500 Mbps. This is not
an exact protected-frame reservation: current 64 KiB Noise DATA is 65602 wire
bytes; packetization and one partial record can overshoot. There is no new
congestion window, pacing rate, socket-buffer size, Product-ACK wait or polling
timer. Native sent-but-unacknowledged flight is not bounded by this reserve.

Forecast: materially reduce the demonstrated unsent residence without starving
healthy useful service. Added wake/syscall cost, CPU contention, refill
starvation, partial-write corruption or adverse startup/recovery would falsify
promotion; the reserve is frozen, not enlarged until a favorable result appears.
No quantified universal speed gain was promised.

The actual server-writer permission RED claims 65536 source bytes despite an
explicit unavailable opportunity, failing the intended zero-claim assertion.
Separate five real-socket checks cover unavailable/wake/cancellation/terminal,
exact socket option scope and unchanged `SO_SNDBUF`. GREEN runs contain 81 TCP
checks and 35 prepared-source checks (overlapping filters, not 116 unique
tests). The ordinary optimized build takes 1m09s, with the existing unused
`apply_and_write_ready_stream_data_batch` warning only. Logs are
`./.tmp/reflection/native-refill-{red,adapter-green,green,prepared-green,build}-0910.log`.
These controls establish mechanism, not composed performance.

## Profile and measurement domains

Root ran CONTROL then CANDIDATE, no concurrent compiler or feature observer.
Both use three TCP carriers and the existing 40-second bulk-plus-serial-echo
workload. The fixed shared cut is 500/500 Mbps, DOWN 30 ms / UP 70 ms, no
configured loss, jitter, QoS change or outage. HTB rate equals ceil; burst and
cburst are 65536 bytes; netem limit is 8192 packets. All 82 sampled rows verify
that profile, with zero class and qdisc drop deltas. Both runner exits are zero.
Repeated HTB quantum warnings occur in both run logs; no shaping change follows.

Raw inputs are the five files under each
`./.tmp/reflection/results/tcp-combined-down-native-refill-{control,candidate}-0910/`.
Every body bin and all 123 actual echo attempts are retained in those records.
Phase means use untrimmed body bins; echo phases use request start. Quantiles
use sorted index `round((n-1)*rank)`. Serial slow echoes reduce attempted count,
not create unreported failed attempts. Application bins above 500 Mbps reflect
buffered reads, not physical link capacity. Sampling windows, native socket
counters and delivered application bytes are distinct domains.

## TCP DOWN: practical timing and sustained service

Both probes are `ok`, HTTP 200, with one intentional duration-stopped partial
8 GiB response, not full-object completion. Stderr is empty; all 43 control and
80 candidate echoes succeed with exact 64-byte replies and no timeout/mismatch.

| Metric | Control | Candidate |
|---|---:|---:|
| Body bytes | 2,086,315,690 | 2,203,092,310 |
| Body elapsed, s | 40.001350 | 40.041319 |
| Whole goodput, Mbps | 417.249 | 440.164 |
| First body, s | 0.586752 | 0.583288 |
| Maximum read gap, s | 0.402695 | 0.321440 |
| Gap interval, s | 11.491759–11.894454 | 22.066706–22.388147 |
| Body counters around gap, B | 560,562,550 / 560,628,086 | 1,196,007,804 / 1,196,022,404 |
| Echo successes / failures | 43 / 0 | 80 / 0 |
| Echo p50 / p95 / max, ms | 937.902 / 1269.087 / 1328.312 | 299.562 / 322.754 / 507.695 |
| Worst echo interval, s | 10.762009–12.090321 | 32.510816–33.018510 |
| Maximum successful-echo spacing, s | 1.328328 | 0.711607 |
| Last echo completion, s | 40.279200 | 39.818443 |

Goodput improves 5.49%, whole echo p95 improves 74.57%, and the longest body
gap improves 81.255 ms. Startup first-body difference is only 3.464 ms; this is
not a material first-response cure. Candidate's first body bin is lower
(2.097 versus 2.621 Mbps), and individual later bins still vary. The benefit is
not obtained by lowering total useful load or hiding slow/failed requests.

Each phase entry is `body Mbps; echo count / p50 / p95 / max ms`. All phases
are healthy; the labels do not imply a restriction or recovery event.

| Phase, s | Control | Candidate |
|---|---|---|
| 0–5 | 322.542; 9 / 560.616 / 969.395 / 969.395 | 342.651; 10 / 262.682 / 313.422 / 313.422 |
| 5–15 | 428.444; 9 / 1015.123 / 1328.312 / 1328.312 | 454.865; 20 / 298.419 / 312.595 / 313.561 |
| 15–25 | 432.110; 10 / 991.831 / 1306.451 / 1306.451 | 452.457; 20 / 302.849 / 322.754 / 500.664 |
| 25–40 | 431.449; 15 / 919.411 / 1243.619 / 1253.072 | 455.849; 30 / 299.076 / 428.626 / 507.695 |

All 80 raw body bins, in order from second zero:

```text
Control: 2.621,263.717,285.322,510.115,550.936,410.856,553.648,245.430,631.914,436.180,460.922,151.720,601.198,422.324,370.252,507.218,435.755,446.444,486.776,382.674,430.060,476.375,269.893,436.082,449.825,415.116,473.336,443.174,435.518,416.515,437.935,428.954,341.063,431.385,470.361,422.206,418.674,465.883,421.738,449.881
Candidate: 2.097,321.793,433.586,480.772,475.005,470.286,473.956,453.744,494.170,470.442,463.283,301.659,453.977,478.057,489.073,467.616,480.856,455.840,469.425,490.209,471.421,456.114,256.513,514.543,462.035,478.310,446.756,504.005,460.765,445.109,500.033,462.595,336.958,397.296,472.150,509.197,423.436,467.815,489.160,444.156
```

### Costs, native unsent debt and remaining shared queue

| Sampled cost | Control | Candidate |
|---|---:|---:|
| Rows / final elapsed, s | 41 / 40.016485 | 41 / 40.004610 |
| DOWN / UP class-byte deltas | 2,378,662,186 / 21,902,244 | 2,356,812,020 / 9,277,400 |
| DOWN backlog p50 / peak, B | 9,046,438 / 16,769,616 | 14,073,656 / 15,027,136 |
| UP backlog p50 / peak, B | 40,545 / 47,112 | 16,877 / 22,748 |
| Client RSS peak / final, KiB | 57,164 / 41,984 | 40,012 / 26,672 |
| Server RSS peak / final, KiB | 184,024 / 162,708 | 142,272 / 139,920 |
| Last client / server lifetime `ps` CPU, % | 58.6 / 124.0 | 23.0 / 56.0 |

Every socket sample contains three established kernel `bbr` carriers per role.
The following socket figures pool all 41 samples; RTT quantiles pool the three
connections, whereas byte figures first sum the three simultaneous sockets.
They are not exact per-echo stage timestamps or independent trials.

| Native observation, p50 / p95 / max | Control | Candidate |
|---|---:|---:|
| Server NOTSENT, B | 48,203,353 / 53,774,030 / 61,195,604 | 285,434 / 343,619 / 361,777 |
| Server Send-Q, B | 61,000,201 / 67,100,548 / 67,490,048 | 17,909,741 / 18,578,014 / 18,696,692 |
| Server sent-unacknowledged byte estimate `Send-Q − NOTSENT`, B | 12,777,152 / 15,890,352 / 20,203,944 | 17,612,555 / 18,370,584 / 18,419,014 |
| Server RTT, ms | 216.814 / 274.315 / 338.299 | 296.303 / 308.082 / 313.157 |
| Server minimum RTT, ms | 100.025 / 100.026 / 100.093 | 100.026 / 100.027 / 100.089 |
| Client NOTSENT, B | 0 / 1550 / 13,432 | 0 / 52 / 11,755 |
| Client RTT, ms | 214.205 / 269.463 / 340.853 | 298.257 / 310.539 / 318.305 |

Native unsent debt collapses without collapsing sent flight. This is consistent
with the intended handoff correction rather than a small congestion window.
The sent-unacknowledged estimate is native socket sequence accounting, not
unique application delivery or an exact physical packet-in-flight measurement.
Candidate server median RTT rises about 79 ms and median DOWN class backlog
rises about 5 MB; the successful echo remains around 300 ms, not the unloaded
100 ms. Lower unsent debt therefore must not be described as eliminating
shared-path queueing or all transport delay. The ordinary capture does not
track exact protected echo intervals; the earlier observer supplies causality,
while this pair supplies practical composition evidence.

Count only one class per direction, not class plus qdisc totals. Lower return
bytes do not by themselves identify which ACK/control/retransmission category
changed. Lifetime `ps` CPU is not interval CPU or host utilization; sampled RSS
is not post-teardown leak testing. Capture windows differ slightly from exact
body duration. Both proxy logs contain only the subsequent client reset/server
RemoteClosed duration-stop warning pair, not a failed recorded request.

## Mixed DOWN: lower useful goodput despite better whole echo tails

The next predeclared CONTROL→CANDIDATE pair uses the same binaries and fixed
reserve, normal three-TCP-plus-one-QUIC membership, unchanged healthy profile
and no feature diagnostics. Inputs are
`./.tmp/reflection/results/mixed-combined-down-native-refill-{control,candidate}-0910/`.
All 82 profile rows again agree, with zero blackhole and class/qdisc drop deltas.
Both runners and probes succeed, HTTP 200 with one duration-partial response;
all 157 actual echo attempts succeed and both stderr files are empty.

| Metric | Control | Candidate |
|---|---:|---:|
| Body bytes / elapsed, s | 2,087,704,363 / 40.000152 | 1,970,432,498 / 40.001110 |
| Whole goodput, Mbps | 417.539 | 394.076 |
| First body, s | 0.579448 | 0.587630 |
| Maximum read gap, s | 0.345924 | 0.370100 |
| Gap interval, s | 32.904737–33.250661 | 11.497553–11.867653 |
| Body counters around gap, B | 1,710,134,427 / 1,710,170,427 | 530,734,286 / 530,799,822 |
| Echo successes / failures | 77 / 0 | 80 / 0 |
| Echo p50 / p95 / max, ms | 289.487 / 678.431 / 1174.209 | 270.309 / 410.124 / 532.697 |
| Worst echo interval, s | 7.303547–8.477756 | 11.502401–12.035098 |
| Maximum successful-echo spacing, s | 1.321822 | 0.699359 |
| Last echo completion, s | 40.154328 | 39.758083 |

Whole goodput falls 5.62%, or 117,271,865 fewer body bytes in approximately the
same duration. Maximum body gap worsens 24.176 ms. Whole echo p95/max improve
39.55%/54.63%, but this is not a latency improvement in every phase. Candidate
body mean is lower in all four windows; its middle/late echo medians and late
p95 are higher. Similar historical runs vary, so one pair does not uniquely
attribute the goodput loss to this policy, but neither historical lower
controls nor better TCP-only results waive this observed adverse comparison.

| Phase, s: body Mbps; echoes / p50 / p95 / max ms | Control | Candidate |
|---|---|---|
| 0–5 | 275.246; 10 / 220.654 / 713.201 / 713.201 | 269.806; 10 / 212.927 / 382.288 / 382.288 |
| 5–15 | 470.494; 17 / 398.648 / 765.599 / 1174.209 | 431.870; 20 / 244.876 / 386.568 / 532.697 |
| 15–25 | 427.390; 20 / 222.790 / 349.783 / 364.456 | 406.464; 20 / 261.474 / 355.196 / 377.113 |
| 25–40 | 423.098; 30 / 286.719 / 424.823 / 850.991 | 402.070; 30 / 301.496 / 439.597 / 444.664 |

All 80 mixed raw body bins follow; all 157 individual attempts remain in raw.

```text
Control: 2.621,180.857,258.462,712.905,221.387,677.001,414.525,504.225,512.797,435.280,457.039,307.137,628.670,349.981,418.286,416.658,433.840,388.867,438.076,442.508,460.790,462.672,404.286,386.136,440.070,410.357,459.780,389.682,350.263,501.870,381.039,413.365,419.646,448.070,404.404,397.078,467.141,424.932,428.749,450.090
Candidate: 2.620,142.059,451.648,465.885,286.818,564.072,299.029,516.343,398.281,433.955,454.517,394.590,241.125,603.788,412.996,437.206,442.771,388.791,395.579,366.354,424.320,428.033,304.486,492.073,385.023,400.342,430.521,357.447,291.165,524.939,415.383,387.659,378.986,310.484,369.882,571.233,397.994,404.827,400.077,390.117
```

### Mixed costs and observed carrier service, not guessed allocation

| Sampled cost | Control | Candidate |
|---|---:|---:|
| Rows / final elapsed, s | 41 / 40.004938 | 41 / 40.058030 |
| DOWN / UP class-byte deltas | 2,423,488,123 / 40,606,052 | 2,403,965,363 / 40,517,518 |
| DOWN backlog p50 / peak, B | 12,365,144 / 19,920,810 | 12,970,924 / 21,537,188 |
| UP backlog p50 / peak, B | 63,101 / 216,643 | 72,026 / 111,728 |
| Client RSS peak / final, KiB | 90,832 / 81,996 | 74,204 / 74,204 |
| Server RSS peak / final, KiB | 345,660 / 327,584 | 370,104 / 336,972 |
| Last client / server lifetime CPU, % | 79.2 / 192.0 | 85.5 / 197.0 |
| Server TCP NOTSENT p50 / p95 / max, B | 1,200,320 / 17,997,578 / 24,377,804 | 0 / 237,910 / 586,440 |
| Server TCP sent-unacknowledged estimate p50 / max, B | 6,238,252 / 18,381,582 | 5,560,916 / 12,435,665 |
| Server TCP RTT p50 / p95 / max, ms | 266.829 / 372.651 / 380.400 | 275.641 / 404.594 / 428.102 |
| Server QUIC RTT p50 / p95 / max, ms | 274.040 / 358.028 / 378.027 | 275.840 / 384.990 / 429.455 |
| Server QUIC native flight p50 / max, B | 9,247,788 / 15,436,212 | 10,560,396 / 17,877,024 |

TCP remains kernel `bbr` on three stable sockets; both QUIC paths remain the
same physical instance throughout their respective captures. TCP queue/RTT
come from direct socket samples; QUIC values are sampled native management
observations, not simultaneous individual-echo measurements. TCP minimum RTT
medians remain 100.027 ms in both cells. Lower native unsent bytes coexist with
higher DOWN backlog and native tail RTT, higher server RSS and higher lifetime
CPU; the TCP-only resource saving is not universal.

For common management rows 1→40, every exact output retains one native counter
epoch with monotonic acknowledged bytes. Server TCP native acknowledged-byte
delta is 1,036,128,148→867,853,369; QUIC is
1,272,919,002→1,425,119,991. TCP's share of those counters is 44.873%→37.848%.
The client return-direction deltas are TCP 11,881,160→12,191,707 and QUIC
1,124,934→267,245. These are actual native counter differences, not ratios of
estimated capacity. They include protocol/control and repeated Product data;
they are **not unique Original placement or useful-copy shares**. Counter
producer timestamps and queued/cancellation tails differ from the body clock.

Thus the capture demonstrates changed carrier service distribution alongside
the smaller unsent backlog, but does not separate Original/repair allocation,
native competition or random execution history as the cause of lower useful
goodput. Ordinary management lacks exact per-cause accepted-byte counters.
Neither more QUIC native bytes nor lower TCP unsent debt alone proves improved
MPP allocation. No new parameter adjustment follows this observation. Warning
logs contain the duration-stop BrokenPipe/RemoteClosed and subsequent
`H3_NO_ERROR` close, with no failed recorded request.

## UP: exact settlement, but adverse confirmation gaps in both modes

Root completed the predeclared TCP and mixed UP CONTROL→CANDIDATE pairs with
the same frozen binaries and reserve. These are opposite-direction affected
controls, not repeats of mixed DOWN. Inputs are the five files per cell under
`./.tmp/reflection/results/{tcp,mixed}-combined-up-native-refill-{control,candidate}-0910/`.
All four runner exits are zero; probes are `ok`, one stream completed, zero
failed streams, exact and valid target acknowledgment accounting, no probe
errors and empty stderr. Each upload offers work for 40 seconds and then waits
for exact settlement. There is no concurrent echo worker in this workload.

| Metric | TCP control | TCP candidate | Mixed control | Mixed candidate |
|---|---:|---:|---:|---:|
| Accepted = target-confirmed bytes | 2,194,997,248 | 2,322,333,696 | 2,144,927,744 | 2,115,633,152 |
| Total elapsed, s | 41.578920 | 41.316458 | 41.442981 | 41.198541 |
| Confirmed goodput, Mbps | 422.329 | 449.668 | 414.049 | 410.817 |
| First local write, s | 0.105607 | 0.105267 | 0.105014 | 0.106935 |
| First target confirmation, s | 0.408636 | 0.408035 | 0.409412 | 0.414631 |
| Maximum confirmation gap, s | 0.514412 | 0.617220 | 0.465272 | 0.694198 |
| Maximum local write gap, s | 0.272396 | 0.316957 | 0.316136 | 0.309293 |
| Time beyond nominal offered period, s | 1.578920 | 1.316458 | 1.442981 | 1.198541 |

TCP confirms 127,336,448 more bytes in 0.262462 seconds less total time;
goodput improves 6.47%. Its longest confirmation gap nevertheless grows
102.808 ms and local write gap grows 44.561 ms. Mixed confirms 29,294,592 fewer
bytes in 0.244440 seconds less total time, with goodput down 0.78%; confirmation
gap grows 228.926 ms while local write gap improves only 6.843 ms. These are
not equal-byte drain-time comparisons. First confirmation changes less than
6 ms, not a material startup cure. UP output has no exact maximum-gap endpoint
fields; neither phases nor a nearby management row locate those gaps exactly.
The old five-second upload stall does not recur in either side of either pair,
so these results do not prove its cause or repair.

| Raw confirmation Mbps by phase | 0–5 s | 5–15 s | 15–25 s | 25–40 s |
|---|---:|---:|---:|---:|
| TCP control | 359.533 | 429.182 | 415.970 | 439.423 |
| TCP candidate | 385.483 | 450.613 | 453.725 | 449.898 |
| Mixed control | 322.905 | 433.592 | 384.430 | 444.834 |
| Mixed candidate | 328.619 | 421.981 | 421.873 | 401.095 |

TCP fixed-window means improve throughout; mixed is better in startup and
15–25 seconds but worse in the other windows, especially late. All 168 raw
one-second confirmation bins follow (42 per cell), including settlement tails.
As with DOWN, confirmations above the shaped rate are buffered application
acknowledgments, not a physical capacity claim.

```text
TCP control: 11.415,409.994,261.095,658.506,456.655,210.239,614.466,519.045,239.600,371.720,730.857,237.503,392.692,511.181,464.519,320.864,502.792,478.675,382.730,485.491,430.440,305.136,456.131,472.383,325.059,426.246,473.956,442.499,387.448,563.086,403.701,384.828,396.361,433.587,448.266,452.984,414.712,475.005,439.354,449.315,401.605,317.837
TCP candidate: 20.041,455.082,482.870,489.161,480.262,491.768,357.140,512.654,484.338,504.313,478.300,315.874,467.410,417.154,477.176,520.249,472.263,411.330,535.822,368.485,507.626,421.396,383.621,489.095,427.361,502.422,514.384,397.226,497.023,487.393,482.053,413.663,291.414,460.439,463.455,521.896,403.045,478.217,491.650,344.195,460.062,399.342
Mixed control: 21.494,113.726,218.768,867.556,392.980,308.709,468.145,517.664,249.473,695.058,336.785,354.847,586.922,404.130,414.188,231.307,642.017,361.663,381.497,442.883,389.443,384.023,432.751,364.735,213.983,743.817,380.663,426.630,264.794,648.707,433.350,348.083,464.704,356.331,446.074,465.427,447.653,423.602,339.753,482.921,364.328,327.837
Mixed candidate: 16.463,296.319,322.533,407.039,600.741,307.310,407.789,577.489,432.683,383.324,427.534,412.134,329.514,541.375,400.662,450.791,384.920,379.524,398.626,523.144,463.610,313.955,361.233,428.678,514.249,362.730,464.914,349.716,286.285,335.295,490.495,492.106,574.997,343.256,338.505,282.319,397.640,556.302,478.342,263.529,593.601,233.395
```

### UP profile, resources and remaining native/shared debt

All 168 sampled UP rows verify the same 500/500 Mbps, DOWN 30 / UP 70 ms,
zero configured loss/jitter/outage and unchanged HTB/netem settings. All class
and qdisc drop deltas are zero. Each cell has 42 rows; final elapsed is
41.004294 / 41.004380 / 41.009373 / 41.126654 seconds in table order. Therefore
the last sample still precedes exact target settlement, and the mixed candidate
sampling window is slightly longer. CPU/RSS and queue accounting caveats above
continue to apply; these are not post-teardown leak or per-echo measurements.

| Sampled cost | TCP control | TCP candidate | Mixed control | Mixed candidate |
|---|---:|---:|---:|---:|
| Whole DOWN return class bytes | 22,418,953 | 9,267,135 | 49,825,818 | 61,158,742 |
| Whole UP data class bytes | 2,468,153,997 | 2,436,885,065 | 2,492,249,708 | 2,440,271,950 |
| Common rows 0→40 DOWN bytes | 21,783,673 | 9,122,693 | 48,430,582 | 60,212,811 |
| Common rows 0→40 UP bytes | 2,402,536,179 | 2,392,743,419 | 2,438,750,742 | 2,412,791,432 |
| UP class backlog p50 / max, B | 11,400,258 / 16,791,196 | 16,586,786 / 17,550,634 | 13,211,564 / 18,499,096 | 14,749,495 / 33,977,285 |
| DOWN class backlog p50 / max, B | 16,839 / 46,003 | 6464 / 13,204 | 28,515 / 61,750 | 40,900 / 80,970 |
| Client RSS peak / final, KiB | 143,468 / 128,532 | 135,368 / 107,216 | 306,996 / 290,744 | 386,088 / 356,588 |
| Server RSS peak / final, KiB | 54,520 / 38,692 | 43,480 / 39,360 | 103,448 / 94,116 | 64,712 / 64,712 |
| Last client / server lifetime CPU, % | 89.8 / 42.2 | 43.4 / 23.0 | 175.0 / 104.0 | 183.0 / 121.0 |

TCP return bytes and CPU fall, but median UP shared queue rises by about 5.2 MB.
Mixed return bytes grow 22.75%, peak UP backlog grows from 18.5 to 34.0 MB,
client RSS grows about 79 MiB and both roles' lifetime CPU rise despite slightly
less useful work. The local unsent reduction cannot erase these adverse costs.

| Native sender observation, p50 / p95 / max | TCP control | TCP candidate | Mixed control | Mixed candidate |
|---|---:|---:|---:|---:|
| Client TCP NOTSENT, B | 53,911,036 / 60,755,978 / 67,944,827 | 290,468 / 339,866 / 354,780 | 16,808,860 / 27,132,784 / 29,092,318 | 104,256 / 165,072 / 272,812 |
| Client TCP sent-unacknowledged estimate, B | 12,719,232 / 13,903,696 / 17,842,256 | 17,670,258 / 18,619,488 / 18,730,502 | 7,439,824 / 10,518,272 / 13,363,592 | 3,834,598 / 10,827,684 / 14,021,712 |
| Client TCP RTT, ms | 212.659 / 231.453 / 289.949 | 297.158 / 313.826 / 319.924 | 241.382 / 322.877 / 341.467 | 276.368 / 426.183 / 546.648 |

All TCP samples retain three `bbr` sockets per role, with sender minimum RTT
medians 100.025 / 100.024 / 100.025 / 100.027 ms. Native flight remains large
while unsent debt falls; this is not another small congestion window. The
mixed client QUIC native RTT p50/p95/max also rises from
249.125/319.485/343.537 to 271.880/491.549/538.762 ms, while its native flight
p50/max rises from 6,860,700/10,406,484 to 10,935,012/20,836,200 bytes. These
global observations do not assign a precise causal share of the longest target
confirmation gap, but clearly prevent describing lower NOTSENT as universally
better latency or less shared-path pressure.

Native byte distribution is again taken from stable exact-output/epoch
acknowledged counters over common management rows 1→40, not displayed rates:

| Native acknowledged-byte delta | Control TCP / QUIC | Candidate TCP / QUIC |
|---|---:|---:|
| TCP-only client data direction | 2,259,511,044 / absent | 2,250,662,874 / absent |
| TCP-only server return direction | 5,991,419 / absent | 1,927,250 / absent |
| Mixed client data direction | 1,279,267,430 / 1,020,544,509 | 707,780,808 / 1,566,999,355 |
| Mixed server return direction | 11,338,953 / 4,402,163 | 17,841,617 / 5,418,033 |

All counters are monotonic within unchanged epochs. Mixed TCP share of these
client native bytes falls from 55.625% to 31.114%. That confirms changed
carrier service, not unique Original placement or retransmission counts.
Per-output native origins and sample ages differ; first/last management rows
are not exact start/settlement boundaries. The TCP logs have no warnings; mixed
server logs contain only the later `H3_NO_ERROR` close. No integrity, incomplete
settlement or new transport error accompanies these four uploads.

## Current disposition

The TCP healthy DOWN result meets the ordered-service forecast materially;
TCP UP also carries more exact useful bytes at lower process cost. However,
native/shared latency grows, both upload maximum confirmation gaps worsen,
mixed DOWN loses 5.62% goodput, and mixed UP has adverse queue/RTT/return-traffic
and client-memory costs. Those are part of the model's practical composition,
not waived by the bounded-wake proof or a good TCP-only average. This complete
eight-cell panel does not support unqualified non-regression or promotion.

These are fixed ordinary pairs, not identical native histories or proof of one
specific causal regression. The next decision must retain this tradeoff and
use existing ownership/native evidence, not tune the reserve until it passes.
No original five-second-stall cure, blackhole recovery, mixed-mode release,
Cloudflare competitiveness or ideality claim follows. No public performance
table or runtime threshold is changed on these numbers.

Root created and listed `NATIVE_REFILL_ORDINARY_20260910.raw.tar.gz`: 49 files
(40 exact result files, eight build/test/run logs and the runner). It preserves
all eight ordinary cells before further disposition. The separately declared
reverse-order mixed pair is not included in this closed archive.
