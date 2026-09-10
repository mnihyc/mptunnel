# Receipt publication at an actual target-write pause: ordinary comparison

Recorded: 2026-09-10. Control `364d417`; bounded candidate `ba56290`.
**A demonstrated receipt-liveness correction, not an accepted performance fix
or a proved cure for the earlier five-second upload stall.** Ordinary UP
settlement succeeds in both cells. Candidate goodput increases 2.61%, but its
longest confirmation gap grows from 408 to 605 ms. DOWN goodput and maximum body
gap improve, but worst echo rises from 806 to 1046 ms. Those adverse timings
remain part of the decision, not something averages or focused tests override.

## Exact defect, bounded change and forecast

The actual server DATA actor admitted receipt of `[0,2)`. A real duplex target
accepted one byte and then returned Pending; feedback capacity/credit were
valid, yet no StreamAck was offered. The RED failed at that intended assertion,
not setup. Previously new receipt materialization followed target delivery,
while the retained-I/O helper serviced only already-materialized publication.
This violates the existing RFC 8.3/8.4 separation of receipt and consumption.
It does not identify the cause of the independently observed five-second stall.

Candidate `ba56290` offers one forced ACK-only generation when this retained
DATA write/flush first actually returns Pending. Further Pending wakes retry
the same generation. Immediately Ready operations retain their ordinary
postwrite publication. The same pinned operation, partial-write state, current
MAX grant, exact output fences, route deadlines and capacity wakes remain;
the new generation's blocked recipients re-arm capacity waits before parking.
No receive credit is granted before consumption. There is no new timer, quantum,
native controller, ACK batching policy or RFC redesign. The previously rejected
broad cadence trial is not restored.

Forecast: receipt progress and ownership release under target backpressure;
no guaranteed bulk gain for immediately Ready service and no asserted numerical
speed cure. Real or cooperative Pending can nevertheless add earlier feedback,
so composition can change. The predeclared stop condition preserves material
ordinary timing/cost harm rather than adding compensating cadence/timeout tweaks.

The focused real-actor RED and all 93 server checks are preserved in
`./.tmp/reflection/target-write-receipt-{red,green}-0910.log`; GREEN completes
in 1.03 seconds. Controls include first and established receipt, full ACK queue
with capacity-only wake, unchanged MAX, partial-write-once, retained route
expiry and terminal/recovery behavior. Independent review found no concrete
credit, wake or cancellation counterexample. This is mechanism evidence, not
end-to-end performance acceptance. The optimized build takes 1m04s and retains
one existing unused `apply_and_write_ready_stream_data_batch` warning in
`target-write-receipt-build-0910.log`.

## Ordinary profile and accounting

Root ran CONTROL then CANDIDATE with frozen ordinary binaries: healthy mixed
UP first, then the planned healthy mixed DOWN pair. No build ran during traffic.
The same existing target was retained; the preceding extra target/socket/PID
observer is not part of these ordinary cells. Inputs are the five files per
cell under `./.tmp/reflection/results/`, named
`mixed-combined-{up,down}-target-write-receipt-{control,candidate}-0910`.
All raw bins, attempts, byte totals and service samples remain authoritative.
Root created and listed the [23-file archive](TARGET_WRITE_RECEIPT_ORDINARY_20260910.raw.tar.gz),
including the four ordinary cells and original RED/GREEN/build logs.

The fixed profile is one shared 500/500 Mbps cut with DOWN 30 ms / UP 70 ms,
zero configured loss/jitter/QoS/blackhole, HTB rate equal to ceil, 65,536-byte
burst/cburst and 8,192-packet netem limit. Same settings do not imply identical
native histories, allocation or offered byte totals. Phase means use untrimmed
one-second confirmation/body bins and fixed nominal windows, not trimmed means.
Values above 500 Mbps are application confirmation/read bursts, not claimed
physical link rates. A returned target acknowledgment, native delivery counter,
class byte total and local socket acceptance are distinct accounting domains.

## UP: exact settlement with an adverse confirmation tail

Both cells are `ok`, `complete=true`, with valid exact target accounting, one
completed stream, zero failed streams, no probe errors and empty stderr. The
40-second offered upload is followed by final target-confirmed settlement;
there is no concurrent echo worker.

| Metric | Control | Candidate |
|---|---:|---:|
| Accepted = target-confirmed bytes | 2,070,937,600 | 2,192,441,344 |
| Total elapsed, s | 40.750652 | 42.042557 |
| Confirmed goodput, Mbps | 406.558 | 417.185 |
| First local write, s | 0.106840 | 0.105754 |
| First target confirmation, s | 0.410337 | 0.411884 |
| Maximum confirmation gap, s | 0.408449 | 0.605196 |
| Maximum local write gap, s | 0.533130 | 0.515499 |
| Elapsed beyond nominal offered period, s | 0.750652 | 2.042557 |

The candidate confirms 121,503,744 more bytes and runs 1.291905 seconds longer.
This is not an equal-byte drain-time comparison. The maximum confirmation gap
increases 196.747 ms while the local write gap improves 17.631 ms. First
confirmation is almost unchanged. Neither cell reproduces the original
5.140-second stall; its absence in both cannot be credited to the patch.
The UP JSON does not record exact maximum-gap endpoints, so this report does
not locate the adverse gap from an unrelated service row or one-second bin.

| Confirmation Mbps | 0–5 s | 5–15 s | 15–25 s | 25–40 s |
|---|---:|---:|---:|---:|
| Control | 320.627 | 386.205 | 413.035 | 415.355 |
| Candidate | 313.945 | 425.741 | 434.861 | 428.393 |

Startup mean is slightly lower; later fixed-window means are higher. All raw
confirmation bins follow, including the partial settlement bin: 41 control
and 43 candidate entries from second zero.

```text
Control: 22.017,125.260,470.592,590.856,394.412,357.993,354.323,344.745,643.781,500.023,411.714,390.115,439.139,270.939,149.274,847.441,396.650,309.758,401.176,147.074,788.211,218.628,387.973,475.425,158.011,803.261,278.685,365.953,345.506,594.262,172.631,277.540,829.283,452.948,215.091,170.726,788.249,326.203,436.208,173.776,741.649
Candidate: 22.018,115.631,743.344,305.948,382.782,449.175,509.128,418.013,367.282,205.062,829.026,454.609,366.710,395.971,262.438,657.975,460.421,297.175,398.059,428.795,220.176,437.029,572.224,564.297,312.454,425.678,217.753,729.885,365.512,406.758,325.975,447.264,271.762,631.583,312.100,534.405,439.049,452.986,350.001,515.183,479.139,393.507,65.250
```

### UP costs and limits

All 84 service rows verify the declared profile with no blackhole and zero
class/qdisc drop deltas. Count one shaped class per direction, not duplicated
parent/child/netem counters. UP contains upload data; DOWN is the return
direction, but includes all protocol/native traffic, not only ACK frames.

| Sampled cost | Control | Candidate |
|---|---:|---:|
| Rows / final elapsed, s | 41 / 40.004855 | 43 / 42.005752 |
| Whole DOWN / UP class bytes | 44,675,613 / 2,364,782,693 | 47,107,588 / 2,544,343,257 |
| Common rows 0–40 DOWN / UP class bytes | 44,675,613 / 2,364,782,693 | 45,158,113 / 2,453,200,209 |
| Peak DOWN / UP class backlog, B | 72,111 / 20,168,366 | 118,673 / 39,221,514 |
| Client RSS peak / final, KiB | 325,260 / 299,824 | 339,108 / 310,492 |
| Server RSS peak / final, KiB | 125,376 / 125,376 | 118,416 / 110,476 |
| Last client / server lifetime `ps` CPU, % | 163.0 / 97.8 | 175.0 / 103.0 |

The whole candidate cost window is two seconds longer. Common rows show return
bytes increasing 1.08%, not a demonstrated return-traffic saving. Peak sampled
upload backlog nearly doubles; that is relevant adverse context, not a measured
position or cause of the longest confirmation gap. Counter windows do not equal
the exact probe/settlement window. Sampled RSS is not post-teardown ownership;
lifetime `ps` percentages are not interval CPU or total-host utilization. The
only proxy warnings are subsequent QUIC `H3_NO_ERROR` closes, not failed uploads.

## DOWN: slightly more body service, not better timing in every phase

Both probes report `ok`, HTTP 200 and one intentional duration-stopped partial
8 GiB response, not a fully completed object. Stderr is empty. All 155 recorded
echo attempts succeed with no timeout, disconnect or mismatch: 78 control and
77 candidate. Echoes are serial 64-byte transactions at nominal 500 ms cadence;
slower replies alter attempt count. Quantiles use the probe's sorted index
`round((n-1)*rank)`; phase membership follows attempt start, not completion.

| Metric | Control | Candidate |
|---|---:|---:|
| Body bytes | 1,951,431,279 | 1,999,026,736 |
| Body elapsed, s | 40.000153 | 40.000236 |
| Whole body goodput, Mbps | 390.285 | 399.803 |
| First body, s | 0.598446 | 0.587642 |
| Maximum body gap, s | 0.371775 | 0.342114 |
| Maximum gap interval, s | 22.049271–22.421045 | 11.542428–11.884542 |
| Body counters before / after maximum gap, B | 1,070,059,839 / 1,070,083,839 | 557,005,946 / 557,071,429 |
| Echo successes / failures | 78 / 0 | 77 / 0 |
| Echo p50 / p95 / max, ms | 268.254 / 611.769 / 805.674 | 267.527 / 615.671 / 1046.090 |
| Worst echo interval, s | 1.759320–2.564994 | 16.424618–17.470708 |
| Maximum successful-echo spacing, s | 1.056145 | 1.166937 |
| Last echo completion, s | 39.767134 | 40.236985 |

Whole body goodput rises 2.44% and maximum read gap improves 29.660 ms, but
worst echo worsens 240.416 ms. Median/p95 for the whole capture are almost
unchanged; they conceal middle-phase differences. First body improves only
10.805 ms, not evidence of a material startup cure. Successful-echo spacing also
worsens. These are one pair's observations, not proof of either causal harm
or non-regression across execution histories.

Each phase entry is `body Mbps; echo count / p50 / p95 / max ms`. Profile stays
healthy throughout; none of these windows is an impairment/restoration phase.

| Phase, s | Control | Candidate |
|---|---|---|
| 0–5 | 314.500; 9 / 343.274 / 805.674 / 805.674 | 283.692; 10 / 187.201 / 791.495 / 791.495 |
| 5–15 | 412.918; 20 / 315.872 / 478.811 / 699.091 | 456.628; 19 / 384.427 / 603.673 / 615.671 |
| 15–25 | 405.189; 20 / 243.798 / 512.170 / 611.769 | 400.923; 18 / 320.664 / 827.172 / 1046.090 |
| 25–40 | 390.513; 29 / 229.625 / 405.553 / 663.932 | 399.864; 30 / 193.331 / 401.128 / 826.709 |

All 80 untrimmed body bins follow, from second zero. All individual echo
attempts and exact full-precision intervals remain in the raw results.

```text
Control: 2.097,155.927,268.744,727.462,418.271,460.594,299.391,548.135,372.877,478.236,362.765,357.249,439.582,380.153,430.194,393.026,347.119,449.467,365.432,418.711,431.133,443.677,465.812,413.720,323.791,297.895,416.813,383.978,451.502,428.740,358.463,386.099,365.149,444.656,426.643,433.781,331.230,343.196,343.665,445.882
Candidate: 2.597,176.161,109.480,790.321,339.903,436.006,470.540,424.140,244.524,768.729,418.563,338.802,341.089,671.505,452.383,435.925,388.754,336.223,514.318,345.065,380.470,389.769,404.358,388.734,425.612,359.545,433.992,454.186,371.009,320.953,439.897,394.571,403.207,360.801,381.733,375.701,452.346,448.942,417.494,383.577
```

### DOWN costs and limits

All 82 service rows verify the same fixed profile and zero class/qdisc drops.
UP is now the return direction. Cost domains and timestamp limitations are
the same as for UP; class bytes cannot identify receipt-frame counts or their
causal share of a particular echo delay.

| Sampled cost | Control | Candidate |
|---|---:|---:|
| Rows / final elapsed, s | 41 / 40.009399 | 41 / 40.004609 |
| DOWN / UP class bytes | 2,410,114,342 / 42,981,344 | 2,404,109,202 / 41,931,648 |
| Peak DOWN / UP class backlog, B | 22,460,948 / 181,180 | 26,295,282 / 250,840 |
| Client RSS peak / final, KiB | 82,800 / 82,800 | 93,168 / 86,652 |
| Server RSS peak / final, KiB | 370,076 / 365,572 | 374,076 / 352,996 |
| Last client / server lifetime `ps` CPU, % | 80.4 / 197.0 | 82.0 / 192.0 |

Class return bytes fall 2.44%, while sampled peak backlogs rise in both
directions. Neither observation establishes what delayed the candidate's
worst echo. Duration-close broken-pipe/RemoteClosed/QUIC close warnings remain
in both proxy logs; no additional echo or body-probe failure accompanies them.

## Disposition against the forecast

The exact receipt/consumption contract failure is real and its targeted test
now passes. The ordinary pair does not prove a broader latency improvement:
UP confirmation gap and DOWN worst echo/spacing worsen despite modest whole
goodput gains, and UP settles a larger accepted workload over a longer interval.
No integrity failure or recurrence of the original five-second stall occurs.
The test establishes this correction's mechanism, not that the old stall had
that mechanism. Ordinary snapshots do not identify exact ACK timing or the
critical byte behind either adverse tail.

Performance promotion remains unsupported by these pairs alone. Preserve the
correctness rationale separately from the practical decision; no larger timer,
cadence adjustment, favorable repeat or new native policy follows from this
report. CURRENT_CLOSURE_PLAN owns the next bounded decision and all remaining
return-cut, outage, TCP service, changing-link and browser acceptance gates.

### Concrete consequence and current decision

Independent source audit establishes a material ownership consequence, without
claiming that it occurred in the original five-second capture. Request streams
share one SessionSendBuffer: defaults are64MiB, the minimum of the configured
stream window and retained-repair bound. Source reads reserve there; actual
unique Data ACK release returns capacity and wakes other waiting streams.
Withholding a receipt for already admitted bytes can therefore delay another
stream at an exhausted shared budget. It also leaves exact retained-frontier
recovery eligible under its existing deadline/alternative/copy conditions.
Neither effect grants new MAX credit or makes target consumption unnecessary.

Do not overstate the byte bound: normal newly admitted ready batches are at
most512KiB; one separately accepted legal frame may reach1,048,512B. Gap closure
can unblock a window-sized ordered span, but already ACKed sparse positives
are not released twice. A catch-up ACK releases only still-charged unique
bytes, at most the remaining session budget. Request-local requalification
needs previously explicit ACK gaps; cold contiguous receipt absence alone does
not globally mark a carrier Suspect or prove native loss.

Root also ran the existing shared-budget release/wake and exact request ACK/
copy-suppression checks: seven pass in the same compiled test binary. Together
with the actual publisher RED/GREEN these support retaining the correction's
correctness scope, not a demonstrated other-stream speed gain. Ordinary timing
remains mixed and release acceptance open; no favorable repeat or compensating
parameter is selected. Continue the existing substantial TCP loaded-latency
owner with a causal observation, not another performance-model change here.
