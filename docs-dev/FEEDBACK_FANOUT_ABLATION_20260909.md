# Feedback fanout: same-build causal ablation

Recorded: 2026-09-09. Category: mixed-mode return-service attribution.
**Diagnostic only; not an accepted feedback policy or release result.**
Global disposition remains [CURRENT_CLOSURE_PLAN](CURRENT_CLOSURE_PLAN.md).

## Question and controlled change

The scoped ACK model removed repeated multi-range encoding, but mixed TCP+QUIC
still stalls when the return link narrows. The preceding
[codec/header evidence](SCOPED_ACK_ORDINARY_20260909.md) showed continuing
return pressure dominated by small payload-carrying TCP records, without proving
which publication behavior caused it. This pair tests whether full client
ACK/MAX publication on TCP contributes materially, rather than changing native
congestion control, data scheduling or the impairment profile.

Both runs use the same frozen `lab-diagnostics` binary,
`.tmp/reflection/bin/feedback-fanout-20260909/mptunnel`. Control leaves
`MPTUNNEL_LAB_FEEDBACK_ABLATION` unset; the client-only ablation wrapper sets it
to `quic-only`. The [exact patch](FEEDBACK_QUIC_ONLY_ABLATION_20260909.patch)
adds 30 lines and removes six. It withholds client TCP STREAM_ACK and
STREAM_MAX_DATA publication, including cumulative retries and corresponding
pending/capacity notifications. Other frames, TCP/QUIC data eligibility and
server logic are unchanged. The diagnostic source was reversed after freezing.

The forecast is directional: if redundant publication is a significant cause,
withholding it should reduce return costs and queues while improving restricted
body/echo service. There is no promised throughput multiplier. Changed feedback
arrival can change native state, placement and repair; unchanged scheduling code
does **not** mean identical data-allocation trajectories.

This is deliberately unsafe under failure of the chosen return carrier. Full
independent publication in `5e1ace67` addressed actual 7–14 s selected-wire-
blackhole stalls. Demonstrating its cost does not justify reinstating that defect.

## Matched profile and raw evidence

One 40 s HTTP download plus serial 64 B echoes, 500 ms interval and 3 s timeout;
three TCP carriers and one QUIC carrier share one cut. DOWN stays 500 Mbps;
UP changes 500→10→500 Mbps during 15–25 s. DOWN/UP delay is 30/70 ms, zero
configured jitter or random loss, no UDP blackhole; HTB burst 65,536 B and
netem queue limit 8,192. All sampled effective profiles match between runs.

Results are `mixed-combined-down-feedback-fanout-{control,quic-only}-0909` in
the [raw archive](FEEDBACK_FANOUT_ABLATION_20260909.raw.tar.gz), alongside the
build/run and intervention material. Both runs exit 0; both `probe.err` files
are empty. Every raw bin, echo attempt and service sample is retained.

## User-visible results

| Measure | Control | QUIC-only feedback ablation |
|---|---:|---:|
| Body bytes / elapsed s | 1,792,049,469 / 40.004071658 | 1,940,635,823 / 40.000130364 |
| Whole-run Mbps | 358.373 | 388.126 |
| Raw 5–15 s mean, Mbps | 438.435 | 423.271 |
| Restricted 15–25 s mean, Mbps | 266.246 | 399.106 |
| Restored 25–40 s mean, Mbps | 387.452 | 390.184 |
| First body, s | 0.585323017 | 0.577572174 |
| Longest body gap, s | 1.242531616 | 0.390761852 |
| Actual successful / failed echoes | 71 / 0 | 80 / 0 |
| Echo p50 / p95 / max, ms | 309.867 / 766.902 / 2405.033 | 202.823 / 326.132 / 803.287 |
| Echoes starting at 15–25 s: count / max ms | 11 / 2405.033 | 20 / 273.765 |

Each run returns one duration-partial HTTP 200 response, zero completed 8 GiB
bodies. Neither echo socket disconnects; there are no unattempted disconnected
slots. Serial exchanges delay later attempts when an exchange is slow.

Control's longest body gap is 22.107890147→23.350421763 s, byte positions
973,731,201→973,796,737. Worst echo is attempt 37,
21.185051984→23.590085474 s. Earlier restricted echoes take 1.636581 s at
15.719971682→17.356552630 and 1.284589 s at 18.266825226→19.551414624.

Ablation's longest body gap is at startup, 0.577572174→0.968334026 s,
bytes 58,192→123,728. Its worst echo is also at startup, attempt 3,
1.500647813→2.303934439 s. All 20 echoes starting during restriction take
100.845–273.765 ms; the restricted maximum is at 24.427348481→24.701113160 s.

All 40 one-second body bins follow, in temporal order, Mbps. These are
application delivery intervals, not physical capacity measurements; buffered
delivery can exceed 500 Mbps in an individual bin. Phase means above use the
full indicated bins without trimming.

```text
seconds     control
 0–10       2.097 134.934 248.941 677.356 414.441 496.834 211.408 604.179 474.618 431.011
10–20       460.707 316.551 550.170 164.682 674.185 436.939 263.871 203.698 326.093 74.637
20–30       136.666 444.401 41.433 278.867 455.857 324.163 420.568 307.840 392.647 405.062
30–40       272.126 494.882 383.313 309.860 524.340 366.289 359.332 443.858 194.896 612.602

seconds     QUIC-only feedback ablation
 0–10       1.572 198.159 227.445 702.440 318.848 495.644 381.802 486.311 419.318 452.246
10–20       387.261 342.049 447.381 424.445 396.250 405.672 335.817 509.175 372.465 404.592
20–30       398.074 390.711 307.737 498.749 368.069 395.939 390.079 395.703 413.212 379.068
30–40       383.666 397.118 344.670 338.566 497.465 406.387 375.877 398.213 306.844 429.949
```

## Return cost and continuing TCP data service

| Sampled whole-run measure | Control | Ablation |
|---|---:|---:|
| Last service elapsed s, 41 rows each | 40.052585422 | 40.005089833 |
| DOWN class byte / packet deltas | 2,177,806,717 / 1,587,636 | 2,385,862,136 / 1,588,657 |
| UP class byte / packet deltas | 75,292,922 / 648,968 | 26,277,843 / 258,553 |
| DOWN / UP drops | 0 / 16,508 | 0 / 0 |
| Peak DOWN / UP backlog, B | 29,716,864 / 1,101,574 | 14,329,492 / 63,176 |
| Client RSS peak / last, KiB | 95,728 / 95,728 | 90,020 / 90,020 |
| Server RSS peak / last, KiB | 355,536 / 331,044 | 309,988 / 307,172 |
| Client / server peak lifetime `ps %CPU` | 103 / 186 | 76.5 / 187 |

Actual restricted rows 16→25 span Unix 1788920886957→1788920895957 for
control and 1788920943790→1788920952790 for ablation. Their elapsed spans are
15.049642280→24.050641484 and 15.001730699→24.003156010 s. UP class byte
deltas are 10,803,585 / 6,069,800 B, or 9.602121 / 5.394523 Mbps; drop
deltas 13,047 / 0. Control already has 3,461 drops at the starting snapshot.
Restricted sampled backlog ranges are 360,505–1,101,574 / 27,639–57,589 B.

Same-instance, same-epoch native ACKed-byte deltas during those intervals:

| Direction / native carriers | Control B | Ablation B |
|---|---:|---:|
| Server→client, three TCP combined | 132,402,096 | 275,034,328 |
| Server→client, QUIC | 179,775,740 | 263,069,681 |
| Client→server, three TCP combined | 4,285,408 | 2,016 |
| Client→server, QUIC | 1,337,597 | 2,282,806 |

All three TCP data carriers remain active and make forward native progress in
both runs; this is not a QUIC-only data test. Last server TCP native counters
sum to 1,052,041,881 / 1,264,753,583 B; client TCP counters to
24,354,864 / 13,126 B. Native counters are not deduplicated application bytes.
Sequential telemetry timing, different sampled windows and offload prohibit
exact wire-efficiency arithmetic; parent/child qdiscs must not be added.
Lifetime CPU and RSS peaks do not establish interval CPU causality or a leak.

## Bounded conclusion

Withholding TCP feedback improves restricted goodput by 49.9%, eliminates the
observed multi-second restricted echoes and class drops, and lowers whole-run
sampled UP bytes by 65.1% while TCP still transfers substantial data. The
intervention supports a causal contribution from feedback fanout service cost,
not merely a correlation between a low goodput result and busy return traffic.
It does not establish an exact ACK-only wire amount: scheduling, native feedback,
repair and timing trajectories can change downstream of the intervention.

This is one pair, not a confidence interval or an accepted policy. Restored
goodput barely changes; healthy 5–15 s mean is lower, and startup delay remains.
It proves neither chosen-feedback-carrier failure recovery nor all-condition
competitiveness. Retain the independent-return availability invariant rather
than shipping this ablation. The next bounded candidate is recorded in the
global closure plan; no delayed-backup policy or broader redesign is accepted
by this experiment. README/PERFORMANCE and release promotion remain deferred.
