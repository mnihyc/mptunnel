# ACK versus credit fanout: same-build diagnostic

Recorded: 2026-09-09. Category: mixed-mode return-service attribution.
**Diagnostic only; neither intervention is an accepted feedback policy.**
Ordinary runtime remains the scoped-model checkpoint `b2aa215`; release and
README/PERFORMANCE promotion remain deferred. See [the active plan](CURRENT_CLOSURE_PLAN.md).

## Question, origin and controlled intervention

The [combined fanout ablation](FEEDBACK_FANOUT_ABLATION_20260909.md) implicated
client TCP feedback publication in restricted return service, but did not
separate receipt ACKs from receive-credit updates. This three-cell experiment
asks which owner supplies material relief, without changing data eligibility,
native control, receiver batching, server behavior or the impairment profile.

All cells use `.tmp/reflection/bin/feedback-kinds-20260909/mptunnel`, the same
frozen `lab-diagnostics` binary. The client process leaves
`MPTUNNEL_LAB_FEEDBACK_ABLATION` unset for control, sets `tcp-max-withheld` for
TCP STREAM_MAX_DATA suppression, or `tcp-ack-withheld` for TCP STREAM_ACK
suppression. They run in that order, with fresh processes and no concurrent
compiler. The [exact patch](FEEDBACK_KIND_ABLATION_20260909.patch) is confined
to one file, 34 additions and six removals; source is reversed after freezing.

Independent review checked all seven publication/retry/pending/capacity sites.
MAX publication delegates to its selected retry; ACK publication and retry use
the same selector as their pending/capacity queries. Desired state still
advances, suppressed attachment cursors are not falsely committed, and initial
advertised credit, data, return plans and server logic are unchanged. Unset,
unknown and nonfeature selectors preserve ordinary eligibility. Selection is
cached once per process, not dynamically changed during a run.

Full independent fanout in `5e1ace67` fixed actual 7–14 s selected-return-wire
blackhole stalls. Suppressing either kind on TCP can restore such a failure
when QUIC return service fails. These interventions are causal discriminators,
not proposed fixes or proof that ACK/credit independence should be removed.

Predeclared information forecast: a uniquely large one-kind benefit focuses
that publication owner; material effects from both leave shared saturation or
interaction unresolved. Lower byte cost alone is insufficient without service
timing. This is not a full factorial experiment: the earlier both-withheld
capture is context, **not a fourth matched cell** or an interaction estimate.

## Profile and evidence

One 40 s HTTP download plus serial 64 B echoes at 500 ms intervals, 3 s timeout;
three TCP carriers and one QUIC carrier share a cut. DOWN stays 500 Mbps; UP
changes 500→10→500 Mbps during 15–25 s. DOWN/UP delay is 30/70 ms, with zero
configured jitter or random loss and no UDP blackhole. HTB burst/cburst is
65,536 B, netem queue limit 8,192. Every sampled effective profile agrees
across the three cells; regenerated netem seeds do not alter this zero-randomness
profile. Equal configuration does not make packet, placement or timing traces
identical.

Results `mixed-combined-down-feedback-kinds-{control,max-withheld,ack-withheld}-0909`
are retained in the [raw archive](FEEDBACK_KIND_ABLATION_20260909.raw.tar.gz),
including full probe/echo histories, service samples, logs and build material.
All three run logs return 0 and all `probe.err` files are empty. Each run has
one duration-partial HTTP 200 response and **zero completed 8 GiB bodies**.

## User-visible service, without phase trimming

| Measure | Control | TCP MAX withheld | TCP ACK withheld |
|---|---:|---:|---:|
| Body bytes | 1,651,901,179 | 1,679,245,192 | 1,818,681,960 |
| Body elapsed s | 40.000941453 | 40.000251474 | 40.000720851 |
| Whole-run Mbps | 330.372 | 335.847 | 363.730 |
| Raw 5–15 s mean, Mbps | 437.226 | 435.211 | 418.905 |
| Restricted 15–25 s mean, Mbps | 169.929 | 243.267 | 296.144 |
| Restored 25–40 s mean, Mbps | 376.967 | 350.247 | 384.824 |
| First body, s | 0.577292528 | 0.588209333 | 0.583166084 |
| Longest body gap, s | 0.655855142 | 0.275847953 | 0.390816364 |
| Actual successful / failed echoes | 79 / 0 | 79 / 0 | 80 / 0 |
| Echo p50 / p95 / max, ms | 273.619 / 510.861 / 1246.461 | 247.062 / 450.769 / 1175.848 | 225.305 / 468.112 / 569.006 |
| Echoes starting at 15–25 s: count / max ms | 19 / 1246.461 | 19 / 1175.848 | 20 / 470.844 |
| Echoes starting before 5 s: max ms | 387.602 | 385.105 | 540.505 |

No echo socket disconnects or times out; no missing disconnected slots are
counted as attempts. Slow serial exchanges delay subsequent attempts. Restricted
MAX suppression improves body mean by 43.2%, ACK suppression by 74.3%, but MAX
restored mean is 7.1% lower and ACK healthy mean is 4.2% lower than control.
Those phases and startup echo costs remain part of the outcome.

Control's longest body gap is 16.767996774→17.423851916 s, byte positions
794,143,583→794,155,583. Its worst echo is attempt 32,
16.177243937→17.423705155 s; the preceding attempt takes 628.587 ms.
MAX suppression's body gap is at startup, 0.588209333→0.864057286 s,
58,192→123,728 B. Its worst echo is still restricted, attempt 32,
16.056596552→17.232445026 s. ACK suppression's body gap is likewise startup,
0.583166084→0.973982448 s, 58,192→123,728 B; its worst echo is attempt 10,
5.065273899→5.634279692 s. Its restricted maximum is attempt 41,
20.637765602→21.108609809 s. Thus ACK suppression removes the observed
one-second restricted echo in this cell, not all loaded latency or startup delay.

Runtime logs retain close/reset warnings around duration stop: client mixed
handler reset at 04:11:37.282 UTC, broken pipe at 04:12:44.663, and reset at
04:14:13.153 respectively; server `RemoteClosed` follows at .356, .734 and
.224 in those seconds. Control and ACK cells also log QUIC `H3_NO_ERROR` close
at 04:11:38.291 and 04:14:14.180. These are retained termination observations,
not additional failed echo attempts or proof of a new in-load defect.

All 120 raw one-second application body bins follow, Mbps. Buffered ordered
delivery can exceed 500 Mbps in a bin; these are not physical-link rates.

```text
seconds     control
 0–10       2.097 142.274 274.009 658.985 411.691 507.786 428.470 459.178 261.801 653.463
10–20       431.274 281.008 464.443 501.764 383.076 371.871 119.958 90.737 260.139 138.217
20–30       144.634 125.718 102.054 169.234 176.725 226.229 385.536 376.367 394.276 391.553
30–40       289.958 485.193 416.763 309.275 565.131 344.157 309.158 427.575 371.087 362.250

seconds     TCP MAX withheld
 0–10       2.620 107.462 185.406 764.262 335.636 525.957 268.554 603.838 462.627 380.297
10–20       454.095 345.097 504.128 407.391 400.122 413.442 152.502 173.404 352.679 208.811
20–30       224.591 233.850 265.161 199.327 208.906 302.044 279.251 355.162 384.415 343.779
30–40       369.428 360.254 307.564 407.644 359.320 376.823 342.583 253.924 461.547 349.964

seconds     TCP ACK withheld
 0–10       1.572 215.410 337.974 526.816 544.733 215.637 672.107 386.630 446.926 401.792
10–20       406.794 375.023 438.046 434.018 412.080 474.030 402.877 280.315 387.570 237.450
20–30       235.466 194.733 228.018 310.257 210.719 336.269 391.709 387.187 365.879 403.672
30–40       430.756 373.453 382.161 360.107 397.207 405.211 419.188 387.978 292.509 439.079
```

## Return pressure, native progress and resource cost

| Sampled whole-run measure | Control | TCP MAX withheld | TCP ACK withheld |
|---|---:|---:|---:|
| Last service elapsed s, 41 rows each | 40.008363492 | 40.004951715 | 40.009015866 |
| DOWN class byte / packet deltas | 2,066,480,073 / 1,514,550 | 2,186,422,339 / 1,560,276 | 2,242,369,526 / 1,580,814 |
| UP class byte / packet deltas | 73,964,789 / 666,689 | 59,330,808 / 543,276 | 56,827,872 / 514,941 |
| DOWN / UP drops | 0 / 0 | 0 / 0 | 0 / 0 |
| Peak DOWN / UP backlog, B | 17,526,850 / 878,624 | 18,353,794 / 782,253 | 15,111,287 / 536,994 |
| Client RSS first / peak / last, KiB | 32,044 / 92,820 / 92,820 | 31,900 / 88,956 / 88,956 | 32,076 / 79,612 / 79,612 |
| Server RSS first / peak / last, KiB | 30,328 / 431,152 / 412,532 | 30,596 / 409,048 / 385,600 | 32,700 / 397,168 / 379,176 |
| Client / server peak lifetime `ps %CPU` | 100 / 188 | 102 / 195 | 104 / 194 |

Actual sampled UP10 rows are **15→24 in each cell**, selected from qdisc rate,
not inherited from earlier captures. Client management Unix-ms endpoints are
1788927072221→1788927081221, 1788927139605→1788927148606, and
1788927228097→1788927237096. Corresponding service elapsed windows are
15.005272138→24.006657273, 15.001895626→24.002853968, and
15.001795431→24.003819003 s. Server management endpoints are respectively
1788927072225→1788927081225, 1788927139607→1788927148607, and
1788927228094→1788927237095. Row 25 already reports restored UP500.

| Actual restricted sample window | Control | TCP MAX withheld | TCP ACK withheld |
|---|---:|---:|---:|
| DOWN byte / packet deltas | 249,976,458 / 197,338 | 363,120,329 / 264,960 | 409,860,070 / 294,154 |
| UP byte / packet deltas | 11,055,398 / 100,107 | 10,785,307 / 97,471 | 10,997,022 / 98,370 |
| UP delta rate, Mbps | 9.825508 | 9.585919 | 9.772933 |
| UP backlog min–max, B | 26,090–878,624 | 49,495–782,253 | 60,850–536,994 |
| Peak DOWN backlog, B | 1,968,436 | 9,859,370 | 9,049,370 |
| Peak UP queue length | 7,844 | 6,942 | 4,760 |
| Server→client TCP native ACKed-byte delta | 136,392,898 | 157,707,162 | 149,263,264 |
| Server→client QUIC native ACKed-byte delta | 130,367,820 | 210,108,588 | 260,233,265 |
| Client→server TCP native ACKed-byte delta | 3,534,675 | 2,931,283 | 2,668,556 |
| Client→server QUIC native ACKed-byte delta | 1,171,697 | 1,713,154 | 2,021,049 |

Native deltas match exact underlay/path/instance/epoch within each role. Every
TCP data carrier advances and remains active; none of these is a QUIC-only
data test. Last server TCP counters total 1,098,921,900 / 1,045,622,198 /
932,909,801 B; last QUIC counters are 856,848,513 / 1,028,652,548 /
1,203,858,833 B. Last client TCP totals are 22,264,695 / 14,589,697 /
12,894,820 B, QUIC 7,386,950 / 8,587,320 / 9,754,588 B. These are final
native counters, not whole-run deltas from nonexistent initial TCP samples.

Whole sampled UP bytes fall 19.8% and 23.2%, but restricted UP remains near
its cap in all cells. Remaining feedback and native ACKs share that service;
more forward progress can generate more return work. The deltas therefore do
not measure an exact removed ACK/MAX wire amount. Unchanged scheduling code
does not imply unchanged placement, repair or estimator trajectories.

Service clocks bracket sequential collection, not simultaneous router/endpoint
events; sample windows also differ from the ten full body bins. Do not add
parent/child qdisc counters or treat offloaded packets as exact physical packets.
Native ACKed bytes are not unique ordered application bytes. Queue drain time
under an assumed constant rate is not a measured individual echo delay. RSS
and lifetime-average `ps %CPU` do not prove a leak or interval CPU causality.

## Bounded conclusion and next-model constraint

Both interventions give material restricted body relief, with stronger observed
restricted echo improvement for ACK suppression. The result supports causal
contributions from **both publication kinds** and a shared return-service
pressure mechanism; it does not establish a unique ACK defect, additive
effects, or an exact split between wire cost and changed feedback timing.
MAX suppression leaves a one-second restricted echo; ACK suppression still
leaves return queues, loaded latency and lower healthy-phase throughput.

The information forecast narrows the next model to publication service, not
BBR or guessed link capacity, while rejecting a one-kind-only cure from these
three cells. One execution per cell supplies no confidence interval or proof
of latency non-regression. No safe suppression, deferred-return policy,
coalescing framework, new timer or threshold follows automatically. Preserve
independent-return recovery and credit/receipt timing contracts before selecting
any implementation. This diagnostic is complete; ordinary acceptance and the
global comparison/experience gate remain unresolved.
