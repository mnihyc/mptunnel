# Latest-credit actor-yield ablation

2026-09-09. **Partial DOWN recovery; no universal non-regression or performance
acceptance.** Removing the nested input-only cooperation wrapper raises
healthy mixed DOWN to 403.212 Mbps from the two latest-state observations
386.555 / 384.476 Mbps, but remains below the retained 412.873 / 413.225 Mbps
controls. Mixed UP stays near 415 Mbps while both maximum gaps worsen
relative to the latest-state UP cell. All outcomes are retained.

## Exact question and execution

The [mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) and
[current ledger](CURRENT_CLOSURE_PLAN.md) govern this bounded ablation.
The [ten-cell state-model comparison](LATEST_CREDIT_SERVICE_20260909.md),
preserved at `e27e4f9`, established useful mixed-UP gains but a recurring
roughly 6–7% DOWN throughput deficit under both run orders. It did not
identify a new critical client-input stall or winning-byte placement cause.

The [exact ablation](LATEST_CREDIT_ACTOR_YIELD_ABLATION_20260909.patch)
removes only `tokio::task::coop::cooperative` around
`RemoteInput.recv_frame`. The existing outer whole-turn
`RelayServiceTurn::select` cooperative boundary remains. Input/Dispatch/Read
rotation, real MPSC await, finite ready-Data collection, credit/FIFO fairness,
Notify arming, RESET and cancellation are unchanged. The sole production
caller is under that outer boundary; this is not an unbounded uncooperative
actor. It adds no timer, rate or batching threshold.

The pre-run information forecast was that redundant input-only charging
could change Data service yield timing. Recovery of the observed 26–29 Mbps
loss was plausible **only if that boundary was causal**; zero gain or
regression was also plausible. No measured CPU fraction or removable-time
budget was claimed. Absent gain stops this branch; improved DOWN cannot
authorize sacrificing the mixed-UP benefit or latency.

Ten focused latest-state/whole-actor-cooperation checks and 773 affected
checks pass. Default test build is warning-free 55.88 s; ordinary default
build is warning-free 1m 04s. Independent removal-only review passes.
The frozen executable is
`./.tmp/reflection/bin/latest-credit-actor-yield-20260909/mptunnel`,
state-model checkpoint `83734b2` plus the exact archived ablation, with
diagnostics off at both endpoints.

Root ran exactly mixed DOWN → mixed UP, tag `latest-credit-actor-yield-0909`;
no build overlapped the cells. Results are under
`./.tmp/reflection/results/mixed-combined-{down,up}-latest-credit-actor-yield-0909/`.
Both runners exit 0, respectively 41.004742 / 42.039283 s, with empty
`probe.err` and no settlement guard.

## Profile, completion and timing

All 83 sampled shaping rows match the declared healthy shared-cut profile:
mirrored=true; eth0 server→client 30 ms, eth1 client→server 70 ms;
HTB rate/ceil 62,500,000 B/s = 500 Mbps both ways; zero jitter, no loss
clause or UDP blackhole; burst/cburst 65,536 and netem limit 8,192 unchanged.
There is no QoS/outage transition at 15, 25 or 30 s. Mixed has three TCP
plus one QUIC carriers sharing that cut, not four independent links.

| Ablation measure | Mixed DOWN | Mixed UP |
| --- | ---: | ---: |
| Exact useful B | 2,016,075,971 body-read | 2,131,230,720 confirmed |
| Probe s | 40.000306 | 41.055800 |
| Whole Mbps | 403.212112 | 415.285 |
| First useful delivery s | 0.615795 | 0.411310 |
| Maximum useful gap s | 0.323468 | 0.789938 |
| First / maximum local-write gap s | Not measured | 0.106810 / 0.630792 |
| Completion | One duration-partial successful body | Exact 1/1 complete |

DOWN is HTTP 200 / bulk status `ok`, one intentionally partial 8 GiB
response after 40 s, not full-response completion. UP is target-sink-ACK
accounted, accepted = confirmed = final bytes, valid exact ACK accounting,
zero failed streams/errors; time beyond the 40 s load is 1.055800 s.
Different completed work prevents treating shorter settlement alone as a
fixed-work speedup.

DOWN recovers 16.7–18.7 Mbps relative to the two latest-state observations,
but remains 9.7–10.0 Mbps below the retained controls. Its first body is
later than latest-state 0.583560 / 0.577596 s and controls
0.580507 / 0.581713 s. Maximum read gap 0.323468 s is below the first
latest-state 0.385547 s, but above its reverse-order 0.206452 s and both
controls' 0.265968 / 0.275026 s. The exact ablation gap spans
33.011939 → 33.335407 s, body bytes 1,676,103,105 → 1,676,115,105.

UP goodput 415.285 versus latest-state 412.793 Mbps retains the bulk gain
over `fcc0b22`'s 325.867 Mbps. However maximum confirmation/local-write
gaps worsen 0.697809 / 0.540541 → 0.789938 / 0.630792 s. Confirmation
remains better than `fcc0b22`'s 1.192361 s; local-write does not beat
`fcc0b22`'s 0.434806 s. The UP probe has no per-maximum-gap timestamps;
these gaps are not assigned to an invented exact stage.

DOWN loaded echo: all 80 actual 64 B attempts succeed, zero timeouts,
disconnects or unavailable slots; request/response each total 5,120 B.
Nominal interval/timeout remain 500 / 3,000 ms. p50/p95/maximum are
**0.334069 / 0.501201 / 0.561082 s**; maximum successful-response spacing
is 0.714449 s. Slowest echo index 30 spans 15.018660 → 15.579741 s,
not the bulk worst-gap interval. Median/p95 vary across the prior pairs;
the lower worst echo is not universal latency improvement. All exact
attempt timestamps remain in the archived probe. UP has no separate echo
measurement, not a zero echo latency.

## Full history and sampled stages

Means of matched full raw-bin windows, without trimming startup/tail:

| Window [s,s) | DOWN latest-state first / reverse Mbps | DOWN ablation Mbps | UP latest-state / ablation Mbps |
| --- | ---: | ---: | ---: |
| 0–5 | 299.958 / 289.023 | 282.777 | 293.354 / 273.975 |
| 5–15 | 426.595 / 429.475 | 417.455 | 415.203 / 431.370 |
| 15–25 | 406.032 / 414.299 | 423.883 | 450.435 / 468.174 |
| 25–35 | 383.995 / 363.743 | 439.524 | 388.801 / 391.887 |
| 35–40 | 359.250 / 371.687 | 381.201 | 498.580 / 386.687 |

DOWN recovery is concentrated in the later body, especially 25–35 s;
early phases are not all improved. UP's later body is also not uniformly
faster despite its similar whole goodput.

For UP, S/T/Rs/Rc mean client source-read / server target-written / server
reply-read / client reply-delivered bytes. For DOWN, source/delivery are
server `to_peer` / client `from_peer` aggregate reliable I/O, including
protocol and echo bytes. None is claimed C or exact mux F. Client/server
management snapshots are independent; L is each `service.jsonl` line.

| Cell / row | Client / server Unix ms | Source / delivery B | UP Rs / Rc B |
| --- | --- | ---: | ---: |
| DOWN L11 | 1788885578827 / 1788885578829 | 495,965,915 / 430,680,605 | — |
| DOWN L31 | 1788885598827 / 1788885598830 | 1,561,573,235 / 1,496,098,811 | — |
| DOWN L41 | 1788885608828 / 1788885608830 | 2,076,897,685 / 2,012,474,557 | — |
| UP L11 | 1788885622350 / 1788885622357 | 520,773,205 / 467,362,701 | 652 / 540 |
| UP L31 | 1788885642350 / 1788885642357 | 1,624,028,541 / 1,560,154,941 | 2,001 / 1,986 |
| UP L42 | 1788885653350 / 1788885653357 | 2,131,230,720 / 2,129,735,773 | 2,811 / 2,811 |

Target/client delivery advances in every sampled second in both cells.
These observations preserve forward/return distinctions, but cannot measure
the cooperation boundary's exclusive cost or critical-byte residence.
Final management counters precede final probe accounting.

All **82 untrimmed raw one-second bins**, Mbps; DOWN first (40):

```text
[
 1.049, 114.412, 199.089, 697.374, 401.962, 429.559, 280.037,
 576.671, 392.788, 409.143, 437.760, 427.295, 439.415, 372.361,
 409.524, 449.546, 467.000, 293.889, 528.906, 446.646, 411.002,
 474.194, 332.999, 474.429, 360.217, 470.088, 424.058, 445.286,
 441.673, 403.259, 464.585, 459.526, 467.068, 405.639, 414.060,
 416.721, 402.363, 407.667, 388.537, 290.716
]
```

UP confirmation (42):

```text
[
 11.007, 232.548, 540.925, 385.359, 200.034, 412.807, 245.891,
 315.754, 345.713, 369.195, 870.407, 471.335, 555.707, 441.354,
 285.539, 771.906, 229.001, 337.378, 637.325, 480.265, 421.547,
 472.339, 133.834, 592.349, 605.793, 467.617, 444.076, 324.409,
 505.944, 326.374, 516.424, 88.073, 189.488, 764.731, 291.733,
 439.781, 142.703, 441.975, 666.230, 242.745, 734.291, 97.943
]
```

Bins above 500 Mbps are buffered application reads/confirmations, not physical
link capacity. UP's last bin includes partial settlement; DOWN stops at its
duration cutoff. No favorable subset replaces the whole history.

## Bounded costs and disposition

| Cell | Client / server PID | Client RSS peak / last KiB | Server RSS peak / last KiB | Client ps %CPU max / last | Server ps %CPU max / last |
| --- | --- | ---: | ---: | ---: | ---: |
| DOWN | 328374 / 334009 | 89,856 / 89,856 | 337,060 / 318,196 | 124 / 124 | 199 / 199 |
| UP | 329313 / 334954 | 311,664 / 287,100 | 124,064 / 124,064 | 167 / 164 | 104 / 100 |

Each PID is stable. ps %CPU is a sampled process-lifetime average, not
exclusive input/actor CPU; RSS last is not settled post-load ownership.
No leak or normalized CPU-per-useful-byte claim follows from unequal work
and these samples.

Router HTB class first→last deltas, not parent-plus-netem sums:

| Cell / physical direction | Delta B | Delta packets / drops | Peak sampled class backlog B |
| --- | ---: | ---: | ---: |
| DOWN / download eth0 | 2,427,896,902 | 1,798,671 / 0 | 27,646,828 |
| DOWN / upstream eth1 | 140,977,420 | 860,324 / 0 | 383,733 |
| UP / return eth0 | 56,684,158 | 394,228 / 0 | 99,442 |
| UP / upload eth1 | 2,429,715,096 | 1,675,768 / 0 | 45,952,728 |

Class traffic includes framing, retransmission, copies and other work, not
only useful body. Peak UP backlog exceeds the latest-state UP observation's
34,936,366 B despite similar goodput; no per-byte delay or exact copy fraction
is inferred. Final class backlogs are DOWN 158/0 B, UP 0/0 B. Independent
review verifies probe/echo accounting, shape, PID/cost and class counters.

**Outcome versus forecast:** partial DOWN recovery with UP bulk retained is
consistent with a meaningful influence of nested cooperation. This single
ablation does not establish it as the sole cause, recover the full DOWN
deficit, or guarantee unchanged timing: adverse UP gaps and remaining DOWN
first/read gaps persist. Global non-regression and promotion remain withheld.
No additional code, threshold or best-run repeat is authorized by this report.

[LATEST_CREDIT_ACTOR_YIELD_20260909.raw.tar.gz](LATEST_CREDIT_ACTOR_YIELD_20260909.raw.tar.gz)
contains exactly 17 files: both five-file result sets, four test/build logs,
two run logs and the exact ablation patch. Members total 3,432,059 B;
archive size 393,799 B. Gzip integrity, exact ordered member list and
byte-for-byte comparison against all source files pass. The ten prior
ordinary comparators remain in their separately linked report/archive.

