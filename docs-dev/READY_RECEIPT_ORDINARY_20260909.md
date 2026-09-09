# Ready receipt: bounded model correction and ordinary comparison

Recorded: 2026-09-09. Category: mixed-mode receipt and feedback service.
**REJECTED as a performance correction: both healthy test orders worsen tails.**
The restricted-link benefit does not authorize retaining this runtime candidate.
No performance acceptance, opposite-direction pair or release follows.
The [global closure plan](CURRENT_CLOSURE_PLAN.md) owns subsequent decisions.

## Origin, model and forecast

The [same-build feedback ablation](FEEDBACK_FANOUT_ABLATION_20260909.md)
established that independent client ACK/MAX fanout adds material return-service
cost, but withholding TCP feedback would revive selected-wire-blackhole stalls.
This correction retains every feedback recipient and changes an older constraint:
visible history `3353d7d` introduced contiguous-only ready batching for vectored
application delivery. Both receive actors disabled batching whenever reorder
data existed, and collection otherwise required offsets at the delivered
frontier. Receipt validation does not require immediate application delivery.

Existing mixed receive-owner evidence contains four already-queued 12,000 B
QUIC frames behind a TCP gap, each followed by separate publication/write setup.
All 48,000 B fit the unchanged default 512 KiB receipt-turn input bound. The
forecast was four publication turns becoming one for this concrete opportunity,
not a promised Mbps multiplier; other ready work, native queues and placement
determine whether that reduction materially improves ordered user service.

The implementation collects only already-ready same-stream Data within the
existing FIFO/item-snapshot/input-byte limits. It removes offset-contiguity and
empty-reorder eligibility tests, not receive credit, geometry, FIN, control or
resource validation. Original frames retain exact ingress metadata and pass
through the real receive owner in FIFO order. There is no wait for future data,
new timer, enlarged bound, fanout/cadence policy, ranking or native-CC change.
[RFC 8.3](../RFC.md#83-mpp-data-ack) now makes that receipt/delivery distinction
explicit and preserves independent publication before the bounded actor parks.

Proof: after accepted input i, let R_i be received coverage and d_i the
contiguous delivery frontier. Atomic receive validation precedes mutation;
returned output is only the new prefix [d_(i-1), d_i). Induction over FIFO
inputs gives the same accepted coverage and exactly-once concatenated delivery
as sequential application. Final scoped evidence retains the new positive and
truthful missing facts. On a later rejection, preserve accepted-prefix feedback
and delivery, then return the original error; client partial writes remain one
retained future and preserve I/O-error precedence. Previously retained reorder
payload can also become deliverable: the output envelope is prior retained
payload plus bounded new input, not merely the input-byte count. This proof
does not guarantee identical ACK transactions, rate samples or practical speed.

The actual four-ready-record producer/receive/two-attachment test failed on
control at batch length 1 versus 4. The earlier unqualified exact test filter
selected zero tests and proves compilation only, not mechanism success. The
corrected run passes 1,244 affected checks, including sparse/duplicate/hole-fill,
control/FIN/size boundaries and later real receive-resource rejection. Independent
review found no concrete regression in either caller or accepted-prefix handling.
These are mechanism checks, not the ordinary acceptance gate.

## Fixed comparison and profile

Control is ordinary `b2aa215`, fixed before the candidate. Candidate is the
uncommitted receipt correction frozen in
`.tmp/reflection/bin/ready-receipt-20260909/mptunnel`; both are ordinary optimized
builds without observers or feedback-ablation features. Raw result names are
`mixed-combined-down-ready-receipt-{control,candidate}-0909` in the
[raw archive](READY_RECEIPT_ORDINARY_20260909.raw.tar.gz), with the exact patch,
test/build/run evidence. No older or more favorable control substitutes for it.

One 40 s HTTP download plus serial 64 B echoes, 500 ms interval, 3 s timeout;
three TCP carriers plus QUIC share one cut. DOWN remains 500 Mbps; UP changes
500→10→500 Mbps during 15–25 s. DOWN/UP delay is 30/70 ms; zero configured
jitter/loss, no UDP blackhole, 65,536 B HTB burst and 8,192 netem queue limit.
All sampled effective profiles match. Both runs exit 0 with empty probe stderr;
no compiler overlaps either lab.

## Full ordinary outcome

| Measure | Fixed control | Candidate |
|---|---:|---:|
| Body bytes / elapsed s | 1,722,184,258 / 40.000098236 | 1,803,656,922 / 40.001099282 |
| Whole-run Mbps | 344.436 | 360.721 |
| Raw 5–15 s mean, Mbps | 433.747 | 433.185 |
| Restricted 15–25 s mean, Mbps | 235.091 | 299.175 |
| Restored 25–40 s mean, Mbps | 367.121 | 370.929 |
| First body, s | 0.587367362 | 0.581723053 |
| Maximum body gap, s | 0.581397285 | **0.622214704** |
| Actual echoes: successes / failures | 75 / 0 | 79 / 0 |
| Echo p50 / p95 / max, ms | 254.092 / 746.712 / 969.400 | **277.892** / 541.200 / 706.855 |
| Echoes starting during restriction: count / max ms | 16 / 969.400 | 19 / 706.855 |
| Echoes starting after 25 s: count / max ms | 30 / 512.866 | 30 / 307.333 |

Both HTTP 200 responses are duration-partial, zero completed 8 GiB bodies.
Neither echo socket disconnects; no unattempted disconnected slots are hidden.
Serial exchanges can delay later attempts. Control's worst echo is attempt 40,
22.283922589→23.253322849 s; candidate's is attempt 33,
16.741486343→17.448341199 s. Control's body gap is
16.788212007→17.369609292 s, bytes 824,268,378→824,282,978; candidate's is
21.863669963→22.485884667 s, bytes 1,016,305,394→1,016,319,994. The maximum
gap worsens by **40.817 ms**, and median echo by **23.801 ms**; neither is waived
as noise or declared a causal regression from this single pair.

All 40 untrimmed one-second application delivery bins, Mbps:

```text
seconds     control
 0–10       3.146 271.186 322.071 452.365 533.421 427.103 284.536 580.224 419.467 452.987
10–20       412.843 252.467 592.001 442.330 473.511 254.011 420.479 130.215 516.475 186.215
20–30       190.789 183.940 108.524 151.498 208.762 351.370 363.064 353.169 366.947 379.295
30–40       288.264 417.640 366.981 393.576 351.310 298.811 414.263 382.233 360.604 419.284

seconds     candidate
 0–10       2.097 107.050 338.982 516.239 576.828 440.325 232.881 642.413 431.881 422.048
10–20       457.940 288.353 557.239 483.150 375.622 500.470 413.382 124.921 513.408 212.617
20–30       279.540 213.054 319.194 197.850 217.312 388.535 347.366 375.926 371.901 369.079
30–40       324.273 434.285 373.822 403.005 379.129 335.199 363.870 377.848 357.944 361.749
```

Buffered delivery can produce an individual application bin above 500 Mbps;
these are not physical capacity estimates. All individual echo attempts remain
in the archive, including startup and pre-restriction latency spikes.

## Return service and resource cost

| Whole sampled measure | Control | Candidate |
|---|---:|---:|
| Last service elapsed s, 41 rows each | 40.051117988 | 40.006106716 |
| DOWN class byte / packet deltas | 2,159,276,552 / 1,580,764 | 2,236,547,929 / 1,631,521 |
| UP class byte / packet deltas | 80,224,133 / 700,000 | 64,581,063 / 580,298 |
| DOWN / UP drops | 0 / 0 | 0 / 0 |
| Peak DOWN / UP backlog, B | 21,528,870 / 910,962 | **31,266,892** / 724,093 |
| Client RSS peak / last, KiB | 92,176 / 85,168 | 75,416 / 75,416 |
| Server RSS peak / last, KiB | 393,112 / 376,936 | 393,016 / 393,016 |
| Client / server peak lifetime `ps %CPU` | 107 / 192 | 98 / 196 |

Actual UP10 snapshots are rows 16–25. Control Unix bounds are
1788921998478→1788922007478, elapsed 15.043315624→24.044476922 s; candidate
1788922768980→1788922777980, elapsed 15.001750796→24.003111195 s. The next
snapshot in each is restored UP500. Restricted interior class deltas:

| Measure | Control | Candidate |
|---|---:|---:|
| DOWN bytes / packets | 317,032,039 / 234,265 | 390,319,875 / 287,465 |
| UP bytes / packets | 10,672,046 / 82,353 | 10,818,906 / 93,934 |
| Observed UP service, Mbps | 9.485039 | 9.615352 |
| UP backlog min / max, B | 71,077 / 910,962 | 48,848 / 724,093 |
| DOWN backlog min / max, B | 192,684 / 11,575,512 | 844,314 / **21,290,571** |
| Server→client native TCP ACKed-byte delta | 141,492,824 | 181,489,328 |
| Server→client native QUIC ACKed-byte delta | 192,353,721 | 203,237,829 |
| Client→server native TCP ACKed-byte delta | 4,330,651 | 3,742,141 |
| Client→server native QUIC ACKed-byte delta | 1,482,529 | 1,413,580 |

Native deltas match exact instances and counter epochs; all TCP/QUIC carriers
remain active. Unlike the ablation, TCP return feedback is still independently
published. Native counters are not deduplicated application bytes. Whole-run
class windows differ slightly and telemetry collection is sequential; parent
and child qdiscs overlap. Do not infer exact wire efficiency, per-byte queue
delay, interval CPU or a memory leak from these aggregate counters.

## Initial pair disposition against the forecast

The observed opportunity is real and the model correction is independently
checked. Ordinary restricted goodput improves 27.3%, whole goodput 4.7%, with
19.5% fewer whole sampled UP bytes and better echo tail latency. This is a
useful partial benefit, not a component-only milestone. But return service
still nearly saturates 10 Mbps; both runs have zero drops, so packet loss is
not necessary for this remaining deficit. Healthy/restored means change little,
maximum body gap and median echo worsen, and DOWN queues grow substantially.

The pair does not measure realized batching count or prove that all observed
differences are caused exclusively by fewer publication turns. It establishes
neither non-regression nor full mixed-mode stall closure or competitiveness.
Retain the adverse timing and cost evidence; do not replace this control or run
until a favorable result appears. The affected controls below determine the
final rejection. README/PERFORMANCE and release remain held.

## Affected controls: QUIC and healthy mixed

The same unchanged ordinary candidate and `b2aa215` control were then compared
on QUIC-only under the same return restriction, and mixed with UP/DOWN both
500 Mbps throughout. Other delay/workload parameters remain unchanged. QUIC
ran candidate first, control second; healthy mixed ran control then candidate.
Tags are `ready-receipt-quic-{control,candidate}-0909` and
`ready-receipt-healthy-{control,candidate}-0909`. All four exit 0 with empty
probe stderr, one duration-partial HTTP 200, zero completed 8 GiB bodies and
no echo failures/disconnections. These are affected checks, not new baselines.

| Measure | QUIC control | QUIC candidate | Healthy mixed control | Healthy mixed candidate |
|---|---:|---:|---:|---:|
| Body bytes | 2,143,205,602 | 2,158,380,692 | 1,966,836,602 | 1,962,830,734 |
| Body elapsed s | 40.000692896 | 40.000128648 | 40.003644128 | 40.000099438 |
| Whole Mbps | 428.634 | 431.675 | 393.331 | 392.565 |
| Raw 5–15 s Mbps | 441.061 | 452.008 | 421.801 | 439.234 |
| Raw 15–25 s Mbps | 441.383 | 443.262 | 411.814 | 404.996 |
| Raw 25–40 s Mbps | 438.912 | 438.317 | 396.151 | 390.702 |
| First body s | 0.408391047 | 0.407006344 | 0.579399509 | 0.585432821 |
| Maximum body gap s | 0.100443382 | 0.106380811 | 0.310480249 | **0.331439630** |
| Successful echoes | 80 | 80 | 79 | 79 |
| Echo p50 ms | 104.519 | 104.245 | 280.491 | 286.590 |
| Echo p95 ms | 160.788 | 168.332 | 492.308 | **528.341** |
| Echo maximum ms | 318.800 | 287.649 | 817.280 | **1042.248** |

QUIC's longest gaps occur at startup: control 0.408391047→0.508834429 s,
bytes 12,000→48,000; candidate 0.407006344→0.513387155 s,
11,792→47,792. Worst echo is attempt 4 in each: control
2.000621650→2.319421869 s, candidate 2.000606763→2.288255572 s.
The candidate's +5.937 ms maximum gap and +7.544 ms p95 remain adverse values,
not concealed by its slightly higher goodput. No material QUIC degradation is
demonstrated by this pair, but it is not a universal non-regression proof.

Healthy mixed control's longest gap is 11.473123297→11.783603546 s,
bytes 529,565,786→529,577,786; candidate's is
11.776322693→12.107762323 s, 553,278,522→553,293,122. Worst control echo is
attempt 22, 11.092474734→11.909755201 s. Worst candidate echo is attempt 26,
**13.035325964→14.077573666 s**, +224.967 ms compared with control's maximum.
Similar average goodput does not erase these unhealthy timing differences.

```text
seconds     QUIC control
 0–10       9.604 384.731 459.284 446.309 437.093 452.135 448.748 438.371 398.959 439.488
10–20       446.090 460.869 454.962 429.193 441.794 458.638 444.486 446.858 461.694 385.635
20–30       443.744 436.427 455.998 441.000 439.347 461.666 448.863 453.344 394.741 437.507
30–40       449.511 451.934 445.565 429.505 456.434 448.753 453.427 440.371 421.037 391.021

seconds     QUIC candidate
 0–10       9.607 384.429 451.176 448.145 446.149 461.558 442.411 445.914 454.192 444.406
10–20       460.030 453.947 442.335 466.833 448.452 439.925 463.316 450.480 439.796 470.432
20–30       381.845 447.328 440.213 447.084 452.196 450.160 444.613 453.214 387.995 426.115
30–40       429.468 459.937 448.836 450.168 433.898 405.839 444.835 446.265 449.577 443.831

seconds     healthy mixed control
 0–10       2.620 109.553 378.691 607.495 357.397 500.119 256.955 581.848 295.903 506.698
10–20       452.232 265.073 519.144 497.549 342.492 481.869 442.693 294.587 543.405 405.652
20–30       429.516 357.766 378.935 387.486 396.232 398.280 351.981 400.180 406.280 386.689
30–40       311.559 470.295 342.695 385.001 395.407 457.448 242.576 522.612 429.892 441.363

seconds     healthy mixed candidate
 0–10       2.597 114.206 144.223 776.310 362.386 510.516 311.786 544.927 255.786 607.232
10–20       415.802 380.457 523.956 414.352 427.521 448.628 455.592 310.147 492.025 396.230
20–30       420.279 355.798 374.345 223.451 573.464 397.397 421.108 458.501 407.245 425.830
30–40       322.659 362.662 440.290 290.674 530.131 392.097 368.084 377.913 369.442 296.502
```

| Sampled cost | QUIC control | QUIC candidate | Healthy mixed control | Healthy mixed candidate |
|---|---:|---:|---:|---:|
| Service rows / last elapsed s | 41 / 40.004694 | 41 / 40.006416 | 40 / 39.463100 | 41 / 40.013181 |
| DOWN class bytes | 2,272,589,709 | 2,288,706,902 | 2,401,711,304 | 2,388,048,726 |
| UP class bytes | 35,645,747 | 36,327,694 | 94,647,790 | 71,328,680 |
| DOWN / UP class packets | 1,521,152 / 330,628 | 1,531,940 / 337,676 | 1,772,435 / 836,838 | 1,739,876 / 646,123 |
| Drops both directions | 0 | 0 | 0 | 0 |
| Peak DOWN backlog B | 9,179,136 | 8,623,368 | 27,469,333 | **32,137,720** |
| Peak UP backlog B | 84,247 | 89,560 | 294,110 | 185,852 |
| Client / server peak RSS KiB | 37,820 / 361,064 | 37,632 / 364,284 | 85,960 / 367,076 | 90,944 / 368,788 |
| Client / server peak lifetime CPU % | 97.2 / 154 | 97.7 / 155 | 125 / 200 | 109 / 196 |

QUIC's actual UP10 row 16→25 timestamps are Unix
1788922959632→1788922968632 (control) and
1788922894823→1788922903823 (candidate). UP byte deltas are
8,167,774 / 8,500,576 B, or 7.259202 / 7.555064 Mbps, zero drops. The healthy
mixed pair never restricts UP; its nominal 15–25 s bins are time windows only.
Different service-window lengths, especially healthy control's shorter window,
preclude exact efficiency arithmetic between whole sampled totals.

### Existing evidence around the adverse healthy echo

Candidate rows 14/15 at elapsed 13.006649/14.006766 s, client-management Unix
1788923122525/1788923123524, bracket most of its worst echo. DOWN backlog is
32,137,720→27,389,788 B; UP only 48,980→140,320 B, all at 500 Mbps with zero
drops. Server native SRTTs rise from TCP 290–317 ms / QUIC 288.918 ms to TCP
502–550 ms / QUIC 523.530 ms. All four carriers remain active and their native
ACKed-byte counters advance. This is loaded path service, not an observed
carrier outage or absent target reply.

At candidate row 15, the server echo flow has received and echoed 1,728 B;
the client has sent 1,728 B but received only 1,664 B. Thus the 27th reply is
still pending after real target echo. This localizes the late interval after
target processing, but identifies neither its actual carrier/copy nor its
position in a native or router queue. At 500 Mbps, 8Q/C for those sampled DOWN
queues is 0.514/0.438 s; these are aggregate drain times under constant service,
**not** measured echo residence, a lower bound, or additive delay components.

Control rows 12/13 at 11.004569/12.004685 s, Unix
1788923035604/1788923036604, bracket its 0.817 s echo. DOWN backlog is
13,820,963→8,795,700 B, UP 105,035→48,610 B, with native TCP SRTTs roughly
297–318→383–416 ms and QUIC 304.233→301.053 ms. Both runs therefore show
substantial loaded queueing. Existing samples do not establish whether the
candidate's larger queue is a repeatable consequence of changed receipt timing
or variation already present in mixed operation.

The predeclared adverse-timing stop paused promotion and deferred the opposite-
direction pair, not waived it. The global ledger then authorized exactly one
unchanged-source/profile reversed-order healthy pair to distinguish repeated
candidate harm from existing variability. It is preserved below alongside,
not instead of, the unfavorable first pair.

## Predeclared reversed-order healthy pair and final rejection

Tags `ready-receipt-healthy-reverse-{candidate,control}-0909` ran candidate
first, then control, with the same frozen binaries and healthy 500/500 Mbps
profile. Both exit 0 with empty probe stderr; each HTTP 200 body is duration-
partial, zero completed 8 GiB bodies. All attempted echoes succeed, without
disconnection. No source/profile change or favorable-repeat loop occurred.

| Measure | Reverse-pair control | Reverse-pair candidate |
|---|---:|---:|
| Body bytes / elapsed s | 2,033,553,456 / 40.000102130 | 2,060,439,984 / 40.001175908 |
| Whole Mbps | 406.710 | 412.076 |
| Raw 5–15 / 15–25 / 25–40 s Mbps | 429.451 / 433.361 / 415.959 | 433.369 / 427.274 / 430.014 |
| First body s | 0.577041060 | 0.582842119 |
| Maximum body gap s | 0.273263716 | **0.385985401** |
| Successful echoes | 80 | 79 |
| Echo p50 / p95 / max ms | 325.031 / 466.176 / 523.741 | **345.588 / 567.908 / 842.107** |
| Service rows / last elapsed s | 41 / 40.005265 | 41 / 40.005445 |
| DOWN class byte / packet deltas | 2,411,484,108 / 1,780,875 | 2,425,020,787 / 1,765,592 |
| UP class byte / packet deltas | 91,580,596 / 809,286 | 66,849,568 / 604,228 |
| Drops both directions | 0 | 0 |
| Peak DOWN / UP backlog B | 26,701,418 / 292,243 | **30,389,520** / 217,257 |
| Client / server peak RSS KiB | 89,804 / 377,208 | 87,424 / 333,912 |
| Client / server peak lifetime CPU % | 114 / 197 | 99.3 / 195 |

Control's longest gap occurs at startup, 0.577041060→0.850304776 s,
bytes 58,192→123,728. Candidate's is 24.408866888→24.794852289 s,
bytes 1,221,708,192→1,221,773,728. Control's worst echo is attempt 44,
22.008695707→22.532436313 s; candidate's is attempt 27,
13.681635816→14.523743190 s. The adverse gap/p95/max differences are
112.722/101.732/318.367 ms in this reversed pair. They reinforce rather than
cancel the first healthy pair's adverse 20.959/36.033/224.967 ms differences.

```text
seconds     reverse-pair control
 0–10       2.597 113.010 209.715 732.002 343.497 522.147 317.944 524.535 345.087 495.504
10–20       423.615 312.535 557.983 499.379 295.777 513.182 464.475 396.735 415.392 458.498
20–30       409.533 457.607 405.084 455.816 357.290 484.851 458.021 453.604 434.241 401.104
30–40       447.317 457.414 457.932 361.190 453.183 359.593 388.138 377.557 364.366 340.880

seconds     reverse-pair candidate
 0–10       2.597 118.151 243.794 659.245 403.000 432.789 460.281 374.432 299.071 617.021
10–20       405.675 272.827 591.485 428.837 451.273 418.673 457.900 253.860 604.140 387.671
20–30       417.954 449.839 449.972 443.758 388.968 364.594 490.734 390.850 327.966 478.084
30–40       426.352 464.640 321.236 478.970 503.246 425.917 473.607 462.850 264.199 576.969
```

Around the reverse candidate's worst echo, rows 14–16 at elapsed
13.002126/14.002210/15.002338 s (Unix
1788923470855/1788923471855/1788923472855) show DOWN backlog
23,150,019/23,372,998/28,460,264 B and UP 69,517/112,633/84,979 B. Native
TCP SRTTs at the middle row are 427–469 ms, QUIC 446.622 ms; all four paths
remain active and their native ACKed counters advance across the samples.
Unlike the first healthy pair's late response sample, at 14.002210 s the
client has sent 1,792 echo bytes but the server has received/echoed only
1,728 B. The 28th request has not yet appeared at the target. The evidence
therefore does not localize both slow exchanges exclusively to downstream
reply service or prove one queue is their complete cause.

Reverse control's worst echo is bracketed by rows 23/24 at
22.002572/23.002676 s (Unix 1788923534393/1788923535392), with DOWN backlog
12,960,638/26,556,570 B and UP 103,490/166,771 B. Native TCP SRTTs are
377–388→296–311 ms, QUIC 364.858→286.393 ms. Existing mixed operation also
queues substantially. Sampled 8Q/C, native flight and old estimator values
cannot identify exact echo position, critical copy, or the cause of the
candidate/control difference; concurrent queue quantities are not additive.

**Final disposition: rejected as a performance correction. The four runtime/
test files and nine RFC clarification lines are restored to the preceding
control.** Preserve the exact candidate patch and all eight
runs in the raw archive. The opposite-direction pair was not started after this
stop. No runtime acceptance, additional lab or release is authorized by this
report; the root agent owns the next decision. The restored source matches
`b2aa215` exactly across `src`, RFC and Cargo, and its freshly rebuilt affected
suite passes 1,241 checks. The archive contains 61 regular files, including the
exact reversible candidate patch, rollback checks, all raw results and the
unchanged runner/profile/configs; gzip integrity and byte comparison pass.

Why the forecast failed acceptance: fewer return bytes and 27% restricted
goodput gain did not compose into preserved healthy latency. Both orders show
worse maximum read gap and echo p95/max, with larger peak DOWN queues. The
symbolic proof covers admitted coverage, exactly-once prefix delivery and error
ownership; it does **not** prove feedback/estimator/placement timing equivalence.
Changing receipt/ACK transaction granularity can change those timing histories
without changing any controller constants. The precise causal mechanism for
the adverse healthy difference remains unproven; assigning it to a specific
estimator or batch delay would exceed this evidence. Do not retain the patch
merely because its narrow semantic proof and restricted throughput are good,
or add a compensating threshold to force the healthy comparison to pass.
