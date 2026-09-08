# Latest-credit ingress: ordinary comparison

2026-09-09. **All ten ordinary cells are complete; performance is not
accepted.** Mixed UP improves 325.867 → 412.793 Mbps and its maximum
confirmation gap falls 1.192361 → 0.697809 s, with adverse local-write and
single-mode gaps retained below. Mixed DOWN instead falls 412.873 → 386.555
Mbps and its maximum read gap worsens 0.265968 → 0.385547 s. Loaded echo
median/p95 improve, but maximum/spacing worsen. This supports a bounded UP
benefit, not global non-downgrade or a completed architectural cure. The
predeclared reverse-order DOWN pair repeats the roughly 6–7% throughput
deficit, but not the adverse maximum read/echo gaps.

## Question, model and status

The [mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) and
[current transaction](CURRENT_CLOSURE_PLAN.md) govern this comparison.
The preceding final-only MAX fold is ordinary checkpoint `fcc0b22`;
its [ordinary evidence](READY_CREDIT_SERVICE_20260908.md) showed useful mixed
bulk improvement but adverse maximum confirmation gaps. Its subsequent
[healthy diagnostic](READY_CREDIT_RETURN_SERVICE_20260908.md) reconciled
187 Original replies and found prompt enqueue/claim/write and prefetched-Data
return. During the largest 1.729 s reply-frontier hold, before any covering
reply decoded, disjoint ordinary-QUIC reader windows contained conservatively
at least 1.254354 s downstream send-await, including 0.890604 s handing
MAX_DATA. These are wall waits, not exclusive CPU or promised removable time.

The implemented replacement represents received matching credit as one shared
latest maximum at attachment ingress, retaining the first exact source of the
greatest value. It replaces the final-only fold rather than stacking another
fold. ACK/Data and other events retain their FIFO content/order; fair service
must prevent either continuously ready FIFO work or isolated credit from being
starved. Same-stream RESET seals credit ownership; an opposite-direction FIN
or carrier error does not revoke an already received logical grant.
Wire publication, grant authority and resource/window limits are unchanged.

The [actual publisher/forwarder RED](LATEST_CREDIT_INPUT_RED_20260909.patch)
reaches the intended two credit turns versus one after semantic premises
pass; the two previous closed/order controls pass. The RED default build is
warning-free, 59.13 s. All five focused input/lifecycle/fairness controls and
773 affected checks now pass (3.68 s), with a warning-free 1m 32s default
test build. Both independent reviews pass. The warning-free ordinary
default-feature build completes in 1m 04s and is frozen as
`./.tmp/reflection/bin/latest-credit-20260909/mptunnel`; both candidate
endpoints use that executable, with diagnostics off. The old final-only fold
is removed. This state-model candidate is preserved as checkpoint `83734b2`,
explicitly unaccepted. All four initial candidate ordinary measurements and
the unchanged reverse-order pair are complete below.

The forecast is fewer obsolete merged-queue entries and
actor preparation turns, with possible upstream backpressure relief. No
precise Mbps or wall-time gain is promised: ACK work, physical queuing,
Native service and other actor work remain, and gain may be absent or adverse.
Candidate results must preserve first response, full service, gaps, loaded
latency and costs; a favorable average alone cannot authorize promotion.

## Comparator identity and unchanged healthy profile

All four cells use ordinary `fcc0b22` behavior with diagnostics off. The
retained UP cells are the prior report's **candidate** cohort, now explicitly
the **comparators** for the new representation. Their frozen executable is
`./.tmp/reflection/bin/ready-credit-20260908/mptunnel`. Both endpoints use the
corresponding ordinary runtime; none of the attribution binaries supplies an
ordinary result here.

Result directories under `./.tmp/reflection/results/` are:

- `tcp-combined-up-credit-ready-healthy-candidate-0908/`
- `quic-combined-up-credit-ready-healthy-candidate-0908/`
- `mixed-combined-up-credit-ready-healthy-candidate-0908/`
- `mixed-combined-down-credit-state-healthy-control-0908/`

Each contains `probe.json`, `probe.err`, `service.jsonl`, `client.log`
and `server.log`. All four probe error files are empty. Run records are
`./.tmp/reflection/credit-ready-healthy-candidate-0908-run.log` and
`./.tmp/reflection/credit-state-healthy-control-0908-run.log`.
UP ran TCP → QUIC → mixed before the fresh DOWN cell. Candidate TCP → QUIC
→ mixed UP, then mixed DOWN followed; this is not randomized or simultaneous
execution.

All 167 comparator shaping/management samples retain `mirrored_impairment=true`,
500 Mbps HTB rate/ceil in both directions (62,500,000 B/s), zero jitter,
no random-loss clause and no UDP blackhole. HTB burst/cburst is 65,536 and
netem limit is 8,192. Epoch numbers change every 5 s but do not introduce a
15–25 s capacity cut or 30–33 s outage in these cells.

Physical directions remain router eth0 = server→client, **30 ms**, and
eth1 = client→server, **70 ms**. Thus UP data traverses 70 ms and DOWN data
30 ms, with the opposite delay on feedback. DOWN did not silently swap back
to an unmirrored 70 ms data leg. All share a 100 ms configured round trip
and the same physical cut, not four independent 500 Mbps links. Sampled
client membership is three TCP carriers for TCP, one QUIC for QUIC, and
three TCP plus one QUIC for mixed. Session membership does not prove an
individual echo's current output eligibility or placement.

## Full useful completion and timing

Times are seconds; rates are decimal Mbps. UP is target-sink-ACK-confirmed
upload, with 40 s source load followed by settlement. In every UP cell,
accepted = confirmed = final bytes, `complete=true`, 1/1 stream completed,
0 failures and valid exact ACK accounting. No loaded echo runs in this UP
workload: latency is **not measured**, not zero. Maximum confirmation gap
is separate from time to the first confirmation; it need not include startup.

| Comparator | Exact useful B | Probe s | Whole Mbps | First useful s | Max useful gap s | First / max local-write gap s | Completion |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| TCP UP | 2,191,917,056 | 41.546112 | 422.069 | 0.409158 | 0.409046 | 0.105763 / 0.515635 | Exact complete |
| QUIC UP | 2,283,732,992 | 41.467653 | 440.581 | 1.112591 | 0.307224 | 0.105876 / 0.703102 | Exact complete |
| Mixed UP | 1,670,447,104 | 41.009286 | 325.867 | 0.412460 | 1.192361 | 0.106449 / 0.434806 | Exact complete |
| Mixed DOWN | 2,064,426,284 | 40.001220 | 412.872664 | 0.580507 | 0.265968 | Not measured | Duration-partial success |

UP time beyond the 40 s load is 1.546112 / 1.467653 / 1.009286 s
(TCP / QUIC / mixed), not a measured age of every remaining byte.
Runner times are 42.005410 / 42.005943 / 42.005851 s for UP and
41.062895 s for DOWN, all exit 0 with no settlement guard.

DOWN reads one HTTP 200 response with declared length 8,589,934,592 B
for 40 s: one partial request, zero full response completions, bulk status
`ok`. Its 2,064,426,284 B are useful body bytes actually read, not full
8 GiB completion or a failed transfer. The longest body gap spans
16.791663 → 17.057631 s, advancing the probe byte count
833,334,956 → 833,346,956 after 0.265968 s.
There is no configured failure/recovery epoch.

A fixed full-bin orientation, without discarding startup or tail:

| Raw bin window [s,s) | TCP UP Mbps | QUIC UP Mbps | Mixed UP Mbps | Mixed DOWN Mbps |
| --- | ---: | ---: | ---: | ---: |
| 0–5 | 345.505 | 354.963 | 210.480 | 272.655 |
| 5–15 | 419.588 | 447.561 | 318.558 | 451.130 |
| 15–25 | 447.165 | 450.627 | 343.765 | 421.542 |
| 25–35 | 423.363 | 455.235 | 291.587 | 442.502 |
| 35–40 | 441.556 | 449.583 | 320.672 | 400.040 |

Mixed UP is slower throughout these body windows, not just at startup.
Its late bins include zero at 38 s and 921.898 Mbps at 40 s. Buffered
confirmation bursts can exceed the physical link rate; this does not prove
extra wire capacity. DOWN bins likewise count application reads and can
burst above 500 Mbps from previously buffered bytes. Neither an individual
zero/burst bin nor a trimmed mean attributes the current deficit.

## Loaded echo: DOWN comparator

The concurrent echo uses 64 B request/reply, nominal 500 ms interval and
3,000 ms timeout. All 79 actual attempts succeed, with zero failed,
timed-out or unavailable-after-disconnect entries; request and response each
total 5,056 B. UP has no corresponding echo measurement.

| Outcome | p50 s | p95 s | Maximum s | Maximum successful-response spacing s |
| --- | ---: | ---: | ---: | ---: |
| Mixed DOWN: 79/79 successes | 0.362451 | 0.576166 | 0.667799 | 0.752901 |

The slowest echo is index 48, 24.347151 → 25.014950 s. This occurs during
unchanged healthy shaping, not a 25 s QoS transition. The bulk worst gap is
earlier, so the two maxima are not one joined event. Echo source/output
membership and exact critical-byte ownership are not observed; high bulk
goodput does not establish latency isolation, nor identify the cause of
these echo delays. The last echo finishes at 40.100375 s.

All 79 measured echo latencies in attempt order, milliseconds rounded to
0.001 ms; complete start/end timestamps and original precision remain in
the DOWN `probe.json`:

```text
[
 102.430, 100.865, 291.598, 363.936, 309.354, 174.298, 108.184,
 123.943, 303.646, 332.499, 288.668, 251.466, 336.065, 435.449,
 257.178, 221.169, 313.187, 380.039, 386.265, 372.565, 388.498,
 395.495, 275.196, 352.498, 464.139, 412.506, 568.408, 576.166,
 512.235, 454.301, 490.784, 472.006, 473.533, 396.379, 310.798,
 318.405, 323.365, 396.239, 399.936, 416.271, 415.032, 384.665,
 446.575, 309.513, 394.514, 335.831, 588.636, 595.425, 667.799,
 534.562, 360.244, 355.364, 463.608, 338.060, 161.457, 320.627,
 346.729, 356.488, 357.188, 343.193, 381.417, 334.639, 357.709,
 339.936, 356.384, 184.820, 327.959, 382.356, 460.012, 483.970,
 479.249, 522.623, 511.160, 491.413, 509.222, 636.475, 208.624,
 325.690, 362.451
]
```

## Sampled source, target and reply phases

For UP, S is client relay source-read bytes, T server target-written bytes,
Rs server target-reply-read bytes and Rc client-delivered reply bytes. They
are not claimed C, mux F or an assumed exact response frontier. Client/server
management clocks are independently generated; L denotes each cell's own
`service.jsonl`.

| Mixed UP row | Elapsed s | Client / server Unix ms | S / T B | Rs / Rc B |
| --- | ---: | --- | ---: | ---: |
| L11 | 10.002096 | 1788880940592 / 1788880940592 | 399,133,146 / 395,308,058 | 652 / 554 |
| L21 | 20.003259 | 1788880950592 / 1788880950592 | 841,067,322 / 824,937,146 | 1,324 / 1,198 |
| L31 | 30.004460 | 1788880960592 / 1788880960592 | 1,249,684,282 / 1,246,461,946 | 2,011 / 1,801 |
| L39 | 38.005352 | 1788880968592 / 1788880968592 | 1,569,152,906 / 1,565,136,490 | 2,581 / 2,401 |
| L41 | 40.005572 | 1788880970592 / 1788880970592 | 1,652,171,322 / 1,649,139,130 | 2,731 / 2,536 |
| L42 | 41.005679 | 1788880971592 / 1788880971592 | 1,670,447,104 / 1,670,447,104 | 2,790 / 2,686 |

T/Rs/Rc progress through the sampled late seconds despite the zero
confirmation bin. The final sampled T reaches the exact upload total while
Rc trails Rs by 104 B; the subsequent exact probe settles. No exact
1.192361 s gap-to-stage join is available from these ordinary snapshots.
At the roughly 10 s UP rows, TCP and QUIC T are 509,673,101 and
520,321,401 B, versus mixed 395,308,058 B. The mixed shortfall reaches
target service, not merely probe accounting.

DOWN uses the opposite data direction. The following are aggregate reliable
I/O counters: server `to_peer` source and client `from_peer` delivery include
bulk protocol data and echo, not just the probe's HTTP body.

| Mixed DOWN row | Elapsed s | Client / server Unix ms | Server source / client delivery B |
| --- | ---: | --- | ---: |
| L11 | 10.002266 | 1788883177486 / 1788883177487 | 519,050,540 / 456,476,204 |
| L18 | 17.003012 | 1788883184487 / 1788883184488 | 900,446,204 / 833,337,276 |
| L21 | 20.003815 | 1788883187486 / 1788883187487 | 1,063,240,316 / 998,807,292 |
| L31 | 30.005054 | 1788883197486 / 1788883197488 | 1,598,944,892 / 1,535,047,292 |
| L41 | 40.062727 | 1788883207486 / 1788883207490 | 2,128,978,620 / 2,063,233,948 |

Both DOWN counters advance at every sampled second after startup; they do
not demonstrate a multi-second delivery freeze. Their difference includes
ordinary outstanding/buffered work and independent sampling, not a measured
copy queue or claimed ownership. Last management delivery is not the final
probe body total, and last RSS is not post-load settled ownership.

## Bounded resource and wire context

RSS is KiB. ps %CPU is a sampled **process-lifetime average**, not interval
or handler-exclusive CPU; it may exceed 100% across cores. Each side keeps
one PID per cell. Unequal completed work/direction and pre-completion samples
do not establish normalized CPU efficiency, critical handler cost or a leak.
A high or final-sample memory maximum alone is not unbounded ownership.

| Comparator | Client / server PID | Client RSS peak / last KiB | Server RSS peak / last KiB | Client ps %CPU max / last | Server ps %CPU max / last |
| --- | --- | ---: | ---: | ---: | ---: |
| TCP UP | 316729 / 322448 | 147,352 / 124,464 | 54,872 / 37,140 | 84.8 / 83.9 | 42.5 / 42.5 |
| QUIC UP | 317705 / 323412 | 508,620 / 463,692 | 37,328 / 37,328 | 131 / 130 | 100 / 100 |
| Mixed UP | 318675 / 324375 | 504,332 / 373,004 | 68,736 / 68,736 | 158 / 157 | 100 / 99.3 |
| Mixed DOWN | 321672 / 327360 | 93,184 / 93,184 | 328,120 / 299,460 | 109 / 109 | 191 / 191 |

UP has 42 samples/cell; DOWN 41. DOWN server RSS peaks at L34
(33.005344 s), client at L41; CPU maxima are the listed last values.
Direction changes which endpoint performs bulk sender/receiver work; these
are not exchangeable resource roles.

Router **HTB class first→last deltas**, not parent-plus-child sums:

| Comparator / physical direction | Class delta B | Delta packets | Delta drops | Peak sampled class backlog B |
| --- | ---: | ---: | ---: | ---: |
| TCP UP: upload eth1 | 2,467,945,549 | 1,663,943 | 0 | 17,169,502 |
| TCP UP: return eth0 | 22,379,561 | 244,418 | 0 | 20,043 |
| QUIC UP: upload eth1 | 2,415,593,062 | 1,616,979 | 0 | 12,031,182 |
| QUIC UP: return eth0 | 34,766,092 | 356,667 | 0 | 36,452 |
| Mixed UP: upload eth1 | 1,912,408,549 | 1,313,401 | 0 | 17,991,692 |
| Mixed UP: return eth0 | 57,720,025 | 495,624 | 0 | 83,022 |
| Mixed DOWN: download eth0 | 2,424,268,949 | 1,764,704 | 0 | 37,399,885 |
| Mixed DOWN: upstream eth1 | 125,327,301 | 728,931 | 0 | 508,218 |

Class counters include framing, Native retransmissions, copies and other
protocol traffic; they do not identify an exact duplicate fraction. Traffic
before the first or after the last sample is excluded. Zero sampled class
drops do not imply no buffering. These aggregate backlogs do not locate the
critical byte or prove a per-byte delay bound.

## Complete raw one-second bulk series

Every untrimmed recorded bin is retained in index order, starting at zero:
42 per UP cell and 40 for DOWN, 166 values total. Units are Mbps. UP includes
its final partial settlement interval; DOWN ends at its duration cutoff.
Do not replace these histories with the trimmed average. The full original
probe files also retain exact timing/acknowledgment fields.

TCP UP comparator:

```text
[
 11.007, 364.904, 393.216, 583.533, 374.866, 428.868, 558.367,
 380.109, 408.420, 530.056, 371.195, 238.027, 522.191, 472.383,
 286.261, 621.281, 438.305, 461.374, 435.159, 472.907, 444.597,
 154.665, 567.280, 363.332, 512.754, 449.838, 426.247, 428.868,
 465.043, 394.789, 448.791, 354.418, 366.478, 458.752, 440.402,
 445.645, 443.548, 456.131, 435.159, 427.295, 452.984, 245.894
]
```

QUIC UP comparator:

```text
[
 0.000, 398.508, 463.995, 460.185, 452.128, 471.559, 450.902,
 458.226, 462.474, 470.190, 461.853, 374.706, 420.126, 452.737,
 452.837, 435.831, 457.844, 460.121, 467.258, 465.898, 452.985,
 389.546, 453.221, 458.112, 465.456, 471.045, 455.665, 466.845,
 452.032, 448.421, 464.153, 414.551, 455.897, 460.591, 463.146,
 439.508, 443.964, 459.238, 449.837, 455.370, 469.334, 243.567
]
```

Mixed UP comparator:

```text
[
 11.302, 196.372, 396.154, 282.939, 165.631, 301.850, 270.520,
 259.868, 364.994, 403.280, 263.525, 365.672, 254.988, 413.361,
 287.517, 304.212, 360.141, 368.878, 264.130, 344.819, 423.329,
 376.816, 243.905, 341.924, 409.498, 203.639, 257.137, 325.613,
 255.154, 420.761, 411.857, 88.560, 543.207, 92.939, 317.002,
 728.487, 70.276, 543.354, 0.000, 261.243, 921.898, 246.825
]
```

Mixed DOWN comparator:

```text
[
 2.621, 97.395, 292.124, 635.592, 335.544, 575.004, 312.284,
 554.844, 348.514, 520.218, 435.547, 265.006, 666.501, 449.310,
 384.074, 441.233, 350.866, 527.959, 398.160, 402.346, 447.527,
 433.290, 486.244, 395.621, 332.178, 495.452, 469.103, 358.234,
 475.148, 454.552, 435.587, 455.601, 467.883, 457.370, 356.086,
 432.610, 362.594, 403.380, 370.250, 431.365
]
```

## Completed candidate UP comparison

The three ordinary candidate UP cells have completed, TCP → QUIC → mixed,
using the frozen latest-credit executable described above. Their directories
are `./.tmp/reflection/results/{tcp,quic,mixed}-combined-up-latest-credit-healthy-candidate-0909/`;
the run log is `./.tmp/reflection/latest-credit-healthy-candidate-0909-run.log`.
All 1/1 uploads complete exactly, accepted = confirmed = final bytes, zero
failed streams, valid ACK accounting, empty `probe.err`, runner exit 0 and
no settlement guard. Runner elapsed is 42.010118 / 42.007553 / 42.011669 s.
All 126 candidate shaping samples match the healthy profile and expected
three-TCP / one-QUIC / mixed membership; no loss, jitter, QoS or outage is
introduced. Completed candidate DOWN is recorded separately below.

| Candidate | Exact accepted = confirmed B | Probe s | Whole Mbps | First / max confirmation gap s | First / max local-write gap s | Probe time beyond 40 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP UP | 2,206,793,728 | 41.873866 | 421.608 | 0.408813 / 0.492783 | 0.105602 / 0.432479 | 1.873866 |
| QUIC UP | 2,266,103,808 | 41.396222 | 437.934 | 0.209224 / 0.896869 | 0.106614 / 0.957041 | 1.396222 |
| Mixed UP | 2,155,937,792 | 41.782413 | 412.793 | 0.413820 / 0.697809 | 0.105409 / 0.540541 | 1.782413 |

Mixed whole goodput improves **325.867 → 412.793 Mbps (+26.7%)** while
maximum confirmation gap improves **1.192361 → 0.697809 s**. It confirms
485,490,688 more bytes. This is material useful service, not merely a faster
final ACK. Mixed remains below candidate TCP/QUIC, and its maximum local-write
gap worsens **0.434806 → 0.540541 s**. First confirmation is slightly later
0.412460 → 0.413820 s; settlement duration also increases with more work.
The late confirmation series remains uneven rather than becoming constant.

Single-mode throughput is close: TCP 422.069 → 421.608 Mbps and QUIC
440.581 → 437.934 Mbps. Adverse gaps are retained: TCP maximum confirmation
0.409046 → 0.492783 s; QUIC maximum confirmation 0.307224 → 0.896869 s
and maximum local-write 0.703102 → 0.957041 s. QUIC first confirmation
improves 1.112591 → 0.209224 s, but its first bin contains just 0.096 Mbps.
At candidate L2 (~1 s), T=4,635,467 B and Rs/Rc=44/10; by L3 (~2 s)
T=60,763,819 B and Rs=Rc=108. These observations support an early small
reply followed by bulk-service interpretation. **The probe has no maximum-gap
timestamps, so this does not locate the exact 0.896869 s gap at startup.**

| Raw bin window [s,s) | TCP candidate Mbps | QUIC candidate Mbps | Mixed candidate Mbps |
| --- | ---: | ---: | ---: |
| 0–5 | 341.731 | 351.198 | 293.354 |
| 5–15 | 433.948 | 454.058 | 415.203 |
| 15–25 | 421.114 | 452.408 | 450.435 |
| 25–35 | 434.477 | 443.751 | 388.801 |
| 35–40 | 440.822 | 429.536 | 498.580 |

Mixed improves in every matched full-bin window. The following exact sampled
T increase confirms a forward-service benefit, not only less reply backlog.
All three candidates' T and Rc advance at every sampled second. Ordinary
snapshots still cannot attribute individual maximum gaps to an exact byte,
Native input, actor work or the changed credit representation.

| Mixed candidate UP row | Elapsed s | Client / server Unix ms | S / T B | Rs / Rc B |
| --- | ---: | --- | ---: | ---: |
| L11 | 10.003304 | 1788884491225 / 1788884491224 | 547,083,191 / 482,661,303 | 639 / 639 |
| L21 | 20.006795 | 1788884501225 / 1788884501224 | 1,070,693,687 / 1,006,335,479 | 1,326 / 1,283 |
| L31 | 30.008407 | 1788884511225 / 1788884511224 | 1,600,431,319 / 1,533,322,455 | 2,046 / 2,046 |
| L40 | 39.010196 | 1788884520225 / 1788884520224 | 2,043,614,359 / 1,977,828,215 | 2,691 / 2,691 |
| L41 | 40.010323 | 1788884521225 / 1788884521224 | 2,132,708,247 / 2,070,096,439 | 2,766 / 2,766 |
| L42 | 41.011523 | 1788884522225 / 1788884522224 | 2,155,937,792 / 2,120,037,655 | 2,841 / 2,841 |

At ~10 s, mixed T rises 395,308,058 → 482,661,303 B; at ~30 s,
1,246,461,946 → 1,533,322,455 B. Maximum sampled Rs−Rc falls from the
comparator's persistent reply-lag pattern to 43 B in this cell; these
independent management differences are not exact decoded-byte residence.
The last sampled target total is below the eventual exact final probe total.

Candidate costs have the same definitions and limits as the comparator table:

| Candidate | Client / server PID | Client RSS peak / last KiB | Server RSS peak / last KiB | Client ps %CPU max / last | Server ps %CPU max / last |
| --- | --- | ---: | ---: | ---: | ---: |
| TCP UP | 322616 / 328301 | 159,256 / 144,152 | 59,908 / 32,664 | 88 / 87.4 | 46.6 / 45.9 |
| QUIC UP | 323587 / 329265 | 510,556 / 482,748 | 34,656 / 34,656 | 128 / 128 | 102 / 102 |
| Mixed UP | 324555 / 330229 | 303,088 / 286,888 | 125,820 / 125,820 | 175 / 173 | 95.1 / 95.1 |

Mixed client peak RSS falls 504,332 → 303,088 KiB, but server peak rises
68,736 → 125,820 KiB; client maximum sampled ps CPU rises 158 → 175%.
Neither shift alone is normalized cost, leaked ownership or causal handler
time. More useful work and distinct native/host histories remain relevant.

| Candidate / physical direction | HTB class delta B | Delta packets | Delta drops | Peak sampled class backlog B |
| --- | ---: | ---: | ---: | ---: |
| TCP UP: upload eth1 | 2,464,996,519 | 1,660,983 | 0 | 17,146,330 |
| TCP UP: return eth0 | 22,233,783 | 242,144 | 0 | 20,237 |
| QUIC UP: upload eth1 | 2,397,188,419 | 1,604,739 | 0 | 12,872,304 |
| QUIC UP: return eth0 | 35,260,700 | 362,486 | 0 | 37,468 |
| Mixed UP: upload eth1 | 2,455,380,691 | 1,697,234 | 0 | 34,936,366 |
| Mixed UP: return eth0 | 57,424,171 | 427,786 | 0 | 86,635 |

Mixed's peak sampled upload backlog increases 17,991,692 → 34,936,366 B,
while its return byte delta is close despite more useful work. These
physical class totals are not copy fractions or critical-byte delay bounds.
No load-echo measurements exist in either UP cohort.

All 126 candidate UP raw one-second confirmation bins follow, untrimmed:

TCP UP candidate:

```text
[
 11.532, 378.537, 419.955, 542.638, 355.992, 424.673, 569.377,
 369.099, 379.060, 608.174, 341.311, 188.220, 601.883, 471.335,
 386.348, 500.748, 451.412, 395.838, 470.811, 457.703, 336.069,
 211.288, 534.774, 444.596, 407.896, 497.549, 489.685, 376.963,
 522.715, 437.256, 427.819, 374.342, 326.631, 341.836, 549.978,
 445.645, 439.353, 455.607, 408.945, 454.558, 451.936, 394.266
]
```

QUIC UP candidate:

```text
[
 0.096, 398.362, 456.131, 455.518, 445.881, 463.073, 464.584,
 450.031, 439.597, 415.664, 460.465, 464.314, 456.240, 455.038,
 471.571, 468.005, 398.791, 452.385, 455.837, 470.758, 464.283,
 451.028, 467.569, 446.036, 449.390, 454.955, 425.217, 453.453,
 412.844, 454.705, 454.773, 460.678, 455.178, 406.139, 459.564,
 448.746, 449.242, 467.117, 364.764, 417.813, 459.173, 263.820
]
```

Mixed UP candidate:

```text
[
 11.239, 158.060, 728.760, 427.058, 141.654, 379.969, 686.197,
 324.246, 339.377, 593.677, 423.338, 350.402, 433.254, 242.332,
 379.237, 705.507, 202.183, 349.988, 711.223, 191.985, 479.532,
 674.323, 267.609, 592.777, 329.224, 535.918, 297.020, 311.774,
 690.178, 305.252, 156.217, 246.045, 757.640, 421.720, 166.244,
 720.135, 397.078, 399.603, 258.238, 717.846, 382.841, 360.601
]
```

## Completed candidate DOWN: adverse bulk and mixed echo timing

The mixed DOWN candidate, same ordinary executable and tag, completes its
intended 40 s observation with HTTP 200 / bulk status `ok`: one partial
8 GiB response, no full response completion. Directory:
`./.tmp/reflection/results/mixed-combined-down-latest-credit-healthy-candidate-0909/`;
run log:
`./.tmp/reflection/latest-credit-healthy-down-candidate-0909-run.log`.
Runner exit is 0 / 41.007755 s; `probe.err` is empty. All 41 samples preserve
the comparator's mirrored 30 ms download / 70 ms upstream, 500 Mbps both
directions, no loss/jitter/outage, and three TCP plus one QUIC carriers.

| DOWN measure | Comparator | Candidate |
| --- | ---: | ---: |
| Exact body B | 2,064,426,284 | 1,932,793,085 |
| Bulk probe s | 40.001220 | 40.000402 |
| Whole body Mbps | 412.872664 | 386.554728 |
| First body s | 0.580507 | 0.583560 |
| Maximum body-read gap s | 0.265968 | 0.385547 |
| Echo successes / attempts | 79 / 79 | 78 / 78 |
| Echo failures / disconnect | 0 / none | 0 / none |
| Echo p50 / p95 s | 0.362451 / 0.576166 | 0.305074 / 0.556878 |
| Echo maximum s | 0.667799 | 0.826614 |
| Maximum successful-echo spacing s | 0.752901 | 1.055755 |

Body goodput falls **6.37%** and maximum read gap rises by 0.119580 s.
The candidate gap is 21.967347 → 22.352895 s, with body bytes
1,076,985,361 → 1,076,997,361. Slowest echo index 42 spans
21.716267 → 22.542881 s, overlapping that body gap. Overlap is not a joined
critical-byte cause. Candidate echo request/response each total 4,992 B;
all 78 attempts actually succeed, with no censored or timed-out slots.
The improved median/p95 does not erase the worse echo maximum/spacing.
Different counts follow the existing serial interval/deadline workload;
they are not equivalent to a missing failed attempt.

| Raw bin window [s,s) | DOWN comparator Mbps | DOWN candidate Mbps |
| --- | ---: | ---: |
| 0–5 | 272.655 | 299.958 |
| 5–15 | 451.130 | 426.595 |
| 15–25 | 421.542 | 406.032 |
| 25–35 | 442.502 | 383.995 |
| 35–40 | 400.040 | 359.250 |

The candidate starts somewhat faster by this window average, then serves
less useful data in every listed body window. Its lower whole average is
not just a worse startup or a single final partial bin.

All 40 raw DOWN candidate body bins, Mbps:

```text
[
 2.097, 153.069, 223.891, 768.766, 351.968, 543.975, 284.569,
 516.788, 465.440, 442.474, 392.442, 312.995, 456.292, 410.283,
 440.688, 436.213, 359.850, 400.655, 401.800, 378.585, 441.495,
 431.548, 367.857, 475.529, 366.786, 406.653, 352.880, 368.909,
 338.011, 408.625, 391.073, 400.973, 385.880, 318.825, 468.117,
 367.755, 376.984, 338.247, 312.997, 400.266
]
```

All 78 actual candidate echo latencies, milliseconds rounded to 0.001 ms,
in attempt order; full start/end timestamps remain in `probe.json`:

```text
[
 101.388, 759.331, 275.410, 799.354, 470.264, 142.536, 248.721,
 425.871, 411.328, 395.377, 288.723, 225.581, 295.662, 443.873,
 309.849, 361.363, 418.433, 358.645, 500.294, 324.046, 383.889,
 417.427, 446.358, 315.546, 180.981, 350.796, 393.095, 280.822,
 217.997, 416.713, 556.878, 595.772, 312.689, 364.709, 339.035,
 427.182, 500.101, 305.074, 287.518, 308.950, 276.757, 345.815,
 826.614, 124.851, 270.820, 338.204, 353.534, 335.311, 323.524,
 364.832, 355.337, 339.884, 273.418, 242.349, 175.723, 186.764,
 216.112, 236.422, 243.330, 283.587, 284.269, 325.699, 312.866,
 265.178, 214.973, 153.132, 234.201, 235.269, 242.692, 270.835,
 260.494, 300.632, 292.667, 273.105, 206.620, 194.331, 127.497,
 164.671
]
```

### DOWN service, physical queues and attribution limit

Candidate aggregate server source and client delivery both advance at every
sampled second; these are reliable I/O, not C, mux F or solely HTTP body bytes.

| Candidate DOWN row | Elapsed s | Client / server Unix ms | Server source / client delivery B | Download HTB backlog B |
| --- | ---: | --- | ---: | ---: |
| L11 | 10.001390 | 1788884583864 / 1788884583864 | 528,334,573 / 468,016,817 | 14,032,946 |
| L21 | 20.002532 | 1788884593864 / 1788884593864 | 1,029,207,117 / 965,669,341 | 12,521,364 |
| L23 | 22.002747 | 1788884595864 / 1788884595864 | 1,141,669,045 / 1,076,928,257 | 6,670,378 |
| L24 | 23.002924 | 1788884596864 / 1788884596864 | 1,186,278,285 / 1,121,695,789 | 16,728,838 |
| L31 | 30.003686 | 1788884603867 / 1788884603864 | 1,524,892,621 / 1,461,221,165 | 10,690,074 |
| L41 | 40.007602 | 1788884613864 / 1788884613864 | 1,994,017,741 / 1,930,538,669 | 0 |

At ~10 s, candidate delivery is ahead, 468,016,817 versus 456,476,204 B;
by ~30 s it is behind, 1,461,221,165 versus 1,535,047,292 B. Source
service slows along with delivery rather than accumulating an observed new
multi-second flat client-delivery interval. The snapshots do not resolve
the exact subsecond probe gap.

Server management includes configured listeners **and** actual directional
Native entries. The following use only `native_delivery.direction =
server_to_client`, keyed by stable Native epoch within each cell, not
listener rows or client-to-server feedback. L2 is the first row containing
all four Native epochs; deltas below are L2→L41 (~1→40 s). Endpoint fields
are null, so rows cannot be equated across cells by array order or a supposed
physical ordinal.

| DOWN cell / underlay | Native epoch | ACKed-byte delta L2→L41 |
| --- | --- | ---: |
| Comparator TCP | 9452861877879043269 | 552,628,344 |
| Comparator TCP | 5975724225894100844 | 487,333,896 |
| Comparator TCP | 9996533255083664468 | 224,521,840 |
| Comparator QUIC | 3816496373588445011 | 1,030,063,346 |
| Candidate TCP | 14595351255459235583 | 219,781,860 |
| Candidate TCP | 1367895338732497987 | 637,505,367 |
| Candidate TCP | 5422985630797463689 | 214,111,329 |
| Candidate QUIC | 17083715407202981968 | 1,220,833,191 |

Response Native service shifts toward QUIC and becomes more concentrated on
one TCP entry. Those counters include transport-carried framing/copies and
are **not winning logical Original allocation**. Native snapshots have their
own per-epoch sampling clocks; they are not synchronous with `ss` or probe
events. No exact Original/copy offset or QUIC critical-byte read boundary is
available from this ordinary capture. Subtracting Native or HTB totals from
body bytes cannot manufacture an exact copy fraction.

Independent TCP socket snapshots show server ACKed-byte deltas summed across
three stable connections of 1,269,399,026 → 1,073,303,818 B (L1→L41).
Client aggregate TCP Recv-Q peaks at 237,472 → 26,064 B; candidate L22–L24
near the body gap all have Recv-Q zero. Server aggregate TCP Send-Q peaks
at 22,546,094 → 28,818,655 B. Last server socket RTT is roughly 327–358 ms
in the comparator versus 164–165 ms in the candidate. These support distinct
Native/queue histories, but neither prove an input-service improvement nor
rule out unsampled buffered QUIC or actor work.

| DOWN resource | Comparator | Candidate |
| --- | ---: | ---: |
| Client / server PID | 321672 / 327360 | 325520 / 331189 |
| Client RSS peak / last KiB | 93,184 / 93,184 | 89,176 / 76,840 |
| Server RSS peak / last KiB | 328,120 / 299,460 | 363,452 / 355,600 |
| Client ps %CPU max / last | 109 / 109 | 125 / 125 |
| Server ps %CPU max / last | 191 / 191 | 198 / 198 |
| Download HTB delta B | 2,424,268,949 | 2,407,775,510 |
| Download HTB delta packets / drops | 1,764,704 / 0 | 1,763,072 / 0 |
| Upstream HTB delta B | 125,327,301 | 152,740,569 |
| Upstream HTB delta packets / drops | 728,931 / 0 | 859,508 / 0 |
| Download / upstream peak HTB backlog B | 37,399,885 / 508,218 | 22,227,210 / 616,098 |

Nearly equal sampled download class bytes accompany less useful body, while
upstream traffic increases. Different Native allocation, framing/copy work,
service scheduling and run history remain competing explanations; aggregate
CPU cannot select one. The lower sampled client TCP backlog is counterevidence
to a simple sustained TCP-input-queue accumulation explanation, not proof
that all client input service is prompt.

## Eight-cell disposition and next declared comparison

All eight authorized ordinary observations are preserved: six exact complete
UP streams and two duration-partial successful DOWN bodies, with all 332 raw
bulk bins and all 157 actual echo latencies. The forecast is supported for
healthy mixed UP useful throughput and maximum confirmation gap, but **global
non-downgrade is not established**: mixed DOWN bulk/read maximum and worst
echo/spacing regress, with single-mode UP gap caveats also retained.
Neither this result nor component GREEN is performance acceptance.

Existing telemetry establishes a changed response transport-service
distribution, not its causal reason or exact winning-byte allocation. The
predeclared unchanged reverse-order DOWN pair tests recurrence below.

## Predeclared reverse-order DOWN pair

The unchanged ordinary candidate ran first, then the retained `fcc0b22`
control. This was the declared order discriminator, not selection of a better
run. Results are in
`./.tmp/reflection/results/mixed-combined-down-latest-credit-healthy-reverse-{candidate,control}-0909/`,
with the corresponding
`./.tmp/reflection/latest-credit-healthy-reverse-{candidate,control}-0909-run.log`.
Both runners exit 0 (41.031199 / 41.006723 s); both probe error files are
empty. Each is one successful HTTP 200 duration-partial 8 GiB response, not
a completed 8 GiB body. All 82 service rows independently match the unchanged
healthy profile, including mirrored 30 ms download / 70 ms upstream,
500 Mbps both ways, no jitter/loss/outage and zero sampled class/netem drops.

| Reverse DOWN measure | Candidate, first | Retained control, second |
| --- | ---: | ---: |
| Exact body B | 1,922,405,558 | 2,066,423,504 |
| Bulk probe s | 40.000578 | 40.005745 |
| Whole body Mbps | 384.475560 | 413.225355 |
| First body s | 0.577596 | 0.581713 |
| Maximum body-read gap s | 0.206452 | 0.275026 |
| Echo successes / attempts | 80 / 80 | 76 / 76 |
| Echo failures / disconnect | 0 / none | 0 / none |
| Echo p50 / p95 s | 0.246563 / 0.362593 | 0.345434 / 0.634428 |
| Echo maximum s | 0.662985 | 1.162301 |
| Maximum successful-echo spacing s | 0.765489 | 1.162322 |

The DOWN throughput deficit **recurs at 6.96%** under reversed order.
Candidate rates are 386.555 and 384.476 Mbps; controls 412.873 and
413.225 Mbps. This supports a sustained candidate-associated throughput
effect in the declared healthy DOWN workload, not merely one unfavorable
first-run average. It does not identify the causal implementation boundary.

The earlier adverse maximum read/echo gaps **do not recur**: both maxima
improve in this reverse pair. Candidate's maximum body gap is at
0.577596 → 0.784048 s, bytes 58,192 → 65,536; control's at
0.581713 → 0.856739 s, bytes 58,192 → 123,728. These are explicitly
startup body gaps, unlike the first candidate's 21.967347 s gap.
Slowest candidate echo index 50 spans 25.007757 → 25.670742 s;
control index 66 spans 34.383468 → 35.545769 s. All actual echoes succeed;
request/response bytes are 5,120 each candidate and 4,864 each control.
The complete 156 additional attempt records and their exact start/end times
are preserved in the raw probes and archive, not reduced to successful-only
percentiles with hidden failures.

| Raw bin window [s,s) | Reverse candidate Mbps | Reverse control Mbps |
| --- | ---: | ---: |
| 0–5 | 289.023 | 285.479 |
| 5–15 | 429.475 | 422.099 |
| 15–25 | 414.299 | 442.294 |
| 25–35 | 363.743 | 430.048 |
| 35–40 | 371.687 | 431.896 |

The candidate is initially comparable/ahead, then substantially behind
through the latter body. At ~10 s, aggregate client delivery is
460,682,298 versus control 455,276,456 B; at ~30 s it is 1,463,309,148
versus 1,534,588,416 B. Source and client delivery advance in every sampled
second in both cells. This is reduced sustained service, not a new
multi-second flat delivery span in the available samples. Independent
management clocks and non-body protocol/echo bytes retain their earlier
interpretation limits.

| Reverse DOWN resource | Candidate | Control |
| --- | ---: | ---: |
| Client / server PID | 326471 / 332127 | 327421 / 333066 |
| Client RSS peak / last KiB | 85,696 / 85,696 | 85,272 / 85,272 |
| Server RSS peak / last KiB | 413,964 / 413,964 | 316,908 / 309,556 |
| Client ps %CPU max / last | 131 / 131 | 106 / 106 |
| Server ps %CPU max / last | 200 / 200 | 192 / 192 |
| Download HTB delta B | 2,410,036,958 | 2,432,605,701 |
| Download HTB delta packets / drops | 1,770,252 / 0 | 1,767,428 / 0 |
| Upstream HTB delta B | 163,257,403 | 123,823,504 |
| Upstream HTB delta packets / drops | 884,651 / 0 | 685,265 / 0 |
| Download / upstream peak HTB backlog B | 18,648,598 / 769,107 | 32,309,666 / 480,172 |

The candidate again has higher server RSS and sampled process-lifetime CPU
despite less useful body. This is relevant adverse cost context, not proof
of exclusive input-handler CPU or a memory leak; most peaks are final
pre-cleanup samples. No per-byte causal gain/loss is inferred from these totals.
Both final directional class backlogs are zero. Candidate again carries
similar download class bytes but more upstream bytes for less useful body;
the class counters still cannot distinguish copies from other overhead.
Independent review verifies all reverse probe/echo outcomes, cost fields,
shaping rows and directional class deltas against the raw files.

All 80 additional raw one-second body bins, untrimmed:

Reverse candidate:

```text
[
 3.337, 206.142, 214.242, 698.211, 323.185, 576.289, 251.099,
 600.858, 411.096, 429.639, 424.365, 355.553, 393.147, 451.039,
 401.665, 441.942, 350.142, 486.576, 405.519, 400.933, 409.963,
 396.417, 476.075, 398.779, 376.641, 345.043, 367.483, 332.582,
 410.033, 388.570, 346.779, 352.115, 354.820, 389.824, 350.181,
 376.980, 383.088, 296.984, 429.304, 372.081
]
```

Reverse retained control:

```text
[
 2.597, 112.626, 312.335, 633.370, 366.467, 473.528, 412.170,
 383.827, 479.607, 474.815, 357.361, 261.270, 652.309, 447.585,
 278.520, 401.757, 606.138, 440.487, 422.467, 407.883, 494.553,
 429.242, 403.115, 406.298, 410.999, 469.108, 436.675, 484.865,
 433.164, 396.231, 451.079, 336.218, 501.124, 458.059, 333.959,
 456.807, 361.377, 280.354, 618.714, 442.228
]
```

## Ten-cell disposition and preserved evidence

The ten ordinary observations retain six exact complete UP uploads and four
duration-partial successful DOWN bodies, all **412 raw bulk bins** and
**313 actual echo attempts** (all successful, none censored). The report
preserves the first pair and the contrary reverse latency outcomes instead
of selecting one maximum or average.

Healthy mixed UP useful throughput/confirmation benefits are supported.
The roughly 6–7% sustained DOWN throughput deficit repeats under reversed
order; global non-downgrade and performance promotion remain withheld.
Worst-latency regression is not a consistent result across the two DOWN pairs.
Native distribution, transport/queue realization and local service work
remain causally unresolved. A candidate-introduced cooperation-boundary
ablation is a separately declared question, not a proven diagnosis or a
threshold adjustment; no such result is included here.

[LATEST_CREDIT_SERVICE_20260909.raw.tar.gz](LATEST_CREDIT_SERVICE_20260909.raw.tar.gz)
is self-contained for these ten cells: 50 original result files, 14
build/test/run logs and the exact RED patch, 65 regular-file members total.
It includes both ordinary-build logs and all six cohort run logs. The
uncompressed members total 15,216,751 B; the archive is 1,509,390 B.
Gzip integrity, exact ordered member list and byte-for-byte tar comparison
against every source member pass. No executable or private configuration is
embedded. The earlier diagnostic is separately linked above, not substituted
for any ordinary comparison.
