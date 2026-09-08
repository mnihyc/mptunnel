# Ready-credit service: ordinary UP comparison

2026-09-08. **All six healthy ordinary cells complete; performance is not accepted.**
Mixed confirmation goodput rises 204.707 → 325.867 Mbps (about 59%), with
improvement through the body, not just its buffered tail. Its maximum
confirmation gap nevertheless worsens 0.769314 → 1.192361 s, and it remains
below TCP/QUIC. Single-mode goodput is nearly unchanged. This is useful but
mixed practical evidence, not a complete receive-service or redesign cure.
The separate harsh mixed UP comparison also improves bulk but worsens both
maximum gaps. Healthy and harsh questions must not be conflated.

## Question and bounded change

The [mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) and
[current transaction](CURRENT_CLOSURE_PLAN.md) govern acceptance. Diagnostic
checkpoint `92f3860`,
[response-return evidence](RESPONSE_RETURN_SERVICE_20260908.md), separately
proves a useful TCP Original was decoded before the entire 943 ms F325 hold,
yet a later QUIC repair won. It also records 15,998 STREAM_MAX_DATA
predecessors / 12.405800 s summed handoff waits, versus 6,138 STREAM_ACK /
3.276955 s. Those are asynchronous downstream waits, not exclusive CPU
or an additive time-saving budget. That harsh diagnostic is not an ordinary
healthy comparator, and its largest 6.510 s confirmation gap was predominantly
before the next reply was produced.

The MAX-only fold preserves immediate full-window credit publication:
replace only a finite, already-ready, consecutive same-stream MAX_DATA run by
its greatest grant, retaining that frame's exact origin and every intervening
non-credit boundary. It must not delay an isolated credit, fold ACKs, cross
lifecycle/stream boundaries, invent credit or change queue/window parameters.
The pre-run forecast was fewer redundant actor-input/preparation turns where
such runs actually exist. Material goodput benefit was unknown, including
zero or regression; there was no promise to recover the whole mixed deficit.
Mechanism RED/GREEN and the declared ordinary comparison remain
separate gates. No candidate performance acceptance is asserted here.

Focused execution now reaches both intended RED assertions: three input
turns versus the proposed one, after semantic premises pass; the other two
focused controls pass. With only the RemoteInput already-ready MAX fold,
all four focused checks and all 772 affected checks pass (3.57 s), and the
independent runtime review passes. Production changes are confined to
`attachment.rs`, with tests in its existing attachment test file; RFC 8.4's
maximum-credit semantics remain unchanged. The warning-free ordinary release
build completes in 1m 20s. Candidate runtime is `0449b9f` plus this reviewed
MAX-only RemoteInput fold, frozen as
`./.tmp/reflection/bin/ready-credit-20260908/mptunnel`, with no diagnostics.
Component GREEN does not establish that credit runs explain the entire mixed deficit.

## Ordinary identity, profile and completion

Control endpoints use ordinary runtime `0449b9f`, frozen as
`./.tmp/reflection/bin/response-claim-20260908/mptunnel`; diagnostics are off.
Both cohorts run TCP → QUIC → mixed, controls first. Tags are
`credit-ready-healthy-control-0908` and `credit-ready-healthy-candidate-0908`;
both endpoints use their respective cohort binary.
The existing `combined` upload probe offers one stream for 40 s, then drains
to its existing completion deadline. No concurrent loaded-echo probe runs in
this upload workload; its latency outcome is **not measured**, not zero.

All 252 management/shaping rows show mirrored routed UP, shared upload and
return HTB rate/ceil 62,500,000 B/s = 500 Mbps, 70 ms upload + 30 ms return
delay, zero jitter, no random-loss clause and no UDP blackhole. Epoch labels
continue every 5 s, but QoS/loss/outage changes are disabled; there is no
15–25 s capacity cut or 30–33 s outage in this cohort. Netem limit 8,192 and
HTB burst/cburst 65,536 remain unchanged. TCP has three active TCP carriers;
QUIC one active QUIC carrier; mixed three TCP plus one QUIC in every sampled
client membership. This is a shared cut, not four independent 500 Mbps links.

Every probe reports `complete=true`, 1/1 completed stream, 0 failed streams,
`status=ok`, exit 0 and exact valid target-sink-ACK accounting. Confirmed,
locally accepted and final byte counts are equal in each cell; all six
`probe.err` files are empty. Control runner elapsed times are 42.004505 /
42.010936 / 42.010030 s; candidate 42.005410 / 42.005943 / 42.005851 s
(TCP / QUIC / mixed), all exit 0, with no settlement guard.
QUIC-bearing server logs contain H3_NO_ERROR connection-close warnings around
end-of-run cleanup; these are not independent failed-upload evidence.

All timing columns are seconds. “After load” is probe elapsed minus 40 s,
not an exactly observed per-byte drain age.

| Cell | Exact accepted = confirmed B | Probe s | Confirmed Mbps | First / max confirmation gap s | First / max local-write gap s | After load s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP control | 2,214,330,368 | 41.524227 | 426.610 | 0.409116 / 0.435956 | 0.105409 / 0.385813 | 1.524227 |
| TCP candidate | 2,191,917,056 | 41.546112 | 422.069 | 0.409158 / 0.409046 | 0.105763 / 0.515635 | 1.546112 |
| QUIC control | 2,284,257,280 | 41.456239 | 440.804 | 1.111888 / 0.319541 | 0.105666 / 0.740514 | 1.456239 |
| QUIC candidate | 2,283,732,992 | 41.467653 | 440.581 | 1.112591 / 0.307224 | 0.105876 / 0.703102 | 1.467653 |
| Mixed control | 1,070,858,240 | 41.849305 | 204.707 | 0.411364 / 0.769314 | 0.106087 / 0.512849 | 1.849305 |
| Mixed candidate | 1,670,447,104 | 41.009286 | 325.867 | 0.412460 / 1.192361 | 0.106449 / 0.434806 | 1.009286 |

The 42-bin histories below retain startup, body and final partial interval.
For orientation only, means of the same fixed full-bin windows are:

| Bin window [s,s) | TCP control | TCP candidate | QUIC control | QUIC candidate | Mixed control | Mixed candidate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0-5 | 358.823 | 345.505 | 355.105 | 354.963 | 117.602 | 210.480 |
| 5-15 | 429.392 | 419.588 | 451.866 | 447.561 | 190.712 | 318.558 |
| 15-25 | 437.151 | 447.165 | 453.008 | 450.627 | 203.271 | 343.765 |
| 25-35 | 436.575 | 423.363 | 449.250 | 455.235 | 215.829 | 291.587 |
| 35-40 | 423.205 | 441.556 | 452.264 | 449.583 | 225.286 | 320.672 |

Mixed remains below both single modes through the body, despite improving
in every listed phase window. It confirms 599,588,864 more bytes than its
control, while single-mode byte counts stay close. Its faster first confirmation
than QUIC does not erase the remaining sustained deficit. Mixed's local-write
maximum improves 0.512849 → 0.434806 s, while its confirmation maximum worsens;
TCP's local-write maximum also worsens 0.385813 → 0.515635 s. These adverse
boundaries are retained, not averaged away. Single-mode
controls establish that this configured high-capacity path can support over
400 Mbps in these cells, not that all differences are causally due to MAX_DATA
processing. Execution order, carrier mix, distinct work volumes and host/native
histories are not randomized or packet-identical controls.

## Source, target and reply observations

S = source bytes locally read by the client relay; T = upload bytes written
by the server relay to the target; Rs = target reply bytes read by the server;
Rc = reply bytes delivered locally by the client. S is not claimed C, T is
not mux F, and Rc is not an assumed exact response mux frontier. Client and
server management stamps are independently generated, not an atomic snapshot.
Each cell has 42 rows; L refers to its own `service.jsonl`.

| Control / row | Runner elapsed s | Client / server Unix ms | S / T B | Rs / Rc B |
| --- | ---: | --- | ---: | ---: |
| TCP L11 | 10.001060 | 1788880128911 / 1788880128912 | 553,713,611 / 487,260,107 | 652 / 652 |
| TCP L21 | 20.002214 | 1788880138911 / 1788880138912 | 1,119,616,971 / 1,053,753,291 | 1,314 / 1,314 |
| TCP L31 | 30.003275 | 1788880148912 / 1788880148912 | 1,649,344,459 / 1,582,628,811 | 2,049 / 2,049 |
| TCP L42 | 41.004371 | 1788880159911 / 1788880159912 | 2,214,330,368 / 2,184,052,683 | 2,859 / 2,859 |
| QUIC L11 | 10.002481 | 1788880173145 / 1788880173144 | 574,423,862 / 509,018,934 | 664 / 664 |
| QUIC L21 | 20.003617 | 1788880183145 / 1788880183144 | 1,147,076,842 / 1,081,719,914 | 1,371 / 1,371 |
| QUIC L31 | 30.005278 | 1788880193145 / 1788880193144 | 1,709,861,566 / 1,644,575,062 | 2,121 / 2,121 |
| QUIC L42 | 41.010799 | 1788880204145 / 1788880204144 | 2,284,257,280 / 2,256,580,694 | 2,946 / 2,946 |
| Mixed L11 | 10.001233 | 1788880217693 / 1788880217688 | 241,430,573 / 239,474,637 | 650 / 426 |
| Mixed L21 | 20.007422 | 1788880227693 / 1788880227688 | 491,788,237 / 489,368,941 | 1,322 / 1,112 |
| Mixed L31 | 30.008482 | 1788880237693 / 1788880237688 | 771,102,669 / 766,301,005 | 1,980 / 1,812 |
| Mixed L42 | 41.009892 | 1788880248693 / 1788880248688 | 1,070,858,240 / 1,065,010,413 | 2,732 / 2,597 |

In the controls, mixed's sampled reply lag is persistent: at L10, server Unix
1788880216688 / client 1788880216693, Rs=580 and Rc=314 (266 B difference).
At L11 the target has already received 239,474,637 B, but Rc is 426 versus
Rs 650. At the comparable 10 s rows, TCP and QUIC have roughly 487 MB and
509 MB target-written, with Rs=Rc. Mixed also has much less S−T separation
after startup; this observation does not establish which source-admission,
claim, native or feedback boundary limits it.

After the initial row, S, T and Rs advance at every sampled second in all
three controls; mixed Rc also advances at every sampled second. Thus these
snapshots show sustained reduced service and reply lag, not a repeated
multi-second flat T plateau. They cannot resolve subsecond critical ranges or
assign the lag to a particular reader, actor, mutex or MAX_DATA run. QUIC's
Rc=0 through its 1 s row is consistent with its 1.111888 s first confirmation.
All control final sampled T values are below the exact final probe bytes:
these are pre-completion observations, not evidence of missing final delivery.

Candidate mixed forward and reply observations:

| Candidate mixed row | Runner elapsed s | Client / server Unix ms | S / T B | Rs / Rc B |
| --- | ---: | --- | ---: | ---: |
| L11 | 10.002096 | 1788880940592 / 1788880940592 | 399,133,146 / 395,308,058 | 652 / 554 |
| L21 | 20.003259 | 1788880950592 / 1788880950592 | 841,067,322 / 824,937,146 | 1,324 / 1,198 |
| L31 | 30.004460 | 1788880960592 / 1788880960592 | 1,249,684,282 / 1,246,461,946 | 2,011 / 1,801 |
| L38 | 37.005247 | 1788880967592 / 1788880967593 | 1,534,270,234 / 1,524,327,194 | 2,521 / 2,326 |
| L39 | 38.005352 | 1788880968592 / 1788880968592 | 1,569,152,906 / 1,565,136,490 | 2,581 / 2,401 |
| L40 | 39.005459 | 1788880969593 / 1788880969592 | 1,615,198,126 / 1,604,557,114 | 2,656 / 2,461 |
| L41 | 40.005572 | 1788880970592 / 1788880970592 | 1,652,171,322 / 1,649,139,130 | 2,731 / 2,536 |
| L42 | 41.005679 | 1788880971592 / 1788880971592 | 1,670,447,104 / 1,670,447,104 | 2,790 / 2,686 |

At the 10 s row, candidate mixed T is 395,308,058 B versus control
239,474,637 B; at 30 s it is 1,246,461,946 versus 766,301,005 B.
The improvement reaches actual target writes, not merely local acceptance
or more prompt probe accounting. Candidate reply lag remains: at L40,
Rs=2656/Rc=2461, and at the final sampled L42 T has reached its exact total
but Rc still trails Rs by 104 B. The final probe subsequently settles exactly.

The late zero confirmation bin 38 and 921.898 Mbps bin 40 coexist with
continued sampled T/Rs/Rc progress at L38–L42. They are not evidence of a
one-second complete forward-service stop. Management snapshots can be cached
and their generated clocks are not the probe's bin boundaries; no exact
1.192361 s gap-to-stage join is available here. Candidate single-mode T/Rs
also keep advancing at each sampled second; the exact forward/reply records
needed to attribute the remaining mixed variance are absent.

## Bounded costs

RSS is KiB. ps %CPU is a sampled **process-lifetime average**, may exceed 100%
across cores, and is not per-interval or handler-exclusive CPU. Each side
retains one PID throughout its cell. Peak/last RSS does not measure post-load
settled ownership; control mixed's peak at the last sample is not a demonstrated
memory leak. Unequal completed work and pre-completion sampling preclude a
causal or normalized cost-efficiency claim from these totals.

| Cell | Client / server PID | Client RSS peak / last KiB | Server RSS peak / last KiB | Client ps %CPU max / last | Server ps %CPU max / last |
| --- | --- | ---: | ---: | ---: | ---: |
| TCP control | 313823 / 319557 | 170,172 / 146,072 | 52,932 / 40,548 | 84.6 / 83.6 | 43.9 / 43.9 |
| TCP candidate | 316729 / 322448 | 147,352 / 124,464 | 54,872 / 37,140 | 84.8 / 83.9 | 42.5 / 42.5 |
| QUIC control | 314791 / 320522 | 512,044 / 484,952 | 36,856 / 36,856 | 136 / 135 | 100 / 100 |
| QUIC candidate | 317705 / 323412 | 508,620 / 463,692 | 37,328 / 37,328 | 131 / 130 | 100 / 100 |
| Mixed control | 315762 / 321480 | 625,192 / 625,192 | 60,072 / 60,072 | 143 / 143 | 62.3 / 62.3 |
| Mixed candidate | 318675 / 324375 | 504,332 / 373,004 | 68,736 / 68,736 | 158 / 157 | 100 / 99.3 |

Mixed client RSS peak falls 625,192 → 504,332 KiB, but maximum sampled
ps CPU rises 143 → 158% client and 62.3 → 100% server. These accompany much
more useful work; they neither prove reduced CPU per byte nor identify the
remaining service owner. The control's last-sample RSS peak is not a leak
that this one candidate cell proves fixed.

Router **HTB class** first→last deltas (L1→L42), not a sum of parent HTB and
child netem counters. eth1 is upload; eth0 return. The independently sampled
class bytes include carrier framing/retransmission/reinjection traffic and
other protocol work; they do not reveal an exact duplicate fraction or
per-byte queue position. Counts omit traffic before the first and after the
last sample. Configured zero random loss does not mean zero queuing; these
class drop deltas happen to be zero in all cells.

| Cell / direction | Class ΔB | Class Δpackets | Class Δdrops | Peak sampled class backlog B |
| --- | ---: | ---: | ---: | ---: |
| TCP control return | 22,352,889 | 244,204 | 0 | 19,515 |
| TCP control upload | 2,481,712,425 | 1,671,761 | 0 | 17,089,062 |
| TCP candidate return | 22,379,561 | 244,418 | 0 | 20,043 |
| TCP candidate upload | 2,467,945,549 | 1,663,943 | 0 | 17,169,502 |
| QUIC control return | 34,016,903 | 348,716 | 0 | 38,582 |
| QUIC control upload | 2,416,137,202 | 1,617,398 | 0 | 11,781,684 |
| QUIC candidate return | 34,766,092 | 356,667 | 0 | 36,452 |
| QUIC candidate upload | 2,415,593,062 | 1,616,979 | 0 | 12,031,182 |
| Mixed control return | 29,970,336 | 245,945 | 0 | 58,877 |
| Mixed control upload | 1,319,500,967 | 911,561 | 0 | 6,110,528 |
| Mixed candidate return | 57,720,025 | 495,624 | 0 | 83,022 |
| Mixed candidate upload | 1,912,408,549 | 1,313,401 | 0 | 17,991,692 |

The mixed candidate's upload class delta rises 1,319,500,967 → 1,912,408,549 B,
return delta 29,970,336 → 57,720,025 B, and peak upload backlog
6,110,528 → 17,991,692 B. More throughput can entail more queueing and protocol
work; these unequal-work, partial-window totals cannot establish a precise
copy/overhead fraction, CPU efficiency or per-byte critical queue delay.

## Full raw confirmation series

Untrimmed `interval_goodput_raw_mbps`; bin i starts at i seconds, nominal width
1 s. The last bin is partial. No startup/tail trimming or zero-bin omission.
Values above 500 Mbps (including candidate mixed 921.898 in bin 40) are
buffered target confirmations, not physical transmission above the link cap.

| Bin | TCP control | TCP candidate | QUIC control | QUIC candidate | Mixed control | Mixed candidate |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 11.534 | 11.007 | 0.000 | 0.000 | 11.007 | 11.302 |
| 1 | 370.672 | 364.904 | 400.319 | 398.508 | 102.620 | 196.372 |
| 2 | 367.002 | 393.216 | 462.090 | 463.995 | 97.658 | 396.154 |
| 3 | 383.246 | 583.533 | 458.752 | 460.185 | 138.080 | 282.939 |
| 4 | 661.660 | 374.866 | 454.366 | 452.128 | 238.647 | 165.631 |
| 5 | 314.049 | 428.868 | 468.259 | 471.559 | 147.657 | 301.850 |
| 6 | 385.352 | 558.367 | 461.409 | 450.902 | 141.225 | 270.520 |
| 7 | 631.242 | 380.109 | 434.057 | 458.226 | 87.571 | 259.868 |
| 8 | 381.158 | 408.420 | 417.673 | 462.474 | 180.436 | 364.994 |
| 9 | 381.682 | 530.056 | 450.452 | 470.190 | 224.395 | 403.280 |
| 10 | 420.479 | 371.195 | 454.181 | 461.853 | 181.751 | 263.525 |
| 11 | 453.508 | 238.027 | 466.066 | 374.706 | 192.163 | 365.672 |
| 12 | 403.178 | 522.191 | 450.443 | 420.126 | 235.228 | 254.988 |
| 13 | 452.985 | 472.383 | 452.031 | 452.737 | 327.045 | 413.361 |
| 14 | 470.285 | 286.261 | 464.091 | 452.837 | 189.652 | 287.517 |
| 15 | 322.962 | 621.281 | 452.365 | 435.831 | 193.987 | 304.212 |
| 16 | 491.258 | 438.305 | 466.388 | 457.844 | 147.325 | 360.141 |
| 17 | 497.025 | 461.374 | 457.407 | 460.121 | 175.540 | 368.878 |
| 18 | 354.943 | 435.159 | 457.603 | 467.258 | 163.489 | 264.130 |
| 19 | 620.757 | 472.907 | 467.530 | 465.898 | 217.956 | 344.819 |
| 20 | 403.177 | 444.597 | 460.804 | 452.985 | 232.356 | 423.329 |
| 21 | 214.957 | 154.665 | 396.546 | 389.546 | 185.694 | 376.816 |
| 22 | 438.305 | 567.280 | 458.147 | 453.221 | 256.901 | 243.905 |
| 23 | 596.641 | 363.332 | 455.917 | 458.112 | 235.361 | 341.924 |
| 24 | 431.489 | 512.754 | 457.372 | 465.456 | 224.100 | 409.498 |
| 25 | 401.605 | 449.838 | 453.802 | 471.045 | 210.166 | 203.639 |
| 26 | 486.014 | 426.247 | 452.904 | 455.665 | 213.895 | 257.137 |
| 27 | 424.674 | 428.868 | 458.420 | 466.845 | 264.573 | 325.613 |
| 28 | 399.508 | 465.043 | 455.267 | 452.032 | 173.015 | 255.154 |
| 29 | 480.772 | 394.789 | 463.803 | 448.421 | 195.713 | 420.761 |
| 30 | 416.809 | 448.791 | 450.012 | 464.153 | 212.582 | 411.857 |
| 31 | 398.458 | 354.418 | 388.554 | 414.551 | 289.200 | 88.560 |
| 32 | 452.986 | 366.478 | 463.234 | 455.897 | 187.710 | 543.207 |
| 33 | 455.606 | 458.752 | 441.325 | 460.591 | 147.050 | 92.939 |
| 34 | 449.315 | 440.402 | 465.176 | 463.146 | 264.390 | 317.002 |
| 35 | 452.985 | 445.645 | 458.043 | 439.508 | 211.472 | 728.487 |
| 36 | 448.266 | 443.548 | 449.583 | 443.964 | 203.114 | 70.276 |
| 37 | 345.505 | 456.131 | 464.828 | 459.238 | 250.765 | 543.354 |
| 38 | 440.402 | 435.159 | 453.168 | 449.837 | 229.394 | 0.000 |
| 39 | 428.867 | 427.295 | 435.698 | 455.370 | 231.684 | 261.243 |
| 40 | 449.839 | 452.984 | 438.113 | 469.334 | 232.230 | 921.898 |
| 41 | 323.487 | 245.894 | 257.862 | 243.567 | 522.068 | 246.825 |

## Harsh mixed UP: separate stress comparison

This is ordinary runtime, but a **diagnostic stress profile**, not a clean-link
throughput target or performance acceptance. The comparator is the retained
ordinary `0449b9f` cell
`mixed-combined-up-response-claim-combined-candidate-0908`, documented in
[response-prepared service](RESPONSE_PREPARED_SERVICE_20260908.md).
It is **not** the 68.360 Mbps response-return diagnostic.
The new cell is `mixed-combined-up-credit-ready-combined-candidate-0908`,
using the same frozen ordinary MAX-fold candidate as the healthy cohort.

The unchanged mirrored upload profile has 500 Mbps / 70±20 ms upload and
500 Mbps / 30±5 ms return, five-second loss epochs
upload [3,8,5,6,10,3,5,8]% and return [1,2,0.5,3,2,0.5,1,2]%,
upload QoS 10 Mbps at 15–25 s and UDP blackhole 30–33 s.
The configured queue limit is still 8,192. Shaping snapshots agree in both
cells, but realized losses, offered/completed work and host/native histories
are not identical. No profile, threshold or controller was changed for this
comparison; no subsequent favorable rerun is included.

Both probes are exact complete (1/1, zero failures, valid target-sink-ACK
accounting; accepted = confirmed), runner exit 0. Candidate runner elapsed
is 46.034378 s. It completes 97,517,568 more bytes in less elapsed time, but
this duration-limited unequal work does not supply a fixed-work speedup.
Its first confirmation/write and both maximum gaps worsen.

| Harsh cell | Exact accepted = confirmed B | Probe s | Confirmed Mbps | First / max confirmation gap s | First / max local-write gap s | After 40 s load |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0449b9f ordinary control | 419,823,616 | 49.378940 | 68.017 | 0.419667 / 5.164575 | 0.119936 / 3.303759 | 9.378940 |
| MAX-fold ordinary candidate | 517,341,184 | 45.643458 | 90.675 | 0.476069 / 6.383313 | 0.128752 / 4.800875 | 5.643458 |

### Decisive sampled phases and limits

The candidate is initially worse despite its higher whole-run total:
at each cell's L5 (~4 s), T is 3,604,215 B versus control 68,908,535 B.
By L16 (~15 s), candidate T is 259,218,827 B versus 205,769,303 B.
Do not erase this early reversal by quoting only the final rate.

Candidate's longest constant T/Rs/Rc interval is L18→L24,
elapsed 17.002032→23.002685 s: T=264,383,535 and Rs=Rc=696.
Server generated Unix ms 1788881105160→1788881111161 spans 6.001 s;
client stamps span 6.000 s. S grows 330,206,368→331,492,399, reaching
exactly T+64 MiB at L20, then remaining flat. The upload HTB is at
10 Mbps and its sampled backlog is 4,186,305→1,089,916 B
(not monotonically decreasing: 4,711,288 B at L20).
This is a real forward/target-service plateau, not evidence that already-read
reply bytes alone explain the long confirmation gap. It does not identify the
blocking range, native packet state or a defect instead of shared-cut effects.
The control's corresponding longest QoS plateau is L19→L23, roughly 18–22 s,
T=212,625,711 and Rs=Rc=532, server stamps spanning 4.000 s.

Candidate outage/recovery L31→L35 (~30–34 s) holds
S=419,996,887 / T=352,888,023 (64 MiB separation), Rs=850.
Server stamps 1788881118160→1788881122161 span 4.001 s.
Rc catches 836→850 at L34 while T remains fixed: subsequent forward waiting
cannot be attributed to that already-delivered reply alone.

A distinct candidate return-service witness remains at L36→L38:
server stamps 1788881123161→1788881125161, T rises
391,548,011→420,091,959 (+28,543,948 B), Rs rises 878→920,
but client Rc stays 878 over stamps 1788881123161→1788881125160
(1.999 s). L39 has Rc948/Rs962. This proves a sampled already-read-reply
delivery lag, not its decode/actor/native owner. The retained control's L42→L46
also held Rc966 for 4.000 s while T rose 65,232,320 B and Rs966→1190.
These unequal intervals are not an attributable two-second gain from the fold.

The final candidate sample L46 at elapsed45.034225 s has
S517,341,184 / T512,389,391 / Rs1200 / Rc1130. Exact probe settlement
follows; this sampled remaining work is not censored failure. Neither maximum
probe gap has an exact winning-byte join in these ordinary logs. The sampled
plateaus must not be relabeled exact mux-F residence or summed as removable time.

### Harsh costs and complete raw bins

Same RSS/ps definitions as above: independent process-lifetime samples, not
exclusive CPU, normalized cost or settled ownership. The comparator has
50 management rows, candidate 46.

| Harsh cell | Client / server PID | Client RSS peak / last KiB | Server RSS peak / last KiB | Client ps %CPU max / last | Server ps %CPU max / last |
| --- | --- | ---: | ---: | ---: | ---: |
| control | 304488 / 310305 | 781,556 / 763,860 | 127,688 / 124,248 | 121 / 98.7 | 44.8 / 20.6 |
| candidate | 319638 / 325337 | 404,776 / 348,044 | 136,968 / 135,872 | 83.1 / 60.2 | 35.8 / 24.3 |

Client RSS/CPU observations improve while server peak RSS increases; different
work/elapsed times and no post-load ownership measurement preclude a leak fix
or exact CPU-efficiency claim. Directional HTB first→last sampled class deltas:

| Harsh cell / direction | Class ΔB | Class Δpackets | Class Δdrops | Peak sampled class backlog B |
| --- | ---: | ---: | ---: | ---: |
| control return | 28,699,235 | 153,551 | 1,912 | 147,715 |
| control upload | 549,867,678 | 466,343 | 8,492 | 4,885,283 |
| candidate return | 31,374,766 | 157,007 | 2,048 | 84,157 |
| candidate upload | 677,175,453 | 558,679 | 10,130 | 9,908,006 |

The differing drop totals are not an implementation-induced loss count or an
exact copy fraction. All raw confirmation bins follow; same nominal one-second
definition and buffered-confirmation caveat as above. “—” means the candidate
probe has ended, not zero throughput or an unavailable in-run observation.

| Bin | Ordinary control Mbps | MAX-fold candidate Mbps |
| ---: | ---: | ---: |
| 0 | 3.144 | 2.214 |
| 1 | 6.000 | 5.124 |
| 2 | 296.513 | 10.487 |
| 3 | 70.018 | 10.486 |
| 4 | 40.038 | 560.987 |
| 5 | 51.425 | 207.811 |
| 6 | 101.623 | 123.112 |
| 7 | 0.000 | 79.071 |
| 8 | 49.327 | 95.516 |
| 9 | 76.310 | 3.574 |
| 10 | 109.812 | 3.766 |
| 11 | 0.000 | 540.017 |
| 12 | 241.409 | 1.495 |
| 13 | 138.464 | 242.063 |
| 14 | 460.690 | 133.504 |
| 15 | 0.000 | 0.000 |
| 16 | 0.000 | 84.501 |
| 17 | 1.477 | 0.000 |
| 18 | 0.000 | 0.000 |
| 19 | 0.000 | 0.000 |
| 20 | 0.000 | 0.000 |
| 21 | 0.000 | 0.000 |
| 22 | 54.851 | 0.000 |
| 23 | 0.000 | 11.438 |
| 24 | 0.000 | 16.445 |
| 25 | 27.598 | 43.944 |
| 26 | 31.694 | 78.839 |
| 27 | 77.065 | 79.404 |
| 28 | 0.000 | 140.513 |
| 29 | 0.000 | 197.217 |
| 30 | 238.587 | 0.000 |
| 31 | 0.000 | 151.579 |
| 32 | 158.589 | 0.000 |
| 33 | 81.888 | 0.000 |
| 34 | 130.195 | 0.332 |
| 35 | 11.202 | 0.000 |
| 36 | 57.576 | 0.000 |
| 37 | 34.079 | 544.071 |
| 38 | 16.585 | 27.307 |
| 39 | 35.511 | 14.584 |
| 40 | 70.779 | 0.000 |
| 41 | 0.000 | 196.106 |
| 42 | 0.000 | 0.000 |
| 43 | 0.000 | 222.564 |
| 44 | 0.000 | 0.000 |
| 45 | 103.539 | 310.661 |
| 46 | 123.678 | — |
| 47 | 117.765 | — |
| 48 | 84.410 | — |
| 49 | 256.748 | — |

**Harsh disposition:** more bulk, earlier settlement and lower client cost
observations coexist with worse startup and maximum confirmation/write gaps,
plus forward QoS/outage plateaus and a remaining return-service lag.
This does not accept performance or justify changing ACK semantics. The
next declared question is exact retained local-service delay on the healthy
high-capacity candidate; no further production change follows from these
ordinary aggregate results alone.

## Evidence and current disposition

Raw source directories:
`./.tmp/reflection/results/{tcp,quic,mixed}-combined-up-credit-ready-healthy-{control,candidate}-0908/`,
each containing `client.log`, `server.log`, `service.jsonl`, `probe.json`
and `probe.err`. Run logs:
`./.tmp/reflection/credit-ready-healthy-{control,candidate}-0908-run.log`.
Mechanism/build logs are `ready-credit-{red-build,red-tests,green-build,green-tests,regressions,ordinary-build}.log`
in `./.tmp/reflection/`. Harsh raw directories are
`./.tmp/reflection/results/mixed-combined-up-{response-claim,credit-ready}-combined-candidate-0908/`;
the new run log is `./.tmp/reflection/credit-ready-combined-candidate-0908-run.log`.
The retained comparator is also preserved in the earlier response-prepared
archive. The [combined raw archive](READY_CREDIT_SERVICE_20260908.raw.tar.gz)
preserves exactly these40 result files plus6 mechanism/build and3 run logs.
All49 members pass gzip integrity, exact listing and byte-for-byte comparison
against their retained originals. The real-producer RED is separately retained
in [the test patch](READY_CREDIT_INPUT_RED_20260908.patch).

Independent read-only verification agrees on both healthy cohorts and the
harsh pair's shape, per-process costs, directional class deltas and selected
source/target/reply rows. Current disposition:
**the finite MAX fold has a material healthy mixed bulk improvement in this
pair, but worse confirmation variance and a remaining single-mode deficit;
no performance acceptance.** The forecast is supported in the limited sense
of useful bulk service, not an exact causal seconds budget or general fix.
Do not select a favorable rerun, attribute every remaining gap to the fold,
or let a harsh-cell rate substitute for healthy/loaded timing. Existing
healthy DOWN and adverse harsh/loaded results remain relevant non-regression
evidence; these eight cells do not waive them.
