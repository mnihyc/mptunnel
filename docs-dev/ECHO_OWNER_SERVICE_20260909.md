# Echo ownership and loaded service — 2026-09-09

Status: diagnostic attribution, not performance acceptance or a runtime correction.
The measured response delay is predominantly after successful TCP handoff. This
does not identify socket FIFO, physical network, peer read-task or framing time
individually. The slowest whole echo also contains a separate pre-read interval.

## Question, identity and evidence

The [matched ordinary panel](HIGH_CAPACITY_REFERENCE_20260909.md) exposed mixed
echo p95 501ms versus raw/Xray/H2 126/127/114ms under healthy bulk load.
The declared discriminator asked whether slow echoes wait before response source,
at claim/write, after handoff, or at local delivery, and which attachments are
actually eligible. No speed benefit was forecast for an observer; absence of a
joined critical delay would leave the hypothesis unresolved.

Runtime `4c7e232`, with only the archived 11-file
[ECHO_OWNER_TRACE_20260909.patch](ECHO_OWNER_TRACE_20260909.patch) overlay;
frozen `./.tmp/reflection/bin/echo-owner-20260909/mptunnel`.
The optimized diagnostic build finished in 1m07s without compiler warnings.
Observer source was reversed before the capture; no ordinary policy change.
Result directory: `./.tmp/reflection/results/mixed-combined-down-echo-owner-0909/`.
Build/run logs are `./.tmp/reflection/echo-owner-diagnostic-build-0909.log` and
`./.tmp/reflection/echo-owner-0909-run.log`. Runner exit0, elapsed41.007714771s;
the repeated HTB quantum warnings are retained. Empty `probe.err`.

[Raw archive](ECHO_OWNER_SERVICE_20260909.raw.tar.gz) contains those five result
files, both logs and the exact observer patch (eight regular files).
Archive size 217842B, uncompressed members 2245667B; gzip integrity, exact ordered
member list and byte-for-byte tar comparison against all sources pass.
The raw JSON preserves every original-precision probe field and attempt.
C/S references below mean physical client/server log lines, not equal cross-process
sequence or monotonic clocks. Client PID336908, server342462; echo logical stream0,
session `17369686462201229024`, selected at target port10022 (C1/S1).
The observer records Unix milliseconds; equal stamps do not mean zero work.

## Full outcome, not an ordinary speed comparison

| Metric | This diagnostic | Prior ordinary mixed |
|---|---:|---:|
| Body bytes / seconds | 1926370149 / 40.000825853 | 2016075971 / 40.000305744 |
| Body goodput Mbps | 385.266075 | 403.212112 |
| First body / maximum body-read gap, seconds | .578728 / .245008 | .615795 / .323468 |
| Echo success / attempted | 80 / 80 | 80 / 80 |
| Echo p50 / p95 / maximum, seconds | .290887 / .420549 / .762385 | .334069 / .501201 / .561082 |
| Maximum successive echo-completion gap, seconds | 1.058675 | .714449 |

Both bodies are deliberately duration-partial HTTP200 reads of an 8GiB response:
one request, zero fully completed bodies, one partial; this is not failed settlement.
Diagnostic worst body gap:11.525885505→11.770893583s, bytes529541500→529553500.
All echo outcomes succeed; request and response totals are each 5120B.
The diagnostic/ordinary timing differences do not establish a regression or gain
from an unchanged runtime with added observation and a different realization.

All 40 untrimmed one-second body bins, index0 onward, in Mbps:
```text
2.097, 112.071, 187.508, 716.000, 438.569, 458.834, 430.935, 374.382, 306.079, 526.813
452.314, 230.846, 620.224, 382.202, 390.083, 398.310, 414.147, 322.967, 520.120, 410.156
443.873, 445.476, 325.611, 515.942, 340.222, 368.662, 379.390, 341.918, 296.494, 440.511
398.116, 393.887, 371.992, 329.821, 496.105, 340.234, 394.546, 396.483, 352.982, 343.993
```
Bins above 500Mbps reflect buffered application delivery, not physical link capacity.

## Exact conservation and useful response boundaries

Client source reads: 80 / 5120B. Server positive response reads, source enqueues and
successful Original claims: 81 each / 5120B, gap-free [0,5120), no overlapping
Original ownership. Every enqueue has U-before0, offset=C, U-after=read length;
claims advance C to the exact end and empty that fragment's prepared source.
The second 64B echo is split into 53+11B, explaining 81 rather than80 claims.

All 89 positive TCP writes, 89 authenticated decodes and 89 mux applications reconcile
to 5568B: 5120B Original plus eight late-copy fragments /448B. Every Original wins
its exact range; all copies are nonadvancing. Eight copies represent seven echoed
requests, including the53+11 split. Local delivery makes 80 writes /5120B; no novel
out-of-order suffix or unaccounted winning copy is needed. Server target-write
callbacks are a separate batch/frontier observation, not a response-fragment count.

| Stage, across 81 winning Original fragments | Median / p95 / maximum, ms |
|---|---:|
| Positive source enqueue → actual claim | 0 / 1 / 7 |
| Claim → positive write completion | 0 / 1 / 1 |
| Positive write completion → authenticated decode | 210 / 329 / 461 |
| Authenticated decode → winning mux Apply | 0 / 1 / 2 |
| Winning mux Apply → corresponding local delivery | 0 / 1 / 1 |

Unweighted fragment quantiles use the nearest ordered rank, round((n−1)×q).
Original write-begin and positive flush-completion share a Unix-ms stamp in all 81
cases. These are stage measurements, not a sum of exclusive CPU or a guaranteed
speedup. TCP positive flush is native acceptance, not wire departure. This trace
does not measure native input availability or isolate the receiver's read task.

### Largest post-handoff response: [1408,1472), echo 22

| Boundary | Exact Unix ms / log lines |
|---|---|
| Request successfully written to local target socket | 1788887919978 / S283 |
| Positive response read and enqueue | 1788887919980 / S284–285 |
| Actual Original claim; write begin / positive flush | 1788887919980 / S289, S293–294 |
| Winning wire-TCP0 decode; mux; local delivery | 1788887920441 / C101–103 |
| Losing wire-TCP1 copy write / decode | 1788887920312 / S296; 1788887920521 / C105 |

The 461ms positive-write→decode interval dominates this 533.724838ms whole echo.
The later copy arrives 80ms after the Original. Both TCP candidates were Ready
(S286–292); the selected wire0/physical4 had native queue834476B, flight114392B,
reported carrier limit5792B and SRTT219.999ms; the other wire1/physical2 had
queue0B, flight7106784B, limit10728232B and SRTT303.997ms.
These native fields are not an exact echo service forecast or a socket-buffer cap;
they do not prove that choosing the other output would have won.

### Slowest whole echo: [64,128), attempt 1

C6 reads 64B at1788887909143. S12 reports successful target-socket write through 128
at1788887909213 (+70ms). Positive response reads, enqueues, claims and writes for
[64,117) and [117,128) occur at1788887909493 (S13–34): another 280ms.
The actual read-await measurements are only 4/5µs; the 280ms is not a continuously
polled socket-read wait and does not prove response bytes were already available.
The target-write event is not Python target receipt.

Both Original fragments decode/mux/deliver at1788887909905 (C7–11),412ms after
write completion; postdecode adds no full millisecond. Losing wire1 copies
(S36/S38 at1788887909716) decode/apply at1788887909932 (C13–16),27ms too late.
Thus 70+280+412ms brackets the probe's762.385400ms attempt, with millisecond
quantization; neither the entire 762ms nor its 280ms pre-read portion is removable
by a response placement change. Contrary cases also retain substantial request-side
service: attempt 27 has 339ms request-read→target-write within 504.858ms overall;
attempt 66 has 380ms within 449.632ms. Response-only changes cannot remove those intervals.

## Actual eligible membership and rejected explanation

The first echo has one accepted TCP candidate; later claims have two:
server wire1/physical2/attachment1 maps to client runtime index0/physical3/
attachment0, and server wire0/physical4/attachment2 maps to client runtime
index2/physical4/attachment1. These namespaces must not be numerically equated.
All 322 initial/committed candidate rows are TCP, Active, nonstale, admission-active,
Available, nonbackup and Latency. Final snapshots have 55 both-Ready and 26
one-Ready cases; no QUIC candidate or QUIC echo write is recorded.
The session's single QUIC physical1, native epoch `15040490905496125359`, remains
stable in all41 service samples. Session membership is not echo attachment eligibility.

The proposed permanent two-TCP attachment-count dead end is disproved by source:
`lifecycle.rs`'s count<=1 predicate governs first-stall alternate attachment.
The persistent-product-stall branch in `control.rs` opens recovery before the
>1 preserve-set branch, and receive-hole recovery opens independently of that count.
This does not prove that either trigger occurred for this echo, or that a QUIC
attachment would improve it. Current observations select the adaptation/eligibility
contract for review, not a new threshold, preferred protocol or controller change.

## Shape and resource context

All 41 samples, elapsed0.000194312→40.007569679s, retain mirrored healthy shaping:
500Mbps HTB rate/ceil each way, burst/cburst65536B; router eth0 download30ms,
eth1 upstream70ms, zero jitter, no random-loss clause, netem limit8192 and no
UDP blackhole. All sampled HTB/netem drops are zero. One physical QUIC connection
plus three TCP carriers share the cut; only the two exact TCP attachments above
are observed in this echo's response candidate set.

| Sampled cost | Client | Server |
|---|---:|---:|
| RSS peak / last, KiB | 87708 / 87708 | 361324 / 345732 |
| Process-lifetime ps %CPU maximum / last | 130 / 130 | 205 / 205 |

Client RSS peaks at serviceL41, server atL32. These are diagnostic process totals,
not exclusive echo CPU, normalized costs or evidence of a memory leak.
HTB class first→last deltas: download2412636715B /1789060packets; upstream
151605044B /881186packets; both zero drops. Maximum class backlogs are18814839B
download (L22) and794993B upstream (L34); final backlogs both zero.
Class totals include bulk, control, framing and native transport traffic, not
unique echo bytes. Parent HTB and child netem accounting must not be added.
Network/native queues remain competing causes; aggregate occupancy supplies no
exact missing-byte position or per-byte delay. The matched ordinary panel's
lower-latency, higher-throughput baselines preclude declaring this penalty an
inevitable consequence of the shared 500Mbps profile.

## All 80 actual echo attempts

All succeed. Tuples are [index,start_s,end_s,latency_ms], rounded to 6 decimals;
the archive preserves exact JSON precision and outcomes. Sequential pacing means
starts may follow the previous completion instead of the nominal500ms grid.
```json
[
[0,0.102962,0.204093,101.131337], [1,0.500383,1.262769,762.385400], [2,1.262798,1.364769,101.970902], [3,1.762880,2.183429,420.548782],
[4,2.262946,2.561841,298.894760], [5,2.763030,2.900570,137.540211], [6,3.263117,3.364320,101.202658], [7,3.763203,4.142936,379.733401],
[8,4.263270,4.562993,299.722685], [9,4.763356,5.029976,266.619914], [10,5.263558,5.503590,240.031955], [11,5.763630,6.045549,281.919316],
[12,6.263875,6.487403,223.528065], [13,6.763953,7.087919,323.965668], [14,7.264056,7.633385,369.328900], [15,7.764136,8.114110,349.974871],
[16,8.264211,8.582620,318.409379], [17,8.764319,8.971189,206.870023], [18,9.264385,9.548000,283.614806], [19,9.764437,10.023741,259.304674],
[20,10.264519,10.593053,328.533639], [21,10.764597,11.068110,303.512410], [22,11.264700,11.798425,533.724838], [23,11.798452,12.071754,273.301447],
[24,12.298541,12.495155,196.614336], [25,12.798701,13.140592,341.890167], [26,13.298753,13.698451,399.698302], [27,13.798823,14.303682,504.858156],
[28,14.303697,14.513135,209.438490], [29,14.807750,15.053027,245.277320], [30,15.307818,15.664747,356.928436], [31,15.807979,16.196587,388.607599],
[32,16.308066,16.652638,344.572694], [33,16.808156,17.105846,297.690280], [34,17.308236,17.563025,254.789422], [35,17.808313,18.035843,227.529682],
[36,18.308411,18.572626,264.215146], [37,18.808497,19.056609,248.111510], [38,19.308563,19.565764,257.200630], [39,19.808638,20.121938,313.300844],
[40,20.308903,20.610313,301.409775], [41,20.809009,21.086058,277.048521], [42,21.309078,21.646735,337.656867], [43,21.809220,22.213846,404.625935],
[44,22.309319,22.655994,346.675137], [45,22.809407,22.970382,160.974936], [46,23.309493,23.506181,196.688485], [47,23.809521,24.129181,319.659708],
[48,24.309601,24.668675,359.073869], [49,24.809681,25.132417,322.735908], [50,25.310835,25.604757,293.921783], [51,25.810914,26.041701,230.787103],
[52,26.310996,26.559430,248.434611], [53,26.811131,27.091365,280.233683], [54,27.311377,27.604159,292.781857], [55,27.811889,28.078971,267.082306],
[56,28.312073,28.435499,123.425836], [57,28.812145,28.977201,165.055713], [58,29.312215,29.505379,193.163632], [59,29.812300,30.033411,221.110078],
[60,30.312360,30.557417,245.056567], [61,30.813129,31.104016,290.887315], [62,31.313206,31.629879,316.673587], [63,31.813283,32.103435,290.151999],
[64,32.313511,32.606576,293.064998], [65,32.813606,33.079590,265.983877], [66,33.314642,33.764275,449.632164], [67,33.815505,33.967081,151.576055],
[68,34.315596,34.564231,248.635170], [69,34.815687,35.080696,265.008621], [70,35.315780,35.604200,288.420037], [71,35.815851,36.141469,325.618745],
[72,36.316359,36.626361,310.001631], [73,36.816600,37.113115,296.515401], [74,37.316792,37.646670,329.877085], [75,37.816867,38.156781,339.913251],
[76,38.316953,38.611633,294.680388], [77,38.817072,39.196242,379.170573], [78,39.317458,39.457055,139.596642], [79,39.817589,40.006961,189.371475]
]
```

Disposition: the useful response post-handoff delay is measured; a long persistent
prepared-source or postdecode queue is not its dominant owner in this capture.
A separate slow pre-read interval is retained. No wire/network-only attribution,
QUIC-eligibility assumption, runtime fix, practical promotion or new run follows
from these observations alone.
