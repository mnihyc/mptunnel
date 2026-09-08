# Feedback encoding service — 2026-09-09

Status: diagnostic attribution only. Mixed's retained client snapshot contains
47.375MB of encoded MPP ACK frames versus QUIC's 3.607MB; MAX contributes
7.313MB versus 3.490MB. This identifies a material MPP serialization owner,
not the full physical traffic composition, a removable-byte budget or a speed win.

## Question and capture identity

The [ordinary asymmetric panel](ASYMMETRIC_FEEDBACK_SERVICE_20260909.md) showed
mixed-only collapse and an echo timeout under a 10Mbps UP cut, with DOWN 500Mbps.
The observer separates serialized ACK/MAX/source/control from unknown native traffic.
Small ACK contribution would falsify that owner; large contribution selects its
publication semantics for proof, not an immediate ACK-rate or correctness change.

Runtime `4c7e232`, diagnostic executable
`./.tmp/reflection/bin/feedback-encode-20260909/mptunnel`, with only the two-file
[FEEDBACK_ENCODE_TRACE_20260909.patch](FEEDBACK_ENCODE_TRACE_20260909.patch).
The feature-only optimized build finished warning-free in 1m03s. All 69 observer
additions were reversed before capture; no ordinary runtime policy changed.
The same frozen binary runs first mixed, then QUIC, under tag
`feedback-encode-0909`, with no compiler/lab overlap.

Sources: `./.tmp/reflection/results/{mixed,quic}-combined-down-feedback-encode-0909/`.
Build/run logs: `./.tmp/reflection/feedback-encode-diagnostic-build-0909.log`,
`feedback-encode-0909-run.log` and `feedback-encode-quic-0909-run.log` in that
same reflection directory. [Raw archive](FEEDBACK_ENCODE_SERVICE_20260909.raw.tar.gz)
retains all 10 result files, both run logs, build log and exact patch:14 files.
Archive 247905B, members 3097370B uncompressed; gzip integrity, ordered 14-member
manifest and byte-for-byte tar comparison against all original files pass.
All 80 raw body bins and 151 actual echo attempts, including original-precision
start/end timestamps, remain in the two probe JSONs. Both `probe.err` are empty.
All encoder summaries, not only selected rows below, are archived.

## What the observer does and does not count

A hook after successful frame encoding increments process-wide fixed counters
for wire kind, complete encoded frame length (including MPP framing), maxima,
and ACK range/complete/sparse histograms. Kind 9 is STREAM_ACK; 10 STREAM_MAX_DATA;
8 STREAM_DATA. It retains neither frame contents nor logical stream/path identity.
The existing codec already packs range integers when that is shorter; these
measurements are not estimates assuming 16 fixed bytes per range.

An encoded attempt can belong to a batch later rejected, cancelled or not written.
Native TCP/QUIC ACKs, encryption/transport framing and retransmissions are outside
the hook. Encoder bytes are not native acceptance, transmitted bytes or useful
application bytes. Summaries take a diagnostic mutex and have observation cost.

Reports are event-driven, at least 1s apart, without a final drain report; later
activity is censored, not proved absent. Cumulative snapshots must not be summed.
Each kind has its own timestamp; one report can straddle milliseconds.
Use consecutive SAME-KIND rows, not equal-ms cohorts or another kind's denominator.
Diagnostic monotonic origin is not runner/probe time or the peer's clock.
C/S references identify lines in that cell's client/server log.

## Retained final encoder snapshots

Client PIDs mixed 340653 / QUIC 341605. Final client observations are mixed
1788889356134Unix-ms (diagnostic mono 39395ms; C476–488), QUIC1788889594691
(mono 39027–39028ms; C464–475). These are not exact whole-run terminal totals.

| Client encoded kind | Mixed frames / bytes | QUIC frames / bytes |
|---|---:|---:|
| ACK (9) | 396286 / 47375035 | 134221 / 3606579 |
| MAX (10) | 281279 / 7313254 | 134222 / 3489772 |
| STREAM_DATA (8) | 72 / 6772 | 80 / 7524 |
| All other control kinds, bytes | 2467 | 682 |
| All retained encoded bytes | 54697528 | 7104557 |

Mixed ACK has 6562914 total encoded range entries, mean 16.561 ranges per frame;
QUIC has 134221 entries, exactly one per ACK. Mixed mean ACK length 119.548B
versus 26.870B; maxima 540B /27B. All retained ACKs have `complete=true`.
Mixed has 378020 sparse (>1-range) frames, 95.391%; QUIC has none.

| ACK range-count bin | Mixed frames | QUIC frames |
|---|---:|---:|
| 0–1 | 18266 | 134221 |
| 2–8 | 129640 | 0 |
| 9–32 | 195428 | 0 |
| 33–128 | 52952 | 0 |
| >128 | 0 | 0 |

Maximum observed range count is 87 versus 1. Histograms sum to the respective
ACK frame counts, and all cumulative counters remain monotonic. Mixed's
21-byte fixed ACK overhead contributes 8322006B; its encoded range section
contributes 39053029B. This is an accounting decomposition, not a removable
39MB forecast: those ranges carry real positive and, for complete snapshots,
bounded omission evidence. No per-generation or per-attachment duplication
fraction is measured by this observer.

Server snapshots are separately timed: mixed PID346178 at1788889356214 and QUIC
PID347125 at1788889595200 (both S265–271). STREAM_DATA totals are 116184 frames/
1925435694B and 201705/2184046798B; ACK 3403/1942B, MAX 3900/2210B.
Absent source/copy identity, these cannot reconstruct unique or reinjection bytes.

## Exact per-kind time deltas and opposite interval

All rates here mean encoded-attempt Mbps, not physical throughput. Exact Unix-ms
endpoints and physical client-log lines make the calculation auditable.

| Cell / kind | Lines | Unix-ms start→end | Encoded bytes Δ / elapsed ms | Encoded Mbps |
|---|---|---|---:|---:|
| Mixed ACK, pre-QoS peak | C51→64 | 1788889320758→1788889321758 | 3607981 /1000 | 28.863848 |
| Mixed ACK, constrained interval | C246→259 | 1788889336269→1788889337273 | 2630413 /1004 | 20.959466 |
| Mixed MAX, its own constrained interval | C247→260 | 1788889336269→1788889337273 | 213668 /1004 | 1.702534 |
| Mixed ACK, long next-report interval | C259→272 | 1788889337273→1788889340129 | 351795 /2856 | .985420 |
| QUIC ACK, whole-capture peak | C108→120 | 1788889564674→1788889565674 | 108126 /1000 | .865008 |
| QUIC MAX, corresponding own interval | C109→121 | 1788889564674→1788889565674 | 104130 /1000 | .833040 |

The constrained mixed ACK delta contains 9826 frames and 414084 range entries.
Its service snapshots L20–22 confirm UP 10Mbps at client management Unix
1788889335586,1788889336586,1788889337587. ACK generation alone exceeds that
physical cut during the selected interval, with simultaneous native/control
traffic still unmeasured. This is material offered serialization pressure,
not proof that all encoded bytes crossed the cut during that same interval.
The following 2856ms low-generation interval must not be erased: the capture
does not establish a continuously high ACK generation rate throughout the
whole 2.524s body gap or identify every queued packet causing it.

Complete does not mean contiguous receipt or EOF. RFC8.3 bounds complete
omission evidence by the carried horizon; incomplete ranges cannot extend it.
Current publication offers changed cumulative state to each exact live attachment.
This selects repeated publication and sparse-state size for examination—not ACK
as a MAX scalar, fanout deletion without replacement failure service, incorrect
horizon truncation or a reintroduced relative ACK dictionary.

## Full probe outcome and physical context

Both probes return HTTP200 and intentionally read part of an 8GiB body for 40s:
one partial request, zero complete bodies. Both runners exit0; mixed 41.008786164s,
QUIC 41.006916688s. No echo failures or unavailable-after-disconnect records occur.

| Metric | Mixed diagnostic | QUIC diagnostic |
|---|---:|---:|
| Body bytes / probe seconds | 1722195706 /40.000076826 | 2142625642 /40.000050581 |
| Whole-run Mbps | 344.438480 | 428.524587 |
| First body / maximum read gap, s | .583882 /2.523657 | .409358 /.100773 |
| Echo actual successes / attempts | 71 /71 | 80 /80 |
| Echo p50 /p95 /maximum, ms | 331.341 /937.185 /2871.057 | 105.328 /160.935 /301.450 |
| Max successive echo-completion gap, s | 2.871075 | .688874 |
| Body-bin mean 5–15 /15–25 /25–40, Mbps | 451.224 /157.891 /417.733 | 436.932 /438.794 /442.089 |

Mixed's longest body gap is 21.156323184→23.679979757s, bytes918575762→918605298;
body bin 22 is zero. QUIC's maximum gap is its startup interval
.409358335→.510131624s, bytes 12000→48000. Mixed attempt 37 succeeds at
20.621708768→23.492765791s (2871.057023ms). Its last attempt 70 ends 40.346385841s,
after the nominal body duration. All 151 actual attempts succeed; 71 rather than
80 mixed attempts reflects sequential waiting, not nine dropped requests.
Start-time phase echo counts are mixed 30/11/30 and QUIC 30/20/30 in
0–15/15–25/25–40s. Restored maxima are 462.541/226.410ms; unlike the prior
ordinary timeout, this diagnostic tests restored echo service, without proving
repeatable non-regression or performance acceptance.

Both 41-row service files retain DOWN 500Mbps throughout, UP 500→10→500 at 15–25s;
30ms DOWN/70ms UP, no jitter/configured loss/blackhole, mirrored=true, netem 8192,
HTB 64KiB burst/cburst. The intended DOWN-cap gate remains unexecuted.
Mixed UP records 17941 queue drops, QUIC 0; all DOWN drops are 0.
Near mixed's body stall, L21→24 UP class backlog falls 3556023→1107771B,
while DOWN backlog is 242706→4688B. These are physical queue snapshots, not
the missing byte's location or a per-byte queue/capacity delay estimate.

| Sampled process cost | Mixed client /server | QUIC client /server |
|---|---:|---:|
| RSS peak KiB | 89872 /356924 | 38248 /325260 |
| RSS last KiB | 89872 /345056 | 38248 /325260 |
| ps %CPU maximum | 100 /174 | 99.3 /153 |
| ps %CPU last | 100 /174 | 99 /153 |
| DOWN HTB bytes /packets Δ | 2060256823 /1511917 | 2272601711 /1521160 |
| UP HTB bytes /packets Δ | 110030340 /640097 | 35276462 /326581 |
| Maximum class backlog DOWN /UP, B | 31847374 /3556023 | 7426674 /78546 |

All PIDs remain stable. Samples span 0.000145421→40.008600291s mixed and
0.000054590→40.006759652s QUIC. ps percentages are process-lifetime averages,
not exclusive serialization CPU; RSS is not settled ownership or leak evidence.
HTB deltas use first→last class snapshots, not encoder-report windows; parent
HTB and netem child accounting overlap. Do not subtract the encoder totals from
physical totals to label an exact native-overhead or retransmission remainder.

## Complete retained time series

All 40 untrimmed body bins per mode, Mbps, indices 0–39. Above 500Mbps bins reflect
buffered application delivery, not physical capacity. Full echo start/end/outcome
records remain in the archive; below are every actual attempt's latency in index
order, ms, rounded to 6 decimals (no failures omitted).

Mixed body:
```text
2.620, 118.274, 360.801, 601.843, 336.791, 528.866, 228.724, 626.013, 411.355, 487.143, 426.700, 299.614, 641.438, 461.871, 400.512, 418.352, 209.988, 29.257, 363.211, 194.641
183.207, 17.384, 0.000, 28.946, 133.924, 469.601, 384.756, 417.847, 352.433, 474.334, 335.875, 487.194, 412.759, 365.665, 441.522, 346.927, 487.782, 374.050, 458.242, 457.007
```
QUIC body:
```text
9.553, 381.970, 457.895, 449.263, 453.642, 452.697, 433.733, 422.399, 451.693, 463.669, 444.474, 387.357, 437.087, 439.134, 437.075, 456.968, 432.614, 448.645, 457.577, 379.857
433.351, 450.196, 439.015, 434.399, 455.313, 449.084, 441.582, 432.641, 449.713, 445.051, 440.433, 460.730, 385.844, 432.789, 427.344, 454.313, 460.873, 447.811, 453.845, 449.277
```
Mixed echo latencies, 71 attempts:
```text
100.758574, 100.789544, 296.833256, 484.351802, 283.829811, 235.248621, 131.321125, 173.701826, 247.668733, 324.295679, 306.632392, 242.176546, 300.372849, 315.546650, 265.754881
347.632502, 382.422779, 396.689404, 385.777774, 361.121769, 292.863390, 448.740007, 413.100103, 210.046444, 404.608066, 400.446339, 455.985485, 571.180955, 492.347864, 561.187959
597.095336, 802.764749, 1092.677962, 386.733670, 448.031641, 955.865789, 1033.872814, 2871.057023, 937.184876, 226.149541, 151.807181, 254.092765, 255.342292, 297.594221, 358.950064
421.450186, 462.541288, 401.943320, 331.340524, 290.623237, 262.391040, 138.224060, 237.228041, 273.698314, 234.361786, 244.789155, 284.844386, 357.298314, 365.229556, 327.040336
203.039128, 267.137130, 177.304758, 265.444079, 288.888784, 338.705713, 387.517304, 385.106959, 361.131021, 332.252149, 411.323432
```
QUIC echo latencies, 80 attempts:
```text
100.859409, 109.942537, 112.640754, 301.449851, 277.060706, 129.586170, 102.829832, 103.621958, 108.632159, 116.699911, 103.243024, 129.168019, 109.855537, 103.717930, 104.651603
103.212904, 103.833749, 101.912346, 135.434359, 103.729218, 107.242130, 110.678712, 102.894003, 160.934809, 113.392029, 107.624463, 107.875544, 102.556391, 105.363949, 104.698554
114.057785, 109.704186, 101.930357, 104.120771, 102.274768, 103.711048, 101.996987, 140.717533, 101.545065, 104.509573, 102.361250, 102.224239, 101.921967, 108.980002, 135.725321
102.853403, 104.258932, 105.327948, 110.704292, 128.733688, 119.992382, 122.872050, 103.601818, 105.730082, 111.390828, 104.296294, 143.949833, 102.862834, 107.546013, 102.068507
103.369807, 102.795923, 126.980907, 144.143986, 103.642109, 152.369477, 102.702303, 101.547744, 103.832059, 103.985230, 105.004937, 138.245259, 226.409909, 117.073984, 109.182504
102.876464, 102.220620, 170.751583, 135.530301, 105.131418
```

Disposition: the serialized full/sparse ACK state is a demonstrated material
mixed-feedback owner. The exact publication correction and attainable user
benefit remain unproved. This diagnostic does not allocate all physical queue
cost to ACKs, identify every critical range, permit weakened acknowledgement
authority or establish ordinary performance acceptance.
