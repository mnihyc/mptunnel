# Native refill: accepted-copy and receipt attribution

Recorded: 2026-09-10. **Information-only same-feature comparison, not ordinary
performance acceptance.** Native-refill increases accepted repair payload from
188.687 to 294.627 MB (+56.15%) and duplicate receipt from 185.753 to 291.826 MB
(+57.10%), while Original payload and useful body bytes decrease. Almost all
candidate repairs are TCP persistent-gap or active-tail copies. This selects
the existing repair-service interaction for bounded causal review; it does
not prove that every copy is unnecessary, identify winners or authorize blanket
suppression. The adverse ordinary comparisons remain independently preserved.

## Question, controlled difference and provenance

The two ordinary mixed DOWN orders showed lower late goodput after native
refill, but native acknowledged-byte counters cannot distinguish new Original
work from copies. The predeclared question was whether accepted copies and
duplicate receipts increase, or whether unchanged/lower copy volume would
falsify that explanation and leave native/shared contention as the relevant
volume cost. No numerical speed improvement was forecast for this observer.

The reused four-file observer records actual committed Original payload by
underlay/lane, successful accepted copies by cause/underlay, separate
requalification, and checked new/duplicate/ordered-triggered receipt. A fifth
feature-only change in `tcp_write_admission.rs` permits startup opt-out before
socket option acquisition. CONTROL returns `None`, preserving structural-only
handoff; CANDIDATE acquires the unchanged native-refill capability. No observer
changes data, ACK clocks, copy policy, membership, timers or queue limits.
Both roles log exactly three `tcp_native_refill_policy` events: all
`disabled=true` in control, all `disabled=false` in candidate.

The exact patch is `./.tmp/reflection/native-refill-copy-observer-0910.patch`.
Root built the same feature executable once (1m08s; existing unused relay
helper warning only), froze it under
`./.tmp/reflection/bin/native-refill-copy-20260910/`, and reversed the overlay
before traffic. Ordinary source remains based on `b0baca2`; the user's unrelated
document is untouched. Both captures use identical periodic observation, not
per-frame bulk logs. The startup switch is diagnostic, not a user setting.
Build and run logs are `native-refill-copy-{build,pair}-0910.log` under the
reflection directory; root owns the wrappers and durable archive.

Closed inputs are the five files per cell in
`./.tmp/reflection/results/mixed-combined-down-native-refill-copy-{control,candidate}-0910/`.
CONTROL runs before CANDIDATE. Same feature/shape does not mean identical native
history or Original allocation. These Mbps must not replace ordinary-build
evidence or be compared to older feature captures as matched controls.

## Complete timing and useful service

The existing 40-second mixed DOWN load shares a 500/500 Mbps cut, DOWN 30 ms /
UP 70 ms, zero configured loss/jitter/QoS/outage, HTB rate=ceil, 65536-byte
burst/cburst and netem limit8192. All 82 service rows verify those settings;
class/qdisc drop deltas are zero. Both runners and probes succeed. HTTP200
responses are intentionally duration-partial 8 GiB objects, not fully completed
objects. All 156 recorded 64-byte echo attempts succeed; stderr is empty.

| Metric | Control | Candidate |
|---|---:|---:|
| Body bytes / elapsed, s | 2,084,973,000 / 40.000115 | 1,978,835,678 / 40.000051 |
| Whole useful goodput, Mbps | 416.993 | 395.767 |
| First body, s | 0.579165 | 0.621778 |
| Maximum read gap, s | 0.449493 | 0.449480 |
| Gap interval, s | 29.265437–29.714930 | 11.636817–12.086297 |
| Body counters around gap, B | 1,515,162,520 / 1,515,177,120 | 540,403,062 / 540,415,062 |
| Echo successes / failures | 76 / 0 | 80 / 0 |
| Echo p50 / p95 / max, ms | 350.217 / 787.893 / 1215.395 | 252.767 / 354.926 / 890.466 |
| Worst echo interval, s | 35.123804–36.339199 | 11.503541–12.394006 |
| Maximum successful-echo spacing, s | 1.215417 | 1.079274 |
| Last echo completion, s | 40.341755 | 40.144254 |

Candidate whole goodput is 5.09% lower; first body is 42.613 ms later and
maximum body gap essentially unchanged. Echo tails improve. Quantiles use
`round((n-1)*rank)` and phase membership uses request start; serial echoes alter
attempt counts. In particular, this diagnostic does not reproduce the adverse
late echo timing of both ordinary pairs; it cannot establish all their causal
performance mechanisms from copy volume.

| Probe phase: body Mbps; echoes / p50 / p95 / max ms | Control | Candidate |
|---|---|---|
| 0–5 s | 302.845; 10 / 255.562 / 616.385 / 616.385 | 285.875; 10 / 175.826 / 368.843 / 368.843 |
| 5–15 s | 429.591; 19 / 356.412 / 634.256 / 840.849 | 442.736; 20 / 283.974 / 394.356 / 890.466 |
| 15–25 s | 452.744; 20 / 394.534 / 513.108 / 572.986 | 428.924; 20 / 255.056 / 301.843 / 311.079 |
| 25–40 s | 422.801; 27 / 342.155 / 819.072 / 1215.395 | 378.974; 30 / 224.942 / 326.292 / 354.926 |

All 80 untrimmed body bins follow; all individual attempts remain in raw JSON.
Values above500Mbps are buffered application reads, not physical link rates.

```text
Control: 2.621,173.089,186.429,760.446,391.642,505.910,389.842,430.867,421.019,538.709,401.692,326.280,445.765,535.298,300.524,591.188,358.590,230.185,672.555,449.449,441.430,410.596,322.061,655.486,395.898,365.525,427.874,414.071,444.267,132.108,629.649,563.181,391.009,401.299,475.896,370.758,428.422,491.848,409.904,396.209
Candidate: 0.990,138.113,383.255,603.692,303.326,540.267,281.064,554.953,423.991,425.770,447.993,219.811,560.049,523.663,449.802,460.856,451.597,457.470,459.589,437.188,457.594,389.959,418.836,378.094,378.061,469.810,376.887,348.477,420.386,382.120,315.318,349.234,316.401,413.958,378.478,355.856,410.227,359.710,420.531,367.214
```

## Actual Original and accepted-copy accounting

The following are successful producer observations, not plans, native delivery
or receiver winners. Each cell is `count / payload bytes`. `udp` copy labels
denote the QUIC underlay, consistently with `quic` Original labels.

| Producer component | Control | Candidate |
|---|---:|---:|
| TCP Throughput Original | 29,341 / 1,393,007,177 | 13,606 / 544,608,865 |
| TCP Latency Original | 70 / 4480 | 30 / 2064 |
| QUIC Throughput Original | 15,107 / 720,827,823 | 37,459 / 1,455,869,647 |
| QUIC Latency Original | 6 / 384 | 51 / 3264 |
| TCP persistent ACK-gap accepted copy | 11,787 / 150,831,420 | 18,890 / 226,051,055 |
| TCP active-tail accepted copy | 769 / 25,374,974 | 2651 / 68,490,793 |
| QUIC persistent ACK-gap accepted copy | 818 / 10,424,368 | 6 / 85,000 |
| QUIC active-tail accepted copy | 44 / 2,055,744 | no emitted event |
| All Original payload | 2,113,839,864 B | 2,000,483,840 B |
| All accepted copy payload | 188,686,506 B | 294,626,848 B |
| Original plus copy payload | 2,302,526,370 B | 2,295,110,688 B |

No control/realtime Original, other repair cause or requalification component
is emitted in either cell. Enabled counters and source-close coverage support
absence of those observed accepted events, not fabricated zero-valued samples
or proof about every runtime branch. All component counters are process-wide,
not separate per-stream/physical-output ledgers. Latency Original bytes are
not by themselves a per-request winner classification.

TCP's actual Original payload share falls from65.900% to27.224%. Accepted copies
rise105,940,342B despite113,356,024B less Original payload. Copy/Original grows
8.926%→14.728%; copy/(Original+copy) grows8.195%→12.837%. Total admitted payload
is almost unchanged, while more of it is repeated DATA. This provides an actual
volume mechanism beyond the ordinary native-rate/share observations.

### Reconciliation, late service and close fences

All 800 control/765 candidate server perf rows and 798/828 client rows pass
per-component interval-versus-cumulative reconciliation for count, bytes and
microseconds, with one PID per role and consecutive perf sequence numbers.
The observer's ZERO-duration bookkeeping is floored by the recorder; those
microsecond fields are not measured service or CPU work.

For late producer accounting, take each component's latest cumulative value
at or before local recorder cuts25s and40s. Corresponding global periodic
fences are control24.100→39.116s and candidate24.060→39.083s. Inactive component
values remain unchanged between their last emission and the global fence.
These are approximately late source windows, **not exact probe25–40s windows**:
the probe has no exact Unix start anchor and role-local monotonic zeros differ.

| Late recorder-window payload | Control | Candidate |
|---|---:|---:|
| TCP / QUIC Original | 381,493,480 / 409,600,944 B | 93,816,228 / 623,663,784 B |
| TCP persistent / active-tail copies | 61,680,992 / 17,336,408 B | 106,439,606 / 49,243,776 B |
| QUIC persistent / active-tail copies | 5,350,992 / 1,453,856 B | 0 / 0 B |
| All Original / accepted copy payload | 791,094,424 / 85,822,248 B | 717,480,012 / 155,683,382 B |

Increased accepted copies are not startup-only. TCP persistent-copy rows
continue roughly every second through the source-close fence; the final
interval is3,156,536B control versus10,378,430B candidate. This does not sum
interval durations or claim a precise contribution to an individual body gap.

Control Original counters close at Unix1789023007861–7862ms; final global
server `stream_close` is3008202ms, including later QUIC active-tail/native
work. Candidate Original/persistent-copy close is3051369ms; its global close
is3051511ms. Server source flush follows body drop and path-close handling.
Client global `multipath_stream_close` is3008232/3051541ms; TCP receipt's last
changed rows are earlier at3007790/3051296–1297ms, while QUIC records include
the final echo. Earlier inactive components are not discarded. These fences
cover the observed accepted relay producers, not every arbitrary future native
completion/control event; sender and receiver cancellation/transit tails differ.

TCP successful encoded-plaintext totals are1,570,497,030B/42,692calls versus
840,223,160B/35,733calls. QUIC encode and successful-write totals are both
735,613,935B/8934calls in control; candidate encode is1,460,516,019B/11,814calls
but successful write is1,459,974,466B/11,813calls. Preserve the541,553B/one-call
domain difference; do not silently count every encode as successful native
handoff. TCP/QUIC encode minus accepted Original+copy residuals are
1,278,979/2,305,616B control and1,070,383/4,558,108B candidate. Those are arithmetic
remainders, not proof that all remainder is framing or that all payload drained.
Native retries and Noise wire overhead are not these plaintext byte counters.

## Receiver conservation and attribution limits

Receipt arithmetic is checked: new is the increase in frontier+buffered bytes,
duplicate is input−new, and ordered-triggered is frontier increase. A hole-fill
can release buffered data from another ingress, so ordered-triggered is neither
this carrier's transported bytes nor completed local socket delivery.

| Client component | Control TCP / QUIC | Candidate TCP / QUIC |
|---|---:|---:|
| Applied DATA count | 41,639 / 65,961 | 34,914 / 132,153 |
| New unique payload, B | 1,387,319,601 / 701,283,807 | 543,830,033 / 1,435,905,694 |
| Duplicate payload, B | 173,285,154 / 12,468,048 | 291,741,248 / 85,000 |
| Ordered-triggered bytes | 616,477,555 / 1,468,518,053 | 121,099,301 / 1,857,765,705 |

All new/duplicate/ordered triplets have equal counts at every observed flush.
Independent `mux.receive_data` totals107,600applications/2,274,356,610B and
167,067applications/2,271,561,975B exactly equal the sums of TCP+QUIC input.
No invalid arithmetic component occurs. Duplicate/input rises8.167%→12.847%
overall and11.104%→34.915% on TCP. New minus ordered leaves3,607,800/870,721B
not released by those observations, not a post-teardown memory-leak claim.
Ordered bytes exceed body+echo by17,744/24,208B; HTTP/local-delivery and stop
boundaries explain why those domains must not be forced into equality.

Per-underlay `max(0,new receipt−all-lane Original payload)` is zero in both
cells. This provides no positive useful-copy lower bound, but **does not prove
that no copy won**. A copy may win and the later Original become the duplicate;
accepted copies may still be in transit at close. Exact ranges and winning
receipt/ACK chronology would be required to label particular repair unnecessary.
This observation selects a material volume question, not a blanket removal.

## Sampled native and resource costs

| Cost | Control | Candidate |
|---|---:|---:|
| Service rows / final elapsed, s | 41 / 40.006723 | 41 / 40.007342 |
| DOWN / UP class bytes | 2,411,057,008 / 29,822,013 | 2,414,760,683 / 43,140,421 |
| DOWN backlog p50 / max, B | 12,817,790 / 26,873,854 | 9,898,976 / 20,304,365 |
| UP backlog p50 / max, B | 43,026 / 176,121 | 82,297 / 108,864 |
| Client RSS peak / final, KiB | 93,032 / 93,032 | 84,844 / 84,844 |
| Server RSS peak / final, KiB | 268,588 / 268,588 | 387,464 / 387,464 |
| Last client / server lifetime CPU, % | 67.2 / 179.0 | 92.2 / 205.0 |
| Server TCP NOTSENT p50 / max, B | 3,116,988 / 23,014,228 | 0 / 299,310 |
| Server TCP sent-unacknowledged estimate p50 / max, B | 10,676,782 / 23,197,120 | 4,538,050 / 13,910,482 |
| Server TCP RTT p50 / p95 / max, ms | 284.184 / 427.062 / 508.711 | 252.977 / 339.093 / 393.307 |
| Server QUIC RTT p50 / p95 / max, ms | 275.934 / 470.368 / 515.223 | 243.662 / 350.303 / 389.070 |
| Server QUIC native flight p50 / max, B | 4,396,656 / 13,136,244 | 9,400,248 / 14,686,980 |

All sampled TCP sockets remain three per role. Lower native RTT/DOWN queue in
this observed pair differs from the ordinary adverse latency pattern, another
reason not to use it as ordinary acceptance. DOWN class bytes are nearly equal
while body goodput falls; return class traffic, CPU and server RSS rise. Count
one class per direction; native packetization, retry and control costs prevent
equating router bytes with accepted payload. Native flight here uses TCP
Send-Q−NOTSENT sequence accounting and QUIC's native metric, not unique receipt.
Lifetime `ps` CPU is not interval CPU, and RSS is not post-teardown ownership.
Only duration-stop BrokenPipe/RemoteClosed and candidate's later `H3_NO_ERROR`
occur in warning logs, with no failed recorded request.

## Disposition

The predeclared information forecast succeeds: increased accepted repair and
duplicate volume is real, material and sustained into late service. It is not
just an estimated-rate display or unchanged copy traffic. The actual Original
distribution also changes substantially, so the captured association does not
isolate every causal feedback between placement, native service and recovery.
Keep the ordinary trial unpromoted; review this existing repair-service boundary
with exact authority/timing evidence before selecting any correction. No new
threshold, third favorable ordinary repeat, blanket hedge suppression, public
performance claim or release is justified by this diagnostic.

Root preserved and listed sixteen files in
`NATIVE_REFILL_COPY_ATTRIBUTION_20260910.raw.tar.gz`: ten result files, the exact
five-file overlay, both explicit-mode wrappers, build/run logs and the runner.
The overlay was removed before traffic and ordinary executable restored.

Practical next disposition: retain `b0baca2` as the working candidate with the
documented mixed cost, not global acceptance. Restoring the proved second-long
TCP FIFO delay is not justified by these smaller costs alone; equally, volume
does not justify another repair policy. Continue the existing larger return-cut
and failure/recovery gates with ordinary binaries, preserving all adverse data.
