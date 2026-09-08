# Ready-credit return service — healthy mixed UP

2026-09-08. **Residual reader/input coupling is observed; no new fix or
performance acceptance.** The largest 1.729 s reply-frontier hold contains
at least 1.254 s of ordinary-QUIC reader downstream handoff waits before
any covering response is decoded. These waits overlap useful service, but
are not exclusive CPU, proven earlier native availability or an additive
speed budget. The new MAX-fold deferred slot is not the dominant residence:
all 421 observed Data prefetches return to the actor within 5 ms.
Useful postdecode delay also remains, up to 294 ms.

## Scope, identity and complete result

One declared high-capacity residual-stall diagnostic follows the
[ordinary eight-cell result](READY_CREDIT_SERVICE_20260908.md),
[active forecast](CURRENT_CLOSURE_PLAN.md) and
[mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md).
Runtime `fcc0b22` includes only the reviewed ready-MAX fold over `0449b9f`;
the response-only
[19-file observer](READY_CREDIT_RETURN_TRACE_20260908.patch)
is frozen separately as
`./.tmp/reflection/bin/ready-credit-return-20260908/mptunnel`.
The diagnostic build is warning-free 1m 05s. All observer hooks were reversed
before capture, with no build/lab overlap and no production policy change.

The unchanged healthy mirrored UP profile is one shared 500 Mbps upload and
500 Mbps return cut, 70 ms upload + 30 ms return, zero jitter, no random-loss
clause, no QoS reduction and no UDP blackout. All 41 shaping rows agree.
The configured netem limit is 8,192. This is not the harsh loss/QoS/outage
profile and the diagnostic rate is not a new ordinary speed result.

Raw directory:
`./.tmp/reflection/results/mixed-combined-up-ready-credit-return-healthy-0908/`.
The [raw archive](READY_CREDIT_RETURN_SERVICE_20260908.raw.tar.gz) retains
the five result files plus build/run evidence; observer patch is separate.
There are 4,942 client log lines (all diagnostic), 2,055 server lines
(2,054 diagnostic plus one end-of-run H3_NO_ERROR warning) and 41 service rows.
`probe.err` is empty; runner exit 0 takes 41.004697 s.

| Probe field | Result |
| --- | ---: |
| Local accepted = exact target-confirmed B | 1,748,172,800 |
| Exact completion / completed / failed streams | true / 1 / 0 |
| Probe elapsed s / confirmed Mbps | 40.796490 / 342.808 |
| First confirmation / maximum closed gap s | 0.409043 / 1.728562 |
| First local write / maximum local-write gap s | 0.105118 / 0.357530 |
| Elapsed beyond 40 s load boundary | 0.796490 s |
| Accounting | valid exact target-sink ACK; no probe error |

Elapsed after the load boundary is not a measured per-byte drain time.
No concurrent loaded-echo workload runs in this UP probe. All 41 untrimmed
raw confirmation Mbps bins follow; final bin is partial. Values above
500 Mbps, including 1,139.021 in bin 40, reflect buffered confirmations,
not wire transmission above the configured capacity.

```text
0: 11.416, 211.361, 373.101, 164.722, 222.682, 466.328, 103.432, 343.645, 399.936, 157.305
10: 448.728, 266.486, 312.287, 316.146, 455.079, 500.931, 154.155, 357.027, 367.760, 320.631
20: 280.844, 415.702, 674.271, 149.997, 302.570, 266.584, 195.085, 533.341, 122.052, 266.333
30: 848.732, 0.000, 199.430, 945.342, 0.000, 437.588, 159.804, 52.862, 450.932, 591.732
40: 1139.021
```

C/S denote one-based client/server log lines. Exact stamps below are Unix
milliseconds unless explicitly marked microseconds. Client PID320722 and
server PID326413, session6066022389664856043 and logical stream0 apply
throughout. Endpoint monotonic origins differ; do not subtract them across
processes. Millisecond observations retain their rounding uncertainty.

| Carrier | Server wire / physical / attachment | Client runtime / physical / attachment | Native stream |
| --- | --- | --- | --- |
| QUIC | 0 / 1 / 4 | 0 / 1 / 3 | ordinary H3 request4; repair8 |
| TCP wire0 | 0 / 2 / 2 | 2 / 2 / 1 | shared TCP session, remote port7443 |
| TCP wire1 | 1 / 4 / 1 | 0 / 4 / 0 | shared TCP session, remote port7443 |
| TCP wire2 | 2 / 3 / 3 | 1 / 3 / 2 | shared TCP session, remote port7443 |

These are joined namespaces, not assumptions that integer IDs are equal.
TCP authenticated decode supplies wire/session identity; later routing and
shared-input events supply runtime/physical/attachment identity. Concurrent
copies of the same range must use that mapping, not the next same-offset log
line. QUIC repair8 and ordinary4 are distinct ordered native streams.

## Source, transaction and delivery conservation

All 187 positive target-reply reads, 187 source enqueues and 187 successful
Original claims reconcile 2,794 B, gap-free [0,2794), without Original overlap.
Every source enqueue has U_before=0, offset=C+U_before and U_after=its bytes;
each successful claim ends at its recorded C and leaves U=0.
Actual read completion→enqueue is at most 1 ms, enqueue→claim 2 ms and
claim→positive local-write completion 3 ms. The latter maximum is
[947,961), S818@1788882156003 / S819@1788882156004 /
S821@1788882156007. No multi-second small-response prepared-placement hold
is observed. Socket availability before the read begins remains unmeasured.

The 187 Originals comprise 150 QUIC and 37 TCP transactions. Another
373 accepted repair dispatches add 5,347 B; all 560 resulting Data
transactions / 8,141 B have write-begin and positive local completion:
401 TCP, 150 ordinary QUIC and 9 repair QUIC. TCP flush/QUIC write completion
means local acceptance, not wire arrival. Producer and write keys are
unambiguous by range and mapped identity in this capture.

All 560 transactions decode at the client. Only 559 / 8,126 B reach shared
input and mux: final losing TCP wire1 [2735,2750), positively written at
S1988@1788882181346, decodes at C4940@1788882182357 and routes
C4941–C4942 with `attachment_present=false`, after logical completion.
This is not an unexplained network loss or missing useful delivery.

The mux has 159 frontier advances and 153 successful local-write batches,
conserving 2,794 B through final F2794 at C4797@1788882182273.
Some adjacent advances share a local-write batch; every advance is covered
by the following successful delivery. Advancing ingress is 94 QUIC Originals,
9 TCP Originals, 3 QUIC copies and 53 TCP copies. A newly arriving head does
not own every byte released from buffered suffixes.

Of 400 nonadvancing mux applications, 28 contribute previously unseen buffered
suffixes totaling 515 B, while 372 are fully covered duplicates. Examples:
[1129,1143) enters at C2369 while F1115 and is released by C2389/C2390;
[2000,2015) enters C3770 while F1970 and releases with C3875/C3876;
[2315,2330) enters C4248 while F2300 and releases with C4337/C4338.
The raw exact-range evidence preserves all other suffix provenance.

## Three largest exact reply-frontier holds

| Missing F / hold | Original source enqueue / positive write | First covering decode | Winning decode / mux / local write |
| --- | --- | --- | --- |
| 1970; C3726@1788882172093 → C3871@1788882173822, 1729 ms | S1560@1788882170665 / QUIC S1563@1788882170666 | TCP wire2 C3838@1788882173566 | same TCP C3838 / C3871@1788882173822 / C3872 same ms |
| 1700; C3351@1788882168844 → C3518@1788882170428, 1584 ms | S1377@1788882167287 / QUIC S1380@1788882167288 | TCP wire2 C3504@1788882170292 | QUIC C3508@1788882170337 / C3518@1788882170428 / C3519 same ms |
| 2225; C4057@1788882175437 → C4220@1788882176890, 1453 ms | S1701@1788882174091 / QUIC S1707@1788882174093 | TCP wire2 C4162@1788882176665 | same TCP C4162 / C4220@1788882176890 / C4222 same ms |

For F1970, the winning TCP copy is positively written at
S1636@1788882172364; write→decode is 1,202 ms. Its decode→route begin
C3845@1788882173757 is 191 ms; route ends C3846@3758,
shared send C3857@3799 / C3859@3800, prefetch C3869@3822,
actor API return C3870@3822, then mux/local write in the same millisecond.
Postdecode residence is 256 ms. The Original QUIC reply decodes later,
C3894@1788882174210, and loses; its total write→decode age is not the
critical hold duration or automatically a native transport delay.

F1700's QUIC Original wins only 91 ms after decode. Its actual native read
starts at 1788882170337029 us and takes 1 us; route begins C3513@1788882170383,
ends C3514@0384, shared send C3515@0419 / C3516@0420,
actor return C3517@0427, mux/local C3518–C3519@0428.
No prefetch event occurs for this winning transaction. The earlier-decoded
TCP copy is not routed until C3520@1788882170460, after QUIC wins.

F2225's winning TCP copy is positively written S1763@1788882175518,
then spends 225 ms from decode to mux/local delivery. Original QUIC decode
C4261@1788882177500 is again later than the winner. These joins show that
the large holds are not entirely the final shared slot or postdecode stage.
They do not split TCP write→decode into wire, native receive queue, framing
and reader polling.

## Ordinary-reader predecessor waits inside the actual holds

All 150 ordinary-QUIC response decode summaries reconcile exactly with
302 typed predecessor rows: counts, summed send-await and maximum agree.
The ordinary reader's windows are disjoint, reset after the previous
response's mailbox completion; repair-reader waits are separate.

| Preceding Frame kind | Count | Sum send-await us | Largest single send-await us |
| --- | ---: | ---: | ---: |
| STREAM_MAX_DATA | 97,670 | 28,738,139 | 223,107 |
| STREAM_ACK | 24,885 | 6,928,086 | 61,484 |
| PATH_PROOF_ACK | 2 | 1 | 1 |
| PATH_PROOF_DATA | 1 | 0 | 0 |

These are real asynchronous downstream handoff waits, not CPU samples and
not counts of credits surviving the later MAX fold. Different typed causes
and later actor work must not be collapsed into “ACK processing.”
The total does not reveal which receiver handler was executing.

For each window W with summed wait w and critical interval I, a conservative
overlap is max(0, w − duration(W outside I)). Sum only disjoint windows.
Per-kind bounds can use that kind's first-send-start / last-send-end envelope.
The bounds below use [F-start-ms+1 ms, interval-end-ms], conservatively inside
the rounded event timestamps. They include **all intervening response windows**,
not only the final head's decode summary. All values below are milliseconds.

| Missing F | Whole hold | Before first covering decode | Lower-bound all / MAX waits before first decode | Lower-bound all / MAX waits within whole hold |
| --- | ---: | ---: | ---: | ---: |
| 1970 | 1729 | 1473 | 1254.354 / 890.604 | 1510.354 / 1146.604 |
| 1700 | 1584 | 1448 | 1330.797 / 1073.016 | 1448.867 / 1163.538 |
| 2225 | 1453 | 1228 | 1066.205 / 834.868 | 1224.708 / 910.886 |

Auditable F1970 windows; all endpoint stamps and wait columns here are **us**.
The “before decode” end is C3838's rounded
1788882173566000 us, with start1788882172094000 us. A zero lower bound
means the aggregate alone cannot place that window's wait inside this
intersection, not that no wait occurred.

| Decode row / response range | Window start → end Unix us | All wait us | All wait lower bound before first decode us | MAX lower bound before first decode us |
| --- | --- | ---: | ---: | ---: |
| C3728 / [1835,1850) | 1788882171883390 → 1788882172104731 | 207412 | 0 | 0 |
| C3746 / [1850,1865) | 1788882172105123 → 1788882172341190 | 223217 | 223217 | 185275 |
| C3758 / [1865,1880) | 1788882172343090 → 1788882172575697 | 220177 | 220177 | 188544 |
| C3776 / [1895,1910) | 1788882172578755 → 1788882172960038 | 325534 | 325534 | 254648 |
| C3792 / [1910,1925) | 1788882172960153 → 1788882173236353 | 249509 | 249509 | 201884 |
| C3894 / [1970,1985) | 1788882173237074 → 1788882174210372 | 880289 | 235917 | 60253 |

The largest lower bounds are not proof that the missing bytes were already
available in Quinn or a socket throughout those intervals. They establish
that the ordinary reader could not continue reading while forwarding prior
control work downstream. That is a concrete local-service dependency
overlapping the critical hold; replacing it requires its own model and proof.
No 1.254 s or 1.510 s performance saving is promised.

## Local return boundaries and contrary cases

All 559 shared Data transactions have actor-visible API returns:
553 async and 6 ready-only. Exactly 421 first pass through
`shared_prefetch_boundary`; the other 138 return directly.
Prefetch→actor return is at most 5 ms, API return→mux at most 4 ms,
and mux→successful local delivery at most 3 ms for advancing work.
Shared-send-begin→API return is at most 25 ms. Thus this capture does not
support blaming the MAX-fold pending slot for the multi-second residual
response ages. Send-complete is not a queue linearization timestamp: another
worker may log a dequeue before the sender logs completion.

The maximum useful decode→local delay is 294 ms:
TCP Original [149,163), C193@1788882145244→C208@1788882145538;
and [1115,1129), C2346@1788882160879→C2390@1788882161173.
The first occupies 294 ms of a 320 ms current-F hold. The second was decoded
before F1115, so its entire 106 ms F1115 hold is local, but its whole
294 ms postdecode age must not be called a 294 ms critical hold.

The strongest wholly already-decoded head interval is F2060:
TCP wire0 C3951@1788882175184 precedes F becoming2060 at
C3960@1788882175192. A QUIC Original decodes C3963@5266 and wins
C4002/C4003@5417; the TCP reply is only routed C3982@5362 and
reaches shared send C4010@5419, after the winner. The entire 225 ms hold is
locally unserved despite available decoded covering bytes. This is useful
contrary evidence, not the global 510 ms postdecode maximum from a losing
[331,345) copy (C833→C913).

All 150 ordinary response read-awaits are at most 346 us after the read is
actually entered; this does not say when bytes first became readable before
entry. The longest repair read is 3.278680 s for [2735,2750),
C4525, but starts1788882178099184 us, long before that repair is positively
written S1991@1788882181346. It includes prior idle time and is not a
3.279 s network-delay measurement. Neither absent native hooks nor prompt
individual reads prove absence of physical/native waiting.

## Sampled context and bounded cost

S is locally read upload source, T server target-written upload, Rs server
reply reads and Rc client local reply writes; S≠claim C, T≠mux F and Rc
must not be substituted for an exact F event. For example, service L32→L33
has server stamps1788882172450→1788882173450: T rises
1,330,519,729→1,373,804,257, Rs2105→2180, while client Rc1970 is flat
at stamps1788882172449→1788882173448. This sampled forward progress and
reply lag supports the exact F1970 join without defining its endpoints.

Client RSS peak/last is 531,776 / 531,776 KiB; server 54,464 / 54,464 KiB.
Sampled ps %CPU maximum/last is client159 /159%, server97.5 /97.4%.
These are process-lifetime averages, not exclusive actor/reader CPU. Last
service L41 at elapsed40.004558 s is before logical completion:
S1,733,752,897 / T1,729,538,365 / Rs2750 / Rc2540.
No post-load settled-memory or leak claim follows.

Router HTB class first→last sampled deltas: upload1,896,397,372 B /
1,293,129 packets /0 drops; return48,687,423 B /362,214 packets /0 drops.
Peak sampled class backlog is upload17,408,789 B, return67,412 B.
Do not sum parent and child qdiscs, infer exact duplicate fractions or
native byte availability from these totals. The configured healthy link
still permits queueing; zero sampled drops is not a proof of zero transport
or service delay. Diagnostic instrumentation cost is not an ordinary
performance comparison.

An independent read-only recheck reproduces the exact F1970 overlap lower
bounds and all source/write/mux/delivery conservation totals. All 41 raw
confirmation bins match the probe JSON.

**Disposition:** the source/claim and prefetched-slot hypotheses are not
material explanations in this capture. Ordinary-reader downstream backpressure
overlaps the dominant healthy reply holds, with smaller but real useful
postdecode coupling. Exact earlier native availability and the downstream
handler's exclusive cost remain unresolved. The evidence selects a service
boundary for a bounded model decision, not automatic ACK batching, a new
controller or another performance claim.
