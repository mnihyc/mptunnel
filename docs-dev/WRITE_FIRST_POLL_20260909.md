# Existing local delivery: first-poll opportunity

Recorded: 2026-09-09. Category: joint-feedback model discriminator.
**Information-only observer; no candidate or performance acceptance.**
Ordinary runtime remains `b2aa215`; [the active plan](CURRENT_CLOSURE_PLAN.md)
and [publication model](SCOPED_ACK_SERVICE_MODEL.md) retain the decision boundary.

## Question and unchanged behavior

The next model alternative would conditionally publish receipt and consumed
capacity together when local write/flush completes without waiting, retaining
the original prompt ACK path when it cannot. Before a roughly 15-file candidate,
this capture asks how often the **existing whole write/flush future** completes
on its first poll and how much synchronous elapsed work that poll performs.
The protected origin is `444fb38`: a one-byte-then-Pending sink previously
prevented ACK, OPEN and FINAL progress. That correction is not removed here.

The [77-line observer patch](WRITE_FIRST_POLL_20260909.patch) only wraps the
existing invocation in `src/runtime/relay/io.rs`. It polls the same retained
future once, records Ready-success/Ready-error/Pending, then returns exactly
that result and continues polling the same future if needed. No ACK moves,
extra poll, timer, generation merge, queue, frame or scheduling decision is
introduced. Empty delivery is separate. Root froze
`.tmp/reflection/bin/write-first-poll-20260909/mptunnel` and reversed source
before capture. Existing feature summaries were enabled, individual perf
samples were not: neither log contains `mptunnel_lab_perf_sample` records.

Information forecast: frequent ready completion with small observed work can
justify the next coherent candidate/RED decision. Rare completion or material
synchronous work can defer it. Observation occurs at the old post-ACK location,
so readiness there is not proof of pre-ACK readiness or latency equivalence.
No throughput gain follows algebraically from a ready fraction.

## Measurement meaning and complete captured totals

`offered_bytes` is the sum of already-reassembled delivery buffers before
polling, not bytes synchronously written. A Pending poll can have written a
prefix; its offered bytes cannot be classified as wholly blocked. ReadyOk
means this complete write/flush transaction returned successfully. ReadyErr
can include a deferred apply error, not just a socket write failure.

Elapsed time encloses the existing inner future, including its instrumentation
and possible thread descheduling; it excludes the new outer record call.
These are elapsed polls, not CPU-only syscall timings. Existing diagnostics
record `max(floor(elapsed_us), 1)` and integer-truncate displayed averages.
Means below are recomputed as `total_us / total_count`, still subject to that
1 µs floor. No per-poll percentile can be recovered from these summaries.

| Role / direction / delivered batch / first result | Calls | Offered B | Total µs | Mean µs | Max µs |
|---|---:|---:|---:|---:|---:|
| Client download / nonempty / ReadyOk | 73,914 | 1,832,410,598 | 2,537,430 | 34.329 | 16,195 |
| Client download / nonempty / Pending | 3 | 42,845,490 | 12,415 | 4,138.333 | 5,815 |
| Client download / nonempty / ReadyErr | 1 | 12,000 | 3 | 3 | 3 |
| Client download / empty / ReadyOk | 42,638 | 0 | 57,720 | 1.354 | 872 |
| Server upload / nonempty / ReadyOk | 70 | 4,484 | 2,208 | 31.543 | 91 |
| Server upload / empty / ReadyOk | 10 | 0 | 10 | 1 | 1 |

No empty Pending/ReadyErr or server Pending/ReadyErr is recorded. All 130
first-poll summaries reconcile interval count, byte, time and maximum fields
with cumulative totals. Client final Ready totals are at Unix 1788930477669,
summary sequences 660–662; server nonempty final is 1788930477745, sequence
598. Sparse categories retain their last cumulative value rather than emitting
invented zero samples. These are captured totals, not a universal workload bound.

Among 73,918 nonempty client calls, ReadyOk is **99.9946% by count but 97.7146%
by offered bytes**. The three Pending calls cover 2.2848% of offered bytes;
the closure ReadyErr covers 0.000640%. Empty calls must not inflate pairing
opportunity. Server-upload here covers the HTTP request and small echoes only,
not a sustained upload, Android/TUN or slow local application acceptance test.

Pending evidence is not negligible large-buffer evidence: one offered batch
of 7,756,534 B has a 969 µs first poll in the summary interval ending
1788930441694 (previous ready summary 1788930440692). Two further Pending calls
offer 35,088,956 B combined, first-poll total 11,446 µs/max 5,815 µs, in the
restored interval ending 1788930466835 (previous summary 1788930465834).
That latter interval also contains the 16,195 µs ReadyOk maximum. The aggregate
does not identify its individual offered batch or separate work from descheduling.
It is evidence against assuming one poll is always a tiny syscall.

## Actual restricted-phase alignment

UP10 is present in service rows 15–24: client management Unix
1788930452626→1788930461626, server 1788930452635→1788930461635;
service elapsed 15.003263068→24.004296499 s. Row 25 is restored UP500.
Perf `interval_ms=1000` is nominal even for explicit or delayed flushes;
`interval_bytes_per_s` is therefore not used for precise phase timing.

Subtract cumulative snapshots strictly inside those observed UP10 endpoints:

| Role / category | Actual Unix-ms endpoints | Calls / offered B | Mean / max µs |
|---|---|---:|---:|
| Client nonempty ReadyOk | 1788930452708→1788930460830 | 12,150 / 283,441,462 | 34.732 / 5,856 |
| Client empty ReadyOk | Same 8.122 s interior | 6,284 / 0 | 1.193 / 81 |
| Server nonempty ReadyOk | 1788930452856→1788930460933 | 9 / 576 | 25.889 / 38 |
| Server empty ReadyOk | Same 8.077 s interior | 4 / 0 | 1 / 1 |

Client nonempty snapshots are sequences 244→376, empty 243→375;
server nonempty 226→340. Sparse empty totals are carried through common
flush boundaries. No Pending/ReadyErr occurs in these interiors. They do not
cover the whole ten-second restriction or identify exact event times within a
summary. The first-poll opportunity is high during this sampled return stress,
but the maximum still represents milliseconds of observed pre-yield work.

## Diagnostic user service and all body bins

Same 40 s mixed download plus 64 B serial echoes every 500 ms, 3 s timeout;
three TCP carriers and one QUIC share DOWN500, UP500→10→500 Mbps at 15–25 s.
DOWN/UP delay is 30/70 ms, zero configured jitter/loss, no outage. All 41
effective profiles match that schedule, burst/cburst 65,536 B and queue limit
8,192. Source instrumentation can affect this run; it is not an ordinary pair.

| Measure | Observed result |
|---|---:|
| Body bytes / elapsed s | 1,875,239,464 / 40.000790977 |
| Whole diagnostic body Mbps | 375.040 |
| Raw means 5–15 / 15–25 / 25–40 s, Mbps | 436.937 / 283.432 / 418.362 |
| First body, s | 0.583365182 |
| Longest body gap, s | 0.721830497 |
| Actual successful / failed echoes | 69 / 0 |
| Echo p50 / p95 / maximum, ms | 366.158 / 988.655 / 2046.487 |
| Echo start phases 0–5 / 5–15 / 15–25 / 25–40 s: counts | 10 / 19 / 11 / 29 |
| Same phase echo maxima, ms | 519.470 / 769.803 / 2046.487 / 918.226 |

Body gap: 20.581562772→21.303393269 s, 929,777,584→929,801,584 B.
Worst echo: attempt 35, 19.475338825→21.521825974 s. Other long restricted
echoes are 16.196446210→17.744411249 (1.547965 s) and
23.510929502→25.183195377 (1.672266 s). No echo timeout/disconnect occurs;
slow serial exchanges explain fewer actual attempts, not hidden failed slots.
All per-attempt start/end/outcome records remain in the raw probe.

```text
seconds     raw application body Mbps
 0–10       2.097 113.316 270.847 661.987 474.455 437.093 298.071 582.938 392.616 475.249
10–20       413.567 329.705 580.734 401.298 458.095 442.377 290.537 26.203 301.743 439.815
20–30       45.479 183.353 481.874 355.747 267.195 210.502 547.549 304.725 630.244 393.912
30–40       401.034 484.835 373.634 499.363 422.460 446.024 384.697 384.453 406.370 385.627
```

These are all 40 untrimmed bins; buffered ordered release can exceed physical
500 Mbps in a bin. Runner exits 0, elapsed 41.008 s, empty `probe.err`; one
duration-partial HTTP 200 and zero completed 8 GiB bodies. At duration stop,
client logs Broken pipe at 05:07:57.670 UTC, server RemoteClosed at .745 and
QUIC H3_NO_ERROR close at 05:07:58.700. The one ReadyErr is in the close flush;
these retained termination observations are not additional failed echoes.

## Physical service and resources

| Measure | Whole sampled window | Actual restricted rows 15→24 |
|---|---:|---:|
| DOWN class bytes / packets | 2,223,083,695 / 1,621,250 | 376,584,493 / 275,784 |
| UP class bytes / packets | 72,610,865 / 620,755 | 10,951,976 / 88,972 |
| DOWN / UP drops | 0 / 11,576 | 0 / 11,576 |
| Peak DOWN / UP backlog, B | 25,828,891 / 1,436,549 | 20,445,732 / 1,436,549 |
| Peak UP queue length | 8,184 | 8,184 |
| UP delta rate, Mbps | 14.519445 | 9.733972 |

The whole service window is 0.000052220→40.007568712 s; restricted UP
backlog ranges 32,830–1,436,549 B. Same-instance/same-epoch native ACKed-byte
deltas within restriction are server TCP 186,878,006 B, QUIC 151,984,060 B;
client TCP 4,001,803 B, QUIC 1,324,645 B. All four carriers remain active and
every TCP data carrier progresses. Return service is still constrained even
though almost every local write finishes on its first poll.

Client RSS first/peak/last is 31,940/79,640/79,640 KiB; server
30,548/331,880/331,880 KiB. Peak lifetime `ps %CPU` is 100/186 client/server.
These are not interval CPU causality or leak proof. Router and endpoint samples
are sequential, native counts are not unique application bytes, and overlapping
qdiscs must not be added. Queue/rate arithmetic does not identify the exact
blocking byte or prove individual echo delay unavoidable.

## Disposition

The readiness part of the forecast is supported for this local TCP-sink
download: nearly all nonempty transactions complete on the first poll at the
existing post-ACK boundary. The stronger claim that one poll is uniformly
negligible is not supported: rare large Pending offers and a 16.195 ms ready
elapsed maximum remain. This permits a bounded candidate decision, not a
promise of saved frames, wire bytes, lower latency or pre-ACK equivalence.

The capture does not resolve candidate behavior for blocked/partial/error
writes, startup/FIN work, TUN/DNS sinks or bulk upload. Preserve the original
ACK-before-Pending guarantee and exact retained future; do not implement a
timer workaround or infer a speedup from the 99.9946% call share. Ordinary
timing comparisons remain required if a coherent candidate is selected.

Raw result `mixed-combined-down-write-first-poll-0909`, logs, build and patch
material are retained in [the raw archive](WRITE_FIRST_POLL_20260909.raw.tar.gz).
README/PERFORMANCE and release promotion are unchanged.
