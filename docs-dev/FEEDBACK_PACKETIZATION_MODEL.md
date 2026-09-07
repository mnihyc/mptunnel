# Feedback packetization: bounded next discrimination

2026-09-06 23:35 UTC. This is the existing mixed/asymmetric feedback owner,
not a new controller or a claim that every mixed-path stall has one cause.

## Observed result and attribution

Clean 500/10 Mbps, 100 ms RTT: the ordinary relative-ACK candidate delivers
174.755 Mbps, with a 1.666 s read gap and 1,335 ms echo p95. It is not accepted.
The unmodified control gives 46.249 Mbps; QUIC-only and raw TCP give about
432 and 444 Mbps. Full ordinary and diagnostic series are retained in
ACK_RELATIVE_ENCODING_20260906.json. Diagnostic timing is not acceptance.

A separate aggregate encoding trace records, near the end of the same profile:
174,643 relative ACKs / 4,869,209 encoded bytes; 144 independent complete ACKs /
30,946 bytes; zero partial ACKs; and 99,557 credit frames / 2,588,482 bytes.
Thus the dictionary is working: repeated full vectors are no longer the main
remaining encoded cost. The return cut carries about 29.1 MB, and native ACKed
client payload totals about 12.4 MB across the four carriers. Those quantities
are different layers and observation instants; their difference is not an
exact packet-header attribution. The trace counts attempted encoding, not
proof that every encoded record completed native transmission.

The TCP writer currently commits and flushes every ordinary command, including
a small ACK or credit update, separately. This rule entered in checkpoint
3a6d0ea. Its intended benefit is prompt priority/lifecycle arbitration and exact
post-write accounting, not a requirement for one encryption record or packet
per feedback frame. The original frame-by-frame rule can turn ready small
feedback into many records/packets. No production packetization change follows
from these counts alone; the bounded ready-feedback comparison below decides.

## Model / counterexamples before code

Distinct boundaries must remain distinct:

1. The logical receive owner updates current ACK/credit state promptly.
2. Every exact attachment retains its own successful-publication fence.
3. The final writer selects dependency-ready work with existing priority.
4. Several already-ready feedback records may share one protected write.
5. Exact writer debt and completion publish only after write and flush succeed.

An ACK or MAX_DATA batch may contain only the already-ready contiguous feedback
head of the priority queue. It must not wait for more work or cross lifecycle,
open, data, requalification or other command barriers. Retirement/control work
retains priority. Existing byte/item run bounds bound the finite transaction;
they are resource bounds, not a timer or throughput tuning parameter. Every
decoded frame, its order and its complete flag remain identical. No positive
range is discarded; no negative horizon, receive grant or fanout policy changes.

If the next queued item is not eligible feedback, retain that exact head for
normal arbitration, without releasing its byte ownership or moving it behind
a later item. Selected records retain charge until commit or terminal cleanup.
New higher-priority work may wait behind the already-selected finite feedback
transaction, but never behind additional lower-class bulk acquired by it.
An empty or one-frame ready set flushes immediately. No sleep, ACK-frequency
threshold, native window, startup-rate prior or protocol preference is added.

The bounded comparison must first test exact ready-head order, barriers,
queue/receiver drop accounting and encrypted multi-frame equality/record cost.
Then use the identical 500/10 profile with diagnostics disabled, followed by
both-direction clean and adverse controls if it is practically useful. If it
does not remove meaningful wire/timing cost, reject it rather than enlarging
the batch, slowing publication or adding another dictionary.

## Reflection on earlier acceptance

CREDIT_CADENCE_CLOSURE proved conformance to the then-written RFC, not a
practical benefit from putting every small credit increment on the wire.
Calling pre-admission coalescing bounded was true about memory but insufficient
about packet cost after queue acceptance. Preserve immediate retained credit
and failover publication while investigating this distinct physical cost.
Neither a blanket rollback of that commit nor a new delayed-credit threshold
is justified yet.

Reference, not transplanted policy: QUIC explicitly balances feedback overhead
against prompt loss/flow-control progress and permits processing already-ready
packets together; it retransmits current control information, not necessarily
old frames. MPTCP's cumulative Data ACK has different semantics from MPP's
selective positive/negative snapshots, so copying it would not preserve MPP
repair authority. See [RFC 9000 §§4.2, 13.2.2, 13.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.2)
and [RFC 8684 §3.3.2](https://www.rfc-editor.org/rfc/rfc8684.html#section-3.3.2).

## Actual writer RED — 23:38 UTC

`ready_feedback_shares_one_tcp_transaction_without_taking_bulk` constructs the
existing protected server TCP session, queues an ACK, MAX_DATA and ordinary
data, then invokes the real writer run. Old code leaves MAX_DATA queued instead
of sharing the already-ready feedback transaction. It fails before any candidate
writer change. The candidate must consume exactly ACK and MAX_DATA, decode both
unchanged at the peer, and leave ordinary data for a later arbitration.

The queue implementation may retain one inspected priority head because Tokio's
bounded channel does not provide a non-consuming peek. This is an extra bounded
envelope, not a logical-flow map or history. It retains its byte charge and is
visited before later priority arrivals in ordinary receive and terminal drain.
The test set must cover its byte bound, retirement priority and drop ownership.

## Component GREEN — 23:44 UTC

The real server-writer test now consumes ACK and MAX_DATA in the same write,
decodes both unchanged, and leaves bulk queued. Three queue tests cover the
encoded-byte bound, non-feedback head, terminal drain, higher-priority control,
stale-stream filtering, and exact retained-head charge on drop. The existing
one-data-head priority test and all 74 TCP controls pass. The protected-record
control sends the same 64 credit frames: TLS 3,072 -> 1,686 bytes; Noise
2,816 -> 1,682 bytes, without a timer or loss of a logical frame. This proves
record overhead, not a network-speed improvement.

The implementation does not scan a growing transaction to validate each new
frame: induction from its first frame keeps that check constant-time. The
next head's priority and class are checked at each ready selection. The existing
writer-run byte/item bounds apply; no new number is used as a performance gate.
The RFC clarifies the distinction between arbitration and record boundaries.
Strict Clippy initially rejected a redundant boolean expression; simplifying
that expression changes no predicate. Queue controls and strict checks rerun
before the ordinary optimized comparison. No source commit or acceptance yet.

## First ordinary comparison — 23:47 UTC

Strict Clippy and all 26 queue controls pass. The optimized build takes 1m03s;
no traffic overlaps it. The unchanged 500/10 case with ready-feedback batching
gives 313.253 Mbps, 0.524 s maximum read gap and 564 ms echo p95; 49 attempts
succeed without a failure. The return cut still carries 29.833 MB. The earlier
relative-only ordinary case gives 174.755 Mbps / 1.666 s / 1,335 ms, but its
diagnostic runs vary substantially. One matched ordinary control/candidate
repeat is next. This is not a competitive gate or proof that all feedback cost
is fixed; raw and QUIC-only remain about 444 and 432 Mbps with 0.10 s gaps.
The current candidate changes neither logical feedback cadence nor fanout.

## Decision — 23:53 UTC: remove the packetization candidate

The matched ordinary repeat gives relative-only 291.867 Mbps / 0.879 s gap /
876 ms echo p95 versus batched 301.671 Mbps / 1.001 s / 874 ms. Both make 42
successful echo attempts. The earlier large apparent gain does not repeat;
the repeat is only 3.4% in mean with essentially unchanged p95 and a longer
worst gap. The return queue also grows to 3.78 MB in the batching repeat.
Full series are FEEDBACK_PACKETIZATION_COMPARISON_20260906.json.

This is real protected-record overhead but not an accepted practical timing
fix. Remove the ready-feedback queue head, both writer integrations, associated
tests, helper export and proposed RFC paragraph. Checks confirm no batching
symbols remain and the prior held queue/mailbox/repair diffs are unchanged.
The composed experiment diff and frozen executable remain under .tmp/reflection;
the composed diff includes older held work and is NOT a standalone patch.
Do not enlarge a batch or add a timer to make this experiment pass.

The separate lossless ACK encoding candidate remains uncommitted. Its direct
return-cut improvement is substantial but its other affected clean/adverse
controls still decide component acceptance. This failed packetization trial
does not establish that publication cadence or fanout should change, and it
does not disprove the independently captured slow-original ordering problem.
After these bounded encoding controls, resume that existing allocation owner;
no further feedback-policy redesign follows from record counts alone.
