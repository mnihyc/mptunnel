# ACK-direction causal gate: download hypothesis rejected

2026-09-09 22:21 +08:00. Category: completed source/publisher characterization,
not a runtime correction, performance milestone or release acceptance.

## Question and outcome

The proposed half-BDP correction cannot address the demonstrated mixed download
return-feedback cost through this caller. The helper consumes an opposite local
outbound rate, but ordinary client DATA invokes `ack_only` before application
write, forcing ACK evaluation. Every changed receive batch materializes its
generation irrespective of the calculated byte threshold. An unchanged forced
call retries the current generation; it does not manufacture another ACK.

The real publisher test varies only the snapshot's legacy delivery rate, holds
RTT at100ms, and replays identical contiguous, sparse, duplicate and hole-fill
receipts. The prewrite `ack_only` / postwrite `new(...,false)` sequence exercises
RequestSenderService, ReliableRecvStream, an exact live command output and the
actual codec, not a replacement implementation of their predicates.

| Input / actual result | Low outbound scalar | High outbound scalar |
| --- | --- | --- |
| Delivery scalar |351,000bps |500,000,000bps |
| Calculated bulk byte threshold |65,536B |3,125,000B |
| Generations after six receipts |1,2,3,3,4,5 |1,2,3,3,4,5 |
| ACK frames / codec bytes |5 /127B |5 /127B |
| MAX frames / codec bytes |3 /78B |3 /78B |

All frame contents, scopes and encoded traces are identical. These are plaintext
Product codec bytes, not protected records, native transmission or network bytes.
The duplicate creates no new generation. Sparse receipt publishes its new facts
without advancing receive credit; hole fill advances the contiguous prefix.
The test models completed application writes at the postwrite boundary; it does
not execute the entire relay actor or attribute elapsed network service.

## Commands and observed verification

Temporary test patch: [characterization](ACK_DIRECTION_CHARACTERIZATION_20260909.patch).
It was applied only for verification and then fully removed; it is deliberately
not a permanent assertion that future publication cadence must remain unchanged.

```sh
git apply docs-dev/ACK_DIRECTION_CHARACTERIZATION_20260909.patch
CARGO_BUILD_JOBS=2 CARGO_PROFILE_TEST_DEBUG=0 cargo test --lib client_prewrite_ack_publication_is_independent_of_outbound_rate -- --nocapture
```

Observed build30.72s, warning-free;1test passed,0failed,2434filtered,0.00s.
Both printed rows report changed_generations=5, ack_frames=5,
ack_codec_bytes=127, max_frames=3, max_codec_bytes=78.

The same compiled executable then ran the filters below together:8passed,
0failed,2427filtered,0.06s. No second compile or network lab occurred.

```text
client_recv_progress
client_stream_ack_publication
response_startup_ack_open_and_final_progress_during_blocked_local_delivery
final_feedback_backpressure_keeps_fin_pending_until_ack_is_queued
retained_in_order_fin_commits
restart_reset_terminates_during_blocked_product_write
```

Root review and independent source/fixture audits agree. Runtime/RFC/Cargo diff
againstb2aa215 is empty after removal, the saved patch applies cleanly, and
target/release/mptunnel equals the frozen scoped-ack ordinary executable.

## Why the forecast failed and what remains justified

The half-BDP arithmetic was correct, but the proposal did not follow the
unconditional caller override. It therefore forecast an opportunity at a gate
that does not control ordinary download generation. Changing its input has zero
effect on the demonstrated forced sequence. Source and direct execution suffice
to close this branch; the predeclared observer/asymmetric capture was canceled
before implementation because it could not change that decision.

This does not prove all scheduling or future membership is rate-independent.
Server upload's ordinary DATA caller is unforced and is a separate question;
no new material upload attribution justifies moving the experiment there.

The original444fb38 change addressed real blocked application delivery preventing
ACK, OPEN and FINAL progress. Its protection remains necessary. Merely removing
force has a source-reachable counterexample: after a first ACK, a subthreshold
bulk receipt arrives before the timer and its application write remains Pending.
The inner write loop retries only already-materialized attachment generations;
the unmaterialized pending-ACK deadline is owned by the suspended outer loop.
No incidental attachment event is guaranteed, so new receipt feedback can wait
indefinitely. The existing first-receipt/Latency startup test would miss this
particular regression. This counterexample is source evidence, not an executed
force-off candidate or another observed deployed defect.

RFC8.3 also currently requires offering pending receipt before park/yield.
A future publication model must explicitly distinguish unmaterialized receipt
from per-attachment publication debt, preserve service during blocked I/O,
independent output retries and exact-once delivery, and revise the RFC if it
permits ACK delay. MAX must still reflect actual freed capacity. No such model,
estimator, threshold, timer or rejected batching variant is implemented here.

The known mixed return-feedback stall and separate native-contention latency
remain open. This result removes one unjustified fix, not either user-visible
defect. Broader timing, recovery, aggregation, baseline and experience gates in
CURRENT_CLOSURE_PLAN are unchanged; no README/PERFORMANCE or release changes.
