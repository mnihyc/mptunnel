# Request pipelining: live-owner RED

2026-09-07 07:36 UTC. Category: bounded test-only reproduction.
Runtime unchanged; no lab, performance claim or release acceptance.

## Real ownership path

The two tests in `src/runtime/sender/request/tests_multipath.rs` share one
fixture and use `RequestSenderService::send_frame`: ordinary selection,
exact Product admission, writer reservation, qualification receipt/flight
commit and carrier publication. Each legal frame is read from the carrier
command receiver; dequeue does not release its Product debt. Only live
`apply_product_ack` releases the exact disjoint ranges.

Normal socket/path-open context is installed, but no capacity-proof flag,
qualification, numeric rate or sampling state is injected. Default resource
geometry gives a 1,048,576-byte TCP coverage floor. A complete first cohort is
admitted, then ACKed after 100 ms: qualification, ACK-clock proof and numeric
sample 1 are all established by that actual release. No acquisition Owner
remains. The subsequent pipeline reaches two coverage cohorts outstanding,
well within the unchanged configured Product resources.

Pipeline ordering is `assign A; assign B; ACK A; assign C; ACK B; ACK C`,
using 50 ms observation steps. The staged control assigns B after ACK A and
C after ACK B. Both reuse the same admitted, qualified exact TCP attachment.
These times construct causal ordering, not throughput acceptance thresholds.

## Run and result

```sh
CARGO_BUILD_JOBS=3 CARGO_TARGET_DIR=./target cargo test --release --locked --features lab-diagnostics --lib request_owner_rate_refreshes_after_admitted_ -- --nocapture
```

Release compilation took 3m21s; both tests together ran in 0.31s.

| Case | Bootstrap samples | Subsequent samples | Refresh on each ACK | Final Product debt |
| --- | --- | --- | --- | --- |
| Pipelined | 1 | 2, 2, 2 | yes, no, no | 0 |
| Staged control | 1 | 2, 3, 4 | yes, yes, yes | 0 |

`request_owner_rate_refreshes_after_admitted_pipelining` fails exactly at the
second subsequent cohort: its epoch observation timestamp remains the prior
ACK, 51.213 ms earlier. The staged control passes. Every exact ACK-progress,
qualification, proof, no-acquisition-Owner and final zero-debt assertion passes
before that failure. Replaying the final ACK releases no additional progress
and changes no epoch in either case.

Thus real request admission permits the overlapping assignments, exact release
is correct, and the moving post-ACK fence rejects their numeric refresh. This
is not a forged receipt, pre-bootstrap authority shortcut or missing data ACK.
The paired-cohort correction remains unimplemented at this milestone. This
test does not attribute server-to-client download stalls or quantify speed gain.

## Fixture failure kept distinct

The first compile/run (3m08s, tests 0.00s) failed both cases before sampling:
the helper expected Product as the first dequeued command. Normal path-open
publication advanced the proof generation, so ordinary preparation legitimately
queued a replacement `PathProofData` first. That is not a Product defect.

The helper now recognizes only that permitted command, asserting exact path,
current proof ID and nonempty payload, then asserts exact Product stream,
offset and length. Unexpected frames/commands or absent Product still fail.
No runtime change or blanket command drain was used. Independent read-only
review found no hidden proof/sample injection or authority bypass.
