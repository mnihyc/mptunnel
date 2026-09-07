# Request cohort correction: incomplete ordinary upload pair

Recorded 2026-09-08 00:31 +08:00; observations 2026-09-07 07:47--07:50 UTC.
Category: practical acceptance withheld; bounded causal evidence, not release
or a measured causal performance gain. Applies the mandatory
[performance method](PERFORMANCE_METHOD_AND_LESSONS.md).

## Comparison and disposition

Baseline runtime `11d6f3a` versus its uncommitted request cohort clock correction,
whose exact scope and 23 focused GREEN checks are in
[the correction](REQUEST_COHORT_CLOCK_CORRECTION_20260907.md). Frozen ordinary
executables: `./.tmp/reflection/bin/late-startup-scope-20260907/mptunnel` and
`./.tmp/reflection/bin/request-cohort-20260907/mptunnel`. Native/event diagnostic
flags were off. The second run is a separate random realization, not the same
packet-loss sequence replayed or a causally conclusive one-pair regression.
The exact unaccepted source/test/RFC diff is preserved in
[the candidate patch](REQUEST_COHORT_UNACCEPTED_20260907.patch); retaining this
evidence is not promoting it into accepted runtime.

| Observation | Baseline | Candidate |
| --- | ---: | ---: |
| Target-confirmed bytes | 109,313,995 | 133,561,732 |
| Locally accepted bytes | 168,296,448 | 191,758,336 |
| Probe duration | 85.895780 s | 85.563214 s |
| First target confirmation | 0.441956 s | 0.491257 s |
| Greatest confirmation gap | 1.004911 s | 2.021790 s |
| Greatest local write gap | 7.147240 s | 2.042942 s |
| Transfer completed | no | no |

Both reach the existing runner's 85-second observation guard. Teardown closes
the tunnel before the sink's terminal acknowledgement, producing the recorded
`upload sink closed before terminal acknowledgement` error. This is not an
independent spontaneous Product reset or a proof of permanent noncompletion.
The roughly 59 MB remaining in each run is nevertheless a real observed
settlement backlog. A quotient of partial confirmed bytes over censored time
is not a completed-transfer rate or an accepted 22.7% improvement.

Stop promotion here: no subsequent QUIC/mixed candidate cells and no controller,
timer, resource limit or topology change. Correct numerical refresh is not
enough to accept composed timing.

## Unchanged experiment

TCP-only: three default native carriers to one server endpoint, not three
independent links. Same owned Docker routed profile and runner `./.tmp/reflection/run.py`;
whole-profile mirroring puts the main impairment on upload. Each direction has
a 500 Mbps configured link; main delay/jitter 70/20 ms, return 30/5 ms. Main
five-second loss epochs are `[3,8,5,6,10,3,5,8]` percent (mean 6%); return epochs
`[1,2,0.5,3,2,0.5,1,2]`. Main QoS is 10 Mbps at 15--25 s; UDP-only blackhole
30--33 s does not disable these TCP carriers. Source load is 40 s, not a claim
that intermediate queues finish at 40 s. FIFO override is off. No initial-rate
override, loss/jitter removal, host shaping, native trace or overlapping build.

Raw probes and all 86 management/socket/router samples per run are preserved
in [the archive](REQUEST_COHORT_ORDINARY_20260907.raw.tar.gz), with paths under
`results/tcp-combined-up-request-cohort-{control,candidate}-0907/`.
The upload probe discards its interval map whenever terminal accounting fails
(`lab/bulk_upload_probe.py`, `ack_accounting_valid` and `interval_metric_fields`).
Both preserved confirmation-bin arrays are therefore empty. Management counters
cannot reconstruct those missing confirmation times or locate the reported
2.021790-second maximum exactly. No fabricated curves are supplied.

## What the counters actually prove

Source inspection establishes these distinct boundaries:

- Client management `io.to_peer_bytes`: successful reads from the local source
  socket, via `ObservedProductIo::poll_read` and `relay/control.rs`. Not native
  transmission or exact carrier allocation.
- Server management `io.from_peer_bytes`: successful ordered writes to the
  target socket, via `ObservedProductIo::poll_write` and `relay/server.rs`.
  Not raw Product receipt/reassembly or target application consumption.
- Client `ss` ACK/Send-Q/notsent: native TCP carrier byte domain, including
  framing/control/copies. Not additive with Product flight/debt.
- Sink confirmation: cumulative target consumption observed back at the
  client. Return-path and scheduling delays remain included.

At the last service sample, control source/target counters are
168,296,448/108,986,315 B; candidate 191,758,336/133,496,196 B. Final actual
client Send-Q totals are 7,703,048 and 21,817,582 B respectively. Thus not all
unconfirmed work can be called native send-queue backlog. The source/target
difference repeatedly equals 64 MiB; that alone is not grounds to lower the
resource allowance or treat resource permission as required placement.

Candidate session `6493328813790544979` maps path/instance `0/3` to client
`:44630`, `1/2` to `:44628`, and `2/1` to `:44644`, all toward server `:7443`.

Two useful bounded observations survive independent read-only audit:

1. **Stale telemetry is not a stopped socket.** Path `0/3` management retains
   ACK 76,432,366, sample time 27,551,408 us and queue 32,807,912 B throughout
   samples 28.005252--50.007632 s. Actual `ss` ACKs rise 76,834,670--87,372,274 B
   and notsent falls 32,423,060--21,999,824 B. A large native queue is real;
   the apparent ACK freeze is not. At the final sample this socket still has
   Send-Q 20,085,116 B and Product debt 20,447,232 B, overlapping inventories.
2. **One carrier's ordered delivery pauses while siblings progress.** At
   samples 39.006448--40.006565 s, target writes remain 108,002,692 B. Server
   `:44628` received remains 23,260,439 B; its client's SACK count rises
   340--692 while cumulative ACK moves one MSS. Other server sockets receive
   another 341,728 and 251,904 B. By 41.006694 s the former socket and target
   writes both advance. This is consistent with a blocked native prefix, but
   does not establish which carrier owns the missing logical Product range.

## Next exact question and falsifier

Map one actually stalled upload's first missing logical range to its original
carrier, any repair, receiver frontier and target-write boundary. Distinguish
slow native ordered service from late/ineffective MPP placement or recovery,
and from an already-received prefix held by a Product actor or target writer.

If the putative owner supplies that exact prefix only when the frontier
advances, the native dependency is supported; if it was already available at
the receiver during the plateau, that explanation is falsified. A high rate
estimate, aggregate sibling ACK progress or finite total backlog cannot decide
this. First check whether existing diagnostic events provide the mapping;
collect only missing discriminating observations, not a new harness or another
broad matrix. This artifact does not assign the slower gap to the sampler.

## Follow-up: exact prefix discriminator, 2026-09-08

One unchanged-candidate, unchanged-profile TCP upload capture used existing
events, with no rebuild or instrumentation/probe edit. Its raw evidence is
[archived here](REQUEST_PREFIX_DIAGNOSTIC_20260908.raw.tar.gz), under
`results/tcp-combined-up-request-cohort-prefix-diag-0908/`.
Enabled events: `sender_service_decision`, `server_receive_hole`,
`server_receive_delivery_stall`, `stream_ack_received`, `tcp_sender_metrics`,
`client_sender_enqueue`, `request_path_stale`, `client_path_frame_error`.
Synchronous diagnostics make this causal evidence, not a throughput comparison.
Again censored at the existing guard: 97,189,623/116,588,544 confirmed/accepted
bytes in 85.616251s; confirmation gap 4.171412s. Products/probe are stopped;
three origin services remain. No subsequent experiment follows this capture.

### Exact observed episode

Logical stream 0; physical mappings remain stable throughout:
client path index 0/1/2 maps to instance 2/1/3 respectively.

| Boundary | Timestamp, Unix ms | Evidence |
| --- | ---: | --- |
| Original `[21364471,21430007)` published on TCP index 1 | 1788798709118 | `client.log:2051`, post-command-publication event |
| Last reassembly progress, frontier 21364471 | 1788798715759 | `server.log:3091` |
| Next reassembly progress, frontier 21430007 | 1788798719935 | `server.log:3619`, explicit 4,176,371 us gap |
| Covering repair | none | Complete client publication trace has none |

The original was published 10.817s before its reassembly release. During the
4.176371s gap, 318 ACK events release 6,946,816 B of other received work and
8,650,752 B of new originals are published across the three carriers. Every
ACK evaluation reports an alternative available, persistent repair not ready,
and zero queued gap repairs. Matching target writes remain 21,364,471 B in
service samples 26--29 s. The receiver's reordered bytes grow from 4,587,520 to
11,665,408 B. This disproves missing source assignment and a completely stopped
Product actor; it does not divide the original's downstream delay precisely.

The first hole in the same capture was different: original `[131072,196608)`
on index 0 already had a partial copy on index 1 published 21ms before server
hole observation. Later stalls must not be generalized to "repair never ran."
Persistent nonempty reorder state likewise does not mean 85s without delivery.

### Proven recovery-evidence mismatch

All 27 complete ACK events have greatest end at most **H = 11,009,783**;
the last is at 1788798692173. A larger complete observation cannot be hidden
by the idempotency early return: `AuthoritativeStreamAckSnapshot::subsumes`
explicitly rejects an incoming horizon greater than stored H. The synchronous
ACK update then reaches the recorded evaluator. Partial positives are correctly
clipped to H only in this negative-authority snapshot; actual cache/flight ACK
release and positive mux frontier advance independently.

Thus, at stalled positive frontier **F = 21,364,471 > H**:

1. Authoritative ACK-gap recovery cannot select the current missing range.
2. Retained tail recovery also requires stored ranges exactly `[0,F)`. Its
   stored prefix is bounded by H, so it returns before original age, eligible
   alternate or service admission. Source closure does not bypass that guard.

The negative-horizon rule is intentional and must remain: a partial ACK's
omission is not loss. `3353d7d` introduced retained negative authority and
`5e1ace6` corrected its horizon to receiver evidence rather than local sent
extent. The composition error is using this narrower snapshot as a prerequisite
for separately valid positive retained-owner fallback. RFC8.3 explicitly keeps
retained ownership above H valid for Product recovery, without declaring loss.
This is not a stateless packed-codec defect or proof that every copy is useful.

Symbolic case: complete `[0,1)`, then partial `[1,2)` and `[3,4)`, leaves H=1,
F=2 and exact retained `[2,3)` on A. Even after A's original recovery interval
and with qualified B available, the equality guard rejects. Correcting only
sparse publication cannot solve the general case: legal multi-frame snapshots
are all incomplete. A focused real-owner RED/control is the next stage before
changing fallback authority. Preserve native ownership, exact copy suppression,
ranking and service bounds; delayed ACKs/shared contention are the adverse
case because a speculative copy may arrive unnecessarily and cost latency.

### Focused retained-tail RED/control

2026-09-08 00:55 +08:00. No recovery implementation change.

```sh
CARGO_BUILD_JOBS=3 CARGO_TARGET_DIR=./target cargo test --release --locked --features lab-diagnostics --lib retained_completion_tail_ -- --nocapture
```

Optimized compilation: 3m09s; two tests: 0.20s, one expected RED and one GREEN,
no ignored cases. The initial compile found two usize/u64 comparisons in new
assertions; correcting those test-only types is not a Product failure.

Both cases commit three 4 KiB cache chunks through ordinary Product admission
and actual carrier command publication. A receiver accepts the first two and
produces legal ACK frames; validation and Product/cache release derive F=8192,
N=12288 and exactly 4096 B retained on the original owner. They wait until the
actual original assignment plus unchanged recovery interval. The alternate's
rate is explicitly fixture-seeded; the real fallback selector must verify
that exact target and sufficient service before the tested entry point.

| Case | H / F | Result |
| --- | --- | --- |
| `retained_completion_tail_survives_partial_ack_frontier_beyond_horizon` | 4096 / 8192 | RED: no recovery queued, no capacity/model-publication block |
| `retained_completion_tail_with_aligned_complete_ack_horizon_control` | 8192 / 8192 | GREEN: exact retained range and target queued |

The two setups differ only in whether the second receiver ACK is partial or
complete. Neither injects H/F, cache release or original flight ownership.
This proves the sender-contract eligibility defect, not actual copy delivery
or a timing improvement. Independent audit confirms the distinction: the
fixture directly uses a legal delta for contiguous receiver progress, whereas
the real TCP sparse producer uses deltas while reordering persists. The live
capture above supplies that producer reachability; the fixture alone does not.

The isolated test-only diff is
[preserved here](REQUEST_RETAINED_HORIZON_RED_20260908.patch). It remains an
intentionally failing test in the working tree, not an accepted runtime patch
or a CI/release pass. Next, correct the retained-owner fallback contract without
widening negative ACK authority, then preserve this RED/control and compare
ordinary timing with the sampler candidate kept independently attributable.

## Isolated retained-frontier correction — 2026-09-08 01:20 +08:00

The sampler source/RFC overlay is shelved; its exact patch and executable are
retained above. The recovery correction has no sampler dependency. Retained
recovery now reads F/N from the positive mux/cache state, while active source
and partial feedback cannot disable an aged exact original frontier. Negative
H and authoritative-gap/path-withdrawal consumers are unchanged.

The two completion fixtures now pass, along with an active-source test using
four ordinarily committed2KiB chunks and receiver ACKs for chunks0,1,3. H=2KiB,
F=4KiB; only the retained2KiB middle hole is eligible after its original clock.
Fresh-owner, queued duplicate, ACKed suffix and final release checks pass. This
uses the shared production actor predicate and sender method, not the full
actor select-loop integration. The prior live trace supplies reachability.

Before accepting the broadened branch, review requested a three-output control:
actual original A, actual accepted B copy, then measured vacant C while B's
immutable deadline D remains future. Without a shared-range guard, it fails:
`RequestCompletionTailEnqueueOutcome { queued: true,
blocked_for_carrier_capacity: false, waiting_for_path_model_publication: false }`.
Target avoidance excludes B but does not suppress another same-range copy on C.
The correction uses the existing exact-frame suppression accessor over all
scoring frames after owner age and before target modeling. It retains full
attachment membership and the existing D wake. The same fixture now blocks C
before D, permits it after D, and preserves B's outstanding Product ownership.

All21 distinct focused tests pass in1.74s. Exact invocation after the release
lib test build (features `lab-diagnostics`, events off):

```sh
target/release/deps/mptunnel-3a813700b0d8f97b \
  retained_completion_tail_ active_request_retained_hole_ \
  retained_frontier_suppresses_ completion_tail_ client_live_tail_ \
  ambiguous_prefix_ack committed_request_copy_deadline accepted_request_copy \
  exact_recovery_copy authoritative_ack_snapshot complete_ack_negative_authority \
  retained_authoritative_ack request_live_tail_uses accepted_copy_wake_ \
  --nocapture --test-threads=3
```

Current closure plan records original intent, tradeoff, ordinary pair and
unchanged global acceptance. The existing request wake may wait for the later
successor floor absent other events; this correction does not rewrite that
policy. No claim that eligibility alone removes the original measured stall.
