# Performance reflection and stop decisions

Date: 2026-09-07 03:52 UTC. Category: requested retrospective and bounded
decision. No runtime edit, new build or experiment was performed for this
review. This supersedes any implication that component fixes establish an
accepted performance improvement. CURRENT_CLOSURE_PLAN remains the global
gate ledger; this is not a new SEEN/UNSEEN inventory.

## Answer

The current composition has not earned release acceptance. Useful defects
have been corrected, and one severe reordering collapse has a substantial
measured improvement. Nevertheless, the user's main objective—fast, stable
ordered service, including short requests and recovery—is not achieved.
More completed fixes do not establish that the product is better overall.

There is no current released-binary versus final-held-composition comparison
that establishes either overall improvement or overall regression. The actual
released `d1a99ad` versus `7189e69` routed jitter-only comparison was 0.670
versus 0.738 Mbps: that collapse already existed in the release. This excludes
the intervening corrections as its introduction, not every earlier change or
every subsequent candidate as a regression source. Historical clean-link
300–400-Mbps results are not comparable to a run containing a 10-Mbps rate
cut, asymmetric loss, deep packet reordering and a UDP outage.

## What the changes bought—and what they did not

| Change family | Intended benefit and actual evidence | Cost, failed expectation and decision |
| --- | --- | --- |
| Native reordering, receive history and packet evidence | Stop repeatedly treating successfully reordered originals as genuine lost delivery. Actual packet/ACK counterexamples and a jitter-only comparison improve 0.585 to 185.313 Mbps, with a 0.097-s gap and 50/50 echoes. | Strongest demonstrated speed lead. Changes native recovery behavior and history lifetime; combined QoS/loss/outage timing remains poor. Retain as an isolated candidate, not a claim of general recovery success. |
| Same-ACK RTT transaction | Learn reordering excess against the RTT published by that same ACK, avoiding double counting. Exact encrypted RED/GREEN; stationary QUIC observation improves 157 to 177 Mbps with similar 0.35-s gaps. | Combined controls still have seconds-long pauses. A local arithmetic/order correction cannot remove every queued or missing-prefix dependency. No new gain or threshold follows. |
| Recovery scan work | Remove repeated overlap/frontier scans. Profiles and deterministic work counts identify real quadratic work; upload drain improves in recorded comparisons. | Preserves exact decisions and is useful under the demonstrated load. It cannot improve a network- or ordered-prefix-limited interval just because it saves CPU. Overall throughput signs were not uniformly positive. |
| Mailbox wake and actor service | An independent mailbox-capacity wake was masked by a pending native write; input draining could starve ready work. Real interlock and service tests justify removing those dependencies. | Revised actor cooperation followed an unsuccessful first candidate. These changes affect timing across the pipeline; component passes are not evidence that every upload or latency tail improves. Keep composition acceptance open. |
| Paired QUIC repair stream | A repair had 49.326 MB of preceding work in its native ordered stream. A separate ordering stream removes that particular prerequisite without extra physical capacity. | Adds native half-stream lifetimes and a wire change; shared connection and physical queues remain. Crucially, its implementation introduced clean-EOF cancellation of the ordinary terminal exchange. Part of the recent work repaired our own experiment. It is not an accepted speed feature. |
| Terminal ownership/reconciliation | After successful requests, server completion could miss final reconciled state; retiring a client recipient could cancel still-owned terminal writes. With repair EOF corrected, 1,941 mixed plus 64 single-mode requests reclaim all owners, remaining clear after 68 s quiet. | Real finite sustainability benefit. The earlier held composition retained 1,571 owners after 1,932 successful requests. This does not identify the sole cause of the user's uncaptured deployed RAM incident, and it is not a throughput gain. Preserve legitimate half-closes and real failure handling. |
| Packed/relative ACK representation | Preserve complete ACK meaning with fewer return bytes. Packed ACKs roughly halve reverse traffic; relative encoding improves a clean 500/10-Mbps mixed observation from 46 to 175 Mbps. | Still a 1.67-s gap and about 1.34-s loaded p95. A mirrored comparison worsens from 81.5 to 44.9 Mbps and 5.63 to 13.65-s gaps. Random realizations do not prove the codec caused that difference, but they fail non-regression acceptance. Hold, do not silently ship the wire change. |
| Ready-feedback batching | Reduce native protected-record/write cost without dropping logical feedback. | First improvement did not survive the paired repeat: 292 to 302 Mbps, essentially unchanged loaded latency and a worse gap; the candidate also lost echo service. Removed. A byte-saving unit test was insufficient justification to retain it. |
| Restart/reset, exact copy ownership, diagnostics | Prevent specific incorrect recreation/reset/retirement actions and distinguish measured delivery from retained capacity. Exact branch tests and ordinary completion controls support these corrections. | Preserve their bounded correctness claims. They do not make an estimated 400 Mbps into delivered 400 Mbps, or establish that unrelated slow downloads are resolved. |

The full commit-by-commit history and older SEEN/UNSEEN dispositions remain in
[the evidence review](REVIEW_AND_PRACTICAL_ACCEPTANCE.md) and
[the earlier change reflection](RECENT_SEEN_CHANGE_REFLECTION.md). Disproved
or unused model suggestions do not become mandatory performance work.

## The architectural mistake that still matters

Commit `65edae3`, already contained in v0.4.7 and v0.4.8, correctly removed
prediction-derived hard admission limits. Its response-side change also
removed a placement wait without supplying a replacement ordered-service
decision. Resource permission was incorrectly treated as an obligation to
dispatch immediately. These are different contracts.

A free slow TCP writer can consequently receive an original prefix while a
faster QUIC writer is briefly busy. Later QUIC suffix delivery cannot release
that prefix. Exact observed TCP frontiers required 2.940 and 3.248 seconds;
another delayed frontier was QUIC-owned, so a permanent TCP penalty would be
an unjustified shortcut. Restoring the old wait or deleting startup treatment
alone did not solve the complete timing problem. Neither larger BBR gains nor
another eligibility limit replaces ordered placement and recovery.

For byte arrivals A(u), ordered completion through x depends on the latest
arrival among all bytes through x, not the sum of carrier bandwidth estimates.
This distinction should have constrained the design before accepting changes
that maximized queue admission. Current source audits also show that native
QUIC capacity, per-flow TCP ACK goodput and an unknown startup prior are not
interchangeable rates. A new allocator cannot acquire evidence merely by
renaming these values.

## What the latest observations actually locate

The ordinary three-cell composition remains unsatisfactory:

| Case | Whole-run Mbps | Largest delivery/read gap |
| --- | ---: | ---: |
| QUIC download | 108.3 | 5.684 s |
| TCP+QUIC download | 91.4 | 2.351 s |
| Mirrored TCP+QUIC upload | 93.7 | 6.216 s |

Both download echo connections have one actual three-second timeout, followed
by unavailable schedule slots. Upload confirms every accepted byte. These
results are finite observations, not a causal A/B against release or a new
baseline ranking.

During the QUIC-only gap, native counted ACK delivery increases by 5.28 MB
and the router serves 6.27 MB while contiguous application delivery is flat.
The physical backlog represents roughly 4–4.8 seconds of serialization at
the imposed 10 Mbps. This is not a measurement of the missing byte's queue
residence and not proof that the whole gap is unavoidable. It does disprove
the claim that there was no native service. The echo request reached its
target and the target produced its reply; delayed return delivery remains.

The mirrored upload's largest gap likewise occurs during the QoS step, with
64 MiB of Product flight occupied while native ACK and physical service
continue. It does not reproduce the older post-requalification gate as that
interval's dominant cause. Current ordinary snapshots do not reveal the exact
missing native stream offset versus MPP receive frontier. Consequently an
allocator rewrite alone cannot be called this QUIC-only gap's fix.

## Reflection on the process

1. Component tests were repeatedly promoted into broader milestones. They
   establish a specific invariant, not application timing or competitiveness.
2. Too many interacting held native, actor, repair and codec candidates
   accumulated before their individual performance decisions were closed.
   The resulting integration defects consumed time that should have gone to
   establishing the end-to-end benefit of a smaller candidate.
3. Work/byte reductions and an improved average were treated as stronger
   evidence than they are. Benefits depend on the active bottleneck; fewer
   ACK bytes do not guarantee prompt feedback, and packet delivery is not
   contiguous application delivery.
4. Some observations were overinterpreted: live-packet ACK counters are not
   all native ACKs; idle RSS is not a live-owner ledger; per-second buffered
   receipt bursts are not wire-rate measurements. These mistakes caused
   unproductive causal branches. Corrected records do not excuse the cost.
5. The originally useful principle—clean ownership rather than numerical
   tuning—was insufficiently coupled to performance obligations. Correct
   resource accounting still needs a practical ordered-service policy.

## Deterministic next decision, not another expansion

- Freeze the current composition and record a same-profile comparison with
  the released executable before claiming improvement or regression. Reuse
  the existing runner; no new benchmark framework or parameter search.
- Identify the exact missing-prefix/native-stream event in the existing
  QUIC QoS failure. Keep the separately demonstrated mixed slow-path placement
  failure distinct. Do not infer a new controller or allocator fix from an
  aggregate native rate or from a filled resource envelope.
- Change only the responsible model owner, then test its previous failure
  and the affected ordinary timing control against the frozen reference.
  If a held repair/codec feature does not earn its practical benefit, exclude
  that feature as a unit rather than treating its maintenance as progress.
- Resume the existing asymmetric/independent-link, upload/download,
  cold/warm/browser and raw/Xray/H2 gates only after the targeted comparison.
  Preserve complete timing, completion and ownership evidence. No release or
  README superiority claim follows from this reflection.

Evidence: REORDERING_PERFORMANCE_DIAGNOSIS;
QUIC_REORDERING_EXCESS_DELAY_MODEL; ACK_TRANSACTION_RTT_CONTROLS_20260907;
ACK_TRANSACTION_RTT_STEADY_20260907; ORDERED_REPAIR_SERVICE_BOUNDARY;
REPAIR_HALF_CLOSE_RETENTION; QUIC_PRODUCT_RECIPIENT_RETIREMENT;
SERVER_TERMINAL_RECONCILIATION; TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907;
ACK_RELATIVE_UPLOAD_CHECK_20260907; FEEDBACK_PACKETIZATION_COMPARISON_20260906;
TERMINAL_RETIREMENT_TIMING_CONTROLS_20260907;
QUIC_QOS_ORDERED_DELIVERY_ATTRIBUTION_20260907;
MIRRORED_UPLOAD_TIMING_ATTRIBUTION_20260907;
RESPONSE_PLACEMENT_RATE_SCOPE_AUDIT_20260907;
ALLOCATION_DISCOVERY_OWNER_AUDIT_20260907.
