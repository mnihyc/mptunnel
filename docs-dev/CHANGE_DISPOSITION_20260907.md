# Decisive change disposition

2026-09-07 04:20 UTC. User-requested pruning of the existing change inventory.
No new defect search, scheduler or congestion experiment is authorized by
this disposition. Three independent bounded source reviews covered native
reordering, protocol/transport features, and actor/qualification/lifetime
changes. Root checked the already-documented unused model groundwork.

## Decision standard

KEEP requires a reachable observed/tested failure and a concrete benefit from
the actual mechanism, with its cost and limits stated. A component may be
justified without claiming that the complete tunnel is faster everywhere.
REMOVE applies to rejected unused groundwork or an optional implementation
whose demonstrated benefit has not justified its complexity. Removal is not
retroactive proof that the underlying problem was fake or that the candidate
caused every adverse measurement.

## Remove

1. **Unconsumed exact-action score and static ranking prototype.** It existed
   only under `cfg(test)` and had no runtime caller. Its proposed sustained
   migration was rejected by the existing non-starvation counterexample.
   Remove its formula, canonical sorting/incumbent machinery, helper constants
   and ten prototype tests from `src/model/advisory_score.rs`. Keep the live
   timing scope/epoch types and their validation. This removes maintenance
   and misleading groundwork; it is not a runtime throughput optimization.
   The historical source is in git and T03_ADVISORY_SCORE preserves the model
   critique. There is no need to resurrect it to complete this release.

2. **Stateful relative ACK dictionary from the active candidate.** Its byte
   savings are real: one protected sequence shrank from about 84.7 to 6.0 KB,
   and a constrained-return-path experiment improved. But it adds ordered
   encoder/decoder state, cancellation/commit rules and reconstruction work
   across TLS, Noise and HTTP/3. The prescribed mirrored upload comparison
   gave 81.5 to 44.9 Mbps and 5.63 to 13.65-second gaps. This is insufficient
   practical non-regression evidence to justify that optional complexity.
   Remove its format bit, contextual decoder, state/plumbing, specific tests
   and RFC clauses as one feature. Preserve stateless ACK packing, complete
   ACK meaning, paired repair binding and terminal/interlock corrections.
   ACK_RELATIVE_REMOVED_20260907.patch records the exact removed feature for
   future analysis. Additional compression is deliberately sacrificed; no
   speed improvement or codec-corruption finding is inferred from removal.

Ready-feedback batching, the absolute-delay reordering candidate, the
wrapper-less actor candidate and the three N1 probe-policy experiments were
already removed. They are terminal rejections, not future fix obligations.

## Keep, with concrete benefit and cost

| Mechanism | Real defect / direct benefit | Cost and limitation |
| --- | --- | --- |
| Bounded QUIC receive history plus wider short packet numbers | Authenticated reordered originals were discarded; 4,948 exact trace matches and actual encrypted packet tests establish it. The history/encoding pair preserves delivery and duplicate rejection within its declared range. | 4 KiB per packet-number space and one additional header byte where one-byte numbers were used. Finite history is not unlimited reorder tolerance. |
| Evidence-learned excess-delay reordering, exact packet lifetime/classification and same-ACK RTT order | Repeated successful late originals kept producing false loss; evidence was also retired before native ownership ended. Current tests cover true loss, duplicates, lineage and rising/falling RTT. Jitter-only observed throughput improves 0.585 to 185.313 Mbps with short gaps and complete echoes. | Learning reordering delays some real-loss declarations. No universal recovery guarantee. Preserve native congestion ownership; no newly raised gain, compensation or window. |
| Product qualification projection | A qualified QUIC output was falsely projected as startup, rejecting an estimated 71-ms action while TCP's 2.6-s candidate received 11.47 MB. The one-field correction preserves the real qualification owner. | Changes which paths are eligible. It does not make a throughput estimate measured capacity or prove a complete placement policy. |
| Independent mailbox-capacity wake | An accepted ACK waited 12.173 s because a full Product mailbox was conflated with a writer-owned barrier. PendingMailboxFrame preserves one exact frame and independently observes its recipient while the native write remains pinned. | One retained pending frame plus a boxed reservation future/send closure on the full-mailbox slow path; no extra queue or timeout. Real interlock tests preserve input order, cancellation, write-wins and terminal barriers. |
| Cyclic Product service including executor cooperation | An always-populated input mailbox disabled ready source/dispatch indefinitely. The corrected arbiter gives each continuously-ready class service within three completed selections and respects the executor's existing budget. | Long handlers still take time; this is not a wall-clock latency guarantee. The first uncooperative candidate caused a 61.5-s stall and is not restored. |
| Paired native QUIC repair stream | A frontier repair had 49.326 MB of earlier native-stream work ahead of it. Actual native tests establish that same-stream priority cannot overtake it; the separate stream removes that ordering dependency. One ordinary download gap improves 2.689 to 0.384 s. | Two native streams per logical attachment, with shared physical congestion/credit. Binding registry, pair-open serialization, queue-charge transfer and `2N+1` stream allowance belong to this feature, not independent optimizations. |
| Stateless packed ACK ranges | Repeated fixed-width ranges caused real reverse-path work. Encoding preserves identical complete frames and is selected only when smaller. One ordinary mixed comparison reduces reverse traffic from 348 to 165 MB. | Integer coding costs CPU; fewer bytes do not guarantee low latency. No per-stream dictionary, new feedback cadence or dropped negative ACK authority. |
| Terminal/recipient lifetime corrections | Clean companion EOF cancelled ordinary exchange; client input retirement cancelled still-owned FIN/DETACH; server completion preceded final reconciliation. Exact tests and 1,941 mixed plus 64 single-mode requests now reclaim all owners. | Fixes include maintenance of our experimental companion. Preserve independent half-closes and real failures; this does not attribute the sole cause of the uncaptured deployed RAM incident. |
| Recovery overlap/frontier scan corrections | Actual profiles and operation counts locate quadratic repeated work. Snapshot/sweep implementations preserve the same byte sets and decisions with bounded work. | Reduces CPU and demonstrated drain cost, not every network-limited stall. No percentage gate or new copy permission. |
| Restart, no-target reset, lineage/journal ownership, exact accounting and diagnostic corrections | Previously documented reachable reset/recreation/retention/accounting failures have isolated tests and ordinary completion or ownership evidence. Keep the established fixes rather than reintroducing them to change path preference. | Each retains its bounded claim. Honest diagnostics do not increase capacity; correct restart or finite journal ownership does not alone establish global speed or sustainability. |

Negative ordinary evidence remains part of these KEEP decisions: mailbox-only
mixed uploads were about 237 to 169 and 260 to 165 Mbps; the cooperative actor
comparison was 228 to 211 Mbps with 2.28 to 3.13-second gaps; qualification-only
was 16.69 to 8.05 Mbps with an echo failure. Stateless packing's first byte-saving
comparison worsened echo p95 from 548 to 910 ms; the next comparison reversed
latency ordering. These are not ignored passes or conclusive single-feature
causal rankings. They prevent a speed-win claim. The retained benefit is the
separately proved progress/qualification invariant or reduced physical bytes;
current interaction/timing failures remain in the existing acceptance gate.

The native 16-recovery aging reference is
[RFC 8985 section 6.2](https://www.rfc-editor.org/rfc/rfc8985.html#section-6.2),
not a newly selected lab value. That reference does not establish that this
QUIC implementation is literal TCP RACK or universally optimal. Its exact
learning and ownership choices are supported separately by native tests.

Retain the live typed rate/timing provenance: it has actual producers and
consumers. Do not restore the rejected startup-only replacement for dynamic
TCP scalar evidence. Research-only contention factors, all-stage receipt
ledgers, static allocation and statistical no-flap proposals remain outside
the implementation. They are not SEEN defects to be patched automatically.

## Verification and acceptance boundary

The prototype removal passes 29 focused timing tests, including live producer
scope/epoch/exhaustion controls. Its live timing implementation is preserved;
the removed algorithm was test-only. Changed-file formatting is checked.
The initial full-tree formatter reported three pre-existing formatting-only
differences in `src/transport/quic/tests_congestion.rs`. They were mechanically
formatted in269c843; full-tree formatting and ordinary diff checks now pass.
No test condition or runtime behavior changed with that formatting.

Relative-codec removal is complete. The release library test build succeeds
without compiler warnings in 3m09s. All67 selected existing protocol,
protected-stream, H3, repair and terminal guards pass in0.12s, including the
five stateless packing tests and six terminal-retirement tests. Exact command,
test names and output are in PRUNING_CHECKS_20260907. The relative-only archive
passes git apply --check and an independent in-memory replay reconstructs all
nine pre-removal snapshots exactly. No obsolete dictionary test was rewritten
to bless another behavior; those six feature-specific tests were removed with
the feature. The current retained runtime contains no contextual ACK encoder,
decoder or basis fallback.

Retained mechanisms are now intermediate source checkpoints: d268aa4 native
reordering, b7abd78 qualification, 59fbd22 cyclic service, and b9a1600 the
coupled carrier I/O/repair and stateless wire composition. These record the
already-reviewed mechanisms, not newly introduced changes during this pass
or release acceptance. The ordinary application build succeeds in1m31s and
is frozen at `./.tmp/reflection/bin/pruned-20260907/mptunnel`. Existing opt-in
lab diagnostics remain off during ordinary execution. It has not yet been
performance-tested; do not transfer old results to this new composition.

Current finite performance still has multi-second gaps. KEEP means justified
component retention, not release acceptance. The global next step remains a
same-condition released/candidate comparison and attribution of the existing
ordered-delivery failures. No new parameter or model layer is introduced by
this pruning pass.

Evidence sources: PERFORMANCE_REFLECTION_20260907 and its referenced exact
traces/tests; REVIEW_AND_PRACTICAL_ACCEPTANCE for the historical SEEN/UNSEEN
disposition; QUIC_REPAIR_ORDERING_MODEL; NATIVE_PRODUCT_QUALIFICATION_CLOSURE;
ACK_RELATIVE_UPLOAD_CHECK_20260907; FEEDBACK_PACKETIZATION_COMPARISON_20260906;
TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907.
