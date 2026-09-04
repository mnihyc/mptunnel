# v0.4.7 finite closure plan

Status: internal release-candidate contract. Production baseline `93e6284` is
**release-RED**: hard percentage recovery admission, unresolved rate authority,
32 library-test failures, and incomplete runtime acceptance remain. This
document does not claim that the candidate is accepted.

## Purpose

v0.4.7 MUST terminate as either an accepted release candidate or an explicit
rejected candidate. It MUST NOT grow whenever another experiment suggests a
possible improvement. The complete authorized scope is the promoted SEEN
ledger below and in `SEEN_ACCEPTANCE_LEDGER.md`.

Deterministic means that a production change follows from a stated invariant,
a symbolic or fixture-level counterexample, one causal correction, and an
exact post-change proof. It does not mean that independent random-loss runs
produce identical throughput. Runtime evidence is a finite release gate; it
does not choose constants or define the model.

All measured rate, RTT, loss, freshness, confidence, quality, recovery,
path-choice, and extra-traffic percentage thresholds are soft evidence. They
MAY influence ranking, probing, confidence, cost, or reconsideration. They
MUST NOT become a hard native-rate ceiling, an ordinary-Product or recovery
admission cap, or a permanent path ban. Finite protocol structure may still
prove a cardinality bound, such as one exact copy per distinct native ordering
domain in one unresolved-frontier epoch. Explicit memory, queue, session, and
connection maxima remain operator resource contracts; they are not inferred
network-performance thresholds.

## Reflection on the delay

The elapsed time did not come from one unusually difficult compiler or lab.
It came from four process errors:

1. Focused component GREEN was reported as end-to-end acceptance.
2. A favorable random realization was promoted while adverse repetitions were
   still possible under the same condition.
3. Aggregate goodput hid first-body delay, upload collapse, Product read gaps,
   and loaded interactive latency.
4. Several layers were changed or discussed together, so later failures could
   not be attributed to one invariant.

Those errors made an otherwise finite defect list appear renewable. The
correct rule is that a component commit is only a checkpoint. Release
acceptance occurs once, after the complete frozen matrix.

## Scope lock and terminal outcomes

The symptom set is now closed. A causal detail needed to explain a listed
symptom stays inside that symptom; it does not create a new work package. An
independent finding discovered after this checkpoint is recorded for the next
candidate. If it invalidates v0.4.7 safety or protocol correctness, v0.4.7 is
rejected rather than enlarged.

Every atomic transaction ends in exactly one state:

- **FIXED**: an exact pre-change RED, one invariant correction, focused GREEN,
  independent audit, and its affected ordinary runtime gate all pass;
- **STALE/UNREACHABLE**: the reported premise does not exist in the current
  wire/runtime model, so only its fixture or internal document changes;
- **MODEL-CONSTRAINED**: the implementation matches the justified model and
  the requested property is impossible under the stated resources; this
  rejects the release if the property is an acceptance requirement; or
- **PATCH-REJECTED**: the proposed correction violates an existing invariant
  or fails its affected gate, so it is not accumulated with another tweak.

There is no fifth state called “run it again until it looks good.” Numeric
network thresholds remain hints; the finite number of validation cohorts is a
review procedure, not a runtime cap.

The no-new-scope rule applies during P1 through P8, not just during final
validation. A newly exposed fact may remain in the current transaction only
when it is necessary to explain that transaction's frozen falsifier and adds
no independent acceptance gate.

## Frozen baseline and suite inventory

The production baseline is `93e6284`. The documentation checkpoint may advance
HEAD but does not change that production identity. On this baseline,
`CARGO_PROFILE_TEST_DEBUG=0 cargo test --lib --quiet` reports 2,184 passed,
32 failed, and zero ignored. This is the fixed input inventory, not permission
to weaken production until old expectations pass.

The failures are assigned as follows; every exact name must end in one of the
terminal states above:

- Admission/authority (6):
  `cold_quic_same_underlay_candidate_uses_bounded_startup_flight_with_an_existing_hole`,
  `cold_tcp_same_underlay_candidate_uses_bounded_startup_flight_with_an_existing_hole`,
  `same_underlay_data_ack_debt_does_not_reconsume_released_carrier_credit`,
  `same_underlay_low_confidence_sender_samples_remain_startup_admissible`,
  `stale_tcp_send_window_cannot_mint_additional_path_exploration_credit`, and
  `tcp_path_uses_product_service_and_connection_reorder_windows` in
  `model::admission::tests`.
- Capacity/startup (2):
  `data_level_budgets_expand_for_bulk_without_second_congestion_feedback` and
  `unknown_path_startup_inflight_uses_default_bdp_not_configured_ceiling` in
  `model::capacity::tests`.
- Typed QUIC-rate provenance (1):
  `runtime::path::quic::estimator::rate_tests::arbitrary_native_ack_bytes_cannot_mint_product_reachability_or_rate`.
- Recovery/tail (5):
  `runtime::relay::control::tests::request_live_tail_uses_the_immutable_shared_epoch_as_its_actor_wake`;
  `failed_original_tail_reinjection_queues_one_bounded_target_flight`,
  `live_original_without_data_ack_does_not_probe_prefix`, and
  `persistent_tail_reinjection_preserves_original_flight_attribution` in
  `runtime::relay::server::tests`; and
  `runtime::sender::request::multipath::tests::retained_tail_uses_only_a_measured_earlier_completion`.
- Attachment/registry identity (9): all current failures in
  `runtime::stream::registry::tests`: `accepted_stream_keeps_its_authenticated_opening_carrier_across_reattachment`,
  `attachment_identity_is_immutable_and_cannot_overwrite_live_response_lane`,
  `late_open_and_closed_output_replacement_inherit_path_evidence`,
  `new_stream_publishes_zero_admission_before_target_owner_runs`,
  `repeated_same_key_reconnect_does_not_wait_for_predecessor_cleanup`,
  `replacement_carrier_does_not_inherit_retired_path_proof`,
  `request_requalification_ack_can_return_on_a_healthy_same_session_sibling`,
  `routed_request_data_updates_feedback_ingress_on_the_same_stream_event_snapshot`,
  and `terminal_session_fence_rejects_existing_stream_reattach_after_carrier_scan`.
- Product confidence/rate floors (2):
  `confidence_and_durable_progress_use_explicit_sample_thresholds` and
  `fresh_product_point_rate_requires_epoch_and_lifetime_floors_for_completion_authority`
  in `runtime::stream::response::snapshot::tests`.
- Fixed/runtime service evidence (4):
  `fixed_output_graduates_fragmented_product_acks_at_exact_sample_floor`,
  `fixed_response_output_learns_product_rate_from_stream_ack_batches`,
  `server_registry_replaced_output_does_not_reuse_cached_bulk_metrics`, and
  `server_response_sender_slices_large_reads_to_service_quantum` in
  `runtime::tests`.
- Outward-TCP sticky selection (3):
  `control_outward_tcp_open_without_terminal_preserves_success`,
  `pre_model_red_outward_tcp_open_does_not_retry_after_sticky_terminal`, and
  `pre_model_red_outward_tcp_open_prefers_sticky_session_terminal` in
  `runtime::tests`.

Definition anchors for those groups are, respectively,
`src/model/tests_admission.rs:1303`, `:1352`, `:2110`, `:2222`, `:2277`, and
`:2327`; `src/model/tests_capacity.rs:97` and `:192`;
`src/runtime/path/quic/tests_estimator_rate.rs:46`;
`src/runtime/relay/tests_control.rs:28`,
`src/runtime/relay/tests_server.rs:2620`, `:2990`, and `:5820`, plus
`src/runtime/sender/request/tests_multipath.rs:2114`;
`src/runtime/stream/tests_registry.rs:947`, `:1254`, `:1574`, `:2106`,
`:2443`, `:2624`, `:3028`, `:3128`, and `:3233`;
`src/runtime/stream/response/tests_snapshot.rs:215` and `:1874`; and
`src/tests_runtime.rs:784`, `:829`, `:889`, `:2398`, `:2538`, `:2642`, and
`:3157`.

T01 first resolves an authoritative-model contradiction introduced by the
non-release `3a6d0ea` checkpoint. RFC 15.1 requires an exact physical-carrier
all-stage ledger and peer service receipts, while the runtime has neither.
Three owner audits prove that current counters cannot be relabelled as that
ledger: they can both double-count one writer and omit QUIC siblings, use a
payload-like unit, and expire at local flush. A separate model audit rejects
the ledger as a Core prerequisite: finite receipt-retained `N^B` imposes
`rate <= 8*N^B/receipt_delay`, while predicted `Z` is still not exact. T01 is
therefore an RFC correction, not a broad runtime rewrite. Exact Product and
resource authority stay separate from an advisory local-feed rank.

After that boundary is authoritative, T03 owns the pure scoring mismatch at
`src/scheduler/policy.rs:165` through `:287`: jitter, loss, confidence,
active-flow division/penalty, and `Suspect` are folded into `S`, while
`src/scheduler/policy.rs:110` adds a queue-only deadband conjunct. Its REDs
mutate every non-score observation, require upward service-time rounding and
identity-stable ties, and use a 20-ms/200-Mbit/s path with three active flows
versus an 80-ms/200-Mbit/s path for 4 MiB. The corrected advisory scores are
178 versus 208 ms; current `C/3` produces about 513 ms and reverses selection.

The post-checkpoint sustained-owner audit narrows that result. The checked
formula is a one-action component, not an allocator: with `A=0`, fixed inputs
and a canonical key select the same path on every action. Writer capacity does
not repair it because a slot reopens after native write/flush, before native or
MPP acknowledgment, and the same path can refill every released quantum. The
64-MiB Product envelope can therefore leave 53.7/134.2/268.4 seconds of
wrong-owner work at 10/4/2 Mbit/s while another ready carrier receives none.
Queue shrink and one-action stop-and-wait are rejected because the existing
queue deliberately pipelines high-BDP service and its charge has a separately
proved late-release invariant. T03 runtime migration is closed as NO-GO; the
typed component remains isolated until dynamic service discovery and one
physical-carrier/direction allocation owner are independently proved.

T04 separately starts from `src/model/admission.rs:398` through `:510` and
`:648` through `:775`. Its RED holds configured `W/P/E`, queue/headroom,
Product debt, position, and payload fixed, changes only inferred ETA across
the completion horizon, and requires resource admission not to flip. This
does not prejudge mixed typed/lifecycle branches or weaken configured window,
queue, memory, session, or connection limits.

## Current exact recovery result

At clean commit `93e6284`, the focused diagnostic replay produced 252.913
Mbit/s aggregate goodput, 0.243076-second first body, and a 1.064233-second
maximum application read gap. The exact blocked Product range was
`[634464056, 634478656)`, 14,600 bytes.

The original was selected for QUIC by a 7.535-ms predicted-completion edge and
entered its native writer in 2 ms. The client first exposed the hole 571 ms
later; the server actor received that evidence 184 ms after the client emitted
it. The existing fallback then waited 99 ms. Three distinct TCP repairs were
accepted sequentially with zero Product-command wait and 1--2 ms writer
handoff:

```text
TCP1 at +0 ms, immutable retry evidence 273.364 ms
TCP2 at +275 ms, immutable retry evidence 252.699 ms
TCP0 at +530 ms, immutable retry evidence 256.246 ms
```

TCP0 delivered the missing frontier 251 ms after its handoff. The observed
gap therefore decomposes into evidence propagation, the owner fallback,
sequential distinct-domain attempts, and native service. No hard admission
limit, sender starvation, actor wake delay, or path-command queue wait caused
this replay.

This is important because it closes the current implementation question: the
captured execution followed the present RFC. It does not prove every recovery
execution correct. More timeout adjustment is not justified by this trace. The
remaining question is a model question: whether sequentially consuming the
available ordering domains can satisfy the already reported recovery and
latency requirements.

## Existing correction verdicts

The following corrections remain isolated checkpoints. None alone authorizes
release.

| Commit | Invariant restored | Why retain it | Required final-tree disposition | Remaining release risk |
| --- | --- | --- | --- | --- |
| `38286aa` | Response OriginalData uses ordinary completion-ranked placement; acquisition state cannot override it. | The prior override could pin fresh work to acquisition rather than the best current Product service. The fix changes no native controller or rate ceiling. | Keep. | Mixed-path composition and flapping remain matrix-gated. |
| `a4679b5` | A configured TCP initial-rate hint remains typed, output-scoped authority. | Replacing an explicit operator hint with unrelated telemetry violated provenance; that exact plumbing correction is retained. | Keep the provenance plumbing; split or supersede the unproven response-rate authority hunks before shipping. | The same commit demoted fresh kernel/peer delivery evidence and made completion depend on `max(C0, qualified Product receipt)`. With no hint it may stay at the 350.75-Kbit/s prior; with a high hint it may resist a QoS downshift. |
| `842a0cc` | Request OriginalData follows the same ordinary completion rule. | Direction symmetry prevents request acquisition state from becoming an implicit route preference. | Keep. | Upload, short-object, and mixed-path composition remain open. |
| `444fb38` | MPP startup/ACK control follows Product acceptance and cannot be blocked by a local application write. | A blocked local sink is not authority to suppress protocol progress in the opposite stage. | Keep. | Full-flow-control and partial-write composition remain open. |
| `0b50e9a` | Live-owner repair cannot renew unbounded duplicate authority. | The pre-change mechanism produced a proven repair flood, so its non-renewal evidence remains valid. | Keep the attribution/non-renewal mechanism only after superseding the hard percentage gate. | Its percentage budget is a hard recovery-admission limit and violates the hints-only rule. |
| `a9450d8` | Configured latency priority reaches the actual Quinn stream. | The prior value was diagnostic-only and therefore could not affect native QUIC scheduling. | Keep. | It cannot preempt already accepted bytes in one native ordered stream; matrix acceptance remains open. |
| `53d9ab5` | One non-accumulating frontier opportunity survives exhausted optional credit, with exact target/epoch/wake ownership. | Its exact target/epoch/wake machinery proves how non-renewal can remain deterministic. | Preserve useful identity/lifecycle pieces only after superseding the hard token and unproved service policy. | The hard over-credit token and sequential percentage-gated service are not final. |
| `dc4853d` | Before fallback, an alternate races predicted owner completion rather than an unrelated authority timer. | The timer determines when fallback authority exists; it is not a delivery estimate. | Keep only with `93e6284` and the final P1 authority model. | The projection is advisory and aggregate; it cannot prove exact byte position or finite native service. |
| `93e6284` | The already accepted owner frontier is not charged as new payload a second time. | OriginalData debt already contains the frontier. Charging it twice is a deterministic accounting error. | Keep. | The runtime replay did not log a winning early race, so its end-to-end benefit remains matrix-pending. |

Blanket reversal is not justified: it would reintroduce exact, independently
reproduced defects. Equally, retaining a correction does not waive its
remaining runtime cell. A correction that fails its finite affected gate is
redesigned at its stated model boundary or rejected; unrelated constants are
not tuned around it.

## Promoted SEEN work packages

The former UNSEEN consent boundary is removed for this candidate. Every item
is now either a finite work package or explicitly proved irrelevant to the
release symptoms.

The promotion is not a presumption that all former UNSEEN ideas deserve code:

| Formerly held item | v0.4.7 necessity verdict |
| --- | --- |
| Current 32 full-suite failures | Necessary to inventory and close because a release cannot knowingly leave CI red. A stale premise changes the fixture, not production. |
| Retained-tail and two ACK-floor fixtures | Necessary only as suite truth; existing evidence says their premises are stale. No production change without a reachable counterexample. |
| Lowest-frontier temporal service | Necessary: an exact current trace reproduced the application gap while the captured execution followed the RFC. This is the first recovery-model decision. |
| Additional native ordering domains | Already necessary to adjudicate for TCP because SEEN-6D1 established native FIFO debt on every configured TCP carrier; P1 may add a QUIC case. This does not presume that adding a domain is the accepted answer. |
| Typed RateHint and Product-neutral requalification | Necessary: omitted-hint startup and restart-dependent recovery are already observed symptoms, and `a4679b5` bundled an unproven authority policy. |
| Generic score/admission conformance | Necessary: current deterministic tests disagree with RFC 15.1 and the reported mixed-path sway is still open. |
| Overlapping contention-factor inference | Not presently necessary. Keep it out of v0.4.7 unless an exact listed trace cannot be explained or corrected at L3 without it. |
| `STREAM_ACK` service frontier | Adjudicated as an unsupported draft dependency of the rejected mandatory all-stage ledger. Remove it from Core RFC and keep the current Product ACK wire shape; do not add a compatibility branch or wire traffic. |
| `STREAM_MAX_DATA` cadence and partial writes | Necessary reachability/correctness adjudication, but not presumed to explain speed. |
| Target-bound generic tail | Already disproved as the current SEEN-6E cause by zero Product-command wait and immediate native handoff. Close without production change unless another frozen cell supplies an exact counterexample. |
| All-stage work/service-frontier accounting | Rejected as a mandatory Core prerequisite for v0.4.7. Existing exact Product/resource bounds stay authoritative; stage-local counters remain explicitly scoped diagnostics. A future optional profile needs a separate performance proof. |
| Evaluator or benchmark-harness expansion | Not necessary. Reject it unless a listed acceptance cell literally cannot be observed with the current runner. |

## Atomic transaction queue

Only one row may change production at a time. Other agents may audit the same
row or inventory later rows, but they may not mix their fixes into the active
transaction.

| Order | Package | One decision | Exact starting evidence | Permitted closure |
| --- | --- | --- | --- | --- |
| T00 | Checkpoint | Freeze this production SHA, scope, verdicts, and 32-test inventory. | `93e6284`; 2,184/32/0 library result. | Documentation commit only. |
| T01 | P4-model | Is an exact receipt-retained all-stage ledger necessary authority for ordinary placement? | Three owner audits prove it absent; symbolic `8*N^B/receipt_delay` disproves its neutrality; wire kinds 44--48 reject as unknown. | Remove the rejected mandatory ledger/service-receipt/observation dependency from Core RFC; retain Product/resource/native authority, and document implemented v10 return-plan fields plus kind 49. No production change or compatibility branch. |
| T02 | P3-rate | What typed authority and normalized-MPP byte basis supply `C` and `M` for explicit-hint, omitted-hint, request, and fixed-output states? | Valid explicit-hint RED, 350.75-Kbit/s prior persistence, stale floor fixtures, current mixed rate scopes, and exact encoded-frame work. | One exclusive typed directional authority automaton and coherent work unit; split/supersede unproved `a4679b5` hunks. An optional Section 10.2 action-score component consumes one already-reduced `C`; it never selects or maximizes sources. |
| T02b | P3-compatibility | Did the T02 sidecar alter the scalar still consumed by the legacy scorer before a replacement existed? | `score_path` ignores the sidecar; exact request/fixed/response/L3 cases changed live 90--200-Mbit/s evidence to startup, generic fallback was removed, and legacy Unlimited changed from 1 Tbit/s to 351,472 bit/s. | Give an exact activation-scoped QUIC Native shape first refusal, then restore the complete pre-`b5b4b5a` scalar source/precedence/scope behavior and legacy Unlimited shim in every remaining non-Native branch at still-live scalar consumers. Retain the parallel typed authority, normalized typed Unknown, diagnostic provenance, QUIC current-shape fences, Product/resource bounds, and request flow-local isolation. Removing any legacy source is a later independently proved transaction. |
| T03 | P4-score-component | What does the exact score of one already chosen action contain? Can that score alone allocate a sustained sequence? | Arithmetic/timing component REDs are GREEN. A static-winner trace, exact writer release lifetime, 131-slot queue geometry, and 64-MiB Product bound disprove sustained allocation with uniform `A=0`; TCP has no valid dynamic `C`. | Retain the typed single-action arithmetic and coherent timing components. Reject every request/response/L3 runtime-owner migration; make no production selection change. A later allocator must follow dynamic service discovery and own atomic physical-carrier/direction reservations. |
| T04a | P4-accounting | Does current response scoring count one dequeued writer charge twice? | `server_bulk_output_snapshot_at` includes total `commands.pending_bytes`; `response_completion_snapshot` then adds its `writer_pending_bytes` subset again. Request does not. | One exact RED/GREEN removing only the duplicate projection; preserve queue admission, charge lifetime, native metrics, and request behavior. |
| T04b | P4-admission | Can inferred ETA, loss, confidence, flow count, or BDP deny ordinary Product work when lifecycle-valid resource headroom exists? | Exact `completion_horizon`/`ecf_no_completion_gain` admission flip; structural `W/P/E` and configured resource limits held constant. | Make inference ranking-only where reachable, or prove a branch structural/unreachable. |
| T05 | P1-authority | How is renewable repair prevented without a cumulative percentage admission cap? | Proven 434,790,952-byte renewal versus 108,847,604-byte old budget, plus current hard denial sites. | Prove and implement stable live-copy identity, or reject candidate; percentage remains cost/rank only. |
| T06 | P1-service | Which sequential, staggered, or concurrent action minimizes frontier time without assuming independent service? | Exact 1.064233-second sequential replay and coupled-service countermodel. | Choose only a symbolically safe policy and exact two-direction RED/GREEN, or reject it. |
| T07 | P2-domain | Can latency/recovery work overtake native predecessor debt with current domains? | SEEN-6D1 TCP trace; any residual P1 QUIC lower bound. | No-change proof, one carrier-neutral domain design, or model-constrained candidate rejection. |
| T08a | P3-requalify | Does fresh typed evidence restore 10-to-500 service without restart and preserve cold/warm startup? | User traces where restart immediately restored throughput; current portable TCP startup has no valid dynamic replacement. | One evidence-lifecycle/service-discovery correction or no-change/impossibility proof; no initial-rate cap and no `cwnd/SRTT` authority. |
| T08b | P4-allocation | After T08a, can one shared owner divide sustained work without starving a ready carrier or retaining phantom/catch-up debt? | T03 static-winner and early writer-release counterexamples; QUIC stream-local writers share one connection-wide native domain. | One atomic refundable physical-carrier/direction allocation model with rate-aligned remaining work and unknown-rate exploration, or reject runtime migration. Do not resize safety queues or serialize native service. |
| T09 | P4-stability | Does the corrected L3 rule avoid statistically indistinguishable swaps while retaining materially better paths? | Alternating-evidence/deadband RED and mixed-path trajectory. | One RFC-aligned no-flap/work-conservation rule, or no-change proof. No fixed protocol preference/group. |
| T10a | P5-MAX_DATA | Is `STREAM_MAX_DATA` publication cadence exact? | Current conformance debt; reachability not yet presumed. | Exact RED/GREEN, or unreachable proof plus RFC/test correction. |
| T10b | P5-write | Is partial local-write consumption reflected at the correct Product frontier? | Current conformance debt; reachability not yet presumed. | Exact RED/GREEN, or unreachable proof plus RFC/test correction. |
| T10c | P5-tail | Can target-unbound tail authority change a current frozen outcome? | Current recovery trace has zero Product-command wait and 1--2-ms handoff. | Reachable RED/GREEN or proved irrelevant; no speculative queue rewrite. |
| T11a | P7-values | Are direction, freshness, absence, Evidence, Quality, and recovery-score provenance truthful? | Existing stale/zero/reversed/missing-value reports. | Data/projection-only correction; unsupported is `-`, stale is `~`. |
| T11b | P7-identity | Does each local/peer row map the correct path, configured slot, incarnation, active port, and retirement state? | Existing delayed/accumulated/missing path and port reports. | Identity/lifecycle projection correction only. |
| T11c | P7-browser | Does the dashboard render compact tables and natural three-state numeric sorting without changing data? | Existing width, order, and interaction reports. | Browser-only correction and visual verification. |
| T12 | P6 | Do all 32 assigned fixtures match the final authoritative semantics? | Frozen exact list above. | Stale fixture update or owning transaction's already-proved production correction; full library suite GREEN. |
| T13 | P8 | Does the unchanged ordinary candidate pass every frozen user-visible cell against matched baselines? | No complete post-correction cohort exists. | Accept or reject candidate; no model edit during this row. |
| T14 | Release | Can the accepted tree be packaged without reopening model work? | T01--T13 closed and CI green. | Release build, public evidence/docs, platform packages, tag/push, and transient cleanup; otherwise reject. |

T04a is now focused GREEN. Its real queue lifecycle produced `P=12,288` total
command bytes and `W=4,096` dequeued-writer bytes; the old response completion
projection reported `P+W=16,384`. The corrected projection consumes `P` once
and preserves the separate native floor. Queue admission, charge lifetime,
Product flight, request accounting, and native telemetry are unchanged. The
symbolic owner proof and exact evidence are recorded in
`T04A_RESPONSE_QUEUE_ACCOUNTING.md`; this result does not waive T04b or any
runtime acceptance cell.

### P1 — Lowest-frontier temporal service

Observed symptom: QUIC impairment can pin a 64-MiB Product window and cause
second-scale application gaps while TCP service remains available.

Current theorem: for missing range `x`, after copies occupy native ordering
domains `d_0 ... d_k`, another write to any same domain cannot overtake its
predecessor. Sequential retry has conditional latency

```text
T_release = T_evidence + T_authority
          + sum(D_i for attempts before the winner)
          + S_winner(x)
```

and has no finite bound if no domain supplies finite service. No score or
timeout can remove that physical constraint.

Promoted decision: determine whether one epoch may make one exact frontier
copy available concurrently to every eligible distinct ordering domain while
retaining exact target identity, one copy per domain, non-renewal, and
ordinary native admission. The extra-traffic percentage remains a scheduling
cost hint; it cannot deny the structurally bounded recovery operation.
Concurrent fanout would have conditional completion

```text
T_release = T_evidence + T_authority + min_i(S_i(x | joint offered load))
```

It would not increase the existing worst-case copy cardinality if the current
sequential policy can eventually visit the same domains, but there is no
unconditional latency dominance: copies sharing a bottleneck or congestion
controller can increase every `S_i`. This is a proposal to adjudicate, not an
accepted theorem or implementation. It may increase copies in successful
cases because later attempts are no longer cancelled before admission. The
proof must cover independent domains, shared bottlenecks, and cancellation on
frontier progress. That traffic/performance tradeoff must be explicit, and
exact wire accounting must expose it rather than enforce a percentage cap.

The candidate structural identity is
`(session, direction, logical stream, Product range, configured ordering-domain slot)`.
An observation timer, a port hop, or a carrier incarnation MUST NOT mint a new
simultaneous copy for the same key. Product acknowledgement past the range or
terminal stream/session disposal retires the key. A definitively failed native
copy may be replaced in its configured slot, but replacement inherits the key
and cannot coexist with its predecessor. This bounds *live* duplicate work by
configured ordering-domain cardinality. It intentionally does not promise a
finite cumulative byte count across an unbounded sequence of terminal network
failures: a finite cumulative retry cap and liveness after an unknown finite
failure sequence are mutually incompatible.

Measured state may rank or schedule these structurally permitted actions, but
loss, rate, RTT, jitter, confidence, freshness, `Suspect`, and extra-traffic
percentage cannot permanently make a lifecycle-valid path ineligible. Only a
verified terminal lifecycle failure or an explicit operator memory/session/
queue resource contract can do that. This identity and eligibility statement
must be proved before choosing sequential, staggered, or concurrent service.

Closure: either a symbolic impossibility/rejection is recorded, or an exact
pre-change sequential counterexample and post-change proof for the selected
service policy pass in both directions, followed by the one affected ordinary
mixed recovery cell.

### P2 — Native ordering-domain sufficiency

Observed symptom: QUIC-only and TCP-only traffic can retain long native HOL
delay even when aggregate carrier rate is high.

Promoted decision: establish whether the existing configured carriers provide
enough independent native ordering domains. This transaction is already
triggered for TCP by SEEN-6D1: exact traces show latency work arriving behind
already-native FIFO debt on every configured TCP carrier, which MPP priority
cannot overtake. P1 may separately trigger the same question for QUIC recovery.
A new domain must be carrier-neutral, direction-correct, bounded by configured
physical carrier resources, and justified against handshake, fairness, CPU,
memory, and extra-traffic cost. It MUST NOT be introduced merely because one
random run was slow.

Closure: either disprove the existing TCP lower-bound trace and close without
code, validate exactly one clean domain design, or mark the loaded-latency
acceptance property model-constrained and reject v0.4.7. No permanent protocol
preference is allowed.

### P3 — Startup, requalification, and typed rate authority

Observed symptoms: short objects start slowly; omitted-hint TCP can remain at
the discovery prior; restored QUIC can retain stale/inflated native state; a
restart immediately recovers speed.

Promoted scope: configured and omitted initial-rate authority, request and
fixed-output provenance, Product-neutral requalification, restoration after a
10-to-500-Mbit/s transition, and exact freshness publication. A configured
initial rate remains a hint and omission remains dynamic discovery. No stale
sample can become a rate cap, and no initial hint can override live native
congestion control.

Closure: cold and warm short-object traces identify the authority used at each
decision; 10-to-500 recovery proves that fresh evidence can replace degraded
evidence without restart. Any mismatch receives one typed model correction.

### P4 — Path stability and overlapping contention

Observed symptoms: default TCP+QUIC can sway between paths; a marginal
completion edge can create a much larger ordering consequence; paths may
share only some bottlenecks.

Promoted scope: decide from first principles whether uncertainty/freshness is
represented in the current completion score. A change must not create
scheduler-caused idle service, must remain carrier-neutral and direction-
specific, and must respond when UDP is degraded but TCP is healthy or vice
versa. A fixed same-host grouping,
fixed protocol preference, fixed percentage switch margin, or transitive
single-group bottleneck label is rejected in advance.

Overlapping contention-factor inference remains experimental unless an exact
current trace proves that missing it causes a release failure. v0.4.7 requires
the common carrier-neutral decision boundary for the reported L4 path sway:
existing measured actions must not flap on statistically indistinguishable
evidence, and a materially better fresh path must remain usable. Experimental
TUN-L3 receives the same correctness rule but adds no performance claim or
matrix cell. Every comparison threshold is a hint.

Closure: one symbolic no-flap/no-starvation model, deterministic alternating-
evidence tests, and the default mixed trajectory gate. If the current model
already satisfies it, close with no production change.

### P5 — Protocol and flow-control conformance

Promoted scope: the version-10 `STREAM_ACK(..., services)` RFC/input mismatch,
`STREAM_MAX_DATA` publication cadence, partial local-write consumption, exact
target-bound tail authority, and the proposed all-stage carrier-work/service-
frontier accounting.

T01's completed theory audit adjudicates the first and last items: service
vectors and a receipt-retained all-stage ledger were dependencies of an
unproved draft ranking model, not implemented Core behavior. T01 closes only
after that model is removed from the Core RFC and documentation checks pass;
it adds nothing to the wire. Current stage-local counters remain scoped
diagnostics and current Product ACKs remain Product-only. This removes neither
an implemented feature nor a release performance mechanism.

These are not automatically performance fixes. Each is first tested for a
reachable current-wire or current-flow counterexample. An unreachable draft
surface is corrected in the RFC; a reachable code mismatch is corrected in
code and RFC as one model. No compatibility branch is added.

Closure: every item has either an exact RED/GREEN or a proof that it is
unreachable/irrelevant to v0.4.7 runtime behavior.

### P6 — Deterministic suite debt

Promoted scope: the 32 current pre-v10/full-suite failures, including the three already known
stale fixtures. A failing fixture is not presumed to be a product defect.
Each failure is mapped to current RFC authority: update the fixture if its
premise is obsolete; change production only if a reachable current-model
counterexample exists.

Closure: targeted affected tests and the ordinary full suite pass. No
production rule is weakened merely to preserve an obsolete expectation.

### P7 — Metrics and diagnostics truthfulness

Observed symptoms: stale values appeared current, unavailable values appeared
as zero, peer/local path mappings accumulated or lagged, and recovery
decisions lacked exact owner scores.

Promoted scope: exact availability/staleness semantics, direction, path and
incarnation mapping, retirement, port projection, and recovery-specific owner
and alternate score provenance. Observability MUST NOT alter runtime choice.

Closure: deterministic projection tests plus one browser rendering check. A
metric that cannot be supported is `-`; stale evidence is marked `~`; neither
is synthesized from an unrelated direction or lifetime.

### P8 — Final performance and experience matrix

This is validation, not another algorithm package. It includes TCP-only,
QUIC-only, and default TCP+QUIC for:

- cold and warm short objects;
- one sustained single-thread transfer;
- Cloudflare-style concurrent download and upload;
- 500-to-10-to-500 and 10-to-500 recovery where applicable; and
- the declared changing 3--10 percent, mean-six-percent loss cohort against
  raw TCP, V2Ray/Xray, and Hysteria2.

Comparison is role-matched: TCP is compared with raw TCP and V2Ray, QUIC with
Hysteria2, and default with the best applicable baseline. Goodput alone cannot
waive first-body time, application gaps, loaded latency, upload, exact byte
delivery, or restart-free recovery.

The matrix is frozen as follows:

- ordinary release build from one unchanged production SHA;
- raw TCP, the repository-pinned V2Ray/Xray baseline, the repository-pinned
  Hysteria2 baseline, MPP TCP-only, MPP QUIC-only, and default MPP TCP+QUIC;
- controlled 500-Mbit/s egresses, 50-ms one-way delay on each traversed
  egress (approximately 100-ms base RTT), 20-ms jitter, and observed five-
  second loss epochs `3,8,5,6,10,3,5,8%` (mean 6%);
- cold and warm 100-KB, 1-MB, 10-MB, and 25-MB transfers in both directions;
- one logical sustained transfer and a concurrent Cloudflare-style transfer
  in both directions over the complete 40-second changing-loss schedule; and
- 500-to-10-to-500 plus 10-to-500 continuation on the same logical transfer,
  compared with a restart arm but accepted only on restart-free recovery.

Every cohort records exact bytes, first-body time, completion-time series,
per-interval goodput, maximum application read/write gap, loaded p50/p95/p99,
recovery time, errors/resets, and wire expansion. Live `speed.cloudflare.com`
is corroborative only; the controlled workload is the acceptance source.
The existing pre-correction six-way cohort is causal history and does not
count toward final P8 cohorts.

“Competitive” uses the user's allowed approximate 10-percent band as a lab
verdict, never as a runtime threshold: MPP goodput must be at least 90 percent
of its role-matched baseline, while first-body/completion/recovery time,
application gaps, and loaded latency must be no more than 110 percent of that
baseline. Exact-byte, reset, hang, zero-progress, and restart-required failures
are unconditional RED. Speed cannot compensate for a latency failure, nor can
latency compensate for a speed failure.

A focused smoke run never counts as a cohort. Start with three complete
cohorts. All three passing closes the matrix; two failures reject it. If
exactly one fails, run two further complete cohorts and accept only if both
pass. Any other five-cohort result rejects v0.4.7. A cohort is invalid only
when the predeclared host-validity snapshot fails, the impairment readback is
missing/late, the trace is incomplete, or exact delivered bytes cannot be
verified; its recorded invalidation reason permits one complete replacement
cohort, never an isolated cell rerun.

## Ordered execution

1. Commit this reflection and promoted ledger; make no production change.
2. The 32-failure inventory above is complete. Close T01's RFC authority
   correction; do not modify production while that model is ambiguous.
3. Close T02 typed rate/unit authority, then T02b's exact compatibility
   projection before claiming transaction isolation. Close the T03
   single-action component and record runtime migration as rejected; then
   close T04a duplicate current accounting and T04b admission separation
   independently.
4. Close P1 as two sequential atomic transactions (T05 and T06): first replace
   or reject the hard percentage authority semantics while preserving exact non-renewal;
   then decide sequential versus concurrent distinct-domain service. Do not
   implement the concurrency proposal before its symbolic safety proof.
5. Re-run the single affected recovery cell once as a smoke gate and then in
   three independent ordinary-build realizations. If paired signs conflict,
   add at most two valid whole-cell realizations. Five valid results are the
   terminal decision set: repeated loss is RED and unresolved mixed evidence
   rejects the candidate. Neither permits rerunning until lucky.
6. Close the remaining P3 work in provenance order (T08a): omitted-hint
   discovery, then restart-free requalification. The cold/warm ladder is its
   affected runtime gate, not a separate tuning transaction. Only afterward
   may T08b attempt the separately proved sustained allocator; a NO-GO closes
   without runtime migration rather than reopening T03.
7. Close the remaining P4 no-flap behavior (T09). Preserve every earlier GREEN
   as a focused non-downgrade test. Do not add
   overlapping-factor inference unless the exact L3 proof requires it.
8. Close the already-triggered P2 TCP ordering-domain decision (T07) and any QUIC
   lower bound P1 establishes.
9. Close each remaining T10 P5 conformance item separately, then update its
   assigned P6 fixtures. Close T11 P7 without using observability to alter
   scheduling.
10. Run the ordinary full suite once. Any remaining failure must already have
   an owner above; it cannot create a new production work package.
11. Run the frozen P8 cohort rule above without modifying the model.
12. Accept and release only if all correctness identities pass and the paired
   trajectory evidence is competitive without a material regression in
   first-body time, application gaps, loaded latency, upload, or wire cost.
   Otherwise reject v0.4.7 at the first consistently failing work package and
   report its theoretical boundary.

The three-to-five-run count is an evidence-sampling boundary, not a runtime
network threshold. It prevents both one-lucky-run acceptance and infinite
repetition.
No new issue discovered during P8 extends this candidate. It is recorded for
the next version unless it invalidates correctness or safety of the current
candidate, in which case v0.4.7 is rejected rather than expanded.

## Change protocol

For every production patch:

1. Name the observed symptom and exact owner layer.
2. State why the old behavior existed and what invariant it protected.
3. State the falsifier and reproduce RED without random timing.
4. State the new invariant and its cost.
5. Prove that prior invariants remain true, especially native congestion
   authority, work conservation, exact copy ownership, anti-amplification,
   direction symmetry, and lifecycle identity.
6. Commit the isolated correction.
7. Run only its affected runtime smoke gate.

Failure at step 3 means no code change. Failure at step 5 rejects or redesigns
the proposal. Failure at step 7 rejects the candidate correction; it does not
authorize unrelated tuning.
