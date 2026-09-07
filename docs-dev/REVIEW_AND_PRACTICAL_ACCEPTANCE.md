# Evidence review and practical acceptance

Updated: 2026-09-07 03:52 UTC. Original reviewed production tree: `7189e69`
(wire11); latest correction checkpoint: `b37bacb`. This is a review/experiment
plan, not a release verdict. Historical SEEN/UNSEEN labels are mapped below;
an old OPEN label is not a newly found bug.

## Current verdict and fixed priority

**Current pruning verdict:** CHANGE_DISPOSITION_20260907 supersedes the older
"held" wording for inclusion decisions. The test-only static scorer/ranker is
removed (11a68f9), as is optional stateful relative ACK encoding. Its exact
former source is archived, not left active. Keep the evidence-backed native,
qualification, wake/service, repair, stateless-packing and terminal mechanisms
for the listed concrete benefits and costs. This is not a performance pass;
negative ordinary results remain explicit. After pruning,29 timing controls
and67 codec/transport/lifetime controls pass. The next ordinary comparison
must use the new composition, not reuse rates from the earlier frozen binary.

**Requested retrospective:** PERFORMANCE_REFLECTION_20260907 records the
explicit benefit/cost verdict for the recent work. The held repair candidate
introduced its own terminal-retention defect; its correction is not proof of
the user's deployed RAM incident or a speed improvement. The latest ordinary
composition still fails timing, and there is no final-composition versus
release A/B. Current native service continues during the QUIC-only QoS gap;
identify its ordered-prefix owner before another allocator/controller change.
CURRENT_CLOSURE_PLAN records that immediate decision. Earlier next-action text
below is historical, not authorization to bypass this reflection.

**Current continuation:** CURRENT_CLOSURE_PLAN is the authoritative next-step
ledger. The reproduced post-churn retention gate now passes after three
separately justified lifetime/order corrections. Initial1932 successful
requests retained1571 server owners; EOF-only correction still retained3.
Exact trace and RED tests then separated server terminal reconciliation from
client input-recipient cancellation of its still-owned writer. Final30 guards
pass. Ordinary1941 mixed requests on one process pair all complete and reclaim,
remaining clear with flat RSS after68s quiet; final32-request TCP-only and
QUIC-only controls also pass. REPAIR_HALF_CLOSE_RETENTION and
TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907 preserve the full boundary.

This closes the demonstrated normal-terminal retention, not the uncaptured
deployed incident's sole cause or unlimited sustainability. Next remains the
already-proved mixed allocation/discovery deficit. No public performance/
release acceptance follows, and no retained native, actor or codec candidate
is silently promoted. Earlier next-action text below is checkpoint history.

Latest bounded decision: the relative ACK representation is semantically
proved but not a network pass (clean 500/10 mixed 175 Mbps, 1.67 s read gap).
Aggregate runtime tracing verifies the dictionary is used; about 175,000 tiny
ACKs and 100,000 credit updates still pass through native writes. The existing
TCP rule separately writes/flushes each command. An actual server-session RED
and protected TLS/Noise byte comparison justify a ready-feedback packetization
candidate, not a new congestion policy. It preserves every Frame, existing
fanout, immediate publication, priority barriers and exact write debt. All 74
TCP controls pass, but the matched ordinary repeat does not establish a useful
timing gain (292 -> 302 Mbps, p95 876 -> 874 ms, gap 0.879 -> 1.001 s).
The batching candidate and its RFC paragraph are therefore removed; only the
separate encoding candidate continues its bounded ordinary controls. See
FEEDBACK_PACKETIZATION_MODEL and the full-series evidence archives. No held
candidate is silently accepted and no public README/release claim follows.

Reflection correction: the earlier credit-cadence unit proof established
RFC/code alignment, not end-to-end performance benefit. Pre-admission latest
state is bounded, but already queued records still incur physical overhead.
That distinction must be reflected in future model acceptance, rather than
declaring every code/RFC mismatch a practically useful optimization.

The former UNSEEN batch was already promoted for investigation, not automatic
implementation. Its useful results are the concrete ownership, qualification,
work-complexity and ordering findings below. A theoretical suggestion does not
become a defect merely because it appears in the old plan. Disproved items and
rejected approaches are terminal dispositions, not a queue to implement later.

The ordinary executable `packet-class-proof` includes the committed component
corrections AND explicitly held receive-history/reordering, Product qualification,
mailbox, actor-service and companion-stream changes. Its results describe that
composition, not pristine HEAD or an accepted release. No temporary diagnostic
hook or startup-exemption deletion is present in that executable. The ordinary
`no-target-recovery` executable adds only the request producer correction in
`b37bacb`; its composition controls are recorded separately. The older held
stack has not been silently merged into that isolated commit.

Follow-up:2252c67 preserves six clean500Mbps/100ms controls without deliberate
loss/jitter. Mixed401Mbps/echo-p95548ms versus QUIC428/166, raw445/127,
Xray435/203,H2464/112 (explicit500Mbps H2 hint). Mixed reverse traffic348MB
versus about24MB single-carrier, with69,170 complete ACK generations carrying
4.82million repeated range entries. This is real feedback encoding cost under
the existing mixed timing owner, not proof it causes every stall. A stateless
packed-range candidate preserves decoded ACKs and passes focused tests; its
first ordinary run cuts mixed reverse traffic to165MB but echo-p95 worsens to
910ms at398Mbps. Matched repeats are required. Do not accept efficiency-only
evidence as a timing fix or release gate, or discard negative ACK authority.

The completed direct500/10Mbps control is decisive: without deliberate loss or
jitter raw444Mbps and QUIC432Mbps retain echo, while mixed46Mbps loses it with
p952.47s and a3.095MB return queue. Compact ACKs improve82Mbps but retain2.03s
p95/failure. The representation alone is not accepted as a root fix. Next
verify a bounded native ordered-stream dictionary and cancellation-resync proof
before implementation; preserve identical decoded full ACKs and all negative
authority. This remains the existing mixed/asymmetric feedback owner. No new
controller, negative-information policy or allocator is bundled. Evidence and
full series are ACK_RETURN_BOTTLENECK_20260906; global timing remains open.

| Priority / remaining owner | What is established | What is not established / next decision |
| --- | --- | --- |
| 0. Existing mid-transfer reset | Unbound repair without a new target becomes a duplicate publication or session-close result. Exact production RED/GREEN,642 focused controls and strict Clippy pass; b37bacb removes the fallback. Two ordinary mixed uploads now settle without reset. | This specific producer is corrected, not every possible reset. Confirmation gaps and mixed download remain poor. REQUEST_NO_TARGET_RECOVERY preserves the complete boundary and tradeoff. |
| 1. Mixed ordered allocation and recovery | Exact originals can enter a slow TCP ordering domain while a faster QUIC writer is temporarily queue-full. Two observed TCP frontiers take2.940/3.248s to close. Conversely another slow frontier belongs to QUIC, and some TCP spillovers arrive in time. | Do not blame every TCP choice. Replace mandatory immediate allocation only with a contract that preserves finite discovery, failure progress and real resource ownership. Restoring old ECF waiting or deleting the startup exemption alone did not close timing. |
| 2. Native QUIC QoS/reordering history | Packet history, sender evidence lifetime and per-packet loss classification each have reachable counterexamples. Component corrections preserve native congestion authority rather than raising gains. | Better average recovery is not uniform gap/latency non-regression. Recheck native ACK service versus ordered application progress before attributing another bound reduction to a bug. |
| 3. Request/upload service and TCP startup | Full-mailbox wake loss, input-priority starvation and two quadratic scans have distinct proofs. The scans and no-target reset are corrected in isolated commits; held actor/wake changes have component tests. | Adverse upload timing/drain remain open. TCP's portable startup value is not measured capacity; no invented native-rate authority or new probe subsystem is bundled. Fresh unchanged TCP51.6Mbps and candidate76.5Mbps both settle but show why the older214.2Mbps point cannot prove a new regression. |
| 4. Lifecycle and sustainability | CREATE/STARTUP disambiguation, blocked-sink terminal progress, native close and finalized journal ownership have tests. Live source bytes have a shared session owner, not4096 independent64MiB reservations. | These facts do not attribute the uncaptured deployed RAM incident. Churn, backpressure, post-load live ownership and CPU/RSS still require a sustained run. Do not lower concurrency or add arbitrary expiry to hide it. |
| 5. Full experience/comparison | Six-way asymmetric combined down/up cohort completes; mirrored raw/Xray/H2/TCP observations and five no-target correction/control cases are archived. Mirrored mixed and QUIC now also settle; complete series are retained. | Independent200-Mbps links, shared-cut controls, cold/warm short/concurrent work and actual Cloudflare browser remain gates. No current README performance headline or release is authorized. |

The next production transaction is the demonstrated allocation/service boundary,
now that the reproduced no-target reset has an isolated correction. No new
allocator is implementation-ready yet. The fresh cohort is a bounded practical
inventory, not permission
to expand into all plausible model improvements. A newly observed symptom is
first assigned to one of these existing owners or explicitly left unattributed;
it does not automatically create a patch.

### Held candidates are not accepted fixes

| Candidate | Evidence and intended benefit | Tradeoff / why still held |
| --- | --- | --- |
| Native receive history, packet-number encoding and excess-delay reordering | The129-packet history discarded timely reordered originals; sender-only changes could not repair that. The integrated model improves the jitter-only cell from0.585 to185.313Mbps, with actual duplicate/encoding and late-original tests. | Must preserve duplicate safety and finite history. The earlier absolute-delay variant retained common queue delay and was rejected. The excess model still lacks full combined timing acceptance; ordinary loss detection changes are not merely diagnostic. |
| Native snapshot preserves Product qualification | A native-rate projection reset an independently established Product-qualified boolean, making a correct later check unreachable. Exact binding RED/GREEN. | Restored eligibility changes allocation. Keep the correctness proof, but do not retain the old bug as an implicit path preference or claim the projection alone improves speed. |
| Mailbox capacity wake | A pending native write masked the independent wake of a full Product mailbox, delaying an accepted ACK12.173s. Real interlock tests cover write/input/cancellation outcomes. | Cross-layer service timing changes; first ordinary composition exposed additional actor starvation. Component GREEN alone is insufficient. |
| Cyclic Product service with executor cooperation | Drain-input-to-empty could postpone a ready source/dispatch indefinitely. Fair service among input, dispatch and read removes that dependency. | Each class can wait for the other finite work quanta. The first candidate omitted executor cooperation and failed; the revised candidate is tested but broader timing/RSS remains open. |
| Paired QUIC repair ordering stream | A frontier repair had49.326MB of actual unsent native predecessors. A second stream removes that serialization prerequisite without new carrier/copy/CC credit. Actual native ordering and pair-lifecycle tests pass. | Deliberate wire12 mapping and two native streams per attachment; shared connection credit can still block it. Initial download improves but upload does not pass. Not an accepted protocol expansion. |
| Stateless ACK range encoding | Fixed16-byte range pairs repeat across fragmented full snapshots. Packed per-frame gaps/lengths preserve exact logical snapshots, arbitrary fixed representations and all gap authority; selected only when smaller.58protocol/123transport/29feedback tests and strict Clippy pass. | Explicit wire13, additional O(n) integer work but no dictionary/actor state. First mixed run saves53% reverse bytes per body byte without improving timing. Repeat/comparison gate pending; not an accepted fix for mixed stalls or snapshot-generation CPU. |
| Ordered-stream relative ACK representation | Repeated full logical snapshots can reuse an exact native-stream dictionary while decoding the identical Frame. ACK_RELATIVE_ENCODING records cancellation/resynchronization and ownership checks; clean500/10 mixed improves46 to175Mbps. | Still1.67s gap and1.34s loaded p95; mirrored adverse timing also fails. Held compression, not an accepted cure for allocation, congestion or application stalls. |
| Same-ACK RTT ordering | Reordering excess used the old RTT before that ACK published its new RTT, double-counting part of the change. Actual encrypted RED297ms versus251ms; rising/falling cases under CUBIC/BBR3 and470 native tests pass. | Eight ordinary comparisons include gains but still multi-second gaps/echo failures. No gain or threshold change, no full experience acceptance. |
| Repair EOF and terminal retirement | Clean repair receive EOF incorrectly cancelled ordinary terminal exchange. Exact trace plus subsequent RED tests separately distinguish server completion-before-reconciliation and client recipient-closure cancellation. | Thirty final guards and ordinary1941-request mixed/64-request single-mode controls pass; all owners reclaim without restart or idle expiry. This accepts those bounded lifecycle corrections, not the prerequisite companion performance model or deployed-incident sole attribution. Half-close, per-recipient ACK obligations and real native errors remain. |

The evidence boundary is explicit: classification/lifetime and scan commits
are intermediate tracked corrections, not proof that every assembled behavior
is non-regressing. Preserve tests of old problems when simplifying; prefer
deleting invalid duplicate ownership/policy over adding another compensator.

## Fresh ordinary comparison — 2026-09-06 21:07 UTC

[Complete probes and one-second observations](REVIEW_COMBINED_COHORT_20260906.json)
contain12 original-profile cases and4 mirrored-upload cases, including failed
and censored outcomes. All use the same frozen composition and owned routed
cut; there was no compilation overlap. This is one realization per cell,
not a repeated ranking or an acceptance verdict. Physical capacity is500Mbps
in each direction; forward/reverse base delay70/30ms, per-packet jitter20/5ms,
and unequal loss epochs. Forward loss is3/8/5/6/10/3/5/8% at five-second epochs
(mean6%); forward service drops to10Mbps at15--25s; UDP is blocked at30--33s.
H2 has its explicit500Mbps prior; MPP has no configured rate prior.

| Mode | Download mean / max closed read gap | Download echo successes | Upload exact completion / max confirmation gap |
| --- | --- | --- | --- |
| Raw TCP |4.808Mbps /0.478s |80/80 |274.677Mbps, all bytes /0.781s |
| Xray/VMess |3.701Mbps /0.566s |80/80 |Incomplete terminal acknowledgement; no accepted rate |
| Hysteria2 |9.974Mbps /15.011s |29, then one I/O failure |Incomplete terminal acknowledgement; no accepted rate |
| MPP TCP |8.905Mbps /4.433s |3, then one I/O failure |214.232Mbps, all bytes /0.638s;10.796s drain after load |
| MPP QUIC |80.751Mbps /5.885s |30, then one I/O failure |306.984Mbps, all bytes /6.031s |
| MPP TCP+QUIC |62.288Mbps /4.785s |30, then one I/O failure |Reset at4.996s;100,859,639 of190,316,544 bytes confirmed |

After an echo connection fails, remaining unavailable slots are not independent
network failures and success-only p95 is not a whole-run latency verdict.
Upload bins record target-confirmation arrival, not wire departure. Likewise a
download bin above500Mbps can be release of an already buffered ordered suffix;
the physical service counters must be checked separately. Raw's low download
and high upload are real directional results here, not one scalar link quality.

The mirrored pass swaps whole directional impairment profiles, because reversing
the application alone leaves the QoS in its ACK direction. Mocked shaper calls
verify the swap and unchanged default mapping. Raw mirrored upload completes
at4.279Mbps. Xray lacks its final acknowledgement. H2 and MPP TCP exceed the
existing85-second runner observation guard; the teardown then resets their
probes. Those are censored draining tails, NOT independently observed spontaneous
runtime resets. TCP has84.804MB confirmed versus135.463MB locally accepted at
the end. Mirrored QUIC/mixed are deliberately not yet run: the early ordinary
mixed reset is now the higher-priority existing failure. Do not relax a timeout
to relabel these cases or infer their missing bytes were permanently lost.

This stress profile's packet-by-packet jitter can create deep reordering; it
is not equivalent to every real network whose ping jitter is20ms. Current
offload flags were not verified because ethtool is absent in the owned router;
configured loss probability alone is not proof of equal per-wire-packet loss
across transports. These limits forbid a public superiority headline, but do
not erase the separately traced Product-prefix, feedback-work and native-owner
defects. Do not modify the impairment to make the current candidate pass.

Decision: current timing and reset gates are RED. The next isolated diagnosis
locates the client's ReliablePathSessionClosed exit before any change. Its
subsequent server H3_NO_ERROR happens during teardown, so is not its cause.
The15-second QoS step and30-second outage did not cause a five-second
reset. Retain this distinction from the censored baseline/TCP tails. Only
after the reset has a root-cause correction and affected controls should the
allocation/discovery transaction resume. Browser, independent200Mbps links,
shared bottlenecks, cold/warm work and sustainability remain unclosed; README
historical qualifiers remain unchanged and no release is authorized.

## Reflection: the actual process failure

The record does not support either "every correction improved speed" or
"every correction made it slower." Several fixes have reachable owner-level
RED/GREEN evidence; several only improve observability; several are unused
model groundwork. Component success was repeatedly described too broadly,
while browser upload, cold/warm timing, mixed-carrier tails and adverse repeats
remained untested. That was an acceptance and attribution failure.

Model preparation should have rejected three approaches before implementation:
an estimate becoming hard admission authority; a static one-action score being
called a sustained allocator; and a receipt-retained work ledger being called
performance-neutral. Tests remain necessary for timing and native transport
composition, but do not substitute for those elementary proofs.

The remedy is not a blanket revert or a mandatory whole-RFC rewrite. Preserve
proved ownership/progress invariants. A new scheduling/controller design must
first state its observable inputs, byte/direction/lifetime domains, causal
assumptions and counterexamples. A rewrite is justified only where the model
itself fails a practical requirement, with an isolated implementation and
matched application results. No speculative redesign is a release prerequisite.

## Evidence-backed corrections

"Component-proved" below is deliberately weaker than "current end-to-end
performance accepted." Exact test and source references are in the linked
diagnosis documents and commits. The previous complete production batch passed
2316 library tests, integration groups6/2/6, and450 native tests plus3 doctests;
the later native packet-class composition passed469 native tests,25 adapter
tests and strict all-target/all-feature Clippy. These are different checkpoints,
not a newly completed full-suite run of every held component together.

| SEEN / current item | Root cause and practical effect | Disposition / regression boundary |
| --- | --- | --- |
| 1 / 1C | Acquisition arbitration overrode ordinary completion ranking on response/request, assigning OriginalData to a worse eligible carrier. | Keep `38286aa` / `842a0cc`; real owner RED/GREEN. No universal speed gain inferred. |
| 2; P3/T02/T02b | A configured TCP startup prior lost provenance. The subsequent typed-sidecar change also replaced live scalar evidence before a sustained successor existed. | Keep provenance and exact native QUIC scope; `b7961f3` restores the legacy scalar consumer. Do not describe the unused typed TCP startup-only model as a completed dynamic estimator. |
| 4; R1 | Waiting on a local sink also delayed accepted Product/control progress; a nested startup-open path deferred terminal RESET, including an obsolete generation. | Keep `444fb38` / `0545610`. Real blocked-sink actor tests prove terminal/control progress without releasing the sink; successful-open ordering remains. |
| Adjacent FIN/source wake | A successful retried ACK publication could leave an already retained FIN uncommitted; zero source admission did not revisit fresh path evidence. | Keep `f6a2df3` / `a3b706b`, with FIN/actor lifecycle controls in the full suite. These are progress corrections, not congestion tuning or permission to recreate targets. |
| Server restart; R2 | CREATE, later STARTUP enrollment and ordinary reattachment were ambiguous when the server forgot the stream. Retrying later enrollment could incorrectly create state or repeatedly fail the initial-plan check. | `afbb75a` / `89037a8`: explicit phase, only CREATE allocates. TCP/QUIC absent/retained tests pass. Requires paired wire11 endpoints; not durable exactly-once target creation across state loss. |
| 6A; P1/T05 | Live-owner repair authority renewed; the first safety fix then used a cumulative percentage as a hard recovery gate. | Flood prevention was justified; percentage authority was not. `72d1237` / `914b9b9` use authenticated configured-slot/range identity and finite structural copy ownership. Preserve non-renewal, not the old percentage guard. |
| 6E; P1/T06 | Apply expanded a ranked one-quantum repair into a large unranked suffix, increasing duplicate load and ordered debt. | `bfac5b8` retains exact live frontier extent, with separate terminal-failure authority. Exact RED/GREEN and historical matched mixed/QUIC gate support this correction; not all failovers. |
| 6B | HTTP/3 stream priority was reported but not applied to Quinn. | Keep `a9450d8`; actual native priority is tested. Cannot overtake an already accepted prefix in the same ordered stream. |
| Earlier QUIC startup/rate authority | Mixed compensated/uncompensated plateau units and application-limited exit premises could terminate backlogged acquisition early; an MPP ACK-window wrapper could separately underfeed a live native controller. | Retain the exact unit/epoch and native authority corrections described in PERFORMANCE and native tests. They do not prove BBR's retained maximum is an accurate current service measurement; N1 is an explicit residual limitation. |
| Response return-plan startup | The first TCP attachment could own a multi-MiB response prefix before a planned QUIC attachment was even opened. | Retain the explicit bounded pre-FINAL return-plan transaction. Readiness is not mandatory data allocation; later CREATE/STARTUP disambiguation completes its restart branch. No fixed QUIC preference. |
| P4/T04a | Response completion snapshot added a writer-owned subset to its already-inclusive queue total. | Exact accounting correction; not a request bug. Queue/resource charge lifetime is unchanged. |
| P4/T04b | Inferred ECF/BDP denied the only otherwise enqueueable Product action. | Preserve structural resource permission, but revise the prior blanket keep/non-regression claim: `65edae3` also removed a response placement choice without replacing its ordered-service obligation. Permission invariance does not require immediate dispatch. ORDERED_REPAIR_SERVICE_BOUNDARY records the history and insufficient rollback ablation; no wholesale restoration of inferred windows. |
| P5/T10a; C1 | Old quarter-window/payload batching withheld an already freed receive-prefix grant despite RFC8.4. | `5c1d288`,512-byte/4096-window RED/GREEN; latest-value coalescing remains. May add control frames; affected throughput/CPU must be measured, not assumed improved. |
| 7; P7/T11 | Retired path lifetime/identity, port projection, absent/stale values and delivery direction were conflated. Native retained bandwidth was presented like current traffic; pacing was raised to the model. | Keep retirement/identity fixes and `6545270`. Current ACK deltas, retained E and literal P are separate. Browser precision/reset/idle/unequal-window/sort controls pass. Presentation cannot fix a controller. |
| M1 retention | Overlapping recovery transactions prevented finalized journal collection; clone/rollback could leave terminal proof in its parked owner or reuse its identity. | `6636091` / `42d1b86`: prefix compaction, exact owner dispatch and lineage IDs. Removes lifetime-growing finalized history and associated scanning. Does not conclusively attribute the uncaptured deployed RSS incident. |
| Mixed upload feedback processing | Every recovery candidate rescanned the existing repair queue. Quiet profiling attributes 34.143 s to the overlap/enqueue loop; an exact native ACK arrived 44 s before MPP decoded it in a separate capture. | `614dc73` snapshots occupied byte intervals once per request batch. Disjoint-candidate equivalence and 2,098,176-to-2,048 inspection RED/GREEN pass, with all 243 affected sender tests. Ordinary comparisons are preserved in the overlap evidence checkpoint; their conflicting throughput signs do not establish global non-regression. No controller, copy limit, or wake-policy change. |
| Mixed reply drain / uniform ownership scan | The chunk-independent ownership calculation introduced in `53d9ab5` rescans N spans at N boundaries. Preparation profiling and a live worker stack locate it in the post-source reply stall. | LIVE_OWNER_FRONTIER_WORK_BOUND preserves the same sets, order and assignment maxima through an endpoint sweep: RED4,196,352 to GREEN12,286 visits for2,048 chunks, plus sorting. Seven model and743 caller tests pass. Ten ordinary comparisons show shorter upload drain; adverse QoS interactive failure predates and survives the change. Component work correction only; no global throughput/retention acceptance. |
| Native send-evidence lifetime | Imported ten-round collection deletes an original still owned by QUIC, so a later valid loss callback is incorrectly treated as unknown raw evidence. |9f522b2 deletes the independent expiry and uses actual transport terminals. Delayed-loss/ACK RED/GREEN and native tests pass; ordinary diagnostic follow-up finds zero of the previously observed62 unknown-evidence bound actions. Other compensated responses and mixed stalls remain separate. |
| Individual loss class versus native undo | Whole-episode undo correctly requires all declared losses to be disproven, but compensation reused that guard for each packet's classification. An actual late original stayed charged because its episode also contained a genuinely lost packet. |QUIC_PACKET_LOSS_CLASSIFICATION_REVIEW documents actual-engine RED/GREEN, batched exact packet terminals, deletion of duplicate transaction retention and unchanged native undo.8,192-round retention remains bounded. Ten ordinary comparisons do not establish timing non-regression; intermediate candidate only, no release claim. |
| R3 native close | Pending ordinary STREAM data made close-only transmission fail the full-cwnd gate, although closing stopped feedback that could reopen it. | `c6ce159`, CUBIC and BBR3 peer-event RED/GREEN, no clock advancement. Upstream Quinn issue/PR2785/2787 corroborate origin. No change to ordinary admission or anti-amplification. |
| P6/T12 | Historical32 failing fixtures mixed obsolete wire/model assumptions with genuine owner failures. | No longer32 current defects: current full suite passes. Fixture corrections do not count as runtime speed fixes; owning production corrections retain separate commits. |

## Disproved, unsupported, or deliberately not implemented

| Item | Why it is not another urgent production patch |
| --- | --- |
| SEEN3 sparse ACK erases reorder debt | Under the checked ownership invariant `S-A=O+H<=W`, acknowledged-but-held bytes remain charged. No demonstrated erased debt; no patch. |
| SEEN5 ghost/replacement request | The captured fixed-request control uses one attempt and one body. This disproves that explanation for that capture, not all possible lifecycle bugs. |
| P4/T01 exact all-stage receipt ledger | Not implemented; proposed finite retained work would add `goodput<=8N/receipt_delay` and still require predicted service. Reject the mandatory model, not rewrite every writer to satisfy it. |
| T03/T08b static score / new allocator | Pure arithmetic is tested, but constant inputs can select the same winner forever; local flush is not network service. Runtime migration was correctly rejected. Existing groundwork has no demonstrated throughput benefit. |
| P4 overlapping contention factors | Plausible research for nontransitive/directional shared resources, not a demonstrated necessary fix for this candidate. Test shared versus independent links first. No factor estimator or protocol preference is bundled. |
| P5/T10b partial local write | Scalar/vectored cursors survive Pending and the batch retains its Bytes until completion. No demonstrated duplicate/lost prefix. Finer-grained ownership/credit release would be an optimization requiring evidence, not an automatic correctness repair. |
| P5/T10c target-bound final tail | Current exact-target enqueue plus ranked frontier already implements the relevant authority. No duplicate tail rewrite. |
| N1 monotone probe target / minimum probe flight / probe fairness | Three diagnostic changes did not correct the complete downshift trajectory. All were removed. Passing a local invariant was insufficient reason to accumulate them. |
| P2/T07 extra ordering domains | Its prerequisite is now observed: a frontier repair had49.326MB of actual unsent native predecessors, almost all new bulk. QUIC_REPAIR_ORDERING_MODEL promotes only a companion ordering stream inside the same physical attachment for investigation, preserving queue/copy/CC ownership. Broad multi-domain allocation remains outside this transaction; the companion is not yet accepted. |
| T09 significance / no-flap formula | A jitter deadband alone is not statistical confidence and cannot prove that a10% rate advantage is real. A stronger formula remains a proposal, not an accepted fix. |
| L3 optimization | Correctness is retained. Experimental L3 performance is not a dependency for fixing L4 browsing/download/upload. |

## What remains practically open

The 2026-09-06 no-target request-recovery transaction is now separately proved
in [REQUEST_NO_TARGET_RECOVERY](REQUEST_NO_TARGET_RECOVERY.md): an ordinary
mixed upload reset maps from unbound repair OutputUnavailable while four
attachments remain registered. Production-sender RED tests show both a second
copy on an existing owner and the matching session-close result after a stale
transition. Removing that producer fallback passes642 focused controls and
strict Clippy; two ordinary mixed uploads now confirm every byte without
reset. This corrects code to existing RFC owner/slot predicates. It does not
justify loosening those predicates or classify all earlier resets alike.
Full ordinary series preserve mixed4.2/3.3s confirmation gaps, a5.3s download
gap/interactive timeout, and large TCP control variability. The reset component
is not acceptance of the pending composition or of overall performance.

The [2026-09-05 experiments](REORDERING_PERFORMANCE_DIAGNOSIS.md) now reproduce
a severe QUIC reordering deficit on both this tree and the last release.
They supersede the prospective wording below where results exist, but do not
close the complete release matrix. No new runtime candidate is accepted.

The [2026-09-06 continuation](QUIC_RECEIVE_HISTORY_DIAGNOSIS.md) proves a second
native owner: bounded129-packet receive history discards timely reordered
originals, with4,948 exact matches to sender loss in a zero-router-drop trace.
Sender-only adaptive candidates remain unaccepted and are withdrawn from the
production tree. Their higher averages did not close timing stability. The
next reordering model must cover sender loss tolerance, receiver history and
packet-number encoding together, preserving duplicate safety and bounded
memory. This supersedes starting another sender-threshold-only attempt.

The [34-run composition review](QUIC_REORDERING_EXCESS_DELAY_MODEL.md) now
provides that experiment. Correcting receive history/encoding and sender
reordering together raises jitter-only QUIC from 0.585 to 185.313 Mbps with
shorter gaps and complete echoes. But the first sender model retained old
common queue delay; actual native loss declarations kept a 2.015-second delay
after RTT recovered below 100 ms. That experimental model is rejected.
Learning excess delay instead passes the exact RED/GREEN counterexample and
461 native tests, and improves same-connection recovery, but combined mixed
stability is still unaccepted. These are not additional claimed production
fixes. Exact candidate patches and full series are archived; the release
baseline remains7189e69 while the working tree contains explicitly held
candidates. The next owner is the existing mixed
QoS-history/ordered-progress interaction, not a new allocator or threshold.

1. **QUIC deep-buffer latency (N1):** with old bandwidth400 Mbit/s and base
   RTT80ms, the old half-BDP probe allows2MB, but a new10-Mbit/s path's entire
   BDP is0.1MB. A queued RTT can enlarge later flight; the retained maximum
   also supplies the feedback clock needed to retire itself. Native and
   dequeue-verified experiments support queue/model inflation. Restoring
   actual service recovered the same stream in both forward and ACK-only
   tests; persistent restart dependence is not established by those tests.
2. **Mixed and TCP timing:** historical TCP loaded p95 was1417ms against
   raw522ms/V2 538ms; native FIFO debt, not command wait, was observed. Current
   combined loss/QoS/blackhole and both-direction tests must determine what
   persists after the accepted frontier fix. Do not waive a product deficit
   merely because one native ordered stream cannot overtake itself.
3. **Startup and browser trajectories:** 100KB at100ms has an end-to-end
   one-RTT ceiling near8Mbit/s before handshake/headers. That alone is not
   bandwidth underestimation. A100MB transfer alternating14 and326Mbit/s,
   multi-second first-body gaps or a lasting upload collapse are different
   symptoms and require attribution. Cold and warm measurements must differ.
4. **Sustainability:** exact journal retention is fixed; overall heap growth
   under browser churn/backpressure is not closed by that fact. RSS includes
   live payload and allocator retention. Record live streams, owned queues,
   RSS/CPU and post-load recovery; do not lower concurrency to hide growth.
   The current `SessionSendBuffer` charges unique source bytes once across
   streams and releases them on Data ACK or cancellation. Treating its64MiB
   session limit as4096 independent64MiB source allocations is a false positive.
5. **Wider current evidence:** P8/T13 still lacks the complete ordinary-build
   six-way, repeated, bidirectional, single-stream/browser, shared/independent
   and combined-condition matrix. Historical charts cannot fill that gap.

## What theory can and cannot promise

For a work-conserving path, with service `c(t)` in bytes/s and backlog `q`,
`dq/dt = offered - delivered` while nonempty. A sudden capacity reduction
creates queueing until offered work falls or the queue drains. For an ordered
prefix of size`B`, a following byte cannot arrive before the service integral
has delivered that prefix. This is a lower bound, not permission to create an
unnecessarily large prefix.

No causal controller is always the clairvoyant optimum: two networks can have
identical observations now and different capacity/outage changes before the
next feedback. The same current action cannot simultaneously be the optimum
for both futures. With every path blackholed, delivery is impossible. With a
shared cut of capacity`C`, adding paths cannot increase total useful traffic
beyond`C`; headers and repair subtract from that ceiling. An independent link
can increase the cut, but native control, discovery and receiver ordering still
have costs. Configured memory`W` and feedback delay`R` imply the additional
pipeline ceiling`8W/R`;64MiB reaches500Mbit/s only while the relevant release
feedback stays below about1.074s.

These are our explicit deductions, consistent with QUIC's feedback-driven
[loss recovery](https://www.rfc-editor.org/rfc/rfc9002.html) and the
[BBR draft06](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html).
BBR draft06 is not a published RFC. Hysteria's configured-rate Brutal mode has
advance operator bandwidth information; its
[configuration contract](https://v2.hysteria.network/docs/advanced/Full-Client-Config/#bandwidth)
must be recorded in comparisons. A practical comparison may use that normal
configuration, but cannot pretend both controllers received identical priors.

## Finite next experiments and decision order

Use the current ordinary build on both endpoints, not the retained wire10
diagnostic executable. Reuse existing receiver probes. Record actual dequeue
bytes, application interval bytes/read gaps, loaded latency, CPU/RSS and each
applied transition; a changed rate label alone is not recovery evidence.

1. Six-way asymmetric same-link run: raw Linux TCP, Xray VMess/TCP,
   Hysteria2 Brutal, MPP TCP, QUIC, default TCP+QUIC.100ms base RTT split70/30,
   unequal jitter20/5ms,500/500Mbit/s directional service, forward loss3--10%
   (eight epochs mean6%), unequal reverse loss. Add500→10→500 forward service
   and a short UDP blackhole. One continuous download plus independent echo
   exposes collapse and loaded latency. Each product starts fresh; no restart
   during transitions. This first cohort is diagnostic, not a ranking proof.
2. Reverse/upload, short-object/concurrent and healthy/loss-only ablations.
   Distinguish request/feedback direction, native service and Product gaps.
   Verify recovery with the same application request, not a replacement.
3. Two independent200-Mbit/s links versus a shared cut, including a matched
   total-capacity control; asymmetric fast directions and mixed-carrier removal
   ablations. Do not call multiple connections on one NIC independent bandwidth.
   The old aggregate runner's300/200 split is not this gate and must not be
   relabelled as such. Direction reversal on the same asymmetric profile is
   informative but is not a mirrored forward-QoS experiment.
4. At the first material reproducible failure, isolate its owner before any
   production change. Exact counterexample → model justification → RED/GREEN
   → affected ordinary comparisons → isolated commit. No lucky rerun selection.
5. After those deficits close, complete browser/Cloudflare and sustained churn
   gates, then repeated matched cohorts. Update public curves from current
   validated measurements, with uncertainty and latency as well as speed.
   Release remains blocked by material correctness, sustainability or
   competitiveness failures. About10% throughput variation is not an excuse
   for seconds of missing service or worse interactive tails.

Independent audit workers are available again. Their current bounded work is
terminal reconciliation, recipient retirement and verification of the exact
same-process churn evidence; earlier usage-limit checkpoints are historical.

### Historical wording must not restart completed work

`CARRIER_NATIVE_AUTHORITY_PROOF.md`, `QUINN_BBR3_NATIVE_OPERATIONAL_V1.md`
and `RESPONSE_STARTUP_READINESS_MODEL.md` contain original baseline verdicts
and candidate obligations. They are not current defect inventories. Current
active-controller coherence/fences have runtime tests, the return plan exists,
and TCP ReceiptMode/new sustained allocation were not deployed. In particular,
conditional symbolic up/down bounds are not empirically established recovery
guarantees. Read the current source and this disposition before treating a
historical sentence beginning "current" as a new bug.
