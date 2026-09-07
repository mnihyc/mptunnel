# Current deterministic closure plan

Updated: 2026-09-07 07:11 UTC. Historical baseline source: `7189e69`; evidence checkpoints:
`282b71f`, `5d52914`, `9f15ffd`, `c44ecee`, `5e604d1`, `efa8181`. This is the active continuation of REVIEW_AND_PRACTICAL_ACCEPTANCE,
not a new SEEN/UNSEEN inventory. No release is accepted yet.

## Current checkpoint and next decision

**Pruning decision:** CHANGE_DISPOSITION_20260907 is the explicit KEEP/REMOVE
record. Commit11a68f9 removes the rejected test-only scorer/ranker and ten
prototype tests; all29 surviving timing controls pass. Stateful relative ACK
encoding and its transport/RFC state are removed from the active worktree;
ACK_RELATIVE_REMOVED_20260907.patch reconstructs the exact former feature.
Stateless packing, paired repair, native reordering, qualification, interlock,
cooperative actor and terminal corrections are retained for their demonstrated
mechanism-level benefits and explicit costs, not called universal speed wins.
Their adverse ordinary results are recorded alongside the benefits. All67
selected codec/transport/repair/terminal controls pass after removal; exact
results are in PRUNING_CHECKS_20260907. No new tuning or issue
inventory is introduced. Intermediate source checkpoints d268aa4/b7abd78/
59fbd22/b9a1600 track the retained native, qualification, actor and carrier-I/O
mechanisms. Ordinary application build succeeds; the pruned executable is
frozen at `./.tmp/reflection/bin/pruned-20260907/mptunnel`. Full-tree formatting
passes after269c843's three mechanical test-formatting corrections. The
older terminal-retirement binary still contains relative encoding and is only
a before reference, not the current candidate executable.

**Requested reflection / no new optimization:**
PERFORMANCE_REFLECTION_20260907 explicitly separates useful corrections from
unproved speed claims and records that the held repair-stream implementation
introduced its own EOF-retention defect. Component correctness did not earn
whole-experience acceptance. No new runtime changes follow from this review.
The last measured pre-pruning composition has 5.684/2.351/6.216-second
gaps (QUIC download / mixed download / mirrored mixed upload), with one real
echo timeout in each download. TERMINAL_RETIREMENT_TIMING_CONTROLS_20260907
preserves all results. The first same-profile comparison now exists in
PRUNED_RELEASE_COMPARISON_20260907: recorded released `d1a99ad` versus frozen
pruned runtime `b9a1600` gives QUIC0.477->95.238Mbps and mixed3.968->93.139Mbps.
Released mirrored upload is incomplete at85s; pruned confirms676,265,984bytes
in44.774s. These are bundled-composition/profile-realization improvements,
not per-commit causality or general non-regression. All six probes,293 samples
and phase arithmetic were independently checked. QUIC's echo timeout coincides
with the deliberate three-second UDP outage; mixed's occurs duringQoS.

**Immediate disposition / next observation:** the exact raw-queue deletion
passed136 selected checks but FAILED the four-cell ordinary non-regression
gate. Mixed down84.025->56.247Mbps; mirrored upload95.349->51.024Mbps and
4.247->26.214s post-load confirmation drain. All runs completed; none reached
the85s guard. Candidate pre-QoS down is faster119.33->143.38Mbps, but restored
25--30s falls196.15->9.27Mbps. Complete curves and193 samples are preserved in
HYSTERESIS_TIME_ORDINARY_COMPARISON_20260907; HYSTERESIS_QUEUE_VETO_CORRECTION
states the decision and limits. Source/tests/RFC are restored exactly to the
pruned baseline, and HYSTERESIS_TIME_UNACCEPTED_20260907.patch reconstructs the
candidate. No ordinary executable is rebuilt merely for this restoration;
use frozen pruned-20260907, not the current target/release candidate artifact.

That native capture is complete. NATIVE_PTO_LATE_STARTUP_DIAGNOSTIC_20260907
preserves both full logs, probe and41 compact samples. It reproduces a9.860s
QoS application gap with continued server native ACKs and PTO0. Native send
count stops while prior flight drains above the reduced congestion window.
After the separate UDP blackout, the armed exponential-backoff timer fires
1.191ms late; the next probes are followed by two live-containing ACK frames
and PTO0 at each endpoint. This is not a stuck timer or retained-only ACK
episode. The capture does not reproduce the candidate's earlier persistent
post-QoS plateau. No timer/backoff/controller change is justified.

No late-startup rejection occurs in that capture, so it is not necessary for
the QoS gap. An independent read-only lifecycle proof is checking whether an
already-written STARTUP can legally arrive after FINAL, and whether rejecting
that attachment incorrectly retires its whole shared carrier. Treat this as
the existing lifecycle branch, not the explanation for every slow transfer.

The combined Product/native capture is also complete. Its2.771s gap is not a
repeat of the previous9.860s realization. Exact Product prefix218414896 is
TCP-owned; QUIC repair admission follows35ms after the frontier stops, and the
repair reaches reassembly2.737s later. Recovery wakes fire about1ms late.
The router holds4.39MB at the10Mbps cut,3.515s service-equivalent work, not an
exact residence measurement. PRODUCT_NATIVE_QOS_FRONTIER_ATTRIBUTION_20260907
and its compact JSON/compressed raw archive preserve the chain. This rules
out missing enqueue/forgotten wake for this gap; no timer/gain/repair-quantum
change follows. Mixed placement and precise carrier/native/wire residence
remain separate open questions. No next run is queued.

**Completed correction:**11d6f3a fixes legitimate late STARTUP after FINAL at
attachment scope. The legal sender control was GREEN while TCP/QUIC adapters
were RED; all34 distinct focused checks now pass, including FINAL immutability,
no resource/evidence publication, trailing cancellation frames, sibling traffic,
later Ordinary opens and existing restart/reset controls. Independent review
passed. Only startup/attachment/registry logic and the matching RFC paragraph
change; no timeout, rate, gain or scheduler mutation. LATE_STARTUP_FINALIZATION_SCOPE
and its test JSON preserve origin, purpose, causal failure and tradeoff.

The ordinary application builds in1m36s and is frozen at
`./.tmp/reflection/bin/late-startup-scope-20260907/mptunnel`, without temporary
native/registry observers. Both ordinary current-snapshot checks complete:
mixed down85.556Mbps with4.423s QoS gap and one real echo timeout; whole-profile
mirrored upload64.602Mbps confirms all478,806,016bytes but has19.293s post-load
drain and5.054s confirmation gap. LATE_STARTUP_SCOPE_ORDINARY_20260907 preserves
full series,100 compact samples and raw archive. This is neither fluent
acceptance nor a causal A/B of the rare refusal branch. No release; products
and probes are stopped. Native/controller constants remain unchanged.

**Next exact model transaction:** common completion-evidence preparation has
identified a concrete request-side sampling predicate, not a new inventory.
The live RequestPathRateEvidence caller requires each cohort's earliest send
to be after the previous ACK, then advances that ACK boundary even when it
rejects the sample. After the first sample, the real acquisition Owner is
cleared and ordinary pipelining is legal. For cohorts assigned at n*d and
ACKed at n*d+R with d<R, every subsequent sample is rejected indefinitely.
Default Product geometry admits overlapping actual coverage-sized cohorts.
Two independent source reviews confirm there is no forced round barrier that
invalidates this schedule. f4206d0 made the old staged predicate unconditional
when removing its ordered-service branch; later expiry handling retained it.

This can preserve the first request-rate estimate or let it expire despite
continuing unique ACK progress. It maps to the existing TCP/upload first-second
and stale-rate issue, not proof of the deployed server-download cause. Next
prove the exact live producer RED and select a clean separation of staged
acquisition/provenance from sustained achieved-service sampling, preserving
compressed-ACK, expiry, copy and incarnation invariants. No boolean flip or
new scalar/window is approved yet. The generic common-advisory and Defer
proposals are deferred behind this concrete producer defect. Their unknown-
path starvation counterexample still blocks automatic implementation.

Both temporary native/registry observers are removed from source; their exact
archive is NATIVE_PTO_AND_ATTACHMENT_DIAGNOSTIC_20260907.patch. The diagnostic
frozen executable remains separately named. All global gates remain below;
do not infer release acceptance from these component checks.

**Exact selection defect, not yet an accepted isolated fix:** the second, temporary prefilter capture proves
one raw-queue hysteresis defect independently of unavailable/unknown paths.
Both candidates are ready and qualified: live QUIC owner ETA500.202ms versus
TCP499.503ms, within19.407ms measured jitter. The extra raw-byte comparison
overrides retention and sends one TCP original between QUIC originals; that
range later holds the receiver frontier669ms. Exact inputs and distinct
unavailable/startup witnesses are in SELECTION_INPUTS_FRONTIER_ATTRIBUTION_20260907.
The independent HYSTERESIS_QUEUE_VETO_REVIEW_20260907 supports removing only
the redundant raw-byte conjunct: all live callers already score queue service
in ETA. First reproduce the exact candidate inputs as a RED test; then retain
duration hysteresis, material-gain preemption and all admission/lifecycle
checks. Ordinary mixed download and mirrored-upload comparisons above block
practical acceptance. No new constant, wait, controller, discovery or protocol preference.
The busy-fast/free-slow and unknown-startup branches remain separate open issues.

Startup classification is now bounded more precisely: at the captured8308472
decision, the live TCP frontier retains Product authority while additional
QUIC's next quantum exceeds its unqualified allowance. QUIC gains actual
Product qualification335ms later and is admitted. This follows current RFC
roles and disproves stuck qualification in that interval; it does not explain
the TCP original's3.199s publication-to-release delay. Do not add a qualification
threshold fix. The remaining question concerns already-assigned prefix service
and recovery, not a new admission violation inferred from a slow outcome.

**Completed attribution steps:** the six ordinary release/pruned cells and two
raw/Hysteria controls are complete. Raw/Hysteria deliver4.138/8.371Mbps here;
their full timing/queue evidence is PRUNED_BASELINE_CONTEXT_20260907. The
remaining pruned mixed pre-QoS swing is the next exact ordered-prefix event.
One unchanged-binary selective dispatch/rank/mux diagnostic reproduced it;
the subsequent prefilter-only diagnostic disambiguated candidate exclusion from
raw-queue hysteresis. Its frozen binary and61-line archived hook are diagnostic
only; the temporary source hook has been removed. QUIC-only QoS already shows
continued native ACK and physical link service during stalled application
delivery; this cannot be assumed to require a cross-carrier allocator fix.
QUIC_QOS_ORDERED_DELIVERY_ATTRIBUTION_20260907 and
MIRRORED_UPLOAD_TIMING_ATTRIBUTION_20260907 bound the observed queue work and
missing causal evidence. Keep mixed placement as its separate known branch.
Allocation/discovery remains a proposal, not the next automatic code change.
The global gates below remain intact; do not expand the issue inventory.

**Current practical result:** the reproduced completed-request retention now
clears in ordinary same-process churn. All958 then983 requests complete; both
endpoints reach zero logical/admission/path-flow owners after about4.5s per
cycle and remain zero after68.0s final quiet. RSS stays flat through the final
interval. TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907 preserves exact PIDs,
flow lists, probes, resource observations and limitations. Final TCP-only and
QUIC-only32-request controls also complete and reclaim; no mixed repetition.

Three separate lifetime/order defects were proved and corrected: clean repair
receive EOF cancelling its ordinary parent; client retired input cancelling
its still-owned terminal writer; server completion checked before final
membership/ACK reconciliation. Six terminal fixtures distinguish real RED
from the already-GREEN half-close control, and30 selected final guards pass.
The unused mailbox-failure policy was deleted. No timeout, rate, admission,
copy, congestion or target-read rule changed. Exact origin, intended purpose,
tradeoff and checks are in REPAIR_HALF_CLOSE_RETENTION,
QUIC_PRODUCT_RECIPIENT_RETIREMENT, SERVER_TERMINAL_RECONCILIATION and
TERMINAL_RETIREMENT_CHECKS_20260907. The initial EOF-only correction was
explicitly partial; its residual owners were not waived. No uncaptured
deployed-incident sole-cause or whole-performance claim follows.

**Pending model transaction:** allocation/discovery for the already-proved
busy-fast/free-slow choice. Read-only producer/evidence audits establish that
no existing physical-carrier owner tracks recurring discovery opportunities;
current TCP/UDP Product-rate estimators also have different time denominators.
Do not substitute those values for native capacity or invent discovery from
durable qualification. No allocator or new numeric parameter is yet approved.
All temporary diagnostic hooks are removed; allocation code remains untouched.
The held native ACK-transaction ordering correction has eight ordinary
before/after observations, not a release pass. Mirrored mixed upload58.093 ->
76.652Mbps retains4.752 ->4.772s gaps; mixed download68.070 ->92.443 retains
5.056 ->2.943s gaps and an echo timeout. Stationary QUIC157.133 ->176.605Mbps
keeps50/50 echoes and0.355 ->0.346s gaps. Preserve the complete application
series in ACK_TRANSACTION_RTT_CONTROLS and ACK_TRANSACTION_RTT_STEADY; do not
interpret a mean gain as whole-experience acceptance.

An actual encrypted packet fixture is RED297ms versus251ms: reordering learns
against the old100ms RTT before the same ACK publishes146ms. Moving learning
after RTT update removes double-counting. All470 native tests pass, and the
extended rising/falling RTT cases pass under CUBIC/BBR3. No controller gain,
timer priority, threshold, Product envelope or compensation hint changes.
The isolated code/test correction is ACK_TRANSACTION_RTT_ORDERING.patch;
it is retained in the held composition, not silently promoted to production.
Return to BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL: a finite per-head wait still
fails unknown-path discovery, and duplicate discovery currently loses exact
Product qualification authority. Do not implement either shortcut unchanged.

The native-timer diagnostic does NOT reproduce the earlier14s ACK pause.
It confirms509,804,544bytes,86.217Mbps/3.806s max gap. PTO counts advance at
31.498/31.920/32.763/34.447s and reset34.612s, after the UDP outage ends33s.
Thus this realization has ordinary exponential PTO recovery. A1.147s learned
excess remains afterwards, but does not alone explain the earlier14s pause.
See NATIVE_ACK_TIMER_OBSERVATION_20260907.json and
UPLOAD_RECOVERY_GATE_ATTRIBUTION. The earlier freeze remains unattributed:
the published counter counts live-packet ACK callbacks, not all late ACKs of
already-lost originals. Do not equate its plateau with no native ACK reception.

The post-requalification Product gate is also identified:12.26MB old OriginalData
debt exceeds the reset512KiB acquisition allowance. This follows the current
RFC; changing it without earlier ordered delivery would only send more suffix.
The repair queue is empty during another stalled interval, disproving a FIFO
change as that interval's fix. A QUIC proof receipt succeeds in121ms; larger
requalification validity is unsupported. Sparse apply events do not establish
an optimistic-apply busy loop. Product trace overlays are archived and removed;
the native timer overlay is removed after freezing
its binary. No source acceptance, controller/window/timeout/traffic-hint change,
new release or public performance claim follows. Global order remains below.

## Completion gates, in order

1. Close the reproduced server-owner retention transaction: exact terminal
   failure, independent review, narrow model correction, then ordinary mode
   ablations and the same two-cycle post-load observation. Preserve legitimate
   half-open sessions, restart/reset and cancellation behavior.
2. Resolve the observed mixed allocation/startup/recovery timing failures.
   A theoretical discovery proposal is not an accepted implementation. Retain
   actual source/credit/copy ownership and finite progress under failed paths.
3. Recheck native QUIC and TCP downshift/recovery and request/upload progress;
   separate current service, retained capacity estimates and actual delivery.
4. Run independent 200 Mbps links and a shared 500 Mbps cut, asymmetric
   directions, varying loss/jitter, QoS and blackhole combinations plus their
   ablations. Preserve full timing series, read gaps, latency, wire overhead,
   completion, restart/churn and post-load resource evidence.
5. Compare TCP-only, QUIC-only and default against raw TCP, Xray and H2 for
   cold/warm single/concurrent work and actual browser/Cloudflare experience.
   Report finite tested envelopes and limitations, never universal optimality.
6. Publish truthful README/PERFORMANCE plots and release only after the
   practical gates pass. Commit isolated accepted changes/evidence along the
   way; do not merge rejected experiments or call component tests acceptance.

New observations must map to these existing owners; speculative improvements
remain proposals. A real earlier-gate regression interrupts the sequence with
its exact causal transaction, not a new open-ended audit batch.

## Historical checkpoints (superseded next-action text)

**Active verdict:** no release pass. The relative ACK candidate preserves
every logical snapshot and materially improves the reproduced clean 500/10
Mbps mixed case (46 -> 175 Mbps), but retains a 1.67 s gap and 1.34 s echo p95.
Raw/QUIC-only are about 444/432 Mbps. It remains uncommitted. The native
encoding trace proves relative encoding is used; remaining tiny frames include
about 175,000 ACKs and 100,000 credit updates. See ACK_RELATIVE_ENCODING and
FEEDBACK_PACKETIZATION_EVIDENCE for complete records, not just means.

**Latest disposition:** ready-feedback batching is removed. Its real protected
record saving and 74 TCP / 26 queue controls did not translate into a repeatable
timing gain: the matched repeat gives 292 -> 302 Mbps, echo p95 876 -> 874 ms,
and worst gap 0.879 -> 1.001 s. No larger batch or delayed publication follows.
Only this candidate's queue head/helper/writer/test/RFC changes were removed;
the older held stack and separate ACK encoding remain untouched. Full series
are FEEDBACK_PACKETIZATION_COMPARISON_20260906.json.

**Next exact transaction:** ordinary encoding controls are complete and do NOT
pass timing non-regression. The mirrored upload repeat gives control81.518Mbps,
5.629s gap versus candidate44.909Mbps,13.646s gap and26.684s post-load drain.
Both settle exactly. Compression remains held, not accepted or committed;
random timing differences do not prove a codec bug. The next diagnostic uses
existing events to map one upload gap to exact OriginalData and repair copies.
Then resume the already-demonstrated ordered-allocation/recovery contract. No
further feedback policy or controller redesign follows merely from counting
records. Release and global timing remain red. Full new controls are in
ACK_RELATIVE_CONTROLS_20260906 and ACK_RELATIVE_UPLOAD_CHECK_20260907.

**Global order stays fixed:** mixed feedback/allocation and bidirectional
startup/recovery; native QUIC/TCP QoS recovery; restart/churn/post-load resource
ownership; independent 200 Mbps aggregation and shared-cut controls; cold/warm
single/concurrent browser work and raw/Xray/H2 comparisons; only then truthful
README curves and release. Existing unit-proved fixes are component evidence,
not blanket acceptance of the held runtime stack. Uncaptured deployed RAM
exhaustion is still unattributed. Independent auditors are available again;
historical usage-limit entries below are not the current status.

The entries below preserve checkpoint history; their earlier “next” statements
are superseded by the active verdict above.

Current decision22:29UTC: six unchanged clean-link controls isolate an existing
mixed timing cost even without deliberate loss/jitter. MeanMbps/loaded-echo
p95ms: raw445/127, Xray435/203, H2464/112, MPP TCP353/953, QUIC428/166,
mixed401/548. These are single observations, not a final ranking. Mixed sends
348MB on the reverse cut versus about24MB for either single carrier mode.
An opt-in ACK-origin trace on the unchanged binary records69,170 complete
snapshots and4,815,231 repeated range entries for one download. All observed
snapshots are complete (at most234 ranges); blindly switching to existing
positive-only ACK deltas would discard negative-gap authority used by repair.
This is a bounded attribution branch of the already-SEEN mixed/asymmetric
feedback/timing issue, not a new speculative allocator or controller batch.

Next gates: (1) preserve this evidence and establish exact publication/encoding
cost; (2) prove any reduced representation preserves ACK information, order,
late-join and cancellation semantics before implementation; (3) focused codec/
publication regressions and ordinary clean/mixed/asymmetric timing controls.
Do not treat feedback cost as the sole cause of the multi-second stalls or
claim a throughput fix from byte-count calculations. Allocation/discovery,
recovery, browser, independent aggregation and sustainability remain open.
Full probes, untrimmed series and router/process counters are archived in
CLEAN_SERVICE_ACK_EVIDENCE_20260906.json. No source change follows yet; the
working composition and unavailable independent auditors remain unaccepted.

22:38UTC:2252c67 checkpoints the six clean controls and model proposal. A
stateless per-frame packed ACK candidate is now compiled: exact full/partial
Frame equality, no dictionary or actor changes, no-larger wire-size bound.
58protocol/123transport/29feedback tests and strict Clippy pass. RED was3765
bytes for234ranges, GREEN956; five new boundary/wide/malformed tests cover
16,384 complete/partial randomized vectors. RFC wire13 explicitly describes
the representation and unchanged Section8.3 semantics. Ordinary clean mixed,
QUIC,TCP comparisons start only after build90156 completes (2m05s), using
.tmp/reflection/bin/packed-ack/mptunnel with diagnostics off. This remains an
uncommitted efficiency candidate, not closure of mixed stalls or release.

22:52UTC decision: integer packing alone is insufficient. Two clean pairs save
53--62% mixed reverse bytes but have inconsistent latency ordering. Four
adverse controls settle upload exactly but retain download echo failure and
multi-second gaps. A direct clean500Mbps/10Mbps return cut reproduces raw444,
QUIC432, mixed46Mbps/echo-p952.47s; compact mixed82Mbps/2.03s still fails.
Return queue3.095MB directly accounts for about2.48s of10Mbps service. Full
ACK_ENCODING_COMPARISON, ACK_ENCODING_ADVERSE and ACK_RETURN_BOTTLENECK
archives retain all series. The minimal runner change is optional return-rate
substitution; existing omitted behavior is unchanged. No compiler/lab remains
active. Compact source/RFC changes remain UNCOMMITTED and unaccepted as a root
fix; no release or public performance claim.

Next bounded transaction remains mixed/asymmetric feedback: prove a single
bounded ordered-transport ACK dictionary can reconstruct identical full Frames
while eliminating repeated snapshots. ACK_ENCODING_MODEL records the required
write-cancellation reset and consumed-record-only decode rules. Audit actual
TCP Noise/TLS and H3 cancellation before implementing. No logical ACK cadence/
negative-authority change, no new controller/allocator/threshold. After this
branch closes, return to allocation/discovery and the unchanged global gates.

23:17UTC: relative ACK candidate now integrates the bounded codec into TLS,
Noise and H3 (ordinary and companion share the same implementation). Complete
Frames are reconstructed before Product coalescing; no negative-info/cadence/
controller change. Actual encrypted sequence RED84,724/84,628bytes becomes
GREEN6,007/6,003 with all200 Frames equal.126transport/287ACK controls plus
43codec controls and strict Clippy pass. H3 lookahead, split-basis transfer,
missing basis, limits, failed transactions, actual native batch roundtrip and
existing cancellation semantics are covered. Source/RFC remain uncommitted.
Optimized relative-ack binary builds next, then repeat500/10 first; do not
interpret the codec byte proof as global timing or allocation closure.

Review continuation21:02UTC: the fresh ordinary six-way down/up combined
cohort is complete. Download mean alone favors MPP QUIC/mixed80.751/62.288Mbps
over raw4.808/Xray3.701/H29.974, but QUIC/mixed have5.885/4.785s read gaps and
lose their echo connection. Raw/Xray keep all80 echoes. Do not call this a
competitive pass. Raw upload confirms all bytes at274.677Mbps; QUIC confirms
all at306.984Mbps but has a6.031s gap. Same-profile upload places the QoS on
the ACK direction; a separate mirrored upload pass has four observed cases.
The small runner change swaps only the shaper direction and records that fact;
mocked shaper arguments and unchanged default mapping pass. No production
setting, threshold or impairment value was changed.

Most importantly, the EXISTING unattributed mid-transfer reset recurs in
ordinary mixed upload at4.996s, before the15s QoS step and30s UDP outage:
100,859,639 target-confirmed versus190,316,544 locally accepted bytes. Client
reports ReliablePathSessionClosed; the later H3_NO_ERROR is during teardown
and cannot be named its cause. This supersedes allocation as the immediate
transaction. Mirrored H2/TCP exceed the existing85s observation guard; their
teardown resets are censored, not spontaneous product defects. Mirrored
QUIC/mixed are not run yet. All16 observed cases and complete series are in
REVIEW_COMBINED_COHORT_20260906.json; no public ranking follows.

Failure-only tracing covers57 client relay exits and four relevant request
planning/exhaustion branches. It builds successfully in1m23s after labs stop;
the frozen diagnostic is .tmp/reflection/bin/closed-origin/mptunnel. Both
temporary source overlays are now restored, and their exact patch is archived
under .tmp/reflection/closed-origin-overlay.patch. The first diagnostic mixed
upload49079 reproduces the reset at26.512s. At Unix1788728959080,
StalePathReinjection has no planned output while four attachments remain
registered; this becomes ReliablePathSessionClosed, then the relay aborts at
the queued-dispatch error branch. Six earlier bound-target cancellations do
not abort. The later server H3 close is teardown, not the initiating event.
No throughput conclusion from this diagnostic. Other held runtime changes
remain untouched. No independent audit is currently available.

The focused RED exercises an actual reachable exhaustion state: a stale
OriginalData owner plus an already accepted alternate copy, whose immutable
retry deadline expires without a Data ACK. Range recovery becomes due, but
both exact attachments already own the bytes and therefore neither may take
another copy. The old no-target fallback queues unbound work despite that
absence of target authority. The correction retains the original source
obligation and wait for existing membership/model/capacity/receipt events,
rather than materialize an impossible command. This deletes a duplicate
obligation; it does not relax copy identity, stale qualification, native
congestion control or genuine terminal errors. Both RED variants reproduce: the
unbound fallback republishes4096bytes on an existing copy owner while it stays
eligible, or returns PathAttachmentRequired(ReliablePathSessionClosed) if that
owner becomes stale between queueing and dispatch. The fallback is deleted;
no-target returns pending service while source/flight debt remains retained.
Both tests pass and resume on a new exact target;143 request,246 relay and253
stream controls pass. Strict all-target/all-feature Clippy passes after making
the now-unused aggregate queue accessor test-only (no warning suppression).
Exact all-target exhaustion is proven reachable by the sender fixture, not
separately instrumented in the network capture. Release build48685 passes in
1m21s and is frozen as .tmp/reflection/bin/no-target-recovery/mptunnel. A final
two-variant test rerun keeps the dispatch-time stale transition on the GREEN
path too, before ordinary mixed upload/download controls. No network lab overlap.

Five ordinary controls are now complete: two mixed uploads confirm every byte
without reset at204.435/185.927Mbps, but retain4.213/3.305s confirmation gaps.
Mixed download66.271Mbps retains5.319s read gap and an interactive timeout.
TCP candidate76.490Mbps and fresh unchanged51.627Mbps both settle exactly;
both differ greatly from the older unchanged214.232Mbps sample. This does not
prove a TCP regression or gain from the correction. Full evidence is
REQUEST_NO_TARGET_ORDINARY_20260906.json; proof/provenance is
REQUEST_NO_TARGET_RECOVERY. Checkpoint this isolated producer correction, not
the held composition or a release. Next return to the existing allocation/
discovery and ordered-progress defect. No more controller/timeout/resource
threshold experiments.

Follow-up21:47UTC: both previously deferred mirrored upload cases complete on
the same ordinary correction composition. Mixed confirms433651712bytes at
78.719Mbps/44.071s,firstconfirmation0.484s,maxgap8.304s;QUIC confirms667549696
at119.983Mbps/44.510s,first1.592s,maxgap4.229s. Direction-swapped forward-QoS
therefore does not reset these streams, but timing remains unacceptable.
REVIEW_MIRRORED_COMPLETION_20260906.json preserves both complete probes/series.
This completes the missing observations, not the release matrix or statistical
comparability; earlier H2/TCP mirrored observations were censored. No compiler
or network lab remains active. Component checkpoint is b37bacb, not a release.

REVIEW_AND_PRACTICAL_ACCEPTANCE now maps old SEEN/UNSEEN entries, real versus
unsupported claims and each held candidate's tradeoff. The review cohort uses
the frozen packet-class-proof composition; the five-case correction cohort
adds only the no-target producer deletion. Neither is pristine HEAD or an
accepted release. After this reset correction, resume the already-proven
mixed allocation/discovery contract below. No duplicate old prefilter or
startup-deletion experiment, new controller knob or public performance claim.

The live-send lifetime correction is committed in9f522b2;8433c6d preserves
actual removal of the unknown-evidence reduction class. The individual
packet-class correction now has its actual-engine RED/GREEN, native/adapter
controls, strict Clippy and8,192-round retention check. It deletes the
duplicate transaction-retention list while retaining whole-episode native
undo guards. Its isolated source/RFC/evidence checkpoint is committed7677fd9;
the earlier reordering, companion, actor and qualification stack stays held.

Ten ordinary comparisons are preserved in QUIC_PACKET_CLASS_COMPARISON_20260906.
They do NOT establish global non-regression: mixed post-QoS service improves
in these runs, but mixed startup/delivery is bursty, gaps persist and QUIC/latency
differences are not uniformly favorable. No release or public headline follows.
Exact-frontier follow-up now identifies both startup and a later stall. In the
existing mixed steady diagnostic,12.24MB of originals enter TCP in the first
second. At+.514s the live incumbent remains eligible while every additional
output, including QUIC, reaches its unproven-flight allowance. One TCP prefix
then holds64.5MB of received suffix until a QUIC repair at+6.292s. At+9.616s a
new51,616-byte original goes to TCP while the live QUIC lower-owner reference
is omitted from ready candidates; it releases19.4MB only3.761s later. Exact
events are in MIXED_PLACEMENT_FRONTIER_EVIDENCE_20260906 and the interpretation
is appended to ORDERED_REPAIR_SERVICE_BOUNDARY. Logging-enabled rates are not
ordinary performance acceptance. Small repair size alone is not the cause.

Next resolve the EXISTING T04b allocation/discovery contract: exact permission
must not require immediate creation of slow ordered debt, but a wait must not
starve unknown useful service or restore an inferred congestion window. The
BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL discovery counterexample still applies.
Do not implement merely a queue-filter deletion, incumbent-exemption deletion,
new timer, repair-size increase or TCP/QUIC preference. Prefer removing invalid
duplicate policy once its replacement obligations are proved. No new defect
batch, congestion knob or receive-window clamp is justified by this capture.

The single prefilter follow-up confirms27 QUIC command-queue exclusions and
23 subsequent TCP commitments; no QUIC stale/lifecycle rejection. First cases
reopen QUIC in5--10ms, later cases157--207ms. Some chosen TCP ranges arrived
before the preceding QUIC original, so not every spillover is harmful; other
exact TCP frontiers take2.940/3.248s while holding received suffixes. Preserve
that negative control in MIXED_ADMISSION_REASON_EVIDENCE_20260906. Temporary
prefilter logging is archived and removed from runtime source.

One ordinary startup-exposure ablation is now complete: narrow the old
live-contiguous exemption to true singleton, preserving every other predicate
and number. This is causal testing, NOT the missing allocation/discovery model
or an accepted resource change. An initial broader deletion was caught before
any run and corrected to preserve singleton with latency co-load as well.
Exhaustive256 Boolean combinations isolate only the intended multi-path,
unproven, over-allowance case. Ordinary trial143.804Mbps/1.296s maximum gap
versus fresh control168.104Mbps/.309s does not close timing; echo p95 is better
in the trial, so no blanket regression attribution. Full evidence is preserved
in MULTI_FRONTIER_ACQUISITION_ABLATION_20260906. Source is restored. The trial
still shows native QUIC progress while ordered receipt stalls and TCP retains
native queues. It is NOT accepted as a standalone fix; do not tune its allowance.
The41.94Mbps512KiB/100ms pre-qualification ceiling remains a real conditional
tradeoff. Next resolve finite allocation/discovery and irreversible-work
ownership before any further runtime candidate. No build or lab is active.

Global gates stay: mixed and both-direction startup/recovery timing; unresolved
mid-transfer reset; restart/churn/retention;500Mbps single/200Mbps independent
multi-link aggregation and shared/asymmetric controls; actual browser and
short/concurrent plus single-stream service; repeated rawTCP/Xray/H2 matched
series and loaded latency; only then README curves and release. L3 remains
correctness-only experimental work. Independent auditors remain usage-limited.
Historical entries below record the causal sequence, not additional open tasks.

## Active transaction and process correction — 2026-09-06 16:13 UTC

The user correctly challenges the slow and apparently expanding fix process.
Knowing the reported symptoms is not knowing all their causal owners. Several
pending runtime corrections have component proofs but lack accepted end-to-end
composition. Treating those proofs as overall resolution makes each subsequent
failed comparison look like a new regression. That distinction must be explicit.
Diagnostic builds and serial attribution have also consumed substantial time;
more instrumentation is justified only by a specific discriminating question.

Current evidence and next actions, in order:

Active refinement (17:44 UTC): after complete ACK/queue/source costs were
excluded as owners of the long reply tail, relay-stage timing and an owned
native stack locate the uniform live-owner frontier's repeated full-span
scans. LIVE_OWNER_FRONTIER_WORK_BOUND proves the equivalent endpoint sweep:
RED4,196,352 versus GREEN12,286 visits for2,048 chunks, plus endpoint sorting.
Seven model tests and244 sender/246 relay/253 stream tests pass. Ten ordinary
optimized comparisons are complete: healthy upload drain28.775/28.633s versus
nearby control39.613s; max gaps2.628/1.274s versus5.185s. Bulk speed remains
variable. Healthy download is398.836 versus408.541Mbps with shorter read gaps.
Adverse downloads still stall and time out interactive service in both old
control and sweep; the repeated control also fails during QoS, disproving
that this timing failure is newly introduced by the sweep. No blanket
non-regression or release acceptance. Preserve exact ownership, timestamps
and T06 ranked extent; no congestion/timeout/queue knob changes. Temporary
profiling hooks are archived and removed. Component checkpoint1817f6d is
committed. The four companion-stack Clippy lints are now cleared with two
equivalent-expression cleanups and two documented, function-local arity
exemptions; independent stream/attachment/connection authorities remain
explicit. Local all-target/all-feature Clippy passes with warnings denied.
These syntax/annotation changes remain with the held companion stack, not
in the frontier commit, and are not counted as runtime performance fixes.
After this isolated component checkpoint, the next exact question is whether
the remaining QoS-era interactive stall is native/physical service or Product
ordered progress, first comparing the existing mixed and QUIC-only ablations.
Do not start an ACK-ledger optimization or a new congestion tweak by intuition.
Global gates below remain intact; independent audit workers remain unavailable
under their recorded usage limit.

QoS follow-up18:06UTC: five ordinary ablations are complete and preserved in
QOS_NATIVE_WINDOW_EVIDENCE_20260906. QUIC alone also has the recovery collapse
with jitter; its window falls304560→45740bytes while RTT recovers and the
router queue drains. Removing only jitter restores immediate400--480Mbps
post-QoS service in that MPP run. H2 also recovers without jitter but is poor
even beforeQoS with this jitter trace; do not exploit that as a headline win.
All variants still hit the3s echo timeout with the initial physical queue.
QOS_NATIVE_WINDOW_DIAGNOSIS keeps that initial queue, later native-window
collapse and other mixed Product stalls distinct. Build5203 adds temporary
1Hz native bound/phase and reordering-deadline observations only. The exact
branch lowering the window is not yet identified; no new BBR/reordering fix
is justified. Archive/remove those hooks after attribution. Existing global
gates and no-release verdict stand.

Native follow-up18:22UTC: the snapshot identifies short-term bounds followed
by long-term/headroom bounds, not only ProbeRTT. Refill does execute. Learned
reordering excess remains about670ms for roughly ten seconds after current
RTT recovers, then ages away. This does not prove incorrect renewal or justify
a cap. QOS_NATIVE_WINDOW_PROFILE preserves all84 events and the full probe.
One diagnostic build adds exact completed-budget inputs and bound-action
causes; it changes no policy. Next decision depends on whether that evidence
supports the reductions, not on another headline Mbps comparison. All source
trace hunks must be removed after attribution. No new unrelated issue batch.

Exact cause18:38UTC: the action trace finds62 raw/unknown-evidence lower-bound
calls,45 with strict short-bound reductions, in addition to62 budget calls.
BBR3's imported ten-delivery-round cleanup can erase a packet still owned by
QUIC's delayed detector. Actual send/ACK callbacks advance16 younger rounds;
the old original then has no send snapshot (RED). The lifetime correction
deletes that expiry and makes non-feedback transport terminals settle metadata
in active and parked copies without invented ACK/loss. Both delayed ACK/loss
are GREEN;466 native tests and targeted wrapper forwarding pass. See
NATIVE_PACKET_EVIDENCE_LIFETIME_MODEL for origin, symbolic storage argument,
discard coverage and tradeoff. Native diagnostic overlays are archived and
removed. Ordinary recovery comparison is next; compensated-budget responses
are a separate decision class and not automatically defects. No global gate
is waived and no code-policy knob was raised.

Ordinary comparison19:04UTC: all25 adapter tests, ordinary optimized build
and all-target/all-feature Clippy pass. Fifteen full probes plus one-second
observations are preserved in NATIVE_PACKET_LIFETIME_COMPARISON_20260906.
QUIC post-QoS recovery improves in both candidates, but startup is variable,
mixed mode still stalls, and the direction of the steady rate difference
reverses across repeats. All scheduled steady echoes succeed; mixed latency
is not uniformly better. No general non-regression or release acceptance.
Checkpoint the isolated lifetime correction as an unaccepted candidate.
Next reuse the existing bound-action trace to verify disappearance of the
specific unknown-evidence reduction class; separately classify remaining
budget responses and mixed ordered-progress stalls before changing policy.
Do not optimize the packet container or adjust congestion settings from
variable average Mbps alone. Independent workers remain usage-limited.

Causal follow-up19:14UTC:9f522b2 is the isolated lifetime checkpoint. Reused
diagnostics find zero raw/unknown lower-bound actions in both QUIC and mixed,
confirming removal of that observed failure class; all remaining actions are
compensated-budget driven. Full749/585 event records and probes are preserved
in NATIVE_PACKET_LIFETIME_CAUSAL_FOLLOWUP_20260906. Temporary source hooks are
removed. Mixed timing remains unaccepted. The next exact counterexample checks
whether native whole-transaction undo eligibility is incorrectly shared by
individual compensation-record reclassification when actual loss and late
originals coexist. That source hypothesis is not yet a proven performance
cause or another accepted fix. Do not change the two-PTO retention authority,
loss thresholds, delay thresholds or gains on this evidence alone.

Packet-class follow-up19:31UTC: the actual packet engine proves the hypothesis:
Data7 is late-ACKed while genuinely missing Data8 shares its recovery episode;
the old journal charges both ordinary. QUIC_PACKET_LOSS_CLASSIFICATION_REVIEW
records RED, prior intent and the separate packet-proof/native-undo model
before implementation. The candidate removes the duplicate transaction-
retention list and uses exact batched native packet terminals. The original
counterexample and468 native tests pass; a further matching-copy/ECN control
passes. Root adapter build24825 is active. This candidate is not committed or
accepted yet. Next ordinary comparisons use frozen transport-owned-metadata
as control; no diagnostic hooks, no gain/percentage/expiry/queue/wire changes.
Preserve the same loaded-latency and series gate. Packet class correctness
does not prove attribution of every remaining mixed stall or speed deficit.

1. The quadratic request-recovery queue scan is proven and its equivalent
   snapshot implementation is committed in `614dc73`. All 243 affected sender
   tests pass. This closes that component's work-bound proof, not practical
   mixed-path acceptance. Ordinary candidate uploads of 266.185 and 127.882 Mbps
   versus controls of 272.832 and 250.306 Mbps are not stable acceptance.
2. The next diagnostic run, `mixed-combined-up-live-gap-service-0906`, resets
   its application connection after 19.872 seconds. It confirms only
   191,919,271 of 277,741,568 locally accepted bytes. Its rate is an incomplete
   lower bound, not a completed performance result. Identify the first close
   owner before interpreting the failure as a new bug, a pending-change
   regression, or a diagnostic artifact. Do not add another runtime fix until
   that classification has evidence.
3. The same partial trace contains 822 accepted live-gap repair decisions.
   Large TCP-owned holes receive 14,600-byte batches despite larger computed
   target service. That establishes batch geometry, not yet the hypothesized
   feedback-cycle throughput ceiling. Finish the exact ownership/timing proof;
   preserve T06's ranked-extent and anti-amplification protections. No quantum,
   gain, timeout or buffer tuning is authorized by this observation alone.
4. Keep the current ordinary control and candidate binaries fixed. Any next
   behavioral change must isolate one established cause, have its own RED/GREEN
   and affected ordinary timing comparison, and retain a separate acceptance
   verdict. Failed comparisons do not justify accumulating speculative fixes.
5. Resume the unchanged global gates below only after this active transaction
   closes. Restart, retention, browser, aggregation and final baseline gates
   are not silently waived. No release or README performance claim is accepted.

The deterministic commitment is scope, evidence requirements and stop/advance
rules, not a promise that all unknown causes or completion time are already
known. A reset observed during the existing mixed-path investigation is not
automatically a new SEEN issue. If it is an interaction in the pending stack,
attribute that interaction rather than opening an unrelated audit.

User clarification, 2026-09-06 16:28 UTC: deletion and simplification are
first-class correction options. Before adding a mechanism, identify whether
an existing rule conflates independent authorities and can be removed or
narrowed while preserving the established invariant. Temporary diagnostics
are not accepted model code and must be archived and removed after attribution.

Follow-up status: five diagnostic uploads complete without the initial reset,
but retain 2.8--11.2 s confirmation gaps. A stale queued persistent-repair target
is correctly cancelled and is not a new defect. The absent-target fallback
hypothesis is also ruled out. The original reset remains open. One focused
production test proves FIN fails when a stale attachment survives the later
removal of its fresh alternate. A minimal eligibility correction now passes
244 sender, 246 relay and 253 stream tests. It preserves fresh-output preference
and all payload/repair rules. See REQUEST_STALE_SURVIVOR_FIN_MODEL; ordinary
timing verification is next. This does not attribute the earlier mid-transfer
reset or close the mixed-path timing gate. The two temporary diagnostic hooks
are archived and removed before the ordinary candidate build.

Latest component checkpoint: `8e27abb` commits the stale-survivor FIN
eligibility correction, production regression test and only its RFC paragraph.
The held companion, actor-service and native-reordering changes are not swept
into that commit. Five ordinary uploads complete exactly, but mixed timing is
still unacceptable (healthy reply gaps 1.25/8.43 s in controls, 13.07 s in the
candidate; adverse gaps 4.09/4.43 s). REQUEST_FIN_COMPARISON_20260906 preserves
full probes and one-second application/process/flight observations.

The next owner is now narrower than the small-repair-quantum hypothesis:
the target has already received the full ordinary healthy upload while its
tiny replies and client Product accounting drain for many seconds. The earlier
11.2-second diagnostic gap also contains continued server target writes and
target reply reads. REQUEST_FEEDBACK_DRAIN_WORK_MODEL documents exact boundaries
and remaining attribution limits. Profile synchronous Product ACK, flight
release, path recovery and ACK-gap work without new transport policy. The
temporary duration scopes must be removed after this discriminator. Do not
increase repair size, native gains, timers or queue bounds from these symptoms.

## Previous transaction checkpoint — 2026-09-06 15:46 UTC

The historical execution entries below are evidence, not simultaneous tasks.
Latest owner: native FIFO obstruction is PROVEN and committed in e99694d.
The bounded companion candidate is integrated but UNACCEPTED. Three actual
Quinn ordering/credit tests, queue-transfer/binding checks, 302 carrier tests,
53 codec tests and the actual bidirectional companion/half-close/sibling test
pass. Functional root tests used root-only opt0 after opt3/opt1 test builds
were SIGKILLed; the compared executables are ordinary optimized release builds.
First mixed loss/jitter down comparison: control141.918Mbps/gap2.689s versus
candidate155.674Mbps/gap.384s, no failed interactive requests. Candidate still
has uneven delivery and a1.015s interactive maximum (control.685s), so this
does NOT establish non-regression. Full series/RSS are saved separately in
REPAIR_COMPANION_COMPARISON_20260906.json. The next upload comparison is
UNACCEPTABLE: control fails to drain before the existing85s runner guard;
candidate completes211,419,136bytes in63.487s with a32.816s confirmation gap.
Do not rank the control's149.007Mbps lower-bound/teardown result as a complete
transfer. Candidate26.641Mbps is not accepted. Native pair-credit cancellation
passes an actual low-limit test. Wider comparisons pause at this observed
failure: trace client original/repair assignment and exact server prefix,
distinguishing Product accounting, native service and target ACK return.
The lighter trace now proves49.324s without new original assignment while the
server has already delivered208.786MB and client feedback lags near146MB.
REPAIR_UPLOAD_FEEDBACK_BOUNDARY owns the next exact ACK-stage discriminator;
do not reinterpret the absent next assignment as native loss of that byte.
Subsequent ACK-stage traces locate5--9s before client decode, with millisecond
reader-queue handoff and subsecond Product handling in the latest follow-up.
That discriminator is complete: an exact ACK batch is contiguously acknowledged
natively44.098s before client MPP decode, while observed native loss deadlines
remain below.4s. This exact delay is client processing, not native recovery.
Small per-frame queue waits cannot exclude accumulated FIFO backlog age; the
earlier queue inference is corrected explicitly. Quiet aggregate profiles then
give230.989Mbps/gap2.064s and45.407Mbps/gap14.616s. Recovery enqueue processing
grows from2.374s to29.233s, against a46.7s second run; Product ACK transaction
cost is only2.418s there. Nested scopes overlap and must not be added.
The subowner profile completes33.012Mbps/gap14.965s and isolates34.143s in
queued-overlap/enqueue, versus.879s frame extraction and.383s target selection.
The actual batch helper's RED visits2,098,176 extents for2,048 queued repairs.
614dc73 replaces repeated scans with one normalized occupied-interval snapshot;
the disjoint mux-candidate proof preserves sequential overlap outcomes. All243
affected sender tests pass. REQUEST_RECOVERY_OVERLAP_WORK_MODEL records history,
proof and tradeoff. This intermediate component commit is not global runtime
acceptance. Next ordinary optimized before/after upload/download comparisons
must examine gap series, interactive latency and RSS, not only mean throughput.
No ACK thinning, congestion gain, copy-budget, dirty-wake or timeout change.
All temporary native getters, ACK-stage hooks and profiling scopes are archived
and removed from active runtime source. Remaining held candidates stay open.
REPAIR_COMPANION_UPLOAD_EVIDENCE_20260906.json preserves both outcomes and
1Hz native/Product/RSS series. No native gain, queue-cap or protocol-preference change. The simple
response ECF rollback is already rejected; do not repeat it.

1. COMPLETE for download repair and upload native boundary; ACTIVE for upload processing: read-only native offsets distinguish accepted, first-unsent and contiguous
   acknowledged bytes of the exact H3 stream. Map its first repair record to
   native offsets and to Product receipt. No controller, threshold, writer
   credit or topology change. This resolves whether repair service is blocked
   before transmission or only by native/receiver ordering.
2. Specify the smallest allocation/repair change that addresses that measured
   boundary. State independent permission, placement, discovery and committed
   native-work owners. Prove singleton, unknown-path, preferred failure,
   direction/identity, concurrent ownership and high-BDP counterexamples
   before runtime policy changes; revise affected RFC sections explicitly.
3. Component RED/GREEN then ordinary before/after timing comparisons close
   this transaction, or explain a failed candidate and remove its policy.
   Do not declare all issues fixed based on this component.
4. Continue the existing fixed order below through both-direction timing,
   aggregation, browser, sustainability and final baseline comparisons.
   Preserve500-Mbps single cuts,200-Mbps independent cuts and asymmetric
   impairments. No release until material identified deficits are resolved.

Only new evidence needed for those existing issues enters this batch. An
unsupported theoretical concern is not another production fix. Audit workers
remain unavailable; root analysis and tests do not substitute for independent
sign-off. Intermediate evidence commits preserve exact unresolved owners.

## Fixed order and closure obligations

| Order | Identified issue | Required evidence / disposition |
| --- | --- | --- |
| 1 | QUIC reordering and mixed QoS-history delivery gaps | Separate router service, native reliability and Product ordered progress on one timeline. Compare QoS-only, jitter-only, loss-only, isolated outage and combined history. Preserve duplicate safety, actual loss recovery, bounded history and existing lifecycle fixes. The archived excess-delay composition is an ablation, not an accepted baseline. |
| 2 | TCP/mixed timing, cold/warm startup, upload and return to recovered carriers | Both directions, short and sustained requests, source eligibility versus actual native delivery. No hard estimated-rate admission or fixed QUIC/TCP preference. Close with same-request recovery, first-body delay, gap series and loaded latency as well as useful bytes. |
| 3 | Shared/independent aggregation and asymmetric failures | Real routed independent cuts versus shared aggregate service, protocol/path ablations and combined disturbances. Never add carrier estimates as physical capacity. |
| 4 | Browser/Cloudflare instability and concurrency | Actual browser and local repeatable short/concurrent workloads alongside single-stream transfer. Preserve failures in the record, cold/warm distinction and upload confirmation semantics. |
| 5 | Sustainability and existing restart/retention branches | Exercise churn, backpressure and server restart with live ownership, CPU/RSS and post-load recovery. Keep proven journal/phase/close corrections; do not claim the uncaptured deployed RAM incident fully attributed. |
| 6 | Final matched acceptance and publication | Repeat affected healthy/adverse cases against raw TCP, Xray and Hysteria2; all three MPP modes and both directions. Publish current time series and latency, not selected best averages. Release only after material identified gaps are closed. |

For each issue: inspect current owner and introduction intent -> causal
counterexample -> dimensional/lifetime model and explicit assumptions -> exact
RED/GREEN -> affected ordinary end-to-end comparisons -> isolated accepted
commit or rejection. A component pass never closes the global gate.

No theoretical promise of clairvoyant optimality or zero service during an
all-path outage. Measured physical queue-drain bounds are recorded separately
from avoidable software stalls. New unrelated hypotheses stay outside this
batch until evidence and user scope justify inclusion. Already disproved items
and closed component invariants are not repeatedly reopened as new defects.

## Current execution

- User clarification: single-link nominal service is 500 Mbps in each
  direction; multiple independent links are 200 Mbps each. Keep directional
  delay/loss and temporary QoS asymmetric. Earlier 500/100 runs remain labelled
  historical diagnostics, not matched measurements for this new cohort.
- The initial QoS/jitter/loss/outage ablations and owner traces identify one
  exact response projection defect: native snapshots erase existing Product
  qualification, making faster QUIC fail the additional-output startup check.
  NATIVE_PRODUCT_QUALIFICATION_CLOSURE records the counterexample, original
  isolation intent, bounded correction and RED/GREEN obligations. Component
  proof is green, but its practical composition remains unaccepted. The next
  exact trace identifies a TCP-owned missing frontier, substantial stale-owner
  repair backlog and a native observation blind spot. Continue
  MIXED_RECOVERY_QUEUE_DIAGNOSIS before changing allocator or recovery policy.
- Adjudicated recovery refusals match occupied-copy/stale/queue state; do not
  remove those invariants to create apparent eligibility. The current RFC's
  immediately-admissible-action rule permits busy-fast/free-slow placement that
  can strand ordered progress. A model revision is possible, but the initial
  bounded-wait proposal fails unknown-capacity exploration unless it obtains
  a separate evidence/discovery owner. BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL
  explicitly records that pre-implementation constraint, not an accepted fix.
- Mixed still exhibits a 3.442-second gap without the deliberate QoS or outage
  (asymmetric variable loss and jitter retained). The matched QUIC-only case
  has a .507-second gap. Raw TCP, Xray and Hysteria2 controls are archived with
  their complete series; a poor baseline result cannot waive MPP's own gap.
  Next exact trace identifies the deferred TCP input kind during a long write,
  separating actual native delay from actor-level feedback obstruction.
- That trace now identifies Product mailbox pressure: TCP1 retains STREAM_ACK
  for 12.173 seconds until native write completion. Corresponding TCP and QUIC
  interlocks conflate mailbox capacity with an actor-ordering barrier. Next
  bounded transaction is MAILBOX_WRITE_WAKE_MODEL. Its production-interlock
  RED now turns GREEN, along with cancellation/closed-recipient checks, all
  297 carrier tests and 253 stream tests. The ordinary comparison remains
  unacceptable: mixed loss/jitter download still has sustained trickle, and
  two mixed-upload candidates give165--169 Mbps against237--260 Mbps controls.
  Endpoint isolation gives148 Mbps for old client/new server versus296 Mbps
  for new client/old server. The next trace covers every original byte and
  identifies pre-assignment gaps. Input-state tracing then captures2.633 s
  with a live source output, positive read budget and no sender retry blockage,
  but buffered input disables source reads and sender service. Next is the
  bounded/fair Product input-service model in MIXED_UPLOAD_SOURCE_GAP_DIAGNOSIS;
  preserve per-frame ACK validation, incarnation/lifecycle ordering and actual
  Product/native admission. Do not merely remove guards or tune a deadline.
  PRODUCT_ACTOR_SERVICE_MODEL now states cyclic ready service across input,
  dispatch and source reads, not a carrier-selection policy. The production
  arbitration RED retains the legacy empty-queue veto and selects Input six
  times despite all three classes being ready. Candidate removes that veto,
  preserves all three handler bodies and adds explicit RFC 10.4 separation
  from final-writer priority. Four service checks and240 other relay tests
  pass; one server FIN fixture fails before the asserted operation because its
  synthetic completed proof can be future-dated. A test-only timestamp
  correction awaits the next full verification; no production timing change.
  The first ordinary actor candidate is unacceptable:20.818 Mbps and61.501 s
  confirmation gap versus228.118 Mbps and2.280 s in its matched mailbox control.
  Wider comparison is paused. A second exact RED shows its synchronous dispatch
  bypassing exhausted executor budget when mpsc input asks to yield. The proof
  omitted executor fairness. The revision uses Tokio's existing cooperative
  boundary, not a new MPP budget or timer; five production-module tests pass.
  Ordinary revised binary is building; practical attribution remains open.
  The live trace measured resource eligibility, not the
  application socket's readable-byte count; do not overstate that observation.
  MAILBOX_WRITE_WAKE_MODEL and its full-series evidence retain the results.
  Preserve one retained frame,
  exact recipient, partial-write ownership and terminal/requalification
  barriers. Component success is not an isolated throughput improvement.
- Per-range recovery logging heavily perturbs the first upload traces; those
  rates are not acceptance numbers. Large line counts count range attempts,
  not actor iterations, and do not prove a busy loop or attribute the deployed
  RAM incident. Quieter gate traces preserve the source-read obstruction.
  Diagnostic fields are archived and removed from active runtime source.
- The cooperative revision's ordinary comparisons and796 affected tests are
  now complete. Mixed upload211.236 Mbps/gap3.125s, QUIC upload327.385/gap3.463s;
  the first actor candidate's61.5s gap does not recur in that run. Mixed
  loss/jitter-only download remains129.953/gap2.033 versus QUIC152.147/gap.659,
  both80 successful interactive probes. Combined cases still have long gaps
  and interactive failures. The cooperative client's40s RSS543148 KiB exceeds
  control335424 KiB. Keep all runtime candidates UNACCEPTED; do not use the
  fixed source-service invariant to waive ordered-frontier/resource concerns.
  The completed-proof fixture correction is test-only and independently green;
  it is not counted as a deployed performance fix. No additional native tuning.
- The next mixed loss/jitter traces establish a post-submission ordering
  obstruction, not absent QUIC service. An original frontier is assigned to
  TCP; its QUIC repair returns from H3 write in134us but first appears at
  Product receive2.376s later, after44.24MB of preceding QUIC Product payload.
  ORDERED_REPAIR_SERVICE_BOUNDARY records exact events, interpretation limits
  and the RFC boundary. Current15.1 immediate-admission and10.4 native
  nonpreemption can both be obeyed while producing this bad ordered outcome.
  Do not tune BBR, shrink buffers, force QUIC preference or erase copy slots.
  The next transaction is the allocation/discovery and irreversible-handoff
  contract; its unknown-capacity and reversed-quality cases must be settled
  before implementation. Two diagnostic-only hunks are archived and removed;
  no additional runtime behavior changed. Resource cost remains open.
  History review identifies65edae3/T04b's incomplete argument: resource
  permission invariance does not mandate immediate dispatch or establish
  ordered-latency non-regression. Keep exact resource separation, but replace
  the missing allocation choice explicitly; do not restore all old ETA/BDP
  gates or claim this single commit explains every previous regression.
- The response-only ECF restoration ablation is now complete and insufficient:
  ordinary136.161 Mbps/gap1.494s versus matched current135.348/gap3.523s, both
  80echo successes. Different random realizations do not establish a stable
  gain; both retain stalls/buffer-release bursts. A diagnostic confirms the
  restored predicate executes and still reproduces a TCP-owned hole with
  about60MB of reordered suffix. All ablation source hunks are removed; full
  series, patches and attribution are retained in ORDERED_REPAIR_SERVICE_BOUNDARY
  and RESPONSE_ECF_PLACEMENT_ABLATION_EVIDENCE_20260906.json. No simple rollback
  is accepted. Next work is the four-way permission/placement/discovery/repair
  service contract, compatible evidence and irreversible-work ownership proof.
  Revise affected RFC15.1/10.4/15.2 explicitly only after that proof; do not
  rewrite unrelated accepted ACK/lifecycle semantics or start another gain,
  queue-cap or protocol-preference sweep. Global gates above remain open.
- Native outage trace shows ordinary exponential PTO backoff, not a stuck
  timer in that capture. Physical queue drain, native reordering tolerance and
  Product-prefix stalls remain separate causes; do not collapse them into the
  newly identified qualification defect or waive the other gates.
- No new policy before attribution. Independent audit
  workers remain unavailable under their recorded usage limit; no substitute
  independent sign-off is claimed.
- Persist every verdict, exact executable/configuration, series and next owner
  in PROGRESS and committed docs-dev. Preserve evidence across compaction.
