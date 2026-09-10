# Current deterministic closure plan

Updated: 2026-09-10 23:57 +08:00. Authoritative repository: `./`.
**MPP is not performance-accepted. No release, push or public README update.**
Continue the authorized closure loop; do not conclude at an intermediary commit.

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each new
transaction and after compaction. This is the active decision ledger, not a
growing inventory. Complete preceding chronology and adverse outcomes remain at
`git show 79ddb41:docs-dev/CURRENT_CLOSURE_PLAN.md`, earlier49143f2, and the
linked reports/archives. Condensation discards no experiment.

## Active transaction: discriminate recovery invalidation without new ACK facts

### Completed discriminator and rejected candidate

Runtime baseline is back to d44ca8e. The attempted
bound-chooser reuse and its test are removed, with exact patch, executable,
RED/focused/build logs and ordinary results retained. It is not a dependency
of the next candidate. Reports and archives:
[complete work/stage/ordinary evidence](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md).

- Preselect89074 locates22.346s gap work and44.081s dispatch work. Stable-absent
  owner queries are only1.232s/5.51%of gap time: defer their pruning. In its
  own17.164s stalled interior, dispatch owns16.488s, not the small query.
- Dispatch-inner16610 locates13.410s direct versus.218s queued dispatch.
  Bound planning11.123s/472,741calls dominates;459,573sends block and12,558commit.
  Native-stale retry is absent, successful Product commit only.072s: no index/
  retry fix. Its exact winning repair is decoded8.381s before Product delivery.
  In8.033s stalled interior, preselect4.638s and dispatch2.498s both matter.
  Across ALL gap-service callers, lower owner/target model13.746s over1,768,251
  calls;27,699nonempty service evaluations. This63.84ratio is an average, not
  a source upper bound or a path count. Nested/different-scope timers cannot
  be summed/subtracted as exclusive preselect CPU.
- The bound-chooser candidate proves capture-count RED(6,3,7,3,10,6)versus2,
  after all exact target/queue/measured controls.74focused tests pass. But
  ordinary51554 reaches85s guard:141,370,950confirmed/201,261,056acceptedB,
  maxconfirmation19.827287s/write11.731949s. No rawbins/exact completed rate.
  Guard teardown causes terminal-ACK absence; it is not spontaneous shutdown.
  Practical benefit forecast FAILS; the candidate is removed, not promoted.
  This single realization does not prove its small change caused every decline.

The last ordinary failure starts beforeQoS: client reply309B at10–15s while
target135.10→172.74MB. Restored25→40s target service is.140Mbps. At60→67s,
source adds2.62MB while target173,775,270B and server/client reply729/365B
stayflat. Client lifetime CPU remains102–103%, with two TCP Recv-Qs~127–130KB,
empty native send queues and large native windows. Some small native ACKs
continue; that is not bulk progress. These ordinary counters do not expose the
critical bytes or exact repair/command authority. No BBR/QoS explanation or
new CPU/fairness threshold is justified by this state.

### Full-view proposal rejected before runtime

Issue/impact: recovery enumeration introduced by79ddb41 multiplied expensive
lower model work and preceded multi-second local reply holds. d44's narrower
selector reuse has not restored practical service. The model must make one
finite decision affordable, not merely reduce one helper count.

Competing causes: repeated native/health capture for each region, pure Product
projection/owner-ledger work, and repeated service invalidation without changed
facts. Source audit identifies all three but only the first has the following
whole-decision reuse argument. Subsumed ACKs concretely re-dirty structural
recovery despite no new positive/negative facts; however their frequency is
not captured, so that is NOT a selected second fix. There is no unconditional
dispatch spin: exhausted scans clear dirty state and retain prearmed wakes.
Round-robin service order alone does not bound a whole failed scan's cost.

Exact proposed correction: ONE lazy full native/health observation across
data_ack_gap_reinjection_service's synchronous finite region evaluation.
First force it at the existing lower-model capture point AFTER the same owner/
uniform-geometry/exact-cache checks. Preserve inputs None/Throughput/
PATH_OPEN_SCORE_BYTES/true and full measured/native-proof provenance. Preserve
live cause-specific command checks, per-exact-range clocks, owner eligibility,
source quantum, distinct-target semantics and fresh final plan/reservation/Apply.
No across-call/dispatch cache, queue shortcut, new flag/configuration or threshold.

Origin/model: dc4853d intentionally froze owner and alternate within each lower
model.79ddb41 later enumerated independently due omitted regions, multiplying
that capture. a747bda shares immutable ownership geometry but not path evidence.
RFC10.1's immutable observation/finite action order suggests a single Observe
for this pre-enqueue evaluation; structural dispatch is different because failed
sends can reconcile/detach. Between these gap regions only exact assignment
clock init/min-tightening and local result/deadline accumulators mutate; no
ownership/debt/cache/queue/epoch/qualification/membership mutation or await occurs.
Those clock fields do not feed Product snapshot projection. Independent source
challenge is active before code. Health maintenance/native updates would be
seen at the next evaluation: disclose this temporal boundary, not bit-identical
replay. Current generation-backed publication/capacity wake and final Apply
must preserve newly relevant authority.

Forecast: if N scored regions would each capture all P paths, capture work goes
from N*C_capture(P) to C_capture(P), while region geometry/projection/ranking/
clock work remains. The diagnostic averageN=63.84 suggests materially more
avoidable collection than the failed chooser-only change, but capture's share
of13.746swhole/1.718scritical model time is not isolated. Those enclosing times
are ceilings, not predicted removable delay or a Mbps claim. No gain/regression
is possible if another stage dominates or the changed observation boundary
hurts service. The hypothesis is worth one actual producer RED and the unchanged
ordinary cell, not another broad profiler/harness or numeric tuning.

Smallest action: independent mutation/wake audit plus ONE test-only actual
multi-region gap-service counterexample with semantic candidate/clock/coverage
controls before capture count; no synthetic snapshot-only test. Empty/no-owner
paths retain zero captures. Implement only after intended RED and audit. Targeted
affected checks, then same ordinary200+200QoSUP; no compiler overlap.
Acceptance/stop: wrong target/clock/eligibility or failed wake rejects; same-cell
incomplete service, material adverse timing/cost or no useful gain stops
promotion. No favorable reruns, threshold rescue or stacking the removed
chooser candidate. Actual result must be recorded against this forecast.
Global gates below remain intact; no new unseen model obligation is added.

Independent challenge rejects the full-view proposal AS STATED. Concrete
pre-binding countercase: first region forces a view with B/C Backup, B faster,
but exact existing copies exclude both. TCP PathStatus independently promotes
C to Regular before a later region that permits both. Current per-region
capture picksC; reused full view bindsB. Later main plan intentionally preserves
that bound target and does not compare the original parent tier set. Real
producer is tcp/client/receive.rs→update_peer_path_usage, outside Product lock.
Future publication wake is not validation of the already bound choice. Pure
Product immutability therefore does not freeze structural eligibility.
Do not bolt on a new generic fence/cache mode to rescue this candidate. Its
test-only static draft is saved at ./.tmp/reflection/finite-gap-observation-
test-draft-0910.patch and removed before any build; no RED/GREEN or runtime
claim follows. No new implementation is present.

### Selected next information capture: exact subsumed-ACK invalidation

Existing source counterexample: apply_client_stream_ack's validated subsumes
branch changes only idle-progress time and returns0, but its caller still sets
request_recovery_dirty=true. Do NOT use released_bytes==0 as the proof: new
negative evidence/copy release can matter without releasing Original bytes.
The existing exact positive+negative subsumption predicate is the discriminator.

Question/forecast: in the same ordinary failure context, how often do truly
subsumed ACKs change recovery from quiescent to dirty, and do those events
coincide with material repeated-dispatch/reply-held intervals? Counts of all
ACKs or aggregate model time cannot answer this. A temporary periodic counter
will separate new evidence from exact no-ops and no-op redirty transitions;
reuse the existing owner/dispatch/reply observer, no bulk ACK logs or runtime
policy change. This capture predicts information, NOT speed. A rare/nonmaterial
transition defers the change; a material actual trigger selects a focused
producer invalidation test and minimal change preserving all new-evidence,
capacity/model/deadline wakes. No ACK batching, cadence change or fairness knob.
Same200+200QoSUP profile, d44ordinary source, one frozen diagnostic overlay,
fully reversed before traffic. Root owns build/lab; independent review checks
classification and observer cost. No another whole-view implementation.
Execution: reused18-source-file dispatch-inner overlay is applied on d44,
plus feature-only TLS classification at the existing synchronous subsumption
branch and three count-only counters at the actual successful ACK caller.
The Drop recorder precedes Product guard declaration and emits after unlock,
including early exits. Successful new-facts + subsumed counts partition only
successful applies; subsumed_redirty is their exact prior-dirty=false subset.
Artificial1us floors are not ACK timing. No per-ACK raw logs/new validation.
Release diagnostic build33516 is active. Initial format-check51603 stopped
before cargo on pre-existing observer layout; this is not Product/test failure.
The only final concurrent edit was import ordering, with no semantic or line-
count change; saved overlay is refreshed. No further source edits during build.
Build33516 closes0/3m37s; full source matches saved ack-invalidation-profile-
0910.patch, frozen in bin/ack-invalidation-profile-20260910 and wrapper. All
18source-file observer changes are fully reversed BEFORE diagnostic6884.
Same physical/profile/selected reply events, PERF1/SAMPLES0; new counters are
periodic only. Independent actual observer review passes. Capture is active.
Diagnostic6884 closes0/61.007750s:exact504,627,200B in60.621599s, maximum
confirmation7.746007s/write4.058593s. Successful ACK counts are6,094newfacts+
22,774subsumed=28,868, with17,870actual subsumed false→true redirties and4,904
duplicates alreadydirty. Whole actorhold56.815s; dispatch35.784s (direct35.589),
plan30.265s/989,922sends (970,904blocked/18,964commit), preselect14.958s.
These exact transitions are real producer evidence, not a synthetic dirty-bit
test; own reply-held interval reconciliation is pending before runtime choice.
Neither redirty percentage nor counter durations estimate removable time:
independent native/model/capacity wakes can still require the same scan.

Own decisive window: Q0 repair[1405,1419) spends6.246s decode→Product inside
a6.991s advancing-reply gap. Complete5.032s interior has414subsumed/63new-fact
ACKs and136actual redirty transitions, with4.758717s dispatch versus.053776s
preselect; nested bound plan4.103400s/98,243sends. This establishes material
overlap, NOT that136resets caused all1,527direct dispatch calls. The largest
7.746s gap instead has late server acceptance and only78ms postdecode: keep
that different cause, do not call every stall local.

Selected correction: make successful ClientStreamAckOutcome explicit with
released_bytes and has_new_facts; false only on existing exact subsumption,
true on every successful full ACK application. Real caller keeps identical
buffer release, changes only recovery_dirty=true to |=has_new_facts. Preserve
already-dirty work, idle-progress update, generic Input retry reset, prepared/
FIN handling and every independent model/capacity/deadline/membership wake.
Origin:59fbd22 introduced dirty scheduling; b2aa215 added the helper's exact
ACK no-op fast path without communicating that distinction back to the caller.
It intended to avoid repeated ACK-owned work, but still invalidates recovery.
This is an implementation result/consumer mismatch, not an RFC/rate change.

Capture6884 supplies the real-producer RED (observed exact no-op false→true
transitions), so do not invent a helper-only failing test for reachability.
Focused regression must include duplicate positive evidence and a novel negative
scope with zero released bytes, which MUST still invalidate recovery, plus
unchanged prior-dirty/idle handling. Independent type/wake audit finds no missed
service countercase. Foreseeable benefit is removing repeat scans caused solely
by these replay events; at most the affected fraction of4.759s critical/35.784s
whole dispatch is removable. Independent wakes and preselect remain, so no
specific Mbps or complete-cure forecast is justified. A cheap narrow change
targets a measured material owner without another tuning/observer project.
Acceptance remains the SAME ordinary200+200QoSUP full completion/timing/cost
gate after targeted tests. No practical gain/adverse result stops promotion;
no parameter rescue, broad ACK batching, or revival of either removed view.

Execution: ACK-outcome correction is present only in client.rs/control.rs, with
focused tests in tests_client.rs/tests_control.rs. Independent final four-file
semantic/wake review passes. The actual apply sequence is positive scope4/[4,8),
replay, expanded negative scope0/same positive, replay: expected released/new
outcomes (4,true),(0,false),(0,true),(0,false). Existing source/copy/queue/frontier,
idle and invalid-ACK controls remain. Test build72193 stops before tests on a new
fixture SmallVec-versus-Vec assertion type mismatch; correcting it to compare
slices changes no runtime/test intention. Retry28256 is active, default test
profile/jobs1. This compile error is not a Product RED. Diagnostic6884 report
and verified11-member/752474B archive are complete. Its local6.246s hold and
different late-acceptance maximum gap are recorded separately. Ordinary source
has no observer, rejected chooser or whole-gap observation candidate.

### Completed loss-CPU discriminator; no CPU fix justified

Four ordinary500Mbps DOWN controls complete on d44ca8e runtime: QUIC0/20loss,
then mixed0/20loss; unchanged100ms RTT, no jitter/QoS/outage. Whole useful rates
430.558/48.261/385.581/61.069Mbps; late30–40s rates441.397/1.906/371.853/3.021.
QUIC20 loses the echo socket once and has33 later unavailable records; preserve
that failure rather than reporting successful-only latency as recovered service.
At20loss, server startup process peaks101.2/138.1% of one core, coinciding with
substantial traffic. Late server means3.87/7.11% rule out sustained CPU starvation
as the cause of THESE late collapses, not short bursts or the deployed incident.
Thread peaks cannot refute the user's total-process one-core observation.

Native ACKs progress while QUIC flight limits/pacing contract. Default10%loss
allowance plus2%residual permits native congestion response above11.8%;20% is
outside that allowance. This explains why backoff is authorized, NOT why
near-zero service is necessary or correctly calibrated. Exact budget/BBR phase
attribution is absent. No CPU/fairness or controller-threshold patch follows.
Role/version/platform remain unanswered; do not block the main no-loss stall.
[Full CPU/timing/policy report](QUIC_LOSS_CPU_20260910.md) and its27-file raw
archive preserve all four runs. Information forecast met; deployed attribution
and sustained high-loss service remain unresolved, not new speculative fixes.

## Preceding experiments — closed decisions, not instructions to rerun

The complete prior transaction chronology remains in the preceding commits and
the linked reports. No recorded adverse result is waived by condensation.

- Original independent-cut b0 counterexample: while46UP slows,47 stays healthy,
  but authoritative recovery sends14.6KB extents108–203ms apart. Exact receipt/
  copy observations establish one-head service serialization, not a physical
  healthy-link ceiling. [Model and actual producer RED](INDEPENDENT_QOS_RECOVERY_MODEL_20260910.md).
- Pilot79ddb41 allows independently due omitted successors while preserving
  exact copy occupancy, clocks and ranked extent.95focused checks pass, but
  ordinary53390 is incomplete at85s with26.666s confirmation gap. Profile37140
  locates31.400s evaluator work in37.167s stalled service. Enumeration multiplied
  an expensive query. [Full pilot result](AUTHORITATIVE_GAP_SERVICE_ORDINARY_20260910.md).
- Viewa747bda removes repeated horizon sweeps, actual RED5290vs2442visits plus
 101checks. Ordinary10116 completes323158016B/56.678s but gap7.471s/write14.916s;
  healthy/restored service still poor. The real work correction is insufficient.
- Boundary/stage captures34863,34271,60654 identify seconds after shared reply
  enqueue;60654 is incomplete at85s. Carrier input-only priority is not the cure.
  Owner-profile17084 attributes repeated short exclusive holds, not one long
  lock or acquisition contention. [Full view/stage/owner record](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md).
- d44ca8e shares one lazy existing target observation, unchanged projection,
  eligibility and fresh Apply. Actual RED82013 observes2/4/4captures instead of
 1/1/1after semantic controls;73focused checks pass. Ordinary22211 completes
 419954688B/64.942209s but confirmation worsens7.471→9.215s, write improves
 14.916→5.172s. All target bytes and1720replies exist by sample46; client only
  reaches1720 at65. No performance promotion. Same report plus
  RECOVERY_TARGET_OBSERVATION_ORDINARY_20260910.raw.tar.gz retain full evidence.
- Current preselect observer89074 is an information result on d44ca8e, not an
  ordinary comparison. The active decision and exact counts/held windows are
  above. CPU0/20control outcome is separate and also above.

Deferred/rejected remedies stay deferred: fixed protocol preference, native
threshold/quantum changes, ACK batching/cadence, and stale-only pruning without
material measured benefit. A successful local test never upgrades pilot status.

## Next ordinary contract — topology retained, no setup project

Owned direct topology: client eth0=46.10/eth1=47.10; server eth1=46.20/eth0=47.20.
Two independent200Mbps cuts, each shared by its own TCP+QUIC; not eight physical
paths. UP70/DOWN30ms; no loss/jitter/UDP outage in this discriminator.
Only46UP200→10→200 at15–25s;47 remains200.40s offered, existing50s probe
completion timeout/85s runner guard. Preserve RAW windows and exact completion,
first service/gaps, actual class/native/target progress, wire/CPU/RSS.
No compiler overlap; host load need not be zero.

Use existing run.py aggregate combined up, REFLECTION_LINK_RATE=200mbit,
MIRROR_IMPAIRMENT=1, NO_LOSS=1, NO_JITTER=1, NO_BLACKHOLE=1, MANAGEMENT=1.
Unset ROUTED, NO_QOS, per-role binaries and diagnostic/target/return overrides.
Select a new frozen ordinary executable/tag; never overwrite earlier capture.
If supported, next healthy aggregate UP(NO_QOS=1), then affected shared500
controls. Before routed reuse restore exact routes/network attachments and
remove only gate-owned endpoint HTB roots; never stack router/endpoint shaping.

## Prior practical results and unresolved owners

1. **Native TCP refillb0baca2 remains working, not globally accepted.**
   Exact e722c46 observation attributes1.353s of1.497s echo to native unsent FIFO.
   Native LOWAT131072 plus fresh Original preclaim/writer readiness improves
   TCP DOWN417→440Mbps, echo p951269→323ms; TCP UP422→450 exact.
   Both mixed DOWN orders lose~5% late throughput; copies189→295MB and some
   tails worsen. Do not tune LOWAT or suppress all repairs to erase this.
   [Ordinary/costs](NATIVE_REFILL_ORDINARY_20260910.md),
   [copy attribution](NATIVE_REFILL_COPY_ATTRIBUTION_20260910.md).
2. **Healthy independent aggregation works before heterogeneous failure.**
   One200→two200: DOWN148→302Mbps, echo p95918→262ms; UP169→316 exact.
   UP maximum confirmation/write gaps.411→.420/.317→.528s worsen; CPU/RSS grow.
   [Independent report](NATIVE_REFILL_INDEPENDENT_20260910.md).
3. **Older mixed healthy UP5.140055s hold remains not exactly attributed.**
   Later unchanged target observation did not reproduce it. Sink history is a
   real observer resource issue, not proved MPP leak/cause; don't rewrite it.
4. **Receipt-before-target-parkba56290** fixes real ACK liveness,93server+7
   controls. It is not proved5s cure; ordinary speed/timing tradeoffs retained.
   [Receipt report](TARGET_WRITE_RECEIPT_ORDINARY_20260910.md).
5. **Return-round364d417** removes impossible two~100ms proof rounds in one
   ~175ms budget. Restricted DOWN202→385/UP340→416, but outage gaps worsen.
   No timer rescue; [round evidence](RETURN_ROUND_ORDINARY_20260910.md).
6. **Shared native contention and mixed copy service** remain conditional costs,
   not all declared physical necessity. Post-refill mixed outage gap1.21s has
   native progress; exact64B observer shows150ms before TCP repair admission and
   968ms after, not a late wake. Q-only recovers~449Mbps after sole-UDP outage,
   but loses echo availability; no fixed QUIC preference follows.
   [Recovery attribution](NATIVE_REFILL_RECOVERY_20260910.md).
7. **Restart/ownership/lifecycle** have real focused checks and1941mixed+64single
   churn reclamation. Deployed random RAM/CPU incident is not fully attributed.
   No hypothetical re-audit or erased adverse results:
   [dispositions](CHANGE_DISPOSITION_20260907.md),
   [reflection](PERFORMANCE_REFLECTION_20260907.md).

Current complete12cell harsh asymmetric matrix is retained in
[NATIVE_REFILL_COMBINED_20260910](NATIVE_REFILL_COMBINED_20260910.md):
DOWN TCP/Q/mixed/raw/Xray/H2=404.684/311.418/312.384/363.575/299.315/144.912Mbps,
with all failed echoes/tails. UPQ96.986/mixed62.511/raw7.293 settle; TCP and H2
are guard-censored, Xray terminal-closure censored. No completed-speed ranking
of incomplete results. This poor-all-baselines case is diagnosis, not a public win.

## Unchanged global acceptance order

1. Close material mixed allocation/startup/stalls with exact causes and ordinary
   completion, first service, time series, read gaps and loaded latency.
2. Both directions/all3MPP modes under changing loss/jitter, sudden QoS,
   blackhole and restart-free recovery, with their ablations.
3. Single500Mbps and independent200Mbps links, shared/asymmetric cuts,
   aggregation plus simultaneous changing impairments.
4. Cold/warm short objects, sustained single/concurrent work and actual
   speed.cloudflare.com; matched rawTCP, Xray, H2, MPTCP where available.
5. Restart/churn, post-load reclamation and CPU/RSS; platform checks.
6. Only then publish honest README/PERFORMANCE timing/latency curves, costs and
   completion, and release after practical competitiveness. No universal
   instantaneous optimum under unknown future events; no avoidable-stall waiver.

Pinned harsh profile: routed500Mbps, UP70±20ms/DOWN30±5ms; five-second loss
UP[3,8,5,6,10,3,5,8]%mean6, DOWN[1,2,.5,3,2,.5,1,2]%; UP10Mbps15–25s,
wholeUDP30–33s.40s offered,85s runner/90s probe guards. No changing it to pass.
Healthy/heterogeneous phases, not a100Mbps threshold, determine usable speed.

## Execution safeguards and continuity

Preserve prepared/claimed ownership, receipt truth, exact incarnation/copy slots,
rank/Apply extent, nonrenewing clocks, half-close/cancel and current capacity
wakes. No guessed bandwidth, fixed protocol preference/bottleneck partition,
new resource cap, or revived rejected candidate without contrary evidence.
Correctness, sustained useful delivery, latency and wire/resource costs are
joint acceptance; neither component GREEN nor high average Mbps suffices.

Frozen binaries under .tmp/reflection/bin:
- native-refill-20260910: ordinaryb0 comparator.
- authoritative-gap-service-request-20260910: failed ordinary79ddb41 trial.
- authoritative-gap-service-profile-20260910 and wrapper: diagnostics ONLY.
- target-write-receipt-20260910: ba56290; return-round-20260910:364d417.
Other previous observers/ordinary comparators remain, not cleanup targets.

No sudo, host shaping, outside-root work or /mnt/storage use. Owned Docker only.
No deletion in this transaction. User's seven-line
docs-dev/LIVE_OWNER_FRONTIER_WORK_BOUND.md remains untouched/unstaged.
Use exact intermediary commits; docs-dev needs exact force-add. PROGRESS is an
ignored continuity journal, not force-added. AGENTS.md is immutable.

Telegram failed ordinary/CPU attribution update sent15:17UTC; next nonurgent after16:17UTC.
Meaningful measured milestones/blockers only; commentary within60s, minute
lab/build polling. Before compaction preserve exact active session, next
decision, source/binary identities and adverse/open outcomes. Continue, do not
end execution because a candidate or checkpoint closed.
