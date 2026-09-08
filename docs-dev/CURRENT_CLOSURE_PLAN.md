# Current deterministic closure plan

Updated:2026-09-08 22:05 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before every
transaction and after compaction. This is the active scope/decision ledger,
not a new issue inventory. Full pre-condensation chronology is retained at
`git show 13876d6:docs-dev/CURRENT_CLOSURE_PLAN.md`; the subsequent observer18:56
entry is preserved below. Earlier history, including rejected approaches,
remains at `git show ebad57f:docs-dev/CURRENT_CLOSURE_PLAN.md`. Linked reports
retain exact ranges, raw timing bins, costs, RED/GREEN logs and observer patches.

## Active transaction: user-requested mixed-mode architectural redesign

**Ordinary diagnostic outcome22:10:** mixed down's longest gap improves
7.418→3.720s, but first body0.546→1.055s and echo42/75→30/75 are adverse.
Echo disconnects at18.477s rather than24.362s; faster bulk cannot hide lost
service. Mixed up settles419823616 exact bytes/49.378940s, but maximum
confirmation gap is5.164575s versus4.004233s in the current ordinary control.
This is a mechanism-correct candidate, not a practically accepted stall fix.
No gain/window/queue/controller compensation follows. Preserve the complete
report/raw result and commit the independently verified ownership migration
as an intermediary checkpoint, explicitly withholding performance acceptance.

**Next discriminator / forecast:** use the existing reorder-recovery scenario:
500Mbps, initial3% forward/1% return loss and70/20+30/5ms delay/jitter; at8s
remove loss/jitter but keep500Mbps and100ms RTT for the remaining32s.
Compare ordinary control mixed, candidate mixed, candidate QUIC, then Hysteria2
sequentially on that same scenario. This is a new restored-high-capacity
question, not a favorable repeat of the harsh pair: does retained mixed state
continue to damage service after impairment ends while singleton/baseline
recovers? Existing runner/probes only; keep priors and all40 bins, echo failures,
resource/wire data. A restored persistent deficit selects retained allocation/
recovery for exact attribution; comparable prompt recovery rejects that claim
for this scenario and leaves the harsh receive/native boundary unresolved.
No universal recovery, heterogeneous aggregation or final release claim from
four finite cells. Root serializes labs and builds; no runtime change for this
discriminator. Record actual recovery timing, not just40s averages.

**Mechanism checks22:07:** the final test-only fixture drives the actual scoped
server Native metrics service, rather than a one-time staged shape. Both legal
QUIC latency-notice cases now complete exact peer delivery; four lifecycle
checks pass in0.28s and all692 affected regressions pass in3.71s. Test build6
is warning-free39.06s. The ordinary binary remains release2: no production
change was needed for those missing fixture refreshes. The declared harsh
mixed-down comparison is running; no build overlaps it.

**Healthy outcome22:05:** all six ordinary control/candidate cells completed.
TCP411.785→420.820Mbps, QUIC433.960→427.082, mixed410.025→404.191;
there is no high-BDP pipeline collapse. Mixed maximum read gap0.551→0.399s
and echo p950.707→0.652s improve, but first body0.441→0.613s and server
process-lifetime-average CPU maximum158→196% are adverse. TCP first body
0.448→0.580s, echo p951.177→1.212s and server CPU73.5→118% are also adverse;
QUIC echo p950.165→0.215s. Single cells do not prove causal CPU attribution.
Full bins, failures, resources and startup remain in the response service
report; no clean improvement or release acceptance is claimed.

**Next bounded comparison:** finish the test-only actual Native cadence
fixture build and the unchanged affected lifecycle/regression cohort. Then
execute the already-declared mixed down/up harsh diagnostic cells on the
frozen ordinary candidate, without changing controllers, queues, hints or
impairment. The question is whether eliminating private pre-native ownership
reduces the captured ordering failure, not whether aggregate exceeds100Mbps.
An adverse pair stops promotion and selects attribution, not another tweak.
The upload handoff and already-native hold are still separate unresolved
mechanisms. Healthy adverse costs cannot be waived by a harsh-profile win.

**Integration21:36:** response source/binding/actor and actual TCP/QUIC writer
consumers now form one uncommitted candidate. Native writers acquire Product
only via prearmed try-lock; actor Product-to-Native repair remains legal because
there is no blocking reverse edge. Independent review withdrew its conditional
deadlock concern after checking those actual edges; no repair rewrite followed.
The first compiler pass exposed only the common claim enum's missing test import
and unused bindings, now corrected; this is integration, not a platform defect.
An independent review also caught two real candidate priority-path errors:
latency source notices are legal in QUIC's priority lane, including while an
exact probe awaits ACK credit. Such a notice now waits weakly for physical
writer availability rather than throwing Protocol; ordinary metadata refusal
does not withdraw idle Ready. The existing real QUIC fixture covers this
composition, without changing its ACK-drain-before-input ordering.

**Verification contract:** actual protected-writer prefix/started-owner/ACK/
cancellation controls, Native source/Busy controls, EOF-with-U and post-FIN
repair, then affected request/server/QUIC regressions. Build one ordinary
candidate with diagnostics disabled only after those pass. Compare its
TCP/QUIC/mixed healthy cells against the three frozen controls below, then
mixed download/upload in the unchanged harsh diagnostic profile against the
current ordinary baseline cohort. Preserve all timing bins, failures and costs.
Any high-BDP underfeeding or worse service stops performance promotion; do not
compensate with a gain/window/queue adjustment. An already-native hold and the
separate upload receive-handoff boundary remain open even if prefix placement
passes. No new model is accepted or speed improvement claimed yet.

**Checks21:39:** seven real prepared-writer/Native controls pass, as do601
sender/response/TCP/QUIC/control regressions. Actor controls give87pass/1failure;
the new EOF test also fails. Review identifies fixture assumptions: EOF watches
only A although unchanged control placement may select empty B; the old idle
source test expects SendFrame rather than driving its new real Ready claim.
Migrate those consumers and rerun the same assertions, not their deadlines.
An ordinary non-test build is running concurrently with test-only repair to
expose stale production-only dependencies; this expedites the earlier build
ordering but does not authorize labs before lifecycle checks pass. No labs
overlap compilation. Independent audit also identifies potential extra actor
turns and two selection projections per claim; healthy timing/resource results
must determine their material cost, not speculation or a compensating tweak.

**Lifecycle21:51:** corrected actor consumers now reach closure and exact
post-FIN repair. The expected128-byte statistic initially returned130:
the inherited4f584213 non-reinjection branch counted one resource-accounting
unit per FIN/replay as payload. Only that nondata payload-stat mutation and its
unused plumbing are removed; queue charges, budgets and wire behavior remain.
EOF/recovery and idle-source requalification then pass. QUIC Busy preserves
Ready and writes successfully. The deferred-probe test proves no Protocol,
unchanged U/cache and actual ACK receipt, then exposes Native
TransportSourceChanged with C=0/U=26/cache=0. Its helper-only fixture omitted
the real server's metrics cadence: refresh scheduling shape, fenced registry
stage and binding fanout. Replay that actual lifecycle and consume its retained
wake; do not fabricate capacity or change the timeout. No delivery failure or
new production Native defect follows from a never-claimed source.

**Execution deviation21:55:** the latest real-lifecycle cohort passes691/692;
the repaired deferred-probe case passes, while Busy's later single claim now
hits the same missing fixture cadence (its Busy/Ready/wake assertions passed
and it previously delivered). Completing the shared real cadence fixture is
test-only. Run the already-built/frozen ordinary candidate's three healthy
cells now, with no compiler or runtime edits in parallel, instead of blocking
real-service observation on another fixture-only rebuild. This is a diagnostic
acceptance step, not a waiver: all checks must pass before committing runtime
or promoting performance. Source identity: response-claim-20260908, default
features, including exact C-only payload stats and no observer/controller edits.

**Current decision21:06:** the two frozen captures are complete. UP's current
useful replies have1.112s already-decoded residence and a6.403s frontier hold
overlapping substantial reader handoff awaits. Those predecessors are untyped;
neither ACK-handler dominance nor a three-owner-lock cause is established.
All915 measured owner sections peak at13.405ms, and all101 useful mux advances
reach local delivery within13ms. Do not implement the paused-planner separation
as a seconds fix. Preserve the receive-service boundary as unresolved.

DOWN supplies a separate material sequence: at Unix1788871829040,128 TCP
Originals cover an exact7,714,742B lower prefix, all published but not yet
writer-started. QUIC then positively writes57,120,620B of later Originals.
The receiver takes9.486s to finish that lower prefix and releases a60MB suffix.
This demonstrates premature private-writer assignment beside real alternate
service. The largest individual3.589s hold was already native-accepted; neither
that hold nor the losing9.850s TCP queue residence is the promised gain.

**Replacement / prospective benefit / falsifier:** pursue response Original
claiming at the actual writer boundary, sharing the existing request ownership
contract, not a new ranking formula or live-copy fanout. Until an imminent writer
claims them, lower source bytes remain shared U; C/cache/exact Original ownership
commit together. Earlier QUIC opportunities can then carry lower bytes rather
than a stranded-prefix suffix. The practical forecast is fewer multi-second
ordering sequences and less useless reassembly, not a precise Mbps or9.486s
saving. Physical loss, already-native queues and feedback changes can leave
the gain zero; an adverse or ambiguous ordinary pair stops promotion.
Preserve full configured high-BDP authority, native controllers, source/ACK/FIN
conservation, qualification/startup, chosen-instance fences, weak cancellation
and current request semantics. No shrinking queue/window to force this result.
Independent reviews agree the source/cache owner and writer consumers must
migrate together; merely marking a fixed-target command prepared is invalid.

**Evidence hygiene:** all five prospective structural test/seam files are
archived in MIXED_SERVICE_STRUCTURAL_RED_20260908.patch and reversed; its clean
apply check passes. Their two intentional REDs are not left in CI. Runtime is
again unchanged d999fea, all observer hooks reversed. Raw result/build/test/run
files are being preserved in MIXED_RESPONSE_PLACEMENT_SERVICE_20260908.raw.tar.gz.
No build or lab is active. Source implementation requires the reviewed atomic
claim contract and one coherent integration; no implementation is accepted yet.

**Implementation transaction21:16:** the reviewed response claim migration is
now in progress, split by ownership: response source/claim/binding, server actor
integration, generic weak notices/native writers, and independent real protected
writer controls. No candidate build or lab yet. Preserve the full membership
when selecting Ready candidates: an occupied fresh loser must not mask a stale
Ready survivor. This carries the already-proved request fallback rule into the
new response prepared entry; simply replacing queue eligibility with Ready while
retaining that mask would revive the b783cd6 liveness counterexample. Legacy
non-prepared callers retain their policy. No new rate or score tuning.
All16 raw archive members pass gzip integrity and byte-for-byte comparison;
the report's100 timing bins and29 successful echo latencies match probe files.

**Healthy comparator forecast21:21:** while the disjoint source migration is
being implemented (no compiler), freeze ordinary d999fea TCP/QUIC/mixed download
service on the existing500Mbps/100ms routed profile with loss/jitter/QoS/outage
explicitly disabled. This is the healthy high-BDP ablation, not replacement of
the harsh diagnostic or final real-Internet proof. Existing runner and full
body/echo timing only; tag response-claim-healthy-control-0908. The candidate
must preserve singleton and mixed pipelining here, not merely reduce the harsh
gap by underfeeding native service. One control per mode now, no favorable
reruns or diagnostic binaries; root serializes labs and subsequent builds.

**Healthy controls21:25:** all three ordinary cells complete their40s observation.
TCP/QUIC/mixed rates are411.785/433.960/410.025Mbps, maximum read gaps
0.402/0.101/0.551s, first body0.448/0.414/0.441s. Echo succeeds48/48,80/80,
74/74 respectively; p95 is1177/165/707ms. The rates are healthy, but slower
loaded interactive service remains visible; no throughput-only acceptance.
These are the candidate's high-BDP controls, not a random-Internet comparison.
No lab/build is active after the third cell. Telegram attribution update sent
13:25UTC; next nonurgent not before14:26UTC. Stable notification keys:
task=mptunnel-performance-closure, session=mptunnel-20260908.

**User direction20:33:** the all-baselines-poor combined profile is diagnostic,
not a final performance claim or a100Mbps objective. Keep the stall and default
mixed-mode defects as the priority; prove the replacement rather than optimize
an arbitrary aggregate in this harsh environment. Final performance needs
healthy/high-capacity and restored heterogeneous paths as well.

**Current transaction / forecast:** two bounded structural controls are being
prepared independently: real prepared-claim versus ready response delivery,
and real response publication to an unconsumed writer versus a newly usable
writer. Root first checks whether current d999fea captures retain material
already-received return-byte residence; historical2.226s is an opportunity,
not a current gain forecast. The controls prove dependency/ownership, not
seconds saved. Current small replies have2–4ms publication-to-write residence,
so response parity is not automatically the first performance change.

**Decision / falsifier / verification:** a present multi-second useful-byte
hold caused by opposite planning selects directional receive/commit ownership
separation. Prompt local service rejects that causal attribution for the
interval and selects the existing ordered-prefix/recovery question instead.
No arbitrary paused mutex becomes a practical performance proof. Before any
candidate, require exact earlier legal service and preserve ACK/credit,
attachment/terminal, source/copy and chosen-native fences. Then one attributable
implementation, focused actual-producer controls, and ordinary current/candidate
timing plus healthy high-BDP service; no congestion/threshold changes. Root
owns builds/labs; agents own disjoint proposed test files only after confirmation.

**Current discriminator20:40:** existing d999fea native/forward captures lack
return decode/mux/write events;91/80 reverse ACK rows supply no hidden positive
client-receive-before-write bound either. Reuse the reviewed18-file reply
observer, adding only acquisition and section elapsed at the three response
Product-owner cuts, logged after guards release. Timings include descheduling,
not exclusive mutex/CPU time; early terminal exits may leave a section censored.
One frozen d999fea diagnostic mixed upload on the unchanged harsh profile
asks whether current useful replies spend material time at those local cuts,
earlier ingress service, or before receipt. This determines whether the
direction-isolation implementation is the material first change. A tiny
measured acquisition/section rejects that attribution for the captured hold;
do not promote a model-only barrier RED into a performance claim. Archive and
reverse the observer after its build, before capture. No protocol, rate, queue,
controller or task-ownership change in this observer; no final Mbps claim.

**Structural controls executed20:46:** both intended model-contract assertions
fail after their real-producer and cleanup/accounting prerequisites pass.
Response: healthy B admits/writes/receives its actual source, but unconsumed A
still owns131072B; A-already-written opposite passes. Receive: actual prepared
claimant's paused planning prevents otherwise-ready response delivery;
uncontended opposite passes, with eventual delivery/ACK/conservation verified
before the intended assertion. These are two prospective contract REDs, not
current seconds or ordinary throughput proof. Five test/seam files only,
all new runtime seams cfg(test). No production model change yet.

**Capture execution:**18-file reply+three-owner-timer overlay built warning-free
in1m31s, frozen as receive-owner-service-20260908, archived separately and fully
reversed before1m37s default-feature test build. Both2-test filters each give
1pass/1intendedfailure. One unchanged mixed-upload diagnostic is now running;
no build overlaps it. Current source contains only the proposed test controls.

**Current owner result20:49:** capture completes397344768B/59.830669s,
maximum confirmation gap6.403523s. All915completedowner records (305percut)
bound acquisition/whole section at13.040/13.042ms batch-bound,
13.393/13.405ms receive-feedback, and0.486/0.489ms write-poll setup.
This rejects the three owner-acquisition cuts as a multi-second cause in this
capture. The structural dependency RED is not promoted to a practical fix.
Exact useful return arrival/routing joins are being completed; no new lock
mechanism is justified by the failed aggregate alone.

**Next existing mixed boundary / information forecast:** reuse the same frozen
reply observer in one mixed download (no new build/hooks) on the unchanged
diagnostic profile. The ordinary mixed-down7.418s gap/64MiB separation lacks
exact Original publication, protected writer, decode and ordered-prefix joins.
The existing observer provides those response-direction cuts. A useful prefix
held before writer acceptance beside earlier real alternate service selects
late response placement; prompt publication/write excludes it. Already-native
or already-decoded waits select their respective existing boundaries. No
inference from A's synthetic pause duration, no harsh-profile speed target,
and no candidate until the material removable interval is identified.

**2026-09-08 19:52 +08:00 — scope:** the user explicitly asks why mixed-mode
still fails and requests a careful comprehensive redesign. This supersedes
another narrow native observer as the automatic next action. Runtime remains
d999fea; no new controller, threshold or deployment change is authorized by a
slow result alone. The completed native discriminator remains evidence below.

**Observed failure / exact question:** default mixed upload still has5.50s
confirmation gaps in its latest ordinary run; previous mixed download loses
interactive service despite substantial bulk throughput. Does the existing
allocation/ownership/recovery/feedback composition make avoidable cross-carrier
ordered-service dependencies, and which minimal replacement contract removes
them without disabling useful aggregation or restoring old hard admission?

**Alternatives / information forecast:** compare immediate independent-writer
claiming, a common ordered-service placement policy above unchanged resource
authority, and a change of native ordering scope only if evidence requires it.
Existing exact frontier and reader captures can establish or reject proposed
dependencies without another lab. The useful output is one coherent model,
explicit impossibility limits, retained/deleted responsibilities, a benefit
forecast and decisive old/new counterexamples. It is not an expected speed gain
from documentation. Do not promise to remove physical queue-drain or unobserved
packet-loss time; seconds-long policy-induced stalls are the material target.

**Smallest next action / stop:** root reviews RFC/history/code while independent
audits challenge authority, ordered service and causal evidence. Persist a
bounded redesign proposal and amend normative RFC text only for a demonstrated
wrong contract. No implementation before counterexamples, failure/recovery and
both-direction composition are covered; no new topology-estimator framework.
Then freeze the comparison basis before a candidate so a full product-level
baseline cannot again be displaced by a chain of local corrections.

**Redesign review20:04:** root inspected the65edae3 origin, current RFC10/15,
request prepared claims, response queue-time commitment and receive mailbox
boundaries. Three independent reviews separate placement, local receive service
and physical/native limits. [Proposed replacement](MIXED_SERVICE_REDESIGN_20260908.md)
retains exact ownership, configured pipeline and controllers; it explicitly
does not claim a specified allocator or authorize speculative performance-Defer.
Current ordinary comparison is now the first execution step, not another fix.

**Comparison forecast / execution:** frozen d999fea ordinary binary,
redesign-baseline-0908 tag. Existing runner/profile only, diagnostics disabled;
downstream phase runs TCP,QUIC,mixed,raw,VMess,H2, then upstream mirrors the
whole impairment. One initial cell per product/direction, no build overlap.
This separates current mixed-mode interaction from single-carrier limits and
provides an honest baseline snapshot; it is not a statistical universal ranking.
Retain all gaps, echo failures and censored outcomes. Stop only the failed cell,
verify its owned probe has exited before proceeding, and do not change profile
or protocol to obtain favorable results. No runtime candidate yet.

**Comparison complete20:20:** all twelve ordinary cells have outcomes;
TCP/H2 upload reached the existing settlement guard and their probes exited
before continuation. VMess upload ended before exact terminal confirmation.
No lab/build remains active. [Full current cohort](REDESIGN_BASELINE_20260908.md)
replaces historical controls as this transaction's comparison basis, not as
a statistical ranking or public release gate. QUIC/mixed downstream rates
69.511/37.212Mbps and gaps4.838/7.418s; upstream97.046/66.846Mbps, gaps5.294/
4.004s. All MPP downstream persistent echo probes time out; raw/VMess preserve
80/80 attempts at lower throughput. H2 also fails severely. Mixed is not
uniformly worse on every timing measure; single-mode stalls remain material.

**Architecture audit / next decision:** current response delivery takes the
request Product owner before reassembly and first local-write polling while
request claims plan under that same mutex. Independent review confirms the
dependency, not a present multi-second duration. Separate directional receive/
write ownership from expensive opposite planning; preserve small authoritative
attachment/terminal/ACK publication transitions. Before implementation, the
real claimant/receiver control must prove dependency and current useful-byte
residence must identify the material removable portion. No three try-lock
tweaks or guessed capacity. Response late placement is a separate boundary:
current small-reply captures bound publication-to-write to2–4ms, so it must
not be promoted as the upload-seconds fix. A bulk-response alternate-writer
counterexample remains needed. Both findings and exact preserved prerequisites
are in the redesign proposal. No further Native tuning or new audit inventory.

## Completed discriminator: native ordered-read service during recovery

**Issue / observed failure / exact question:** the completed d999fea forward
capture has a 6.095822s confirmation gap. In restored capacity, QUIC Original
[379841438,379853438) completes its local write at Unix1788865120575 but
decodes at5124171 (3.596s); mux follows4ms later. Its current-frontier hold is
1.270544s, not the whole residence. Was its native reader waiting for absent
ordered bytes, or were available bytes waiting for local polling/parsing?

**Existing evidence / competing causes:** exact forward claims, local writes,
decodes and ACKs reconcile. Repair route residence is tiny, but an ordinary
reader can wait on its input channel. QoS-period repair delays also coexist
with6.151MB queued on the shared10Mbps cut (4.921s aggregate drain), so do not
classify that physical effect as an MPP defect. During restored-capacity rows
41–43, the router backlog is only92–101KB at500Mbps and native QUIC ACKs barely
advance; Native queued bytes and missing offsets are not in those gauges.
A separate2.169s frontier gap has2.076s before Original claim and only93ms
write→decode/mux; preserve this assignment/feedback boundary, not all-Native blame.

**Smallest model / action / falsifier:** diagnostic-only ordered-read episodes
start at the wrapper's actual Poll::Pending, not a partial read that returns
bytes. Under the already-held native guard, retain exact connection/stream
identity and missing native offset r. First successful validated ingestion
covering r records availability; first nonempty ordered Chunk closes the
episode. Record producer times; repeated pending at the same r adds no state.
Cancellation/terminal episodes remain censored. Native offsets include H3
framing, not Product DSN; join connection/H3 identity and retain ambiguous
frame/chunk mappings. Early availability with late return selects local
service; late availability with prompt return selects pre-processed-receipt
delay, not automatically packet loss/PTO. No sender packet trace yet.

**Acceptance / stop:** reuse the exact Product forward/repair observer plus
only these sparse native transitions. Root and independent review before one
build; archive/freeze/reverse before one unchanged capture. No runtime policy,
queue, rate, timer, sampler or RFC correction is justified yet. No favorable
rerun or performance promotion from diagnostic Mbps/non-reproduction. Prove
an actual causal defect before implementing a correction.

**Execution19:18:** root and independent whole19-file reviews pass. Sparse
Native episodes preserve actual no-data Pending, validated ingestion and
nonempty Chunk boundaries; all bindings are lock-free existing identities.
New diagnostic fields are cfg-only, initialized/reset on every Recv lifetime.
Native clock/sequence are distinct from Product logs; canceled reads remain
broad local-service/censored cases. NATIVE_READ_SERVICE_TRACE_20260908.patch
archives the complete temporary overlay and validates as a reverse diff.
One diagnostic build is running, no lab. Freeze/reverse every source/Cargo
observer change before the unchanged capture; no runtime correction yet.

**Classification correction19:23:** the user's challenge is binding: an
unideal result can be physical loss/queuing, a throughput/latency tradeoff,
or a real defect. Q/C does not locate the blocking byte, and an already-created
queue does not prove earlier sending decisions optimal. Historical matched
raw completes4.279Mbps/maxgap1.611s; VMess and H2 are incomplete (H2 at the
observation boundary), with unequal work and no current exact MPTCP counterpart.
These do not establish an MPP-specific defect or universal superiority.
Compare matched controls before further model corrections; the current sparse
trace only separates Native processed-input availability from local service.
The mandatory method now explicitly records this classification rule.

**Capture19:23:** warning-free3m57s diagnostic build frozen separately as
native-read-service-20260908. All19source/Cargo changes reversed and their
diff verified empty before one unchanged-profile mixed upload; no build/lab
overlap. Exact runtime remains d999fea; target/release is diagnostic-only.

**Method requirement19:39:** before any next experiment or implementation,
record expected material user benefit, its removable critical portion,
plausible range/upper bound, assumptions, uncertainty, costs and falsifier;
compare the actual outcome against that forecast afterward. Diagnostics need
an explicit information/decision forecast instead of a promised speed gain.
Defer unsupported or negligible opportunities; correctness-only necessity
must not be presented as a performance improvement. This is mandatory in
PERFORMANCE_METHOD_AND_LESSONS, not a new numerical Product threshold.

**Completed native result:**391184384 exact bytes settle in49.043824s;
maximum confirmation/write gaps are3.594738/6.955278s. All553 completed
native Pending→availability→Chunk episodes reconcile. Availability→return
is at most5.457ms (p95 440us), excluding that local step as a multi-second
wait within these episodes. The winning repair for F165558377 has3.021829s
before processed head availability and72us afterward. The longest9.567s
native wait is not a useful winner: source work was initially absent and
TCP already advanced the frontier before that QUIC decode. Do not attribute
all native elapsed time to useful blocked work, transport defects or physical
loss. No runtime correction or ordinary performance promotion follows.
Root whole-report review and independent accounting/cost checks pass;
[exact joins and full timing history](NATIVE_READ_SERVICE_20260908.md) retain
the raw archive and observer patch. No next experiment is running. The next
causal decision must distinguish missing processed input from sender service,
physical loss/queuing and earlier placement; this observation alone does not
select a correction or justify another diagnostic without an information
forecast. Previously observed preclaim/return holds remain separate boundaries.

## Completed discriminator: forward prefix after ACK work reduction

**Observed failure / question:** ordinary d999fea still has5.497708s maximum
confirmation gap. QoS T234499808/Rs556 stay fixed for5.999s at server
Unix1788863964962→3970961; S298391872→301608672, Rc528→556. During separate
outage/recovery L32–36, S367394816/T300285952/Rs=Rc738 stay flat4.001s with
exact64MiB S−T. TCP Recv-Q is0, TCP native ACKs advance7.31MB, but exact
QUIC physical3 native ACK and62.97MB Product-debt counters remain flat.
These facts select forward-prefix service, not all-Native or local-reader
attribution. Which exact Original/copy owns the blocking byte; is it unclaimed,
not locally written, predecode, or decoded but not delivered to target?

**Model / existing evidence / alternatives:** preserve source A, claimed C,
retained Original/copy ownership, native acceptance and receiver ordered F as
distinct boundaries. Aggregate S−T and queue gauges cannot reconstruct C/F or
the winning carrier. Existing QOS_FORWARD_PREFIX and QOS_REPAIR_READ observers
already cover these boundaries, including repair-reader read/route residence.
No new cost/claim framework or Native-controller inference is necessary yet.

**Smallest next action / falsifier / stop:** reuse the archived11-file exact
forward+repair-service observer on unchanged d999fea, omitting noisy general
reinjection events. One unchanged diagnostic after independent review and
archive/freeze/reverse. Join all Original claims/write/decode and ACK totals,
then the actual winning-prefix interval. Prompt local delivery after decode
excludes that stage; source/commit absence selects assignment rather than
transport. Predecode alone still cannot assign Native loss/timer blame.
No new or resurrected correction before a reachable real counterexample;
diagnostic rate and non-reproduction are not ordinary improvement.

**Observer18:56:** root and independent whole11-file reviews pass. Exact
forward/repair hooks reused without policy, queue, lock or await changes;
fresh archive POST_ACK_FORWARD_TRACE_20260908.patch preserves valid diff
metadata. Repair read time includes scheduling/unavailable work; server F is
mux release, not target-write completion. Product ACK debt and Native pending
bytes have deliberately separate lifetimes; different counters are not a leak.
The diagnostic was frozen and its hooks reversed before the unchanged capture.

**Execution update:** warning-free3m32s diagnostic build completed and frozen
as post-ack-forward-20260908. All11observer files were reversed and the source
diff verified empty. The unchanged mixed-upload capture, post-ack-forward-0908,
completed:436797440 exact bytes/48.135091s, maximum confirmation/write gaps
6.095822/5.560319s;49 raw one-second bins and49 service rows. This is diagnostic
evidence, not ordinary acceptance. [Raw archive](POST_ACK_FORWARD_SERVICE_20260908.raw.tar.gz)
preserves exactly five results plus build/run logs; gzip integrity, exact member
list and byte-for-byte source comparison pass. No build or lab remains active;
target/release is diagnostic, not ordinary d999fea. Exact causal joins and the
next discriminator remain root-owned.

## Comparator source and retained ordinary disposition

Comparator runtime **d999fea** restricts request ACK-release work to exact support;
RED **0683286**,546focused checks. Parent **b783cd6** separates prepared stale
preference; RED **f1b0900**,544checks. Earlier **e476308** makes the two advisory
native-writer Product acquisitions nonblocking; RED **584b748**,542checks.
**9ea25e2** indexes exact additive copy debt; RED **3396087**,538checks and
evidence checkpoint **ebad57f**. All are mechanism checkpoints, not acceptance.
No shelved sampler, Native observation-sharing policy or congestion tuning is active.

| Ordinary pair | Parent → candidate exact bytes / seconds | Maximum confirmation / write gaps, seconds | Disposition / evidence |
| --- | --- | --- | --- |
| b3dfef1 → 9ea25e2 |93570384confirmed /180748288accepted /85.948541 →254083072 /48.973579 |66.779472 /15.796057 →11.042148 /1.011139 |Parent incomplete at guard; candidate settles, but11s gap remains. [Copy debt](COPY_DEBT_SERVICE_20260908.md) |
| 9ea25e2 → e476308 |355532800 /44.964483 →409796608 /49.163739 |5.052667 /1.356326 →6.197568 /5.462440 |Timing mixed/adverse; no promotion. [Advisory owner](ADVISORY_OWNER_SERVICE_20260908.md) |
| e476308 → b783cd6 |389218304 /47.562042 →326041600 /52.965807 |4.845539 /3.553143 →6.082958 /2.223001 |Less work, slower completion/larger confirmation gap; no promotion. [Stale preference](PREPARED_STALE_SERVICE_20260908.md) |
| b783cd6 → d999fea |473104384 /47.992259 →393150464 /46.768581 |3.060424 /4.396938 →5.497708 /5.665049 |Less work and worse gaps; no promotion. [ACK support](ACK_SUPPORT_SERVICE_20260908.md) |

Different work, duration and random packet realizations prevent causal rate
ratios or uniform non-regression claims. Preserve every adverse phase; no
favorable third run and no rollback to a proven defect from one average alone.
For9ea25e2, first3s target service worsened while10s service/settlement improved;
client/server peak RSS511988/90968→677496/129508KiB with more work. Parent reset
followed guard teardown; missing confirmation bins are unavailable, not zero.
Forb783cd6,10s target service improved but15s worsened; client peak RSS775264→
832336KiB. Detailed first-service, full-bin, socket and cost evidence remains
in each linked report and its raw archive.

### What the recent corrections prove

- **d999fea — ACK support:** fd32e60/f4206d0's global flight snapshot settled
  exact copies;765683b preserved byte-exact ambiguity. Only starts<H=max ACK
  end can contribute release/evidence. Extract that prefix, preserving crossing
  flights and their H-bucket precedence; leave unrelated keys untouched.
  When every key is eligible, keep the whole-map fast path. Real131072B Original
  plus14600B copy fixture failed only at66versus3record work; corrected cases
  each process2records with identical release/proof/receipt/debt/epoch/order.
  No ACK coalescing, policy, timer, cap or RFC change. Existing exact-event
  replay independently bounds4470499 needless suffix visits across6367ACKs;
  this is not measured CPU savings. [Evidence](ACK_SUPPORT_SERVICE_20260908.md).
- **b783cd6 — one stale preference:**5d660f3b's legacy fresh-output mask ignored
  Ready;9720e4b's finite claim tiers could not restore a masked stale survivor.
  Prepared claims now consume unmasked resource observations, then unchanged
  full-membership Ready/fresh/backup tiers and W/P/E/final fences. Legacy
  callers retain their policy. Actual stale-Ready claim RED0→65536B, fresh-Ready
  opposite and no-requalification checks pass. Positive fixture is FirstPath;
  E exhaustion is component coverage, not a falsely claimed facade test.
  [Evidence](PREPARED_STALE_SERVICE_20260908.md).
- **e476308 — advisory owner:** actual held-Product producer tests failed at
  both advisory acquisitions and cancellation, with cleanup guards rather
  than Product latency thresholds. Existing freshly prearmed try-lock/Busy
  now applies at both cuts; final Native fence, source, readiness and
  cancellation remain unchanged. No blocking reverse Product lock or new
  fallback permission. This does not attribute every local hold to contention.
  [Evidence](ADVISORY_OWNER_SERVICE_20260908.md).
- **9ea25e2 — additive copy debt:** actual dispatcher emits the same4096B
  repair from69632retained bytes with two queries; irrelevant fragmentation
  raised4→130record visits before RED. J_i=sum of retained copies on exact i
  is now maintained on append, ACK split and drain; overlap multiplicity,
  expiry/Native-ACK nonrelease, zero-key retirement and replacement identity
  remain exact. Full-scan lifecycle oracle and independent audits pass.
  [Model and ordinary comparison](COPY_DEBT_SERVICE_20260908.md).

## Completed discriminators — findings, not new fix queues

- **Reply residence on b783cd6:**387579904B/52.491225s diagnostic,
  maxconfirmation3.508291s. Winning[863,877) spent3.247s server-read→delivery,
  including599ms after authenticated decode; preceding nondata reader send
  awaited1.961027s within1.965s. Real local backpressure is established, not
  ACK-only CPU attribution or an all-Native explanation. This selected the
  existing ACK-work question; no new cost framework was added.
  [Reply residence](REPLY_RESIDENCE_20260908.md), evidence checkpoint **bd3b810**.
- **Terminal service:** ordinary b783cd6's6s postfull-target tail was not
  reproduced. Diagnostic424017920B/56.939769s,maxconfirmation7.996908s.
  EOF had C391950079+U32067841=final; FIN cannot omit U. Same-offset FINs
  published with24.47MB retained cache, disproving a cache-zero prerequisite.
  Server initially pended FIN, then final Data made it ready; target shutdown
  and final13B reply read were prompt. Duplicate FIN replay is normal.
  The8.909s source-EOF→publication and1.258s write→decode intervals remain
  composite. Nonterminal replies were already read7.155s/15.686s before
  delivery. No FIN/RFC fix justified; earlier6s tail remains unattributed.
  [Terminal evidence](TERMINAL_SERVICE_20260908.md), checkpoint **cd97c5d**.
- **Prepared claim service:**484507648B/44.176499s,maxconfirmation4.478852s/
  maxwrite8.647851s.10428Originals and6367ACKs reconcile exactly; all Originals
  write/decode, four losing TCP copies lack positive completion. Largest
  server holds4.408347/3.514626s concern already-written QUIC Originals.
  Pending-source C329149108 held4.987s inside a9.273s selected QUIC write;
 353+TCP claim attempts exclude lost notice/global critical-front explanations
  for that interval. Separate sampled stale-Ready veto selected b783cd6;
  it does not explain the largest holds or prove every resource was free.
  [Claim/notice evidence](PREPARED_CLAIM_SERVICE_20260908.md).
- **QoS forward/repair service:** first diagnostic374669312B/65.783216s,
  maxconfirmation5.592993s;18779Originals and24009total commits reconcile.
  QoS F185133452/F187796428 holds3.432533/2.661961s release within1ms of decode.
  Late F298838412 was repaired9.751s after local acceptance;4.750s between
  repair decodes coexisted with6970816B ordinary QUIC decode on that connection.
  This excludes connection-wide silence, not native stream delay versus routing.
  Follow-up485359616B/52.139505s,maxconfirmation7.128953s:1407completed repair
  routes total41.466ms/max3.494ms; QoS winning-copy route27us excludes that
  route as cause there. Long7.094s read included unavailable work: actual copy
  was published only252ms before decode. A different F471135545 hold had
 14224071consumed but unclaimed bytes. Four losing TCP copies lacked positive
  completion, not Original loss/corruption. No Native/queue/threshold fix.
  [Both captures and raw archives](QOS_FORWARD_PREFIX_20260908.md).
- **Earlier copy-debt service diagnostic:**366018560B/43.726528s,
  maxconfirmation4.375291s did not reproduce ordinary11s. F639 was mostly
  predecode; F737 had1.203s local residence; F821 had>=2.722s preceding reader
  send-await overlap with different cost composition.73winning mux advances
  reached local write within4ms.4786flight releases took7.808784s within
 8.160194s ACK-handler elapsed; contained windows include246899/285275us.
  Nested/concurrent elapsed is not additive CPU, nor proof of a sole cause.
  [Exact joins and cost limits](COPY_DEBT_SERVICE_20260908.md),
  [prior sparse evidence](PREPARED_REPLY_SERVICE_20260908.md).

Read/write/decode timestamps belong to their actual producer stages. Server F
means mux release, not completed target write; local native acceptance is not
transmission/receipt. Shared-send completion may follow concurrent dequeue.
QUIC IDs are connection-local; TCP wire IDs and runtime indices differ, and
per-side physical IDs are not interchangeable. Join exact byte coverage,
including split/coalesced records. Preceding nondata waits are not ACK-only,
and unlogged/left-censored intervals remain unknown. Losing copies, native
pending bytes and Product debt have different lifetimes—not automatic leaks.

## Retained checkpoints and unfinished boundaries

| Mechanism / checkpoint | Proven scope; remaining disposition |
| --- | --- |
| Native reordered-packet/history corrections |Real encrypted packet counterexamples; jitter-only.585→185.313Mbps. Other network/recovery gates remain open |
| Restart / late STARTUP / lifecycle ownership |Scoped refusals/wakes/reclamation;1941mixed+64single-mode churn reclaims owners. Deployed random RAM/CPU incident still not fully attributed |
| Request retained recovery **011aee9**, unique ACK atoms **765683b** |Real ownership/attribution counterexamples; adverse/incomplete ordinary pairs retained, not promoted |
| Response retained recovery **953a54f** |Immutable per-assignment recovery with active/final quantities preserved; ordinary aggregate improves but gap worsens to6.080s |
| Ranked-prefix query **445011f** |Exact output, fewer irrelevant visits; average improves but gaps worsen |
| Direct structural recovery **1436ff4** |Actual global-byte-order/overlap-work REDs; allowance and independent-target service preserved; ordinary early target service adverse |
| Prepared request ownership **9720e4b** |U→Original at physical claim, no premature TCP assignment;533checks/audit, then ordinary75s return hold/incomplete settlement |
| Persistent idle readiness **b3dfef1** |Real two-writer all-refused retry recurrence removed;535checks/audit; long ordinary return/target holds remain |
| **9ea25e2 / e476308 / b783cd6 / d999fea** |Exact work/admission mechanisms proved above; respective ordinary comparisons remain unaccepted |
| Shelved paired-clock request sampler |Actual[2,2,2]→[2,3,4] correction; ordinary incomplete/adverse, not stacked into runtime |

Earlier exact ownership and admission evidence remains in
[request cohort](REQUEST_COHORT_ORDINARY_20260907.md),
[request prefix service](REQUEST_PREFIX_SERVICE_20260908.md),
[response handoff](RESPONSE_HANDOFF_20260908.md) and
[prepared ownership model](PREPARED_ORIGINAL_OWNERSHIP_MODEL.md).
Historical SEEN/UNSEEN labels are dispositions, not automatic new obligations:
[change disposition](CHANGE_DISPOSITION_20260907.md),
[reflection](PERFORMANCE_REFLECTION_20260907.md),
[practical acceptance](REVIEW_AND_PRACTICAL_ACCEPTANCE.md).

Preserve A=prepared-source end, C=native-claimed end, U=A−C and
B=U+sum Original debt. Claim U→O does not increase B; ACK cannot exceed C;
FIN waits for U=0. Failed protected writes retain exact ownership. Preserve
chosen readiness epoch, current exact qualification and final Native fence.
Product→Native actor calls coexist with Native→tryProduct writer calls:
no blocking reverse edge or guard across await. Deadlock freedom is not latency.
Structural byte order must not block independently eligible targets; retain
exact copy deadlines/J/Native Apply. **Response prepared-source parity now passes
mechanism checks, but its practical comparison remains adverse/mixed** as above;
this is not both-direction stall closure.

Rejected static ranking, relative ACK codec, ready-feedback batching,
wrapperless actor, absolute-delay reordering,3N1 and isolated raw-byte hysteresis
deletion stay rejected. Do not restore whole-ready-set invalidation or
metadata-refusal readiness churn; chosen epoch and physical idle lifetime
have real counterexamples. No suffix queues, ACK-per-frame structural allowance,
renewable deadlines, scalar same-host protocol preference or guessed capacity.
Original-QUIC-ignored claim was disproved by attachment timing.
Further Native observation-sharing is **not implemented**: an omitted better
target cannot be restored by chosen-target Apply. It is not equivalent cleanup.
No topology-inference framework, controller retuning or speculative inventory.

## Global gates — unchanged and not satisfied

| Order | Scope | Required evidence |
| --- | --- | --- |
|1 |Mixed allocation, upload sampling, cold/warm startup |Exact cause/model/real RED/control/audit/GREEN plus ordinary first-body/gaps/loaded latency/completion |
|2 |TCP/QUIC loss, jitter, QoS, blackhole/recovery |Both directions, same-request restart-free recovery; Native receipt versus ordered user service |
|3 |Aggregation/shared contention |Single500Mbps, independent200Mbps each, shared cuts, asymmetric3–10%mean6 loss/jitter/QoS/outage combinations and ablations |
|4 |Experience/baselines |TCP,QUIC,default; cold/warm single/concurrent/real speed.cloudflare.com; raw TCP,Xray,Hysteria2 with matched topology/configuration, including failures |
|5 |Sustainability |Restart/churn, backpressure, ownership, CPU/RSS and post-load recovery; reopen only on contrary evidence |
|6 |Publication |Full timing/latency series and costs/completion with goodput; README/PERFORMANCE and release only after competitive gates |

Pinned profile: routed/mirrored single500Mbps; upload70/20ms and return30/5ms
delay/jitter. Five-second upload loss[3,8,5,6,10,3,5,8]%mean6;
return[1,2,.5,3,2,.5,1,2]%. Upload10Mbps15–25s; UDP outage30–33s;40s load,
85s runner guard/90s probe boundary. Do not tune it to pass. Random realizations
are not packet-identical controls; bins above500Mbps can be buffered confirmation.
Matched mirrored review-mirrored-0906 controls remain historical context:
raw4.279Mbps/maxgap1.610879s completes; Xray/H2 incomplete. Nonmirrored
raw274.677Mbps is not a substitute; no redundant rerun just to obtain wins.

## Execution, evidence and continuity

- Root owns builds/labs. Response claim verification and the declared ordinary
  healthy/harsh comparisons are complete; no observer hooks are active.
  Owned Docker only: no sudo, host shaping, outside-repo work or build/lab overlap.
- Ordinary candidate:`./.tmp/reflection/bin/response-claim-20260908/mptunnel`.
  Ordinary comparator:`./.tmp/reflection/bin/ack-support-20260908/mptunnel`.
  Current diagnostic:`./.tmp/reflection/bin/native-read-service-20260908/mptunnel`.
  Previous forward diagnostic:`./.tmp/reflection/bin/post-ack-forward-20260908/mptunnel`.
  Previous reply diagnostic:`./.tmp/reflection/bin/reply-residence-20260908/mptunnel`.
  Do not use diagnostic target/release as an ordinary comparator.
- Exact intermediate commits only; preserve raw evidence before scoped cleanup.
  No deletion in this condensation. User's seven-line
  LIVE_OWNER_FRONTIER_WORK_BOUND.md edit must remain untouched and unstaged.
- Telegram last attribution/comparison report:13:25UTC; next nonurgent not
  before14:26UTC. Respect hourly minimum/soft-frequency advice; no component-only
  success notification or unfinished completion claim.
- Method reflection: symbolic conservation justified exact work removal but
  did not predict every timing phase. Follow the same winning-prefix evidence,
  retaining adverse first-service, settlement and resource observations.
  No plausible shortcut stack, favorable-average rerun or silent coverage waiver.
- Universal clairvoyant optimum under arbitrary future outages is impossible;
  this does not waive avoidable delay or practical gates. Never call unfinished
  work ideal or promise cost-free capacity discovery.
