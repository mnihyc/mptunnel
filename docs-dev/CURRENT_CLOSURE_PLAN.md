# v0.4.9 release closure plan

Updated: 2026-09-11 11:13 +08:00. Authoritative repository: ./.
Category: user-directed release convergence. No release acceptance yet.

## Active amendment: challenge claims, prioritize severe user impact

The user now explicitly requests proving/challenging the preceding conclusions,
including TCP on link 1 and QUIC on link 2, and correcting demonstrated severe
defects. Stable browsing/downloading/uploading/gaming is the objective, not an
ideal allocator. A speed deficit within 20% or an isolated gap under 3s is not
a tuning task in this batch. These are user impact triage guidelines, not new
runtime thresholds, permission to hide recurring disruption, or proof that
larger differences are automatically defects. Preserve all timings and failures.

Next transaction (before code or experiments):

- Issue/question: does native shared-cut competition adequately explain the
  observed mixed deficit, and can protocol-isolated links provide usable
  aggregation and recovery without prolonged application stalls?
- Existing evidence: healthy shared500 mixed about398--404Mbps versus single
  modes429/443Mbps is preceding011 evidence; external TCP raises QUIC latency
  without MPP TCP scheduling. Prior independent200+200 uses TCP AND QUIC on each
  cut, not protocol isolation. Current04f has an independently observed ordered
  prefix hole; neither that nor native competition explains every result.
- Competing causes: no extra physical capacity; unequal native achievable
  service; ordered-prefix/feedback coupling; local processing/resources;
  measurement or endpoint limitations. Do not assume which dominates.
- Information forecast: using the SAME current04f executable and ordinary
  probes, TCP46 alone, QUIC47 alone and TCP46+QUIC47 reveal whether extra cut
  capacity translates to useful delivery. This can falsify a blanket shared-
  bottleneck explanation, not prove a universal allocator or promised400Mbps.
- Smallest next action: six healthy200Mbps-per-cut cells (three modes, UP and
  DOWN), then four split-mode QoS/outage cells (TCP46+QUIC47 and reversed
  TCP47+QUIC46, both directions). Retain the shipped three-TCP carrier pool
  in both singleton and split controls; one QUIC per configured QUIC endpoint.
  Healthy has zero deliberate loss/jitter and UP70/DOWN30ms. Stress changes
  only46 in the transfer direction to10Mbps at15--25s (UP uses mirror=1;
  DOWN mirror unset gives DOWN70/UP30ms) and blocks UDP30--33s. This tests placement and recovery,
  not the entire random Internet. No change to existing guards or shapers.
- Only measurement change: an explicit client-config override in the existing
  scratch runner; archive it and exact path sets. No new harness or runtime
  diagnostic overlay. Verify actual interface/connection counts and service.
- Acceptance/stop: retain full receiver-confirmed series, completion, all
  stalls, loaded echo, wire/CPU/RSS. A material failure selects its smallest
  discriminator before runtime edits; no gain tuning or favourable rerun loop.
  Isolated/shared baselines and released-v0.4.8 controls distinguish ordinary
  native limits and new regressions as needed. Existing source/platform and
  practical release gates below remain; fresh matching cells may be reused.
- First closed outcome: current04f healthy DOWN singleton TCP46=179.278Mbps,
  QUIC47=178.857, split=309.377; 80/80 echoes each, worst body gaps .401/.101/
  .274s. Independent checks verify3TCP46/1QUIC47 and both200Mbps cuts used;
  split beats the best singleton72.57%, not a universal aggregation guarantee.
  UP drivers also closed; exact results under review. Two earlier unsuffixed
  QUIC/split DOWN attempts are invalid: root launched a successor before the
  prior driver closed, causing shaping/teardown interference. INVALID.md marks
  both raw directories. Neither is an MPP failure; strict completion guards now
  precede every successor. Their serial replacements are the valid observations.
  Independent agents check topology and prior claim scope;
  root alone runs all builds and traffic. Broad research remains deferred.

### Active severe failure selected after the ten cells

Current04f split TCP47+QUIC46 upload completes but has a5.699s confirmation gap
and five zero confirmation bins20--24 while QUIC46 UP is10Mbps and TCP47 UP
remains200Mbps. Same orientation DOWN delivers about10--22Mbps through the cut
despite a healthy independent TCP cut; max read gap alone(.327s) hides this
sustained collapse. TCP46+QUIC47 DOWN also has a five-second17.608Mbps band.
These exceed the user's practical severity filter; whole means are not a pass.

Next exact question: does the unaffected TCP47 singleton sustain native/user
service under the identical physical schedule, and is split UP held at target
delivery or only returned confirmation? Run current TCP47 singleton UP/DOWN
and publishedv0.4.8 split TCP47+QUIC46 UP/DOWN, unchanged200+200 QoS/outage
profile. These four controls distinguish physical/singleton limitation and
new-versus-existing regression. Inspect existing target/native evidence first;
do not resurrect rejected urgency clocks or prefer a protocol from the average.
Forecast: if TCP47 is healthy, physical shortage on that cut cannot explain
split collapse; if the released composition also stalls, this is not solely a
new04f regression. Neither outcome alone chooses a model fix. Bound any next
diagnostic to the exact missing prefix/feedback owner. No runtime edit yet.

Control outcome,2026-09-11: unaffected current TCP47 sustains179.141Mbps DOWN
and180.160 UP, with80/80 echoes and maximum read/confirmation gaps .391/.455s.
Publishedv0.4.8 split DOWN is127.054Mbps with interactive failures; UP209.835
with3.330s confirmation gap. This is not solely a newly introduced04f failure.
The current UP5.699s confirmation gap includes a return-prefix hold: over a
five-second bracket the client reply frontier stays1119B, server replies grow
1119→1231B and target successful writes advance80,143,392B. TCP47 native ACKs
also advance86,071,574B. Do not call that entire interval a forward-service
freeze or assume a QUIC congestion-controller cause.

Next bounded diagnostic transaction: reuse the existing04f observation-only
clipped-prefix executable, unchanged TCP47+QUIC46 UP QoS/outage profile, one
capture. Enable only response Original/repair, receive-hole/ACK, recovery wake
and existing selected-stream small-frame TCP boundary events; no native trace
or performance instrumentation. Information forecast: join the capture's
actual missing reply frontier with Original/repair admission and arrival to
distinguish unissued recovery from an already-issued but late copy. Existing
events do not guarantee exact QUIC native write/decode attribution, and Original
claim lacks selected path. Preserve those limits rather than infer them.
Falsifier/stop: no reproduced severe hold means this capture cannot attribute
the ordinary hold; do not tune or rerun for a favorable number. If a concrete
blocking owner is identified, inspect its exact model/RFC/history before any
bounded correction. No runtime logic or product parameter change is authorized
by an aggregate-only throughput difference.

Trace outcome: one exact961,347,584B upload completes; maximum confirmation
gap4.145898s. Its missing reply is[1078,1092), committed at1789091895282ms.
QUIC releases it at1899197ms(+3.915s). The first/only overlapping repair is
TCP2 at1899639ms,442ms AFTER that release; queue residence0ms, native write18us,
client authentication30ms later. Thus late TCP repair delivery is falsified for
this critical hold: the delay precedes repair admission. This does not yet
distinguish original-owner maturity from other admission gates.

Next discriminator: one same-policy capture with two observation-only hooks:
exact response Original identity and the active retained-frontier evaluation
outcome (head, frozen owner deadline, queued/pending, capacity-blocked, extent).
Reuse existing events for final repair admission and client ordered release.
Forecast: a future owner deadline identifies an intentionally withheld recovery
clock; an already-due deadline with no queue selects an actual admission gate.
Neither observation is a performance improvement. No timer or congestion gain
change; remove observer source after freezing its explicit diagnostic binary.

Gate capture outcome: exact1,010,368,512B completion, max confirmation1.829351s;
the >3s hold did NOT recur. Its own QUIC heads1121 and1163 wait1.660/2.092s
before receipt; all recorded active-head evaluations stop at a future retained
owner deadline (approximately4.98/6.30s after their Original assignments),
before target/copy gates. This identifies that policy in those shorter episodes
only, and does not justify a timer change. First trace serverF1078 was already
known3.152s before release, so older positive-frontier lag is not its whole cause.
Both diagnostic source hooks are removed; exact diff against04f is empty.
The fresh diagnostic binary is retained-reply-gate-20260911; target/release is
now that observer executable, NOT an ordinary candidate.

Next bounded discriminator remains the separately observed severe DOWN B
collapse (13.381Mbps duringQoS versus TCP47 control183.595). Reuse the same
observer once in DOWN with bulk stream selected, unchanged physical schedule.
Question: is useful TCP service withheld at a missing response prefix, or is
the sender simply not offering work on TCP while the other cut is throttled?
The direction swap is not a favorable repeat of the shorter UP trace. It
addresses the already-selected ten-second service collapse that max-gap<3s
hides. Preserve all bins and actual allocations/receipts; no policy edits.
If it does not reproduce, stop that discriminator without tuning its settings.

DOWN discriminator CLOSED: raw16--24s mean14.248778Mbps; HTTP/80echoes succeed,
max read gap.396887s. In exact seven-second interior, all765 advancing heads
are pre-QoS QUIC Originals. Their12,549,504B of ordered progress equals new
TCP Original assignment; assigned-minus-positive-frontier repeatedly fills
64MiB while received reorder grows25.955→34.238MB. No newQUIC Originals there.
TCP native flight~.21MB versus~7MB windows and physicalcut200 remain underused.
395 acceptedTCP repairs total5,041,220B,98.55% already below an earlier recorded
client release frontier; every repair START is1--31ms late. Exact head579050494
takes5.692s fromQOriginal toQreceipt, its firstTCPcopy follows18ms afterward.
All4,305 retained-helper outcomes are skipped defaults because ACK-gap exposes
one/two frames, not proof that the retained fallback itself was evaluated.
This selects oldQdebt/ordered-credit/recovery service, not newQallocation or a
nativeTCP bandwidth ceiling. Observer numbers do not replace ordinary speed.

Next mechanism transaction: prove/disprove response recovery head-of-line
serialization with one real-producer regression. After a first exactTCPrepair
is accepted, does an unchanged positive/negativeACK frontier prevent serving
a disjoint mature retained successor even though another repair quantum fits?
Current first-gap/positive-F selection checks queued/live-copy overlap only
after constructing that same prefix; ACK-gap frame_count also suppresses the
independent retained helper even when it queued nothing. Terminal/stale-owner
recovery has different uncovered-range machinery and is not this allegation.

Information forecast: a production-path second-admission RED distinguishes
unnecessary receipt-frontier serialization from actual target-service limits.
Correction candidate, only after RED/review: retain owner clocks, all exact
copy/slot/credit/native bounds and one ranked quantum per evaluation; choose
the lowest retained range NOT already covered by queued/current live repair,
rather than require the receiver ACK frontier to advance after every quantum.
Prefer using immutable retained fallback for successors, without broadening
negativeACK authority or changing speculative loss clocks. Do not revive the
rejected request urgency or full-gap repeated enumeration that caused31sCPU.
Tests must constrain actual next disjoint admission, not a particular helper.

Benefit forecast, conditional not promised: the observed33--41MB retained debt
needs roughly1.5--1.9s at180Mbps TCP useful service versus26--33s at10Mbps;
the allowed target window/native service and actual loss determine the result.
The removable issue is waiting for Product feedback between recovery quanta,
not old bytes already inside the QUIC queue. Expect material recovery in the
ten-second cut phase if this dominates; healthy extra copy/CPU/latency can
instead regress. A lack of meaningful ordinary phase benefit rejects the
candidate, not another threshold adjustment. Verify exactcopy/expiry/positive
ACK holes/immature owner/retirement and bounded work first, then unchanged
ordinary affected DOWN/UP plus healthy split/shared controls. No runtime
correction or RFC change is accepted yet.

Pre-implementation review: the old RFC also says retained fallback starts at
positive F. Its useful intention was one exactly ranked quantum per decision,
not an aggregate suffix grant; that wording can impose unnecessary receipt-
serialized decisions. Candidate model is `min(retained cache minus queued/live
copy coverage)`, with unchanged per-assignment maturity and exactly one current
owner/rank evaluation. Covered bytes remain unacknowledged and consume credit;
expiry does not erase their exact publication identity. Do not skip the first
uncovered immature, ambiguous, or no-service owner to find easier later work.
Preserve actor-wide copy-expiry wakes even if no uncovered range exists.
Independent review rejected two coarse gates before implementation: requiring
two payload-eligible outputs excludes a retained ineligible Original plus one
valid alternate; checking only global F's Original misses an uncovered successor
with a different owner. A cheap current-membership/any-eligible-output gate can
avoid singleton full-ledger work without pretending to select the exact range.
The final selected range retains all fresh eligibility checks.

Mechanism RED,2026-09-11 02:36UTC: the actual-producer Q+2TCP test passes all
Original/receiver-ACK/first-copy/native-target controls and fails only at the
second uncovered-prefix queue assertion. Compile-only Debug formatting was
corrected in the test (ReliablePathCommand intentionally has no Debug); that
compiler failure is not the RED. Production behavior was unchanged for RED.
Candidate now implements the coverage frontier and changes both ACK callers
from frame_count==0 to queued==0. It preserves all existing owner clocks,
per-decision sizing, exact-target credit, copy expiry and final writer admission.
RFC15.2 explicitly separates recovery coverage from affirmative receipt. This
is a candidate model correction, not an accepted practical improvement; next
are exact successor final admission and boundary checks, then ordinary DOWN B.

Mechanism controls CLOSED:170 response/tail/expiry tests pass, including real
second TCP writer admission with unchanged F/debt and unchanged first-copy D,
positive-ACK clipping, exact detach/replacement, immutable assignment clocks,
full target reserve and prearmed capacity release. Two old tests explicitly
required stopping after a covered head despite a disjoint mature successor;
they were migrated to successor/no-overlap/all-covered-wake controls, not waived.
The ordinary no-feature candidate is building as response-uncovered-prefix;
no diagnostic overlay or parameter change. Independent final diff review and
the unchanged DOWN B full-series comparison still decide promotion.
Specific competing explanation retained: native TCP flight/window headroom is
not exact Product K. Retained TCP Originals may consume P and leave only the
existing emergency quantum; if so, coverage pipelining alone may not remove
the observed collapse. Do not enlarge reserves to rescue the experiment.

Ordinary outcome CLOSED,2026-09-11: candidate DOWN B improves the selected
QoS15--25s mean13.381→153.633Mbps but fails composed user service. Healthy5--15
falls324.202→193.307Mbps(-40.37%); whole215.335→181.536; worst readgap.327493→
3.300801s with zero bins18,19,21,22,23. HTTP200 and80/80echoes remain, echo
p95 rises326.850→436.064ms (max654.008→541.064ms). Both200Mbps cuts stay busy;
sampled DOWN wire/useful rises1.305826→1.718119. Exact bulk-only management
plateaus match the body's blocking frontier; substantial TCP nativeACK progress
continues. Native service is not ordered user service. No CPU-dominance claim
or exact copy-byte attribution follows from aggregate wire counters.

Disposition: do NOT promote. Retained-cache silence is not receiver omission;
pipeline eligibility can turn many already-mature unknown-receipt suffixes into
copies. The unchanged min-tightened clock observer also runs more/earlier when
the old caller/gate no longer suppresses it. These are known composition risks,
not a Rust safety defect or a reason to increase timeouts/reserves. Exact cause
of each new plateau remains unassigned in the ordinary capture. Independent
source review found no broken copy/credit/wake invariant; source correctness
and170GREEN do not justify shipping the practical regression. Checkpoint this
isolated trial and withdraw it before another model is considered.

The competing Original-debt explanation was narrowed without another lab:
default target P is64MiB, not the smaller native window. Existing D bounds
TCP1 Original debt at24.27--32.24MB and the other TCP outputs<=1.10MB, ignoring
sparse positive releases (conservative overestimates). O alone therefore does
not exhaust P in that old collapse. Exact all-instant K remains unexported.
The next permitted work is a theoretical evidence-type check, not candidate2:
can explicit receiver-omission recovery pipeline while ACK-silence fallback
retains its bounded head probe, without stale-proof amplification or renewing
cause clocks when copy expiry moves the selection cursor backwards? If that
requires a broader timing model, stop implementation and present the concrete
tradeoff rather than stack another plausibly helpful correction.

Withdrawal CLOSED: ec8cd2f retains the isolated adverse trial;843a63f restores
all eight owned runtime/test/RFC files exactly to dbed0c5/current04f. No trial
is left in working source. Explicit ordinary trial binary remains archived in
scratch; target/release still contains that rejected binary and MUST NOT be
used as the current comparator. Telegram adverse-result notification sent
02:53UTC approximately (next nonurgent>=03:53UTC), keys
task=mptunnel-stability-closure/session=mptunnel-v049-20260911.

Independent theoretical challenge rejects a simple omission-cursor rewrite:
the scalar ACK-gap progress resets clocks when first-gap start changes. Copy
expiry can move that selection backwards and renew the unchanged Original's
old deadline; carrying a previous head's readiness can instead prematurely
repair a younger successor. Canonical G/F must not be changed by coverage.

Next diagnostic transaction (no candidate2 implementation): question whether
the adverse trial's material extra recovery is inside explicit receiver G or
only retained-cache silence. Existing ordinary counters cannot answer; existing
stream_ack_received logs expose counts, not exact ranges. Freeze one ec8 trial
diagnostic binary with only an observation field for incoming ACK ranges and
the post-apply canonical G, reuse existing actual-copy/receiver-hole events,
restore current source, and run the same DOWN B schedule once. Forecast is
information only: a large outside-G component supports considering bounded
post-fallback omission service while preserving old silence probes; predominantly
inside-G copying falsifies that easy explanation. No throughput promotion from
the observer. No new timing fields, controller, threshold or reserve. If the
observer does not reproduce relevant copy/service behavior, keep that limit;
do not rerun for a favourable number or stack a hypothetical model.

Post-fallback-only variant review (theory, NOT implementation): preserve the
old early ACK-gap scalar and the raw-F silence probe; any new successor service
would be restricted to canonical G and use the existing per-Original retained
fallback minima, avoiding a new ACK-loss journal or coverage-cursor renewal.
This still cannot assume G reflects current receiver occupancy, and additional
tail-clock observations still touch all Originals. One admitted quantum across
the paths, exact G clipping, no skip of an uncovered blocked owner, unchanged
credit/copy identity and global wakes remain required. The capture must first
justify this narrower hypothesis; source review alone does not recommend it.

Diagnostic build CLOSED1m23s: ec8 runtime plus the single observation-only
ACK field patch, explicit bin/response-uncovered-proof-20260911/mptunnel.
Its exact patch/build log are preserved. The temporary source overlay is removed
and the entire src/RFC/manifests diff against dbed0c5 is empty before traffic.
No ordinary candidate2 exists. target/release now contains this rejected-trial
observer, never the comparator. Ordinary adverse evidence is archived as
RESPONSE_UNCOVERED_PREFIX_ORDINARY_20260911.raw.tar.gz (9files/235057B), with
full timing/cost and copied-feedback attribution limits in the report appendix.

Diagnostic outcome CLOSED: the rejected trial plus exact-G observer delivers
182.224Mbps, healthy5--15=187.601 and QoS15--25=182.704. It reproduces the low
healthy useful service and high wire/useful(1.711), NOT the ordinary3.3008s body
gap (this capture max.9522s). All65 attempted echoes succeed; p95=1381ms and
five consecutive echoes exceed1s. Observer timing does not replace the adverse
ordinary acceptance result.

Independent event replay reconstructs all21,335 bulk canonical-G updates from
positive receipts and explicit scopes exactly. All15,221 bulk repair admissions
classify:567,283,320 accepted payload bytes,485,543,876 inside immediately
preceding G and81,739,444 outside. None overlaps an already-applied positive
receipt. In the conservative healthy interior94.00% of all repair bytes and
93.63% of tail-copy bytes are already inside G. Only3.20% of healthy tail bytes
are provably below an earlier client ordered frontier. Unknown receiver state
must not be called an already-delivered duplicate. G is admission/log-order
evidence, not a reconstruction of the earlier queue-time decision.

Decision: the next proposed G-only restriction lacks its forecast premise and
is NOT implemented. It would not reject most observed healthy copy volume on
the tested membership predicate. The QoS interior also changes recovery cause:
TCP carries stale-owner recovery while tail copies go toQUIC; do not attribute
the entire gain to faster live-tail service alone. No candidate2 exists.

Last bounded read-only question, using current source/history and this SAME
capture: are fallback epochs incorrectly aged in local source staging, or do
copies follow actual receiver omission while their Originals still have normal
native service? Production source inspection places Original sent_at after the
exact writer-ready claim, not before source staging. Claim still precedes actual
native completion; that distinction alone is not a wrong-epoch defect. Inspect
only observable range receipt/repair ordering and record its limits. Forecast:
a concrete wrong timestamp/coverage owner could support a bounded correction;
intended timing plus insufficient causal trace ends this model trial without
another timer, rate, reserve, or protocol-preference change. No new traffic or
observer is planned. Root owns the final disposition after independent review.

Read-only timing challenge CLOSED: production sent_at follows consumption of
the exact writer-ready claim (0449b9f), while physical transport completion is
later. No source-staging epoch defect is present. The full-ledger minimum clock
update is also intentional (953a54f), so extra/earlier calls can alter maturity
without changing a formula; the capture does not isolate that effect. One
healthy8,352B suffix is known already reordered before its ordered release and
is copied again150ms afterward while the sender still reports it missing.
Its earlier copy's immutable suppression period has elapsed. This proves that
stale receipt knowledge can permit an unnecessary copy; it does NOT justify
extending suppression or explain most healthy copying. Other first-arrival
and Original/native timing facts are not in this capture.

Final disposition of this bounded challenge: independent DOWN aggregation is
proved, a blanket shared-bottleneck explanation is falsified, and severe
split-link recovery remains a release blocker. The failed pipeline and the
unsupported G-filter/timestamp remedies are not accepted fixes. Local range,
credit and clock correctness did not establish useful composed service.
No runtime change survives this transaction; no public performance update or
release is permitted from these results. The existing finite-plan stop rule
applies: resolving the remaining recovery tradeoff requires a new, explicitly
scoped timing/service model decision, not stacking another unproved patch.
Preserve this concrete blocker and its exact observations for that decision;
do not silently expand into congestion-control or bottleneck inference work.

## Decision: freeze scope, finish a release

The user's latest instruction stops further broad exploration and prioritizes
the next release. This plan supersedes the former open-ended optimization
sequence, not its evidence or known failures. Follow
[the execution method](PERFORMANCE_METHOD_AND_LESSONS.md), with the release
scope below. No new controller, scheduler, timing-policy, bottleneck-inference,
upstream merge, configuration knob or diagnostic framework enters this batch.

Configured GitHub release lookup confirms v0.4.8 was published on
2026-09-04T22:26:57Z. The next intended version is **v0.4.9**.
Do not republish an existing version or tag before the gates pass.

## Frozen candidate and exclusions

- Candidate runtime is **04f1e56**, also the runtime at evidence checkpoint
  e2375e9 and current reversal checkpoint **0453a40**.
- Retain the existing exact correctness/lifecycle/telemetry corrections and
  finite client ACK/MAX Input correction. The clipped repair-range correction
  preserves an existing exact-owner exclusion contract, with real producer and
  live-prefix evidence. These are candidate contents, not blanket acceptance
  of every performance interaction.
- **8596f30 ordered-credit head urgency is removed** from all three owned
  runtime/test/RFC files. Exact diff against e2375e9 is empty. Its ordinary
  comparison was essentially flat in speed and worse in continuity.
- The proposed follow-up observer was canceled before any source edit, build
  or run. Other previously rejected trials remain removed. Do not restore them.
- Current ordinary candidate executable:
  ./.tmp/reflection/bin/clipped-range-20260911/mptunnel.
  target/release/mptunnel is now the rejected-trial exact-G observer executable,
  frozen at ./.tmp/reflection/bin/response-uncovered-proof-20260911/mptunnel,
  NOT the ordinary candidate. bin/ordered-credit-head-20260911/mptunnel
  remains the REJECTED trial executable. Always use explicit paths.
- Version/package/docs changes may follow the gates. A runtime change requires
  a reproduced release blocker, its exact cause and a bounded correction or
  withdrawal. No speculative cleanup accompanies it.

## Finite gate order

### 1. Source, declared behavior and lifecycle

Run the existing release-quality checks once on the frozen candidate: format,
Clippy with warnings denied, Rust all-feature tests, standalone locked Quinn
tests, existing lab/config/packaging contracts and release-version self-test.
Use the existing restart, exact attachment, ACK/credit, half-close, cancellation,
idle cleanup and ownership/churn checks; reuse already-preserved ordinary
evidence where the relevant source is unchanged. No new stress harness.

Block on failed integrity, deadlock, failure to recover after server restart,
unreclaimed live ownership, broken configured routing/DNS/bypass behavior, or
a real platform/build incompatibility. Fix only the identified release blocker;
do not turn a lint or platform adapter failure into a model redesign.

### 2. Fixed practical regression comparison

Compare the frozen candidate with the **published v0.4.8** control using existing
ordinary binaries/probes and containers. Build a missing control once in
project-local scratch; no environment recreation, host shaping or new harness.

The fixed set is eighteen ordinary cells:

| Profile | Modes/directions | Candidate + released control |
|---|---|---:|
| Healthy single 500 Mbps, asymmetric 70/30 ms | TCP, QUIC, mixed; UP and DOWN | 12 |
| Existing independent 200+200 Mbps profile, temporary 10 Mbps QoS and UDP blackhole | Mixed; UP and DOWN | 4 |
| Existing 20% loss then clear, single 500 Mbps | QUIC DOWN | 2 |

Keep each existing profile and observation/settlement guard unchanged. The
second row uses the declared existing no-random-loss/no-jitter QoS/outage
ablation; the last row covers the currently reported high-loss/recovery issue.
Do not silently replace either with another random profile.

Use complete receiver-confirmed timing series, startup, every write/read/
confirmation gap, settlement, existing loaded-echo latency, and wire/CPU/RSS.
Do not rank local accepted bytes or cached capacity as delivered throughput.
An initial burst followed by persistent collapse is a failure. No claim that
a short confirmation catch-up bin exceeds physical link capacity.

Also perform one existing actual-browser Cloudflare smoke on default mixed
mode for browsing/concurrent behavior. This is not a new baseline matrix or
a claim that all Cloudflare modes are optimized.

Acceptance is usable, materially nonregressing service and recovery against
the released control, not compilation alone or universally highest Mbps.
Retain known latency/efficiency limitations. Small noisy changes do not open
new optimization tasks. If a material result is genuinely ambiguous, allow one
predeclared order-reversed comparison of that affected cell only; preserve
both outcomes. No favorable rerun loop or parameter adjustment to pass.

### 3. Version, docs and platform CI

Once the source and practical gates pass, bump to v0.4.9 and update release notes,
README/PERFORMANCE with the accepted evidence and existing time-series/latency
plots. Public docs contain supported results and limitations, not rejected
experiments, internal source paths or claims of universal optimality.

Run the existing linked/package checks for Linux, Windows, macOS and Android
targets. Push the fixed candidate and monitor CI at five-minute intervals.
A source-affecting correction reruns its affected tests/cell; packaging-only
changes rerun packaging, not the entire performance matrix. Do not merge new
upstreams while waiting.

### 4. Publish and stop

Publish only after the required quality, regression and platform gates pass.
Verify release artifacts/version and summarize the actual improvements and
remaining limits. Preserve useful evidence in docs-dev before scoped scratch/
build cleanup, with explicit preflight and absolute deletion paths. No broad
deletion while evidence or an active build still needs it.

Stop after the release handoff. Further performance research is a separate
batch requiring user direction.

## Stop conditions and deferred work

A failed gate does not authorize another open-ended optimization campaign.
Identify the exact blocking regression and remove the attributable unaccepted
change, or make its smallest proven correction. If attribution needs broad
research, report that concrete release blocker and stop rather than silently
restarting exploration or claiming the release is ready.

Deferred, with no resolution claim:

- Shared mixed-mode loaded-latency/efficiency superiority over every baseline.
- Broad random/asymmetric multi-link ablations, all-mode browser comparisons,
  full MPTCP comparison and universal optimality.
- Further BBR/Brutal/startup/QoS-recovery tuning or alternate repair policies.
- Unattributed deployed RAM/CPU incident and the exact high-loss burst owner.
  Local startup one-core work is reproduced; sustained low-speed collapse was
  not CPU-saturated, and snapshot-dominant work was falsified. Profiling needs
  the already-requested explicit sudo consent. No permission workaround.
  A reproduced leak, runaway CPU or stalled recovery in the fixed gate is
  still a release blocker; the unresolved report is not declared harmless.

## Evidence and continuity

Full prior plan and decisions are preserved in:
`git show 0453a40:docs-dev/CURRENT_CLOSURE_PLAN.md`.
[Complete comparison record](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md)
retains all earlier outcomes; it is not a list of mandatory new release tasks.

Latest rejected trial: 988413952 bytes/44.573108s = 177.401 Mbps versus
176.241; worst write 1.865375 -> 2.495271s and confirmation 1.717023 ->
4.357523s. Target delivery advances only29200B over a two-second cut interval.
Sampled UP bytes/useful improves1.876008 -> 1.853830, but peak UP backlog
38.663 -> 75.378 MB and native flight61.235 -> 95.685 MB. Do not mislabel every
cost worse or attribute the entire gap to the trial. It failed useful
composition despite a real producer RED,153 focused checks and independent
review, and is not part of the candidate.

[Closed trial archive](ORDERED_CREDIT_HEAD_ORDINARY_20260911.raw.tar.gz):
21 regular files; independent input-byte verification, root complete171-line
appendix review and gzip/manifest checks done. No observer follow-up exists.

No build or laboratory run is active. Independent capture/timing reviews are
closed; the final eight-file diagnostic archive is51,090,323B raw/2,786,019B
compressed, with full replay and receipt-timing limits in the evidence appendix.
Runtime/RFC/manifests are restored exactly to dbed0c5/current04f.
Root alone runs builds/labs, without overlap. Existing project Docker only;
no sudo, outside-root work or /mnt/storage use. Preserve the unrelated user
seven-line edit in LIVE_OWNER_FRONTIER_WORK_BOUND.md; AGENTS.md is immutable.
Next nonurgent Telegram notification no earlier than03:53 UTC.
Commentary stays timely; no release notification
before an actual milestone.
