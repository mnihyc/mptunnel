# Current deterministic closure plan

Updated: 2026-09-08 06:27 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each
transaction and after compaction. This is the active scope and next-action
ledger, not a new issue inventory. Completed transaction detail is preserved
in the named evidence below and in git checkpoint `93370dc`:
`git show 93370dc:docs-dev/CURRENT_CLOSURE_PLAN.md`.
Older history is CLOSURE_PLAN_HISTORY_THROUGH_20260907. Shortening this ledger
does not discard failed experiments, authorizations, or global gates.

## Current runtime and ordinary results

Current runtime checkpoint: `953a54f`, response retained assignment recovery,
with333 affected component checks and mixed/adverse ordinary timing. Its fixed
comparator is `765683b`; ordinary executable
`./.tmp/reflection/bin/ack-atoms-20260908/mptunnel`.
No diagnostic or request-sampler overlay is active. The user's seven-line
LIVE_OWNER_FRONTIER_WORK_BOUND.md addition remains untouched.

| Checkpoint / scope | Proven | Ordinary disposition |
| --- | --- | --- |
| `11d6f3a`, late STARTUP after FINAL | 34 focused checks; refusal belongs to attachment, not shared carrier | Retained comparator. Mixed combined down85.556Mbps /4.423s gap; up64.602Mbps /5.054s gap and19.293s drain. Not fluent acceptance. |
| `011aee9`, retained repair beyond complete-ACK horizon | 21 checks; actual positive F and retained ownership remain usable when negative H<F; same-range accepted-copy deadline preserved | TCP pair incomplete/adverse. Mixed control70.598 vs candidate49.967Mbps; no practical promotion. |
| `765683b`, unique ACK atoms beside partial copies | 66 checks; original65536/copy14600 yields50936 unique proving bytes in both directions, without changing settlement/epoch fences | Mixed control63.923 vs candidate26.050Mbps; maxconfirmationgap6.000557 vs6.035895s. Exact completion, less work in more time. No practical promotion. |
| Shelved request cohort sampler | Actual pipelined samples[2,2,2] vs staged[2,3,4]; candidate23GREEN | TCP pair incomplete, confirmation gap worsened. Frozen candidate/patch retained; NOT implicitly stacked into current runtime. |

Unseeded pairs do not alone prove a causal regression, but adverse or incomplete
results stop promotion. No third favourable trial replaces them.
REQUEST_COHORT_ORDINARY_20260907 contains full raw timing series, identities,
cost and attribution limits; its linked raw archives preserve observations.
Late-startup ordinary detail is LATE_STARTUP_SCOPE_ORDINARY_20260907.

## Just closed: what causes the slow prefix

1. **False QUIC silence was a real byte-attribution defect.** In the retained
   requalification trace, a full65536B original ACK with only14600B copied was
   classified wholly ambiguous. Unique progress at3620ms was discarded and
   QUIC withdrawn3624ms. `765683b` corrects exact set subtraction; it does not
   claim to fix every subsequent ordered-service gap.
2. **Actual live-repair feedback serialization is proven.** Two adjacent pairs
   of14600B QUIC copies beat every intersecting TCP original/copy at receiver F.
   Native writer acceptance0--1ms; peer advance64--98ms; next disjoint copy
   waits another127--128ms for client ACK application despite L1.8--2.0MB.
   Actual cause is persistent ACK-gap recovery, not retained fallback alone.
   These pairs occur inside10Mbps QoS, not500Mbps spare-wire evidence.
3. **Initial ready-path selection false positive is closed.** One membership
   capture publishes187 contiguous TCP originals totaling12,189,643B in25ms,
   every event with exactly one attachment. QUIC was already opening, attaches
   at91ms and receives data93ms. It was not ignored while ready.
4. **Other causes remain distinguished.** Early QUIC native work is substantial
   before13s; some copies lose to TCP. At13--20s, source and ordered target
   advance3,276,800B while the leading TCP debt falls by exactly that amount,
   retaining64MiB separation. Late50--55s all222,429,184B are already written to
   the target while client Product debt remains43MB: debt is not undelivered
   payload. During UDP outage, native silence is a different stage again.

Evidence: ACK_ATOMS_SERVICE_TRACE_20260908.patch and
ACK_ATOMS_SERVICE_DIAGNOSTIC_20260908.raw.tar.gz; initial membership observer
INITIAL_PLACEMENT_TRACE_20260908.patch and
INITIAL_PLACEMENT_DIAGNOSTIC_20260908.raw.tar.gz.
All overlays were frozen and reversed before their captures. Diagnostic rates
do not pass the ordinary gate; latest membership capture has a13.500752s gap.

## Completed model decision: do not implement naive copy-ahead

Three independent read-only reviews agree. A repair-placement cursor can be
distinct from actual received F, but that distinction alone does not make
ahead copies useful or control their aggregate service cost.

- T06/`bfac5b8` deliberately preserves one ranked lowest-frontier hedge.
  It removed a demonstrated112.6-times score/Apply suffix expansion. A test
  demanding another adjacent copy before DataACK would assert a new policy,
  not a RED against that existing contract.
- Keep exact original ownership, H/F, qualified epochs, stable copy slots,
  same-range suppression and native authority. Every proposed next range would
  need its own age/rank/Apply proof; provisional or accepted copies are not
  received bytes. Cancellation/detach must expose an earlier hole again.
- Existing service admission subtracts queued+accepted copy debt. Ranking
  cannot erase that debt, nor simply add it atop native queue/flight where
  overlap exists. Already delivered but un-DataACKed copies still own Product
  resources without necessarily needing more wire service.
- An ETA guard is not an exact-prefix remedy: request/multipath.rs computes
  owner completion from ALL OriginalData debt, not native work ahead of the
  queried range. Increasing a later suffix can change an earlier-prefix
  decision. Freshness and jitter margins do not fix that scope mismatch.
- The existing fallback epoch holds one frontier. Moving it through successive
  speculative positions can overwrite an earlier immutable deadline; returning
  after cancellation must not renew that mature cause.
- At10Mbps, observed L1,811,008--1,966,080B costs at least1.449--1.573s of
  shared serialization before framing/retransmissions. Independently ranking
  repeated Q on the sole alternate can still fill L. Some old TCP prefixes are
  already received awaiting feedback, so such work can be entirely redundant.

**Disposition:** real conditional service limitation; naive cursor and
aggregate-ETA-guard forks rejected BEFORE implementation. Existing producers
support ownership/accounting, not the promised exact-range service forecast.
No quantum, timer, traffic percentage, controller, P/E or native-queue change.
No extra trace is needed to reach this decision. The existing due-F fallback
remains. A future useful policy may be a clearly declared empirical heuristic;
it must not be presented as a guarantee derived from unavailable evidence.

## Completed transaction: initial-owner causal ablation

- **Issue:** mixed upload has long ordered stalls after committing a large
  prefix to its sole initial TCP attachment. The exact trace establishes
  ownership and timing, but not how much whole-profile delay would disappear
  with another initial owner. Do not infer dominance from correlation alone.
- **Question / competing causes:** does initial irreversible TCP placement
  dominate the early slow period, or do subsequent allocation, recovery, native
  service and feedback produce similarly poor timing regardless of first owner?
- **Smallest preflight:** inspect supported configuration and the existing
  initial-open selection seam. Determine whether initial QUIC selection can
  be changed without changing later mixed membership, rate hints, qualification,
  budgets, native controllers or the network. No permanent protocol preference.
  If configuration cannot isolate it, declare the exact temporary lab-only
  intervention and its confounders before any build/run; no implicit backup
  policy, reorder of all later decisions or new harness.
- **Planned causal comparison:** one declared ordinary control/intervention
  mixed combined upload pair on current runtime, diagnostics off, initial
  owner the sole intended difference. Both retain three TCP plus one QUIC.
  Same500Mbps routed/mirrored asymmetric profile and existing receiver probe.
  Distinct frozen identities/configuration; no build/load overlap.
- **Preflight resolved, 04:03:** no config changes only initial choice. Array
  order also changes later ties; rate/backup changes persist. Use a throwaway
  one-file intervention in open_remote_stream_active, immediately after the
  existing local opening_candidates sort: move its first UDP candidate to the
  front without changing its ordinal, frozen return plan, other candidates or
  later scheduler. Guard by the existing lab-diagnostics compile feature;
  archive and reverse after freezing the executable. No product option or
  persistent runtime change. Keep server on the ordinary current executable.
  Both cells enable ONLY existing initial/open and additional-attachment events
  to verify actual first owner and fallback; no bulk or repair tracing. This
  narrowly supersedes diagnostics-off for setup identity only, and still cannot
  supply ordinary acceptance. Logs may contain later replacement setup too.
  Control that already starts on QUIC, failed intended QUIC open, missing
  required physical readiness, or different final membership limits the
  ablation; do not repeat to manufacture the desired contrast.
  The prior capture has all3TCP and physicalQUIC active143ms before its first
  OriginalData, so the91ms delay there was logical attachment, not cold native
  establishment. The fixed1s runner wait is not a readiness barrier; check
  both new cells' initial snapshots and retain their handshake costs.
- **Frozen, 04:07:** build2m03s; experimental executable
  `./.tmp/reflection/bin/initial-quic-owner-ablation-20260908/mptunnel`.
  Ten-line one-file overlay independently reviewed, archived as
  INITIAL_OWNER_ABLATION_20260908.patch and fully reversed before execution.
  Server stays on ordinary ack-atoms-20260908 in both cells. Labels
  `mixed-combined-up-initial-owner-{control,quic}-0908`; setup event filter
  `reliable_stream_open_attempt,reliable_stream_open_success,reliable_stream_open_timeout,relay_additional_path_open_spawned,relay_additional_path_open_attached,relay_additional_path_open_failed`.
  Independent review notes the existing last-alternative timeout follows
  attempt order: a failed QUIC then successful TCP is not the intended contrast.
- **Environment-invalid cell, 04:08:** control12182 hit its existing guard;
  client.log13 identifies the actual problem: the owned pinned test certificate
  expired at2026-09-07 19:49:30UTC, before this run. QUIC could not attach, so
  this is NOT a mixed comparison or an MPP throughput regression. Products and
  probe are stopped; retain raw invalidcell instead of deleting or promoting it.
  Renew only the owned certificate with the same key, CN/SAN localhost and
  critical CA:FALSE, valid30days to cover this continuing lab task. Both next
  cells use the same renewed certificate; verification remains enabled.
  Check validity and actual native/setup readiness. The previous membership
  capture at19:30UTC preceded expiry and remains valid. New pair labels
  `mixed-combined-up-initial-owner-valid-{control,quic}-0908`; this repeats an
  invalid environment attempt, not an adverse valid result to seek a pass.
- **Measurement:** complete confirmed bytes, first/max gaps and full raw bins,
  startup/attachment and native service history, drain, directional wire and
  RSS/CPU. Initial handshake time must remain visible. A different first owner
  does not by itself prove an equivalent warm native state or selection bug.
- **Falsifier / stop:** similarly long early owner-blocked timing argues against
  initial TCP commitment as the sufficient dominant cause. Improvement confined
  to the early interval supports that mechanism only, not a QUIC-first default.
  Different random realizations limit causal strength. No third run-to-pass,
  automatic controller change, copying larger suffix or matrix expansion.
- **Acceptance:** this is a diagnosis ablation, never release or policy
  acceptance. A proposed placement revision must then state its cold/unknown,
  sole-path/high-BDP, UDP-QoS, discovery and shared-cut countercases before RED
  and code. Preserve existing T03/T04b constraints unless explicitly revised
  with replacement guarantees; do not silently restore a rejected shrink.

**Pair complete, 04:13:** valid control235,470,848B/53.153263s/35.440Mbps;
QUIC-first374,734,848B/49.660557s/60.367Mbps, both exact complete. First
confirmation worsens.417902->1.551972s; maxconfirmationgap5.697254->5.256939s;
maxwritegap9.670569->4.932933s. At4s ordered target7,667,553B versus81,312,587B.
This supports a material early initial-owner effect, not a sufficient fix or
QUIC preference. Both physical sets were warm and all four logical attachments
established. Intervention later records a TCP session-closed warning, without
an observed physical-identity replacement; retain that distinction.
Full series and cost: INITIAL_OWNER_ABLATION_20260908.md and its raw archive,
which also preserves the explicitly excluded expired-certificate cell.

**Separate return stage proven:** intervention4--6s target advances7,041,888B,
server reads sink replies171->262B, but client delivers no reply beyond67B.
At43--47s target advances9,473,237B to the complete total; replies1166->1347B
are read by server while client remains942B. Thus confirmation zeros are not
all forward-upload silence. At22--23s a different actual forward plateau has
64MiB source/target separation despite1,656,000B QUIC native ACK progress.
Existing snapshots do not identify exact holding ranges or native winners.

## Active transaction: response retained-frontier symmetry correction

- **Issue:** the same mixed request has long return delivery stalls after the
  server has read small acknowledgement bodies, including outside deliberate
  QoS. This affects confirmation/interactive service, not only bulk estimates.
- **Question / competing causes:** response queue/native/client ordering may
  hold an already-published prefix; or response retained recovery still rejects
  current positive F when its historical complete-ACK horizon is behind.
  Source inspection finds the generic active tail still requires historical
  completeness/prefix equality, while its exact-owner helper is final-only.
  The request correction011aee9 changed this family; RFC15.2 now explicitly
  permits active retained-owner recovery independent of EOF or old completeness.
- **Smallest action:** independent source/history/producer review to determine
  whether a legal response ACK sequence can actually yield H<F at those gates.
  Check actual positive frontier, stored snapshot and callers; no full trace,
  runtime correction or claim that the captured67-byte stall has this cause.
  If reachable, construct one real response admission/ACK-ledger RED plus an
  aligned-H control before changing production. Existing request tests are not
  proof of symmetric response behavior.
- **Falsifier:** if response producer/state invariants require H>=F there, or
  another active retained fallback already reaches the same exact prefix and
  target, this proposed rejection is not a defect. Do not patch an unreachable
  branch or change freshness/quantum to manufacture eligibility.
- **Preservation / gate:** actualF and negativeH remain distinct; exact original
  age, copy/deadline/target identity, ranked range, qualification and Native
  admission stay. Component RED would prove a contract mismatch only, not
  attribute all return stalls. Symmetric mechanism controls and the affected
  ordinary timing/cost comparison still precede practical promotion.

**Producer review / RED authorized, 04:35:** response publication uses cumulative
`ack_frames()`, not the request-side sparse delta producer. Supported
`max_ack_ranges=1` realizes two incomplete chunks after receiving originals
0,1,3 of four64B assignments. Both cells retain exactly[128,192), actualF128;
an earlier complete snapshot gives H64 versus aligned controlH128. The test
uses real send admission, receiver ACK production and cache/binding release,
then actual mature ownership and alternate admission. It isolates the active
queue predicate, not actor wake scheduling. With default256 ranges this H/F
geometry needs extensive fragmentation: do NOT attribute the small67B return
plateau to it. Separately review the same retained obligation before any ACK;
that is its initial-state boundary, not a new recovery policy. Production is
unchanged while the RED/control compiles.

**RED/control complete, 04:37:** release focused build1m10s. Aligned-H control
passes actual alternate dispatch; fragmented-H case passes all reachability,
cache/owner/maturity/admission assertions then fails intended queue count0vs1.
Command: `cargo test --release --locked -j 1 --config
'profile.release.package.mptunnel.opt-level=0' --lib
active_response_retained_frontier_ -- --nocapture` (1PASS/1RED).

**Bounded correction authorized:** migrate response active/final to the existing
exact retained-owner helper using actualF. Remove the duplicate generic live
contiguous-tail branch; retain authoritative-gap and failed/unknown-owner paths.
Preserve ACK/output enqueue-before-drain order, pre-armed native capacity,
accepted-copy wakes and exact ranked/Apply authority. Return the currently
validated owner deadline for wake ownership instead of blindly reusing an old
epoch when no retained obligation exists. Never reset a mature immutable epoch
because an alternate temporarily disappears. No new knobs, quantum, controller,
P/E limits or timer adjustment; RFC15.2 already specifies this shared contract.
Focused boundary controls and independent source review precede ordinary pair.

**Candidate verification / integration HOLD:** first unified candidate passes
82 server-component checks in0.11s after1m12s compile, including actual no-ACK,
fragmented-H, aligned, final and resolved-wake fixtures. This is NOT promotion.
Root source/formula review found the generic active branch legitimately ranked
and fully admitted up to65536B; the reused final helper silently clipped it to
14600B. T06 permits the former. Reviewers retracted their too-broad quantity
neutrality claim. A private Active/FinalDrain operation phase now preserves
inherited sizing (EOF selects quantity, not authority); real65536B active versus
14600B final dispatch control is added but NOT yet run.

Two migration countercases still block ordinary execution: (1) the final helper
uses positive-service target selection then clips to K, while old active
preflight requires its whole frame; preserving the cap alone does not preserve
target eligibility, (2) adjacent same-owner sparse appends grow the ranked
extent and its latest assignment time, which can renew the fallback epoch for
unchangedF. Example64B@t0, next64B@t0+R/2 moves D fromt0+R tot0+1.5R.
This is not an accepted new policy or a reason to tune R/Q. Complete the bounded
active-retained model (exact mature prefix, immutable oldest obligation and
existing full-frame admission), then prove these integration controls before
ordinary comparison. No shared request/helper rewrite is implicitly authorized.
The need for these checks is a methodology lesson: a proven final-only helper
is not behaviorally neutral in an active caller. Compare actual quantity,
selection, assignment lifetime and wake formulas before declaring reuse safe.

**Model decision before further implementation, 05:07:** the clock belongs to
each exact OriginalData assignment, not the maximum assignment time in an
appendable candidate. Retain `D_j = a_j + R_j` on the existing response flight;
on first retained-owner observation initialize it, subsequently only tighten.
Observe existing fragments coherently under the existing output/flight lock
order, so fragments created before first observation share their assignment's
first interval. This explicitly observes current Original flights together,
not only a selected cache fragment. Later ACK splitting inherits D; evidence
invalidation cannot erase it; removal of flight ownership removes it. Do not
freeze a guessed interval at Original commit or reuse an old head's shorter R
for a newly assigned suffix after native timing has changed.

Keep the complete current owner/copy-avoid/cache-contiguous prefix, limited by
the inherited operation quantum. At time t, cut it at the first Original span
with `D_j > t`. The assignment(s) covering actual F supply the head wake;
fresh later spans cannot alter that wake. Once all adjacent assignments mature,
they can aggregate to Q: do not freeze a tiny first-observation extent or impose
cache-chunk boundaries. A due head still has to pass the unchanged distinct
target, copy/slot, Product service and native Apply gates. This neither infers
negative ACK loss nor installs a new timer, pacer, copy-ahead cursor or budget.

Response-local scope: add one fixed optional deadline to existing flight
metadata, use its coherent mature-prefix observation, and remove the response
aggregate fallback epoch once redundant. Keep shared/request models unchanged
in this transaction; record their analogous append question separately rather
than claiming global closure. Cost is bounded by retained entries plus their
observation scan; ordinary CPU/RSS and timing must verify it, not be assumed.

**Admission preservation decision:** one shared retained obligation may keep
the pre-existing operation-specific publication forms. Active must reuse its
per-cache-frame full-credit preflight and unbound TailReinjection dispatch;
FinalDrain keeps exact bound-target/positive-credit shrinking. A boolean full-
frame check alone does not preserve Active's multi-frame behavior: two32KiB
frames versus one64KiB common prefix can select/queue differently. Do not force
synthetic concatenation or claim these plans are equivalent. There is one
ownership/maturity cause, not two independent recovery controllers.

**Fresh-append RED confirmed, 05:12:** the built focused suite reports6PASS and
one intendedRED. The late-alternate case proves head64B mature, suffix64B fresh
and target credit128B before the queue0vs1 assertion fails. The mature-adjacent
control dispatches128B; real active65536B/final14600B quantity controls pass.
Command: `target/release/deps/mptunnel-29ba8ebb1d3edd33
response_retained_frontier --nocapture` (no rebuild or ordinary run).
The per-assignment correction above is now authorized, with Active's inherited
unbound publication form explicitly preserved. Then verify increasing owner R,
ACK splitting, exact owner identity, no wake after release, copy/capacity
controls and both inherited publication forms before the ordinary gate.

**Implementation and independent source review, 05:22:** response-local D
metadata and coherent observation are implemented. The mature-prefix query is
capped at inheritedQ; clocks are observed over retained entries, but the pure
ownership sweep runs after releasing locks and only over overlapping spans.
A fully mature result reuses that sweep. Active preserves unbound per-frame
full-credit dispatch and the owner-based successor observation; FinalDrain
preserves bound-target shrinking and its target-based successor observation.
The response aggregate epoch is removed. Independent review finds no blocking
model discrepancy, explicitly withholding runtime/cost acceptance.
The shared actual-dispatch fixture checks bound_reinjection_deadline presence
only for FinalDrain; two32KiB Active frames thus distinguish the publication
forms without a new production accessor or synthetic native credit.

**Focused GREEN, 05:26:**88 server tests pass after1m12s build, including both
original REDs and clock/quantity/dispatch controls. The same executable passes
155 response stream and90 response sender checks in0.03/0.02s, including native
Apply, ACK fragments, qualification, copy clocks and terminal boundaries.
One fixture constructor signature was corrected before this run; no Product
change was made to satisfy a setup error. Independent source review remains
clear. Freeze an optimized diagnostics-capable ordinary candidate (all tracing
disabled during comparison), then run the declared pair. Labels are
`mixed-combined-up-response-retained-{control,candidate}-0908`; candidate path
`./.tmp/reflection/bin/response-retained-20260908/mptunnel`. No ordinary result
or practical promotion is claimed by this GREEN.

**Freeze/checkpoint, 05:29:**953a54f records the independently reviewed mechanism
and333 checks, explicitly not release acceptance. Optimized ordinary build
completed2m03s; candidate is frozen at the declared path. Control run is started
with both endpoints on765683b, exact existing profile and runtime tracing off.
Owned certificate remains valid through2026-10-07; no product/probe was running
before launch. Host load1.69 on the available host did not justify waiting.
One compiler warning exposed the now-unused ACK-horizon extent helper; its
only remaining consumer was its own unit test. Remove those orphaned pieces
separately after binary freeze, without changing the pair executables or
calling that cleanup a performance change. No compile overlaps the ordinary
pair; the cleanup's focused compile follows it.

**Ordinary pair complete; promotion stopped, 05:32:** both transfers complete
exactly. Control234,815,488B/52.755080s/35.608Mbps; candidate259,457,024B/
47.594305s/43.611Mbps. First confirmation0.463283->0.406435s and maximum local
write gap4.655458->3.440146s improve, but maximum confirmation gap worsens
4.799931->6.080193s. Higher bulk average does not pass the timing gate. This is
an adverse aspect of an unseeded pair, not conclusive attribution to the change.
No third acceptance run or download/full-matrix promotion follows it.
Full raw bins, stages and costs are being preserved in
RESPONSE_RETAINED_ORDINARY_20260908.md with its raw archive.

**Remaining stage narrowed:** candidate40.101511--46.102124s has client reply
delivery frozen at840B while server reply reads1218->1526B and ordered target
writes240,683,927->254,488,023B. Thus the long confirmation gap contains actual
held return work, not just forward upload silence. At47.102227s every payload
byte is target-written while only924 of1553 response bytes are locally written.
Snapshot timing cannot identify the missing response range, ownership or copy
service. The model correction remains component-proven, not a demonstrated
closure of this return-stage stall. The orphan-helper cleanup independently
passes48 I/O component checks after1m06s build, no new warnings.

## Completed discriminator: exact response frontier service capture

- **Question:** during a repeated return-stage stall, which exact lowest
  response byte is held between server source read and client local write?
  Does retained repair fail to publish, publish but wait for native service,
  or arrive ahead of a different missing prefix/behind local delivery?
- **Existing evidence:** the ordinary40--46s boundary above, the real H/F and
  fresh-append controls, and333 affected component checks. These do not identify
  a native cause or prove every delayed response has the repaired geometry.
- **Smallest action:** one diagnostic capture on frozen953a54f using existing
  events only: server_sender_dispatch, server_repair_carrier_accept,
  server_data_ack_recovery, server_response_recovery_wake, server_output_update,
  stream_ack_received, receive_hole, receive_hole_release and
  tail_stall_reinjection_blocked_frontier. No new code/build, sampler,
  congestion/quantum setting or network change. Label
  `mixed-combined-up-response-retained-service-diag-0908`.
- **Discriminator:** join exact response offsets, original/copy target,
  admission timestamp, receiver frontier release and positive ACK application.
  A timely admitted copy rules out the old eligibility gate for that range;
  a received frontier ahead of local writes moves the question downstream.
  Missing events are not proof of native loss or suppression. Preserve current
  native/management epochs and the profile's actual phase times.
- **Stop:** one capture, then source/history/model review of the identified
  stage. No repetition seeking a favourable rate, no bulk throughput promotion
  from an instrumented executable, no new global controller hypothesis. If the
  stall does not recur or events are insufficient, record that limit before
  deciding the next smallest action. Products are stopped before any build.

**Capture complete, 05:43:**314,376,192B exact completion in56.086584s,
44.842Mbps, first confirmation.518576s and maxgap6.315621s. Instrumented rate
is not ordinary acceptance. RESPONSE_RETAINED_SERVICE_20260908.raw.tar.gz
preserves this one capture (4537 client/7505 server log lines).

- Stream0/session15425282828787695206: response[870,884) original UDP0 at
  Unix1788816968546ms (server.log4147), TCP1 copy170ms later (4230), TCP0
  copy at6968918 (4249). That second copy's frozen deadline is5,482,425us;
  third TCP2 copy at6974401 (4546), ACK F884 at6974919 (4551). Early copies
  already publish: this is not the former H/F/no-ACK eligibility rejection.
- Exact[912,926) TCP0 copy commits at1788816974919 (server4556), appears in
  client mux at6977828 (client3003):2.909s later. Command commit does NOT mean
  native socket/wire acceptance. This witness establishes the remaining stage.
- Outside outage,[1262,1276) original UDP0 at1788816984760 (server5794);
  TCP2/0/1 copies at6985031/5244/5445 (5991/6066/6122), about200ms copy
  deadlines. First ACK F1276 only at6987421 (6282). All three admitted copies
  still precede the hold: the earlier5.48s deadline is not a sufficient cause
  and does not justify shortening it.
- Client records only four hole/release events at6977826--7829, F912->954
  with at most14B reordering. No observed sustained post-mux hole explains the
  F870 or F1262 plateaus. During the former's6s client-Rc plateau, this same
  relay applies456 request ACKs with max57ms adjacent gap; the latter's2s
  interval applies138 with max47ms gap. Its pending-local-write branch does
  not service incoming ACKs. A seconds-long already-applied response stuck
  in that local write/flush therefore does not explain these intervals.
- Native client TCP Recv-Q has57,005/158,084B at row31 and drains slowly,
  but these socket bytes are not identified response frames. The cause is
  not yet attributable to wire delay, writer/read service, mailbox backpressure
  or relay input arbitration. FIFO or a biased select alone proves no defect.

## Current next action: locate post-command/pre-mux service delay

- **Question:** where does an exact dispatched response range spend the
  observed seconds before client mux application: native writer/transport/
  decode, carrier actor, attachment mailbox, or relay fan-in/service?
- **Smallest action:** independent read-only review of those exact source and
  history seams, then determine whether existing events can distinguish them.
  No runtime fix, timer change or new broad audit. If exact handoffs lack
  observations, declare a temporary offset/identity-keyed lab-only observer
  and one diagnostic run before building; freeze and reverse the overlay.
- **Falsifier / stop:** timely decoded and queued bytes localize later delay;
  timely mux application rules out input service; late native completion
  rules out a not-yet-reached downstream queue. Missing records remain unknown.
  Do not revive rejected ready-feedback batching or tune deadlines merely
  because control volume or a queue is large. Preserve ordinary comparators.

**Observer preflight, 05:51:** existing TCP reader/route/wait counters aggregate
duration without exact frame identity. Client attachment forwarding bypasses
ReliablePathStream::recv_frame; instrumenting that handle would miss this path.
Independent source/history review finds class rotation intentional (59fbd22),
not proof of wall-clock service; the shared FIFO predates this batch (da63e85).

Authorize one temporary lab-diagnostics-only, StreamData-only observer overlay:
server TCP command writer stage/start/flush completion; client authenticated
TCP decode and stream routing; attachment forwarder shared-send before/after;
shared fan-in dequeue (both async/ready-only); every client mux application,
including in-order input. Record exact path/instance/attachment where owned,
stream/offset/end and queue snapshots. Do not mutate protocol, queue/admission,
timing, priority or selection to observe them. Send-completion logs bound
admission but are not its linearization instant; cross-worker dequeue may log
first. Queue length is not a guaranteed frame position.

Freeze the overlay executable, archive/reverse it before one same-profile
`mixed-combined-up-response-handoff-diag-0908` capture. Ordinary binaries remain
unchanged. Only these response handoffs plus existing exact response dispatch,
repair, hole and ACK events are enabled; no upload-payload log flood, new test
harness or ordinary rerun. If native decode is late and downstream service is
prompt, mailbox/actor hypothesis is falsified for that range. If input is
decoded early, locate the exact subsequent handoff before proposing a fix.

**Frozen/reversed, 05:56:** optimized build2m04s, no warnings. Diagnostic
binary `./.tmp/reflection/bin/response-handoff-diag-20260908/mptunnel` includes
only the archived7-file RESPONSE_HANDOFF_TRACE_20260908.patch atop95a5292.
Overlay is fully reversed before execution; runtime worktree matches HEAD.
Events response_tcp_handoff/response_handoff/response_handoff_mux_apply join
existing server_sender_dispatch/server_repair_carrier_accept/
server_data_ack_recovery/stream_ack_received/receive_hole/receive_hole_release.
Server local write/flush completion is not wire receipt; normal and
interlocked client routing both observed. No sampler/native trace/build overlap.

**Capture closed, 06:06:**259,325,952B exact complete/49.986587s,
41.503Mbps, firstconfirmation.448020s/maxgap3.215542s. Not an acceptance
comparison. RESPONSE_HANDOFF_20260908.md/raw archive and observer patch record
the chain. All149 TCP response transactions dispatch/stage/write/localflush
within1ms perstage.218 complete attachment->mux tuples: sharedadmission<=9ms,
sharedresidence<=170ms, dequeue->mux<=1ms. Do NOT replace these stages or tune
their capacities to fix a seconds-long gap they did not produce.

Some TCP copies spend up to10.163s before authenticated decode and2.415s
after decode; these are losing duplicates. All7 TCP frontier-winning frames
are OriginalData, not repairs, and have decode->mux<=12ms. Actual largestF gap
1147->1161 is3.216s, won by QUIC at1788818247409; its attachment sharedsend
begins only89ms earlier.156 requestACK effects continue during that gap,
maxadjacent32ms. Likewise F1203's2.767s gap spends only106ms after attachment
sendbegin. The dominant winning-frame delay remains BEFORE that handoff.
This closes a false inference from losing-copy or maximum queue delay to user
stall. The observer's decode/server path_index field means wirePathId, while
route/attachment path_index is runtimeindex; map0->0/1->2/2->1 explicitly.

**Next bounded discriminator authorized:** complete the missing QUIC winning-
frame ingress chain, not another controller/batching experiment. One temporary
StreamData-only observer at actual server QUIC write begin/completion, client
ordinary frame decode, normal/interlocked carrier forwarding, and the existing
attachment/mux seams. run_client_udp_stream uses the shared QUIC reader;
its datagram callers must not be mistaken for Product input. Preserve exact
identity where available and native request-stream id/declared singleton
mapping where not; no guessed path identity, extra native state or changed
queue policy. Also record before/after reader mailbox send for the same data
frame, so an earlier decoded frame blocked on delivery is not called wire loss.
One frozen/reversed overlay capture, label
mixed-combined-up-response-quic-handoff-diag-0908, same profile. Falsifier:
if the winning data decodes early, local pipeline caused later service delay;
if decode is late, the remaining native/reader boundary must be distinguished
before blaming congestion. Do not implement ACK coalescing, queue limits or
actor priority simply because they look relevant. No further ordinary run.

Reader observer records aggregate preceding nondata-frame channel-send await
count/total/max since the preceding response, then resets at response mailbox
completion. These cfg-only timing counters avoid logging every upload ACK;
they measure await wall time, not CPU or exact socket arrival. Explicit optional
client Product identity disables this observer for server/datagram callsites.

**QUIC observer frozen/reversed, 06:12:** optimized build2m04s, no warnings.
`./.tmp/reflection/bin/response-quic-handoff-diag-20260908/mptunnel` has the
archived9-file RESPONSE_QUIC_HANDOFF_TRACE_20260908.patch only. Runtime overlay
fully reversed before execution. Server traces both normal/interlocked and
deferred-direct writes; client traces reader, normal/interlocked forwarding,
fanin and mux. No TCP overlay, sampler or native-controller tracing enabled.
One same-profile diagnostic begins; root/runtime matches f64b904 before launch.

**QUIC chain closed, 06:20:** exact260,571,136B/42.258328s/49.329Mbps;
maxconfirmationgap9.433234s. Diagnostic only, not ordinary comparison. Raw
RESPONSE_QUIC_HANDOFF_20260908 archive and matching trace patch retained.
All82 ordinary QUIC writes join through decoded input and mux;78 advanceF.
Logged dispatch->writebegin<=2ms/writebegin->localcompletion<=1ms. No server
writer delay of seconds for these observed frames.

**Actual local service defect class proven:** winning[768,782) has a1.856s
F768 plateau (Unix1788819201805->9203661). Between previous ordinary-response
reader mailbox completion9200797 and this decode9202424,474 nondata channel
sends consume1.622826s of1.627s. At least~.615s overlaps the actualF plateau;
the disjoint decode->mux interval contributes1.237s. Thus~1.852s of1.856s is
inside local input handoffs, not native-read waits. Winning[796,810) likewise
accounts for~.862s of.875s. Early[114,126) crosses decode->mux in1ms with
available queues. These are useful delayed winners, not duplicate maxima.

Do not overclaim: the11.707975s preceding-send counter for[754,768) spans
21.872s since prior ordinary response[657,670), including11.975s before this
frame's publication. It cannot all be assigned to the largest9.433s F754 gap.
Its1.012s postdecode delay is directly observed. Await totals include executor
scheduling and recipient service; sampled full queues do not identify exact
CPU cost or prove every elapsed microsecond was Pending on capacity.
Nondata counts are not solely ACK counts. F670's QUIC repair winner correctly
bypasses ordinary reader tracing, not a missing observation or decoder defect.

**Next exact question, no fix yet:** identify what limits client input service
under existing workload: expensive per-turn scheduling/recovery preparation,
per-ACK ownership/evidence work, excessive/redundant feedback, native-write
interlock or executor service. Read source/history and existing cost hooks;
select one small causal cost discriminator before another build/run. No queue
size/priority/ACK-coalescing or congestion tweak from saturation alone. Preserve
exact ACK semantics, epoch/lifecycle fences, native authority and cooperative
class rotation. Completed handoff traces locate the problem but are not proof
that a particular processing optimization is safe or practically sufficient.

**Cost discriminator declared, 06:27:** reuse the September6 aggregate profiler,
not a new harness. That earlier profile disproved direct flight/ACK cost as the
dominant old tail and found the since-corrected quadratic preparation sweep.
Current source has the equivalent sweep, so do not repeat that attribution.
One temporary lab-only overlay records whole live relay preparation (including
any awaits), complete ACK handler including subsumed ACKs, per-kind Input,
Dispatch and Read handlers, and nested retained-frontier, path-recovery,
ACK-gap, staleness, source-admission, Product-ACK and flight-release scopes.
Reuse existing per-second count/total/max aggregation with individual samples
off. Enable client-only via the existing diagnostic event filter, without
changing runner/environment plumbing. Reuse the exact QUIC handoff observer
to align costs with actual winning F gaps; omit per-ACK diagnostic printing.

Question/falsifier: does synchronous ACK or preparation work occupy those
intervals, does Dispatch/local-I/O waiting dominate, or is time largely outside
the instrumented actor? Nested totals overlap; await wall time is not CPU.
One same-profile `mixed-combined-up-response-client-cost-diag-0908` capture on
a frozen/reversed overlay, no runtime policy or profile change. If one owner
dominates, inspect its input sizes and existing equivalence model before a RED
or fix; otherwise follow the measured missing boundary. No ordinary rerun,
queue/priority/coalescing adjustment or broad new audit is authorized by this
profile. Ordinary timing and the unchanged global matrix still gate acceptance.

**Cost observer frozen/reversed, 06:31:** optimized build2m03s, no warnings.
Independent review confirms only observation scopes, no control-flow/policy
change. Frozen binary `./.tmp/reflection/bin/response-client-cost-diag-20260908/mptunnel`;
full overlay RESPONSE_CLIENT_COST_TRACE_20260908.patch is archived and reversed.
Source runtime matches5cc2c2b before the capture. Preparation includes any
disconnected waits, Dispatch includes its cooperative yield, and Input_Data
includes local-I/O waits; timestamps describe wall time, not CPU attribution.
Events include exact QUIC response handoffs, dispatch/repair and holes, plus
client_cost_profile. Individual stream_ack_received printing is disabled.

**Cost capture closed, 06:36:** exact450,166,784B/44.532321s/80.870Mbps;
maxconfirmationgap5.083127s. Instrumented random capture is not an ordinary
improvement over the preceding capture. Preparation7.009682s includes retained
frontier3.140200s; whole ACK handling2.070443s and Dispatch1.585453s are separate.
Actual winning F65 plateau1.140s has~1.136887s in local handoff/service, and
F54's.957s has~.952771s. Early busy profile windows contain preparation.40--.64s
and ACK.13--.35s per approximately1s. This is substantial competing local work.
The largest5.083s gap instead includes3.161s before the winning response's
dispatch; actor preparation is small there. Do not call one computation the
root cause of all stalls or optimize flight settlement from aggregate totals.

**Bounded work correction preflight:** retained recovery performs a full
`[F,N)` owner-uniform discovery, then repeats discovery on the already limited
scoring extent. The first query's assignment timestamps are unused. Both
queries originated in53d9ab5 to remove storage-chunk dependence; bfac5b8 then
enforced ranked-extent Apply. Active eligibility011aee9 increased how often this
same work runs. The equivalent O(N log N) sweep remains correct, but scanning
unrankable later suffixes and doing the second query is unnecessary work.
Its measured3.14s is a useful bounded optimization target, not a claim that
removing it will recover3.14s of user latency or close the largest outage gap.

Before production code: prove prefix restriction under frozen ledger/live
membership. If U is the end of the full owner/avoid-uniform prefix and Q is the
unchanged selection limit, one query over`[F,min(N,F+Q))` returns exactly
`[F,min(U,F+Q))`, including original ordered identities and assignment maxima
within that returned prefix. Use that shorter prefix when U<F+Q, not a new
full-Q admission requirement. Every later Apply already has service<=scored
extent, so owner/avoid/cache/clock/rank/credit inputs stay equal. No RFC policy,
quantum, clock, membership, controller or queue change is required.

Smallest RED/control: actual request cache/flight producer and retained helper
with identical ranked prefix but a large legal later suffix; count existing
test-only model visits and show the helper unnecessarily visits the suffix.
Keep a short-prefix control, owner boundary<Q and pure overlap/clipping oracle
equivalence. Production changes only after this actual intended RED. Then
remove the full query, independently audit all preserved inputs, run affected
focused checks, and declare the isolated ordinary comparison. Reject on semantic
counterexample or adverse ordinary timing; no restoration of T06 amplification,
ACK-coalescing, preparation-skipping or new caching state is implied.

**Actual helper RED, 06:43:** existing target/ranked-range/shorter-owner-boundary
checks pass, then legal64B storage chunks give229chunks/2744visits control
versus1024chunks/7514visits, both with the same14600B scoring limit. Only the
intended unrankable-suffix work assertion fails. No resource or timing parameter
was changed. Command `cargo test --release --locked -j1 --config
'profile.release.package.mptunnel.opt-level=0' --lib
completion_tail_uses_cache_independent_ranked_frontier_for_target_and_apply
-- --nocapture`; build1m10s, test.86s. This observes real cache/flight producers
and synchronous retained helper work, not a claim of native data delivery.
Two independent reviews confirm prefix-restriction equivalence and Apply bounds.
The one-query deletion is now authorized; pure clipping/oracle controls and
affected focused GREEN precede its ordinary comparison.

**Single-query implementation, 06:44:** productionrequest.rs removes the full
discovery and redundant second query, querying the current ranked limit once.
It uses the returned shorter owner-uniform prefix, not a full-Q requirement.
Independent diff review confirms unchanged cache, assignment/copy clocks,
owner/avoid ordering, target/rank/native authority and publication cause.
No shared sweep algorithm or RFC policy changes. Component tests still pending.

**Focused GREEN, 06:46:** after1m08s functional build, both229- and1024-chunk
cases perform1372visits (formerly2744/7514) and the unchanged exact admission/
boundary controls pass. The existing4096 oracle cases also pass four restriction
quanta each, including zero. Same executable passes4 frontier-model,31 request
sender,28 requeststream and258 relay tests (321distinctchecks); no warnings.
This proves cheaper equivalent work for the fixtures, not full performance.
Freeze the normally optimized candidate, then execute the already declared
ordinary pair with no diagnostics/build overlap. No other runtime change.

**Declared ordinary gate after GREEN:** one fixed control/candidate mixed
combined upload pair. Control is frozenresponse-retained-20260908 (953a54f);
candidate is only the request one-query deletion atopcurrentruntime. Both
endpoints use their cell's binary, diagnostics off; no overlays or sampler.
Labels `mixed-combined-up-frontier-scope-{control,candidate}-0908`. Same network,
40s load/probe/observations; preserve exact confirmed bytes, full bins, first/
maximum confirmations and writes, orderedtarget/reply stages and cost. Removing
work predicts cheaper evaluation, not an automatic speedup or changed policy.
An adverse/ambiguous pair stops promotion; no third run-to-pass. This pair
isolates a new exact-equivalent computation change, not a repeat of the earlier
response-recovery pair hoping for a favourable number. Wider gates stay open.

**Initial-state boundary confirmed:** with no response ACK, the default
snapshot has H=None/F0. Even a complete empty ACK leaves the old contiguous
predicate false because it also requires F>0. The apparent no-ACK exception
only enters an outer block whose inner recovery branches reject this state.
Negative-H staleness correctly has no observations here; exact failure needs
actual owner removal. Thus a mature live original, open response source and
admitted distinct alternate have no active retained fallback until ACK/EOF.
Unlike the fragmented-H case, this needs no unusual range count and can affect
first replies. Native recovery still operates; delayed DataACK may make the
bounded copy redundant. It remains unproven attribution for the67B plateau.

**Next ordinary gate (after focused GREEN and audit):** one control/candidate
mixed combined upload pair, both endpoints ordinary diagnostics-off, fixed
765683b versus only this response correction. Same profile/probe, no initial
QUIC intervention, sampler, network/config adjustment or extra bulk tracing.
Keep actual first/max confirmation and write gaps, complete receiver-confirmed
bytes and full bins, ordered target/reply stage snapshots, directional wire,
CPU/RSS. An adverse/incomplete pair stops promotion and requires one causal
question; no third run-to-pass. Useful download/both-direction and full global
gates still follow, not waived by a small response RED.

Exact network/probe profile remains unchanged: upload70/20ms delay/jitter;
return30/5ms. Five-second loss epochs upload[3,8,5,6,10,3,5,8]% (mean6),
return[1,2,.5,3,2,.5,1,2]%; upload10Mbps at15--25s, UDP outage30--33s.
40s load, existing85s runner observation guard and90s probe completion boundary;
censoring is not complete throughput. Netem random realizations are not identical.
Single500Mbps is a configured link limit, not a per-confirmation-window bound.
Do not reuse the runner's old aggregate300/200 defaults for the required200each.

## Existing dispositions and attribution limits

- Retain demonstrated native reordered-packet/history corrections, exact
  qualification, mailbox-capacity wake, cooperative actor, equivalent
  live-owner frontier sweep, paired QUIC repair, stateless ACK, and exact
  terminal/reconciliation cleanup. CHANGE_DISPOSITION_20260907 and
  PERFORMANCE_REFLECTION_20260907 preserve origins, costs and adverse data.
- Static score prototype, stateful relative ACK codec, ready-feedback batching,
  absolute-delay reordering, wrapperless actor, 3N1 and isolated raw-byte
  hysteresis deletion remain rejected/removed. Do not revive by another name.
- Queued original waiting behind a native domain and accepted repair awaiting
  actual wire/peer service are distinct. One earlier repair was accepted in35ms
  but arrived2.737s later; publication is not delivery.
- Captured probe3 reached client mailbox before D but actor application followed
  expiry. RFC15.2 and `3d68243` intentionally fence effects at application.
  This observation does not license lengthening D, accepting a retired probe
  or relabelling ingress as effect. Actor delay's ordinary contribution remains
  unquantified; it is separate from initial ownership and the partial-copy fix.
- Finite1941 mixed requests plus64 single-mode controls reclaim owners with
  flat observed post-load RSS. Preserve restart/half-close/terminal semantics.
  The uncaptured deployed random RAM/CPU incident is not fully attributed.

## Global gates — unchanged, not satisfied by an isolated GREEN

| Order | Scope | Required evidence |
| --- | --- | --- |
| 1 | Mixed allocation, request/upload sampling, cold/warm startup | Exact cause, conditional model, real RED/control, independent audit, focused GREEN; ordinary first-body, gaps, loaded latency, complete bytes. |
| 2 | TCP/QUIC QoS, loss, jitter, blackhole and recovery | Both directions; same-request restart-free recovery. Separate native work, Product receipt and ordered application service. |
| 3 | Aggregation/shared contention | Single500Mbps, independent200Mbps each; shared cuts and asymmetric changing3--10%mean6 loss/jitter/QoS/outage combinations plus ablations. |
| 4 | Actual user experience | TCP, QUIC and default; cold/warm, single/concurrent and actual speed.cloudflare.com. Raw TCP, Xray and Hysteria2 under matched conditions, including failures and upload confirmations. |
| 5 | Sustainability | Restart/churn, backpressure, ownership reclamation, CPU/RSS and post-load recovery; reopen components only on contrary evidence. |
| 6 | Publication | Complete timing/latency curves, overhead and completion alongside goodput. README/PERFORMANCE and release only after practically competitive gates. |

## Execution and continuity

- Owned Docker topology only; no sudo, host shaping, outside-repo work or
  build/lab overlap. Products/probes are currently stopped; origins retained.
- Read method/current plan after compaction. Continue the one active transaction,
  not a new broad audit. Commit isolated evidence/model/code dispositions.
  No component-green => whole performance inference; no average hides stalls.
- Preserve useful artifacts before scoped cache cleanup. No deletion this
  transaction; no unaccepted runtime overlay or user edit belongs in a commit.
- Telegram milestones authorized no more often than hourly; last sent
  2026-09-07 about22:08UTC (adverse ordinary result and response-stage isolation).
  Respect its soft frequency advisory: nonessential messages can wait.
- Universal clairvoyant optimality under arbitrary future outages is impossible.
  This does not waive avoidable software delays or any practical acceptance
  gate. Never describe unfinished work as ideal or promise cost-free discovery.
