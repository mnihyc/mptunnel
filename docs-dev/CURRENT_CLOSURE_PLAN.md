# Current deterministic closure plan

Updated:2026-09-09 09:54 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before every
transaction and after compaction. This is the active scope/decision ledger,
not a new issue inventory. Full pre-condensation chronology is retained at
`git show 13876d6:docs-dev/CURRENT_CLOSURE_PLAN.md`; the subsequent observer18:56
entry is preserved below. Earlier history, including rejected approaches,
remains at `git show ebad57f:docs-dev/CURRENT_CLOSURE_PLAN.md`. Linked reports
retain exact ranges, raw timing bins, costs, RED/GREEN logs and observer patches.

## Active transaction: user-requested mixed-mode architectural redesign

**Attribution10:03:** interim model checkpointb2aa215 committed with1241checks
andfullordinarypair, not acceptedperformance. The reused69lineobserver is
againfullyremovedafterfreezing. DiagnosticACK344391frames/9646477B, everyACK
onerange(max34B);MAX191677/4983602B. InsideactualUP10, same-kind8.006s
countersgiveACK1.574927Mbps+MAX.759566Mbps. Prior sparsehistorycost is gone;
furtherhistorycompression is not the nextfix. PhysicalUPremainsqueued~9.75Mbps.
Diagnostic293Mbpsrestricted/.321sbodygap/79echoes(max.838s) is NOT a replacement
forordinary186Mbps/.811s/max1.479s. Preserveboth observercostandrunvariability.

ExistingTCPsocketrow20 shows~2.3MBpayload/~47–49kdatasegments percarrier,
mean~47–49B; nativepacketization contributes beyondencodedMPPbytes. Read
FEEDBACK_PACKETIZATION_MODEL completely before anyrenewedproposal: its earlier
ready-feedback batching failedpairedtimingrepeat(292→302Mbps,worsegap), so do
notreviveit merelyfromsmallrecords. Nextreadonly nativecounterjoin andexisting
writerboundary distinguishpureNativeACKs, tinycontrolsegments andQUICresidual.
No newcontroller,fanout,cadence,queuepolicy orpacketizationfixselectedyet.

**Residual discriminator09:50:** candidate UP remains continuously queued
throughrestriction,9.782594Mbps sampledphysicalservice versuscontrol9.733527.
UP bytes fall118.606→81.781MB but packets rise556903→731328, and candidate
CPU/serverRSS rise. Telemetrywindows differ by~.54s; no exactwireefficiency
or ACK-only attribution follows. Reuse the existing two-file codec observer,
only renaming complete counter to scoped: fixedframes/encodedbytes/rangebins
and same-kind timestamps. One sameprofile mixed capture separates surviving
cumulative-range repetition/ACKfrequency/MAX from external/nativepacket cost.
Information forecast: lowACKcost rejects further ACK serialization work;
dominantACKs select producer/publication service, not controller tuning.
No new policy/queueimplementation; freeze diagnostic then reverse its source.
Keep partial ordinary benefit and higher costs, no performance promotion.

**Ordinary outcome09:47 — promotion held:** first candidate completes the
unchanged40s mixed DOWN cell:336.430Mbps whole;427.275→185.569→385.708Mbps
in5–15/15–25/25–40windows. Refreshedcontrol327.137whole,
441.551→104.527→411.605. Max bodygap2.249929→.811220s;76/76candidateechoes
succeed versus34success/1timeout/35unattemptedcontrol. Candidateecho
p50/p95/max250/580/1479ms. HTTP200durationpartial, notcomplete8GiB.
This is partial restrictedservice benefit, NOT declaredstallclosure; restored
phase worsens~6.3% and baselineQ/raw/H2restrictedservice remains~441–476Mbps.
Keep the exact model/tests but withhold performance acceptance. Analyze physical
returnservice/costs, then use the existing codec observer if needed to separate
residualACK amplification from native/recovery/local service. No favorable
rerun, parameterchange or queued-latestimplementation stacked. Telegram01:47UTC;
nexteligible>=02:47UTC.

**Ordinary gate09:45:** all1241affected protocol/mux/path/stream/sender/relay
checks pass after the independently reviewed fixture repairs. No new runtime
patch was required by those five failures. Build the default optimized binary,
freeze scoped-ack-20260909, and run mixed DOWN once using exactly the refreshed
control profile/tagged scoped-ack-candidate-0909. Acceptance requires actual
restricted/restored user service and echoes, not merely less encoded feedback.
If material stalls remain, stop promotion and attribute the residual before
adding changes. Otherwise check Q-only and healthy mixed plus UP composition.
No public README or release claim follows a single improved case.

**Focused outcome09:44:** lower-memory test build succeeds in1m35 without
warnings. Broad scoped-name filter25/25passes (includes unrelated matching
names, not25newtests). Affected suite1236passes/5fails. Independent review
attributes allfive to fixture migration: two passed positives where exactG
was required; one expected a redundant now-omitted scope; two unknown-owner
fixtures used an old helper that inventedH=assigned4096 from ACK[0,1024).
Replace those two with genuine scoped positives[0,1024)+[4095,4096), giving
G[1024,4095); retain survivor/dispatch assertions. Do not widen runtime recovery
to satisfy that fabricated authority. Retained-owner controls outsideG already
pass. Rebuild/recheck actual repairedfixtures before ordinary tests.

**Build boundary09:38:** semantic test-call migration is complete and reviewed.
The third default debuginfo2 test compilation reports no type errors, then
rustc exits by SIGKILL before producing a test executable. This is not a
Product test result or proof of a runtime memory leak. Retry once with test
debuginfo0 and two build jobs to reduce compiler footprint; no Product/profile
configuration or ordinary release optimization changes. Remove the now-unused
has_sent_ack accessor left by removal of TCP-only sparse publication. All
functional checks and the ordinary mixed asymmetry pair remain required.

**Control outcome09:19:** frozenordinary4c7e232 reproduces the existing defect:
441.551→104.527→411.605Mbps (5–15/15–25/25–40 bins),2.249929s body gap
22.724777→24.974706s,34echo successes plus one actual3s timeout19.954177→
22.954310s and35unattemptedafterdisconnect. ActualUP10Mbps/DOWN500 during
restriction confirmed; all40rawbins retained. Thus the new candidate still
has a material ordinary comparator, not just a diagnosticcounterfactual.
No compilation overlapped. Same-directory rawresults remain forpairedarchive.

**Matched-control09:18:** while source integration continues, run one frozen
ordinary4c7e232 mixed control under the identical asymmetric500Mbps DOWN,
500→10→500Mbps UP profile, RTT100ms split30/70, no jitter/configuredloss/outage.
No build overlaps this41s run. This refreshes the comparator after the elapsed
host/session interruption; it is not a favorable rerun or a candidate test.
Question: does the documented feedback collapse still reproduce on the current
idle host before attribution to the candidate? Retain all bodybins/echoattempts,
actualshape snapshots/costs and any contraryresult. Failure to reproduce weakens
immediatepaircausality and forbids promoting merelydifferentaverages. Tag
scoped-ack-control-0909; use existingrunnerunchanged and ordinaryfrozenbinary.

**Execution09:19:** resume the same scoped-ACK implementation after an agent
usage interruption; three bounded assignments now finish producer, consumer
and semantic tests. No ordinary build/lab has run on the partial tree, and
README remains unchanged. User explicitly defers README/baseline publication
until the model is practically proven immediately before release. Keep all
high-capacity timing/recovery gates; no poor-baseline100Mbps objective.
One algebraic simplification omits an optional scope when its omission set is
empty, avoiding extra wire bytes for contiguous Q updates. This changes no
facts, cadence, controller or limit. Runtime candidate remains unaccepted.

**Selected proof02:27:** SCOPED_ACK_SERVICE_MODEL.md replaces the proposed
base-generation seal with independently scoped positive/negative evidence.
Every frame is meaningful alone; negative authority is only its explicit
interval, never a guessed global horizon. Previously acknowledged bytes cannot
be resurrected by stale negative evidence: intersect with the exact remaining
send cache. Full catch-up chunks each describe their own disjoint scope.
Keep existing publication cadence/fanout/cursors, no timer/rate/CC change.
The fixed-count~71%ACK-byte forecast is conditional; Q-only header cost and
increased feedback under restored forward service are explicit non-regression
risks. Source integration must consume explicit gaps everywhere before tests
or practical promotion. Ordinary source still4c7e232; no release acceptance.

**Queue result / bounded redesign decision02:09:** exact three-file observer
is archived FEEDBACK_QUEUE_TRACE_20260909.patch; warning-free1m03build frozen
feedback-queue-20260909, then ALL source additions reversed. Both audits pass
counter conservation and cancellation; no ordinary runtime changes. One mixed
capture again fails materially:436.083→103.439→419.267Mbps phases,3.642143s
body gap22.008807→25.650950s;34echo successes, one actual3s timeout and36
unattempted-after-disconnect slots. Retain failure and observer cost honestly.

Queued replacement opportunity is real but limited:313269ACKtakes,82354with
newer same-kind work pending(26.29%); associated range support23.36%. MAX29.01%.
Inside restriction roughly31%ACK/MAXtakes and30%range support are flagged.
All225summaries conserve accepted=taken+dropped+pending. These are generous
schedule-dependent opportunities, not exact safe substitutions or saved bytes;
queue-admission ordinals are not producer generations. Native-started work is
outside the scope. ACK encoding still peaks33.207Mbps plusMAX1.119Mbps.

Decision: DEFER standalone queue-latest implementation, not call its real
opportunity a fake defect. The observed~30%scope does not credibly remove the
>3×offered-feedback mismatch, and would add exact-owner/terminal machinery
without a convincing full-defect gain forecast. This discriminator avoided
an unproven implementation and its test/cleanup loop. Keep the proof for later
if useful, but do not optimize small remnants before the structural owner.

Next existing mixed/stall model question: repeated full cumulative receive
history costs O(A×sum R_g) across accepting attachments and generations.
Establish explicit incremental feedback facts/checkpoint semantics using the
single Product positive ledger and existing per-attachment publication fences.
An ordered carrier's prior accepted feedback may justify a delta, but only if
negative horizon/checkpoint completeness and replacement/cancellation/chunk
ordering are proven. A fresh or missed-generation attachment needs truthful
cumulative catch-up. No hidden mutable codec dictionary, timer/rate cap, false
complete delta or shortened gap authority. Source/model audits proceed first;
no wire/runtime change until a bounded proof and magnitude forecast exist.

**Queue-opportunity discriminator01:54:** same-observer Q control sustains
438.794Mbps during restriction and442.089after,80/80echoes,max301ms. Last
reported ACK is134221frames/3606579B, every frame single-range, versus mixed
396286frames/47375035B/6562914ranges. ACK fixed21B framing accounts8.322MB
of mixed; packed range support39.053MB. Half the observed20.959Mbps ACKpeak
plus unchangedMAX still exceeds10Mbps before native overhead. Savings may be
material but only if they preserve the facts; no claimed throughput multiplier.

Next exact question: are obsolete COMPLETE ACK/MAX snapshots still queued at
MPP's cancellable boundary, or already handed to native transport? A temporary
feature-only queue shadow counts actual accepted enqueues, take/drop, pending
same-kind revisions and same-stream barriers. Bounded bookkeeping/no payload
retention, actual frame counts/range counts rather than allocation-size 'bytes'.
This is an opportunity upper bound under the unchanged schedule, not predicted
saved wire. One unchanged mixed feedback-restriction capture, same codec observer,
no runtime policy. Low opportunity rejects this queue-state implementation;
high opportunity selects a producer-owned exact-incarnation latest envelope,
whose first publication is immediate and whose frame is sealed at dequeue.
It must preserve incomplete ACKs, terminal order and one accounting transfer.
No reason yet to change wire format, negative horizon, full fanout or CC.

**Discriminator outcome01:47:** feature-only observer builds warning-free1m03,
is frozen as feedback-encode-20260909, and all69source additions are reversed
before capture. Current ordinary source remains4c7e232. Independent audit PASS:
encoded attempts exclude native overhead/retransmission and may include later
failed/cancelled writes; counters are cumulative at last event-driven report,
not final exact totals. Each kind has its own stamp; compute deltas between
successive rows of that kind, not groups of numerically equal millisecond stamps.

The repeated mixed feedback restriction reproduces a2.523657s body gap,
451.224→157.891→417.733Mbps body phases and71/71actualechoes, worst2.871s.
At last report the client has396286ACKframes/47375035encodedB/6562914ranges
(all complete, max87ranges), versus281279MAXframes/7313254B and other control
negligible. ACK alone reaches20.959Mbps across19.530→20.534process seconds,
during restrictedreturn; before QoS it also repeatedly exceeds10Mbps. Sparse
cumulative ACK serialization is therefore a real material owner, not merely
an aggregate-wire guess. This does not yet attribute every queue byte or prove
baseline-like full-capacity service can be recovered by an arbitrary ACK change.

One Q-only same-observer/profile control now discriminates intrinsic native
feedback cost from mixed cumulative state amplification; no new build/policy.
In parallel establish the smallest proof-preserving publication model. A single
ACK already updates shared Product authority across attachments; all-attachment
copies supply redundant delivery service, not distinct logical facts. Removing
fanout without replacing slow/failed-carrier service is invalid. A narrower
complete prefix also does not prove newly exposed gaps above its own horizon.
No runtime correction is selected yet. Telegram milestone17:47UTC; next>=18:47.

**Next discriminator01:41:** one temporary feature-only codec observer counts
successful per-frame encodings by wire kind, exact encoded frame bytes, and ACK
range/count/complete distributions. Report cumulative counters at one-second
observation intervals; retain every summary, no per-frame flood. These are
encoded attempts, not transmitted bytes: later batch validation, cancellation,
native handoff or retransmission can differ. No transport/control policy change.

Competing causes are MPP cumulative ACK vectors, MAX publication, source/repair
traffic, native ACKs/retransmissions and unrelated control. Source inspection
shows changed sparse receive evidence is repeated cumulatively on each accepting
attachment; unchanged generations are already suppressed. That is a candidate,
not attribution of the120.6MB ordinary return total. Forecast is information,
not speed: classify the dominant serialized return owner during one identical
mixed asymmetric15–25s feedback restriction. Small ACK contribution falsifies
that candidate. Large contribution selects its exact publication semantics for
a counterexample/proof before any fix. Preserve immediate positive release,
complete/partial gap authority, exact per-attachment progress and terminal order;
no rate cap, cadence tweak or restored stateful ACK dictionary is justified.

**Profile correction / decisive asymmetric failure01:32:** the completed
panel's actual mirrored shaping limits the client→server/feedback direction,
NOT server-egress as mistakenly forecast below. All four saved class snapshots
show DOWN500Mbps throughout; UP10Mbps in rows16–25, restored500 atrow26.
Keep this as an asymmetric return-service experiment; do not label it a
download-cap recovery proof, hide the setup mistake, or rerun until favorable.

In that phase Q-only440.691Mbps, raw475.851, H2470.379 remain fluent;
mixed falls124.563Mbps with one actual echo timeout15.988→18.990s.
The42later unavailable-after-disconnect records are probe-owned censoring,
not independent sends or proof that MPP cannot reopen. Mixed bulk maxgap
1.246742s occurs19.675833→20.922575. Restored25–40means are Q428.610,
mixed429.321, raw462.514,H2467.072 with no zero bins. Bulk restoration is
useful, but mixed usability under asymmetric feedback is plainly not accepted.
All other modes retain80/80echoes and30/30in restored phase. Configured random
loss is0, but mixed UP queue overflow drops8508packets; other modes drop0.

This is the highest-impact existing mixed/stall owner: determine which native
or MPP feedback work fills the return queue and creates the critical stall.
Healthy mixed already spends much more return wire bytes than Q-only/raw/H2;
wire totals alone do not identify ACK/MAX/copies or forbid legitimate feedback.
Do not weaken ACK validity, merge authoritative snapshots by analogy to MAX,
restore rejected ACK dictionaries, invent a feedback cap or blame a10Mbps
environment in which the baselines sustain~470Mbps downloads. Next minimal
discriminator is actual per-kind serialized feedback volume plus existing
critical-frontier/path/wire observations. Hold low-volume membership policy
implementation until this larger demonstrated failure is attributed.

**Echo outcome / recovery gate01:29:** all80echoes/5120uniqueB succeed in
the selected diagnostic. All81winning fragments are TCP Originals; eight
late duplicate fragments add448B and win nothing. Source→claim max7ms,
claim→positivewrite max1ms, decode→localdelivery max2ms. Largest post-write
interval461ms; the slowest762ms echo additionally has280ms between target
local-write and response read. Do not call post-write residence network-only.

Actual echo membership is only two TCP attachments despite one stable active
session QUIC carrier. RFC8.1 startup h58400 is never reached by5120B; ordinary
rebalance is Throughput-only since282b8e1, making control's nonbulk branch
unreachable. That branch also requests BulkStriping, so simply ungating it is
not a coherent latency fix. count>1 only preserves the set on first stall;
9ab3cbcb's persistent-stall and receive-hole opens bypass that condition.
Thus this is no ongoing low-volume membership reconsideration, NOT a proved
recovery deadlock or rejection of an eligible QUIC writer. Attachment alone
may save none of the delay because QUIC shares the loaded network queue.
ECHO_OWNER_SERVICE_20260909 retains the joins, ordinary/diagnostic distinction,
observer patch and raw evidence. No latency policy change is accepted.

**Next immediate experiment:** close the latest-credit model's existing
high-capacity recovery gate before another latency implementation. One fixed
ordinary DOWN panel: MPP QUIC, mixed, raw TCP and Hysteria2; existing healthy
500Mbps/100ms asymmetric-delay shared cut, except the already available
10Mbps server-egress epoch15–25s, restored500Mbps25–40s. Keep loss/jitter/
blackhole disabled, every timing bin/echo/failure/cost, unchanged40s workload
and H2's explicit500Mbps prior. No compiler overlap. The10Mbps phase is a
diagnostic perturbation, never a performance claim or target.

Information forecast: determine whether multi-second stalls/poor restored
service survive the current ordinary state model when real capacity returns,
and distinguish mixed-only from Q-only/baseline recovery. Compare observed
restored histories and completion/echo gaps, not whole-run Mbps. A material
MPP-only collapse selects this existing stall owner ahead of low-volume
membership work. If recovery is useful, retain that conditional proof, then
construct the minimal membership counterexample/proposal separately. This
does not waive changing-loss/blackhole/independent-aggregation/Cloudflare gates.

**Echo discriminator01:13:** preserve the completed twelve-cell comparison in
HIGH_CAPACITY_REFERENCE_20260909 (407 available bins,445 attempts,64-member
verified archive). One temporary diagnostic build on4c7e232 will observe only
the actual echo logical stream, bound at its target-aware opening, not the
management display ID. No change to ordinary authority, classification,
placement, native transport, profile or probe workload is authorized.

Information forecast: one healthy mixed DOWN capture can locate the material
~0.4s loaded-echo excess relative to matched baselines among pre-output service,
claim/selected transaction, post-handoff native/network ordering, and client
delivery. Include the echo's exact eligible attachments and claim selection;
existing aggregate path/queue data cannot prove an alternative existed. If no
critical slow echo recurs or the trace cannot join its boundaries, retain that
limit and do not infer a model fix. Diagnostic speed is not ordinary speed.
Any proposed correction must explain why it removes the measured critical wait
without reinstating rejected completion-rate admission vetoes or sacrificing
bulk, sparse work, native ownership and failure/recovery. This is potentially
hundreds of milliseconds per common request, not a ten-millisecond microfix.

**Matched panel complete 00:58:** current ordinary4c7e232 behavior yields
MPP TCP/QUIC/mixed UP421.445/441.123/415.285Mbps, all exact complete;
DOWN420.435/424.650/403.212Mbps, all duration-partial successful bodies.
Raw DOWN445.190, Xray444.097, Hysteria2466.401Mbps. Crucially loaded echo
p95 is raw126ms/Xray127ms/H2114ms, versus MPP TCP1247ms/QUIC191ms/mixed501ms.
All actual DOWN echo attempts succeed; fewer TCP attempts result from slower
sequential echo completion, not omitted failures. This is not near-perfect
experience, even though mixed-UP bulk and confirmation gaps improved materially.

Raw UP459.960Mbps settles exactly. Xray435.647/H2467.542 are confirmed
lower-bound observations only: both lose terminal sink acknowledgement after
the load and leave11588312/11272003 locally accepted bytes unconfirmed.
The runner/probe exit0 is not completion. The current probe suppresses raw bins
when it marks ACK accounting invalid; those curves are unavailable, not zeros.
Do not invent missing histories, call these completed baseline uploads, blame
their protocols without exact closure evidence, or claim an MPP win from this
probe/half-close limitation. DOWN comparisons are independently complete.

**Next highest-impact existing owner:** short-request latency and mixed-path
interference under bulk, now demonstrated on the high-capacity matched panel.
TCP server queue_bytes is tcpi_notsent_bytes (native socket FIFO), whereas
inflight_limit_bytes is cwnd*MSS, not a socket-buffer cap. Row21 unsent
18.2/13.7/17.9MB versus cwnd3.99/3.72/5.62MB is not a proved cap violation.
The exact physical writer can reopen Ready after native write/flush acceptance,
before delivery; priority cannot preempt bytes already in that TCP FIFO.
RFC10.4/15.1 and65edae3 intentionally retain this boundary rather than using a
rate-derived admission veto. Do not restore that veto or invent a queue cap.

One exact slow-echo timeline is the next discriminator: request/target receipt,
positive response-source read, Original claim, physical write begin/end, client
decode and local delivery, joined with logical stream/range/exact carrier and
the echo's actual eligible attachments. Prompt claim/write followed by late
decode selects post-handoff ordered/native or network debt; late write selects
the selected transaction; late source selects pre-output work; late postdecode
selects local service. Session-wide four-path presence does not establish that
QUIC was eligible for this echo. Mixed Native RTT333–366ms versus Q-only101ms
supports shared-queue suspicion, not exact attribution or unavoidable delay.
Reuse the existing response observer with echo-only selection; no large bulk
trace, new Native hook, rate/pool/queue threshold or runtime fix before evidence.
The full reference report/archive is verified; no new lab is running yet.

**Ablation outcome / next evidence 00:44:** ordinary mixed DOWN403.212112Mbps
recovers part of the repeated384–387Mbps cost but remains below both413Mbps
fcc controls. Its maxread .323468s and80/80 echoes (p50 .334069s, p95 .501201s,
max .561082s) are retained, not selected as universal improvements. UP settles
2131230720B/41.0558s=415.285Mbps, preserving the state-model throughput gain;
maxconfirmation .789938s and maxwrite .630792s are adverse versus83734b2's
.697809/.540541s, while confirmation is better than fcc1.192361s. Removing
the redundant helper boundary has supported practical benefit within this pair,
not proof that nested charging was the sole DOWN cause. No further code change
is selected from the remaining10Mbps or subsecond extrema without attribution.

Next is the overdue matched **high-capacity reference panel**, not another
micro-adjustment: use this frozen ordinary variant for the remaining TCP/QUIC
UP and DOWN cells, plus raw TCP, Xray VMess/TCP and Hysteria2 in both directions.
Reuse its just-completed mixed cells. Same healthy500Mbps shared cut,100ms RTT
(UP70/DOWN30), no configured jitter/loss/QoS/outage, same40s probes, CPU allocation
and wire/resource/management sampling; no compiler overlap. Hysteria2 retains
its existing explicit500Mbps up/down prior, MPP dynamic discovery stays unchanged.
This conditional panel measures full user service against matched baselines,
including startup, gaps, echoes, completion and costs; it is not a real-Internet
or mixed-impairment victory. A major collapse or failed completion halts widening
and selects its existing owner. Otherwise preserve the complete comparison,
then the already-required high-capacity QoS/outage recovery and heterogeneous
path gates. No poor-link100Mbps target, release or global acceptance claim.

**Ablation execution 00:40:** exact removal-only diff is independently reviewed
and archived as LATEST_CREDIT_ACTOR_YIELD_ABLATION_20260909.patch. Ten focused
latest-state/whole-actor cooperation checks and773 affected checks pass; default
test build55.88s, default ordinary build1m04s, warning-free. The frozen ordinary
latest-credit-actor-yield-20260909 runs mixed DOWN then UP, same healthy shape.
No compiler overlaps either lab. Root HEAD e27e4f9 retains all ten preceding
ordinary cells;83734b2 is the state-model intermediary, not acceptance.
Telegram practical/attribution update sent16:39UTC; next nonurgent>=17:40UTC.

**Reverse pair / one ablation 00:35:** DOWN throughput deficit recurs: latest
state384.475560Mbps versus retained413.225355. Across both orders the candidate
is384–387, controls413Mbps. Latency is not systematically worse: reverse
candidate maxread .206452s/echo p95 .362593s/max .662985s versus control
.275026/.634428/1.162301s; all80/80 and76/76 echoes succeed. Preserve both
orders and the original adverse maxima, not a favorable selection.

Next exact question is one temporary architectural ablation: remove only the
new nested cooperative wrapper around RemoteInput.recv_frame. The sole
production caller remains inside RelayServiceTurn::select's existing whole-turn
cooperative wrapper (59fbd22); Input/Dispatch/Read rotation, actual MPSC await,
finite ready Data collection, logical-state/FIFO fairness, cancellation and
RESET remain unchanged. A direct unbounded consumer would invalidate this
proof; root and independent search find none. Do not remove outer cooperation
or call the historical uncooperative-actor defect fixed by an inner helper.

Information forecast: nested charging can change yield timing and is the one
candidate-introduced scheduling difference found on Data-only input; its actual
critical cost and Mbps effect are unknown. An ordinary ablation can distinguish
that from retained state/Native-distribution effects. Recovering the recurrent
26–29Mbps loss is plausible only if this boundary is causal; no gain/regression
is also plausible. First run existing actor-yield/fairness tests and latest-state
controls, then one ordinary build and affected mixed DOWN and UP. No new knobs,
timers, profile or best-run repeat. Absent benefit stops this branch; restored
DOWN cannot authorize sacrificing the demonstrated mixed-UP gain or latency.
The current state model and all adverse evidence are an intermediary checkpoint,
not a release/performance promotion.

**Ordinary result 00:26:** all three uploads settle exact bytes. TCP
422.069→421.608Mbps, QUIC440.581→437.934, mixed325.867→412.793. Mixed
max confirmation1.192361→.697809s improves; max local write .434806→.540541s
is adverse. Mixed target service improves in every fixed body window, not only
reply accounting. Q first confirmation1.112591→.209224s improves while max
later gap .307224→.896869s worsens; its first bin carries only .096Mbps.
This suggests a split startup episode but does not locate the exact max gap.

Mixed DOWN is adverse/ambiguous:412.872664→386.554728Mbps, max read gap
.265968→.385547s; echoes79/79→78/78, p50 .362451→.305074s and p95
.576166→.556878s improve, max .667799→.826614s worsens. Do not declare
non-downgrade from the upload gain. The immediate bounded discriminator is
existing DOWN path/native/queue/resource telemetry plus old/new Data-only
input service comparison. There is no new trace, runtime edit, threshold or
stress rerun yet. Determine whether evidence supports a changed service rule,
path allocation/Native realization, or insufficient attribution. If only the
last is supported, one predeclared reverse-order healthy DOWN pair (candidate
then retained control), with every outcome retained, can test recurrence;
it is not permission for repeated best-run selection. A systematic adverse
result stops promotion and requires its critical-path evidence before code.

**DOWN discriminator 00:29:** independent/root review finds identical pure
Data FIFO order, entry-count batching, payload quantum and receive ceiling.
Current empty-input async fallback can incur an additional cooperation boundary;
pending advancing MAX can interrupt a Data batch earlier. Neither is shown to
cause the measured deficit. MAX here grants client request bytes, not the bulk
download bytes: do not invent a high-volume reverse-credit dependency. The
bounded extra-lock/Notify/yield hypothesis is not authorization to tweak code.

Telemetry L2→L41 shows server-to-client Native ACK totals near2.2945/2.2922GB
despite less candidate useful body. TCP shares redistribute and QUIC increases;
this is not winning-Original attribution or a proved copy count. Candidate
client TCP Recv-Q peak falls237472→26064B and is0 in samples around its worst
gap. Different transport/queue histories remain a competing explanation;
no measured client-input critical interval selects a new owner. The predeclared
candidate-then-control reverse pair is running unchanged; preserve it entirely.

**Latest-state checks 00:18:** all five actual-input/lifecycle/fairness controls
pass, as do 773 affected sender/request/response/TCP/QUIC/control/server checks
in 3.68s. Default test build is warning-free 1m32s. Root and independent runtime
review find no scope/authority/wake/cancellation blocker. The old final-only fold
is removed; only matching logical MAX enters shared state. Source is frozen for
one default-feature release build, followed by the four declared healthy cells.
The comparator report LATEST_CREDIT_SERVICE_20260909 retains all 166 bulk bins
and 79 echo samples; no candidate or performance acceptance is implied by GREEN.

**Latest-state RED 00:10:** both actual publisher/attachment-forwarder tests
reach the intended two versus one credit-turn assertion after isolated credit,
greatest exact source, unchanged ACK/Data FIFO/content and full drain checks.
The two prior closed/order controls pass; the default test build is warning-free
59.13s. LATEST_CREDIT_INPUT_RED_20260909.patch preserves the test-only state.
The completion witness observes the actual forwarding boundary independently
of queue representation, not an invented raw-queue occupancy condition.

Implementation is now authorized only for one shared latest-credit ingress in
request/attachment.rs, replacing the final-only fold. Independent test work
migrates the obsolete MAX/ACK barrier expectation and adds the declared logical
RESET, fairness, cancellation and provenance controls. The model's authority
claim is max(max(a,b),c)=max(a,max(b,c)); ACK/Data events remain untouched.
Neither that algebra nor RED proves a wall-clock gain. Fair arbitration permits
at most one pending-credit delivery between continuously ready FIFO deliveries;
an isolated credit has no batching wait. This is service ordering, not a new
time/byte/rate threshold. Same-stream RESET is the only protocol credit seal;
opposite FIN and carrier failure do not revoke already received logical credit.

The retained ordinary mixed DOWN control is complete: 2064426284B in
40.001220s (412.872664Mbps), first body .580507s, max read gap .265968s;
79/79 echoes, p95 576.166ms. This 40s download is intentionally partial, not a
completed 8GiB response. Healthy UP controls remain TCP422.069, QUIC440.581,
mixed325.867Mbps with mixed max confirmation1.192361s. Freeze the replacement
once after GREEN/audit, then compare these affected four cells; no lab/build
overlap, no parameter or impairment adjustment, no claimed acceptance yet.

**Healthy attribution / next model23:56:** fcc0b22 diagnostic completes
1748172800B/40.796490s, maxconfirmation1.728562s. All187 Original replies
enqueue→claim<=2ms and claim→localacceptance<=3ms. All421 Data-prefetch
boundaries return<=5ms, API→mux<=4ms; they cannot explain the large holds.
Before any covering reply is decoded during F1970's1.729s hold, disjoint
ordinary-QUIC reader windows contain conservatively>=1.254354s downstream
send-await, >=.890604s while handing MAX. All healthy returnHTBdropdeltas
are0, with67412B maximum sampled backlog. These facts establish input-service
coupling, not earlier Native availability or exclusive MAX-handler CPU.
READY_CREDIT_RETURN_SERVICE_20260908 preserves the complete joins/41bins,
independent audit and7-member verified raw archive. No performance acceptance.

**Replacement contract before code:** RFC8.4 already makes received credit
one logical, directional, irreversible maximum. A proposed shared credit
ingress at the actual attachment forwarder retains only the greatest matching
MAX and its first exact source, plus one coalesced service wake. ACKs, Data,
proofs and other events keep their unchanged FIFO; none are merged. The relay
fairly takes current credit state or FIFO work; isolated credit never waits
for more credit and sustained credit cannot starve payload/ACK work. Actual
mux grant application, initial carrier-admission zero grant, checked C/ACK/
qualification/Native/W/P/E authority and wire publication remain unchanged.

MAX commutes with ACK validity/release at fixed C: it acknowledges no byte,
while ACK grants no offset. Earlier availability may change assignment timing,
not its authority. MAX also commutes with opposite-direction FIN and exact
attachment error; neither revokes shared credit or qualifies a failed carrier.
Independent review initially proposed per-carrier terminal watermarks, then
withdrew them: preserving an identical scheduling trace is not a demonstrated
correctness need. Do not add those artificial dependencies. Same-stream RESET
ingress seals this logical credit owner; prior pending credit is exposed before
that RESET and later updates cannot revive it. Owner cancellation closes it;
raw StreamId equality cannot share its lifetime with another input owner.

**Forecast / falsifier / execution:** current final-only folding still leaves
97670 MAX messages traversing preceding queues; ACK-separated revisions escape
it. Replacing event backlog with latest state at their first common owner can
remove obsolete merged-queue occupancy and redundant relay turns, relieving
upstream backpressure. It cannot remove wire frames, ACK work, physical loss
or guarantee saving the measured .890604s. Material gain is plausible because
the preceding fold improved healthy mixed205→326Mbps, but it may be zero if
other actor/native work dominates; no precise CPU fraction or Mbps promise.
This is a representation/performance correction, not a claim that every old
MAX frame violates wire correctness. A test-only actual publisher/forwarder
ACK-separated cardinality RED precedes one replacement, with fair service,
greatest source, unchanged event order, cancellation and terminal controls.
Replace the final fold, do not stack more folds or tune capacity thresholds.

Root first freezes one ordinary fcc0b22 healthy mixed DOWN control while tests
are authored (no compiler). Then targeted RED/GREEN/audit, one default build,
healthy TCP/QUIC/mixed UP and affected mixed DOWN timings against retained
controls. Only useful stable service permits the retained harsh comparison
and broader gates. Adverse or absent benefit stops promotion and selects one
causal decision, not another rate or queue adjustment.

**Checkpoint23:39:** the bounded MAX fold and772 passing affected checks
are preserved as an intermediary, not performance acceptance. All eight
ordinary outcomes and348 timing bins are in READY_CREDIT_SERVICE_20260908;
its49-member raw archive passes integrity/list/byte comparisons. Healthy
mixed204.707→325.867Mbps is material, but maxconfirmation.769314→1.192361s
worsens. The harsh ordinary pair likewise improves bulk/settlement while
maxconfirmation5.164575→6.383313s and maxwrite3.303759→4.800875s worsen.
The candidate's6.001s sampled target plateau is forward waiting; a separate
1.999s already-read-reply lag remains. Neither is automatically credit CPU.

**Next exact question / information forecast:** one response-only observer
on this candidate's healthy500Mbps/100ms mixed upload, with loss/jitter/QoS/
outage disabled exactly as its ordinary control. This is a high-capacity
residual-stall discriminator, not a favorable speed rerun. Preserve source
read/enqueue/Original claim/positive write/authenticated decode/route/merged
input/mux/local-write joins and typed preceding reader waits. Adapt only the
input hook to distinguish a physical Data prefetch into the existing deferred
slot from later actor-visible return after MAX folding. No per-credit flood,
owner timers, Native/Cargo hooks or production policy change.

Material already-decoded critical residence selects the existing local input/
actor service owner; prompt local service rejects that branch for the captured
hold and leaves before-receipt or before-production service. A long read-await
does not alone prove earlier Native byte availability. No new ACK batching,
planner cache, controller or threshold follows without its own causal evidence.
Archive/review/freeze/reverse the observer before the single capture, then join
winning exact ranges, not losing-copy ages. Root serializes build and lab.
Telegram practical/attribution update sent15:38UTC; next nonurgent>=16:39UTC.

**Observer23:42:** root and independent whole19-file reviews pass;18 files
match the preceding observer exactly and attachment observation alone is
adapted. Warning-free diagnostic build1m05s is frozen separately as
ready-credit-return-20260908; all hooks are reversed and production diff is
empty against fcc0b22. Exact archive READY_CREDIT_RETURN_TRACE_20260908.patch
reverse-checks before removal and applies cleanly afterward. The one declared
healthy mixed UP capture is running, without build overlap. API-return events
are distinct from physical prefetch and actual mux application; no timing
claim may collapse these boundaries.

**Healthy candidate23:24:** ordinary build is warning-free1m20s. Same3 UP
cells settle exactly: TCP426.610→422.069, QUIC440.804→440.581,
mixed204.707→325.867Mbps. Mixed bulk improves about59%, but maximum
confirmation gap0.769314→1.192361s is adverse and late bins contain zero/burst
service. Client mixed RSS625192→504332KiB, sampled maximum ps CPU143→158%;
more completed work is not a normalized cost comparison. This supports a
material influence of input work, not closure or performance acceptance.
Keep the previously declared unchanged harsh mixed UP next; its aggregate is
diagnostic, not a100Mbps target. No wider batching or tuning follows this pair.

**Candidate checks23:19:** the same4 credit-prefix/isolated/boundary/closed
checks pass;772 affected sender/request/response/TCP/QUIC/control/server
regressions pass in3.57s. Default test build59.18s, warning-free. Independent
full runtime review finds no boundary/cancellation/credit-owner blocker.
One default-feature release build is running; no lab overlap. Freeze as
ready-credit-20260908, then current/candidate healthy UP for all3 modes and
the retained harsh mixed UP. Only after useful service evidence should other
affected healthy DOWN or broader acceptance cells proceed. If the clean
mixed deficit persists, do not label the overall redesign solved.

**Credit input RED23:16:** one warning-free40.35s test build. Both actual
async/try input tests fail only at3 versus1 exposed grants after publication,
charge release, isolated credit, greatest-source/tie, Data and queue-drain
checks pass. Both ordering/closed-input controls pass. This proves the proposed
cardinality counterexample, not a credit-authority violation or CPU saving.
The exact test patch is archived. The bounded shared ready-MAX fold is now
implemented in RemoteInput only; independent review and same checks precede
the ordinary candidate. No ACK, producer, controller or resource-policy change.

**Current return finding23:08:** the capture settles388759552 exact bytes in
45.495787s, but maximum confirmation gap6.510292s persists. Its largest F591
gap is6.474s before the server produces the next reply; read-to-local delivery
is37ms. Do not call that a receive-service stall. Separately, TCP Original
[325,339) is positively decoded before F becomes325, yet the entire943ms
frontier hold follows; a later QUIC repair wins before that Original is routed.
This proves material local service delay, not missing input or one long mutex.
Winning[885,899) also spends790ms decode-to-local. All83 source enqueues and
Originals reconcile1122B; enqueue-to-claim<=3ms, claim-to-write<=1ms.

Typed ordinary-reader predecessors contain15998 MAX_DATA frames with12.405800s
summed send-await, versus6138 ACKs/3.276955s. These are downstream waits, not
exclusive handler CPU. F269's1.253s hold contains conservatively>=481ms of
credit handoff waits and a separate532ms postdecode interval. Whole ages and
overlapping totals are not predicted time savings.

**Next bounded question / forecast:** preserve RFC8.4's immediate full-window
credit publication (5c1d288); do not restore quarter-window gating. Client
merged input currently returns each monotonic credit as a separate actor turn,
repeating admission/path observations. Test the actual input path with a finite
already-ready same-stream MAX_DATA run, followed by useful Data. A proposed
latest-credit fold may replace only that consecutive run by its greatest grant;
snapshot ready work, retain the first non-MAX boundary, never wait/coalesce
ACKs/cross terminal or identity boundaries. One private deferred slot must be
accounted in ready/closed state. Isolated credit still returns immediately.

This can remove redundant actor preparation for such runs without withholding
credit; it cannot remove intervening ACK work, pre-source gaps or all measured
local residence. Practical gain may be zero if those other costs dominate.
Real producer RED and opposite grant/ordering/lifecycle controls precede code;
independent review precedes one implementation. Then affected ordinary UP and
healthy high-capacity timing decide retention, including costs and both halves;
an absent material benefit stops promotion, not a new threshold. No new model
or performance acceptance is claimed. Diagnostic archive has7 exact members.

**Symbolic boundary:** for current grant G and an already-ready consecutive
same-stream sequence g1..gn, repeated application yields
max(...max(G,g1),...,gn)=max(G,max(g1..gn)). These frames acknowledge no byte
and carry no path proof. Keeping the greatest frame's actual instance preserves
origin; keeping every non-credit boundary preserves its order and semantics.
This removes n-1 input/preparation turns for that run, not n-1 ACK operations
or a measured CPU fraction. It intentionally changes scheduling interleavings
by exposing already-received valid credit sooner, without changing authority.
The scan count is the entry backlog, not a configurable timer or new cap.

**Ordinary control23:08:** while the disjoint input RED is authored, no build
is running. Freeze current0449b9f TCP/QUIC/mixed upload on the existing routed
500Mbps/100ms profile with loss/jitter/QoS/outage disabled. Three40s cells,
default binary response-claim-20260908, tag credit-ready-healthy-control-0908.
The relevant high-capacity comparison measures confirmed upload plus its tiny
return replies, all timing bins and costs. Existing current healthy download
cells remain their controls. Candidate comparison requires mechanism checks
first; no healthy result waives the retained adverse harsh/loaded timing.

**Healthy UP controls23:12:** all three settle exactly, no failures. TCP
426.610Mbps/0.435956s maxconfirmation gap, QUIC440.804/0.319541,
mixed204.707/0.769314. Mixed therefore has a major clean500Mbps deficit,
not merely a harsh-loss problem. Client sampled maximum ps CPU is84.6/136/
143%, RSS170172/512044/625192KiB respectively; not exclusive cycle attribution.
This changes the practical magnitude to test, not the cause already established:
credit-fold influence remains unproved, and no promise of2x follows. Preserve
all42 bins/costs per mode; the mixed profile/source/host are not retuned.

**Return observer22:56:** root and independent whole19-file reviews pass;
the exact archived patch reverse-checks cleanly. One lab-diagnostics build
is running, no lab. The overlay changes no scheduling, authority, Native
controller or await boundary. Typed predecessor totals are wall-await time,
not exclusive ACK CPU; terminal windows without a next reply are censored.
Freeze/reverse before one unchanged mixed UP, preserving ordinary binaries.

**Capture22:58:** warning-free1m23s diagnostic build is frozen as
response-return-service-20260908. All19 source hooks are reversed, source diff
is empty and the archived patch applies cleanly. One unchanged mixed combined
upload is running; no build overlaps it. The ordinary model remains0449b9f.

**Next existing stall boundary22:37 — diagnostic, not another fix:** the
current ordinary UP has Rc966 held across41.189–45.189s while T advances65MB
and Rs966→1190. Earlier positively decoded replies also spent1.112s locally.
Response late placement cannot be called their cure; the three measured
owner sections (max13.405ms) already reject a single multi-second lock hold.
The meaningful discriminator is exact current response source/claim/write/
decode/handoff/mux/local-delivery, with typed preceding reader work instead
of assuming every nondata predecessor is ACK processing.

Reuse the archived reply observer, adapting Original commitment to the new
prepared success and source reads to their actual Product enqueue sites.
Retain exact response-only path/stream/range identities and successful-write
semantics. Aggregate preceding QUIC-reader handoff counts and sum/max intervals
by Frame kind; do not log each request ACK or revive the three owner timers.
One unchanged harsh mixed upload on diagnostic0449b9f asks whether current
winning replies wait unclaimed, before read starts, during read, or after
decode. Native missing-input versus local polling remains unresolved if only
a long read-await is observed; do not add Quinn/Cargo hooks automatically.

Information forecast: a material current postdecode/late-read-entry hold with
typed predecessor waits selects that existing service owner for a bounded
counterexample; prompt local stages reject that attribution and retain the
predecode branch. The benefit is choosing the correct seconds-scale mechanism,
not improved diagnostic Mbps. Root/independent review before one build;
archive/freeze/reverse every observer hook before capture. No runtime policy,
queue, timing, sampler, controller, model or ordinary comparison substitution.

**Capacity-drop outcome22:34:** all five QoS-only cells complete. Final10s
means: control mixed405.506, candidate QUIC439.528/mixed451.067, raw455.680,
H2470.396Mbps. No persistent bulk collapse after500Mbps restoration in these
cells; no equivalent proof for every QoS/loss combination. All systems,
including raw/H2, time out one echo at the10Mbps cut. Later unavailable slots
follow the probe closing that same connection, not fresh postrestore attempts.
Do not turn this common timeout into an MPP-specific fix.

Mixed candidate's2.833589s maximum bulk gap is during the cut (control.936306,
QUIC.106309). Interior matched snapshots send9,881,946 physical class bytes
while locally delivering3,473,408 logical bytes; backlog stays38.86→32.08MB.
The cut is busy: neither unused capacity nor exact duplicate/copy fraction
follows. Pending lower-prefix/suffix/native service needs exact-range evidence;
do not respond by reducing queues or guessing a protocol preference. After
initial-loss restoration the candidate mixed remains slower than its control;
the later QoS recovery win does not erase that earlier adverse case.

Telegram full checkpoint/recovery report sent14:34UTC; next nonurgent not
before15:35UTC. The report explicitly withholds performance acceptance.

**Checkpoint / restored result22:22:**0449b9f commits the verified response
ownership migration and all ten ordinary outcomes, explicitly not accepting
performance. The70-member archive passes integrity/member/byte comparisons.
Existing reorder-recovery cells now complete for control mixed, candidate
mixed/QUIC and H2. All echoes succeed. Final20s means: mixed control421.332,
mixed candidate392.256, QUIC438.824Mbps; H2 is approximately467Mbps. The
old control already recovers without restart; no persistent severe post-loss
collapse is reproduced here. Candidate mixed retains adverse tail throughput
and echo p95 versus control, despite higher whole-run bulk. Source remains
0449b9f, no new fix or controller/queue change.

**Existing QoS symptom / next ablation forecast:** initial loss recovery is not
the reported capacity-collapse recovery. Reuse combined with loss/jitter and
blackhole disabled:500Mbps/100ms, shared forward10Mbps at15–25s, then500Mbps
to40s. One control mixed, candidate QUIC/mixed, raw TCP and H2 comparison
with identical capacity schedule, existing configured priors and full timings.
This isolates queue/congestion-history recovery from continued erasure, using
the existing runner. The10Mbps interval is diagnostic; regained500Mbps service
and loaded-latency history decide the question. No arbitrary rate threshold
or wait for a favorable run. If MPP remains stalled after the cut clears while
baselines recover, locate exact forward/return/native authority next; otherwise
do not invent that defect. Keep the earlier adverse evidence and remaining
upload receive boundary open. No new lab infrastructure or runtime edits.

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

- Root owns builds/labs. Response claim comparisons and the subsequent MAX-fold
  mechanism/ordinary UP pair are complete; all adverse outcomes remain visible.
  The current replacement candidate is shared latest-credit input state; its
  predecessor is fcc0b22's finite ready-MAX fold. The completed healthy observer
  selected this bounded transaction; do not repeat that capture without cause.
  Owned Docker only: no sudo, host shaping, outside-repo work or build/lab overlap.
- Ordinary candidate:`./.tmp/reflection/bin/ready-credit-20260908/mptunnel`.
  Ordinary comparator:`./.tmp/reflection/bin/response-claim-20260908/mptunnel`.
  Earlier comparator:`./.tmp/reflection/bin/ack-support-20260908/mptunnel`.
  Current diagnostic:`./.tmp/reflection/bin/ready-credit-return-20260908/mptunnel`.
  Previous forward diagnostic:`./.tmp/reflection/bin/post-ack-forward-20260908/mptunnel`.
  Previous reply diagnostic:`./.tmp/reflection/bin/reply-residence-20260908/mptunnel`.
  Do not use diagnostic target/release as an ordinary comparator.
- Exact intermediate commits only; preserve raw evidence before scoped cleanup.
  No deletion in this condensation. User's seven-line
  LIVE_OWNER_FRONTIER_WORK_BOUND.md edit must remain untouched and unstaged.
- Telegram last attribution/comparison report:16:39UTC; next nonurgent not
  before17:40UTC. Respect hourly minimum/soft-frequency advice; no component-only
  success notification or unfinished completion claim.
- Method reflection: symbolic conservation justified exact work removal but
  did not predict every timing phase. Follow the same winning-prefix evidence,
  retaining adverse first-service, settlement and resource observations.
  No plausible shortcut stack, favorable-average rerun or silent coverage waiver.
- Universal clairvoyant optimum under arbitrary future outages is impossible;
  this does not waive avoidable delay or practical gates. Never call unfinished
  work ideal or promise cost-free capacity discovery.
