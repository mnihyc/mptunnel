# Current deterministic closure plan

Updated:2026-09-10 14:38 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before every
transaction and after compaction. This is the active scope/decision ledger,
not a new issue inventory. Full pre-condensation chronology is retained at
`git show 13876d6:docs-dev/CURRENT_CLOSURE_PLAN.md`; the subsequent observer18:56
entry is preserved below. Earlier history, including rejected approaches,
remains at `git show ebad57f:docs-dev/CURRENT_CLOSURE_PLAN.md`. Linked reports
retain exact ranges, raw timing bins, costs, RED/GREEN logs and observer patches.

## Active transaction: user-requested mixed-mode architectural redesign

### Selected next model gate — confirmed return service with baseline fallback

14:38 +08 native-refill's four ordinary UP cells settle every accepted byte.
TCP control/candidate: 422.329/449.668 Mbps, first confirmation .408636/.408035s,
maximum confirmation gap .514412/.617220s. Mixed: 414.049/410.817 Mbps, first
confirmation .409412/.414631s, maximum gap .465272/.694198s. Preserve the worse
confirmation tails: more bulk work is not a latency pass. Mixed return bytes
rise 22.75%, peak shared UP backlog 18.50→33.98MB and client RSS/CPU rise.
All eight cells, 328 bins, 280 DOWN echoes, four exact settlements and 332
profile rows are independently audited in NATIVE_REFILL_ORDINARY_20260910.

Source review finds no new admission/priority violation. ACK/control/repair
bypass only the Original preclaim check, NOT the socket low-water policy;
they retain exact ownership and the same partial protected transaction. Do not
gate them as a compensating tweak. Changed native competition and repair versus
Original service remain competing causes, not proven by native byte shares.

NEXT information-only noise discriminator: ONE reverse-order mixed healthy
DOWN pair, candidate then ba56290 control, identical frozen binaries and
500/500Mbps, DOWN30/UP70ms, no impairments, normal management/probe, 40 seconds.
The earlier all-phase -5.6% result and historical 389–417Mbps control range
justify testing execution-order dependence before attributing harm to policy.
Keep both pairs and all timings/costs. Repeated sustained harm retains an
explicit throughput/latency tradeoff and stops default promotion; reversed
ordering means causality is unresolved, not a non-regression waiver. No third
favorable-repeat, observer, LOWAT tuning or new issue inventory. The large
TCP latency correction remains an isolated trial; the next decision must
continue the existing mixed/recovery closure task, not end at this checkpoint.

14:27 +08 mixedDOWN ordinary result is NOT non-regression:417.539→394.076Mbps
(-5.6%),firstbody.579448→.587630s, maxgap.345924→.370100s. Echo77/80all succeed,
median289.487→270.309ms,p95678.431→410.124ms,max1174.209→532.697ms. Keep mean
harm andbettertails; currenttrial remainsunpromoted. HistoricalsamehealthyMPP
controls389–417Mbps also showrealization/historyvariation, not evidence to
waivethispair or assigncausalharmfromoneaverage. Reportingwillinspectactual
carrierbytes/phases/cost, not cachedrate. CompletealreadydeclaredTCP/mixedUP
control/candidate classification for symmetry/settlement BEFOREdisposition;
no retune, newmodel or favorable-repeat. This is notcompletingglobalacceptance.

TCPcost also has a real tradeoff: nativeRTTmedian216.8→296.3ms and sharedDOWN
backlogmedian9.05→14.07MB despite greatlybettertunnelecho. Medianunsent48.20MB
falls.285MB, while nativeflightproxy12.78→17.62MB remainsindependent. Lifetime
CPU/RSSdecrease, but no blanketnetworklatencyclaim; bypasslatency isnotmeasured
in thispair. Preserve sharedqueuecost in finalpracticaldecision.

14:24 +08 first ordinaryTCPDOWN pair supports the forecast:417.249→440.164Mbps,
firstbody.586752→.583288s, maxreadgap.402695→.321440s; echo43/80all succeed,
median937.902→299.562ms,p951269.087→322.754ms,max1328.312→507.695ms. Same500/500
healthy40sprofile, nofeaturediagnostics, controlba56290 thennative-refilltrial.
NoLchange. Fullbins/nativequeue/flight/CPU/RSS are under independentreporting;
these stronguser-visible improvements justify the declared next mixedDOWN
control/candidate pair, nowrunning. Notglobalacceptance or an optimalityclaim.
Native/sharedpath residual~300ms mustremainvisible; do noterase it withbulkMbps.

14:19 +08 bounded refill model controls complete. Actualserverwriter permission
RED assigns65,536B despite explicitunavailablepermission, failingat intended
next_offset0 assertion (69scompile). This is a caller-permission model test,
not fabricated native telemetry; exactFIFOcapture proves practicalreachability,
and5realadapter checks separately prove actualnegative/wake/cancel/terminal,
exactoption scope and unchangedSO_SNDBUF. GREEN all81TCPchecks and35prepared
checks pass;55.89s warning-free testcompile. Existingpartialwrite/class/source
and lifecycle controls remain. Independent client/server/adapter review finds
no reachablecounterexample. POLLERR/fullHUP wake witherror, not newOriginal
permission; peerwrite-halfclose is not classified asfullHUP. NoSO_ERRORconsume.

Currentpolicy remains trial/unpromoted. Ordinaryreleasebuild started; compare
frozenba56290control thennewcandidateTCP healthyDOWN40s, unchanged500/500Mbps,
DOWN30/UP70,noimpairments, normalmanagement/probe only (nofeature diagnostics).
SameL=131,072 nativebytes frozen throughout; no tuning afterresults. If useful
service holds whileechoimproves, next affected cells are mixedDOWN and TCP/mixed
UP, retainingcompleteconfirmation/timing/cost. Any practicalharm mustbe kept
and classified beforepromotion. Recordnativeflight andNOTSENT separately;
smallnativeunsent alone isnot proof of sustainedperformance. NoREADME/release.

13:58 +08 bounded native-refill prototype selected after exact causal proof;
observer/report/archive committed e722c46, no production policy changed yet.
The trial is native unsent backpressure AND preclaim readiness, not LOWAT alone.
Use one exact socket writable owner per physical TCP carrier. Both outer
publication and immediate Original claim require fresh native eligibility;
blocked work stays a weak notice, using existing writer_change_wait/deferred
machinery. One actor-native wake republishes the existing physical readiness
boundary and re-enters normal class arbitration. No per-stream socket waiter,
new heartbeat/poll timer, own cwnd/pacer, SO_SNDBUF or Product-ACK wait.

Trial policy is L=2q native bytes, q=the existing maximum64KiB service quantum.
Factor2 follows Linux's below-half-low-water write-space wake. The native-byte
refill reserve is approximately q (1.049ms at500Mbps), NOT an exact complete
protected-frame reservation: current64KiB NoiseDATA occupies65,602wire bytes.
Framing/kernel packetization and the one retained partial record can overshoot;
codec-valid larger records remain legal. No protocol-family preference or
resource-limit shrink is introduced. This is a disclosed policy trial, not a
symbolic guarantee of throughput under arbitrary CPU delays. Nativeflight may
remain6.25MB at500Mbps/100ms or larger; L constrains unsent admission, not flight.
Unsupported native capability retains the existing structural-only contract
explicitly; never report unknown as observed zero/ready capacity.

Forecast: remove much of the demonstrated0.5–1.35s unnecessary unsent residence,
not TCP propagation/retransmission/receiver delay; no promised mean-speed gain.
Cost risk: extra native wakes/syscalls and refill starvation on busy/high-rate
hosts. Falsifier: meaningful healthy goodput/startup/CPU or recovery regression,
lost wake, claimed blocked source, partialwrite replay or stalled cleanup.
Build targeted real native-wake and actual writer preclaim controls first;
retain RED against ungated claim when reachable in existing fixtures. Then ONE
ordinary control/candidate TCP healthyDOWN pair, followed by affected mixed/
UP and existing impairment gates only if supported. Preserve every bin/gap/
echo/settlement/cost; do not enlarge L until a favorable number appears.

13:50 +08 exact native-FIFO discriminator succeeds. Feature TCP healthy DOWN
419.462Mbps,46/46echoes,median893.015/p951253.589/max1497.461ms; not performance
promotion. Worst echo index38/range[2432,2496) first authenticates on path2.
The native initial-send frontier is still below its exact protected interval
end600083448 at1352.786ms after acceptance, crosses by1354.336ms, authenticates
70ms later. The conservative unsent lower bound alone is90.34%of the same
request's elapsed, not aggregateQ/C or a losing-copy attribution. Client event
is pre-mailbox authentication, not an asserted Product winner. Reporter retains
all46requests and75tracked copies; only35first-authcopies have tracked intervals.

Root cause at this boundary: one-frame MPP priority arbitration remains correct,
but repeated successful writes transfer bulk into a shared nonpreemptive native
FIFO. Native socket memory acceptance is not prompt transmission eligibility.
Prior prepared-source/one-frame fixes addressed MPP ownership, not this native
handoff depth; do not remove their exact readiness/partialwrite invariants.
Next bounded model decision is native-backed refill admission BEFORE Original
claim, with actual capacity wakes and unchanged native congestion flight. A
socket option alone can merely move HOL into the one already-claimed write.
Reserve must cover real wake/refill service; Linux wakes below HALF its unsent
low-water threshold. No numeric threshold or production candidate selected yet.
Expected removable delay is the demonstrated~1.35s unsent part of this request,
not all TCP RTT/retransmission/shared-cut delay or a guaranteed Mbps gain.
One coherent prototype and affected ordinary controls must falsify throughput,
CPU, wake/lifecycle and fast-alternative regressions before any promotion.

13:45 +08 TCP observer preparation: independent counter audit validates signed
W−Q for the exact Noise writer/socket lifetime, including presplit handshake
debt. TLS is excluded because read-side protocol writes break exclusive writer
accounting. Existing native observation turns only; no new polling. Retain the
last below-end syscall bracket: a late first crossing alone is only an upper
bound and cannot prove late transmission if actor service was delayed.
Explicit diagnostic stream selector MPTUNNEL_LAB_STREAM_ID=0 has no default;
the existing workload waits for its first successful echo before opening bulk.
One nonempty≤64B, single-frame protected interval per physical carrier may be
pending; never replace an unfinished interval. Client observation is immediately
after authenticated decode, before the reader's bounded actor queue. This is
feature-only attribution, not a new production rate/queue/lifecycle model.
Next remains the ONE declared TCP healthy DOWN capture; retain any selection,
sampling or nonrecurrence limitation rather than infer a policy from it.

13:34 +08 disposition: retain ba56290's demonstrated receipt-liveness
correction, NOT performance acceptance or the original5sincident's cause.
Sharedrequestsourcebudget stays charged until DataACK release; withheld valid
receipt can block another stream at that budget and retain redundant recovery
eligibility. Seven existing release/wake/ACK/copy checks also pass. Requestlocal
explicitgap requalification is not globalcarrierSuspect; do not invent that
benefit. OrdinaryDOWN390.285→399.803Mbps/bodygap.372→.342s, butmaxecho806→1046ms,
p95612→616ms. UPgap.408→.605s remains. No favorable-repeat or compensatingtweak.
Full164bins/155echoes/costs and23-filearchive are retained in
TARGET_WRITE_RECEIPT_ORDINARY_20260910. No further modelchange selected here.

NEXT: existing TCP-only DOWN loaded-service owner, not a new inventory. Its
repeatable~900ms echo versus~206ms direct/~213ms native RTT and~50MB aggregate
NOTSENT motivates a precise kernel-FIFO discriminator. AggregateQ/C≈.8s is an
information forecast, NOT measured per-echo residence or a justified queuecap.
Extend feature-only observation of the existing serialized TCP writer: exact
protected bytes accepted W plus same-socket NOTSENT Q gives native initial-send
frontier W−Q relative to the SAME writer baseline. Track a selected echo's exact
protected interval and its first observed frontier crossing, then compare with
client authenticated delivery/probe response. Must validate counterbaseline,
incarnation, control/TLS/handshake writes and unknownQ; no fallbacks tozero,
raw payload capture or per-bulk-frame log. Reuse existing native observation
turns and bounded one-in-flight diagnostic sample percarrier; no new timer,
queue limit, nativecontroller or scheduling intervention.

One feature capture on currentba56290 TCP-only healthyDOWN40s, existingsame
500/500/30–70/noimpairment bulk+echo, will decide whether large nativeunsent
residence actually contains the materialecho interval. Promptfrontiercrossing
withlateclientreceipt falsifies that stage; latecrossing supports the native
handoff owner but still does not choose aLOWATvalue. Too-smallnativequeue can
starvewake/refill service/highBDP and consumeCPU, so no numericpolicy without
that model and ordinaryproof. Featurecapture is causalnotbenchmarkevidence.
Original5sUPhold, fixed-roundoutagetradeoff and globalgates remainopen.

13:22 +08 pending-receipt UPpair complete: exactcontrol2,070,937,600B in
40.750652s406.558Mbps versuscandidate2,192,441,344B in42.042557s417.185Mbps.
Firstconfirmation.410337/.411884s essentiallysame; worstgap.408449→.605196s
adverse, localwritegap.533130→.515499s. Moreacceptedbytes and+2.6%mean cannot
erase worseconfirmationtail/longersettlement. No promotion. Complete the already
declared DOWNpair for affectedclassification before disposition, not a new
intervention or repeat to waiveUPtail. Currentruntimecandidateba56290 remains
isolated; original5sincident stillunattributed. Ordinarybuild64s emitted the
pre-existing test-only-wrapper dead_code warning inrelay/io.rs, not a new
platform/model regression. No runtimechange followed the measurements.

13:17 +08 bounded receipt correction passes all93server checks, including
the actualfirst+establishedreceipt test with fullACKqueue/capacity-onlywake,
unchangedMAX and partialwrite-once. Existingretainedrouteexpiry/terminal/repair
controls pass. Independentdiff review finds no concretecredit/wake/cancel
counterexample. Removed oneunusedimport warning; no other change. Runtime
remains unpromoted pending ordinarycost/timing, not the5shold's proved cure.

Next fixed comparison: build one ordinarycandidate and retain364d417control;
CONTROL thenCANDIDATE healthy mixedUP, then mixedDOWN, each40s500/500Mbps,
DOWN30/UP70, zerojitter/loss/QoS/outage, existingprobe/managementsampling and
sameunchangedtarget. No extrasocketobserver inperformancepair. Forecast is
receipt-liveness correctness, not a guaranteed bulk gain. Keep firstresponse,
allbins/echoes/maxgaps/exactUPsettlement/classbytes/CPU/RSS and adversephases.
Material ordinaryharm stops promotion; do not add cadence/timeout tweaks or
attribute any fasterstartup to the originaluncaptured5scause. Broaderreturncut,
outage/TCPservice/randomlinks/experience gates stillremain after thispair.

13:12 +08 exact contract RED: the real server DATA actor admits[0,2), a real
duplex target accepts one byte then returns Pending, output credit/capacity are
valid, yet no StreamAck is offered. The focused test fails at that intended
assertion (not setup), log target-write-receipt-red-0910.log;64s build.

Bounded correction forecast: at the FIRST actual Pending of this one retained
DATA write/flush, force one ACK-only materialization of the already admitted
receipt. Keep the same pinned write, current actual MAX grant, exact output
fences/retries/return-route deadlines and normal postwrite credit publication.
If it is immediately Ready, preserve the current postwrite cadence entirely.
Further Pending wakes retry the same generation, not rematerialize it. This
implements existingRFC8.3/8.4; no new threshold, native controller, timing policy
or RFC redesign. Established subthreshold receipt must also be offered, so the
existing force=false predicate is insufficient at the park boundary.

History/tradeoff: postwrite receipt ordering predates364d417; the earlier broad
8a0413d cadence trial was fully reverted5d2af0c after adverse timing. Its global
cadence/timer changes stay rejected. This narrowly restores a required actual
park boundary. Expected benefit is timely receipt/owner release under target
backpressure, not an asserted Mbps gain or proof of the episodic5s cause.
Immediately-ready service has no predicted speed gain. Actual pending writes,
including cooperative pending, can produce more/earlier feedback, so ordinary
UP/DOWN timing/cost controls remain mandatory and may reject promotion. Preserve
partialwrite-once, no premature credit and immediateReady/nonfirst-receipt
controls. Do not fix unrelated duplex target-read scheduling in this patch.

13:08 +08 target observation outcome: no recurrence of the5.14s hold. Exact
2,133,524,480B settle in41.651823s; maxconfirmation/writegap.383092/.403622s.
All41 sink socket samples have zero receive/send queues, receipts/replies
advance, and sink majorfaultdelta is0 with~6.26%one-core CPU over the capture.
The same sinkPID remains. Source1080backpressure is sustained but progresses.
No evidence here attributes the original episodic stall; keep it unresolved,
not fixed by the409.783Mbps average. Sequential sample timing cannot localize
subsecond pauses. The sink retains unused lifetime receive tuples even without
a progress output; that is a concrete observer-resource defect, not proof of
GC/paging as this event's cause or an MPP runtime leak. No sink change yet.

The actual-DATA blocked-target test is compiling under the current runtime.
This is a necessary receipt-contract check, not a throughput optimization;
do not promote it as the cure for the original5s. Native/deadline parameters
stay unchanged. Healthy panel/TCP companion are committed4ef9f50. Telegram
balanced comparison sent05:08UTC; next nonurgent update not before06:09UTC.

13:06 +08 completed healthy panel: all MPP UP cells settle exactly, but mixed
has a5.140055s positive-confirmation gap (TCP.477220s, QUIC.246492s), with four
zero one-second bins during startup. Final371Mbps does not excuse this hold.
At samples4–6 target socket acceptance stops57,046,163B and target replies
already read remain70B. Carrier TCP live queues are empty by then, QUIC fresh
ACK progress continues slowly and both router queues drain. These observations
do not establish the missing byte or distinguish Product recovery from target
application/relay I/O. No congestion or deadline parameter change is justified.

Next ONE unchanged ordinary mixed healthy UP40s observation, same500/500,
DOWN30/UP70ms/no impairments. Add read-only target10023 loopback socket state,
client1080 source socket state, exact sink PID25 stat/status, and the probe's
existing started-file clock anchor. Listener identity was verified as python3
/workspace/lab/tcp_sink.py without progress-file options. Do not restart it or
change source/model/load. Information forecast: sink Recv-Q/nonreading versus
MPP target Recv-Q/unread replies versus empty target queues/absent ordered input
selects the corresponding owner. Full timing and resource evidence remain;
nonrecurrence cannot clear the original hold. Do not add finer instrumentation
until this materially narrows the question. This is not a favorable rerun.

Independent source audit also finds new receipt materialization occurs AFTER
the retained target write, whereas its pending helper services only previously
materialized feedback. RFC8.3/8.4 require receipt independent of consumption.
One actual-DATA/blocked-target focused RED will test that exact contract, not
restore the rejected ACK cadence or claim it caused the5s event. Generic duplex
write/read cycles are reachable, but the short-ACK sink has no proved cycle.

The separate TCP direct-echo companion completed with limited early overlap:
its launch preceded foreground by36s, not the intended short margin. Preserve
that limitation, not a full40s loaded-control claim. Conservative overlapping
bands show direct206ms, native197–219ms and MPP870ms; foreground417.667Mbps,
45/45 tunnel echoes and100/100 direct echoes succeed. This supports an extra
carrier-local service component but not a specific queue position/threshold.
The12-cell report and archives retain Xray/H2 UP terminal-ACK lower bounds.

12:45 +08 healthy panel finds a larger existing carrier-service knot. DOWN
TCP418.339Mbps has sustained median/p95echo912/1276ms; Q426.231Mbps105/160ms;
mixed389.236Mbps267/429ms; raw446.488Mbps103/124ms; Xray449.000Mbps103/128ms;
H2465.296Mbps111/114ms. All DOWN echoes succeed. TCP server native RTT median
213ms and router backlog median9.12MB closely match the earlier three-raw-body
control214ms/8.82MB. TCP MPP has aggregate native NOTSENT median50.30MB. Raw3
has even more72.40MB, but its independent echo socket remains214ms; native
unsent bytes are not inflight or a new memory leak. Exact echo queue position
is not observed, so these facts select a discriminator, not a queue threshold.

Next after the running UP settlement panel: ONE unchanged TCP-only healthy
40s cell with the existing50s direct_echo_context.py companion. It uses the
same remote echo service/cut/host but bypasses MPP's shared TCP byte stream.
Question: does direct echo inherit~0.9s latency, or stay near native/common-cut
~0.2s while tunnel echo remains slow? Preserve companion anchors/unloaded
margins, conservatively aligned loaded interior, all attempts and full existing
workload/costs. Ordinary binary has no exact request-event anchor; do not reuse
old feature-capture millisecond alignment. No new helper/source/build/native
queue/controller/limit change. If same-cell separation persists, the existing
carrier-local ordered handoff owner has material support for targeted modeling;
similar delays retain shared-cut/host attribution. No TCP preference, socket
buffer shrink or NOTSENT parameter is authorized by aggregates alone.

UP baseline caveat: raw settles exactly. Xray again closes before terminal sink
acknowledgment (2,286,658,677confirmed versus2,290,548,736accepted), matching the
already documented HIGH_CAPACITY_REFERENCE half-close limitation. Preserve its
partial/lower-bound result; do not call439Mbps completed speed. This known
non-MPP comparator boundary is not a new runtime defect or permission to change
the probe. Continue the already running panel's MPP settlement checks; any new
MPP integrity/settlement failure stops promotion and selects its exact owner.

12:33 +08 reverse-order outcome: candidate376.650Mbps versuscontrol358.091;
candidate maxbodygap.947738s versus.647854s and echo p95705.587versus650.642ms
repeat the adverse ordering. Worst echo827.366versus1372.285ms and median
312versus326ms improve;77/76echoes all succeed. Do not choose the favorable
metrics or call the pair non-regression. Both ordinary pairs remain; no third
repeat. Trial promotion stays held. Neither the same mean nor the observer's
already-active fallback attributes the repeated~.3s extra worst read gap.

Next information-only gate is the existing current-binary all-mode healthy
panel, NOT promotion or a new fix stack: TCP,QUIC,mixed,raw,Xray,H2; both
directions; shared500/500Mbps, fixedDOWN30/UP70ms (mirror1 for both directions),
40s existing probes, no randomloss/jitter/QoS/blackhole. Reuse the existing
runner's combined mode with all impairments disabled, not25s versus40s
comparison. Old records are context; rerun the six-mode panel once for a
current matched cohort. This broader existing gate classifies practical
shortfall/settlement before another timer/native observer or small isolated
recovery optimization. No source/build changes. All echo attempts, exact UP
confirmation and drain, first service, full bins and costs remain. Stop this
panel at a critical failed completion to attribute its existing owner; no
favorable repetition, profile change or automatic redesign. Less severe
multidimensional tradeoffs remain documented for the final practical decision.
Fresh healthy baselines do not erase outage exposure/failures above or satisfy
loss/aggregation/Cloudflare/global gates. Candidate is still unpromoted.

12:31 +08 fixed baseline classifier completes. QUIC-only pre-outage444.813Mbps
versusH2471.796; restored35–39s roughly454versus471Mbps. Body service resumes
at34.456s versus33.836s after nominal33s restoration; differing runner/probe
clocks and~106ms differing sampled restoration preclude subtracting619ms as
pure native overhead. Qproducer timestamps advance while ACK totals stall;
its restoration is delayed but sustained high service returns without restart.
Both sole-carrier echo probes have one3s timeout followed by13unavailable slots,
so neither provides restored echo latency. Raw is unexposed,453.829Mbps and
80/80echo, maxbodygap.100s. Mixed's73/73echo success is a real resilience benefit,
alongside its later1.098s gap. Full baseline histories have a13-file raw archive.
No precise post-admission code defect was found in independent TCP source
review: ordinary priority precedes newbulk/repair at each arbitration, while
already-written bytes and one retained native write are nonpreemptive.

Next bounded noise decision, not another tuning attempt: one predeclared
reverse-order ordinary outage pair CANDIDATE364d417 then CONTROL0cab2b5, identical
40s profile/workload. The original adverse ordinary pair, lack of recurrence
in the observer and similar native delay across models establish timing-history
ambiguity; diagnostic instrumentation cannot serve as its repeat. Information
forecast: determine whether the adverse recovery ordering repeats under reversed
execution order before selecting a new model owner. Retain BOTH pairs and all
failures/tails, not a best-of result. At most this one declared pair; no more
favorable-repeat search. Repeated material harm keeps the fixed-round trial
unpromoted and requires causal attribution. Reversed/overlapping outcome means
causal harm is not established, NOT proof of universal non-regression; retain
the supported return-cut correction provisionally and advance the existing
current-binary all-mode/high-capacity gates. Do not retune native recovery for
the modest single-Q gap difference ahead of mixed service/unfinished settlement.

Next predeclared transaction: reuse the existing ordinary mixed outage pair;
run frozen364d417 QUIC-only, existing Hysteria2, then raw TCP in the exact same
40s outage-only DOWN case (500/500Mbps,30/70ms delay, no randomloss/jitter/QoS,
UDP30–33s). No build, new instrumentation or runtime change. Information
forecast: classify the material post-admission/restoration delay before another
MPP-only observer. All series, echoes including failures, first service,
restored intervals and cost remain. H2 retains its explicit500/500Mbps priors;
MPP remains unconfigured discovery. Raw TCP is an unexposed negative control,
NOT an equal-failure competitor. Single Q/H2 lose their only carrier, unlike
mixed; retain the unchanged3s echo guard and any censoring, not an artificial
max-gap competition across unequal failure exposure.

Decision: delayed Q-only recovery with prompt H2 selects the existing single-
carrier native/post-admission boundary. Prompt single-carrier restoration with
poor mixed selects its existing allocation/recovery/shared-queue owner. Similar
baseline delays weaken MPP-specific attribution, not prove inevitability.
Independent review supports this discriminator over another observer build.
Stop this fixed comparison when complete and make its causal decision; do not
repeat until favorable or conclude the authorized global task at that point.

12:24 +08 observer outcome: no source change. The ordinary1.098s gap does not
recur; diagnostic maximum is.475828s near restoration. Exact body/DSN alignment
identifies a TCP-owned missing prefix. Full fanout was already active1.105s
before the hold; sender admits its exact14,600B repair70ms after the preceding
frontier, and that carrier releases it406ms later. This rules out delayed
feedback fallback for THIS observed gap, not the earlier ordinary gap. Echo's
worst1,686ms exchange also has prompt proof/credit processing, followed by
1,616ms from server reply queue admission to client logical receipt. Both
echo outputs are TCP; whole-QUIC outage does not test its selected return loss.
No precise writer/native/network/actor split or avoidable timer hold is proved.

The independent bulk/echo report RETURN_ROUND_OUTAGE_OBSERVER_20260910 retains
the exact identities, accepted-versus-queued distinction and evidence limits.
Root created/listed its six-file raw archive including64s feature build. This
32.85MB log capture is causal observation, never an ordinary speed control.
The ordinary DOWN/UP/outage reports and37-file archive preserve all adverse
phases and successful exact UP settlement; promotion stays held. Next decision
is bounded source-boundary review versus same-profile native baseline/mode
classification, not another timer or new issue inventory. A slow admitted
copy alone is not proof of an avoidable MPP hold or of physical inevitability.

12:04 +08 outage result stops promotion: whole384.128→383.408Mbps is nearly
unchanged, but maxbodygap.777480→1.097872s (36.845–37.622→36.937–38.035s),
echo p95697.271→881.787ms and max1534.222→1641.343ms worsen.76→73echo successes,
no failures/restarts. Restored body375.064→425.334Mbps cannot erase the gaps.
Both server QUIC ACK-byte counters stay flat across sampled31–35s despite
UDP restoration near33.1s; native progress resumes36s. This is not evidence
that the return-proof policy alone caused the adverse user interval.

Next information-only discriminator on unchanged364d417: one feature build,
same outage-only DOWN cell, existing filtered events
feedback_return,receive_hole_reinjection_signal,receive_hole_release,
server_data_ack_recovery,server_repair_carrier_accept,
server_stale_output_recovery,server_response_recovery_wake. No new observer,
DATA payload/per-ACK dump or harness. Omit receive_hole: its byte-changing key
logs near each buffered frame. Existing release events can still be numerous;
use them only to reconstruct the exact ordered frontier, not as speed evidence.

Competing causes are delayed proof/fanout activation versus native/retained
DATA-prefix recovery, or application service after a released prefix. Join
actual proof transitions, persistent-hole timer, exact release frontier and
accepted repair extent, retaining queued-versus-accepted distinction. These
events cannot prove copy winner or physical queue position; receiver events
require verification of a single session/stable incarnation mapping. The
capture must pick a material owner or leave attribution unresolved, not blame
a scalar rate/Active label or adjust any timer. Ordinary tradeoffs stay held;
one diagnosed candidate stop does not end the authorized task/global gates.

11:55 +08 ordinary round-trial outcomes: DOWN whole333.025→388.945Mbps;
restricted202.095→384.810, maxreadgap.574399→.322268s, echo p95602.299→422.633ms,
79→80successes, no failures. Return class bytes fall24.46%, actualdrops0 both.
Preserve adverse whole maxecho701.524→726.535ms and restored echo p95
347.933→400.385ms; healthy/restored body means improve. UP completes exactly
397.631→409.247Mbps; restricted340.206→416.408, maxconfirmation/write gaps
.483374/.443350→.473387/.411651s. UP healthy437.461→411.338 and restored
429.498→412.126Mbps are adverse phases, not omitted. Candidate is materially
supported for return-cut service, not accepted across failure/competition.

Next fixed comparison: same ordinary CONTROL0cab2b5 then CANDIDATE364d417,
existing mixed combined DOWN,500/500,30/70ms, no configured randomloss/jitter/
QoS; whole QUIC outage30–33s,40s bulk+echo. The new model deliberately permits
one additional proof interval, so retained same-request recovery and worst
gaps/echo/settlement are the question. No expected throughput gain is promised.
Stop promotion on material recovery harm and trace its owner, not a smaller
timer. This is whole-QUIC failure coverage; without actual selected identity
it does not alone prove loss of the selected feedback output. That narrower
coverage remains explicit, not waived. No broader new inventory or harness.

11:46 +08 route-round trial: the actual policy test failed at its intended
second100ms receipt assertion (six old controls passed), then passes after
removing successor state.52feedback checks and the affected stream/client/
server groups pass (290/32/92, overlapping); the real client control also
asserts a next-round wake immediately after receipt with newer MAX, before
another event/retry. Independent model and source audits find no remaining
counterexample in this bounded contract. Active rounds never renew on facts,
RTT or retries; one delayed pre-failure receipt cannot renew its failed child.
RFC now explicitly states the changed route-liveness bound. No native interval
or unrelated behavior changes. Source change is unaccepted pending ordinary
timing; the two historical successor tests deliberately track the new contract,
not a claim the former implementation violated its old specification.

Next exact ordinary transaction: frozen0cab2b5 CONTROL then this new route-round
CANDIDATE, existing mixed combined DOWN return restriction, followed by its UP
mirror.500Mbps data; reverse500→10→500 at15–25s,100ms total delay, zero configured
jitter/loss/outage; identical existing duration/workload/cost collection.
No build/lab overlap or diagnostic feature. Preserve whole and phase series,
all echo attempts/loaded latency, real confirmation/settlement, wire/RSS and
gaps. This tests composed benefit, not the already established counterexample.
Adverse critical service stops promotion and selects attribution; no timer,
cadence/profile rescue. Full failure/competition/global gates remain below.

Observer reports are CONFIRMED_RETURN_OBSERVER_20260910 and
CONFIRMED_RETURN_OBSERVER_UPLOAD_20260910. Both five-file captures and the67s
feature-build log are archived/listed in the matching DOWN-named raw archive
(11files). Observer DOWN additionally has3,367actual UP queue drops; do not
compare its throughput as an ordinary fix result or attribute every delay to
the successor rule. No raw packet/physical-queue culprit was established.

11:36 +08 diagnosis result and bounded model question: both captures complete.
DOWN stream1 actually loses selection with prompt101/100ms proof exchanges:
token7's successor anchor precedes receipt7, so token11 is created with only
79.792ms remaining and expires. UP has49valid receipts and44selection losses;
all44losses are inherited-successor expiries. Actual selected residence is
about6% of the UP route window. This is a real performance-policy consequence,
not a byte/credit correctness violation. Every observed probe reaches its real
logical owner and obtains reply admission; per-frame/actor starvation is not
the explanation for those exact events.

Keep the second cause separate:427UP discovery attempts also expire with fresh
deadlines, median811ms round trip versus295ms budget; median618ms lies AFTER
reply admission and before the local receipt. Removing successor inheritance
cannot claim to fix that loaded service.125ignored receipts are merely obsolete
discovery tokens, not expiry failures. Feature observer is checkpoint f2481d0;
ordinary0cab2b5 is unchanged. Reports preserve exact joins and timing limits.

Before runtime changes, evaluate deleting per-fact successor deadlines rather
than extending native intervals. Treat confirmation as route liveness (its
actual authority), not per-fact delivery credit. Each admitted probe remains
one fixed-deadline round, with no renewal by new facts/RTT/retries. A valid
receipt can finish that round; if newer facts exist, immediately prepare the
next round with its own full frozen native interval. Missing proof still
restores latest AND future full fanout. No controller, pacing, cadence or
native interval adjustment. This deliberately revises the previous failure
bound: a single in-flight pre-failure receipt may validate once, then the next
round must expire. The bound is remaining old interval plus one new interval,
plus actual actor/alternate service, not an indefinitely renewable wait.

Forecast: removes the observed impossible serialized-successor proof budget
and may materially increase selective publication, reducing redundant return
work. No numeric speed gain is justified; fresh-deadline misses/native queues
and adverse timing can still dominate. Falsifier: bounded healthy individual
round trips still lose selection, a duplicate/late receipt renews authority,
or silent failure can postpone alternate publication indefinitely. Smallest
next action is independent symbolic/caller review and one real-policy RED;
then coherent deletion plus same ordinary return-cut DOWN/UP comparisons and
selected-output failure checks if the model survives. Promotion requires actual
useful timing/completion/cost improvement; no favorable timer or profile rescue.

11:28 +08 observer review: exact session/stream/directional-token transitions
are feature-only and independently reviewed as policy-neutral. One release
feature build is running; ordinary executables remain frozen. A symbolic
discriminator is now explicit: with steady proof round-trip D, native interval
P, and newer feedback after epsilon, receipt1 arrives at t+D but only then
starts receipt2's probe with inherited deadline t+epsilon+P. If
D<P<=2D-epsilon, selection can expire despite two timely individual exchanges.
This follows the current model; it is not a measured Product defect yet.
The capture must show whether this serialized-successor condition, delayed
admission/owner/reply service, or neither dominates the real UP/DOWN behavior.
Do not lengthen P or rewrite the contract from the symbolic case alone.

11:10 +08 upload outcome: both complete exactly,398.184→385.961Mbps;
target-confirmed/local-accepted2071920640→2011234304B in41.627353→41.687789s.
Maximum confirmation/write gaps.664826/.484657→.496251/.348209s improve.
Return bytes instead rise1.24% (47.84→48.43MB), restricted median return queue
279059→631202B, client peakRSS306312→358888KiB. This proves bounded settlement
in this cell, not mirrored cost/speed benefit or a leak. Full series/cost report:
CONFIRMED_RETURN_UPLOAD_20260910. Candidate0cab2b5 remains unaccepted; four
ordinary pairs preserve multidimensional and direction-dependent outcomes.

Next bounded diagnosis, before any policy change or broader matrix: establish
actual confirmed-return participation in DOWN and UP. Competing causes are
(a) logical proof misses its frozen deadline and baseline fanout dominates,
(b) selection works but retained/new-output service or other native traffic
dominates cost, and(c) native/shared queue history changes critical service
despite selected publication. Existing ordinary totals cannot distinguish them;
neither lower bytes nor a QUIC Active label identifies the selected token.
Information forecast only: selected/fanout residence and exact probe creation,
admission, logical receipt/reply, confirmation and expiry will accept or rule
out(a), and establish whether the outage gate actually covered a selected leg.
If proof is normally timely, do NOT change its interval; follow existing native/
allocation evidence instead. If it misses, attribute the actual elapsed stages
before proposing a model correction. No assumed failure, new tuning knob,
controller change, rate forecast or public performance claim.

Smallest action: feature-only transition/marker observer on this frozen trial,
using existing lab_diagnostic and exact stream/output/token identities. No
DATA-frame log, payload, per-frame dump, periodic heartbeat or harness rewrite.
Ordinary binaries stay frozen. Build once, then same return-cut DOWN/UP
diagnostic captures; run-selected-output failure only after participation is
known. Stop observation when this question is answered. Observer timing is not
ordinary performance acceptance; full global gates below remain intact.

11:01 +08 outage outcome: control→candidate370.001→375.288Mbps;
maxbodygap.782613→.594647s, echo max2496.604→1119.198ms,
p95585.031→567.280ms;73→77 successes and no failures/restarts. Median echo
286.851→328.362ms worsens; preserve that tradeoff. Both deliver through the
three-second QUIC outage and restoration, but no selected-feedback identity
was observed, so that narrower gate remains unproven. This supports continued
bounded validation, not release or a universal recovery bound. All six download
cells and three mechanism/build logs are archived and listed in
CONFIRMED_RETURN_ORDINARY_20260910.raw.tar.gz (33files, no configs/keys).
Full phase review adds an adverse restoration result:33–40s379.962→336.794Mbps
(−11.36%), despite improved worst echo/read gap. Do not hide this with whole
averages. This remains a multidimensional unaccepted candidate; mirrored upload
is a necessary direction/settlement diagnostic, not an acceptance promotion.
Telegram delivered the three measured outcomes and this adverse phase at
approximately03:02UTC; next nonurgent update not before04:03UTC, with a soft
frequency advisory to reduce nonessential reports.

Next exact question: does symmetric server-owned feedback routing preserve
receiver-confirmed UP service and post-load settlement through the same RETURN
restriction? Existing caller/credit ownership differs by direction, so download
results cannot answer it. Run unchanged CONTROL then CANDIDATE with existing
40s single-upload probe,500Mbps UP and500→10→500Mbps DOWN15–25s,30/70ms delay,
zero configured jitter/loss/outage. This is the directional mirror, using
MIRROR_IMPAIRMENT=0, not a10Mbps upload-data test. Forecast: potential return
work reduction but no quantified upload gain; real confirmation/completion and
write/confirmation gaps decide. Preserve local accepted versus target-confirmed
bytes, drain time and censored/missing bins. Stop promotion on a new stalled
settlement or adverse critical service; no sampler, native or timing rescue.

10:56 +08 healthy reverse-order result: control→candidate392.069→378.423Mbps,
maxbodygap.277598→.271791s, echo p50348.907→247.177ms,
p95687.778→426.991ms, max781.019→538.751ms;49→50 successes, no failures.
The first pair's latency penalty does not repeat; healthy throughput is3.5%
lower. Retain both observations, not a claim of non-regression or rejection
based on a single tail. Independent decision review recommends the existing
failure/recovery gate before any participation observer or policy redesign.

Next exact question: does the unchanged candidate preserve useful mixed service
and future feedback when QUIC is silently blackholed, then recovers, without
restart or failed settlement? Run CONTROL then CANDIDATE in existing combined
40s workload with500/500,100msRTT,no configured jitter/randomloss/QoS, and
the unchanged UDP outage30–33s. This isolates failure from the prior restriction.
Information forecast: detects a material recovery regression caused by reduced
healthy redundancy, not a promised speed gain. Retain all read/echo gaps,
failed attempts, phase history, cost and restoration. Native/DATA/return service
are all affected by a whole-QUIC outage. Without evidence that QUIC owned the
selected feedback output, do not call it a selected-output proof; add a minimal
observer only if that exact coverage is required and remains inconclusive.
Any new regression stops promotion and targets its owner, not timeout tuning.

10:52 +08 ordinary pair: control→candidate whole336.578→345.632Mbps;
restricted180.238→226.058 and maxbodygap.502562→.452162s improve, but echo
p95465.621→567.032ms and restored p95/max341.525/452.349→731.948/805.945ms
worsen. Return bytes fall16.12%, no class drops in either. Healthy5–15 body
440.255→420.434Mbps and worst echo560.703→964.550ms also worsen; this is not
solely restriction/recovery. Candidate promotion is stopped, not task execution.
Full report is CONFIRMED_RETURN_ORDINARY_20260910; no packet/deadline causality
is inferred from serial management/queue samples.

Next discriminator (predeclared before running): same ordinary executables,
dedicated healthy500/500,100ms RTT, zero configured jitter/loss/outage, existing
25s workload, CANDIDATE then CONTROL to reverse the first pair's order. Exact
question: does the healthy service penalty persist without preceding return
restriction, or is the first pair insufficient to separate candidate impact
from shared-native/run variation? Existing healthy portions are short and
restored portions retain preceding queue history. Information forecast only;
no expected improvement, parameter/source change or favorable rerun. If the
penalty recurs, reject promotion and inspect its actual owner before another
model; if mixed, retain uncertainty and select one causal service question.
No upload/blackhole/global gate claim follows this discriminator alone.

10:45 +08 mechanism outcome:339unique focused checks pass, including both
actors' blocked-I/O and actual-credit proof controls, capacity-one marker
ordering, exact replacement/terminal authority and shrinking-RTT expiry.
Independent cross-review also corrected client pending flags that counted
deliberately skipped siblings; enabled-policy checks now prove debt becomes
eligible again after expiry. No other conforming-path defect was established.
Duplicate-Probe reply-history expansion was rejected: nonce-once reliable FIFO
does not reach the alleged replay, and it added no demonstrated benefit.
The first compile omitted two new diagnostic frame labels; exhaustive matching
identified it before tests. Corrected build90s, tests0.53s; nine additional
affected groups pass. Logs: confirmed-return-{focused,affected}-0910.log under
./.tmp/reflection/. Checkpoint the candidate, build ordinary release binary,
then run the unchanged full phase/timing return-cut comparison. Still no
performance promotion, controller tuning, README update or release.

10:40 +08 integration checkpoint: client/server/wire candidate is source-complete,
with full-fanout discovery, exact token ownership, actual-MAX-gated logical
replies, retained-I/O expiry and terminal full fanout. Common review caught
and corrected a shrinking-RTT successor deadline hidden behind an older longer
deadline; expiry and next wake use their minimum. No timing parameter changed.
Wire15 is an explicit no-compatibility break, not an optional unknown-frame
extension. Root's first focused build is running; direction adapters are under
independent cross-review. This is not a successful performance milestone.
Next: affected mechanism controls, then the one ordinary asymmetric return-cut
pair against the frozen scoped comparator, preserving all phases/tails/costs.

Prerequisite result: both generation-churn REDs now pass (95s compilation,
0.01s execution); all220unique focused stream/sender/client/server checks pass.
Evidence: `./.tmp/reflection/feedback-service-prerequisite-green-0910.log`
and `./.tmp/reflection/feedback-service-focused-0910.log`. Independent helper,
client and server reviews found no remaining concrete counterexample within
this contract. MAX-only service cannot complete terminal ACK; old-tail
completion cannot publish a newer generation. Server registry catch-up retains
actual admitted credit across detach and reconciles after ready-batch dequeue,
before DATA validation. Closed/retiring outputs discard their unsent tails;
the immediate path retains no additional tail allocation.

Disposition: exact prerequisite fixed, combined practical trial pending. No
ordinary speed benefit is claimed, no controller/cadence/pool change, no new
release comparator. Its stated retention cost is unchanged. Next is the one
confirmed-return candidate described below, with full-fanout discovery rather
than an arbitrarily preferred carrier. Old rejected trials remain rejected.

00:52 prerequisite outcome: both real producer/queue tests fail at their
intended assertions before runtime changes. The request backup repeats chunk
zero three times, never acknowledges old [512,513); the response alternate
receives old ACK evidence but no newer MAX in six service opportunities.
The always-fanout request control passes first. Build103s, tests0.01s; evidence
`./.tmp/reflection/return-service-prerequisites-red-0910.log`. An earlier
fixture compile error named PathProof rather than actual PathProofData; it was
corrected by tracing enrollment and is not counted as Product RED.

Implement one joint feedback-service owner, not ACK-first retries plus a
separate credit loop. Move the existing server cumulative ACK vector to the
output owner beside latest MAX (no second copy). Each output retains only its
unfinished ACK tail and alternates successful ACK-chunk/latest-MAX admissions
when both need service. Failed admission does not advance the turn. Continue
while capacity accepts work; no voluntary yield followed solely by a capacity
wait. Every publication/retry returns BOTH effects: admitted MAX must commit
receiver credit even from ACK retry, and ACK completion must refresh terminal
fences even from MAX retry. Root/shared helper and disjoint direction adapters
are one coherent prerequisite transaction. No runtime speed claim or native
controller/cadence/pool change. Confirmed-return routing follows only after
this mechanism and retained-state cost pass focused review.

The matched Q/raw/H2 evidence and same-build feedback withholding already
identify material redundant return publication; another baseline calibration
would not change the decision. No new run was needed. Do not tune controllers,
pool size, ACK cadence or packing. Root reversal checkpoint is5d2af0c.

Evaluate the smaller confirmed-return contract appended to SCOPED_ACK_SERVICE_MODEL:
feedback itself remains immediate and pipelined; a valid exact stream-owner
probe receipt can select one return output. A nonrenewing missing-proof deadline
restores ordinary full fanout for current AND all future ACK/MAX facts. Receipt
loss cannot make new facts wait behind an old checkpoint. First/terminal fanout
remains prompt. No confirmation-based data release, credit, estimator update,
per-generation journal or frozen logical checkpoint is proposed. This revises
the zero-added-alternate-delay policy explicitly; it is not equivalent cleanup.

Independent review found a necessary prerequisite: a skipped backup may need
multiple cumulative chunks, but the existing cursor restarts at chunk zero on
every new generation. One slot per new sparse update can therefore starve old
higher facts. This lag is deliberately induced by selective publication; merely
calling old full fanout on expiry is insufficient. Prove this through the real
producer/queue before any selected-return integration. The proposed correction
retains only the unsent immutable tail of one in-progress job per attachment,
finishes it despite newer generations, then serves the current desired state.
Do not retain a separate frame history per generation or add timeout knobs.

Cost is explicit, not free: worst-case retained range payload is roughly1MiB
per blocked attachment under the65,536-range default, up to4MiB for four
attachments or64MiB at64slots per directional stream, excluding overhead.
Accepted prefixes leave the job; no snapshot is retained when immediate
publication completes. Previously this cost was a reason not to call per-path
snapshots negligible, not proof that a bounded tail is forbidden. Count actual
retention and teardown in focused controls and ordinary RSS. Do not claim a
whole-process memory bound from the per-stream bound. No resource limit is
changed or silently borrowed. Further integration needs an explicit cost review.

Forecast: the cursor correction alone is not expected to improve the captured
one-range steady case. Its purpose is preventing a concrete recovery regression
in the return-service trial. That combined trial could remove material fanout
cost (diagnostic restricted266→399Mbps, worst echo2405→274ms), but neither that
gain nor latency parity is promised. Falsifiers: unproven logical receipt, lost
sparse facts, renewable deadline, stalled newer credit, unbounded retention or
ordinary adverse timing. First action is one real-cursor/producer RED plus
opposite controls, then model review; no broad benchmark or runtime stack.

### Current decision — reject logical ACK cadence; continue the existing owner

Ordinary trial8a0413d is REJECTED. On the unchanged500Mbps DOWN and
500→10→500Mbps return cut, restricted body service151.552→195.641Mbps
improves, but worst echo756.803→1957.233ms, echo p95656.240→1471.908ms and
maximum body gap.563265→.754914s worsen. Return bytes fall4.27%, yet restricted
median return backlog539984→908957B and whole return drops0→3950 worsen.
Native TCP and QUIC RTT rise together.634focused checks establish the candidate
mechanism, not good composed service. Full histories, costs, failures and
limits: [ordinary comparison](ACK_CADENCE_ORDINARY_20260909.md).

The trial source/RFC/tests have been reversed with an exact patch. Source
comparison against39dc2ad passes for src/RFC/Cargo, and target/release matches
the frozen ordinary scoped-ACK comparator. The rejected executable is retained
only for historical attribution. No cadence, estimator, controller or threshold
rescue follows this failure; exact ACK-deadline/assignment causality is not
claimed from ordinary samples. Healthy/upload trial follow-ons are cancelled.

Two bounded source audits close a tempting duplicate-feedback hypothesis:
server ACK subsumption skips already-applied positive/negative facts before
flight, sampling and progress-clock changes; delivery sampling independently
requires newly released unambiguous Original bytes. A zero-release frame can
still legitimately carry new scoped negative evidence after the positive
union frame. Ignoring it would lose recovery authority. No duplicate-specific
seconds-long mutation chain was found; decode micro-optimization is deferred.

TCP multiplicity audit also finds intentional policy, not a new defect. The
mixed configuration omits pool overrides; default max-tcp-carriers=3 expands
one TCP group into three regular members beside QUIC.1a79f69 deliberately
replaced elastic retention with MAX reconciliation; fc8dcd1 preserves regular
siblings for a single TCP group. Historical per-flow-policing benefits do not
prove today's acceptance, but changing3→1 would change the declared case.
RFC7.2's obsolete MIN–MAX introductory spelling is documentation drift, not
the mixed performance root cause. Do not restore elastic pools or protocol
preference from this finding. Existing raw1/3 controls already establish a
native-contention tradeoff distinct from the asymmetric feedback collapse.

Next decision remains a practical return-feedback service contract, with the
existing matched raw/QUIC/H2 restriction evidence reused before any redundant
baseline run. No new runtime proposal is selected by these negative audits.
Candidate rejection is not task completion. Global gates below remain intact.

### Continuing the loop — logical ACK generation, not per-sibling deferral

23:15 +08 candidate component gate:634unique focused relay/request/stream/
capacity checks pass. Publisher RED[1,2,3,4,5] becomes[1,1,1,1,1], with terminal
tail still immediate. Actual subsequent bulk blocked write/flush controls pass
for client and server; client baseline controls also passed before runtime.
Independent source review caught and corrected prototype generation/capacity
wake ordering before tests. No opposite-rate estimator, new tuning knob,
native controller or per-sibling delay. RFC explicitly declares changed timing.
Proceed with ordinary control→candidate return500→10→500 mixed pair under the
unchanged500DOWN/30+70ms profile, then healthy mixed and mirrored upload only
if materially useful without adverse timing. Retain all phases/echoes/gaps/
completion/wire/CPU/RSS; no same-average or component-green acceptance. This
runtime is an unaccepted single candidate, not a new baseline or release.

22:54 +08 update: discriminator completes with68,524/85,674 bulk changed
decisions force-only(~80%). Source observer archived/reversed; ordinary binary
restored, no runtime change. Select one prototype transaction per the complete
logical-generation contract appended to SCOPED_ACK_SERVICE_MODEL. Existing
rate-free service quantum, unique-receipt accounting and nonrenewing existing
PTO/2 deadline replace callback-driven force, preserving prompt urgent work and
independent fanout. Both client/server retained write service are in scope;
upload is explicitly affected. Model proof and independent review precede code;
targeted real-producer RED/blocked controls then one ordinary affected pair.
This is not performance acceptance; prior batching tail regressions remain
candidate rejection conditions. Full observer report is ACK_DECISION_TRACE_20260909.

22:58 +08: real publisher work-bound RED reproduced before runtime editing:
five contiguous1KiB bulk receipts produce generations[1,2,3,4,5]/five ACKs;
the unforced control confirms no subsequent urgent/byte/deadline predicate.
Failure is exactly the candidate coalescing assertion, not setup. Build28.88s,
test0.00s; log .tmp/reflection/ack-cadence-red-0909.log. This is intentional
current-contract publication work, not corruption. Proceed with the coherent
candidate and blocked-I/O controls; all promotion gates unchanged.

2026-09-09 22:35 +08:00. User correctly distinguishes an intermediary decision
from task completion. The rejected direction-input patch closes only that
branch. Continue the existing mixed return-feedback owner; no new issue scope.

Decision: evaluate one logical pending-receipt/generation service contract,
while every materialized generation still gets immediate independent fanout.
This is not ready-receipt batching, first-poll ACK/MAX pairing or selectively
delayed sibling catch-up. Keep one existing dirty receive ledger, independent
materialized-output retries, and the same retained application-write future.
First/Latency/terminal and important gap evidence remain prompt. A deadline
for pending receipt cannot be renewed by more bytes, MAX, retries or snapshot
changes, and must be serviced inside blocked write/flush as well as outside.
RFC8.3 must explicitly change if deferred materialization is selected; no
runtime cadence or rate estimator is selected yet.

Source review already proves unconditional prewrite forcing; it does not say
how often first/gap/byte/timer predicates independently require a generation.
Smallest next discriminator: feature-only bounded cumulative trigger counts
at the actual should_send_ack decision, separated by bulk/nonbulk, changed
state and overlapping predicates; observe actual bulk threshold/rate ranges.
Reuse the existing codec counter and one unchanged500DOWN/return500→10→500
mixed capture. No per-frame log, payload retention, new harness, clock/policy
change or compiled observer marketed as ordinary performance. Root alone
builds/runs; freeze and reverse source before the capture.

Information forecast: substantial changed generations supported only by force
justify completing a logical-cadence model and a blocked subsequent-bulk RED;
dominant urgent gap/first work argues against that candidate before runtime
implementation. Counts are opportunity under the old clock, NOT a prediction
of a coalesced trace or Mbps. Byte/time/range predicates interact after any
clock change. Existing ACK withholding169.929→296.144Mbps is cost context,
not this proposal's promised gain; MAX/native/forward queues remain competing
causes. Preserve complete timing/failures/costs and stop only this candidate
if its prospective material benefit is absent, then continue the global plan.

The shared unique-receipt count is next_offset+reorder_bytes; contiguous gap
release and demand-tracker rates are not new arrival capacity. No receiver
rate substitution is justified. Existing missed-generation multi-chunk retry
limits remain explicit; lower generation frequency is not proof of complete
catch-up under arbitrary capacity service. No extra snapshot framework or
numeric knob is smuggled into this discriminator.

### ACK-direction gate complete — rejected as the download-stall fix

Actual publisher characterization passes:351Kbps and500Mbps snapshots select
65,536B and3,125,000B thresholds, but identical six receipts produce the same
generations1,2,3,3,4,5, five ACKs/127codec bytes and three MAX/78codec bytes.
Eight existing publication/backpressure/startup/terminal controls also pass.
Independent source and fixture review agree. Exact commands, scopes, outcome
and test-only patch are in ACK_DIRECTION_CHARACTERIZATION_20260909. The patch
is archived and removed; runtime/RFC/Cargo remainb2aa215 and the ordinary
executable is unchanged. No observer or asymmetric capture was necessary.

Disposition: source-valid opposite-direction coupling, but bypassed by the
actual client's forced prewrite publisher, so it does NOT justify a download
performance fix. Do not implement a receive-rate estimator or switch to upload
to rescue this forecast. The plan's expected opportunity failed at the caller,
not at random network measurement. This completes the selected causal gate,
not the optimization loop or global acceptance.

Next bounded owner remains return-feedback publication service, not native
controller tuning or fixed bottleneck groups. A replacement must distinguish
unmaterialized receipt progress from already-materialized per-attachment debt
and preserve both while application write/flush is Pending. The existing first-
receipt/Latency startup test alone would miss force-off's subsequent bulk
subthreshold starvation. RFC8.3 currently mandates offer-before-park/yield;
any deferred cadence requires an explicit justified model revision. No such
candidate is selected by the rejected directional-input hypothesis. Rejected
ready-receipt batching, first-poll pairing and deferred sibling models remain
rejected/unselected; new work must supply a new coherent service proof and
material forecast before another implementation/build. Global gates below stand.

### Completed source gate — caller bypass found before instrumentation

2026-09-09 22:17 +08:00. Source review, independently confirmed, finds that
ordinary client DATA uses `RelayRecvProgressSend::ack_only` before local write
(control.rs:3506). That constructor sets force_ack=true. Every changed receive
batch therefore bypasses the half-BDP byte threshold; the later unforced call
sees the same receive state, and an unchanged generation only retries pending
publication. The opposite-direction scalar is real but does not control this
download publication cadence. The previous proposal missed this actual caller.

Smallest remaining proof is a characterization through the real RequestSenderService
publisher, receive map, exact live command output and codec: identical contiguous,
sparse, duplicate and hole-fill traces; vary only351Kbps versus500Mbps at100ms.
Validate different calculated steps but identical actual ACK generations/frames
through the production prewrite/postwrite call sequence. This is not a new
required model or Product RED. Run existing blocked-delivery startup/ACK/FINAL
and retained-publication controls alongside it; no runtime policy edit.

Information forecast/falsifier: identical forced publication closes this
directional-rate branch as a download-stall fix. Differing publication needs
exact attribution before further action. Upload's unforced server DATA caller
is distinct and has no new material-impact evidence; do not silently switch
the experiment to it. Expected download gain from changing this unused gate
is zero for this sequence, not the earlier illustrative48-fold opportunity.

Execution deviation declared before tests: cancel the proposed diagnostic
observer/build/asymmetric capture if this direct publisher characterization
confirms the unconditional bypass. Another capture cannot overturn the source
predicate and would not select a different fix. Preserve444fb38's real
blocked-application protection; merely turning off force would remove prompt
feedback while the write future can remain pending. No replacement timing,
receive-rate estimator or previous rejected batching trial is authorized by
this finding. Runtime remainsb2aa215 and all broader practical gates stay open.

**Proposed next transaction — test ACK-cadence input direction before changing publication policy.**

User requested a practical deterministic fix plan after the no-fixed-groups
discussion. This is a source-backed candidate and a causal gate, not a proven
stall root cause or authorization to promote a runtime change. No runtime,
RFC, test, build or laboratory change has been made for this proposal.

New exact source finding, independently reviewed: ACK byte cadence compares
incoming Product progress with half a BDP derived from the opposite local
outbound snapshot. Client response ACK uses lowest_eta_path_snapshot and
ClientToServer evidence; server request ACK uses an ingress-matched output
whose rate evidence is ServerToClient. Ingress identity and response_lane do
not reverse that rate's meaning. capacity.rs:196/523 reads legacy delivery/
Product scalars, not a direction-checked receive-service record. These may be
startup/configured/native fallbacks, not necessarily measured tiny-request
rates. Borrowing RTT itself is not the claimed defect.

History185377f introduced the half-BDP rule as repair-release cadence;
28dc39a moved it into model capacity geometry. Neither inspected source nor
RFC8.2/8.3 declares opposite-direction Product goodput as a reverse-budget
policy. Shared-scalar ACK coupling was noted earlier, but the actual directional
producer/consumer mismatch is now explicit. It remains distinct from rejected
native-rate ranking, which deliberately left feedback cadence untouched.

Smallest causal gate: characterize the real receive/ACK publisher with an
unchanged incoming trace and timing while varying only the supplied opposite-
direction rate; then use one existing return-restriction capture with bounded
ACK-cause/input summaries. Record actual byte threshold, scalar source and
direction, byte/gap/timer/forced decisions and resulting publication/encoded
counts. Existing codec totals alone cannot say which trigger dominates. Do not
build another harness or log every frame. If clamps, gaps or forced publication
dominate and this input cannot materially explain return cost, end the branch
without implementing an estimator or compensating threshold.

Conditional arithmetic forecast, not observed service: with default resource
geometry and100ms RTT,351Kbps selects the64KiB byte floor, while500Mbps gives
3.125MB half-BDP. Byte-triggered ACK cadence could differ~48-fold for identical
incoming service. Actual scalar availability, clamping, gap/forced/timer ACKs
and MAX traffic can erase much or all of that opportunity. The earlier unsafe
ACK-only withholding pair improved restricted169.929→296.144Mbps and worst
restricted echo1.246s→.471s; it proves ACK work can matter, not a safe gain
forecast or bound for this different candidate.

If and only if that gate establishes material impact, the candidate model
separates received Product progress evidence from local transmit service and
carrier-ranking snapshots. Use correctly scoped local receipt evidence, with
explicit freshness/idle/duplicate semantics; do not substitute a peer rate or
invent capacity. Preserve the existing byte/time geometry pending its own
justification, immediate first/latency/terminal feedback, scoped gap truth,
independent publication on every live attachment, exact pending retries,
MAX credit, native control and all resource bounds. Define the complete
receive-evidence model and its RED/controls before runtime implementation.
This changes the evidence domain, not a multiplier or preferred protocol.

Acceptance order after mechanism proof: one ordinary affected control/candidate
pair first; stop on absent material service benefit or adverse timing. Then
healthy TCP/QUIC/mixed and mirrored upload, sparse/cold/warm feedback, blocked
application writes, receive reordering/duplicates and one-way return blackhole
with restart-free recovery. Retain full bins, first service, gaps, echo tails,
confirmed completion and wire/CPU/RSS costs. No second change to rescue a failed
pair. Native-contention allocation remains a separate next owner; do not add
bottleneck groups or a coupling controller to this candidate. Broader combined
loss/aggregation/Cloudflare/baseline gates below remain required before release.

### Completed return-feedback source/capture review

Both context discriminators are complete. One versus three independently
progressing raw TCP bodies gives450.855→452.044Mbps, but sparse echo median
103.221→214.376ms and p95125.942→235.202ms. All160echoes succeed; all four
bodies have intentional40s partial service, no failed request. The same
500/500Mbps,30/70ms profile has no configured loss, jitter, QoS or outage.
Native competition therefore reproduces substantial common delay without MPP;
this is not all mixed attribution, unavoidable-queue proof or acceptance.
Full histories/costs and exact raw archives are in DIRECT_ECHO_CONTEXT_20260909
and RAW_CONTROLLER_CONTEXT_20260909. No runtime correction follows these cells.

Priority returns to the already demonstrated return500→10→500Mbps stall,
not another healthy ranking/priority experiment. Ordinary scoped ACK retains
185.569Mbps during restriction and an.811220s body gap; the unsafe full TCP
feedback-withholding pair independently removes multi-second restricted echoes
while TCP data still progresses. Matched QUIC/raw/H2 context sustains~441–476Mbps.
The healthy native-count finding does not excuse this separate feedback cost.

Independent source/model review confirms no missing backup retry: each live
exact output immediately receives latest ACK/MAX offers with capacity wakes.
Deferred full ACK fanout still has incomplete catch-up under generation churn;
MAX-only deferral has a tractable scalar but adds alternate-return delay and
can hurt small-window service. Neither is selected. Preserve the independent
publication that repaired selected-return blackholes; no new timer or window.

Completed bounded source-only question: do already superseded, not-yet-written
MAX grants consume immutable output queue/service, or is latest-value
supersession already owned there? This is distinct from the accepted incoming
logical MAX fold and rejected ACK batching/pairing. Check exact writer ownership,
partial writes, close/replacement and independent readiness before any proposal.
Information forecast: an actual redundant queued obligation could justify one
reachable counterexample; existing supersession or work already beyond the
irreversible boundary ends this branch. No new build or speed gain is forecast
from reading. Removing records is not proof of better timing. Do not implement
unless the resulting contract preserves prompt per-output service without a
timer, ACK clock change or unbounded state. Existing scope/gates remain intact.

Outcome: no new contrary evidence. The immutable outbound MAX overlap is
already the deferred queue-latest branch recorded below at11:39, not a new
defect. The actual restricted shadow has44,437MAX takes,9,157with newer work
pending (20.607%). Nine reports show0–1currently pending MAX; the cumulative
peak86does not increase in that interior. No persistent large pre-writer MAX
backlog or exact MAX residence is established. Current queues are bounded and
reclaimed; do not relabel redundant work as a leak or missing retry.

Existing restricted client TCP native Send-Q totals have median/peak244878/
514653B in the ordinary scoped candidate,519891/889397B in the fanout control,
and180262/317058B in the scoped queue observer. All three sockets retain work
in every restricted snapshot. These include native unsent/unacknowledged
bytes, not specifically MAX. Management queue_bytes is native TCP notsent;
Product DATA flight is not feedback queue occupancy. QUIC queue is unavailable,
not zero. A dequeue is also not an irreversible native write. These distinctions
prevent manufacturing a dominant MAX queue from the dashboard.

Disposition: retain queue-latest deferral, reject an alleged absent retry,
and keep all runtime/RFC code unchanged. Do not spend another observer/build
on that known20%opportunity without new evidence of material user-service value.
The original5e1ace67history explicitly records the locally accepted but
wire-blackholed selected feedback path; its protection must not be rolled back
because full fanout now has a demonstrated asymmetric cost.

The next performance transaction still belongs to independent return-feedback
service. Its prerequisite is a coherent publication contract with bounded work
and explicit alternate-return timing, not another frame-packaging, scalar or
controller experiment. Full scoped-ACK deferred catch-up remains blocked by
generation/chunk starvation and retention costs; MAX-only deferral remains an
unselected latency tradeoff. No implementation candidate is currently justified.
This is an unresolved model decision, NOT completion of the optimization loop
or a new release requirement. Healthy native contention is classified separately;
seconds-long stalls and the unchanged global experience gates remain open.

### Completed native-count discriminator (predeclared contract)

The direct companion pair completes: mixed397.858Mbps, MPP echo median/p95
268.501/443.952ms; QUIC-only424.411Mbps,106.692/159.501ms. All160 foreground
and200 direct attempts succeed. Actual loaded direct median/p95 is
250.033/404.408ms mixed versus101.300/164.717ms QUIC-only; unloaded~100ms.
The early429ms QUIC-only direct setup spike is retained outside foreground load.
Thus a material penalty also reaches independent traffic bypassing MPP, and
MPP-only framing/reader service cannot explain the whole mixed delay. Exact
shared-network versus host attribution remains bounded. No runtime change.

Next existing composition question: is the native controller multiplicity
alone sufficient to create the common penalty, or does MPP's specific wire
work/assignment pattern remain necessary? Existing raw reference has one bulk
TCP loop, not mixed's three TCP plus QUIC. Reuse the canonical bulk_worker and
interactive_tcp_worker for one versus three simultaneous direct raw bodies,
one sparse echo and one shared40s clock. Same500/500Mbps30/70ms profile, body,
target, chunk size and host allocations; no Product or controller adjustment.
One independent result per body, full echo attempts, aligned raw bins and common
elapsed denominator. Observe actual simultaneously progressing ESTABLISHED8080
sockets/controller identity; echo10022 separate, not old7443 telemetry filter.
Early EOF/reopen, missing counters, failed workers or serialized target service
invalidate a clean controller-count comparison. Preserve all adverse evidence.

Information forecast: substantial common delay under three raw bodies supports
native contention as a material context; prompt raw service rejects count alone
and returns focus to MPP's work/assignment pattern. Neither proves a numeric BBR
version, universal inevitability or a safe coupling/queue policy. This is a
two-cell baseline ablation, not new performance requirements or favorable reruns.
No build, controller replacement, bandwidth hint or limit change. Expected
information is material because current mixed loaded bypass adds~150ms median
while body throughput remains below the prompt single-native baselines. No
speed improvement is forecast from measuring it. Stop after1/3 cells and record
full timing, aggregate/per-flow distinction, queue/wire/CPU/RSS and identity.

### Completed direct-echo discriminator (predeclared contract)

Source/capture reuse completes before this selection. Two independent audits
confirm exact H3 request-stream priority is applied, latency/repair1 versus
bulk0, native fairness stays enabled, and writes are per-request rather than
one shared bulk writer. TCP flush and QUIC send_data are native acceptance,
not emission. Earlier-frame actor service may precede the TCP reader's next
decode, so near-zero postdecode time cannot exclude pre-read task delay.
No missing-priority or shared-QUIC-writer defect is established.

Independent reuse finds mixed/QUIC-only steady DOWN backlog medians14.605/1.869MB,
whose difference at500Mbps is203.8ms; server QUIC RTT differs200.3ms and winning
handoff→decode medians differ197ms. This is scale agreement, not per-byte cause.
HTB/netem report the same queue and must not be added. All three kernel TCP
sockets report`bbr`; its numeric version is not established by ss. Three TCP
controllers plus QUIC differ from the single-controller raw baseline.

Smallest discriminator: reuse interactive_tcp_worker in direct mode with a
persistent64B request every500ms beside one existing40s mixed workload, then
one QUIC-only context. Fifty-second companion includes unloaded margins;3s
timeout preserves the existing observation convention, not a Product limit.
Use the already frozen echo-membership build with intervention flag UNSET in
both cells, existing exact echo trace, and unchanged500/500Mbps30/70ms healthy
profile. No compile, new runtime observer, priority, queue or controller change.
The thin companion imports the existing worker, preserves every actual attempt
and records its own monotonic/Unix anchor; it is not a new probe implementation.
Root alone runs cells sequentially with no build or other lab overlap.

Information forecast: elevated bypass latency during mixed load supports common
cut/host service; bypass near unloaded latency while MPP replies remain delayed
favors tunnel-native/reader service. A different route/class, weak reproduction
or inconsistent timing leaves attribution unresolved. The companion's~128B/s
per direction is negligible compared with bulk, but its independent TCP packet
schedule and shared target host remain confounders. Do not subtract unpaired
medians as an exclusive delay component. Compare full phase/attempt histories,
same-cell body service, queue/native/CPU costs and exact recorded echo joins.
No speed gain is forecast from observation. Stop after the declared two cells;
no favorable repeat, policy promotion or new inventory follows automatically.

The predeclared native-rank pair completes and is REJECTED for promotion.
Control→advised useful396.378→419.059Mbps; echo p50/p95 worsens
281.651/469.903→318.842/577.845ms, including median in every chronology slice.
Maximum echo1244.017→761.642ms and bodygap.325905→.271684s improve;
all79/78 attempts succeed. Allocation really changes: TCP Original share
33.33%→48.76%, accepted copies275586102→186104196B. QUIC copies nevertheless
rise4.78→26.06MB, and server QUIC median RTT stays254→251ms. All82 profiles
retain500/500Mbps,30/70ms, no loss/jitter/QoS/blackhole/drop. Actual source-use
marker validates the score-only intervention; the unchanged ordinary observer
does not report the substituted scalar. Full timing and costs remain in
TCP_NATIVE_RANK_ABLATION_20260909 and its verified572047B/17-file raw archive.
Source/RFC/Cargo and executable are restored; no runtime correction is retained.
The information forecast succeeds in separating a real allocation effect from
an effective composed correction; the latter fails. No favorable repeat or
compensating scalar/threshold is justified.

Existing mixed issue remains: loaded echo service is materially worse than the
matched high-capacity raw/Xray/H2 panel and same-build QUIC-only context. Exact
winning joins already place most response delay after positive native handoff:
ECHO_OWNER_SERVICE has210/329/461ms median/p95/max handoff→decode, with source
and postdecode stages near0–2ms. Actual extra-QUIC winners retain228ms median
versus31ms QUIC-only. Live-hedge suppression, forced-QUIC Original placement
and native ranking each leave substantial mixed residence; none establishes
one safe policy correction. Preserve separately measured request-side waits.

Next action is source/capture reuse, not another build: map those exact winning
TCP/QUIC handoffs to native stream/connection acceptance, queue position,
packet service and receiver readiness using the archived observer and ordinary
code. Competing owners are native FIFO/pacing, a shared network queue and peer
transport/read-task service; aggregate backlog alone cannot choose among them.
Information forecast: an existing exact queue/service join or reachable model
mismatch can justify a bounded correction/discriminator; absence of the needed
boundary must remain explicit. The~200ms mixed postwrite excess is a comparison
envelope, not a forecast of removable delay. No new observer, controller change,
queue cap, protocol preference, release or broader issue inventory is selected.
Independent audits inspect producer and consumer boundaries while root reuses
the complete timing/cost captures. Global acceptance gates remain unchanged.

### Completed ranking discriminator (predeclared contract)

Actual TCP prepared-input observation completes:398.006Mbps,80/80echoes,
p95/max408.021/639.834ms, bodygap.273840s. All41 profiles unchanged.131events
cover509539 prepared observations, three exact epoch1 TCP outputs, no revoke.
Only TCP2 falls back after startup:29.751–30.130s and37.598–38.783s in the
server diagnostic clock; native advisory remains38.446 and22.463→17.551Mbps.
Its ambiguity totals are flat during both. Eligible Original service and
admission already decline before expiry, with Original debt reaching0 while
native work remains. Thus numeric fallback is reachable, but immediate repair-
ambiguity poisoning and an arithmetic sampler bug are NOT established. Native
socket service includes copies/other traffic; Product rate measures this flow's
eligible Original service. Full evidence: TCP_PRODUCT_EVIDENCE_20260909.

Next bounded diagnostic question: does using that per-flow rate to rank
carrier work materially worsen healthy mixed Original placement, or does the
native/shared service dominate even with another available advisory? Independent
source review permits a score-only intervention: in the two Throughput Original
ETA evaluations, score a temporary copy with the existing qualified local TCP
carrier rate and PathCapacity scope when available. Keep the original snapshot
in every target/admission tuple. Leave Latency lanes (including echo), QUIC,
typed authority, Product qualification, feedback, recovery and shared snapshot
helpers unchanged. The incumbent hysteresis consumes supplied ETAs plus queue/
jitter and does not secretly recompute rates. FirstPath identity can change
because placement changes; unchanged resource rules may then apply elsewhere.

This is a diagnostic rate-source substitution, not a production recommendation.
No multiplier, guessed rate, timer or threshold. Use one frozen feature build
with the same observers in both cells; startup-fixed flag unset control first,
set server-only second. Verify activation and actual finite qualified source
use, retain all native/copy/useful timing and costs, and remove source before
traffic. Same healthy500/500Mbps30/70ms profile, no loss/jitter/QoS/blackhole.
The observer still reports the ordinary prepared snapshot, not the substituted
rank scalar; that distinction must be explicit. No full candidate log flood.

Information forecast: native advisories differ severalfold in observed late
TCP inputs, so their ranking effect could be material; the echo median~294ms
versus earlier QUIC-only~107ms is a comparison envelope, not a removable-delay
estimate. Better sustained timing at comparable useful load and real changed
allocation supports the ranking coupling. No allocation change or adverse/
ambiguous timing stops promotion and requires attribution, not another gain or
favorable rerun. Native advisory can overpredict unique service and increase
queueing; zero gain or regression is plausible. Keep every adverse phase.
No change is accepted without subsequent coherent model/ordinary checks.

### Completed TCP evidence discriminator and preceding repair attribution

The predeclared same-build ablation completes. Control→suppressed useful
420.434→450.065Mbps; echo p50/p95/max434.334/603.703/841.278→
289.985/422.512/760.980ms; bodygap.537601→.413086s. All77/80attempts succeed.
All82 profiles remain500/500Mbps,30/70ms, no jitter/loss/QoS/blackhole/drop.
Accepted repair100017667B→0 with no structural substitution; duplicate receipt
93702451B→0. Actual Original payload rises2120982771→2275304730B, while TCP
share36.1309%→55.6274%. This supports live hedge service and its mediated effects
as a contributor, without lowering useful load; it does not isolate wire cost.
Echo completion spacing worsens.885998→1.017568s, CPU/UP bytes rise, and server
QUIC median RTT remains249ms despite falling from452ms. Full157attempts,80bins,
costs and conservation are retained in LIVE_HEDGE_SERVICE_ABLATION_20260909 and
its verified518654B raw archive. Six-file overlay removed before traffic;
ordinaryb2aa215 source/RFC and executable restored/cmp verified. No suppression
or pipeline policy is retained. No failure/recovery acceptance follows this run.

Next exact source question is already in the split/unaccepted `a4679b5` scope:
can repair ambiguity withhold numeric TCP Product-rate evidence used by the
response allocator even while the dashboard publishes fresh Linux native rates?
Independent review confirms the mechanism is possible: ambiguous releases do
not supply path-proving samples, TCP scalar selection excludes kernel/peer
rates, and a numeric Product epoch can expire back to the startup prior. This
does NOT revoke established tagged qualification. Dashboard native confidence
and rate are not the per-flow completion input. The current capture lacks those
actual inputs, so it does not prove this sequence caused its changed allocation.
Only~13% copied payload relative to TCP Originals bounds a simple proportional
explanation; a much larger effect needs concentrated ambiguity or epoch loss.

`a4679b5` repaired proven configured-prior provenance but bundled rate demotion;
RECENT_SEEN_CHANGE_REFLECTION already separates their verdicts. `b7961f3`
retained that policy while isolating the unfinished typed sidecar. Preserve the
startup correction, durable qualification and native authority. Do not restore
kernel values as typed C, erase ambiguity or call native feedback proof of
Product receipt. Independent consumption review finds this shared scalar also
changes request ACK byte batching through the feedback snapshot, so restoring
it is not strictly rank-only. An initially proposed1.26s feedback starvation
claim was FALSE: every DATA call independently checks elapsed time since ACK;
MAX cannot defeat that predicate. No timer defect/fix follows. This correction
records complete consumer composition, not another issue. No native restoration
is selected merely because RFC17.1 permits a legacy advisory. This remains the
existing mixed allocation issue, not a new inventory.

Next one ordinary healthy40s mixed capture observes the actual per-flow TCP
completion snapshots consumed by prepared selection, not management rows or a
second model. Reuse existing computed snapshot and observation time under its
output lock. Fixed stream1, per-output bounded lifetime; initial/structural
source, freshness and qualification transitions plus periodic numeric min/max/
last over every observation. Include raw Product epoch, effective rate and
native advisory separately, and already-computed eligible/ambiguity-excluded
Original ACK accounting without new range scans. Both advisory and fenced
prepared calls are covered; these snapshots are not claims or admitted choices.
Periodic output does not sample the accumulated observations; final unflushed
tail must be emitted or explicitly censored. Reuse the four-file cause/receipt
observer and existing runner, no new harness. Independent audit, one feature
build, freeze/reverse before traffic, restore ordinary executable afterward.

Information forecast: continuously fresh qualified Product rates through actual
prepared observations falsify the proposed collapse mechanism. Expiry or rate
erosion with ambiguity growth makes that owner worth causal review, but low C
alone can reflect reduced allocation rather than cause it. Do not promise a
Mbps gain or immediately restore native rates. Keep500/500Mbps,30/70ms and zero
loss/jitter/QoS/outage unchanged; full useful bins, echo attempts/gaps, native/
class and CPU/RSS costs accompany this diagnostic. If it cannot distinguish
the sources or logging materially distorts service, stop attribution rather
than tune a policy to its numbers. No new timer, threshold, controller, permanent
hedge suppression or scope expansion. Global gates unchanged; no README/release.

Observer review before capture: root and independent consumer audit pass.
Exact released Original segments form eligible/ambiguity/other partitions;
observer-lifetime counters are separate from the existing qualification-floor
counter that resets on same-incarnation revocation. Closed in-place replacement
resets observer state; observation itself reads no new clock or native handle.
Periodic counts weight actual prepared observations, not time or selected bytes.
No ordinary mechanism/test requirement is added by this diagnostic. Ten-file
overlay includes four reused cause/receipt files, initializer/cfg plumbing and
the small per-output observer; its build/patch are under tcp-product-evidence.

### Completed live-hedge-service ablation (predeclared contract)

The actual admission/native-drain/receiver-ACK characterization passes with a
live distinct TCP alternate: final-drain repair has spare successor service but
waits for positive ACK of its head. The first fixture failed before that check
because its QUIC queue lacked mandatory native authority; a future observation
epoch was a separate chronology error. Corrected real-clock fixture passes1/1
after28.91s build; source is restored and its patch/logs are archived, not made
an unselected service-model requirement. This is final-drain characterization,
not measured attribution of persistent-gap or request-side network performance.

No pipeline candidate is selected. Sender states can be identical when an
Original is stuck and when it already arrived with ACK still in return transit.
An accepted-copy coverage cursor could help the former but amplify late copies
in the latter; per-transaction ranking alone does not bound that competing cost.
The cited7–14s blackhole incident was an older, separately fixed single-attachment
feedback defect, not evidence of this limit. September exact stalls instead
show already-admitted repairs, stale/no-target decisions and native/shared queued
service; none establishes repeated q-to-ACK serialization as its dominant cause.
LIVE_REPAIR_SUCCESSOR_20260909 records the distinction. Do not repair a hypothesis.

Existing impact/question: healthy mixed has~337ms median echo and~322ms median
server QUIC RTT, versus same-build earlier QUIC-only107/101ms. Does actual live
hedge service materially contribute, or does native mixed/shared service remain
slow without it? Cause/window traces establish material ordinary copies and
late local winners but not counterfactual effect. Next one frozen feature build
reuses periodic Original/copy-cause/unique-receipt accounting, then runs same-
build control followed by an unsafe live-hedge-suppressed healthy40s mixed cell.
Both keep500/500Mbps,30/70ms, no loss/jitter/QoS/blackhole, full bins/echoes,
native/class/CPU/RSS observations and unmodified source workload. No new harness,
controller, limit or protocol preference. Feature flag fixed at process start.

Suppress only server live persistent-gap, active/final retained-frontier and
generic live ACK-gap producers, before enqueue; exclude their nonexistent
recovery-only wait. Preserve real alternative facts, Original placement,
already-accepted debt, ACK/MAX, native writes, stale/failed/unknown-owner recovery,
requalification and cleanup. Do not gate dispatch or disable failure timers.
Source inspection must prove no expired-deadline busy loop or substitution via
the generic live branch. Client behavior stays unchanged. Both captures enable
the same periodic observer and disable per-frame logs; actual cause counters
must verify what disappears and what substitutes. Freeze/reverse before traffic.

Information forecast: equal useful load with much lower echo/native RTT supports
live-hedge service as a material contributor, not a safe suppression policy or
proof of one wire/CPU substage. Persistent delay despite eliminated hedges moves
attribution away from that category. Lower latency only with reduced useful load
is mediation by offered service, not a performance correction. Stale/failure
substitution makes a narrower causal comparison and must stay visible. The
ordinary196–318MB accepted copies are~39–64Mbps/40s of payload-service equivalent,
not promised removable bandwidth. The~230ms mixed/QUIC median difference is a
comparison envelope, not a measured removable critical interval. Zero benefit
and worse ordered stalls remain plausible. No favorable rerun or release claim
from this deliberately weaker recovery; restore ordinary binary after the pair.

### Completed live-owner service characterization (predeclared contract)

The fixed-window transaction completes; REPAIR_WINDOW_20260909 and its verified
307265B raw archive retain the result. Ordinary-policy observation408.341Mbps,
80/80echoes, p95/max445.177/773.080ms and bodygap.338428s is not an improvement
comparison. All16777216 window bytes arrive as Originals; all28 accepted copies
(347856 clipped bytes) arrive187–250ms later. All35 queued events are fallback-
authorized, not an early loss/ETA trigger. Originals precede accepted-copy
queueing56–72ms, comparable to configured70ms return propagation. Thus sender
knowledge lag is not proof of faulty ACK publication or a timer correction.
No window extrapolation or blanket repair suppression is justified. The six-file
observer is removed, ordinary executable restored; no runtime/RFC change.

Existing issue: mixed-mode stalls and slow takeover while an Original owner is
still live. Two independent source reviews find that after a head quantum is
actually accepted on alternate B, the live producer revisits the same Product
ACK frontier. B's exact copy ownership excludes B there and truncates the
uniform prefix; no following mature suffix is served before ACK advances it.
Native queue drain alone does not clear Product debt. This follows current T06,
not a loop typo. Exact stale/failed recovery has different authority and escapes
the restriction; this is not a universal stream throughput ceiling.

Question: can actual producer/admission/receipt paths demonstrate this boundary
with a live Original A, healthy credited B, and two mature quanta, and does it
warrant a pipelined takeover contract rather than a deliberately bounded hedge?
Competing cost: continuous repair can amplify the already-observed losing copies
and worsen healthy latency. First characterize real dispatch, drain native
pending work without Product ACK, verify B's remaining service and blocked next
range, then apply actual positive receipt/ACK and verify next-range progress.
Use one focused test, not another observer build or full lab. A missing native
credit/invalid owner fixture falsifies the claimed mechanism.

Conditional information/benefit forecast: if only q repair bytes can advance
per Product-ACK cycle tau while originals do not progress, service is bounded
by8q/tau. At q14600B and tau100ms this is1.168Mbps despite a500Mbps alternate.
Actual q is phase/model dependent; structural failure and Original progress
invalidate that bound. Removing this serialization could materially improve
known multi-second takeover, but no current timing capture attributes its full
stall to this limit, and healthy performance may regress. Do not predict a gain
or select a candidate from the equation alone.

Independent contract review must preserve per-prefix rank=Apply extent,
immutable assignment clocks, exact range/slot debt, lowest unresolved head
obligation, sparse work, failed-copy retry and actor work bounds. Copy acceptance
is not receipt. No G_s hard gate, ACK-renewed fallback or one-copy-per-RTT cap:
those recreate trickle starvation or add an explicit capacity-independent rate
ceiling. No runtime/RFC prototype until the actual characterization and coherent
service proof justify it. Global acceptance gates remain unchanged.

### Completed fixed-window attribution (predeclared contract)

Ordinary-policy cause capture completes:387.730Mbps,78/78echoes with
median/p95/max296.971/503.781/947.406ms and bodygap.317860s. All41effective
profiles remain500/500Mbps,30/70ms and zero configured impairments/drops.
Accepted copies total318381268B: TCP persistent ACK-gap241184388B (75.75%
of all copies), TCP tail74989600B, QUIC gap2076208B/tail131072B. No other
cause or requalification row occurs. TCP dominates actual accepted copy work;
the earlier active-tail-only hypothesis is not the principal explanation.

Client2,260,437,444B received reconcile as1,944,167,384B new and316,270,060B
duplicate. TCP duplicate314106580B,33.20% of its receipt; QUIC2163480B.
All interval/cumulative counts reconcile and no invalid arithmetic is emitted.
A later server Original flush covers client receipt, but the useful-copy lower
bound is zero: copies can arrive first and their Originals arrive duplicated.
This demonstrates material redundancy, not unnecessary repair or a safe fix.
REPAIR_CAUSE_20260909 retains the full record. Four-file diagnostic is frozen
and removed; ordinary source and executable are restored. No public promotion.

Next exact question: in established ordinary mixed service, do persistent-gap
copies beat their Originals, arrive late, or even queue after Original receipt;
which effective loss/fallback deadline and ETA authorized them? Competing
causes are productive hedging, delayed feedback, premature assignment clocks
and native/shared-queue service. Existing aggregates cannot choose among them.

Smallest action: same periodic observer plus range-filtered metadata for one
fixed[512MiB,528MiB) Product window, enabled from process start through teardown.
512MiB places it beyond startup in the observed~50MB/s workload;16MiB covers
roughly0.35s and is expected to contain many repair attempts at the observed
volume. This is diagnostic selection, never a Product cap/threshold. Preserve
whole intersecting frame extents and all arrivals, not only winners. Record
Original claims and exact accepted-copy identities; actual queued enqueue IDs
and dispatch; aggregate scored-prefix assignment/loss/fallback timing and
effective retained clocks with signed offsets; receiver full intervals, exact
ingress and ordered frontier changes. Reconstruct first coverage offline without
retaining live payloads or scanning runtime ranges. Wire IDs are mapped through
stable per-endpoint physical identities, never by equating endpoint allocators.
Ambiguous joins remain unknown. Range selection bounds coverage, not repeat
events; existing40s observation remains, no sampling or per-frame global flood.

Information forecast: late or already-received Original copies direct review
to the precise deadline/feedback owner; meaningful first-arrival wins prevent
a blanket suppression fix. Absent copies or ambiguous identity in the declared
window stop that attribution rather than prompt favorable window hunting.
The current316MB duplicated payload corresponds to~63Mbps of payload service
over40s, a material budget equivalent, not a promised goodput gain: removing
copies can expose stalls, change native sending and fail to reduce queue delay.
No policy, timer, congestion, queue, resource or RFC change; one further frozen
healthy mixed capture only. The next model decision must use exact outcomes.

### Completed ordinary mixed copy-cause/receipt discriminator (contract)

The placement diagnostic is closed without promotion: TCP bulk Originals fall
42.16%→1.20%, useful service393.901→275.892Mbps, winning QUIC residence
179→192ms median. No missing-service recovery even at lower useful load.
Independent source conservation proves at least313432617B TCP recovery payload
in that intervention, but the same conservative bound is zero in the ordinary
control. BULK_ORIGINAL_PLACEMENT_20260909 retains all80 body bins,160echoes,
82profiles, unequal realized echo membership, costs and the verified raw archive.
The protocol-specific non-work-conserving filter is fully removed, never a fix.

Question: in ordinary mixed policy, which existing successful repair cause
produces material copy work, and how much incoming TCP/QUIC data is new versus
duplicate? Existing bulk logs cannot answer this; echo-copy volume cannot be
extrapolated. Competing explanations are useful recovery, premature/repeated
copy work, or mostly non-copy native/shared-queue service. A cause label alone
is not a defect and an accepted copy is not necessarily a winning arrival.

Smallest action: one frozen feature-only observer build, no echo membership or
placement intervention. Reuse periodic perf counters for successful Original
payload, accepted copies by six existing cause groups×actual underlay, separate
requalification, and client receive new/duplicate/ordered-release bytes by exact
ingress. Receipt conservation is delta(frontier+buffered bytes); release is
triggered by this ingress, not proof it carried the unlocked suffix. Use checked
wide arithmetic and no added payload/range retention or batching changes.
No per-frame bulk logging or sampling;1us perf timing floors are not service.
Freeze/reverse before one existing healthy40s mixed DOWN diagnostic; enable
periodic counters on both ends and retain full service/profile/CPU/RSS history.
This is ordinary policy with observation cost, not ordinary performance proof.

Information forecast: substantial accepted tail copies with largely duplicate
arrival support inspecting their assignment-to-native-service timing; different
dominant causes select their existing owner instead. Small copy volume falsifies
copy service as the principal explanation. A conservative useful-copy bound
requires complete covering Original counters; no unflushed-tail assumption.
Wire carries no cause identity, so exact winning cause requires a subsequent
bounded exact-range join ONLY if aggregate results leave that decisive ambiguity.
No recovery suppression, controller/timer/threshold changes or model acceptance
from byte ratios. Expected gain is unknown; this cheap classification must choose
a material mechanism or stop it. Preserve old stall/restart/ownership fixes.

### Completed Original-placement discriminator (predeclared contract)

The same-build QUIC-only cell completes at428.149Mbps, echo median/p95/max
106.536/159.800/274.824ms and bodygap.104166s. All80 responses win on QUIC;
postwrite residence31/89/177ms versus mixed QUIC228/327/426ms. Stable exact
carrier/epoch; more useful load, not a low-load latency comparison. Q RTT
median101ms versus mixed293ms; physical DOWN backlog peak8.19 versus25.77MB.
Mixed context materially contributes. Membership alone does not remedy it.

Next question: does stopping new TCP bulk Original placement after a usable
QUIC attachment exists recover service while retaining mixed membership and
all ordinary feedback/repair machinery? Same frozen observer plus a feature/
env-only final-choice filter; same-build echo-injected control then echo-injected
placement intervention, unchanged healthy40s profile. This is deliberately
non-work-conserving and protocol-specific DIAGNOSTIC behavior, never a candidate
policy or RFC change. No config provides this isolated change: backup/bulk flags
also alter startup/ranking/repair. Root builds once, freezes/reverses, then runs.

Two source reviews establish the precise boundary: preserve the entire target
set through live counts, lead/FirstPath/AdditionalPath and resource admission;
filter TCP only from the final admitted choices, and only for Throughput when
current exact Regular QUIC is Active, nonstale, admission-active and normally
schedulable. Do not require Ready or a historical-use latch. Both preselection
and final fenced re-selection run the same rule. Existing source/binding/native/
writer/ACK waits remain; preserve pre-FINAL ceiling, old TCP debt, every repair,
feedback and echo Latency choice. Missing native shape is not usable QUIC.

Information/benefit envelope: roughly200ms median response residence separates
mixed from QUIC-only at comparable useful service. The diagnostic may remove
none or a material part, not guarantee200ms or a throughput multiplier. Lower
useful load, prolonged Q-only waiting, replacement or absent effective
intervention prevents a favorable inference. Normal ACK/MAX policy is unchanged,
but realized timing/volume can change as an effect of placement. Preserve those
costs. If new bulk Originals actually use QUIC at comparable load while latency
stays high, do not blame TCP Original allocation. If latency/service recover,
that supports a general allocation-model proof, not shipping this preference.
Actual per-underlay Original counts/bytes and exact echo ranges must verify
the mechanism, with no per-frame bulk log flood. No new threshold/controller.
The reused periodic perf recorder is explicitly enabled at the server in BOTH
cells (it was off in the preceding echo captures); sample-by-sample logging
stays off. Its new Original components count real commits/bytes only: their
1us zero-duration floor is not CPU or service evidence. Process-wide per-lane
counts may have an unflushed final tail; reconcile recorded intervals and keep
that scope. This observation cost is common to the new pair, not permission to
compare its Mbps as an ordinary gain or reuse the earlier binary as control.

### Completed membership and carrier-context discriminator

The actual echo-membership counterfactual completes: extra QUIC attaches104ms
after injection and wins45/80 responses. Winning postwrite→decode remains
QUIC median/p95/max228/327/426ms, versus TCP223/382/677ms in that run;
control TCP234/416/483ms. Source/claim/write/postdecode waits are small.
Whole echo median306→312ms does not improve; bodygap.305→.402s worsens.
Membership is not sufficient/dominant here; do not implement its forced QUIC
choice, ungate bulk rebalance or add a priority knob. Full pair and costs are
in ECHO_MEMBERSHIP_COUNTERFACTUAL_20260909. Ordinary source remainsb2aa215.

Next discriminator is one QUIC-only healthy40s cell using the SAME frozen
feature binary and existing client-quic.toml, flag unset; server,500/500Mbps,
30/70ms, queues, observer and body/echo workload unchanged. Reuse both mixed
cells above, no favorable rerun. Question: is winning QUIC postwrite residence
high generally in this implementation, or specifically when mixed TCP work
and its feedback share the service path? Historical ordinary QUIC103/191ms
echo median/p95 is context, not this same-build comparison.

Information forecast: if QUIC-only restores roughly configured-RTT echo and
roughly30ms response residence at comparable useful load, mixed context is
material; compare native flight/RTT, exact delivery and physical/service cost
before selecting an allocation/queue model. This removal changes TCP data,
control and membership together: it cannot isolate their separate effects or
justify assuming all paths share one bottleneck. If residence remains similarly
high, stop the mixed-specific inference and examine the common native service
owner. Failed/ambiguous joins or lower useful load remain explicit. No new
code, controller, parameter, queue size, runtime fix or public claim follows
this diagnostic alone. Independent source review confirms QUIC priority already
works before packetization and cannot overtake emitted shared-queue packets.

Joint ACK/MAX publication is REJECTED. Restricted163→238Mbps and27%less
return traffic did not satisfy timing: healthy reverse-order pair gives
413.444→402.332Mbps, echo p95/max467.854/606.069→561.990/813.918ms.
Median323.976→291.257ms and bodygap.390→.331s improve; these positives do
not erase the adverse tails. All160bins/four runs and costs are in
JOINT_FEEDBACK_20260909. No next QUIC/UP promotion run for this rejected trial.
The exact24-file source/RFC trial was frozen and fully reversed; restored1260
affected checks pass in3.33s after a warning-free1m27s build. The raw archive
passes integrity and byte comparisons. Current
ordinary implementation is againb2aa215. No controller/timer compensation.

Next question returns to the already-recorded ECHO_OWNER_SERVICE finding,
not another packetization attempt or new issue inventory. Its81winning echo
fragments use onlyTCP despite an active session QUIC carrier. Source→claim
max7ms, claim→write max1ms, postdecode delivery max2ms; postwrite→decode
median210/p95329/max461ms. A5120B stream never reaches startup h58400;
recurring rebalance is Throughput-only. This excludes an eligible-QUIC writer
rejection: QUIC was not a flow member. It does not prove QUIC would be faster.

Model transaction: inspect282b8e1's bulk-service intention,9ab3cbcb's preserved
stall recovery, and exact direction/membership/lifecycle ownership. Determine
whether ordinary low-volume service has a coherent opportunity-refresh
contract without BulkStriping misuse, inline open stalls, byte-volume gates
or protocol preference. Existing capture suffices for current reachability;
no new diagnostic build or policy edit before this contract is established.
Competing explanation: all carriers share loaded native/network queues, so
membership correction can save nothing. The461ms response interval is only
an upper removable envelope if a better eligible service really exists;
request-side and separate280ms pre-read waits cannot be claimed as its gain.
Hundreds of milliseconds per common echo justify this review; no Mbps promise.
Falsifier: no reachable membership omission or no better completion opportunity
defers a fix. A proved omission permits one real-model RED and a bounded
candidate, with sparse/bidirectional/startup/retirement controls and ordinary
timing. No universal clairvoyant ranking, new numeric knob, or release waiver.

### Completed joint-publication trial chronology (not current instructions)

Ordinary first pair completes13:53:restricted162.871→237.654Mbps, whole
321.300→348.730; echo p95/max582/1283→398/614ms,77→80successful/no failures.
Whole UP bytes fall26.98%, but longest body gap.580→.658s and median echo
268→307ms worsen. Healthy/restored echo medians and tails also rise. This
is mixed evidence; promotion stops, not a successful speed milestone.

Next one discriminator uses the already-planned healthy mixed pair, now
candidate→control order, same frozen binaries and40s workload; only NO_QOS=1.
Question: does timing cost recur without the return transition, or is it
confined to the transition/changed queue history? Profiles/counter scope and
all timing stay visible. Information forecast: recurring adverse healthy tails
reject this trial; a neutral result only narrows the cause, not acceptance of
the original gap. No favorable reruns, thresholds or controller compensation.
Independent audit agrees on this bounded question. No new issue scope.

The unchanged first-poll capture completes:73,914 nonempty ReadyOk,3 Pending
(42.845MB),1 closure error;42,638 empty calls excluded. ReadyOk99.9946% of calls
but97.7146% of offered bytes; mean34.329us/max16.195ms. Strict return-restricted
interior12,150 ReadyOk,mean34.732us/max5.856ms. Opportunity is common, not
uniformly negligible or proof of readiness at an earlier pre-ACK position.

WRITE_FIRST_POLL_20260909 retains all130 conserving perf summaries,40 bins,
69/69echoes with p95/max.989/2.046s, .722s body gap and11,576 return drops.
Diagnostic phase rates436.937→283.432→418.362Mbps are NOT an ordinary gain.
Feature build3m34, frozen77-line observer fully reversed before traffic; archive
integrity and byte comparisons pass. Production source stillb2aa215.

The final SCOPED_ACK_SERVICE_MODEL section predeclares the candidate forecast,
timing changes, protected444fb38 startup/blocked-I/O behavior, typed pair API,
independent fences/standalone retries, pressure2 and full writer ownership.
At most one record/associated packet can be saved per eligible ACK/MAX pair;
no Mbps promise follows from readiness or unsafe suppression. Current material
return pressure justifies one bounded prototype/ordinary comparison, not a new
timer/controller/fanout policy. Weak/adverse composition stops promotion.

The real publisher RED is confirmed after all ACK/MAX facts, grants, generation,
pressure and retry-deduplication checks:two envelopes instead of one. Build35.17s,
test ends at the exact final work assertion, not a semantic/setup failure.
Root caught and corrected an invalid fixture assumption about already-published
ACK retries before running it; that was not a Product failure. Evidence remains
JOINT_FEEDBACK_20260909.red.patch and the focused test log. Coherent prototype
implementation is complete in three disjoint owners:typed path command/writers,
symmetric publication/fences, and retained-write actor sequencing. Independent
publisher/actor/all-writer accounting reviews are complete. Review caught a
candidate-only stale retry: before current receipt is offered, retrying the
previous ACK generation could spend the only newly freed queue slot. The
prelude now retries only after receipt_offered; first-Pending rearm and the
existing startup/error service are preserved. This is not a shipped defect.
Both focused runs pass1267 checks, including encrypted transport and original
blocked startup. The first build's missing test import was fixture integration,
not Product RED. Two obsolete response publication wrappers were removed;
their four controls now exercise the actual production API. Ordinary release
build is active. Its remaining request publication wrapper is test-only and
semantically redundant; remove it if this trial is retained, rather than
rebuilding an otherwise identical comparator before the practical decision.
Next the same ordinary return
restriction; healthy mixed/QUIC and UP only if materially supported. All global
experience/recovery/aggregation/baseline gates below remain unchanged.

**Proof outcome, after kind separation:** no replacement implementation is
selected. Finite-horizon live-ledger scanning has sound positive/scope semantics
with predecessor lookup and exact acceptance cursor, but legal moving islands
defeat the snapshot's256-frame work bound; a64MiB span permits roughly131k
full chunks. Frozen snapshots instead can retain1MiB per attachment,4MiB for
the shipped set/64MiB at configured slot maximum, before overhead. Deferred
generations also trigger current full sparse catch-up, potentially restoring
already-removed history cost. ACK/MAX fairness and successor deadlines cannot
be inferred from one last-publication timestamp. Full proof/counterexample and
minimal actual caller plumbing are in SCOPED_ACK_SERVICE_MODEL's final section.
No added timer/snapshot/delta framework, numeric knob, RFC or runtime change.

The deterministic next decision is joint publication service, not implementation
of the failed stronger proof: retain independent return recovery while reducing
sent facts/copies with a credible sparse-state, memory/work and failure-delay
bound. First/sparse feedback must remain prompt. A concrete safe model must
resolve these constraints before RED/candidate comparison; do not turn each
constraint into a separate fix inventory. Current ordinary source isb2aa215;
all diagnostic policies and rejected batching are absent. Public promotion held.

**Kind separation complete12:23:** same-feature control/MAX-withheld/ACK-withheld
gives restricted169.929/243.267/296.144Mbps. All79/79/80echoes succeed, but
restricted echo maxima are1246.461/1175.848/470.844ms; neither is a safe policy.
UP remains9.6–9.8Mbps with queue pressure and zero drops in all three cells.
Whole UP bytes fall19.8%/23.2%; MAX restored mean is7.1% lower and ACK healthy
mean4.2% lower. Both kinds contribute; no exclusive owner or additive effect
follows. Every TCP data carrier still progresses. FEEDBACK_KIND_ABLATION_20260909
retains all120 bins and exact123 effective profile samples, costs and failures.
Independent audit passes; build3m34, frozen and source fully reversed before
traffic. Raw archive integrity/byte comparisons pass. Ordinary source isb2aa215.

Next is a finite publication-service proof, NOT implementation: immediate
feedback at exact receipt ingresses with independently serviced sibling
obligations; first/new/terminal and already-overdue after-idle service stays
immediate. Evaluate existing per-path PTO/2 timing with nonrenewing deadlines;
no new parameter or protocol preference. This can reduce busy duplicate work,
but a reverse-only failure can add a backup eligibility interval. Do not hide
that cost, promise all-condition improvement, or enlarge windows to absorb it.
The old obligation model covers the needed caller boundaries. Current proof
asks whether a frozen generation/high-water plus byte cursor can use monotone
current receive coverage without per-attachment snapshot copies or starvation.
Source audit must cover exact multi-ingress batches, retained partial writes,
ACK/MAX fairness and terminal membership; no independent issue expansion.
Information forecast: a valid bounded model with explicit failure cost permits
only a candidate decision and affected RED/recovery/timing controls. A coverage,
memory/work, wake or first/sparse-service counterexample rejects it before code.
No runtime/RFC edit, lab or further suppression policy is selected by this proof.

**Model review / next discriminator12:03:** the scoped queue result and source
audits are retained in SCOPED_ACK_SERVICE_MODEL's publication-alternatives
section. Deferred independent feedback necessarily adds latency under unknown
asymmetric failure, needs finite jobs/nonrenewing per-incarnation service, and
is not selected as a free optimization. Same-event ACK/MAX pairing also is not
selected: dominant client ACK is prewrite and MAX postwrite, while received
MAX intentionally bypasses ACK FIFO. No runtime/RFC change follows either.

The next small causal question separates the already-proven combined fanout
cost: same feature binary, env-unset control, TCP-MAX withheld only, TCP-ACK
withheld only. Reuse the existing one-file ablation with independent selectors;
preserve all data carriers, server behavior, generation state, profile and
receiver probes. Freeze and reverse before running. Information forecast and
falsifiers are in the model: a large one-kind benefit focuses that owner;
nonseparable/weak/adverse results do not justify a policy. These are unsafe
diagnostic interventions, never ordinary candidates. Root alone builds/runs.
No batching/timer/controller changes or public performance claims.

**Scoped queue result11:39:** feature observer builds cleanly in2m04, frozen
and fully reversed before one mixed capture. Ordinary runtime remainsb2aa215.
All160client/server queue summaries conserve counts; captured ACK range weights
and scope/bin counts reconcile. During strict8.175s actualUP10 interior, newer
same-kind work is pending at19.573%of ACKtakes and20.607%of MAXtakes, below
the old pre-scoped~31% assumption. Whole25.743%/28.958% is not the restricted
answer. All capturedACKs have one range; ACK/MAX encoding1.570/1.131Mbps.
Diagnostic phases436.312→202.112→396.835Mbps; .779960s bodygap,76/76echoes,
p95/max742.800/995.736ms. UP still9.680Mbps with205511–946665B queued,
zero drops. Timing is observer-affected and not an ordinary candidate result.

Qualification: take is into_parts, not successful native writing. The unflagged
remainder is not proved irreversibly native-owned; newer ordinals are neither
fact subsumption nor saved-wire estimates. Current measurements justify no
blanket last-ACK-wins map and no throughput forecast from20% alone. Standalone
queue replacement stays DEFERRED: it does not address most per-receipt fanout,
safe removable facts are unmeasured, and receipt batching's ordinary latency
failure forbids assuming all coalescing is harmless. Do not implement it as a
small apparently safe patch or compensate with estimator parameters.

Next bounded work is a publication MODEL, not runtime: separate prompt current
receipt feedback from independent per-attachment recovery obligations without
renewable deadlines or abandoning5e1ace67's blackhole protection. Reuse existing
native timing/eligibility, no protocol preference or new guessed constant.
Before selecting an implementation, explicitly cover exact server ingress,
both-direction blocked local I/O, per-incarnation pending scope/generation,
partial catch-up that cannot restart forever, new/terminal attachments and
unknown/asymmetric/failing return paths. Compute conditional traffic and delay
bounds and identify an actual consumer/caller counterexample for any necessary
change. Earlier ingress-prompt/deferred-backup concerns are prerequisites for
this model, not a new issue inventory. A model that needs a timing tradeoff
must state it; receipt equality alone is no longer a latency forecast.
The causal all-fanout ablation supports potentially material benefit, not this
model's safety. No RFC/runtime edits, new frames, timers or controller settings
are authorized until the full bounded obligation model and falsifiers are read.

**Next bounded discriminator11:22 — current queued feedback ownership:**
receipt candidate is rejected and archived at89911ab; ordinary source remains
b2aa215, with no new receiver policy. Existing scoped ordinary/header captures
still establish material mixed return cost; the unsafe fanout ablation gives a
causal benefit but cannot replace independent-path recovery protection.
Question: after scoped encoding, how much ACK/MAX work still sits at a
cancellable MPP queue boundary with newer same-kind work pending, versus being
already irreversibly owned by a native writer? Old26–31%queue overlap predates
scoped ACKs and cannot answer this. Current old-frame encoding and native/header
cost compete with cancellable duplication; no queue-latest implementation yet.

Reuse the exact existing three-file queue/codec observer, adapting classification
to wire14 ACKs (all independently meaningful), never treating scope=None/Some
as the old complete/incomplete replacement authority. Count queued/taken/drop/
pending conservation and newer same-kind work between unchanged per-stream
barriers. Keep MAX separate; ACK counts are only a generous opportunity bound,
not proof of subsumption or saved bytes. New scoped ACKs can omit older facts.
No payload retention, Product decisions, timers, wire format or limits change.
Freeze the feature build, preserve exact patch, then reverse all observer source
before one identical mixed return-restriction diagnostic run. Diagnostic timing
is not an ordinary comparison. Root alone builds/runs; no compile/load overlap.

Information forecast/falsifier: small current overlap defers queue replacement
without spending a model/implementation loop; material overlap permits only a
scoped-fact/ownership proof and quantitative cost forecast, not acceptance or a
promised speedup. Observer overhead, invalid conservation or scope ambiguity
limits that inference. No extra repeat, controller tuning or replay of rejected
ready-feedback/receipt batching follows merely from a high counter. Preserve
the diagnostic full timing/failures/cost alongside the counters. This remains
the same existing mixed feedback/stall issue, not a new SEEN/UNSEEN inventory.

**Receipt correction REJECTED11:15:** reversed-order healthy control→candidate
406.710→412.076Mbps does not justify bodygap.273264→.385985s, p95echo
466.176→567.908ms, maximum523.741→842.107ms (80→79successful exchanges).
Both healthy pairs worsen these tails despite different execution order.
All four receipt helper/actor/test diffs and nine RFC lines are now reversed;
`git diff b2aa215 -- src RFC.md Cargo.toml Cargo.lock` is empty. The exact
candidate patch/binary, original RED,1244checks and all8ordinary runs remain
evidence, not an implicit dependency. UP was not started; no release/README.

Why the forecast failed: removing publication work did reduce whole UP bytes
and improved restricted service, but byte-coverage/FIFO equivalence did not
prove equivalent ACK timing, sender observation or placement. Healthy queues
and repeated tails become worse, so this is not an acceptable latency tradeoff.
Reverse candidate's late echo sample has a request not yet at the real target;
first pair had a reply already echoed but not delivered. A single downstream
queue explanation is insufficient. Exact causal chain for the extra tail is
not proved; do not disguise it as a confidence/BBR or physical-loss defect.
Source audit identifies actual ACK-transaction-dependent delivery sample counts
and first-clock seeding, but neither establishes this interval's active cause.
No compensating sampler/threshold adjustment is authorized.

Current ordinary runtime is scoped ACK checkpointb2aa215. Preserve this result
as a rejected bounded approach; next question remains material feedback cost
and mixed timing, not a new lifecycle inventory. The old queue-overlap fraction
cannot be imported into the scoped model. Any renewed queue-latest proposal
requires current cancellable-work evidence and exact scoped-fact ownership.
Restored ordinary test build and1241affected checks pass. All8runs,61raw files,
exact rejected patch and RED/GREEN/rollback logs are archived in
READY_RECEIPT_ORDINARY_20260909.raw.tar.gz; gzip integrity and byte comparison
pass. No diagnostic source or receipt candidate remains in runtime.

**Healthy attribution/repeat11:10:** sampled candidate DOWN queue32.138→
27.390MB brackets the worst echo; all four native RTTs rise from about300 to
500ms while ACKs progress. UP queue remains49–140KB/500Mbps, no drops. At
the late sample the server has echoed1728B but the client has only1664B; at
least the late hold is downstream of the real echo target. Control's worst
echo accompanies smaller13.821→8.796MB DOWN queues. Whole-queue Q/C is not
the exact echo delay and these snapshots cannot attribute the queue to the
receipt correction rather than the pre-existing mixed allocator/native state.

One predeclared reversed-order healthy pair (candidate then control) answers
that variability question; preserve BOTH pairs, not replace the adverse one.
No source, profile, queue, load or benchmark changes. Forecast is information:
repeatable candidate tail/queue worsening supports rejection of this correction;
changed ordering/sign with queue episodes in both builds weakens that causal
claim but does not prove global non-regression. This repeat is finite, not
rerun-until-green; inconclusive evidence retains the hold and selects exact
critical-event evidence rather than more repetitions. UP remains deferred.
The queue-latest shadow remains deferred meanwhile: its old26–31% overlap
cannot be reused after scoped ACK/receipt changes, and newer scoped ACKs are
not necessarily supersets. No queue replacement code is authorized.

**Healthy mixed stop11:07:** control→candidate whole393.331→392.565Mbps,
bodygap.310480→.331440s; echo p50/p95/max280.491/492.308/817.280→
286.590/528.341/1042.248ms. Both79/79 actual successes. Candidate's worst
echo is13.035326→14.077574s, not startup; no configured impairment exists.
This is an adverse/ambiguous affected result. Stop promotion, and defer the
planned UP pair until one causal attribution question is answered. Do not
replace this pair, waive the tail, or stack the deferred queue-latest design.
First reuse ordinary native/queue/timing samples around the two slow echoes:
does a newly dominant sampled queue accompany the candidate hold, or is the
existing mixed placement/tail behavior unchanged? These samples cannot prove
the exact blocking byte's location; acknowledge that limit before adding any
observer. Component correctness and return-QoS improvement remain evidence,
not practical acceptance or a reason to silently retain a latency tradeoff.

**Q-only affected control11:03:** candidate-first then fixed control under
the same return restriction. Control→candidate whole428.634→431.675Mbps,
restricted441.383→443.261; restored438.912→438.317. Both80/80echoes; p95
160.788→168.332ms, max318.800→287.649ms; bodygap100.443→106.381ms, firstbody
408.391→407.006ms. Small contrary extremes remain in raw evidence; no material
Q-only service loss appears in this pair. Proceed with the predeclared healthy
mixed DOWN pair, not a repeat of the favorable restricted mixed result.

**Receipt ordinary outcome11:01:** fixed control versus default corrected
build gives restricted235.091→299.175Mbps; healthy433.747→433.185 and restored
367.121→370.929Mbps. Whole344.436→360.721Mbps. Echo p95/max746.712/969.400
→541.200/706.855ms,75→79successes with no actual failures. Firstbody.587367
→.581723s; longest read gap worsens.581397→.622215s (candidate21.863670→
22.485885). Preserve the41ms adverse extreme; do not infer non-regression or
stall closure from one pair. No new threshold or favorable replacement control.

Continue only the predeclared affected controls: Q-only DOWN with identical
return restriction (candidate then control), healthy mixed DOWN and healthy
mixed UP (control then candidate). No source/profile edits between each pair.
These test unintended Q-only receipt cost, ordinary mixed service, and the
server-side receive/error composition and upload settlement respectively.
Forecast is neutral or useful service, not a required speedup in every case;
materially worse completion/gaps/loaded latency stops promotion and selects one
causal question before further cases. The current modest worst-gap increase
stays visible in the final pair report; it is neither proof of regression nor
waived by better means. Raw queues/resources still require the same-window join.

**Receipt verification10:57:** the corrected ordinary test build passes1244
affected protocol/mux/relay/stream/sender/path checks, including the original
four-ready-record RED and real later-range rejection. Independent final review
finds no concrete regression in either caller, prefix error handling, FIFO
metadata, duplicate/hole/FIN semantics, or unchanged all-attachment feedback.
This establishes the receipt mechanism, not practical performance. Default
release compilation now precedes one identical-profile candidate comparison
against the fixed control below; no compiler overlaps a lab. Runtime scope
remains four receipt helper/actor/test files plus the RFC clarification.

**Comparator outcome10:48:** ordinary b2aa215 completes40.000098s,
1722184258body bytes/344.436Mbps. Phases433.747→235.091→367.121Mbps;
firstbody.587367s, maxgap.581397s at16.788212→17.369609s;75/75actualechoes,
p50/p95/max254.092/746.712/969.400ms. This comparator is fixed for the
ready-receipt correction; do not substitute an earlier slower control.

**Ordinary comparator10:46:** while the focused fixture additions finish, run
frozen ordinary b2aa215 once under the unchanged mixed return-restriction
profile, tag ready-receipt-control-0909. No compiler overlaps. This supplies
the declared correction's ordinary comparator; the earlier same-feature
ablation pair remains diagnostic evidence, not its acceptance control.

**Receipt RED10:40:** the actual collector/receive/two-attachment test runs
one test and fails exactly at batch length1 versus4 on unchanged runtime.
The earlier compile command selected zero tests because of an unqualified
exact filter; that log proves compilation only, not GREEN. The corrected
execution is retained in ready-receipt-red-test-0909.log. Implementation is
now authorized only within the four receipt-helper/actor/test files.

Receipt proof: let R_i be admitted coverage after original FIFO input i and
d_i its contiguous delivered frontier. The existing atomic receive operation
validates before mutation, monotonically extends R_i, and returns only the
new contiguous prefix [d_(i-1),d_i), never duplicate bytes. Inductively, applying
a finite ready batch and concatenating its returned chunks equals sequential
application of the same accepted inputs. On the first rejection, retain that
accepted prefix and the original error; later inputs have no surviving Product
obligation after terminal failure. A single scoped ACK formed from final R_i
preserves all new positive and truthful negative facts; the existing independent
per-attachment publication/catch-up rules remain unchanged. Local delivery may
release old reorder storage in addition to new input, so its byte envelope is
prior retained payload plus the unchanged bounded input, not input bytes alone.
ACK transaction granularity and observation timing can change; neither identical
rate samples nor a throughput gain follows solely from this equivalence proof.

**Selected bounded correction10:34 — ready receipt, not delayed fanout:**
the same-feature ablation identifies materialreturnfeedbackcost (below), but
its unsafe single-return path is not a fix. Source/origin review exposes an
older, narrower constraint:3353d7d introduced contiguous-only ready batching
for vectored application delivery. Both roles disable batching if any reorder
data exists; even with no priorbuffer, firstoffset mustequalreceivefrontier.
This is not required for safely applying FIFO inputs to the real receive map.

Existing mixed receive-owner trace0908 seq152–175 contains FOUR alreadyqueued
12000B QUICframes behind a TCPgap, each followed by separate feedback/write
setup; all48000B fit the unchanged default512KiB receipt-turn payload bound.
23540async dequeues also see queueditems+priorreorder, but those older diagnostic
counts are only opportunity observations (controls may interrupt readyqueues),
not predicted batch counts or Mbps. The actual scopedproducer retains every
new fact across one materialization, including merged earlypositives.

Model: collect already-ready same-stream Data in existing FIFO/item-snapshot/
aggregate-input-byte bounds regardless of its offset relative to the delivered
frontier. Retain geometry/credit/FIN/error/control boundaries and exact ingress;
apply every real frame through ReliableRecvStream, never pre-sort or bypass
validation. Application output is still only the receiver's contiguous prefix.
No new wait, timer, resource size, ACK cadence/fanout or native/ranking change.
Client keeps pre-application ACK and one retained partial-write future. A later
apply failure becomes reachable: publish accepted prefix facts, deliver that
prefix once, preserve I/O-error precedence, then surface the deferred error.
Server retains its existing publication/I/O ordering; do not stack a different
server interlock/cadence policy into this receive-eligibility correction.

Forecast: the concrete4frame opportunity reduces4publication turns to1 while
retaining all four feedback recipients; larger gains require actual readywork.
Given the proven TCPfeedback burden and ample captured readywork this can be
material, but nativequeues/placement may still dominate and no Mbps value is
promised. Zero benefit or worse first/recovered/loaded timing rejects performance
promotion. Tests must first RED on the actual collector/receive/publication
path, then cover sparse/duplicate/hole-fill, size/item/control/FIN boundaries,
later resource rejection, prefix ACK/write, partial I/O and chunk release.
Build/run only one coherent implementation; ordinary paired mixed returnQoS,
then Q-only/healthy mixed and opposite-direction controls if useful. No README
or release acceptance. Preserve full series/failures/costs, not mean alone.

**Causal outcome10:31:** same-feature/env-unsetcontrol versus clientTCPACK/MAX
withholding: restricted266.246→399.106Mbps; bodygap1.242532s→.390762s(startup);
worstecho2.405033→.803287s(startup), restrictedecho max273.765ms;71→80successes.
Returnclass75.293→26.278MB,16508→0drops. Interiorreturnservice9.602→5.395Mbps.
TCPData remains productive (servernativeTCP ACKeddelta132.402→275.034MBinside
restriction); only clientTCPreturnfeedbackfalls4.285MB→2016B. Restored mean
387.452→390.184 does NOT fix all performance. BothpartialHTTP200bodies.
This proves a materialcontributor, NOT safetyofselectedQUICfeedback or identical
placement histories. Temporary30add6del diff is frozenandfullyreversed; working
runtime stillb2aa215. Do not implement deferred-backup machinery at this point.

**Next causal ablation10:19:** zero-drop header evidence attributes76.81% of
restricted observedIP bytes toTCP carrying payload,20.95%UDP,2.24%TCPnondata.
History5e1ace67 proves that selecting one locally-accepting feedback path once
caused7–14s blackhole stalls; no rollback of that protection is selected.
Before implementing a replacement publication model, one feature-only diagnostic
will withhold client ACK/MAX fanout from TCP while keeping all threeTCP+oneQUIC
data attachments and the identical500/10/500return profile. All QUIC feedback
generations/catch-up remain unchanged. This is an intentionally unsafe causal
ablation, NOT a proposed single-path policy or release candidate. No blackhole
is introduced in this cell; no inference of recovery safety is permitted.

Question: does removing the redundant small TCPfeedback copies materially
restore restricted ordered service/latency, or do native/local/forward queues
still dominate? Conditional information forecast: if those copies dominate,
restrictedservice should move toward existingQ/raw/H2~440–476Mbps andreturn
queue/drop burden shrink; unchanged stalls reject fanout as a sufficient fix.
Removingobservedcost cannot be converted into an exact gain forecast because
forward work andnativepacket rates change. Preserve every timingbin, failure,
first/restored phase andCPU/RSS; ordinaryacceptance remains held regardless.
Use one small feature/env-gated source diff, freeze executable, reverse all
source before running. No change to thresholds, data placement, CC or profile.
Run one same-feature/env-unset control immediately before the env-set ablation
to separate compiled diagnostic cost from the intended intervention. Existing
ordinary/encoder/header runs remain context, not substituted matched controls.

A possible ingress-prompt/deferred-independent-backup model is NOT selected:
independent audits expose server loss of exact ingress metadata, globally renewed
timers, blocked-write wake requirements and partial cumulative catch-up restart.
Do not implement those lifecycle pieces until causal gain justifies their scope.
Any eventual model must retain coverage, terminal/new-attachment service and
blackhole-safe independent delivery; a deadline alone proves none of these.

**Header outcome10:16:** same ordinarycandidate with externalobserver gives
345.206Mbps whole,235.631restricted,391.113restored; .989s bodygap and2.496s
worstecho,70/70actualsuccesses. Interior8.006s capture hasnoobserverdrops:
TCPdata6.880MB, TCPnondata.200MB, UDP1.877MB. MostTCPdatarecordsare96/97B.
OffloadedSKBs reach14548B; totalsareobservedIPrecords, notphysicalwirepackets.
All1501observerdrops crossrestoration, excludedfrominteriorattribution.
ActualUPclass has9987drops and1.480MBpeakbacklog. Pre-restriction1.410s echo
and34.310MBDOWNbacklog showreturncostdoesnotexplainalllatencyspikes.
Report/rawarchive retain both outcomes; no batching revival or acceptedfix.

**Header discriminator10:06:** same persistentTCPtuples show ordinaryUP
25.409MB/532015datasegments (~47.76B each), versus56095nondata segments;
restricted8.1Kdata versus1.14Knondata segments/s. This rules out pureTCPACKs
as the solecost, but doesnotmeasureUDPcontribution or justifyrejectedbatching.
Run one ordinarycandidate identicalprofile with header-only AF_PACKET observer
atservereth0 ingress AFTERroutershaping. Count exact observedIPv4 lengths by
TCPdata/nondata/UDP, timestamps andobserverkerneldrops; no packetcontents.
Interface/source/targetports restrict toownedlabcarriertraffic. Capture keeps
offloadunchanged, so countersdescribeobservedIPSKBs, not claimedphysicalwire
segments.45s capture spansunchanged40sload; no Productthreshold/configchanges.
Forecastinformation: whichtransportdominatesremainingreturnbytes/packets,
andwhethermanytinydata-bearingrecordsarethematerialowner. Observerdrops or
unaccountableoffload weakenexactclaims; lowTCPshare rejectsTCP-onlyfixes.
Rawcapture socket is availablewithoutnewprivileges; tcpdump absent, use small
stdlibheadercounter, notnewlabframework. No runtimefixselected.

**Attribution10:00:** interim model checkpointb2aa215 committed with1241checks
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

## Earlier comparators and retained ordinary dispositions

Historical comparator **d999fea** restricts request ACK-release work to exact support;
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

- Root owns builds/labs. Joint-publication and ready-receipt trials are REJECTED and fully
  reversed as recorded at the top. Scoped ACK is checkpoint b2aa215 with
  partial ordinary benefit, not acceptance. Feedback fanout's causal ablation
  is evidence only; its unsafe source has been fully reversed. The earlier
  MAX/claim/work comparisons above are historical, not parallel open tasks.
  Owned Docker only: no sudo, host shaping, outside-repo work or build/lab overlap.
- Next ordinary comparator:
  `./.tmp/reflection/bin/scoped-ack-20260909/mptunnel` (b2aa215).
  Previous pre-scoped comparator:
  `./.tmp/reflection/bin/latest-credit-actor-yield-20260909/mptunnel` (4c7e232).
  Frozen diagnostic:
  `./.tmp/reflection/bin/feedback-fanout-20260909/mptunnel`; its env-set wrapper
  is an intentionally unsafe causal intervention, never an ordinary candidate.
  Rejected logical-cadence trial8a0413d is frozen only at
  `./.tmp/reflection/bin/ack-cadence-20260909/mptunnel`. Its123s trial build and unused
  helper warning are historical; reversing the trial removes that warning's
  cause. No trial cleanup or associated tests remain implementation obligations.
  Prior ordinary trial0cab2b5 is frozen at
  `./.tmp/reflection/bin/confirmed-return-20260910/mptunnel`; current364d417
  ordinary is `./.tmp/reflection/bin/return-round-20260910/mptunnel`.
  `target/release/mptunnel` currently contains the feature outage observer,
  also frozen in `./.tmp/reflection/bin/return-round-outage-observer-20260910/`.
  No cleanup or source change occurred between the six new ordinary cells.
  Do not confuse diagnostic and frozen ordinary executables. Protocol trial is wire15; use matched
  binaries at both endpoints, no mixed-version compatibility assumption.
- Exact intermediate commits only; preserve raw evidence before scoped cleanup.
  No deletion in this condensation. User's seven-line
  LIVE_OWNER_FRONTIER_WORK_BOUND.md edit must remain untouched and unstaged.
- Telegram latest measured return-round/outage report sent approximately2026-09-10 04:06UTC;
  next nonurgent not before05:07UTC. Respect hourly minimum/soft-frequency advice; no component-only
  success notification or unfinished completion claim.
- Method reflection: symbolic conservation justified exact work removal but
  did not predict every timing phase. Follow the same winning-prefix evidence,
  retaining adverse first-service, settlement and resource observations.
  No plausible shortcut stack, favorable-average rerun or silent coverage waiver.
- Universal clairvoyant optimum under arbitrary future outages is impossible;
  this does not waive avoidable delay or practical gates. Never call unfinished
  work ideal or promise cost-free capacity discovery.
