# Current deterministic closure plan

Updated: 2026-09-11 05:40 +08:00. Authoritative repository: ./.
**No performance/release acceptance, push or public README update.**
Continue the authorized closure loop; an intermediary commit is not completion.

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each new
transaction and after compaction. This is the active ledger, not a second RFC.
Complete earlier forecasts, failed experiments and attribution are preserved in
`git show 011b724:docs-dev/CURRENT_CLOSURE_PLAN.md` and
[the full evidence report](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md).
Condensation waives no failure and reactivates no rejected candidate.

## Current source and proven correction

Runtime checkpoint **011b724**, finite ordered ACK/MAX Input service, is retained.
Frozen ordinary binary: ./.tmp/reflection/bin/ordered-feedback-20260911/mptunnel.
target/release/mptunnel is the frozen server-ACK-admission diagnostic, NOT ordinary.
Use the explicit ordinary path above. All observer runtime edits are reversed.
Only the user's seven-line LIVE_OWNER_FRONTIER_WORK_BOUND.md is unrelated
dirty source; never edit/stage it. The temporary two-file ACK-admission observer
is fully reversed; build44306 and lab80295 both CLOSED0. No runtime
policy changes are currently proposed.

Exact failure: ordinary a16 mixed UP had7.236s write/4.145s confirmation gaps.
Diagnostic61091 joins an ACK prefix already in client FIFO to actor processing
12.526s later; decoded-to-Apply16.659s. Server generation/admission/successful
native write are timely for the critical prefixes. Repeated client preselect
occupies11.277s of a12.121s full-flush window within the FIFO residence.
Reader/attachment backpressure can propagate upstream; this is not an
independent native/physical fault. Elapsed scopes overlap and are not CPU.

Origin/intention: ccfe817 centralized coherent ACK/state transitions;
faee89db retained inline recovery;79ddb41 expanded gap enumeration to cover real
omitted successors. Per-event discovery plus repeated preselect can postpone an
already-ready later positive ACK while doing work that it immediately invalidates.
The useful ownership guarantee is preserved at the finite Input quantum boundary:

- Keep every ACK's validation, positive release, negative scope, copy qualification,
  sampling, pruning, staleness and progress in the existing returned order.
- Apply actual coalesced MAX credit; snapshot one additional-attempt count.
  Do not wait, replenish the budget, merge ACKs or freeze credit contents.
- Retain other frames/errors/mismatched streams as exact barriers.
- Hold one Product guard; after novel ACK facts do fresh recovery before prepared
  publication, FIN decisions and unlock. Existing native notices can claim upon
  unlock, so postponing publication alone is not sufficient.
- Fatal feedback revokes existing prepared claim authority under that guard;
  terminating streams need no speculative recovery. Cleanup remains unchanged.
- Keep independent preselect/capacity/model/membership/deadline service.
  No new rate, congestion, queue, timer or preference parameter.
- Finite does not mean short: one cooperative turn now covers multiple full ACK
  transactions. Sampling/commit interleaving and initial timing observation can
  change. No wall-clock or timing-equivalence claim. Frozen assignment minima
  and exact native precommit checks remain.

Real-actor RED19106 uses actual native Original claims, receiver-generated ACK1
with a real gap and ready ACK2 filling it. Three intervening heavy discoveries
become zero. Exact readiness, both facts, intermediate gap and final release
are asserted first. Initial44817 was a fixture failure: cumulative full[0,A)
ACK2 correctly omits an empty negative-scope header; no runtime failure there.
GREEN27151 passes52distinct checks (33control,12client,5service,1splitACK,1copy).
Independent actual-source reviews pass, including fatal claim revocation.

## Ordinary evidence and current acceptance boundary

Ordinary build61255 closed0 in1m04s, pre-existing unused-wrapper warning only.
No diagnostics or compile/lab overlap. Exact six-file runtime/RFC/test patch:
./.tmp/reflection/ordered-feedback-candidate-0911.patch.

| Same200+200Mbps QoS UP cell | a16run23406 | 011b724run95903 |
|---|---:|---:|
| Exact confirmed bytes | 526385152 | 1091108864 |
| Elapsed,s / Mbps | 45.465808 /92.621 | 42.660189 /204.614 |
| Maximum write / confirmation gap,s | 7.235997 /4.145083 | 1.162112 /1.542599 |
| Raw bins / zeros | 46 /11 | 43 /1 |
| Raw pre-cut / strictcut / restored,Mbps | 96.384 /104.288 /83.343 | 225.303 /184.959 /205.211 |

All candidate adjacent target-write samples advance; earlier a16 had5s plateau.
First service essentially unchanged. Observed UP/DOWN wire per confirmed byte
falls29.88/21.27%; differing sampled windows are not exact lifetime amplification.
More useful work raises absolute lifetime CPU (clientfinal123→174% ofonecore,
server37.8→68.7%) and server peak RSS75776→126644KiB. No exact CPU-per-byte
claim from lifetimeps. Retained reverse native RTT is also higher; this UP probe
has no echo and cannot establish loaded latency. Preserve residual1.543s gap,
one zero bin, resource costs and native timing. This is material bounded support,
not optimal aggregation or release acceptance.

[Ordinary archive](ORDERED_FEEDBACK_ORDINARY_20260911.raw.tar.gz):
15regular files/249942B, every input byte verified by reader; root read179-line
appendix and verified integrity/manifest. Includes failed fixture, trueRED,
GREEN, exact patches/build/driver/results/runner/shape, no config or executable.

### Closed healthy independent-link pair

Predeclared a16control7360 then candidate91118, same cell but NO_QOS=1.
Tags ordered-feedback-healthy-{control,candidate}-0911.
Both1/1complete,0errors; every accepted byte confirmed.

| Healthy UP | a16 | 011b724 |
|---|---:|---:|
| Confirmed bytes | 526188544 | 1180565504 |
| Elapsed,s / Mbps | 41.740160 /100.850 | 42.766993 /220.837 |
| Worst write / confirmation gap,s | 2.002279 /4.587237 | .722869 /.532616 |
| First write / confirmation,s | .108411 /.411490 | .107294 /.410305 |
| Raw bins / zeros | 42 /8 | 43 /0 |

Independent preliminary review supports advancing: all raw phases improve;
candidate42/42 adjacent target samples advance, versus control5s and3s plateaus.
All rates200Mbps, UP70/DOWN30ms, no loss/jitter/QoS/outage or drops.
More work takes1.027s longer to settle; absolute CPU rises(clientfinal125→193%,
serverpeak50.6→80.9%). ServerRSSpeak87984→78580KiB; client346152→349380KiB.
Wire rises less than2.244xcompletedwork. Full paired native/cost/series appendix
and ORDERED_FEEDBACK_HEALTHY_20260911.raw.tar.gz are complete:148lines read by
root,14regular files/441963B, integrity/manifest and every input byte verified.
Mean220.8Mbps remains below nominal400; do not call this theoretical optimum.

## Next exact gate: shared500Mbps, both directions and loaded latency

Issue/question: does the finite Input quantum preserve ordinary single-cut
mixed service and competing short-request latency? Independent UP has improved,
but carries no echo and cannot validate the higher reverse native RTT observation.
No new runtime change is authorized from that scalar alone.

Forecast: preserve healthy shared-cut service while reducing avoidable queued
feedback work where present. No gain is promised when backlog is absent.
Larger synchronous quanta may harm tail latency or constrained-host headroom.
These are practical acceptance checks, not a forecast based only on call counts.

Smallest next action: four ordinary cells, sequential and predeclared:
a16UP,011b724UP,a16DOWN,011b724DOWN, system mixed/scenario combined.
Use single500Mbps, mirror=1 (UP70/DOWN30ms), NO_QOS/NO_LOSS/NO_JITTER/
NO_BLACKHOLE=1, management=1; no router/diagnostics or per-role override.
Tags ordered-feedback-shared-{control,candidate}-{up,down}-0911.
DOWN reuses the existing bulk+64B echo probe every500ms with3s timeout;
UP reuses the exact-confirmed single-upload probe,40s offered, same guards.
No new harness, queue/profile/controller change or build; both binaries exist.

Compare complete series, startup, all gaps/failures, settlement, native/service,
loaded DOWN echo distribution and wire/CPU/RSS. Preserve failures/censoring.
New material adverse/ambiguous timing or resource cost stops promotion and
selects one causal discriminator, not a favourable rerun or quota adjustment.
Only after this bounded gate proceed to the existing changing-impairment/baseline
matrix. A checkpoint is not authorization to skip any global gate.

Execution03:53+08: shared500 UP pair CLOSED0: control39206 exact428146688B/
45.523054s=75.240Mbps, candidate28579 exact1896284160B/41.853765s=362.459Mbps.
Worst write/confirmation1.489/8.879s becomes .404/.708s; raw zeros24/46→0/42.
All accepted bytes confirmed, no upload errors. This is bounded practical gain,
not final acceptance or a claim that short confirmation bins exceed link capacity.
Control-DOWN75855 and candidate-DOWN2857 also CLOSED0. Bulk366.522→397.802Mbps,
but echo median225.620→268.514ms,p95424.413→530.808ms,max535.156→581.266ms;
body maximum read gap .207720→.372282s. Both80/80successful echoes, no failures.
This adverse/ambiguous timing STOPS promotion; do not advance the matrix or
compensate with a queue/controller/quantum parameter. SharedUP sampled wire per
confirmedbyte also rises4.06%UP/18.18%DOWN; absolute CPU rises. More useful work
does not waive either cost. All four closed files are with the independent reader.

Next bounded attribution transaction: separate larger synchronous client Input
service from shared native/allocation variation and higher offered bulk load.
The correction directly handles client request ACK/MAX; DOWN bulk response ACK
processing is a different unchanged owner. Existing short requests may still
exercise the quantum, so do not assume it irrelevant. Root/independent reviewer
first trace actual request/response callers and exact adverse intervals using
the existing four captures. Information forecast: identify whether a current
owner observation is sufficient, or select ONE causal discriminator. No new
runtime proposal, quota change or diagnostic build.

04:01+08 decision: ONE order-reversed ordinary DOWN pair, candidate then control,
same500Mbps/70+30ms/zeroimpairment/40s bulk+echo and existing binaries. Tags
ordered-feedback-shared-reverse-{candidate,control}-down-0911. Independent source
review confirms high-volume response ACKs use unchanged server ServerFeedbackBatch;
the new client handler receives the one HTTP request's and80x64B echo requests'
feedback. A following echo DATA barrier can still wait, so direct effect is not
ruled out. Information forecast: distinguish repeatable adverse ordering from
run-order/native-allocation variation. This pair cannot separate a causal higher
offeredload effect from direct quantum cost. Preserve BOTH orders; no repeated
sampling until favourable. Repeated adverse timing selects an actual echo-stage/
shared-native discriminator; disappearance holds the regression attribution,
not proof of exact latency equivalence. No runtime change or build.

Reverse pair5956/7714 CLOSED0. Candidate404.090Mbps versus control377.547;
echo median348.040/240.086ms, p95526.798/342.481ms, max788.622/402.024ms.
Candidate79successful/0failed, control80/0; no censored failed attempts.
Body gap .274600/.346357s reverses ordering, so body-gap regression is not
repeatable here. Echo harm DOES repeat; no more ordinary repeats are selected.
Initial pair's larger native RTT and shared HTB backlog coexist with the echo
harm, but neither is an exact winning-byte timing join. Next source reviewer
is selecting the smallest actual echo/client-Input residence discriminator;
do not claim either local quantum or the network caused it from these scalars.

Selected next observation,04:07+08: a cfg-only, one-file client ACK/MAX quantum
observer on current011b724. Verify the actual target port10022 echo and8080 bulk
request streams; record start/guard-acquired/post-unlock end, entry ready count,
actual ACK/MAX and novel ACK counts, and existing deferred barrier metadata.
One log after unlock per quantum; no waits, policy, new queue or per-frame logs.
Include fatal/early-exit coverage or explicitly retain its censoring. The actual
clock is elapsed ownership/residence, NOT CPU attribution. Compare conservative
SUM/UNION across each adverse echo interval, not only the maximum single call.
Information forecast: if even all potentially blocking quantum occupancy is
far below the added100ms-class delay, direct synchronous batching is not its
dominant explanation. Large occupancy selects that exact owner instead. Neither
outcome resolves decoded/FIFO waiting or indirect allocation by itself. Reuse
one unchanged shared500 healthy DOWN cell with the observer, freeze exact patch/
binary and reverse source before running. Ordinary pairs remain the performance
evidence. No old membership injection or12-file observer transplanted.

04:12+08: source observer is one file,255insertions/1 textual brace rewrite;
cfg-stripped ordinary semantics unchanged. Root starts the witness before
ready_frame_count, so pre_guard_us includes that capture plus Product wait;
held_until_post_unlock includes release/notification. Actual deferred-slot
take/censored actor exit is scalar-tracked; monotonic durations are separate
from approximate Unix alignment. Independent actual-diff audit PASS.
Root sole feature build92718 running,4cargo jobs, no lab overlap. Exact patch
./.tmp/reflection/feedback-quantum-observer-0911.patch; build log same stem
with -build. Freeze bin/feedback-quantum-observer-20260911/mptunnel on success,
reverse all observer source before one same shared500 DOWN diagnostic capture.

Reuse the existing direct_echo_context.py alongside that capture:64B raw TCP
echo every500ms across the SAME active47 cut. This adds a tiny diagnostic load,
not an ordinary performance comparison. Its existing start wall timestamp and
all50s records distinguish coarse common-queue delay from tunnel-only delay;
pre-load/post-load records stay visible. It does not identify the winning MPP
copy's exact native residence. No new harness, source/controller tweak or
additional shaping is needed. This cheap context avoids guessing network delay
from global queue/RTT values if local quantum occupancy proves too small.

04:15+08 execution: build92718 CLOSED0 in1m23 (one existing unused-wrapper
warning), exact patch comparison passes. Frozen diagnostic binary exists;
ALL observer source reversed, runtime011b724 clean. target/release/mptunnel
is now diagnostic—do NOT use it for an ordinary comparison by assumption.
Diagnostic lab96888 and direct-echo18374 running, no compile overlap. Tag
feedback-quantum-shared-down-0911, event client_feedback_quantum only;
direct-echo-feedback-quantum-0911.jsonl preserves the separate wall start.
Both sessions CLOSED0. Diagnostic408.986Mbps; MPP echo76/76successful,
p50/p95/max359.938/716.686/934.747ms. Exact206events:133completedquanta,
73consumeddeferreditems, no censored. Across both streams allquantumelapsed
sum1.766ms (max84us), held1.716ms; all deferredsum15.072ms (max3.777msProbe).
Fourteen DATA barriers total1.461ms,max216us. Overlap is NOT additive CPU.
This is too little direct changed-boundary residence to explain100ms-class
echo harm; do not shrink or remove the quantum from that causal hypothesis.
Unchanged preselect/native residence and indirect allocation remain distinct.

Existing raw companion100/100succeeds. Safely interior direct-offset3–39s:
72echoes p50/p95/max361.195/528.898/570.255ms. Postteardown43–50s returns
to100.226/100.249/100.254ms. Approximate wall anchors support those conservative
bands, NOT exact request pairing or quantile subtraction. Common loaded-link
queueing is demonstrated; extra MPP delay remains unassigned. Reader is archiving
all data and checking timing/costs; no deployedCPU attribution follows.

Next predeclared context ablation,04:20+08: ordinary current011b724 QUIC-only
then TCP-only DOWN, same shared500/UP70DOWN30/zeroimpairment/40s bulk+echo as
the original ordinary mixed pairs. Reuse those TWO mixed candidate outcomes;
do not rerun mixed to get a better tail. No extra raw companion in these ordinary
cells, no build/controller/queue/parameter change. Tags
ordered-feedback-shared-context-{quic,tcp}-down-0911.
Information forecast: if single-mode tails remain much lower with comparable
useful service, joint carrier/native load is a material mixed-context owner;
if they also inflate, do not call the penalty mixed-only. Native/wire/copy
attribution still requires its own evidence; neither outcome justifies fixed
QUIC preference or bottleneck partitions. These are bounded existing-owner
ablations, not resumed release promotion or a final baseline comparison.

Context58584/48379 both CLOSED0: QUIC429.451Mbps with echo p50/p95/max
103.955/155.291/312.639ms; TCP442.722Mbps with302.990/348.148/472.096ms;
80/80successful each. Both beat the two ordinary mixed candidates in throughput
and echo p95, while TCP median is not uniformly better. Full matched profiles
and native queues verified: TCP-only already has ~14MB median shared backlog.
The native/joint-load versus MPP cross-carrier mechanism remains unseparated.

Next one context discriminator,04:30+08: unchanged ordinary QUIC-only foreground
bulk+echo, plus THREE independent raw TCP downloads on the SAME active47 cut.
Use existing failover_download_probe.py --parallel-downloads3, synchronized
start, fixed one HTTP request per worker,40s/50s guard, no proxy, target47:8080.
This matches the three native TCP competitors plus one QUIC count, but removes
MPP TCP-carrier striping, TCP-associated Product repair and TCP tunnel framing.
It is not an equal-byte/encryption workload or a baseline rank. Source/CC/
shaping unchanged; workload-only ablation. Save raw start anchor and every
series/failure; one native socket snapshot verifies raw TCP CC/path while active.
Information forecast: if large QUIC/echo/shared-queue inflation appears, native
competition is sufficient without MPP cross-carrier ordering; if absent, those
MPP work/allocation differences remain live causes, not a proof of one. Compare
coarsely aligned interior phases, not summed unaligned rates or an oracle claim.
No new harness, protocol preference, inferred bottleneck partition or runtime fix.

Native-competition29796/raw93933/socket71552 all CLOSED0, no process remains.
QUIC foreground179.994Mbps; echo80/80successful, p50/p95/max
252.303/412.535/773.214ms. Three fixed raw TCP requests carry284.129Mbps over
their own40.001199s; all duration-partial,0failed/0replacement. Native snapshot
verifies three BBR sockets on the SAME47cut, RTT331–339ms/minRTT100ms.
Do not add unmatched whole-window rates as exact aggregate service or identify
echo queue residence from socket NOTSENT. The raw start anchor is preserved.
Native competition produces substantial loaded delay without MPP TCP framing,
striping or associated Product repair; it does NOT prove all mixed overhead
unavoidable or authorize fixed protocol preference. Reader is closing the full
phase/native/cost comparison and archive. Current source remains011b724 clean.

The observer and ordinary single-mode context appendices are complete. Root
read their152/104lines and checked both gzip/manifests; reader verified every
decompressed input byte. The direct quantum-occupancy hypothesis is stopped.
Next bounded source question: can existing accepted Original/copy and receipt
observation identify the current mixed wire cost and its exact repair owner?
Do not infer repair volume merely from the class/body residual. Separately,
review the smallest CPU/journal snapshot discriminator without changing native
coherence or claiming elapsed wait is on-CPU work. No new runtime fix selected.

Selected mixed transaction: reuse the ALREADY BUILT feedback-quantum diagnostic,
with its quantum event disabled. One unchanged shared500 healthy mixed DOWN
capture enables existing server_data_ack_recovery,server_repair_carrier_accept,
receive_hole,receive_hole_release,stream_ack_received,feedback_return events.
No rebuild or new observer. Question: among cross-underlay actual repair ranges,
does the blocking prefix release on the Original owner or accepted alternate,
and is receipt already complete before acceptance? Preserve exact ranges,
all preceding accepted copies, sender/receiver clock domains and mapping.
Information forecast: material actual late/unhelpful receipt chronology selects
feedback/repair timing for exact owner attribution; alternate wins or absent
material examples stop that causal assertion. A late copy alone is not ex-ante
unnecessary: feedback may legitimately be in transit. These events omit frozen
fallback deadlines and nonprogressing arrivals, so do NOT infer pre/post-fallback
from age/latest RTT or claim complete winner accounting. This goes beyond an
already-known aggregate copy-volume finding without another observer project.

Closed46891:410.317Mbps, echo78/78,p95577.826ms; diagnostic91.7MB logs are NOT
ordinary performance. No accepted copy crosses previously logged server stored
frontier. Of14460persistent copies,14439/183027847B already have client
contiguous receipt before admission. Earliest logged receipt→admission median
70ms,p9573ms; admission→server positive cover median1ms,p956ms. Dominant volume
therefore matches the configured70ms return journey, not a proven large local
ACK-processing hold. >100ms lead is only0.527% of bulk accepted-copy bytes;
do not chase that small outlier before the material normal-return interaction.
Late receipt does not prove ex-ante unnecessary repair or corrupted authority.

Next model discriminator: add only scalar fields to existing
server_data_ack_recovery: its retained owner fallback relative to observed_at,
decision-before-fallback, actual legacy owner_completion, and diagnostic
decision-to-log elapsed time to bound the join to later accepted frames.
No timer, eligibility, queue, observer framework or policy change. Both request
and response intentionally still use the legacy owner score; the request None
test concerns replaced attachment identity, NOT a migrated direction.
Independent history review: dc4853d introduced both owner races,93e6284 removed
their duplicate extra payload; aa4f55d/ba177f3 changed RFC wording, not these
callers. T03 explicitly deferred this owner's migration. Clarify RFC15.2's
alternate-ranking versus timing-eligibility distinction without changing any
authority or runtime. Neither None nor unconditional loss_at is a semantics-
neutral cleanup; both would require an independently justified model transaction.
Information forecast: post-fallback-dominated accepted work rejects an early
comparison change as its direct remedy. Material pre-fallback work selects the
legacy owner-completion model for a reachable counterexample before a fix.
No aggregate residual/late-loser observation alone justifies suppression.
Join actual accepts, preserve split extents and unjoined cases, and exclude
boundary-ambiguous timestamps rather than pretending exact sub-ms clocks.
Reuse one unchanged shared500 healthy mixed DOWN cell and the same six events;
one-file cfg-only extension, freeze/reverse before running. No public claim.

Execution81130 build CLOSED0 in1m22; exact one-file patch frozen at
./.tmp/reflection/repair-deadline-observer-0911.patch, diagnostic binary at
./.tmp/reflection/bin/repair-deadline-20260911/mptunnel. Runtime fully reversed
before lab38507, which CLOSED0. Diagnostic414.221Mbps, echo79/79successful,
p50/p95/max375.743/582.986/656.397ms; maximum body gap.772739s. These are not
ordinary acceptance numbers. Independent exact-range appendix is complete.
Of171149109 persistent accepted-copy bytes,42396292 (24.77%) join pre-fallback
decisions,117454867 (68.63%) post-fallback,11297950 remain unjoined. Early
decision does not prove early admission; split and clock ambiguities remain.
Most first receipts again precede acceptance by70ms and sender cover follows
1–2ms later. An actual post-fallback TCP copy rescues the missing QUIC head;
a blanket copy ban would discard demonstrated useful recovery.

Disposition: early-owner score removal is NOT the dominant direct remedy.
Even ideal removal of its42.396MB observed association represents only about
8.48Mbps of payload over40s, before replacement work or indirect effects; this
is a scale calculation, not a guaranteed saving. Later accepted Originals can
indeed raise the legacy whole-output owner ETA without adding predecessors to
the exact head. History93e6284/T03 explicitly retains that approximation;
do not turn this known limitation into an unrelated mandatory score migration.

Next bounded question uses existing source/captures before another experiment:
why does the retained fallback mature during ordinary positive-ACK return?
Trace exact Original claim/sent_at, actual native service, aggregate clock
retention and ACK return ownership. Separately check whether ready positive
ACKs can be hidden behind server Input boundaries despite its positive-first
ServerFeedbackBatch. Neither a1ms later logged ACK nor an old assignment age
proves already-ready feedback or a wrong timer. Information forecast: a real
clock-domain or service-order counterexample with material captured exposure
selects that exact owner; a deliberate bounded-recovery/feedback uncertainty
alone does not authorize a larger timer, copy ban or additional parameter.
Preserve useful winners and loaded native competition in the decision. No
runtime proposal has been selected.

Selected05:15+08 information transaction: instrument successful ACK admission
at the existing registry async send AND try-send seams. Preserve the actual
send result: async wrapper Ok currently includes a closed receiver, and try
Full becomes a PendingMailboxFrame whose later permit publication bypasses
the registry success branch. Record exact positive ranges, stream, scope and
success/full/closed disposition; do not call a pre-send observation admission.
No payload clone, queue change, ACK union/folding, wait, timer or policy change.
All instrumentation is feature-only and event-gated; preserve failed/full
coverage so a missing admission event cannot be read as proof of absence.
One unchanged shared500 mixed DOWN capture. Keep only the new admission event
and existing decision, accepted-copy and applied-ACK events; client receive
history is not needed for the already-ready question. No deadline or CPU overlay.
Necessary two-file clock refinement before build: retain actual post-success
Instant and add the existing decision's observed_at to its event, both as local
Rust Instant Debug scalars. Compare those same-process monotonic instants, not
the later millisecond log stamps. This is a local diagnostic representation,
not a portable wire format or a new clock helper/framework. Unknown parses
remain unknown. Failed/full results are not admission instants.

Question: did a positive ACK covering a later accepted copy already finish
queue admission before the decision, while its application was still pending?
ServerFeedbackBatch correctly sends all collected positives before scopes;
new arrivals after its fixed entry count or other Input boundaries remain a
reachable domain, not measured prevalence.38507decision neighborhoods are
mostly positive-only frontier-advance transactions, not old pending scopes.
Information forecast: material successful-admission coverage selects actual
ready-fact service ordering before any server Input correction. Little such
coverage, with all relevant enqueue paths accounted, rejects it as the
dominant remedy. Full/unknown coverage instead bounds the conclusion. Keep
strict timestamp separation and lifecycle/probe barriers;1ms adjacency alone
is not the proof. The all-copy payload scale is~39.6Mbps over40s, not a promised
goodput or latency gain; native shared contention remains even if all copy
work disappeared. Freeze exact overlay/binary and reverse before traffic;
ordinary011b724 comparisons remain the performance gate. No release promotion.

Build44306 running,4cargo jobs, no lab overlap. Independent actual two-file
review PASS: admitted_at is post-actual-success and before async perf recording;
Full/Closed carry no admission time. Existing decision uses its actual
observed_at, not a later logging instant. Exact patch9058B comparison passes at
./.tmp/reflection/server-ack-admission-observer-0911.patch. On build completion
freeze bin/server-ack-admission-20260911/mptunnel and reverse both runtime files.
Then one tag ordered-feedback-server-ack-admission-down-0911 with events
server_ack_actor_admission,server_data_ack_recovery,server_repair_carrier_accept,
stream_ack_received. No per-role override, native trace, perf or other overlay.

Build44306 CLOSED0 in1m25, one existing unused-wrapper warning. Exact executable
frozen and both source files fully reversed, git diff clean, before lab80295
starts. The capture uses precisely the four declared events and unchanged
shared500/UP70DOWN30 profile. No compiler runs alongside it.
Lab80295 CLOSED0; both exact clock forms appear as declared. Independent
range/admission causality and full probe/native/cost archive are in progress.
No ordinary performance or runtime policy acceptance follows from this capture.

Closed information result: both independent all-range and root prefix-only
joins identify4497persistent copies/56804208B with a covering successful ACK
admitted before the ACTUAL recovery decision. All later obtain positive Apply;
all142284bulk admission records succeed (async135112,try7172), no Full/Closed.
This is real mailbox availability, not an invented1ms timestamp inference.
But the admission can occur after a proposed finite Input quantum's entry
count, and non-ACK boundaries remain unobserved. It is therefore an exposure
ceiling, not a measured safely catchable batch. The nominal payload scale is
11.36Mbps over40s, about2.8% of this capture's404.691Mbps; unknown interaction
effects are not permission to promise a larger gain. This does not establish
a mandatory RFC violation: the contract requires already-applied facts, not
waiting indefinitely for arrivals. No server Input rewrite selected from this
small/uncertain practical forecast. Preserve the finding for a scoped later
transaction; do not resuscitate ACK merging, cadence or a wider ready budget.

Next practical discriminator,05:30+08: matched ordinary healthy500Mbps DOWN
rawTCP, Xray and Hysteria2 bulk+64B echo. Existing ordinary MPP shared comparisons
already show both native competition and mixed latency; compare real baselines
before treating every delay difference from QUIC-only as a uniquely MPP defect.
This is diagnostic acceptance-context, not a resumed final release matrix.
Same active47cut,70msUP/30msDOWN, no loss/jitter/QoS/blackhole,40s offered.
Keep existing H2 explicit500Mbps prior visible; MPP has dynamic discovery.
Use existing programs/probes, no build or runtime change. The sole runner
addition is an explicit raw-target override so raw traffic crosses47 rather
than the historical46default; observed class traffic must verify it. This is
measurement routing, not a Product fix or altered impairment. No CPU-per-byte
claim from lifetimeps, no summed unaligned throughput, no discarded failures.
Information forecast: establish the relevant throughput/latency frontier under
the identical healthy cut and expose whether the mixed cost is exceptional
against these baselines. Either outcome retains the two ordinary mixed timing
regressions; no automatic promotion, protocol preference or smaller lab queue.
The current static-rank counterexample review remains read-only: old131-slot
Original queues changed, but native/shared-Product refill may still invalidate
a static allocator. No T03 runtime migration is selected.

05:40+08 checkpoint: raw52570 and Xray75884 CLOSED0; H2 lab29931 is the sole
live experiment. Actual Xray/H2 endpoint47 and H2's explicit500Mbps prior
verified before launch. Raw whole451.669Mbps,80/80echoes,p50103.384/p95126.229/
max294.789ms; maxreadgap.100167s. All41samples verify the intended active47
cut,500Mbps/UP70DOWN30/noimpairment/noactualdrops. This is meaningful evidence
that low loaded latency is possible, not a native-controller attribution.
Independent full-baseline comparison/archive follows all three closures.
Root read all117stats+104causal ACK-admission lines and checked archive integrity
and exact10member manifest. No new runtime change follows the small/uncertain
copy-work forecast. Separate20%loss review asks whether native contraction
recovers after loss clears; do not equate intentional above-allowance responses
with proof that arbitrarily low sustained service is unavoidable.

Selected05:42+08 recovery discriminator: ordinary011b724 QUIC-only then H2,
same500Mbps both directions, DOWN70/UP30ms, no jitter/QoS/blackhole, existing
40s bulk+64B echo. DOWN loss20% for the first20s then0% without restarting
either endpoint; UP stays0%. H2 retains its explicit500Mbps prior, MPP retains
default10%compensation/dynamic discovery. No native/MPP setting changes.
Issue: current native sender remains non-app-limited and almost window-full
while window/pacing contract and late CPU is low. Default20% response is
deliberate, but whether restored service reopens promptly is unproved here.
Competing causes: native recovery slope/phase, Product supply or logical-prefix
stall, versus in-flight physical recovery and prior differences. Existing
native FIFO tests prove eventual10x recovery in their model, not this live
encrypted20%-loss timeline or a loaded-latency bound.
Information forecast: successful prompt MPP return rejects a permanent stuck
state in this case; delayed native reopening selects its actual window/ACK
history; reopened native delivery with poor body progress selects Product
ordering. H2 comparison tests whether the same physical loss process alone
forces the observed service, not a policy-neutral controller oracle. No speed
gain is promised by this observation. Preserve every echo failure/censored
success series; no threshold, lower rate, or favourable rerun to rescue it.
Smallest invocation change extends existing loss20_cpu.py with optional loss
clear time and H2 process-name CPU collection. Existing runner only changes46
at5s epochs, so the wrapper explicitly updates47 as well; actual before/after
qdisc and monotonic/Unix transition records must verify the used cut. Both
initializations and both directions remain as declared. This is lab routing/
timeline instrumentation, not a Product fix. No build or runtime overlay.
Tags loss-clear-{quic,h2}-0911. Analyse full phases/recovery/gaps/native/cost
before deciding any model intervention; healthy mixed gate remains open.

05:48+08 outcome: healthy raw/Xray/H2 all CLOSED0, respectively451.669/449.343/
465.775Mbps with80/80echoes and p95126.229/128.192/113.856ms. Root read the
complete160line ordinary comparison and verified26file archive integrity/
manifest; independent byte checks pass. Both mixed ordinary398–404Mbps and
527–531ms p95 remain uncompetitive in this cell. Native competition contributes
but does not make every penalty necessary. A read-only actual allocator audit
found no justified stale-rate or duplicate-backlog fix. Counting all QUIC
buffered work as FIFO would be wrong: priority1 echo/repair can preempt bulk0,
and exported pending_bytes is only the current H3 transaction, not that buffer.
Unobserved high-priority repair work can theoretically underprice a subsequent
bulk action, but its critical volume is not measured; no runtime change follows.

Loss-clear QUIC59318/H299401 both CLOSED0. Actual47successful-change Unix
timestamps are1789076549.210883/1789076661.516681, not the wrapper's earlier
profile_elapsed observation; qdisc histories verify both changes. Q raw phase
15–20/20–25/25–30/30–40Mbps is14.121/31.190/402.485/422.198; H2 is132.435/
459.957/471.716/467.003. Native QUIC window rises~97KB at20s to~9.4MB at26s
in the same epoch, with matching ACK/body progress; no permanent stuck state
or later Product stall is demonstrated. Its first four restored bins remain
15–20Mbps, while H2 returns to~472Mbps by21s. Existing native probe waiting/
growth and the explicit-prior difference remain candidate explanations, not
a proven bug or permission to shorten timers. Qserver CPU20–25mean38.651%,
then~156–164% with restored high throughput. No persistent CPU-saturated
recovery failure. Q74/H280echoes all succeed, but Q's preclear2.478s echo is
preserved. Independent full distribution/cost/clock archive is in progress.
No current compiler/lab; no runtime edit or released performance claim.

## Separate open issue: one-core burst near20%QUIC loss

[Four ordinary500Mbps DOWN controls](QUIC_LOSS_CPU_20260910.md) on d44:
QUIC0/20loss and mixed0/20loss,100msRTT,no jitter/QoS/outage.
WholeMbps430.558/48.261/385.581/61.069; late30–40s441.397/1.906/371.853/3.021.
Q20echo has one actual timeout plus33later unavailable records, not34timeouts.

Startup process peaks101.2/138.1%ofonecore occur with substantial transferredwork.
Late server3.87/7.11% rules out sustained MPP CPU saturation as this late collapse's
cause, not external scheduling delay or the deployed random burst. Role/version/
platform remain unconfirmed; do not block the main closure waiting for them.
Startup native-byte-normalized CPU controls do not establish loss-specific
amplification; ratios are not intrinsic costs or an attribution of all work.
20%exceeds the default10% allowance +2%residual response boundary11.8%, but
authorized backoff does not prove near-zero service unavoidable/correctly calibrated.
No Rust/BBR/leak resolution or threshold fix is justified. Native contraction and
prior bounded journal fixes do not identify this incident. Preserve27-file evidence.
If a CPU-focused intervention becomes next, capture actual on-CPU ownership at
the burst, not elapsed diagnostics; no sudo or permission bypass.

Next CPU transaction: first measure the COMPLETE synchronous endpoint
authority, shape and metrics snapshot calls, with Linux THREAD_CPUTIME_ID.
These nonnested public entry points include controller cloning, projection and
temporary destruction; no await occurs inside them. Do not instrument the
nested active_native_controller_snapshot a second time or add totals together.
One cfg-only endpoint file, existing periodic perf recorder, explicit event
enablement; clock failure is a separate unknown/error counter, not zero CPU.
Per-call recorder microsecond rounding/floor error is bounded by call count;
logging/recorder time occurs after the endpoint measurement and native unlock.
Construction's once-only telemetry clone and ordinary migration clones are
outside the declared measured owner. No proto feature/API/TLS or controller
change is needed for this first falsifier.

Information forecast: a small complete-owner CPU contribution in a reproduced
burst rejects snapshot work as its dominant cause, avoiding an unnecessary
deepcopy redesign. A material share selects nested clone/retained-state
attribution before any scalar snapshot correction. No burst means no incident
attribution. Benefit of a runtime correction is unknown until this measurement;
no speed gain forecast from clone counts. Reuse loss20_cpu.py, one QUIC20 DOWN
cell (500Mbps,DOWN70/UP30ms,no jitter/QoS/blackhole), current011b724 feature
binary, preserving existing process/thread ticks and full service series.
Freeze exact patch/binary and reverse source before traffic. This diagnostic
is not an ordinary comparison or threshold/calibration intervention.

04:46+08 execution: build4900 CLOSED0 in1m21; one existing unused-wrapper
warning only. Exact4779B endpoint-only patch frozen, ordinary source fully
restored and checked clean before traffic. Diagnostic binary is
./.tmp/reflection/bin/native-snapshot-cpu-20260911/mptunnel; target/release
is diagnostic. First run27528 CLOSED0 but snapshot CPU recording is INVALID:
host MPTUNNEL_LAB_PERF was not forwarded by docker exec, so no owner CPU records
exist. This is an invocation failure, not a Product failure or zero CPU result.
Preserve that capture under native-snapshot-cpu-twenty-quic-0911. Correct only
the invocation using a four-line exec/env wrapper, as existing observers do;
rerun the identical declared cell with unique tag
native-snapshot-cpu-recorded-twenty-quic-0911, no source/build/profile change.

Valid38084 CLOSED0. Roughly one-core startup99.475% reproduced (1.01CPU-s/
1.015331s) with46.345MB native ACK progress in the approximate band. All three
complete server snapshot owners total59.122ms/2897calls; rounding upper62.019ms
is at most6.14% even if ALL capture work were put into that one peak. Across
capture this is≤1.12% of sampled process CPU; no clock errors. Thus stop the
snapshot-dominant hypothesis: no nested observer, deepcopy/API redesign or
sampling change justified. Remaining burst execution is unassigned; startup
native work and lower late CPU do not prove every instruction necessary.
Late service1.025Mbps coexists with2.751%onecore CPU: sustained local CPU
saturation is not this collapse's owner. One actual echo timeout plus27later
unavailable records are preserved. No loss threshold or native tuning follows.
Root read the complete116line appendix; reader's19file archive verifies both
invocations and every input byte. Source remains011b724; no runtime fix here.

## Preserved dispositions: do not revive or erase

- a16b404 exact-subsumption invalidation is a real44-check mechanism checkpoint,
  with mixed ordinary gain and adverse write gap; now underneath011b724.
- Pending-gap service trial3923:82.855Mbps, worse confirmation/precut/cut despite
  better write/restored phases. FULLY REMOVED8668cd8;47focused checks did not
  establish useful composition. Do not restore its pending owner/wait state.
- Recovery-attached global-projection removal44791:32.008Mbps,26.571s write/
  22.437s confirmation gaps. FULLY REMOVED8fd4800 despite28checks.
- Full observation cache rejected before runtime: independent Regular/Backup
  publication can invalidate unbound ranking despite immutable Product state.
- Queue-readiness filter not selected:93.2%ofrefusals early, ZERO in exactlate
  plateau. Stable-absent query pruning too small. Cyclic direct cursor can skip
  new higher-priority service; resetting on every publication can starve later work.
- ACK publication/batching/cadence trials remain rejected for ordinary timing
  costs. 011b724 does not change generated ACK cadence or native packaging.
- b0baca2 native TCP refill: actual1.353s unsent wait removed, TCP417→440DOWN/
  422→450UP and p951269→323ms. Both mixed orders lose~5%late with more copies;
  no reserve/copy-ban tune. Earlier independent aggregation D148→302/U169→316.
- ba56290 pre-target receipt liveness and364d417 impossible proof-round sequencing
  have real tests, not full stall closure; latter's outage tails worsen.
- 79ddb41 omitted successor service is real, but ordinary85s/26.666sgap; a747/d44
  reduce work without closing stalls. Their costs motivated current feedback fix.
- Request paired-clock sampler remains shelved: actual counterexample, but
  incomplete ordinary pair with adverse confirmation timing. Not in current source.
- Restart/churn1941mixed+64single reclaims owners; deployed random RAM/CPU remains
  unattributed. [Full dispositions](CHANGE_DISPOSITION_20260907.md).
- [12-cell harsh matrix](NATIVE_REFILL_COMBINED_20260910.md) retains failures/
  censoring and all baselines. No incomplete winning rank; all-baselines-poor
  sub100Mbps stress is diagnosis, not public performance acceptance.

## Unchanged global acceptance order

1. Close material mixed allocation/startup/stalls with exact causes and ordinary
   completion, first service, full time series/gaps and loaded latency.
2. Both directions/all3MPP modes: changing loss/jitter, suddenQoS, blackhole and
   restart-free recovery, with their ablations; no TCP/QUIC static preference.
3. Single500Mbps/independent200Mbps links, shared/asymmetric cuts and aggregation,
   including simultaneous changing impairments. No fixed bottleneck partitions.
4. Cold/warm short objects, single/concurrent work and actual speed.cloudflare.com;
   matched rawTCP,Xray,H2 and MPTCP where available.
5. Restart/churn, post-load reclamation, CPU/RSS and platform checks.
6. Only then public README/PERFORMANCE timing/latency curves, costs/completion and
   release if competitive. No universal instantaneous-optimum promise or avoidable
   stall waiver; preserve working declared functionality.

Pinned harsh profile: routed500Mbps,UP70±20/DOWN30±5ms;5s UP loss
[3,8,5,6,10,3,5,8]%mean6,DOWN[1,2,.5,3,2,.5,1,2]%; UP10Mbps15–25s,
wholeUDP30–33s;40soffered,85srunner/90sprobe guards. Do not change it to pass.
Healthy/heterogeneous service, not crossing100Mbps, determines usability.

## Execution and continuity safeguards

Root alone builds/runs, no compilation/lab overlap. Existing owned Docker only;
no sudo, host shaping, outside-root work, /mnt/storage use or deletion now.
Direct topology clienteth0=46/eth1=47;servereth1=46/eth0=47. Two independent
200Mbps cuts in aggregate, each shared by its TCP+QUIC—not eight physical links.
Single configured mixed endpoints use their one actual cut; verify class traffic.
Preserve exact source/copy/incarnation ownership, eligibility, ranked Apply,
nonrenewing clocks, half-close/cancel and retained capacity wakes.

Build/artifact identities and failed candidates remain explicit; never run an
old target/release by assumption. Exact commits only; docs-dev requires force-add.
PROGRESS is ignored continuity. AGENTS.md immutable; userdoc+7lines untouched.
Telegram last21:23UTC, next nonurgent>=22:23UTC. Commentary within60s; verification
polls by minutes. Before compaction record current sessions, next decision,
source/binary identities and open/adverse outcomes. Do not stop at a checkpoint.
