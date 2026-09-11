# Current deterministic closure plan

Updated: 2026-09-11 08:39 +08:00. Authoritative repository: ./.
**No performance/release acceptance, push or public README update.**
Continue the authorized closure loop; an intermediary commit is not completion.

### Live decision summary

- Trialf8b8cac is REJECTED from active source after ordinary9690: mean flat,
  shorter confirmation gap but a3s target plateau and late4.194Mbps forward phase.
  Source restored exactly to011b724; four→zero work/73checks did not establish
  net service gain. User's seven-line edit is untouched.
- Diagnostic83561 exposes clipped-range owner-target refusals on real blocked
  prefixes; true producer RED46767 fails0vs14600B after its reachability checks.
  The bounded exact-range exclusion correction04f1e56 passes152focused checks
  and independent review. Ordinary95178 completes, but shorter stalls accompany
  lower useful speed and higher wire/queue cost. Exact defect fixed; composition
  UNACCEPTED.77827 proves assigned receiver HOL. New19038 identifies a3.980s
  TCP46-owned head stall with an actual measured Q47 alternate withheld by the
  retained future loss clock. Clocks do NOT renew; this is local timing policy,
  not a native requirement or proven CPU bug. Real producer RED52235 now proves
  that withholding. Tiny request-only ordered-credit head pilot is under test/
  independent review below; no blanket hedge/controller/threshold change.
- The finite client Input correction materially improves upload, including
  QoS+QUIC outage: exact-confirmed56.280→186.629Mbps; settlement79.379→43.147s.
  However,6.382s confirmation gap remains. Trace THIS return prefix now.
- Labs58328/81095/51839 CLOSED0; no live compiler/lab. Latest exact queue capture
  proves505–594ms shared-FIFO→owner holds despite already-applied required MAX.
  It does not identify intervening ACK work. Ordinary6.382s remains acceptance
  evidence, and predecode5–7s delays are not attributed to native transport alone.
- Healthy shared mixed latency remains uncompetitive: p95527–531ms versus
  raw/Xray/H2114–128ms, while delivering fewer useful bytes. Neither generic
  native-buffer counting nor small ready-ACK copy savings justifies a fix.
- The20%loss CPU report is not resolved: local startup one-core execution
  reproduced, snapshot-dominant cause falsified. Loss-clear speed recovers in
  ~6s versus H2~1s; actual native reopening—not sustainedCPU—owns that ramp.
  Exact policy/implementation attribution remains open, with no timer tweak.
- All global both-direction/mode/aggregation/changing-link/browser/baseline/
  sustainability/platform gates remain below. No public README/push/release.

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each new
transaction and after compaction. This is the active ledger, not a second RFC.
Complete earlier forecasts, failed experiments and attribution are preserved in
`git show 011b724:docs-dev/CURRENT_CLOSURE_PLAN.md` and
[the full evidence report](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md).
Condensation waives no failure and reactivates no rejected candidate.

## Current source and proven correction

Ordinary comparator runtime is **011b724**, finite ordered ACK/MAX Input
service, with binary ./.tmp/reflection/bin/ordered-feedback-20260911/mptunnel.
Current trial04f1e56 adds only the two-file clipped repair-range
correction described below, plus its focused tests and RFC clarification.
The working tree additionally contains the unaccepted request-only credit-head
pilot below (request.rs predicate, RFC, producer/opposite tests). No ordinary
candidate executable exists for that pilot yet; no performance claim follows.
Rejected **f8b8cac** remains a tracking checkpoint; its frozen ordinary binary
./.tmp/reflection/bin/logical-feedback-20260911/mptunnel is NOT active design.
Ordinary04f1e56 is frozen under ./.tmp/reflection/bin/clipped-range-20260911/mptunnel.
target/release/mptunnel is the six-file exact-head DIAGNOSTIC, also frozen under
./.tmp/reflection/bin/exact-head-20260911/mptunnel. Use explicit binary
paths. All observer and rejected-trial runtime edits are reversed.
The user's seven-line LIVE_OWNER_FRONTIER_WORK_BOUND.md is unrelated
dirty source; never edit/stage it. The temporary two-file ACK-admission observer
is fully reversed; build44306 and lab80295 both CLOSED0. No runtime
other policy changes are implemented. The bounded candidate below is not
an accepted fix or permission to change controller/queue/timing parameters.

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

Next selected main-issue transaction,05:50+08: retain the unresolved healthy
mixed latency cost and test the higher-impact existing upload-stall correction
against outage composition. Ordinary a16 control then011b724 candidate, direct
independent200+200Mbps, UP70/DOWN30ms, no random loss/jitter. Preserve the prior
QoS-only pair's10Mbps cut on46 UP15–25s;47 stays200Mbps. The sole new condition
is the existing endpoint UDP blackhole30–33s on both QUIC paths. Three baseline
native modes are not substituted; this is an exact affected correction pair,
not the final random-link matrix or a rerun seeking a healthy latency pass.
Both candidates/binaries already exist; no compiler or runtime change.
Information forecast: determine whether finite ready ACK/MAX Input retains its
observed92.621→204.614Mbps /7.236→1.162s write-gap gain when native availability
changes and TCP must take over. A large confirmation/write stall or failed
settlement selects its exact actor/native/prefix owner before any new fix.
Faster aggregate bytes do not waive a worse critical interval. Preserve every
confirmed/accepted byte, phase, full gap/censoring/settlement and native/resource
history; no claimed loaded-echo result from this upload-only probe.
Tags ordered-feedback-outage-{control,candidate}-0911. Use existing management
and target-socket sampling with fixed40s offered/85s guard, unchanged hints.
Topology verification: current endpoints each attach both46/47 directly;
REFLECTION_ROUTED alone would bypass its shaped router and be invalid. Do not
pretend this is the pinned routed500 harsh case, recreate containers, add routes
or modify host networking to run this bounded pair. The full routed gate stays
open. An adverse candidate stops promotion, not the global authorized task.

Execution: control53802 is active; candidate has not started. No concurrent
compiler/lab. Root read all199loss-clear appendix lines and verified its15file
archive integrity/manifest; independent byte checks passed. Ask the user
nonblockingly for incident role/version and whether CPU persists after traffic
slows/stops; no deployment assumption or task pause. No runtime change follows
the reproduced startup CPU alone.

Closed53802/76242 outcome,05:57+08: both1/1complete with every accepted byte
confirmed and no probe error. Control558432256B/79.378582s=56.280Mbps;
candidate1006567424B/43.147312s=186.629Mbps. Worst write9.324→2.637s,
confirmation11.951→6.382s. Candidate removes the extended post-offer tail,
but zero confirmation in raw30–33s versus control101.059Mbps remains an
adverse phase; do not declare failover solved from the mean. Its longest
gap spans positive events in bins28→34, partly before the actual30.171–33.489s
UDP drop. Source/target and TCP native ACKs progress during missing return
confirmation, which rules out total forward outage. Client reply count1457B
is flat over five management seconds while server reply-read rises1709→1961B.
Replies catch up by34.968s. Exact max-gap endpoints/internal stage are not
exported; no invented assignment of the full6.382s to physical outage.
Control also has a genuine forward target plateau and a separate~12s return
hold with empty local sink/probe queues. Independent full report/archive is
being finalized. Source011b724 remains unchanged, no new runtime fix selected.

Next one bounded diagnostic: reuse ALREADY BUILT repair-deadline-20260911
binary (011b724 plus its frozen one-file scalar observer), same exact candidate
independent200+200QoS+UDPoutage UP profile. No quantum observer: its old port
filter only covers8080/10022, not this10023 upload. No new build or source edit.
Observe tiny sink-return DATA via existing server sender dispatch/output updates,
accepted repairs and exact relative fallback fields, client receive-hole/release,
applied ACKs, feedback route/proof, attachment mapping and writer-drain events.
Question: does the critical return prefix wait before any eligible repair,
after actual copy admission, or after already-consumed client receipt? Preserve
same-role clocks, actual dropped interval, identity/range joins and unjoined
frames; dispatch/admission is not native transmission, and these events do not
log every prepared Original or every raw decode. No winner or barrier claim
from absence. Information forecast: choose the earliest material observed owner
for a scoped correction; no recurring return hold means no causal conclusion,
not a favourable replacement of the ordinary result. Do not change timers,
copy budgets, native gains, usage or admission. Tag ordered-feedback-outage-
return-diagnostic-0911; root sole runner, management/target observations retained.

06:10+08: diagnostic58328 CLOSED0, exact1093926912B/43.391260s,
201.686Mbps; maximum confirmation3.774884s/write2.797807s,1/1complete,
no errors. Its132243691B logs can perturb scheduling; never substitute its
smaller gap for the ordinary6.382488s outcome. Exact response head[1067,1081)
is a known omission at21.073s, lowest at21.718s; first repair is admitted
25.403s. There are2096 output updates with this lowest frontier, maximum
adjacent gap279ms, so this is not a wholly parked3.775s server actor. The
eventual decision's retained loss boundary is about24.774s, fallback28.476s;
earlier eligibility/target observations are not recorded. ClientQ0 releases
the prefix25.419s, whereas this observed repair selectedQ1. Do not assign the
winning byte to that copy or claim the whole delay is native/outage latency.
During the actual outage, Q1 repair[1515,1529) at32.236s expires at32.647s,
TCP4 repair then releases it at32.677s; a second exact head also reaches TCP.
TCP takeover is reachable, but the initial eligibility wait remains unassigned.
Root/independent reviewers are checking the actual clock producer and ranking
contract before any correction; no timer change or new observer is selected.
Root read all223 ordinary-outage appendix lines and checked the16file gzip
integrity/manifest; independent byte-for-byte checks passed. Archive preserves
adverse outage-phase confirmation, wire/resource costs and all raw bins.

06:11+08 next information transaction: existing Q0 proof157 is admitted at
16.987s, followed by a completed same-output writer drain with zero pending
charge in the same millisecond, but reaches the client logical owner25.186s.
No per-frame write stamp exists, so retain that accounting-based handoff bound.
Native snapshots, not Product RTT, supply the observed3.658sSRTT and corresponding
4.116s ACK-gap threshold. Independent retained-tail fallback has its own earlier
per-flight observation; its value/target refusals are absent. Neither timer is
proven wrong. Server actor output updates continue throughout the critical hold.

Question: is that delayed proof already decoded but waiting for Product/attachment
service, or late at the raw framed reader? Add one cfg-only read boundary event
in udp_path_read_frame for StreamFeedbackProbe/Receipt only: exact stream/token,
read duration and successful decoded timestamp before returning to routing.
No payload clone, await, sample threshold, queue, batching or runtime policy change.
Existing proof publication/receipt and writer-drain events complete the join;
only role/stream/token-unique transactions are attributable. A late decode does
not distinguish network/native buffering from earlier reader backpressure or
executor service; the read duration only bounds that particular invocation.
Information forecast: timely decode plus multi-second logical-owner wait selects
the local postdecode boundary; prompt postdecode service rejects it as the
dominant stage and selects native/framed-reader residence, not a proven native
bug. No material hold means no causal conclusion, not a favourable rerun.
Same independent200+200 QoS/outage UP cell, exact current011 source and existing
runner; one-file observer built/frozen/reversed, no other overlays. Keep current
ordinary6.382s failure as acceptance evidence. No performance gain is promised
from this observation and no further timer/controller adjustment is selected.

Build21654 CLOSED0 in1m24, existing unused-wrapper warning only. Independent
actual-diff review passes: successful read before routing, borrowed token/stream,
event and explicit selected-stream gating; cancellation/errors emit no success.
The event exports read_elapsed_us, not rawread_started. Timing includes
suspension/descheduling; no packet-arrival or continuous-reader claim.
Frozen ./.tmp/reflection/bin/quic-feedback-decode-20260911/mptunnel is current011
plus only ./.tmp/reflection/quic-feedback-decode-observer-0911.patch. All runtime
source reversed and verified clean before solelab81095 started. No compiler
runs with traffic. Tag ordered-feedback-outage-quic-decode-0911 uses selected
stream0 and only the declared decode/proof/drain/return/dispatch/repair events;
no bulk ACK or every-output-update stream, no other observer overlay.

81095 CLOSED0: exact940507136B/47.313948s=159.024Mbps, worstconfirmation
2.066601s/write2.765480s; allaccepted=confirmed,1/1complete,no errors.
77unique QUIC proof triples independently joined. Q0token167 spends7197ms
beforedecode and0msafter,162 spends5888msbefore/1msafter; read_elapsed0us
does NOT show continuous native waiting. Conversely tokens60/61 decode30ms
afterserveradmission then wait1144/1277ms toclientowner. RequiredMAXalready
applied6.041s,12msafterdecode, so it does not explain most of that interval.
Actualreplydelivery also fallsbehind at6–8s; this is material local service,
not just an irrelevant losing proof. Capture still has77582669B logs and
different throughput; neither smallermaximumgap nor mean is an acceptance gain.
ActualQoS15.187–25.215/outage30.215–33.515, allclasses200except46UP10Mbps,
UP70/DOWN30ms,no randomloss/jitter/netemdrops. UP/DOWNwire1.814713GB/17.374MB;
client/serverRSSpeaks343860/116768KiB, lifetimeCPUpeaks148/72% (notintervalCPU).

Next focused information transaction: add only selectedproof timestamps to the
existing exact-attachment forwarder, alongside the same rawdecode observer.
Its receive timestamp and successful shared-logical-FIFO admission divide the
proven decoder→owner local hold into preattachment service, forwarder backpressure,
or sharedFIFO/Product service. Preserve failed sends, MAXcoalescing and ordering;
no payloadclone, extraawait, parameter, reorderedinput or changedquantum.
Existing queues are pernative-reader, perexactattachment, then sharedlogicalFIFO;
ordinary andrepair inputs use the same attachmentforwarder. The owner timestamp
is after Product lock, not just selection. Read elapsed includes scheduling;
tokenuniqueness is valid only within the selectedone-sessiondirection/stream.
Benefit forecast is information, not speed: a concentrated multi-second local
stage selects that actualowner; no material repeat stops localattribution, not
revives a timerfix. No newProductfixselected. Same exactoutagecell once, current011,
freeze/reverse bothobserverfiles beforetraffic. Drop high-volume writerdrain and
senderdispatch logging in this stage-onlycapture; neither supplies the missing
local boundary. Keep proof/decode/attachment and tinyreturnhole/repair events.

06:20+08: root reviewed the actual two-file observer: previous35line decode
hook plus51line attachment hook, no new await/clone/ordinary state. Source
author's formatting/diff checks pass; root solebuild20039 is active, fourjobs,
no laboverlap. Compositepatch frozen at ./.tmp/reflection/quic-feedback-
attachment-observer-0911.patch. Freeze executable in bin/quic-feedback-
attachment-20260911, reverse bothfiles, then one tag ordered-feedback-outage-
quic-attachment-0911. Capture deliberately omits high-volume writerdrain/
senderdispatch logs; no claimed timing comparability of diagnostic averages.

### Selected finite logical-feedback transaction,06:34+08

Build20039 and lab51839 CLOSED0; observer source fully reversed before traffic.
Exact916127744B/43.391247s=168.906Mbps, all accepted bytes confirmed, no errors;
maximum confirmation/write2.469176/3.252321s. Only1.63MB logs in this stage
capture; these remain diagnostic, not an ordinary improvement claim.
All94 probes join exact five-stage identities. Admission→decode median/p95/max
31/3539/5072ms; decode→attachment1/196/594ms; attachment→shared0/1/33ms;
shared→Product17/198/594ms. Tokens108/109 wait594/505ms after successful shared
admission; actual required credit was already applied during those intervals.
All142 selected decoded Probe/Receipt frames reach shared admission successfully.
Reply backlog overlaps local holds, but exact maximum-gap attribution and the
intervening ACK identities remain unknown. No deadlock or native-only claim.

Issue/question: does treating pure StreamFeedbackProbe/Receipt as mandatory
Input-quantum barriers cause repeated recovery before an already-ready later ACK
invalidates it? Current011 intentionally retained the older standalone marker
branches. Their original purpose was ordered ACK/MAX application before logical
proof, not a required unlock or recovery-discovery pass. RFC8.4.1 and exact
receive_feedback_probe/receipt operations support preserving that authority
within a finite transaction. RFC10.4's ACK/MAX-only whitelist must therefore
be deliberately corrected if the counterexample passes; do not call it unchanged.

Reachable RED: actual Original claims, receiver-produced ACK exposing a gap,
then a ready Probe and ACK filling it, all in existing Input order. Assert the
real gap and final release first; observe heavy discovery between those facts.
The correction would preserve every ACK transaction, actual MAX, marker order,
exact per-item attachment, current receipt expiry and final recovery-before-
publication/unlock. Probe.max never grants credit. A marker may observe facts
already applied but does not certify recovery scheduling or freeze future state.
DATA/FIN/RESET/requalification/error/mismatched-stream barriers remain unchanged;
fatal processing revokes prepared claims before unlock. No wait, replenishment,
larger input budget, pacing/cadence parameter, protocol preference or server rewrite.

Benefit forecast: the measured local holds are material half-second service,
but their removable fraction is unknown because intervening work was not logged.
This candidate can remove repeated recovery/turns across READY logical feedback
only. It cannot remove DATA barriers, one expensive final scan, predecode/native
delay or all five-second holds. Zero practical gain or worse latency is possible;
larger Product holds and changed route-publication timing are opposite risks.
Value: one bounded real-actor RED can establish whether the same proven011
feedback-work defect remains reachable through these markers without another
diagnostic logging project. Do not convert call counts into predicted Mbps.

Root solely runs tests/builds/labs. First test-only RED, then smallest coherent
implementation and independent semantic/opposite-control review. If no causal
RED or authority fails, reject before ordinary work. After GREEN, ONE ordinary
candidate run on the unchanged independent200+200 QoS/outage UP cell against
the preserved011 result (186.629Mbps/6.382s confirmation); no new observer.
Material improvement without adverse service selects the healthy shared500
mixed DOWN gate, where011 has repeatable527–531ms p95. No improvement or adverse
timing stops promotion and prompts attribution, not a favourable rerun/timer.
All global gates remain intact; this is not a full CPU or native-recovery fix.

06:37+08 real-actor RED2442 CLOSED101 after59.98s compile. Actual claims,
receiver ACK scopes, entire two-item successor readiness, real MAX unchanged,
matching exact-owner Probe receipt and full final byte release all pass before
the intended assertion: FOUR recovery discoveries occur between ACK1 and ready
ACK2, expected zero. Independent reachability review passes. No fabricated
flight/cache or timeout failure. This proves redundant work, not its measured
wall-time fraction. Exact test-only patch/log: logical-feedback-red-0911 under
./.tmp/reflection/. A bounded implementation is now authorized: carry full
per-item identity and pass one mutable input state sequentially to existing
read/apply closures, avoiding a new queue, trait or admission framework. RFC10.4
and the old scoped-model boundary wording are intentionally updated alongside
the candidate, not silently presumed already compliant. Ordinary promotion
remains withheld until practical tests; no performance forecast is upgraded
merely because the operation-count RED is decisive.

The finite bound is explicit: first item plus R frozen ordered-input attempts,
and at most one actual coalesced-MAX reconciliation per serviced Probe, retaining
the old standalone Probe operation. No waiting/replenishment or drain loop;
credit contents can advance concurrently. This is O(R) work, not a claim that
only R state applications occur. Marker-only prepared publication can reconcile
registrations/lane earlier than the former next-loop head; it creates no new
byte/target authority and notifies only actual changes. Keep that interleaving
risk in ordinary acceptance rather than claiming timing equivalence.

06:44+08 GREEN8906 CLOSED0:35control checks including realactor four→zero
discovery counterexample, original ACK-only case, malformed real Probe state,
blocked write/flush/MAX, exact barriers and terminal cleanup. Additional12client,
5service and21attachment checks pass (73distinct checks total). Independent
actual source/RFC/test review PASS. Fatal prepared revocation and fresh receipt
expiry are source-audited existing branches, not falsely described as a new
two-path actor expiry or malformed-Probe actor-revocation test. Root rustfmt
on the three owned source files resolves only formatting in changed hunks;
targeted format/diff checks pass. No semantic runtime change after GREEN.
Next sole ordinary release-profile build, four cargo jobs; no feature observer
or lab overlap. Freeze exact candidate patch and executable before the one
declared200+200QoS/outage UP comparison. No performance promotion from GREEN.

06:45+08 ordinary build43682 CLOSED0 in1m21, only existing unused-wrapper
warning. Exact five-file candidate patch unchanged during build; frozen binary
./.tmp/reflection/bin/logical-feedback-20260911/mptunnel. target/release is now
ordinary candidate, not011 control. Sole lab9690 active, tag logical-feedback-
outage-candidate-0911, declared exact200+200/UP70DOWN30/46UP10 then recovery/
UDPblackhole profile, management+target observation, no diagnostics. No compiler
overlap. Root will analyse complete series before deciding healthy gate; no
live partial aggregate is acceptance. All other agents build/lab-idle.

06:51+08 ordinary9690 CLOSED0: exact1016135680B/43.076666s=188.712Mbps
versus011186.629, complete/noerrors. Worstconfirmation6.382→3.152s andwrite
2.637→2.216s, zero bins10→3. Firstservice essentially unchanged. BUT phase
means0–15/15–25 drop213.737→193.112 and120.977→96.203Mbps. Own-clock target
evidence—not merely confirmation catch-up—shows3s zero progress at518312276B
from21.961→24.961s; replies1104B both ends, source=target+64MiB. Empty target
loopback queues and fastQ47nativeACK+58.295MB exclude a stopped sink or all-
carrier outage, not identify the missing byte's owner. Late37.960→39.960s
target advances only1MiB (4.194Mbps), versus011204.111Mbps; source again at
the exact64MiB outstanding ceiling. Native epochs stay stable.

Forecast disposition: source-level redundant work is real, but its removal
does not demonstrate a net ordinary service benefit. Whole speed improves only
1.1%; lower maximum confirmation gap redistributes rather than closes material
forward stalls. UP/DOWN sampled wire per confirmed byte1.68661/.02610 becomes
1.67294/.02077, while serverpeakRSS126180→140976KiB. No sole aggregate, lower
wire total or unit proof waives the forward deficit. One pair does not prove
the new quantum caused it. No favourable repeat or healthy-gate promotion.

REJECT the active trial, preserve exact checkpoint/binary/evidence, and restore
all five owned runtime/RFC/model files to11e38dc's source011. Reverse applied
with apply_patch; exact diff against11e38dc is empty for all five files. No
new knob/controller/cadence or queue change. Root corrected a saved patch's
trailing context lost by trimEnd; both saved RED/candidate unified patches now
pass git apply --check. This artifact issue did not affect compiled source or
measurements. The author's final archive will include corrected patches.

Next practical question stays within the existing forward-stall owner: which
exact assigned prefix prevents target delivery while healthy native ACKs and
later work advance? Review existing011 events and immutable recovery/wake model
before selecting ONE capture. Separate actual target deficit from delayed
confirmation; native ACK is not Product ordered progress, and Q46's largeRTT
does not identify it as the missing owner (its displayed Product flight is0).
No new instrumentation/model correction is selected yet. Information forecast:
an exact prefix/owner/deadline/admission chain can choose native service versus
repair eligibility, blocked alternate, or queued logical input. Without that
join, another feedback-boundary or congestion adjustment would repeat the
failure of forecasting user gain from local work counts. Do not end here:
continue this existing material stall attribution; all global gates remain.

06:57+08 next discriminator selected after independent source review: reuse
the already-frozen011 quic-feedback-attachment feature binary, with its added
decode/attachment hooks DISABLED. One unchanged200+200QoS/outage UP capture,
events sender_service_decision,server_receive_hole,server_receive_delivery_stall,
stream_ack_received,request_retained_frontier_reinjection,data_ack_loss_timer.
No new observer/build, no broad per-loop snapshots/perf stream, no parameter
or topology change. Standard hooks do not all filter by selected stream, so use
the existing one-stream workload and verify actual stream identities afterward.
Tag ordered-prefix-outage-diagnostic-0911; management/target sampling retained.

Exact information question is smaller than a full pipeline claim: is the
target-flat episode accompanied by an actual ordered-receive gap, what prefix
R_before=R_after-delivered_bytes finally releases, and which actually admitted
repair extents overlap it? If receive progress already precedes target service,
select local write/service; if an open hole and delayed repair precede release,
select that exact retained/assignment owner next. If no material stall repeats,
no causal conclusion, not a favourable comparison or automatic extra run.

Limits known BEFORE capture: prepared Originals bypass sender_service_decision;
it is an accepted repair extent, not full Original cover/native transmission.
Server receive events occur before local write, omit winner identity and do not
log every head movement; their gap can include previous pending target writes.
Client ACK events omit full ranges/contiguous frontier and subsumed transactions;
largest_end is not F. Target management counts actual socket-write acceptance,
not remote application consumption. No new proof of Original age, fallback
maturity or winner follows without its missing owner evidence. Feature-only
observation can perturb scheduling; ordinary failures remain acceptance evidence.

Source review rules out two tempting guesses: blocked source admission does not
disable ACK-gap/retained-frontier recovery, and native ACK progress alone cannot
release Product retention or reset logical-staleness persistence. Same-assignment
fallback only tightens, while native RTT/ETA still affects early eligibility and
target choice. Source−target=64MiB cannot distinguish assigned retention from
unassigned queue or prove peer MAX exhaustion. Do not implement those disproven
shortcuts or suppress recovery because one carrier's native counters advance.

### Exact prefix closure and next range-identity discriminator

07:13+08 closed69779: exact1031536640B/44.582429s=185.102Mbps, allaccepted
confirmed/noerrors. Diagnosticmaxwrite/confirmation1.692278/1.522445s,45bins,
onezero24;21.406MB logs. Not ordinary improvement or trialf8 acceptance.
Server ordered prefix630414428 waits1.390101s from23.776636→25.166737.
Exhaustive40282acceptedDATArepair ranges show exactly one covering admission,
Q1[630414428,630429028) at25.064737,102ms before release. Three later pieces
[630429028,630465364) were admitted399ms earlier. During the pre-admission
wait189otherrepairs/2.517MB and471novelACKapplications occur. ActualtargetT
and bothreplytotals stay630414428/1373 over sampled24.008→25.007s, sourceT+
64MiB; targetloopbacks empty, stableQ47nativeACK+25.364MB. This excludes prior
localtargetwrite parking for that interior, not identifies Original or winner.
Of57releasegaps, firstcoveringrepair is during37,before18,after1,absent1:
do not apply one attribution to every gap. Full report/archive closes separately.

Next exact transaction: inspect clipped request-repair avoidance before adding
broader owner/deadline instrumentation. Source finds lower target selection
uses sent_instances_for_frame's exact-offset map lookup (f4206d0b, retained
through exact-instance migration), but actual scoring can start inside a retained
Original/copy extent. The outer uniform-frontier guard (53d9ab59) correctly
refuses selecting an already-owning incarnation, yet returns a default model
without selecting the real alternate. If reached, this can skip a serviceable
region while later regions proceed. This is a reachable candidate, not yet a
proven cause of69779: actual Original boundaries/ACK ranges were not recorded.
The current repair FIFO cannot let a continuously queued older head be overtaken;
positive-only/narrower ACK scope cannot erase an already-known retained gap.

Information forecast: one cfg-only event at that EXISTING guard records the
actual refused scored extent, complete already-computed avoidance/owner set,
chosen exact instance and assignment observation. No new query/state/await,
timer, queue, selection or authority. Reuse existing admitted-repair and server
ordered-release events; one unchanged200+200QoS/outage UP capture. A material
stall-prefix overlap selects this wrong-range lookup for correction; absent or
noncritical mismatch stops its promotion as the stall remedy. Simultaneously
prepare one test-only real-producer clipped-gap counterexample; failure before
its intended assertion is a fixture failure, not Product RED. Root solebuild/lab.
Benefit magnitude is unknown before overlap: one1.288s boundary is not a promised
whole-run gain. A correction could restore timely alternate eligibility without
new traffic authority; opposite risks are changed target order/copy cost and
broader helper semantics. Audit all callers before choosing a range-aware helper
versus passing existing proven geometry. No runtimefix/RFC change yet selected.
Tag ordered-prefix-avoidance-diagnostic-0911; freeze/reverse observer beforelab.

07:15+08 execution: observerbuild28999 CLOSED0 in1m22, existing unused-wrapper
warning only. Independent review PASS;19feature-only lines, no new model query.
observed_timing is a pure first-observation aggregate, NOT retainedD or proof
the refused target was yet due. Exactpatch comparison passes; binary frozen at
./.tmp/reflection/bin/gap-avoidance-20260911/mptunnel. All observer source reversed
before solelab83561; no compiler overlap. target/release is this diagnostic, not
an ordinary comparator. One diagnostic if-statement differs from rustfmt style;
it is not a runtime fix and was removed intact, not edited during compilation.
Root read complete109line69779 appendix and verified9member archive integrity/
manifest; independent raw byte comparisons pass. Known-gap erasure hypothesis
is rejected by source: only actual covering positives remove retained G; exact
duplicate elimination does not hide new scoped omissions. Initial unknown
scope remains distinct. No additional ACK-publication fix or full-range log.

07:23+08 lab83561 CLOSED0: exact982056960B/42.996546s=182.723Mbps,1/1complete,
allacceptedconfirmed; maxconfirmation4.184808/write2.447173s.97.652MBdiagnostic
logs are not ordinary performance. The refusal guard actually fires76893times;
76257selectOriginal,636selectanexistingcopy. On real receiver head822407107,
37owner-targetrefusals span34.955→35.568s inside a.705s ordered-release gap;
first coveringQ0copy35.584s precedes release71ms. No direct target management
sample falls inside that whole gap; do not exclude every localwrite portion.
Othercriticalmatchesexist, but maximum1.976s receive gap hasNOrefusaloverlap
andNOopenhole;4.185sconfirmation also includes actualreturnholding whiletarget
advances65.012MB. This is material range-selection exposure, not all-stall cause.

Test60276 FAILED FIXTURE, not ProductRED: proposed fasterOriginal ranking is
false before recoveryassertion; panic underProductguard also poisons cleanup.
7582 repeats onlythat projection aftermovingassertionoutsideguard and confirms
bothpathsPortableStartup351472bps/333ms,notseeded20/100ms. Sharedhelper seeds
PerFlowGoodput, intentionally stripped from unrelated path-capacity authority;
owner29200BProductdebt then makesit slowerthanemptyalternate. No runtimefault
follows. Correctthefixture's declared typed initial-rate-mbps AND initial-srtt-s
inputs (existingconfiguration), keep actualclaims/ACK/debt/clocks, and recheck
actualscoresbeforeintendedRED. Do not boostruntimehints or mutateflightage.
The failedpatch/log and projectionlog stay preserved. No newlab or runtimefix
is selected from testcasefailure. Currentcompile/labidle whiletestauthorcorrects.

07:26+08 TRUE RED46767 CLOSED101 at intendedassertion0vs14600B after all
actualclaim/receiverACK/firstacceptedcopy/unsuppressednextslice/availabletarget/
immutablematurity controls pass. Typedfixturepriors200Mbpsboth,20/100msRTT
areassertedfromactualprojections beforetheknownfasterOriginalrank. No artificial
flightaging or nativeclockadjustment.33xlineproducerfixture stays onecountercase,
not a newharness. ExacttrueREDpatch/log separatelypreserved fromfailedfixture.

Selected bounded correction: target ranking must consume the SAME byte-range
ownership alreadyrequiredby the finalguard, not rederive it fromframe-startkeys.
Pass an explicitrequiredscoring-avoid slice through both productiongap and
retainedfallback extentadapters beforelowerselection. No optional fallback,
extra ledgerwalk/index, newclock/quantum/queue/rate or globalhelperreinterpretation.
Reviewalsofoundownership/eligibility distinction: actor-attached expiredcopies
stillownbytes whiletemporarilyineligible. Useexistingfrontiersweeps/view withthe
actualattachedmask; separatelyretain thecurrentcapableOriginal requirement.
Forward fullscoredM exclusions, notonlyfirstprevieworsoleOriginal. Freshnative/
policy/Regular-before-Backup andfinalApply checks remainindependentandunchanged.
This avoids silentlyclaimingall-attached rangecompleteness fromtheoldexact-key
union. Include a focusedopposite-copy/mask control; noresponse rewrite because
responsealreadyusesoverlapqueries. RFCclarificationwillstateexistingrangecontract.

Benefitforecast: remove demonstratedfalse-no-target service for clippedregions,
including .613s observedrefusalspan onablockingprefix. Thatspanis exposure, not
a measured whollyremovable delay;1.976smaxreceivegap andreturnholdingremainoutside
thiscause. Extra timelycopiesmaycostwire/sharedlatency; a lowercallcountdoesnot
promiseCPUorMbpsgain. AfterRED/GREEN+independentmodelreview, oneordinarycandidate
on the SAME200+200QoS/outageUP cellagainstretained011 baseline. Evaluatefull
target/confirmationseries, firstservice, everygap/completion andcost. Adverseor
no materialbenefitstops promotion; nofavourablererun or tuning. Ifsupported,
affectedhealthyshared500mixedDOWN latencygate follows, not automaticrelease.

Root read complete115line83561appendix and verified11member archiveintegrity/
manifest; independent bytecomparisons pass. Telegrammilestone sent23:27:19UTC,
nextnonurgent>=00:27:19UTC; no CPU-resolution/performancevictory claimed. RFC's
existing range-based exclusion contract is clarified beside its ranked-frontier
rule; codecorrection is underindependentmaskreview, notacceptedfromcomponentRED.

07:38+08 execution: production/RFC independent review PASS; required full scored
range exclusions reach both production adapters, union existing attached history,
and leave final guards, per-query native/policy eligibility and clocks intact.
The test-only wrapper now asserts that its entire scored extent is uniform,
instead of silently forwarding a first-prefix mask for a larger score. Added
one lower-layer opposite control: an expired, temporarily ineligible but attached
copy still ends the uniform extent; exact detachment removes that boundary but
does not acknowledge its bytes. It is not mislabeled as the real producer RED.
Root read the entire actual patch and starts sole GREEN79627, four compile jobs.
No ordinary lab until tests close; no controller/queue/hint/profile change.

07:40+08 GREEN: root79627 passes13client checks including the actual producer
0→14600B counterexample. All74multipath and58request checks pass, including
the clipped expired-copy control, retained fallback, same-key replacement,
fresh Regular/Backup, exact reserve, ranked extent/Apply and copy suppression.
Six assignment-clock checks plus one end-only epoch churn control also pass:
152distinct focused tests. All three independent actual-source reviewers PASS.
No semantic edits after GREEN. The model correction is proven at that scope,
not accepted as a speed improvement. Exact five-file patch is frozen at
./.tmp/reflection/clipped-range-candidate-0911.patch. Next ordinary build with
four jobs, no diagnostics; then the one predeclared outage UP comparison.

07:42+08 ordinary build91090 CLOSED0 in1m23, existing unused-wrapper warning
only. Frozen patch exactly matches04f1e56. No source edits during/after build;
the unrelated userdoc remains untouched. Sole lab95178 uses explicit
clipped-range-20260911 binary and tag clipped-range-outage-candidate-0911,
the declared independent200+200/UP70DOWN30/46UP10/UDPoutage profile, default
config and management+target observation, no diagnostics or compiler overlap.
Compare complete ordinary timing/cost, not live partial rates.

07:50+08 ordinary95178 CLOSED0:960823296B/43.614186s=176.241Mbps versus
011186.629 (-5.57%). Every accepted byte confirms; no failures. First write/
confirmation .105407/.408633s versus .105107/.407922s. Worst write2.636582→
1.865375s and confirmation6.382488→1.717023s, raw zeros10→1 are material
continuity improvements, not "no benefit". However late36–38raw bins4.717/0/
2.623Mbps remain poor. Actual server target790859308B stays flat over distinct
36.956163/37.956163s snapshots, then reaches791186988 at38.956163:327680B
over2s, versus baseline185.855Mbps in its own corresponding target window.
Source−target=64MiB at both later snapshots. Q46/Q47nativeACKs advance39.171/
42.051MB with stable epochs; this is neither whole native freeze nor solely
held confirmation. Independent source correction confirms public target totals
come from ObservedProductIo successful poll_write/vectored, not PathDeliveryStats
pre-write receipt. They include successful partial writes, not whole flushes.

Costs: sampledUP1.698→1.803GB, wire/confirmed1.6866→1.8760 (+11.23%); peak
summedUPbacklog23.89→38.66MB, clientnativeflight24.34→61.23MB. These are sampled
windows, not lifetime exact efficiency. Five repeated client management producer
timestamps and one server repeat must not be treated as fresh observations.
No source-level CPU attribution from lifetimeps. Full independent appendix and
archive in progress. No healthy-gate promotion or favourable ordinary repeat.

Disposition: retain04f1e56 as **exact defect fixed / composition unaccepted**.
Unlike the elective f8 feedback-boundary change, this repairs an existing exact
range-selection contract with actual producer/live-prefix evidence. Its necessity
does not waive the observed wire/queue/late-service tradeoff; one local fix is
not required to solve every unrelated stall, and one pair does not prove it
caused this one. No new runtime correction may be stacked without attribution.

Next bounded information transaction: reuse ONLY the client/server prefix fields
from ./.tmp/reflection/prefix-owner-trace-final-0911.patch on04f1e56, not its
owner/perf infrastructure or old executable. One coherent client Product sample
of assigned A, unassigned U, retained C, positive F, known G and peerMAX; server
received R/reorder plus completed-write W. Sample at existing service points,
once per second, selected stream0. Keep successful write/flush completion
accounting across unlogged batches. Paired sampled write begin/end distinguish
their exact await residence; unlogged long writes remain unknown, not proof of
absence. Public target totals/sink queues corroborate partial write acceptance.
No new async task, waits, controller, timers, queue, eligibility or ownership.

Question: does late actual target stagnation have A=R=W with source U waiting,
R>W pending delivery, or A>R with an actual receive hole? The first selects
assignment/feedback ownership, second local write/service, third exact missing
range recovery. Silence is not a state; snapshots cannot prove native or reader
starvation without another exact readiness witness. No critical plateau means
no attribution, not a favourable performance result or automatic extra capture.
This reuses a previously decisive low-volume discriminator instead of another
large repair log. One unchanged200+200QoS/outageUP capture; freeze/reverse the
two-file feature-only overlay before traffic. No claimed speed gain from it.

07:52+08 observer subset root/independent review PASS: only control.rs(+53) and
server.rs(+109/−2), copied from the saved earlier instrument with current context/
formatting. No owner/perf files restored. The existing write future is constructed
once and its result propagated unchanged; successful W advances for every batch,
errors preserve unknown partial progress. Sample await includes feedback wrapper/
scheduling and begin-log cost, not pure socket CPU/time. Sole featurebuild30981
is running, fourjobs; exact patch frozen at clipped-prefix-observer-0911.patch.
On completion freeze bin/clipped-prefix-20260911, reverse both source files,
then ONE tag clipped-range-prefix-diagnostic-0911 with request_prefix_state,
server_request_prefix_state,server_request_target_write,server_receive_hole,
server_receive_delivery_stall; selected stream0. No all-repair/ACK/perf logging.

07:54+08 build30981 CLOSED0 in1m23, existing unused-wrapper warning only.
Exact two-file patch unchanged during build; binary frozen and all observer
source reversed before solelab77827. Root's first reverse-patch converter read
git diff -R's swapped a/b header incorrectly and failed before modifying files;
corrected header parsing and empty source diff verified before launch. No Product
or measurement failure followed. No compiler/lab overlap. Root read all160lines
of the95178 appendix and verified21-member archive integrity/manifest; independent
raw byte checks pass. Main source04f1e56; only user's seven-line file unrelated.

07:59+08 diagnostic77827 CLOSED0:862453760B/43.384432s=159.035Mbps; allaccepted
confirmed/noerrors, maxconfirmation2.828028/write2.730630s. Only230lines/86123B
logs, but still diagnostic, not ordinary acceptance or a speed regression claim.
Critical prefix R484689200: client24.143640/25.343640s has A=peerMAX551798064,
U=0,F484689200,G starts[484689200,484754736), retainedC7.383→2.252MB and repair
queue0. Server24.329640/25.329640 has R=W484689200 with64.838→64.857MB reorder
and matching first gap. Actual successful target writes plateau there too.
Receive advances4026568B at25.945640 after2.551160s gap; its matched write
completes in1719us. This episode is assigned receive HOL exhausting ordered
credit, not withheld source assignment or a parked target write. Do not transfer
that attribution to every episode or identify the Original from aggregate paths.

Q46nativeACK stops advancing across fresh15.95–23.95s native producer samples,
while Q47 progresses. Later Q46native sampled_at freezes, so later management
timestamps are not fresh native observations. No timer/CC bug follows without
the missing prefix's exact owner/copy admission and retained deadlines. The
next source review is selecting that narrow discriminator; no new overlay or
runtime fix selected yet. Complete independent archive/appendix closes shortly.

Selected08:01+08 information transaction: extend the existing1Hz selected-stream
prefix capture with exact first-byte F ownership and ACTUAL head-recovery outcome.
One read-only ledger pass per prefix sample may expose retained records covering
[F,F+1): exact incarnation/attached status, immutable assignment and current span,
sent_at, retained Original loss/fallback Option, or accepted-copy suppression D.
None means not yet observed, not mature. Do not call a timing observer/native
snapshot/model only to manufacture complete diagnostics or mutate its clocks.

Actual gap-service observation samples its head BEFORE queued/live-copy coverage
is subtracted. Keep those two covering causes separate; if head survives, record
only already-computed actual head model/retained deadline/target and pending or
exhausted result. Report chosen later range separately. No later-region timing
as proof about F; no fresh target eligibility claim when coverage skipped it.
Expose no-frontier/capable-owner, missing-clock, future-clock, due-no-target and
due-ready distinctly. Retain client F versus first authoritative G relationship.
The prior two-file R/W/target capture remains the exact receiver correlation.
Include existing evaluator pre-gates (no alternate/zero limits), post-selection
service/extent limits and actual enqueue outcome: due-ready is not admission.
Use signed offsets from the sample Instant for clocks; absent and expired differ.

Question: is the observed missing head withheld by a still-live copy/queued
repair, its Original clock, or evaluated target availability? These select actual
native-copy service, timing-contract scrutiny, or target/wake scrutiny respectively.
Existing77827 proves the stage but omits these facts; no additional broad audit
or congestion change follows. Information forecast only; no promised speed gain.
Keep all clock/queue/selection behavior unchanged, one bounded observer state,
no generic framework/per-frame history. Once-per-second diagnostic ledger scans
and formatting may perturb timing, so only ordinary95178 remains acceptance.
One unchanged200+200QoS/outageUP capture; no favourable performance rerun.

08:07+08 preparation checkpoint: root read the complete112-line77827 appendix
and verified gzip integrity/all11regular archive members; independent input-byte
checks pass. Author is implementing the declared observer, reviewer independently
checks actual evaluator/ledger boundaries. Expected six feature-only files include
the existing two prefix hooks and narrow sender/ledger bridges. No new runtime
fix, compiler or lab is active; ordinary04f remains composition-unaccepted.

08:15+08 observer review PASS from root and independent actual-diff reviewer.
Six-file feature-only overlay is frozen at exact-head-observer-0911.patch;
sample state is an ordinary Option, no RefCell/Sync change. Actual coverage
uses the existing combined vector's pre-normalization copy/queue slices. Raw
retained fields precede the evaluator; only its real timing observation supplies
the separately labelled evaluated retained clocks. Shorter same-head retries
clear superseded observations; later ranges cannot overwrite the head. All three
early returns and normal completion finalize the sample; actual enqueue counters
do not imply native acceptance. One selected sample per second, no new runtime
authority or query. Root sole four-job feature build next, no lab overlap; freeze
bin/exact-head-20260911 and reverse all six files before the declared one capture.

08:16+08 build96258 CLOSED0 in1m26, pre-existing unused-wrapper warning only.
Frozen source patch unchanged; executable copied to bin/exact-head-20260911.
All six observer source files reversed with apply_patch and empty diff verified
before the sole exact-head-diagnostic-0911 lab. Same200+200QoS/outageUP and the
six declared head/prefix/write/hole events, selected stream0; no compiler overlap.
target/release is diagnostic, not the ordinary04f executable. Source04f remains
clean except docs and the untouched userdoc. Full ordinary acceptance unchanged.

08:23+08 diagnostic19038 CLOSED0:953221120B/44.779863s=170.295Mbps,
allacceptedconfirmed/noerrors; driver45.546014s. Maxconfirmation4.704106s and
write3.290572s are diagnostic, not ordinary regression claims. Exact largest
receive gap is3.979938s atR479411258,21.164629→25.144567s; matched target write
1384us. At22.154910/23.158446/24.273281s, the same sole attached TCP0 physical4
Original[479411258,479476794) owns F; no copy or queued coverage. ACTUAL head
model has Q1 physical1 measured/notpending/notexhausted, targetETA.637/.958/.122s,
but future loss clock prevents enqueue. Retained loss≈25.912683/fallback27.097927s
remain fixed across samples, not renewed. Original assignment≈19.986464s;
current ownerRTT5346.596ms/legacyETA16.054s is not exact remaining head residence.
TCP46 live socket evidence independently confirms real multi-secondRTT and
continuing ACK progress; some management-native stamps later freeze. Q47 ACKs
continue. R=W=T stays athead through21.976–24.976s; winner atrelease is unknown.
The larger confirmation gap also includes reverse reorder, not allforwardHOL.

Information forecast succeeds: this stall has actual Original-clock withholding,
not unassigned source/localtarget write/accepted-copy suppression or absenttarget
at those sampled decisions. There are45 sampled evaluator calls: zero head
enqueue but15 later-range insertions/182336B, not zero whole-run repair. Most
other samples are live-copy covered; do not generalize one head cause to all.
Reader archives full series/cost/profile; no runtime fix or promotion follows.

Next pre-code decision is the timing policy, not timer renewal or a new BBR
parameter. Historyfaee89d borrowed native-style loss thresholds to replace an
older3-interval wait; later exact clocks and owner-completion comparison retained
the early-launch floor. RFC8684§3.3.6 leaves cross-subflow reinjection to local
policy; RFC9002§6.1.2 describes packet-loss inference inside one packet-number
space, not a mandatory Product cross-carrier safety gate. Native rules remain
unchanged. Sources: https://www.rfc-editor.org/rfc/rfc8684.html#section-3.3.6 and
https://www.rfc-editor.org/rfc/rfc9002.html#section-6.1.2 .

Blanket pre-loss hedging is NOT selected: known healthy ACK-return delay and
aggregate owner suffix debt can produce useless extra copies/shared queue harm.
Root is reviewing a narrower ordered-credit rescue contract: exact authoritative
omitted F, A=peerMAX (no new DSNs available), no queued/live copy, and a normally
measured/fresh distinct target. One existing-quantum head repair could avoid
waiting for the congested Original's loss classification without declaring native
loss or using a fallback deadline as a delivery estimate. This is a candidate
policy distinction, not yet implementation or acceptance. Independent critique
must establish its meaning across sparse/healthy/asymmetric/shared conditions.
Conditional information/gain forecast: earlier alternate might remove part of
the observed~2s credit-exhausted interior, but could lose to Original/ACK arrival
or cost shared service. No guaranteed .122s latency or whole-run Mbps gain is
inferred from ETA. Existing clocks, exact exclusion/D, admission and all global
gates must remain; no coefficient/queue/window change or blanket copy ban.

### Selected ordered-credit head rescue pilot,08:27+08

Issue: the measured known receiver head can hold all assigned credit while a
distinct measured alternate exists and only the live Original's local loss
classification floor prevents trying it. This is not renewed-clock corruption.
Independent history/model critique confirms the floor is local anti-duplication
policy. Root read all145lines19038 and checked11-file archive integrity/manifest;
independent input-byte checks pass. Evidence checkpoint e2375e9 preserves it.

Small model change proposed for the REQUEST owner only: when F is exactly the
first authoritative gap start and assigned A equals actual peerMAX, treat that
head as urgent ordered-credit repair. Only its exact ranked quantum M may bypass
the Original maturity comparison. Never apply the exception to later enumerated
gaps, absent/unknown Original ownership, missing target/model evidence, a live
copy/queued overlap, exhausted actual service, or stale membership/Apply. Keep
all Original loss/fallback fields unchanged; urgency is not native packet loss,
a new timer, a fabricated deadline, or fallback/aggregate-owner ETA as proof.

"One" means one existing-quantum action per invocation, not one per lifetime F.
After an accepted copy's immutable D expires, a still-distinct attached slot
may receive another attempt through the existing exclusions/capacity model.
Credit exhaustion is a decision-time urgency trigger. Existing bounded queued
intent may survive later MAX relief; covering positive ACKs still prune it and
fresh exact target/native Apply is unchanged. This deliberately accepts that
after-relief duplicate risk rather than creating a new per-head attempt epoch,
queued-cause framework or cross-layer MAX cancellation contract. Debt bounds
do not prove a low cumulative duplicate fraction. No shared timing helper may
silently extend the pilot to responses, where behavior remains unchanged.

Forecast: earliest actual credit-exhausted witness23.17s bounds this policy's
observed opportunity to at most the remaining~1.98s, not the full3.98s gap.
Advisory targetETA gives a conditional~.75–1s head-service opportunity, not an
arrival guarantee;14.6KiB quantum covers only part of the64KiB group. No gain or
regression remains plausible. Healthy small-window/high-BDP and asymmetric
feedback can also exhaust credit with old gap knowledge; window size is NOT
changed to manufacture urgency, and default64MiB rarity is not a safety proof.
Shared contention/earlier-copy wire/CPU/native queue/latency costs are explicit.
Value: material seconds-long credit stall, not a micro-optimization or pursuit
of an average. Narrow cause scope excludes unsaturated normal reordering.

First test-only reachable RED via actual Original claims, receiver-produced gap
and actual peer credit exhaustion, with true measured target and future retained
clock asserted before the intended failed service/enqueue assertion. Existing
typed priors may establish the declared test topology; no fake aged ledger or
runtime parameter change. Opposite controls retain credit-available waiting,
non-head waiting, live-copy/queued exclusion and exact clock immutability.
The minimal UNIT fixture uses one legal receiver-produced64KiB MAX advertisement
and actual sender credit update, not a reduced Product resource configuration.
This avoids1024claims merely to reach default64MiB; ordinary labs keep defaults.
Reassert the retained loss deadline is future after evaluation so a host pause
cannot make ordinary matured recovery falsely satisfy the intended RED.
Root alone runs tests/builds/labs. Only after causal RED implement the small
request decision predicate/RFC exception, independent review, focused GREEN.

Then ONE ordinary same200+200QoS/outageUP pilot against preserved04f95178:
complete source/target/confirmation series, first service, all stalls/settlement
and wire/CPU/RSS. No observer numbers as comparator or favourable repeat. Material
timing improvement without adverse composition selects healthy/shared controls;
no benefit/adverse cost stops promotion and triggers exact attribution/rejection.
This pilot cannot claim all mixed latency, CPU bursts, or response-side stalls
resolved. Unchanged global baseline/browser/both-direction gates remain required.

08:35+08 first test compile8616 CLOSED101: fixture used sender-private
front_reinjection from the relay test module. This is a test visibility error,
not Product RED. Preserve ordered-credit-head-red-0911.patch/log. Correct only
the test's read through the existing public queue front (source is empty);
do not widen runtime visibility or add an observation API. No runtime change.

08:36+08 independent pre-code wake review finds a required composition detail:
keep clock_due distinct from urgent service. Future Original deadline retention
and existing shorter-assignment retry must still depend on clock maturity, not
the widened urgency predicate. Otherwise an urgent but unavailable target could
silently lose its timer wake or shorter-range opportunity. No new wake/clock;
retain the actual previous obligations while adding the exact-head opportunity.

08:36+08 true producer RED52235 CLOSED101: all claim/receiverMAX/ACK/exacthead/
measuredtarget/room/no-copy and POST-evaluation future-clock assertions pass;
intended enqueue is0vs14600B, ready=false/measured=true/exhausted=false. This
proves the local rule withholds this reachable service; practical benefit remains
unproved. Root implements only the request urgency predicate, keeping clock_due
separate for both previous deadline and shorter-boundary obligations. RFC15.2
explicitly permits the bounded local exception and records response scope and
duplicate/MAX-relief tradeoffs. Independent actual-diff/opposite tests follow;
no native, target preference, queue, window, multiplier or profile change.

08:43+08 GREEN96843:14client checks pass, including the actual producer's
0→14600B rescue and one shared fixture's real unmeasured/blocked target wake,
nonhead, queued/live-copy, actual ACK/MAX relief and unchanged clock controls.
Further74multipath+57request+6assignment-clock+1epoch+1ordered-terminal checks
pass (153distinct). Runtime/RFC and expanded producer independent reviews PASS.
The test's claimed target delivery is command/flight admission, not a physical
network result. No semantic edit after GREEN. Next freeze isolated candidate
checkpoint/patch, sole ordinary four-job build, then the ONE declared comparison.
There is still no practical promotion or CPU/whole-mixed resolution claim.

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

07:50+08 read-only follow-up finds no proven spin. Native BBR3 callbacks retain/
scan packet metadata and mutable journal epochs for current ACK/late-terminal
evidence; checked journal admission still compacts before its absorbing RawOnly
fallback. Finite storage does not prove cheap callbacks, nor their CPU dominance.
A harmless task-clock profiler preflight is permission-denied; no sudo/settings/
capability or attach bypass followed. User was asked asynchronously for scoped
sudo lab profiling approval; none received yet. No new CPU observer/experiment
selected while main ordinary recovery attribution continues. Incident remains
unassigned, separate from the demonstrated low-CPU late native contraction.

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
Telegram exact-head milestone delivered by00:28:10UTC; next nonurgent>=01:28:10UTC. Commentary within60s; verification
polls by minutes. Before compaction record current sessions, next decision,
source/binary identities and open/adverse outcomes. Do not stop at a checkpoint.
