# Ordered repair is not native queue admission

2026-09-06 12:17 UTC. Focused continuation of MIXED_RECOVERY_QUEUE_DIAGNOSIS and
CURRENT_CLOSURE_PLAN. No new runtime policy or release acceptance.

## Architecture finding

TCP need not alter QUIC's congestion controller to delay a mixed Product
stream. Original byte placement creates a dependency across their independent
native streams. A later QUIC repair is also new data in its own ordered native
stream; it cannot overtake the bulk suffix already handed to that stream.
This is distinct from physical shared-bottleneck contention and from the
separately demonstrated Product mailbox/actor service defects.

The current RFC explicitly acknowledges nonpreemption of native-accepted bytes
in10.4. Its15.1 immediate-admission rule nevertheless forbids declining the
only currently queue-admissible original placement, even when another live
carrier has a much earlier advisory completion. Section15.2 priority operates
before irreversible native handoff. Thus the observed behavior can follow the
written rules and still be a poor ordered-service architecture. More BBR gain,
different loss thresholds, or a fixed QUIC preference does not repair this
boundary. Changing the allocation contract requires an explicit RFC revision;
the accepted exact ACK/copy/lifecycle rules are not the defect to remove.

## Introduction history and the incomplete acceptance argument

Commit65edae3 (2026-09-04) removed the ordinary response ECF completion veto
and inference-derived reorder budget while introducing the structural
`BulkProductResourceCheck`. Its purpose was legitimate: low or stale rate
predictions must not shrink configured Product resources or keep a recovered
singleton idle. The request-side BDP limit and response-side placement wait
were nevertheless different behaviors handled in one transaction.

T04B_STRUCTURAL_PRODUCT_ADMISSION proved invariance of resource permission
under changed estimates. It then treated the absence of immediate dispatch
as an admission defect. That inference is too strong: a scheduler can have
permission to publish and still choose a finite advisory wait. The old `None`
result did not represent that distinction or independently prove discovery
and wake liveness. Removing the veto fixed that ambiguous denial but supplied
no replacement ordered allocator. Existing rank compares only admitted
outputs; it cannot select a busy faster output as an alternative action.

Formally, resource permission A depends on structural state sigma, not
prediction e: `A(sigma,N,e1)=A(sigma,N,e2)`. Dispatch choice D may and should
differ with e while remaining subject to A. The former does not imply
`D(sigma,N,e1)=D(sigma,N,e2)` or mandate immediate publication whenever A is
nonempty. The former acceptance argument also confused preservation of the
configured exposure ceiling with preservation of actual exposure and latency.
More committed work can remain beneath the same64-MiB ceiling and still
delay a frontier by seconds. This is an incomplete performance model, not a
reason to weaken exact debt accounting or revive a fake inferred window.

The historical component tests and independent audit are retained as scoped
evidence. This review does not retrospectively claim all old performance was
caused by this single commit; no same-build parent/current comparison was
performed here. It identifies the exact removed decision and the obligation
the non-regression argument failed to cover. Bounded wait/discovery must
replace that obligation explicitly rather than reverting every structural
admission correction.

## Current candidate trace, without deliberate QoS or outage

`mixed-combined-down-cooperative-frontier-trace-0906` runs the current native,
qualification, mailbox and cooperative Product-service candidates with existing
diagnostics. The routed shared cut is500 Mbps in each direction. Asymmetric
loss/jitter remain; the10-Mbps QoS step and UDP outage are disabled. No build
overlaps the run. Its136.842-Mbps mean is diagnostic, not an acceptance rate;
the maximum application read gap is2.279s. All original assignments are logged.

The exact Product frontier is588416410; the corresponding HTTP body offset
is588416202, differing by the208-byte response header. Timestamps below are
relative to the server's first diagnostic event. Client monotonic clocks have
a different origin; the common wall timestamp aligns their events here.

| Relative time s | Event |
| --- | --- |
|31.473|Original65,536-byte range assigned to TCP path2.|
|34.593|Client observes a hole at that range with a later QUIC suffix.|
|34.626|Server queues and accepts14,600-byte QUIC frontier repair.|
|34.721|Server accepts a65,536-byte TCP path1 repair after the first copy's suppression deadline.|
|35.426|The original TCP owner becomes stale; subsequent stale handoff finds no vacant target for the complete preview.|
|36.872|A TCP frame advances the client frontier by97,056 bytes, with45,704,128 bytes of reordered suffix already held.|

The client diagnostic's `source_path_index=0` is the configured TCP group,
not server PathId0; do not use it to distinguish original TCP2 from repair
TCP1. The trace proves the winning carrier family, not the exact TCP copy.
The QUIC repair's2.315s advisory ETA is not its measured queue residence.
Separate native-handoff/receive events are being collected to determine that
residence; do not assert that a command acceptance timestamp is transmission.

The preceding ordinary candidate run independently shows the same separation:
between25--27s QUIC native ACK bytes advance389041393 ->439338022 while client
Product delivery remains382258596. That is continued native service during a
logical gap, not proof of a stalled QUIC controller. In the current trace,
native queue/flight dashboards also do not enumerate all buffered QUIC stream
data: `CongestionMetrics.pending_bytes` is the outstanding H3 write transaction,
released when H3 accepts data, not Quinn's full unacknowledged stream buffer.
Consequently a displayed zero queue does not establish zero repair predecessors.
No metric or scheduling interpretation has been changed from that observation.

## Necessary model constraints, before implementation

For exact Product byte u, let A(u) be the earliest usable arrival among its
physical copies. Ordered completion through x is `F(x)=sup(A(u), u<=x)`.
Summing native rates or maximizing immediate enqueue work does not minimize F.
For a repair with B bytes ahead in the same reliable native byte stream, even
constant service r and no further loss require at least `8*B/r` seconds to
reach it. Four MB ahead at4 Mbps costs8s;60 MB at200 Mbps costs2.4s. These are
conditional dimensional examples, not measurements of this trace's native B.

Three desirable unconditional claims cannot coexist with the current action
set: (a) immediately use any admitting path, (b) never lose ordered latency to
waiting for a busy faster path, and (c) discover arbitrary unknown capacity
without service or extra traffic. A ready slow path/busy fast path defeats
(a)+(b). Two networks with identical observations but different untested-path
capacity defeat a guarantee of(c). Native controllers also cannot know an
unannounced future QoS change. This does not waive avoidable software stalls;
it identifies which earlier absolute requirements need explicit tradeoffs.

A replacement needs two distinct owners: ordered allocation/discovery before
irreversible handoff, and a truthful service position for repair after handoff.
Resource headroom must remain a safety check, not a claim of prompt service.
Choosing to defer needs an actual wake, finite validity and failure fallback;
successive heads cannot indefinitely postpone an unknown useful carrier.
Exploration cannot silently borrow Product ACK qualification from duplicated
bytes. Native-writer acceptance cannot be relabelled network service, and
repairs cannot erase exact-copy occupancy just because their deadline expired.

Shrinking every native buffer to one frame would sacrifice high-BDP pipelining;
a universally larger buffer aggravates ordered repair delay. A separate repair
lane could bypass an earlier stream's bytes but would change attachment,
ordering and admission ownership and could introduce cross-stream starvation.
None is approved merely by this trace. BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL's
unknown-capacity counterexample remains binding until replaced by a complete
discovery contract. The next discriminator is exact QUIC handoff versus receive
time for the missing range, not another throughput-only parameter sweep.

## Handoff discriminator: the repair is behind preceding QUIC work

`mixed-combined-down-cooperative-handoff-trace-0906` retains the same candidate
and impairment configuration. Two opt-in diagnostic events record the existing
H3 write boundary and each client Product frame, including late duplicates.
No scheduling, queue, priority, rate, timeout or protocol setting changes.
The trace gives142.141 Mbps and a2.306s maximum application read gap; it is
still a diagnostic run, not ordinary performance acceptance.

For Product frontier226292404 (body226292196):

| Common Unix ms | Event |
| --- | --- |
|1788695526033|Original65,536 bytes assigned to TCP path1.|
|1788695529712|Stale-owner recovery commits the65,536-byte frontier copy to QUIC; Product queue delay is0ms.|
|1788695529713|The H3 write returns successfully after134us, on native StreamId4.|
|1788695529751--1788695532088|The client processes4,069 preceding QUIC Product frames, totaling44,241,864 payload bytes.|
|1788695532089|The first12,000-byte record of that repair reaches Product receive and advances the frontier.|
|1788695533731|A later TCP copy arrives after the frontier has already advanced past311MB.|

The handoff-to-first-repair-observation interval is2.376s. The receive reordering
buffer holds63,164,704 bytes when that repair starts to unblock it. This
separates prompt Product/native-adapter submission from delayed ordered
delivery. It also explains why raising pre-native repair priority alone cannot
move this repair in front of the already committed H3 stream sequence.

Precision limits: H3 write completion is local handoff, not packet transmission
or peer receipt. The44.2MB is **received Product payload preceding this repair**,
not a measured unsent wire queue at handoff. It may span native send buffers,
in-flight packets, native receive ordering, and local receiver queues. Do not
convert it into a literal link-drain lower bound without native stage evidence.
The h3-quinn0.0.10 adapter's receiver explicitly requests ordered Quinn chunks;
its sender drains the pending write into Quinn's buffered write interface.
This confirms the stream-ordering boundary, not an upstream defect.
[Pinned adapter source](https://docs.rs/h3-quinn/0.0.10/src/h3_quinn/lib.rs.html).

The complete probes, exact relevant events, handoff counts and surrounding
management/process snapshots are in ORDERED_REPAIR_SERVICE_EVIDENCE_20260906.json.
ORDERED_REPAIR_HANDOFF_DIAGNOSTICS.patch preserves the instrumentation; both
instrumented source files are restored to their previous content after saving
the diagnostic binary. No diagnostic hunk remains in the runtime worktree.

## Exact next transaction

The mixed ordered-service failure is reproduced after the mailbox/actor
corrections; they do not close it. The next model decision must own both
**where new ordered debt is created** and **which preceding work a repair must
wait for**. Merely making the repair timer earlier, increasing BBR aggression,
or suppressing TCP globally does not meet that obligation. First specify the
allocation/discovery and irreversible-handoff contract, with explicit
nonclaims for unknown future network service. Only then change the affected
owner and require busy-fast/free-slow, unknown-fast, reversed TCP/QUIC quality,
shared/independent capacity, singleton and abrupt-failure counterexamples.
Ordinary mixed/QUIC timing comparisons follow; the unchanged global upload,
browser, aggregation, sustainability and baseline gates remain required.

## Response-only placement ablation: insufficient, not an accepted rollback

The next bounded comparison restores only the former response ECF completion
comparison after unchanged structural resource checks. It uses the retained
measurement-start predicate; the old inference-derived BDP resource gate is
not restored. Native controllers, configured limits, request scheduling,
mailbox and cooperative actor candidates remain unchanged. No new parameter
or TCP/QUIC preference is added. Both ordinary binaries use the same routed
500/500-Mbps cut and asymmetric variable loss/jitter, without deliberate QoS
or outage. Runs are sequential, with no compilation overlapping traffic.

| Ordinary executable | Mean Mbps | First body s | Maximum read gap s | Echo successes / failures | Echo p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Current cooperative candidate |135.348|.728|3.523|80 /0|261.115|
| Response ECF comparison restored |136.161|.612|1.494|80 /0|300.308|

The restoration does not remove multi-second-scale ordered stalls or the
trickle/buffer-release pattern. These are different random loss realizations;
the smaller maximum gap in one run is not a statistical improvement or a
no-downgrade proof. The ablation's server peak RSS340016 KiB exceeds the
control303128 KiB; no leak attribution follows from those short snapshots.
Do not accept this rollback on its almost unchanged mean.

A separate diagnostic build confirms the restored predicate actually executes.
Its first rejection at server730ms is QUIC, not TCP: predicted candidate ETA
4022.275ms versus TCP lead1587.674ms, both with zero original flight. These are
the implementation's advisory coordinates, not measured service times. That
event does not prove QUIC physically slower at startup, or itself attribute
startup delay; it demonstrates why merely reinstating the legacy comparison
is not a protocol-neutral service proof.

The same diagnostic run still stalls1.633s at Product frontier166667194:

| Common Unix ms | Event |
| --- | --- |
|1788696780913|Original65,536 bytes assigned to TCP path0.|
|1788696785033|Stale-owner recovery accepts the frontier copy on QUIC, with0ms Product queue delay.|
|1788696785121|A further copy is accepted on TCP path1.|
|1788696787216|A TCP frame advances the frontier;59,995,968 bytes of reordered suffix were already held.|

The client path index is not the server PathId, so the winning TCP copy is
not uniquely identified here. The first diagnostic trace's H3 handoff proof
remains separate; this run does not add a new native residence measurement.
The145.670-Mbps diagnostic mean is not an ordinary acceptance result.373
logged no-target evaluations are attempts, not evidence of a busy-loop rate
or justification for removing occupied-copy checks.

RESPONSE_ECF_PLACEMENT_ABLATION_EVIDENCE_20260906.json retains all three complete
probe/interactive and process series plus the exact events. The ordinary and
diagnostic patches are archived separately. Both temporary source variants
were removed with apply_patch after saving their executables; the scheduling
file is unchanged in the active runtime worktree. No new production fix or
regression-suite acceptance is claimed for this experiment.

## Architectural scope after the failed simple restoration

The next transaction is a replacement allocation/repair contract, not another
completion constant. Its four distinct questions are:

1. **Permission:** do exact Product, attachment and native owners allow this
   action? Keep W/P/E, Data ACK, copies and cancellation semantics unchanged.
2. **Placement:** which permitted action improves the ordered prefix, including
   a finite wait for a live busy alternative? Permission is not an obligation
   to dispatch immediately. Unknown service is not a fabricated low rate.
3. **Discovery:** who owns a finite opportunity to learn useful untested
   service across successive source heads? A per-head timeout alone cannot
   prove this. Probe or duplicated bytes cannot silently become unique
   Product goodput, qualification or unambiguous carrier delivery evidence.
4. **Repair service:** what earlier work is irreversibly ahead of this repair?
   Pre-native priority cannot bypass existing same-stream native ordering.
   Either the placement/handoff model accounts for that dependency, or an
   explicit independent repair ordering domain is required. The latter is
   only a design alternative: attachment, connection credit, native scheduling,
   cross-stream fairness, duplicate and terminal ownership must be proved
   before any implementation. It is not permission to mint native credit.

Rewrite the affected15.1 allocation contract and refine10.4/15.2's service
boundary when that model is complete. A wholesale rewrite of unrelated wire,
authentication, ACK or lifecycle rules is not supported by these traces.
Do not preserve a poor policy solely because the RFC currently permits it,
but do not publish an unimplemented performance promise as a completed RFC.

Before code, resolve compatible service evidence and exact irreversible-work
ownership. Then check busy-fast/free-slow, unknown-fast/known-slow, reversed
protocol quality, immediate preferred failure, repeated estimate changes,
singleton, concurrent streams and high-BDP pipelining. A controller with only
past observations cannot guarantee optimal decisions under arbitrary future
QoS changes. State that nonclaim without using it to excuse the demonstrated
software/ordering stalls. The existing asymmetric up/down,500-Mbps shared,
200-Mbps independent, browser, recovery and sustainability gates remain intact.

## Exact mixed frontier after packet-class correction — 2026-09-06 20:16 UTC

MIXED_PLACEMENT_FRONTIER_EVIDENCE_20260906.json preserves selected exact events,
the full probe and dispatch totals from the existing diagnostic feature. The
binary contains packet-class checkpoint7677fd9 plus the still-held composition;
no temporary policy or new observation hook was added. This25-second steady
mixed download uses the existing500-Mbps routed cut, asymmetric jitter and
3% down /1% up loss, without deliberate QoS or outage. Its161.090-Mbps mean
is **not** an ordinary performance comparison. All50echoes succeed, but the
first six application seconds remain poor; maximum read gap alone hides this.

Bulk StreamId1's first dispatch is Unix1788724756254 (relative time0). QUIC
receives its first original at+.414s, before the large TCP suffix. First-second
original assignment is12,244,918B TCP versus739,764B QUIC. At+.514s:

| Candidate | Original flight B | Advisory completion ms | Decision |
| --- | ---: | ---: | --- |
| TCP1, live lower owner |10,551,296|242,299.737|Eligible; owns contiguous frontier|
| TCP2, additional |521,110|13,586.471|Unproven startup flight reached|
| TCP0, additional |524,288|13,591.169|Unproven startup flight reached|
| QUIC0, additional |477,620|28,559.265|Unproven startup flight reached|

The next65,536B range[12,132,714,12,198,250) goes to TCP1. The incumbent
exception and additional-flight saturation together leave the much-worse
advisory choice eligible. These ETAs are predictions, not physical service
bounds or permissible wait budgets. QUIC's sparse operational sample is153
Kbit/s versus266.208-Mbit/s pacing; neither absence of sustained evidence nor
pacing alone proves achievable rate. Do not erase the earlier QoS rate-meaning
correction to improve this comparison.

The exact range is copied by stale-owner handoff at+5.176s to TCP0,+5.378s to
TCP2,+5.833s to QUIC0. At+6.292s a QUIC frame containing its final5,536B closes
the lower hole and releases64,516,960B of ordered data;64,511,424B was already
reordered. This explains the544.927-Mbps application bin as buffered release,
not wire service exceeding500Mbps. No persistent-gap repair event covers that
final subrange before stale handoff. Earlier14,600-byte repairs often concern
different frontiers: their small size alone does not justify enlarging T06's
ranked service extent or rolling back exact-copy ownership.

A second exact case demonstrates that the issue is not confined to startup.
At+9.616s a51,616B original[215,519,722,215,571,338) is assigned to TCP0. The
live QUIC lower-owner reference has advisory completion1,388.920ms but is
absent from ready-candidate rows. The eligible TCP0/2 estimates are70,825.996
and81,957.730ms. The original is repaired onto TCP2 at+13.303s; a TCP frame
closes that range at+13.377s and releases19,410,656B, after3.761s of range
residence. Client source_path_index is a configured group index, not server
PathId, so this capture does not identify which TCP physical copy won.

The ready-candidate prefilter checks staleness, Product admission lifecycle,
command mailbox readiness, backup preference and rankability. The retained
lower-owner reference is evaluated separately. The events establish omitted
QUIC and admitted TCP, but do not log which particular prefilter removed
QUIC at that instant; do not manufacture that missing causal detail.

This strengthens the already-open T04b placement/discovery issue, not a new
SEEN batch. Deleting the incumbent exemption alone could stall all unproven
outputs and recreate a feedback-dependent pipeline cap. Deleting mailbox
readiness without an actual wait owner could select an uncommittable output
forever. Restoring the former ECF predicate already failed the ordinary timing
gate. The next model must distinguish permission, allocation and independent
discovery while preserving singleton service, high-BDP pipelines, exact
Product/transport authority and finite failover. No new runtime fix is claimed.

### Prefilter control and bounded deletion ablation — 2026-09-06 20:28 UTC

The follow-up uses one temporary observation-only prefilter event. All27 UDP
rejections are Throughput command-queue readiness, none lifecycle or stale.
There are23 subsequent TCP commitments with UDP retained as the live reference.
At+8.202s the QUIC mailbox is full; TCP2 receives65,536B; QUIC receives new
originals again6ms later. This resolves the wake-owner question for this run.
The temporary source hook was archived and removed immediately after freezing
the diagnostic binary. All50echoes succeed, but this is not a performance gate.

Do not overattribute: the first two TCP ranges were already buffered before
an earlier QUIC original's final5,536B released them3.095s later. Immediate
spillover is not universally the blocking range. Conversely,+18.660s commits
[412,871,034,412,936,570) to TCP2 while QUIC is queue-blocked; that exact TCP
frame closes the frontier2.940s later, with5.5MB still reordered. QUIC accepts
new work198ms after the original commitment. A second TCP-owned range takes
3.248s and leaves11.5MB reordered. The source does not identify which physical
TCP copy won merely from the client group index. Exact observations are in
MIXED_ADMISSION_REASON_EVIDENCE_20260906. Native queue and application receipt
remain distinct; a large advisory score is not the measured elapsed service.

Before any allocation redesign, one bounded **ablation, not production fix**
will test the startup exemption already implicated by both traces. Delete only
the multi-path live-contiguous exemption from the response selector's existing
unproven-startup-flight predicate. Preserve the separate true-singleton gate,
latency arbitration, qualification, all numeric envelopes, actual native
write/flush and Product ACK ownership. Restore the source after freezing the
executable, regardless of the result. No RFC change follows from an ablation.

Why this particular deletion: without a delivery proof, being the lowest
range's owner does not establish that accepting a large additional prefix
avoids cross-path blocking. It allowed12.24MB and23.39MB of TCP originals in
the two first-second observations. In the altered predicate, a multi-path
unproven output instead retains at most the same existing startup allowance;
Data ACK releases it and existing durable qualification ends that condition.
This tests the exposure mechanism without changing a congestion gain or cap.

Why it is not already an accepted clean fix: while qualification is absent,
the unchanged allowance imposes the necessary ceiling8*E/tau. At512KiB and
100ms feedback that is41.94Mbps per unproven output, not500Mbps. Qualification
may be delayed by native loss, ambiguous repair coverage or a poor return path;
the older high-BDP/singleton obligation cannot be waived. The change also does
not solve discovery starvation of a path never selected, later qualified
spillover, or native queue residence. A favorable bulk mean alone cannot justify
shipping it. Compare full startup bins, gaps and loaded latency against the
frozen ordinary packet-class candidate; reject or develop a different contract
if it merely exchanges one timing failure for another.

### Ordinary result: exposure deletion is not a standalone fix

2026-09-06 20:40 UTC. The corrected ablation and fresh ordinary control finish
without build overlap. MULTI_FRONTIER_ACQUISITION_ABLATION_20260906.json retains
both complete probes,1Hz observations and the exact two-line predicate delta.
The uncorrected first build was never used. The source was restored before
running the frozen corrected binary; no trial runtime change remains.

| Ordinary mixed steady | Mean Mbps | First body s | Maximum read gap s | Echo successes / failures | Echo p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Narrowed startup exemption |143.804|1.319|1.296|49 /0|172.269|
| Fresh packet-class control |168.104|.617|.309|50 /0|509.630|

The trial reaches an early buffered burst in its fourth application second,
but seconds6--9 fall to4.835--6.175Mbps and second21 delivers effectively no
bulk bytes. It does not close the timing gate. Echo p95 is better in this pair;
different random realizations prevent claiming that the predicate caused every
difference, including first-byte delay.49 versus50 successful echo attempts is
not a lost request: both have zero explicit failures within the finite run.

The failure is not simply an inability to drive QUIC. At trial6--9s native
QUIC ACKed bytes advance87.4MB to126.2MB while ordered client receipt advances
only62.3MB to64.4MB. QUIC flight drops to22.8KiB by8s while retaining a3.2MB
native window; TCP still has1.20MB notsent and later607KiB. At20--21s QUIC
ACKed bytes advance355.6MB to373.2MB while ordered client receipt advances
only128B, consistent with the concurrent echo. These are1Hz layer-localization
observations, not an attribution of a particular missing range in this ordinary
run or unique Product credit derived from native ACKs.

Why narrowing exposure is insufficient: it changes only the pre-qualification
predicate. It leaves already accepted native queues, qualified-path allocation,
queue-ready spillover and exact frontier recovery unchanged. Those service
boundaries can still create long ordered residence; in this trial TCP notsent
queues again reach roughly2.5MB. The earlier range-level controls establish
both TCP- and QUIC-owned blockers, so protocol preference is not a clean answer.
The ablation supplies no proof of general unknown-path exploration, high-BDP
non-regression or useful sustained aggregation. It is **not accepted** as a
production fix; do not repeat it with a different startup allowance.

Next remains one contract, not another parameter experiment: ordinary source
bytes may remain unassigned while a suitable live output is temporarily busy,
but discovery and failover must have independent finite progress. Evaluate
whether irreversible carrier binding is earlier than necessary without
equating write/flush with delivery, shrinking native pipelines or introducing
per-frame ACK stop-and-wait. Preserve the previously demonstrated unknown-fast
counterexample and exact partial-write transaction. A proposal lacking those
obligations is still not implementation-ready, even if it deletes more code.
