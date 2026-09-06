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
