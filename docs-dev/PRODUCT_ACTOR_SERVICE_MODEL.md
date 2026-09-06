# Product actor service: input priority is not drain-to-empty

2026-09-06. Candidate model, not runtime or release acceptance.

## Cause and original intent

The exact input-only obstruction in MIXED_UPLOAD_SOURCE_GAP_DIAGNOSIS holds an
eligible source behind incoming work for 2.633 seconds with positive Product
credit. Its next original reaches the server 33 ms after assignment. Commit
9505d7f intended buffered ACK/grant/lifecycle state to inform outgoing work;
its emptiness prerequisite instead makes source and sender service depend on
every carrier's ability to keep the shared Product input queue nonempty.
Neither a native rate increase nor a preference for QUIC repairs this owner.
The trace proves resource eligibility and the disabled read attempt, not the
local socket's readable-byte count throughout that entire interval. Existing
socket sampling covers the carriers, not the application socket. The RED
therefore supplies an actually ready source at the production arbitrator;
ordinary comparisons remain required for practical attribution.

## Replacement contract

The serialized Product actor has three independently ready service classes:
input, queued sender work, and source read. A turn selects the first ready
class in cyclic order, starting after the last class served. Initially input
is first. Disabled or pending classes are skipped, never waited on ahead of
ready work. The cursor advances only on a selected ready result. Existing
finite sender byte/item and ready-only read/data-batch quanta are unchanged.
Input events retain FIFO, exact incarnation metadata and individual validation;
this correction does not coalesce ACKs or make transport type a service class.

Let I, D, R denote ready input, dispatch and source-read work. A continuously
ready class is selected within at most three completed service selections:
there are only two other classes, and the cursor cannot pass it twice without
selecting it. An input producer cannot extend that bound by appending frames.
This is an event-service bound, not a wall-clock throughput promise. Recovery,
lifecycle and capacity notifications remain independent actor events; native
or local writes that already own bytes retain their protected transactions.
Actual Product credit, bounded buffering and exact writer admission still
govern whether source/dispatch work is enabled and can succeed.

If source resource eligibility is true but its read future is pending, input
must remain polled. Otherwise a purported fairness fix deadlocks an idle
application. Polling a read does not grant speculative bytes: a selected read
owns its actual reservation and payload exactly as before; cancelled pending
reservations release their existing resources.

## Safety and tradeoff

Applied ACK and grant state is always used by the next assignment. Unprocessed
input is not committed actor state. Even the old empty-queue check cannot
atomically exclude a reset arriving between that check and a ready read/send.
The new contract therefore preserves processed terminal/admission invalidation
and exact native precommit checks, not an impossible global arrival-before-send
order across independently progressing carriers. A queued terminal remains in
FIFO input order and receives fair input service; no frame is discarded,
reordered, or normalized across lifecycle/validation boundaries. Session
retirement retains its existing outer priority. Remote FIN remains a half-close.

When both directions are busy, outgoing work can now be serviced before the
entire visible inbound backlog drains. That is the intended model correction,
not a hidden compatibility promise. Feedback latency can include the other
existing finite service quanta. Conversely input cannot be starved by a ready
source. The single-input-event quantum may expose processing overhead under
dense ACK traffic; do not preemptively add coalescing or enlarge limits. Its
practical effect must be measured and held if it causes a timing regression.

This is not the final-carrier writer's control/data queue priority. Product
service arbitrates independent directions and work types above those writers.
TCP may legitimately affect QUIC through a shared resource or shared ordered
byte range; keeping a Product input queue busy must not veto all QUIC source
work. This contract states that architecture boundary explicitly.

## Proof and acceptance sequence

Extract the existing input-emptiness guards into the production three-way
arbitrator without removing their effect. Its cyclic cursor is initially
unable to provide fairness because those guards still disable source and
dispatch. A production-arbitrator RED holds input ready and verifies source
and dispatch get service; it must fail because both are disabled. Change only
the arbitration contract, require deterministic rotation, pending/disabled
class behavior and cursor preservation on cancellation. Re-run the affected
relay ACK, half-close, terminal-before-EOF, R1 protected-write and path tests.
Then compare ordinary mixed/QUIC upload and download with the saved before and
mailbox-only binaries under the same asymmetric 500-Mbps conditions, including
the existing QoS/outage sequence. Means, series, gaps and latency all remain
gates. The mailbox, qualification and native candidates remain unaccepted.
No universal optimality, browser acceptance, RSS closure or release follows
from this actor proof alone.

## RED checkpoint

The focused release-lib test builds in 6m42s and fails immediately on the
production arbitrator retaining the legacy veto. Six selections produce
`[Input, Input, Input, Input, Input, Input]` instead of the required
`[Input, Dispatch, Read, Input, Dispatch, Read]`, with all three classes ready.
The old guard's effect is preserved in this RED; it is not an ordinary speed
comparison of the extraction. The next candidate removes only that veto and
uses the cyclic ready-service contract. All three handler bodies compare
unchanged after whitespace normalization. The new RFC 10.4 paragraph separates
Product service from final writer priority; no carrier ranking changes.

## First verification and fixture boundary

The candidate release-lib build takes 6m47s. All four service tests and 240
other relay tests pass, including the actual actor's blocked-local-write,
restart, ACK validation, planned retirement and FIN-publication cases. One
server test, response_fin_keeps_its_exact_decide_target_across_metric_churn,
fails its fixture's asymmetric-R precondition before the asserted operation.
The exact single-test rerun also fails. An earlier short-name invocation with
`--exact` matches zero tests; it is not a pass.

That test uses a proof helper which records a supposedly completed proof with
`sent_at = now` and `elapsed = 1 us`: its implied ACK is in the future. The
unchanged production projection prefers proof RTT when metrics.recorded_at is
earlier than sent_at+elapsed. Thus an immediate metrics publication can lose
to the synthetic 1-us proof, collapsing otherwise asymmetric TCP recovery
intervals onto the existing 200-ms minimum. This helper is test-only, and none
of this test's response timing functions invoke the new client arbitrator.
The bounded fixture correction records sent_at=now-elapsed; it does not sleep,
change recovery timing, relax the assertion, or change production projection.
Its subsequent verification remains required; do not report the 245-test
suite as green yet. The candidate remains held for ordinary comparisons.

## Ordinary failure: the first proof omitted executor service

The first ordinary mixed-upload candidate completes with exact accounting but
only20.818 Mbps and a61.500786-second confirmation gap. Its same-condition
mailbox control gives228.118 Mbps and2.280038 seconds. The wider comparison
matrix is paused. The candidate holds one client worker near100% CPU and its
RSS rises from299,344 KiB at5 s to712,576 KiB at75 s. These counters alone do
not prove the gap's exact owner or connect this case to the deployed RAM report.

The first model proves fairness among polled Product classes but silently
assumes the surrounding executor continues servicing other tasks. The new
synchronous Dispatch result can bypass an exhausted Tokio cooperative budget:
input's mpsc receive returns Pending to yield, but Dispatch then returns Ready.
The existing dispatcher explicitly yields after positive dispatched work, not
after a zero-progress blocked attempt. Thus the prior empty-input veto also
incidentally prevented bypass of input's executor yield. Removing it without
owning cooperative actor service is an incomplete implementation, even though
the class-order test passes. This is a defect in this candidate, not evidence
for changing native congestion control or claiming all deployed stalls solved.

A second RED exhausts the actual Tokio task budget, keeps a real mpsc input
queued, and polls the production arbitrator with Dispatch ready. It incorrectly
returns Ready instead of yielding. The production module's own tests compile
directly against already-built dependencies in0.08 s; no duplicate policy or
new benchmark harness is used. The local wrapper only imports that module.

The complete service boundary must honor both levels: cyclic ready-class
opportunities and executor cooperation for each selected event. Use the
runtime's existing cooperative wrapper, whose budget is committed only when
the wrapped future returns Ready and which yields before polling after budget
exhaustion. This adds no MPP timer, byte/window limit, tuning constant, sleep,
carrier preference or throughput target. Pending selection neither advances
the cursor nor consumes an input event. Tokio documents this exact boundary in
its [cooperative future API](https://docs.rs/tokio/latest/tokio/task/coop/fn.cooperative.html).
The corrected candidate must pass both REDs and repeat the ordinary failing
case before this is accepted as the practical stall cause. Retain the first
candidate and its failing series, rather than overwriting the evidence.

The cooperative ordinary build finishes in3m23s. Its first repeated mixed
upload gives211.236 Mbps,3.125324s maximum confirmation gap and exact final
byte accounting; the61.5s stall does not recur. This supports the
missing executor-yield mechanism but is one random run, not final acceptance.
Its40s client RSS543,148 KiB exceeds the matched control's335,424 KiB; do not
declare memory sustainability fixed. Process %CPU samples are lifetime
averages, not instantaneous thread traces. Full probes and process series
are in PRODUCT_ACTOR_SERVICE_EVIDENCE_20260906.json. Other affected ordinary
cases and full relay/stream tests, including the fixture correction, remain.

The corresponding server queued-send branch already yields unconditionally
after a dispatch attempt, including zero-progress blockage. It is therefore
not the same missing-yield branch; do not add another server wrapper merely
for visual symmetry. Native writer transactions remain unchanged.

## Affected comparisons, still held

| Cooperative candidate | Mean Mbps | Max gap s | Interactive outcome |
| --- | --- | --- | --- |
| Mixed upload, combined | 211.236 | 3.125 | Upload confirmation probe; no independent echo series. |
| QUIC upload, combined | 327.385 | 3.463 | Exact upload confirmation. |
| Mixed download, combined | 66.372 | 3.979 | 45 successful,16 failed/unavailable attempts. |
| QUIC download, combined | 98.101 | 4.017 | 31 successful,41 failed/unavailable attempts. |
| Mixed download, variable loss/jitter only | 129.953 | 2.033 | 80 successful,0 failed. |
| QUIC download, variable loss/jitter only | 152.147 | .659 | 80 successful,0 failed. |

Upload gaps are target-confirmation gaps; download gaps are application reads.
These are individual random trials, not proof that every timing difference is
caused by the actor change. In particular the previous mailbox-only mixed
loss/jitter run has129.597 Mbps but a smaller.655s maximum gap. Do not claim
strict timing non-downgrade from the essentially unchanged mean. Earlier
pre-mailbox mixed results have150.994 Mbps and3.442s, illustrating the need to
retain full owner evidence instead of choosing one convenient scalar control.

Success-only interactive p95 is censored after disconnection. The mailbox
mixed control's218.976ms p95 includes only its30 pre-QoS successes; it has no
successes after15s. The cooperative mixed case retains6 successful requests
during15--25s QoS and9 more before the outage, with slow QoS responses. Its
overall1808.768ms success-only p95 is therefore not comparable to the control
as though the control's45 failures were low latency. Even before QoS its
p95 is286.254ms versus218.976ms in this pair; neither variation nor failures
are erased. Both still miss the desired complete interactive acceptance.

No CPU/RSS sustainability, global throughput competitiveness or browser
acceptance follows. The actor/source counterexample is real; the cooperative
candidate's practical benefit and resource cost remain held. Full affected
tests are rebuilding with the corrected completed-proof fixture. The separate
mixed ordered-frontier/native service issues retain priority afterward; no
new congestion tweak or larger buffer is authorized by these results.

## Full affected verification

At11:36 UTC the release-lib build completes in6m45s. All246 relay tests pass,
including the unchanged FIN-target assertions with the completed-proof
timestamp corrected. The cached artifact also passes297 path and253 stream
tests:796 total affected tests. This validates the fixture correction and
affected ownership/ACK/lifecycle contracts, not strict performance or resource
non-downgrade. Commit the test-only timestamp correction independently; keep
the native, qualification, mailbox and Product-service runtime candidates
held. The next practical owner remains mixed ordered delivery and the
unexplained resource delta, not another congestion gain.
