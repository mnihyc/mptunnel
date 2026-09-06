# Evidence review and practical acceptance

Date: 2026-09-05. Reviewed production tree: `7189e69` (wire11).
This is the current review/experiment plan, not a release verdict. Historical
SEEN/UNSEEN labels are mapped below; an old OPEN label is not a newly found bug.

## Reflection: the actual process failure

The record does not support either "every correction improved speed" or
"every correction made it slower." Several fixes have reachable owner-level
RED/GREEN evidence; several only improve observability; several are unused
model groundwork. Component success was repeatedly described too broadly,
while browser upload, cold/warm timing, mixed-carrier tails and adverse repeats
remained untested. That was an acceptance and attribution failure.

Model preparation should have rejected three approaches before implementation:
an estimate becoming hard admission authority; a static one-action score being
called a sustained allocator; and a receipt-retained work ledger being called
performance-neutral. Tests remain necessary for timing and native transport
composition, but do not substitute for those elementary proofs.

The remedy is not a blanket revert or a mandatory whole-RFC rewrite. Preserve
proved ownership/progress invariants. A new scheduling/controller design must
first state its observable inputs, byte/direction/lifetime domains, causal
assumptions and counterexamples. A rewrite is justified only where the model
itself fails a practical requirement, with an isolated implementation and
matched application results. No speculative redesign is a release prerequisite.

## Evidence-backed corrections

"Component-proved" below is deliberately weaker than "current end-to-end
performance accepted." Exact test and source references are in the linked
diagnosis documents and commits; the latest full suite passed2316 library
tests, integration groups6/2/6, and450 native tests plus3 doctests.

| SEEN / current item | Root cause and practical effect | Disposition / regression boundary |
| --- | --- | --- |
| 1 / 1C | Acquisition arbitration overrode ordinary completion ranking on response/request, assigning OriginalData to a worse eligible carrier. | Keep `38286aa` / `842a0cc`; real owner RED/GREEN. No universal speed gain inferred. |
| 2; P3/T02/T02b | A configured TCP startup prior lost provenance. The subsequent typed-sidecar change also replaced live scalar evidence before a sustained successor existed. | Keep provenance and exact native QUIC scope; `b7961f3` restores the legacy scalar consumer. Do not describe the unused typed TCP startup-only model as a completed dynamic estimator. |
| 4; R1 | Waiting on a local sink also delayed accepted Product/control progress; a nested startup-open path deferred terminal RESET, including an obsolete generation. | Keep `444fb38` / `0545610`. Real blocked-sink actor tests prove terminal/control progress without releasing the sink; successful-open ordering remains. |
| Adjacent FIN/source wake | A successful retried ACK publication could leave an already retained FIN uncommitted; zero source admission did not revisit fresh path evidence. | Keep `f6a2df3` / `a3b706b`, with FIN/actor lifecycle controls in the full suite. These are progress corrections, not congestion tuning or permission to recreate targets. |
| Server restart; R2 | CREATE, later STARTUP enrollment and ordinary reattachment were ambiguous when the server forgot the stream. Retrying later enrollment could incorrectly create state or repeatedly fail the initial-plan check. | `afbb75a` / `89037a8`: explicit phase, only CREATE allocates. TCP/QUIC absent/retained tests pass. Requires paired wire11 endpoints; not durable exactly-once target creation across state loss. |
| 6A; P1/T05 | Live-owner repair authority renewed; the first safety fix then used a cumulative percentage as a hard recovery gate. | Flood prevention was justified; percentage authority was not. `72d1237` / `914b9b9` use authenticated configured-slot/range identity and finite structural copy ownership. Preserve non-renewal, not the old percentage guard. |
| 6E; P1/T06 | Apply expanded a ranked one-quantum repair into a large unranked suffix, increasing duplicate load and ordered debt. | `bfac5b8` retains exact live frontier extent, with separate terminal-failure authority. Exact RED/GREEN and historical matched mixed/QUIC gate support this correction; not all failovers. |
| 6B | HTTP/3 stream priority was reported but not applied to Quinn. | Keep `a9450d8`; actual native priority is tested. Cannot overtake an already accepted prefix in the same ordered stream. |
| Earlier QUIC startup/rate authority | Mixed compensated/uncompensated plateau units and application-limited exit premises could terminate backlogged acquisition early; an MPP ACK-window wrapper could separately underfeed a live native controller. | Retain the exact unit/epoch and native authority corrections described in PERFORMANCE and native tests. They do not prove BBR's retained maximum is an accurate current service measurement; N1 is an explicit residual limitation. |
| Response return-plan startup | The first TCP attachment could own a multi-MiB response prefix before a planned QUIC attachment was even opened. | Retain the explicit bounded pre-FINAL return-plan transaction. Readiness is not mandatory data allocation; later CREATE/STARTUP disambiguation completes its restart branch. No fixed QUIC preference. |
| P4/T04a | Response completion snapshot added a writer-owned subset to its already-inclusive queue total. | Exact accounting correction; not a request bug. Queue/resource charge lifetime is unchanged. |
| P4/T04b | Inferred ECF/BDP denied the only otherwise enqueueable Product action. | Preserve structural resource permission, but revise the prior blanket keep/non-regression claim: `65edae3` also removed a response placement choice without replacing its ordered-service obligation. Permission invariance does not require immediate dispatch. ORDERED_REPAIR_SERVICE_BOUNDARY records the history and insufficient rollback ablation; no wholesale restoration of inferred windows. |
| P5/T10a; C1 | Old quarter-window/payload batching withheld an already freed receive-prefix grant despite RFC8.4. | `5c1d288`,512-byte/4096-window RED/GREEN; latest-value coalescing remains. May add control frames; affected throughput/CPU must be measured, not assumed improved. |
| 7; P7/T11 | Retired path lifetime/identity, port projection, absent/stale values and delivery direction were conflated. Native retained bandwidth was presented like current traffic; pacing was raised to the model. | Keep retirement/identity fixes and `6545270`. Current ACK deltas, retained E and literal P are separate. Browser precision/reset/idle/unequal-window/sort controls pass. Presentation cannot fix a controller. |
| M1 retention | Overlapping recovery transactions prevented finalized journal collection; clone/rollback could leave terminal proof in its parked owner or reuse its identity. | `6636091` / `42d1b86`: prefix compaction, exact owner dispatch and lineage IDs. Removes lifetime-growing finalized history and associated scanning. Does not conclusively attribute the uncaptured deployed RSS incident. |
| Mixed upload feedback processing | Every recovery candidate rescanned the existing repair queue. Quiet profiling attributes 34.143 s to the overlap/enqueue loop; an exact native ACK arrived 44 s before MPP decoded it in a separate capture. | `614dc73` snapshots occupied byte intervals once per request batch. Disjoint-candidate equivalence and 2,098,176-to-2,048 inspection RED/GREEN pass, with all 243 affected sender tests. Ordinary timing/resource comparison remains pending. No controller, copy limit, or wake-policy change. |
| Mixed reply drain / uniform ownership scan | The chunk-independent ownership calculation introduced in `53d9ab5` rescans N spans at N boundaries. Preparation profiling and a live worker stack locate it in the post-source reply stall. | LIVE_OWNER_FRONTIER_WORK_BOUND preserves the same sets, order and assignment maxima through an endpoint sweep: RED4,196,352 to GREEN12,286 visits for2,048 chunks, plus sorting. Seven model and743 caller tests pass. Ten ordinary comparisons show shorter upload drain; adverse QoS interactive failure predates and survives the change. Component work correction only; no global throughput/retention acceptance. |
| Native send-evidence lifetime | Imported ten-round collection deletes an original still owned by QUIC, so a later valid loss callback is incorrectly treated as unknown raw evidence. |9f522b2 deletes the independent expiry and uses actual transport terminals. Delayed-loss/ACK RED/GREEN and native tests pass; ordinary diagnostic follow-up finds zero of the previously observed62 unknown-evidence bound actions. Other compensated responses and mixed stalls remain separate. |
| Individual loss class versus native undo | Whole-episode undo correctly requires all declared losses to be disproven, but compensation reused that guard for each packet's classification. An actual late original stayed charged because its episode also contained a genuinely lost packet. |QUIC_PACKET_LOSS_CLASSIFICATION_REVIEW documents actual-engine RED/GREEN, batched exact packet terminals, deletion of duplicate transaction retention and unchanged native undo.8,192-round retention remains bounded. Ten ordinary comparisons do not establish timing non-regression; intermediate candidate only, no release claim. |
| R3 native close | Pending ordinary STREAM data made close-only transmission fail the full-cwnd gate, although closing stopped feedback that could reopen it. | `c6ce159`, CUBIC and BBR3 peer-event RED/GREEN, no clock advancement. Upstream Quinn issue/PR2785/2787 corroborate origin. No change to ordinary admission or anti-amplification. |
| P6/T12 | Historical32 failing fixtures mixed obsolete wire/model assumptions with genuine owner failures. | No longer32 current defects: current full suite passes. Fixture corrections do not count as runtime speed fixes; owning production corrections retain separate commits. |

## Disproved, unsupported, or deliberately not implemented

| Item | Why it is not another urgent production patch |
| --- | --- |
| SEEN3 sparse ACK erases reorder debt | Under the checked ownership invariant `S-A=O+H<=W`, acknowledged-but-held bytes remain charged. No demonstrated erased debt; no patch. |
| SEEN5 ghost/replacement request | The captured fixed-request control uses one attempt and one body. This disproves that explanation for that capture, not all possible lifecycle bugs. |
| P4/T01 exact all-stage receipt ledger | Not implemented; proposed finite retained work would add `goodput<=8N/receipt_delay` and still require predicted service. Reject the mandatory model, not rewrite every writer to satisfy it. |
| T03/T08b static score / new allocator | Pure arithmetic is tested, but constant inputs can select the same winner forever; local flush is not network service. Runtime migration was correctly rejected. Existing groundwork has no demonstrated throughput benefit. |
| P4 overlapping contention factors | Plausible research for nontransitive/directional shared resources, not a demonstrated necessary fix for this candidate. Test shared versus independent links first. No factor estimator or protocol preference is bundled. |
| P5/T10b partial local write | Scalar/vectored cursors survive Pending and the batch retains its Bytes until completion. No demonstrated duplicate/lost prefix. Finer-grained ownership/credit release would be an optimization requiring evidence, not an automatic correctness repair. |
| P5/T10c target-bound final tail | Current exact-target enqueue plus ranked frontier already implements the relevant authority. No duplicate tail rewrite. |
| N1 monotone probe target / minimum probe flight / probe fairness | Three diagnostic changes did not correct the complete downshift trajectory. All were removed. Passing a local invariant was insufficient reason to accumulate them. |
| P2/T07 extra ordering domains | Its prerequisite is now observed: a frontier repair had49.326MB of actual unsent native predecessors, almost all new bulk. QUIC_REPAIR_ORDERING_MODEL promotes only a companion ordering stream inside the same physical attachment for investigation, preserving queue/copy/CC ownership. Broad multi-domain allocation remains outside this transaction; the companion is not yet accepted. |
| T09 significance / no-flap formula | A jitter deadband alone is not statistical confidence and cannot prove that a10% rate advantage is real. A stronger formula remains a proposal, not an accepted fix. |
| L3 optimization | Correctness is retained. Experimental L3 performance is not a dependency for fixing L4 browsing/download/upload. |

## What remains practically open

The [2026-09-05 experiments](REORDERING_PERFORMANCE_DIAGNOSIS.md) now reproduce
a severe QUIC reordering deficit on both this tree and the last release.
They supersede the prospective wording below where results exist, but do not
close the complete release matrix. No new runtime candidate is accepted.

The [2026-09-06 continuation](QUIC_RECEIVE_HISTORY_DIAGNOSIS.md) proves a second
native owner: bounded129-packet receive history discards timely reordered
originals, with4,948 exact matches to sender loss in a zero-router-drop trace.
Sender-only adaptive candidates remain unaccepted and are withdrawn from the
production tree. Their higher averages did not close timing stability. The
next reordering model must cover sender loss tolerance, receiver history and
packet-number encoding together, preserving duplicate safety and bounded
memory. This supersedes starting another sender-threshold-only attempt.

The [34-run composition review](QUIC_REORDERING_EXCESS_DELAY_MODEL.md) now
provides that experiment. Correcting receive history/encoding and sender
reordering together raises jitter-only QUIC from 0.585 to 185.313 Mbps with
shorter gaps and complete echoes. But the first sender model retained old
common queue delay; actual native loss declarations kept a 2.015-second delay
after RTT recovered below 100 ms. That experimental model is rejected.
Learning excess delay instead passes the exact RED/GREEN counterexample and
461 native tests, and improves same-connection recovery, but combined mixed
stability is still unaccepted. These are not additional claimed production
fixes. Exact candidate patches and full series are archived; the release
baseline remains7189e69 while the working tree contains explicitly held
candidates. The next owner is the existing mixed
QoS-history/ordered-progress interaction, not a new allocator or threshold.

1. **QUIC deep-buffer latency (N1):** with old bandwidth400 Mbit/s and base
   RTT80ms, the old half-BDP probe allows2MB, but a new10-Mbit/s path's entire
   BDP is0.1MB. A queued RTT can enlarge later flight; the retained maximum
   also supplies the feedback clock needed to retire itself. Native and
   dequeue-verified experiments support queue/model inflation. Restoring
   actual service recovered the same stream in both forward and ACK-only
   tests; persistent restart dependence is not established by those tests.
2. **Mixed and TCP timing:** historical TCP loaded p95 was1417ms against
   raw522ms/V2 538ms; native FIFO debt, not command wait, was observed. Current
   combined loss/QoS/blackhole and both-direction tests must determine what
   persists after the accepted frontier fix. Do not waive a product deficit
   merely because one native ordered stream cannot overtake itself.
3. **Startup and browser trajectories:** 100KB at100ms has an end-to-end
   one-RTT ceiling near8Mbit/s before handshake/headers. That alone is not
   bandwidth underestimation. A100MB transfer alternating14 and326Mbit/s,
   multi-second first-body gaps or a lasting upload collapse are different
   symptoms and require attribution. Cold and warm measurements must differ.
4. **Sustainability:** exact journal retention is fixed; overall heap growth
   under browser churn/backpressure is not closed by that fact. RSS includes
   live payload and allocator retention. Record live streams, owned queues,
   RSS/CPU and post-load recovery; do not lower concurrency to hide growth.
   The current `SessionSendBuffer` charges unique source bytes once across
   streams and releases them on Data ACK or cancellation. Treating its64MiB
   session limit as4096 independent64MiB source allocations is a false positive.
5. **Wider current evidence:** P8/T13 still lacks the complete ordinary-build
   six-way, repeated, bidirectional, single-stream/browser, shared/independent
   and combined-condition matrix. Historical charts cannot fill that gap.

## What theory can and cannot promise

For a work-conserving path, with service `c(t)` in bytes/s and backlog `q`,
`dq/dt = offered - delivered` while nonempty. A sudden capacity reduction
creates queueing until offered work falls or the queue drains. For an ordered
prefix of size`B`, a following byte cannot arrive before the service integral
has delivered that prefix. This is a lower bound, not permission to create an
unnecessarily large prefix.

No causal controller is always the clairvoyant optimum: two networks can have
identical observations now and different capacity/outage changes before the
next feedback. The same current action cannot simultaneously be the optimum
for both futures. With every path blackholed, delivery is impossible. With a
shared cut of capacity`C`, adding paths cannot increase total useful traffic
beyond`C`; headers and repair subtract from that ceiling. An independent link
can increase the cut, but native control, discovery and receiver ordering still
have costs. Configured memory`W` and feedback delay`R` imply the additional
pipeline ceiling`8W/R`;64MiB reaches500Mbit/s only while the relevant release
feedback stays below about1.074s.

These are our explicit deductions, consistent with QUIC's feedback-driven
[loss recovery](https://www.rfc-editor.org/rfc/rfc9002.html) and the
[BBR draft06](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html).
BBR draft06 is not a published RFC. Hysteria's configured-rate Brutal mode has
advance operator bandwidth information; its
[configuration contract](https://v2.hysteria.network/docs/advanced/Full-Client-Config/#bandwidth)
must be recorded in comparisons. A practical comparison may use that normal
configuration, but cannot pretend both controllers received identical priors.

## Finite next experiments and decision order

Use the current ordinary build on both endpoints, not the retained wire10
diagnostic executable. Reuse existing receiver probes. Record actual dequeue
bytes, application interval bytes/read gaps, loaded latency, CPU/RSS and each
applied transition; a changed rate label alone is not recovery evidence.

1. Six-way asymmetric same-link run: raw Linux TCP, Xray VMess/TCP,
   Hysteria2 Brutal, MPP TCP, QUIC, default TCP+QUIC.100ms base RTT split70/30,
   unequal jitter20/5ms,500/100Mbit/s directional service, forward loss3--10%
   (eight epochs mean6%), unequal reverse loss. Add500→10→500 forward service
   and a short UDP blackhole. One continuous download plus independent echo
   exposes collapse and loaded latency. Each product starts fresh; no restart
   during transitions. This first cohort is diagnostic, not a ranking proof.
2. Reverse/upload, short-object/concurrent and healthy/loss-only ablations.
   Distinguish request/feedback direction, native service and Product gaps.
   Verify recovery with the same application request, not a replacement.
3. Two independent links versus the same aggregate shared capacity; asymmetric
   fast directions; mixed-carrier removal ablations. Do not call multiple
   connections on one NIC independent bandwidth.
4. At the first material reproducible failure, isolate its owner before any
   production change. Exact counterexample → model justification → RED/GREEN
   → affected ordinary comparisons → isolated commit. No lucky rerun selection.
5. After those deficits close, complete browser/Cloudflare and sustained churn
   gates, then repeated matched cohorts. Update public curves from current
   validated measurements, with uncertainty and latency as well as speed.
   Release remains blocked by material correctness, sustainability or
   competitiveness failures. About10% throughput variation is not an excuse
   for seconds of missing service or worse interactive tails.

Audit workers are unavailable under their usage limit. Root review and tests
can proceed, but no new independent review is claimed or manufactured.

### Historical wording must not restart completed work

`CARRIER_NATIVE_AUTHORITY_PROOF.md`, `QUINN_BBR3_NATIVE_OPERATIONAL_V1.md`
and `RESPONSE_STARTUP_READINESS_MODEL.md` contain original baseline verdicts
and candidate obligations. They are not current defect inventories. Current
active-controller coherence/fences have runtime tests, the return plan exists,
and TCP ReceiptMode/new sustained allocation were not deployed. In particular,
conditional symbolic up/down bounds are not empirically established recovery
guarantees. Read the current source and this disposition before treating a
historical sentence beginning "current" as a new bug.
