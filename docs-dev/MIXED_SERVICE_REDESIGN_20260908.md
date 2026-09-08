# Mixed-mode service redesign

2026-09-08 20:04 +08:00. Requested architectural review and replacement plan.
Runtime comparator: d999fea. Status: proposed contract, not implemented or
performance-accepted. No controller, queue limit, wire version or release change.
Read PERFORMANCE_METHOD_AND_LESSONS and CURRENT_CLOSURE_PLAN first.

## Decision and scope

Retain the proven reliable-byte ownership core and native TCP/QUIC controllers.
Replace independently evolving request/response placement and service policy
with one direction-neutral ordered-service model. Do not call another rank
formula, a request-only queue migration, or a smaller repair quantum the fix.

The redesign has two independently attributable parts: placement before native
commitment, and prompt service of already-received bytes/feedback. Both need
one shared ownership contract; they must not be bundled into an unmeasured
runtime change. A separate native recovery fault requires native evidence.
The evidence below does not establish one implementation cause for every stall.

## Why the previous loop did not converge

1. **Permission became placement.** Commit65edae3 correctly removed inferred
   BDP/ETA from hard Product admission. It also removed the response placement
   wait, without providing a replacement decision about ordered service.
   Unchanged maximum debt does not imply unchanged actual debt, queue residence
   or application latency. T04b's original non-regression argument conflated
   those properties; its later review already records that limitation.
2. **Safety proofs were mistaken for service proofs.** W/P/E, exact copies,
   cancellation and bounded query work are valuable. They do not determine
   whether the next bytes reach the receiver promptly. RFC10.2 and15.1 explicitly
   say their typed score is not the live sustained allocator; the historical
   projected rank remains in use. More local proofs did not close that gap.
3. **The preparation correction stopped at the native boundary.** Request
   U-to-Original claiming removes a private future-writer dependency. A ready
   writer can still accept bytes into a loaded native FIFO. Response-side
   preparation/commit symmetry is unfinished. Copying the request implementation
   alone would not resolve already-native ordering or all reverse service.
4. **Recovery extent and recovery throughput were treated too similarly.**
   T06 corrected a real ranked14,600B-to-published10.67MB amplification defect.
   That proves one hedge has the scored extent, not that repeated small hedges
   can promptly migrate a large live-owned prefix. Restoring the unscored
   suffix would revive the demonstrated defect, not solve this contract.
5. **Local feedback service and transport receipt were mixed in diagnosis.**
   Some winning bytes waited seconds inside MPP; other winning reads waited
   for processed native input and returned in microseconds. Reducing work at
   one stage was repeatedly followed by another stage still dominating.
6. **The product comparator fell behind the implementation.** Intermediate
   mechanism fixes were followed by narrow diagnostics, rather than keeping
   current TCP/QUIC/mixed, both-direction and baseline outcomes together.
   The latest ordinary78.863-to67.250Mbps observation and3.060-to5.498s gap
   do not establish improvement. Randomness limits causality, not disclosure.

## What is established, and what is not

| Evidence | Established boundary | Not established |
| --- | --- | --- |
| Prepared-source historical capture:12.517MB assigned to TCP in23ms;8.585MB still outside its writer when QUIC commits | Premature private-writer binding was real; request preparation addresses that boundary | Current response migration gain or all native debt removable |
| Post-ACK F379448222:2.169s hold, about2.076s before claim,93ms claim/write-to-mux | Material preclaim interval exists | Earlier legal credit/writer opportunity, hence software-removable delay |
| Post-ACK F311994142:4.723s hold, winning TCP copy write-to-mux247ms | Exact missing-prefix recovery, with a useful TCP winner | TCP always harmful or a faster earlier recovery legally available |
| Native F165558377:3.022s to processed head availability,72us to first Chunk | This winning wait is before processed input availability | Native-controller fault, unavoidable loss, or local polling as cause |
| All553 completed native episodes:availability-to-return maximum5.457ms | No multi-second postavailability wait in those episodes | All parser/actor/ingress service healthy |
| Prepared reply F953:winning decode-to-mux2.226s, including1.832s before route | Actual useful bytes had multi-second local MPP residence | Exact current CPU/lock cause after subsequent changes |
| Reply[863,877):preceding nondata mailbox-send wait1.961s;599ms after decode | Local reader/actor service can delay useful reverse traffic | Every predecessor is an ACK, or all predecode time was avoidable |
| QoS shared10Mbps cut with6.15MB backlog | Real serialization pressure exists | Queue position of the blocking byte, or earlier placement optimal |

Sources: PREPARED_ORIGINAL_OWNERSHIP_MODEL, POST_ACK_FORWARD_SERVICE_20260908,
NATIVE_READ_SERVICE_20260908, PREPARED_REPLY_SERVICE_20260908,
REPLY_RESIDENCE_20260908 and T06_RECOVERY_SERVICE_MODEL. The longest9.567s
native episode loses to an already-delivered TCP copy and is not a useful
9.567s recovery opportunity. Historical witnesses are not current recurrence.

## Required service boundaries

For each logical direction retain these distinct boundaries:

```text
source-ready U
    -> one placement decision and imminent protected native claim
    -> exact Original/copy ownership + separate native-owned work
    -> authenticated receipt and interval reassembly
    -> contiguous delivery to target/application
    -> independent ACK/credit publication and processing
```

Source end A, claimed end C and U=A-C remain exact. With O_i the un-DataACKed
unique Original bytes on exact output i, retained unique obligation is
B=U+sum(O_i). Do not create a second mutable byte ledger for scheduling.
Product ACK releases Product debt, not native accepted retransmissions or
duplicates. Native ACK cannot release Product debt or receiver credit.

If a(u) is the first usable receipt time of byte u, ordered delivery through x
cannot precede max(a(u), u<x), plus necessary local application service.
Thus faster suffix delivery does not compensate for a late prefix. This is
necessary stream semantics, not itself a defect. The avoidable question is
which dependencies MPP creates before that receipt or after it.

Three calculations constrain proposals; these are model examples, not labs:

- Filling500Mbps with100ms assignment/feedback latency needs6.25MB of pipeline.
  Keep configured high-BDP capacity. Native propagation/congestion flight is
  not synonymous with avoidable unsent queueing.
- If a slow-owned prefix blocks a receiving64MiB suffix window,500Mbps suffix
  service can fill it in1.074s. The observed64MiB source-to-target separation
  does not locate all those bytes in the receiver; it includes several stages.
- A64KiB repair advanced once per100ms feedback cycle has5.243Mbps conditional
  service. This is not a measured global MPP ceiling. It rejects an ACK-per-
  quantum migration design before implementation.

For a conditional placement example, let a ready slow path have1MiB of exact
unserved predecessor work at5Mbps; both paths have50ms propagation. A64KiB
prefix completes there in1.833s. If an independent500Mbps path can actually
take it after5ms, waiting completes it in56.05ms: at most1.777s saved for that
prefix. These assumptions are deliberately explicit. Current Ready, Product
O and native flight alone cannot establish that service calendar in production.
No new algorithm may treat this calculation as measured spare capacity.

## Replacement responsibilities

### 1. One directional core, transport adapters below it

Use one ownership/policy implementation for client request and server response.
Source reading, target writing, endpoint identity and carrier I/O are adapters.
Reuse the interval, qualification, epoch, copy and terminal machinery; do not
reimplement it. Shared prepared storage stays unbound until an imminent claim.
Cancellation, FIN, half-close and restart must cover both U and claimed bytes.

Physical-writer arbitration remains shared across logical flows, so one flow
cannot treat connection capacity as its own guaranteed share. No global lock
across network I/O, actor RPC before each frame, or one-frame/ACK feeding gate.

### 2. Separate legal actions from the scheduling decision

Compute exact legal actions from membership, policy, proof, receive credit,
W/P/E, copy authority and real writer/native fences. Then make an explicit
decision: DispatchOriginal, DispatchRepair, ResourceBlocked, or PlacementDefer.
There must not be multiple hidden placement vetoes inside can-enqueue helpers.

ResourceBlocked names its existing authority/wake. PlacementDefer, if adopted,
must name the competing useful work, supporting observation, release event,
nonrenewing expiry and fallback. It must not disguise a reduced resource limit,
wait for an unavailable unrelated path, or suppress the sole surviving path
because its estimate is low. Lifecycle revocation immediately discards a plan;
unchanged observations cannot refresh its deadline. An unselected writer's
readiness change is not a failed ownership fence for a valid selected writer.

This is a proposed scheduling outcome, NOT yet an approved defer algorithm.
It changes the current contract: RFC15.1 requires trying another admitting
writer, and PREPARED_ORIGINAL_OWNERSHIP_MODEL forbids waiting on an occupied
preferred writer. Introducing performance-Defer therefore requires an explicit
successor policy and its liveness/uncertainty proof; it is not equivalent cleanup
or permission to restore65edae3's removed Boolean completion veto.
The current observations do not justify an exact physical service calendar.
A consumer must declare what its service/rate estimate measures, how unknown
and stale inputs behave, and its exploration and fallback cost. A renamed
legacy scalar or max(native flight, Product debt) is not that model.

Known important distinction: native backpressure answers whether work can be
accepted, not whether accepting it helps ordered delivery. Conversely, giving
the only surviving carrier less work because its rate estimate is pessimistic
recreates the original collapse. Both counterexamples are mandatory.

### 3. Recovery is a service plan, not a renamed small hedge

Retain exact source ranges, configured-slot vacancy, immutable suppression,
ranked extent, unique/copy accounting and independent native reliability.
Unclaimed source may move without copying; already-native source may not.

Any sustained live-owner migration proposal must justify and account for every
accepted extent through its declared service policy and current target
opportunity. This may use individually ranked/revalidated actions; it must not
reserve a future suffix that blocks a newly eligible lower range. Do not extend
one quantum's score to an unscored suffix or wait for one Product
ACK per frame. Reconsider the lowest useful retained range at each real service
opportunity; an unavailable target cannot block a different usable target.
No migration amount or rate is accepted yet from headroom alone. It needs a
current causal witness showing why the existing hedge/handoff is the bottleneck.

Keep native accepted losing work visible until its actual owner releases it;
do not count Product ACK or attachment withdrawal as native cancellation.
That is a cost/lifetime boundary, not authority to add another copy budget.

### 4. Received data and feedback must not wait for unrelated planning

Keep authenticated receipt, exact ACK/credit application and useful contiguous
delivery service independent of discretionary opposite-direction planning or
bulk recovery scans. State transitions remain authoritative in each affected
direction/ownership domain, not in one total order across both directions.
Splitting execution must not create eventually consistent ownership or stale ACK release.
Preserve actual prerequisite/lifecycle barriers, finite byte/item resources,
target backpressure and fair service. A full data mailbox does not justify
inventing unbounded bypass storage or skipping protocol ordering.

At the native boundary, class arbitration only controls work still removable
by MPP. Already-accepted TCP bytes cannot be preempted. QUIC stream priority
does not create connection congestion/flow credit. Removing a proven local
dependency is distinct from changing physical ordering domains or QUIC framing;
the latter is deferred unless a measured remaining dependency requires it.

## Retain, delete, or redesign

| Retain with existing regression coverage | Replace only with the complete successor |
| --- | --- |
| Exact Data ACK, FIN, copy identity, proof and incarnation fences | Separate request/response placement policy and future-writer Original binding |
| W/P/E, configured memory/concurrency and native controllers | Treating legal admission or a Ready writer as sufficient ordered-service policy |
| Native/MPP evidence distinction and work-bounded range queries | Mixed rate/backlog scopes and duplicate hidden preference filters |
| Exact small hedge and structural failure recovery | Pretending the hedge alone is sustained live-owner migration |
| Fair native command arbitration and lawful backpressure | Discretionary planning on the critical receive/feedback execution path |

Do not simply delete the entire retained stack. Do not revive absolute-delay
reordering, percentage recovery authority, static TCP/QUIC preference, smaller
BDP-derived hard windows, unscored copy suffixes, or raw hysteresis deletion.
The failed configurations and counterexamples remain regression controls.

## Expected benefit, risks and rejection criteria

The target is multi-second service loss, not a1Mbps or10ms isolated gain.
There is no defensible whole-transfer percentage forecast yet.

| Candidate boundary | Supported opportunity and limit | Reject/stop condition |
| --- | --- | --- |
| Independent receive/feedback service | Historical useful postdecode wait up to2.226s for one winning interval; current persistence must first be shown | Current critical interval has no such local dependency, or mandatory ordering/target backpressure explains it |
| Symmetric late placement | Remove a response pre-native private-queue wait without duplicate bytes, only if current response residence and an earlier legal alternate are shown; the historical request witness does not establish this gain | Only already-native debt remains, alternate unavailable, or caller/ACK round trips starve a high-BDP singleton |
| Sustained allocation/migration | Potentially avoid slow-owned prefix stalls; magnitude unknown until exact offered service and counterfactual are established | Guessed capacity, reduced hard window, extra copy storm, added loaded latency, or no material ordered-service gain |
| Native return micro-optimization | At most5.457ms per observed completed pending episode | Defer: wrong magnitude and boundary for current seconds-long waits |

Costs to measure include duplicate physical traffic, CPU/RSS, startup discovery,
loaded short requests, shared bottlenecks and recovery after quality reversal.
Do not sum overlapping historical waits as a promised gain. Zero gain or a
regression remains possible; record forecast versus outcome for each change.

## RFC treatment and execution order

1. Freeze ordinary d999fea and the evidence comparator. Before another
   performance candidate, bring together TCP/QUIC/default and raw/VMess/H2 in
   both directions under the pinned changing-link profile. Preserve timing,
   failures and configuration differences; do not pass historical tables off
   as this cohort. This comparison has information value, not a speed forecast.
2. Establish a current real-producer counterexample for the largest local
   dependency, with a legal earlier-service control. Review its original
   intention and retain the opposite case. No implementation from mere slow
   bytes, losing-copy age, aggregate CPU or a failed acceptance result.
3. Implement that independently attributable boundary using the shared model.
   Check safety plus the forecast in ordinary fixed-work and sustained traces.
   Stop on adverse composition; do not stack compensating changes.
4. Replace response preparation and duplicate policy owners through the same
   core only with both-direction parity controls. Choose placement defer or
   sustained migration only when its needed service inputs and current witness
   are established. No endless sequence of unrelated new fixes.
5. Replace RFC10/15 policy text coherently when the successor algorithm is
   specified and supported. Separate wire invariants from implementation policy;
   remove obsolete legacy/future alternatives instead of appending another
   competing policy. This proposal does not silently claim Core7 already does it.
6. Full acceptance still includes independent200Mbps links, shared cuts,
   asymmetric loss/jitter/QoS/outage combinations and ablations; cold/warm,
   single/concurrent and real Cloudflare; both directions; restart/churn and
   post-load resources. Keep baseline and timing plots current as evidence,
   not only when a result is favorable. No release before those gates.

Independent audits agree that current evidence cannot yet justify an exact
native service calendar or universal best-path guarantee. They retain the
two redesign dimensions and reject queue shrinking as a substitute. This is
a bounded architecture decision, not a claim that implementation is complete.

## Honest limit and established references

Two paths can have identical observed histories when bytes are assigned, then
the chosen path can fail. Immediate duplication avoids that particular risk
but can harm a shared bottleneck. Therefore no online scheduler can guarantee
clairvoyant best-path service, free discovery and zero redundancy cost for every
future network. This does not excuse self-generated waits or waive practical
competitiveness.

[RFC8684 sections3.3.4–3.3.6](https://www.rfc-editor.org/rfc/rfc8684.html#section-3.3.4)
separates connection ordering/flow control and cross-subflow retransmission;
the original subflow may still need retransmission after another copy succeeds.
It does not prescribe one optimal retransmission policy.
[RFC9000 sections2.2–2.3 and13](https://www.rfc-editor.org/rfc/rfc9000.html#section-2.2)
distinguishes ordered stream delivery, stream prioritization and processed
packet receipt. MPP must not equate packet ACK with application delivery.
Neither RFC makes a scalar scheduler score a universal service guarantee.
