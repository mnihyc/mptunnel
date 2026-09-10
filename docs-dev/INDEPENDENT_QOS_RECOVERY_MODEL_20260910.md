# Independent-cut recovery: exact ownership evidence

Updated: 2026-09-10. Category: bounded diagnostic attribution and prospective
model preparation, not a performance acceptance or runtime correction.
The active decision remains in `CURRENT_CLOSURE_PLAN.md`; ordinary outcomes and
the diagnostic profile/cost appendix are in `NATIVE_REFILL_INDEPENDENT_20260910.md`.

## Capture and identity

Source is the closed diagnostic directory
`./.tmp/reflection/results/aggregate-combined-up-native-refill-independent-qos-observer-0910/`.
The ordinary runtime was restored before traffic. One temporary feature-only
`request_frame_commit` hook records successful request Product/source commits,
not native handoff or remote receipt. Existing receiver-hole and delivery-stall
events record actual mux receive frontiers. No scheduling policy was changed.
The diagnostic logs total 36,895,715 bytes; its 243.883 Mbps is not a new ordinary
performance result. The upload settles exactly: 1,275,592,704 bytes in 41.842690 s.

The sole session is `3729823994303374768`, upload stream **0**. Unlike HTTP download,
this raw upload needs no assumed header displacement: all 27,043 Original
commit intervals form a disjoint, adjacent cover `[0,1275592704)`, exactly the
accepted/confirmed application byte count.

| Short name | Client commit identity (underlay/index/attachment/physical) | Destination |
|---|---|---|
| TCP46 | TCP / 0 / 0 / 2 | `10.238.46.20` |
| TCP47 | TCP / 1 / 1 / 4 | `10.238.47.20` |
| Q46 | UDP / 0 / 2 / 8 | `10.238.46.20` |
| Q47 | UDP / 1 / 3 / 1 | `10.238.47.20` |

The direct links each have one 200 Mbps UP cut shared by that link's TCP/QUIC.
Only link46 changes 200→10→200 Mbps during nominal seconds15–25. Link47 stays
200 Mbps. UP/DOWN delays are70/30 ms; loss, jitter and blackholes are absent.
Session and physical identities are joined through management snapshots, not
through coincident server/client attachment ordinals.

Client and server diagnostic monotonic clocks have different origins. All exact
cross-owner joins below use shared `ts_unix_ms`. For compact display, `t` is
milliseconds after **1789028651533**, the initial management generation stamp.
It is not an exact probe-start or impairment-application timestamp. The17–24 s
interior avoids assigning boundary traffic to an uncertain cut instant.

## Placement and copy accounting

| Path | Whole Original bytes | Original bytes, t=17–24 s | Copy bytes, t=17–24 s |
|---|---:|---:|---:|
| TCP46 | 94,639,586 | 0 | 0 |
| TCP47 | 90,902,355 | 0 | 29,200 |
| Q46 | 494,660,998 | 0 | 0 |
| Q47 | 595,389,765 | 10,509,124 | 1,618,000 |

There is no sustained new Original placement on46 in this safe collapse
interior, and no repair is redirected to46. Last pre-restoration Q46 Original
commit is t=15.560 s; the exact cut boundary remains separately qualified.
Q47's134 copy commits in17–24 s cover1,618,000 unique bytes with no overlapping
Q47 reissues. Eighty-seven commits are exactly14,600 bytes; smaller pieces are
often adjacent splits of that extent at existing data boundaries. The two
TCP47 tail copies account for the remaining29,200 bytes.

This excludes continued bad-link placement or same-link copy routing as the
dominant explanation for the *later sustained* collapse. It does not erase
already admitted bad-link Originals or prove which copy wins.

## One exact slowly advancing prefix

The observed missing interval `[573630416,574017168)` belongs wholly to11 Q46
Original frames committed at t=13.577–13.582 s, before the cut. Its first observed
frontier is573,630,416 at t=20.507 s; at t=22.726 s the frontier is574,006,760.
During those2.219 s, ordered progress is376,344 bytes while reorder storage grows
41,487,248→44,175,764 bytes. These are actual mux receipt/frontier observations,
not native ACK counters or target confirmation estimates.

Twenty-three Q47 persistent-gap copies overlap this interval, plus two TCP47
tail copies of exact Q47 ranges. They total277,400 full-frame bytes and265,400
bytes clipped to the stated interval. The Q47 overlap commits occur at
t=20.436–22.656 s,6.854–9.079 s after the contributing Originals. The following
late subset makes the small successive service visible:

| t (ms) | Q47 committed copy interval | Latest logged receiver frontier | Conservative copy bytes above that frontier |
|---:|---|---:|---:|
|21790|[573907160,573921760)|573904560|17,200|
|21967|[573921760,573936360)|573919160|17,200|
|22170|[573936360,573950960)|573933760|17,200|
|22318|[573950960,573965560)|573948360|17,200|
|22426|[573965560,573980160), two adjacent commits|573962960|17,200|
|22547|[573980160,573994760), two adjacent commits|573970096|24,664|
|22656|[573994760,574009360)|573992160|17,200|

The final column unions all already committed Q47 copies above the latest
actually logged frontier; a sampled frontier can lag actual receipt, so this
is an upper bound on still-unordered copied bytes, not a native flight measure.
Successive14,600-byte extents here are108–203 ms apart. For scale only,200 Mbps
over100 ms requires roughly2.5 MB of useful pipeline. A14,600-byte transaction
per100 ms supplies only1.168 Mbps of copy work. This arithmetic is not a ceiling
on total MPP service: Original progress, multiple owners, independently received
suffixes and additional TCP copies coexist.

## Exact source boundary and limits

Current request persistent-gap recovery starts at `first_proven_ack_gap`, while
retained fallback starts at the actual DATA-ACK frontier. Both use
`live_owner_uniform_frontier`, which stops at an Original/copy ownership-set
change. The accepted copy's exact owner is avoided for that range. Retained
recovery also returns immediately while its first extent has an unexpired
accepted-copy deadline. It does not scan to a disjoint successor on the same
healthy target. These are `sender/request.rs::data_ack_gap_reinjection_model`
and `enqueue_completion_tail_reinjection_inner`, `relay/client.rs::
evaluate_client_data_ack_reinjection`, and `model/work.rs::
reliable_live_owner_uniform_frontier`.

Thus the existing framework preserves a bounded first-prefix hedge, not a
sustained live-owner recovery pipeline. This is a proposed stronger service
contract, **not** a claim that current code violates its existing RFC. A
correction must preserve exact same-range suppression, independent successor
age/ownership, score/Apply extent equality and real target/resource admission;
blindly enlarging a scored hedge into the entire suffix is not justified.

Crucial maturity qualification: Original age alone does not prove that every
observed copy is past the owner fallback. The exact Q46 native samples have
SRTT3.678 s at t=19/20,5.305 s at21–23, and7.413 s at24/25. The existing
`SRTT + 4*max(jitter,SRTT/8) +25 ms` PTO gives5.543/7.982/11.144 s respectively.
One-second snapshots do not reveal each decision's actual retained deadline.
Critical copies carry the persistent-gap cause, which can be authorized by an
earlier loss/ETA predicate; there are no queued retained-frontier events in
20–23 s. The four `data_ack_loss_timer` events do not export owner fallback.
A synthetic demonstrably mature successor test therefore proves the proposed
post-fallback contract only, not that such a change covers this entire capture.

Neither this trace nor admission timestamps identify a copy winner or split
native transport, queue and receiver scheduling. `server_receive_delivery_stall`
is conditional on actual delivery and a prior gap; it is not a per-frame arrival
log. `relay_local_read_blocked.received_offset` is the reverse confirmation
stream's received offset, **not** the upload DATA-ACK frontier. Absence of any
selected diagnostic event is not evidence that its unobserved work never ran.

## Candidate: authoritative omission service, not stream-wide hedging

2026-09-10 16:51 +08:00. Prospective request pilot, not accepted runtime.

Let R be retained, actually claimed Product bytes; G the receiver's explicit
authoritative omissions; U queued repair intents; and C(t) accepted-copy ranges
whose exact attachment is still current and whose frozen suppression has not
expired. Current due service candidates are subranges of:

    E(t) = (R intersect G) minus (U union C(t))

Neither U nor C changes receipt truth or releases source/cache/flight debt.
Normalize and split E at exact owner, accepted-copy identity and Original
assignment/maturity boundaries. Evaluate from lowest offset; a blocked target
or immature earlier range does not forbid an independently due/serviceable
successor. Each candidate retains its existing cause-specific loss/fallback
clocks, current target comparison, exact owner exclusion and configured-slot
vacancy. An expired accepted copy still occupies its slot until actual Data ACK
or exact attachment removal; expiry alone releases only same-range suppression.

For the selected range r, capture the existing common quantum q and fresh
target ordering, then admit no more than min(q, bytes(r), target service).
One existing evaluator call enqueues at most that ranked extent. Dispatch
commits its exact copy debt and yields before another evaluation; no new
fill-the-window loop or multi-quantum stale reservation is introduced. Existing
control/input/lifecycle cooperation remains the execution boundary.

### Conditional service and safety claims

- Every selected byte is retained and within an authoritative omission.
  Later positive evidence clips both the owner and candidate, not negative
  evidence beyond the reported scope. Silent retained fallback stays one-head.
- Repeating evaluation cannot produce another overlapping queued/unexpired
  copy. Accepted expired copies retain target-slot occupancy. Distinct ranges
  can use distinct current service without pretending the first has arrived.
- Every accepted extent is a subset of its own immutable ranked extent.
  T06's112.6-times unranked suffix amplification remains forbidden.
- Cause timing belongs to the exact Original assignment, not the current
  enumeration cursor. All surviving ACK fragments share its first observed
  absolute clocks and later minima. A larger current RTT cannot renew them;
  a newer adjacent assignment cannot lend or inherit older maturity.
- Return the earliest relevant future cause/suppression wake. Already due but
  unavailable work relies on existing native/capacity/model/membership wakes,
  not a timer spinning at an expired instant. No target can claim that a queue
  snapshot reserves native service before actual Apply.

These properties establish finite, nonoverlapping, native-admitted service,
not optimal total duplication or guaranteed goodput. The old one-head policy
also reduced speculative work on shared congestion. More concurrent disjoint
repairs can increase that cost even if each one has a valid advisory rank.
The actual bound is per action and retained resource, not aggregate q.
This is an intentional RFC service-policy correction, not mere code alignment.
Only practical affected comparisons can justify promotion.

### Forecast, falsifier and smallest validation

The observed ordinary~170Mbps healthy-link deficit and multi-second prefix
drain make this material. Removing serialization may reclaim a substantial
part of that deficit after ranges independently mature. It cannot undo the
pre-change physical backlog, know a future QoS transition or ensure every
successor is currently eligible. If the existing timing/target rules still
deny every successor, enumeration alone does not meet the forecast.

First prove the real request evaluator currently refuses an independently due
successor solely because its prefix already has repair service. Preserve the
opposite cases: future clocks, receipt exclusion, exact suppression and expired
slot occupancy; preserve the original T06 bound and structural recovery tests.
Then compare ordinary candidate with frozen b0 on the unchanged independent
200→10→200/healthy200 upload profile, followed by healthy aggregation and
affected shared mixed controls. Retain full timing/completion, both actual link
loads, copies/duplicates where available, CPU/RSS and latency. No parameter
adjustment follows an adverse or absent benefit. Response symmetry is assessed
after the attributable request pilot, not silently claimed from a shared idea.

## Pre-change production RED

2026-09-10 17:05 +08:00. Unchanged b0 runtime; one new test only.
`runtime::relay::client::tests::authoritative_request_gap_serves_distinct_successor_before_head_copy_ack`
fails at its intended last admission assertion: queue0 instead of14,600 bytes.
All preceding actual Original publication, receiver-produced sparse ACK,
positive ownership release, first repair commitment, frozen copy suppression,
and successor timing/target/native/Product-room checks pass. Runtime0.21s;
42.78s compilation. No original assignment time or copy deadline was fabricated.

The receiver acknowledges quanta0 and3, proving only1 and2 missing. First gap
service queues/commits quantum1. Without acknowledging that repair, the exact
successor quantum2 has its own matured cause, a measured healthy target and
available command/Product service. The actual evaluator nevertheless reports
no measured target and queues nothing because it examines only quantum1.
This proves the model defect without changing a congestion controller or
declaring the original carrier failed. It is not yet a performance improvement.

Initial compilation failed because the fixture referenced a private helper;
that is test setup, not Product RED. The corrected fixture uses the existing
non-reinjection frame entry, which invokes the same ordinary planner and the
attached stream's Throughput lane. It adds no production visibility bridge.
Logs: `./.tmp/reflection/authoritative-gap-service-red-0910.log` and
`authoritative-gap-service-red-run-0910.log`. Runtime implementation is now
authorized for this bounded candidate, with ordinary gates still outstanding.

## Implementation boundary review

Original flights retain their full accepted extent and an optional timing
observation; ACK survivors inherit both. A first/tighter observation updates
only siblings in that assignment's exact extent, with instance and assignment
time also checked. A queried range aggregates all contributing absolute clocks,
not only the latest assignment timestamp. Metadata is reclaimed with existing
flight ownership; no separate lifetime map, Arc or configured parameter exists.
It adds per-flight memory, which belongs in ordinary resource comparison.
Initial coverage queries still scan the retained prefix, not an interval tree;
only changed-clock sibling updates have assignment-local scan bounds.

Enumeration snapshots queued/live-copy coverage and real assignment/copy
boundaries once per evaluation. It advances by those regions when unavailable,
not by an artificial number of14.6KB iterations. It does not establish an
intersecting-only complexity bound; ordinary CPU cost remains a falsifier.

Important unchanged executor limitation: accepted/drained head copies do not
block selection/dispatch of disjoint service in this pilot. A provisionally
queued earlier bound repair that subsequently loses native capacity can still
block the existing FIFO's later dispatch until that head is reconsidered.
Selecting independent work is not a universal no-head-of-line queue guarantee.
The measured capture and intended RED isolate an already accepted head, so no
new queue scheduler is bundled into this correction. A separate queue-level
failure would require its own reachable material evidence before expansion.

## Focused candidate result

All95 distinct selected checks pass: real request actor, request flight ledger,
request sender and unchanged response T06 bound. Primary successor test also
checks repeated evaluation does not stack queued/accepted ranges, expiry does
not vacate an accepted target slot, and the second repair actually commits
without a head-copy ACK. Eight new ledger tests cover timing/clipping/coverage.
No count is added for the primary test's earlier individual run.

The migrated old integration fixture initially used a pre-loss projected
deadline as though it necessarily chose the early branch. The existing target
model can correctly prefer fallback at that earlier instant; the test now waits
the real loss boundary and evaluates the intended early race there. Its next
failure assumed the normal queued head would pop before a newly critical
successor; assertions now check exact one-head/one-disjoint-quantum contents
irrespective of class order. Neither failure selected a runtime policy change.
Logs focused, focused-r2 and focused-r3 retain these outcomes.

Independent read-only reviews found no concrete timing, range, capacity-wake,
rank/Apply or migrated-safety-coverage counterexample in the stable candidate.
This is component evidence only. The ordinary direct heterogeneous upload is
next, with the predeclared healthy and shared opposite gates if supported.

## Ordinary pilot rejected for promotion

2026-09-10 17:43 +08:00. The unchanged85s observation guard ends an incomplete
upload:368,664,530of442,040,320B confirmed, maximum confirmation gap26.666s.
The actual target-write counter is flat60–85s while all eight native paths
remain active. This is not merely a slowly draining final tail. Reverse
confirmation delivery also remains outstanding; cleanup causes the final reset.

Sampled target service during the cut improves17.028→72.555Mbps, but already
before QoS5–15s falls321.800→67.617; restored25–40s340.910→110.545. The hoped-for
additional repair pipeline therefore brings some service but fails composition
badly. No promotion or healthy gate follows. Full ordinary record/archive:
AUTHORITATIVE_GAP_SERVICE_ORDINARY_20260910.

Source review exposes repeated whole-horizon owner queries per unavailable
structural boundary under the serialized Product lock. One active CPU core and
stalled Product service are compatible with that cause, but not proof. A single
periodic timing/region-count observer is selected to distinguish expensive
enumeration from short wake/dispatch loops or downstream native service. It
does not change timing, quantum, congestion, admission or the tested profile.
This is an information forecast, not another claimed performance correction.

## Measured work owner and equivalent query correction

2026-09-10 17:54 +08:00. The information-only capture settles exactly427,098,112B
in83.485s but reproduces19.444s confirmation gaps. It is not ordinary acceptance.
There are30,766 synchronous service evaluations totaling55.692s elapsed; nested
owner-model queries consume53.014s over7,776,602queries, clock queries only.911s.
Actual flush stamps delimit37.167s inside the target-flat/reply-held interval;
31.400s is synchronous evaluation,30.334s of it model queries. Max single call
is19.387ms there: repeated cumulative work, not one19-second critical section.
Source locates these non-awaiting calls under SharedRequestProduct, ahead of
the cooperative event selector. Per-reply lock residence is not directly
measured; nested durations cannot be summed or converted into a promised gain.

The earlier uniform-frontier sweep fixed one query's quadratic coverage work.
The new enumeration multiplied whole-horizon queries by unavailable assignment
regions, reinstating expensive repeated work at the caller. One bounded output
quantum is not a bound on the work required to select it. This practical failure
is why component ownership proofs cannot replace ordinary timing acceptance.

Selected minimum correction: build one transient per-evaluation exact-instance
view with normalized Original coverage and all-accepted-flight avoidance
coverage. All ledger spans participate, including copies crossing the queried
start. Query with the current eligible-instance mask, not a cached native/
qualification decision. Binary search locates membership at the candidate
start and its next change; the minimum such change is exactly the old constant
owner/avoid frontier. Adjacent same-identity intervals merge, but assignment
boundaries used for maturity enumeration remain separate.

Replace only the first full-horizon query. Keep the existing second scored-
quantum query for exact assignment metadata, lazy timing and immediate sibling
writeback, target observation/scoring, exact-start lower copy lookup, and final
Apply. The first view's avoidance is a set; its order never ranks targets.
Compare that set with the scored query by membership, while the latter's
target-facing order stays unchanged. No timing or shared response modification.

For F retained spans, P exact instances and B candidate regions, this removes
repeated whole-horizon endpoint construction/sorting in favor of one expected
O(F) coverage build plus per-region membership/binary searches. Existing scored
and lower-helper prefix scans remain; no blanket linear evaluator complexity
claim is made. The view is dropped after one evaluation, with no durable index,
new parameter, iteration cap or stale target reservation.

Forecast: remove a material fraction of the observed53s model work and let
receipt/claimants reach their existing service loop sooner. It cannot promise
that all19–26s gaps disappear or erase duplicate/native queue costs. RED must
exercise actual evaluation with real assignments and unavailable target, then
an available-target control, counting the existing model sweep's work rather
than a wall-time threshold. GREEN includes view/oracle range+membership checks,
old95controls and the identical ordinary QoS upload. Reject promotion again
if it still fails completion or materially regresses healthy/restored service;
do not tune the quantum, clocks, reserve or profile to rescue it.

2026-09-10 18:13 +08:00 verification: actual work RED fails only its final
count assertion,5,290visits versus2,442 after all semantic controls. The view
candidate passes101unique focused checks (1.25s runtime/98s compilation),
including five oracle tests and the existing request/response opposite controls.
Independent integration review confirms first-horizon replacement only,
per-query fresh masks and unchanged scored clock/ranking/Apply paths.
Ordinary build76575 closed successfully; next was the same affected upload.

2026-09-10 18:28 +08:00 disposition: ordinary10116 settles323,158,016B/56.678s
but remains severely adverse. Pre-cut7.336/restored35.519Mbps and maximum
write gap14.916s disprove practical acceptance of this view candidate. Its
cut confirmation64.911Mbps is not contemporaneous target service12.133Mbps.
Both forward target and reverse confirmation holds remain despite native
progress. Do not promote reduced sweep visits or smaller exact completion.
Reuse the small periodic timing observation to separate remaining eligibility,
scored-ledger/cache, lower target/native and clock work in this exact candidate.
No new scheduling rule or threshold is justified before that attribution.

2026-09-10 20:03 +08:00 timing-only72362 guard-fails. The new view reduces
observed cumulative query work, but cannot explain the remaining failure as
one work bottleneck: forward-flat16–27 has20.94% synchronous occupancy and
zero scored queries; late68–85 has equal source/target totals and continuing
ACK/response work after evaluator calls end~56s. Full nested/floor-aware
evidence is in AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md. Do not select
another prefix/native query optimization from its whole aggregate.

Independent audit finds queue-to-flight publication atomic, critical repairs
FIFO, current exact copy coverage intact. The next falsifiable composition
hypothesis is ambiguity from increased copying depriving Original owners of
unique progress, causing staleness/eligibility exclusion. Ambiguous receipts
MUST NOT manufacture path qualification; this hypothesis does not justify that
shortcut. Trace actual stale transitions, first gap and candidate owners.
The separate late return hold needs exact response Original/copy commitment
and every reply receipt/frontier. Add only those missing feature observations,
reuse existing events and reverse overlay before sameprofiletraffic. No runtime
change or common-cause assertion follows before the exact joined evidence.
