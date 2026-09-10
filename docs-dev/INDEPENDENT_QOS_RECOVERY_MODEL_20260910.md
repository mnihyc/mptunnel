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
