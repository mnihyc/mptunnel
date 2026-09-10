# Current deterministic closure plan

Updated: 2026-09-10 17:56 +08:00. Authoritative repository: `./`.
**MPP is not performance-accepted. No release, push or public README update.**
Continue the authorized closure loop; do not conclude at an intermediary commit.

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each new
transaction and after compaction. This is the active decision ledger, not a
growing inventory. Complete preceding chronology and adverse outcomes remain at
`git show 79ddb41:docs-dev/CURRENT_CLOSURE_PLAN.md`, earlier49143f2, and the
linked reports/archives. Condensation discards no experiment.

## Active transaction: remove demonstrated repeated ownership-query work

**Ordinary request pilot79ddb41 failed. Work RED79277 is confirmed.**
Actual evaluator takes5,290 sweep visits versus2,442 after blocked-capacity and
released-capacity semantic controls pass (.22s runtime). View implementation
is now in progress; root integration complete, oracle tests/independent review
are finishing. Root owns cargo/labs; no compiler or lab is running.

The original independent-link failure is real: on two200Mbps links, only46
slows200→10→200 during15–25s while47 stays healthy. Existingb0 upload averages
249Mbps but RAW16–24 service collapses16.203Mbps versus healthy47control185.887.
Exact observer locates14.6KB repair quanta108–203ms apart: an already accepted
head repair prevents independently due authoritative omissions from service.
No sustained new Original/copy placement on impaired46 explains that interval.
[Exact model/evidence](INDEPENDENT_QOS_RECOVERY_MODEL_20260910.md).

The request pilot selects one independently due omission after excluding queued
and unexpired-copy service, without declaring those bytes received. Actual
pre-change RED refuses the second otherwise serviceable14,600B extent.95focused
checks pass after correction, including actual commitment before head ACK,
immutable clocks/ACK splits, exact occupied slots and the old T06 rank/Apply
bound. Shared response/io, silent retained fallback and structural recovery are
unchanged. This is a prospective service-model correction, not a cost-neutral
code mismatch or proof that response is fixed. RFC.md is unchanged so far.

### Actual ordinary failure and dominant work evidence

[Full ordinary/profile report](AUTHORITATIVE_GAP_SERVICE_ORDINARY_20260910.md)
retains the failed candidate and the separate timing-only observation.

- Ordinary53390 hits unchanged85s guard:368,664,530B confirmed of442,040,320B
  accepted;26.666s confirmation gap. Not a completed34.503Mbps result.
- Sampled target service5–15s falls321.800→67.617Mbps BEFORE QoS; strict16→24
  improves17.028→72.555; restored25–40 falls340.910→110.545. Target writes are
  flat60–85s while native counters progress. Reverse confirmations also remain.
- Healthy47wire service during the cut rises31.016→165.060Mbps, but useful
  ordered service remains much lower. All86profiles match, zero qdisc drops;
  summed UP backlog peaks15.06→63.22MB. No favorable healthy rerun follows.
- Information-only37140 closes exactly427,098,112B/83.485s with19.444s gap.
  It is not an ordinary replacement.30,766 synchronous gap evaluations consume
 55.692s; nested owner model53.014s/7,776,602queries; clock work.911s.
- In an actual37.167s target-flat/reply-held window,31.400s is synchronous
  evaluator work,30.334s nested model work. Largest call19.387ms there, NOT one
 19s lock hold. Source places these non-awaiting calls under SharedRequestProduct
  before cooperative I/O. Exact per-reply lock residence is unmeasured; nested
  wall durations cannot be added or treated as a promised time saving.
- Feature timing overlay is archived and fully reversed before traffic.
  Ordinary source is79ddb41; feature executable remains separate. No native
  controller, queue limit, quantum, deadline or profile was changed.

Reflection: the old sweep fixed one query's work. The new enumeration multiplied
whole-horizon queries by candidate regions. One bounded repair output is not
bounded selection work. The practical failure rejects promotion despite95GREEN.

### Bounded next correction and proof

Build ONE transient exact-instance ownership view per evaluation: normalized
Original coverage and all-accepted-flight avoidance coverage. Include crossing
spans. Query with the CURRENT eligible-instance mask; do not cache native or
qualification decisions. Membership changes only at union endpoints, so the
earliest change exactly matches the old constant owner/avoid frontier.

Replace only the first whole-horizon owner query. Keep the second existing
scored-quantum query for assignment metadata, lazy actual-ledger clocks and
immediate sibling writeback, exact-start lower copy lookup, fresh target/K/native
scoring and final Apply. Avoidance is compared as a set; only its membership
is consumed from this first view, never target tie order. Original owners must
still be exactly one. No durable index, new threshold or response implementation.

Forecast: remove a material fraction of53s repeated model work and let existing
receipt/claimant service run sooner. No promise that all19–26s gaps disappear;
copy costs, native queues and remaining scored/lower-helper prefix scans persist.
Expected view buildO(F), then per-region/per-path binary-search membership;
do not claim the whole evaluator becomes linear or constant-time.

RED79277 used real assignments, actual blocked alternate command capacity and
a released-capacity positive control. Last assertion counts existing frontier
sweep visits, not wall-clock time or all remaining prefix work.
Test: authoritative_request_gap_evaluation_does_not_repeat_full_horizon_sweeps.
After intended RED: implement view and oracle equivalence checks, retain95
existing controls, build ordinary candidate, repeat SAME aggregate QoS UP cell.
Failed completion or material healthy/restored harm stops promotion and selects
its exact cause; no quantum/timer/reserve/profile rescue.

## Next ordinary contract — topology retained, no setup project

Owned direct topology: client eth0=46.10/eth1=47.10; server eth1=46.20/eth0=47.20.
Two independent200Mbps cuts, each shared by its own TCP+QUIC; not eight physical
paths. UP70/DOWN30ms; no loss/jitter/UDP outage in this discriminator.
Only46UP200→10→200 at15–25s;47 remains200.40s offered, existing50s probe
completion timeout/85s runner guard. Preserve RAW windows and exact completion,
first service/gaps, actual class/native/target progress, wire/CPU/RSS.
No compiler overlap; host load need not be zero.

Use existing run.py aggregate combined up, REFLECTION_LINK_RATE=200mbit,
MIRROR_IMPAIRMENT=1, NO_LOSS=1, NO_JITTER=1, NO_BLACKHOLE=1, MANAGEMENT=1.
Unset ROUTED, NO_QOS, per-role binaries and diagnostic/target/return overrides.
Select a new frozen ordinary executable/tag; never overwrite earlier capture.
If supported, next healthy aggregate UP(NO_QOS=1), then affected shared500
controls. Before routed reuse restore exact routes/network attachments and
remove only gate-owned endpoint HTB roots; never stack router/endpoint shaping.

## Prior practical results and unresolved owners

1. **Native TCP refillb0baca2 remains working, not globally accepted.**
   Exact e722c46 observation attributes1.353s of1.497s echo to native unsent FIFO.
   Native LOWAT131072 plus fresh Original preclaim/writer readiness improves
   TCP DOWN417→440Mbps, echo p951269→323ms; TCP UP422→450 exact.
   Both mixed DOWN orders lose~5% late throughput; copies189→295MB and some
   tails worsen. Do not tune LOWAT or suppress all repairs to erase this.
   [Ordinary/costs](NATIVE_REFILL_ORDINARY_20260910.md),
   [copy attribution](NATIVE_REFILL_COPY_ATTRIBUTION_20260910.md).
2. **Healthy independent aggregation works before heterogeneous failure.**
   One200→two200: DOWN148→302Mbps, echo p95918→262ms; UP169→316 exact.
   UP maximum confirmation/write gaps.411→.420/.317→.528s worsen; CPU/RSS grow.
   [Independent report](NATIVE_REFILL_INDEPENDENT_20260910.md).
3. **Older mixed healthy UP5.140055s hold remains not exactly attributed.**
   Later unchanged target observation did not reproduce it. Sink history is a
   real observer resource issue, not proved MPP leak/cause; don't rewrite it.
4. **Receipt-before-target-parkba56290** fixes real ACK liveness,93server+7
   controls. It is not proved5s cure; ordinary speed/timing tradeoffs retained.
   [Receipt report](TARGET_WRITE_RECEIPT_ORDINARY_20260910.md).
5. **Return-round364d417** removes impossible two~100ms proof rounds in one
   ~175ms budget. Restricted DOWN202→385/UP340→416, but outage gaps worsen.
   No timer rescue; [round evidence](RETURN_ROUND_ORDINARY_20260910.md).
6. **Shared native contention and mixed copy service** remain conditional costs,
   not all declared physical necessity. Post-refill mixed outage gap1.21s has
   native progress; exact64B observer shows150ms before TCP repair admission and
   968ms after, not a late wake. Q-only recovers~449Mbps after sole-UDP outage,
   but loses echo availability; no fixed QUIC preference follows.
   [Recovery attribution](NATIVE_REFILL_RECOVERY_20260910.md).
7. **Restart/ownership/lifecycle** have real focused checks and1941mixed+64single
   churn reclamation. Deployed random RAM/CPU incident is not fully attributed.
   No hypothetical re-audit or erased adverse results:
   [dispositions](CHANGE_DISPOSITION_20260907.md),
   [reflection](PERFORMANCE_REFLECTION_20260907.md).

Current complete12cell harsh asymmetric matrix is retained in
[NATIVE_REFILL_COMBINED_20260910](NATIVE_REFILL_COMBINED_20260910.md):
DOWN TCP/Q/mixed/raw/Xray/H2=404.684/311.418/312.384/363.575/299.315/144.912Mbps,
with all failed echoes/tails. UPQ96.986/mixed62.511/raw7.293 settle; TCP and H2
are guard-censored, Xray terminal-closure censored. No completed-speed ranking
of incomplete results. This poor-all-baselines case is diagnosis, not a public win.

## Unchanged global acceptance order

1. Close material mixed allocation/startup/stalls with exact causes and ordinary
   completion, first service, time series, read gaps and loaded latency.
2. Both directions/all3MPP modes under changing loss/jitter, sudden QoS,
   blackhole and restart-free recovery, with their ablations.
3. Single500Mbps and independent200Mbps links, shared/asymmetric cuts,
   aggregation plus simultaneous changing impairments.
4. Cold/warm short objects, sustained single/concurrent work and actual
   speed.cloudflare.com; matched rawTCP, Xray, H2, MPTCP where available.
5. Restart/churn, post-load reclamation and CPU/RSS; platform checks.
6. Only then publish honest README/PERFORMANCE timing/latency curves, costs and
   completion, and release after practical competitiveness. No universal
   instantaneous optimum under unknown future events; no avoidable-stall waiver.

Pinned harsh profile: routed500Mbps, UP70±20ms/DOWN30±5ms; five-second loss
UP[3,8,5,6,10,3,5,8]%mean6, DOWN[1,2,.5,3,2,.5,1,2]%; UP10Mbps15–25s,
wholeUDP30–33s.40s offered,85s runner/90s probe guards. No changing it to pass.
Healthy/heterogeneous phases, not a100Mbps threshold, determine usable speed.

## Execution safeguards and continuity

Preserve prepared/claimed ownership, receipt truth, exact incarnation/copy slots,
rank/Apply extent, nonrenewing clocks, half-close/cancel and current capacity
wakes. No guessed bandwidth, fixed protocol preference/bottleneck partition,
new resource cap, or revived rejected candidate without contrary evidence.
Correctness, sustained useful delivery, latency and wire/resource costs are
joint acceptance; neither component GREEN nor high average Mbps suffices.

Frozen binaries under .tmp/reflection/bin:
- native-refill-20260910: ordinaryb0 comparator.
- authoritative-gap-service-request-20260910: failed ordinary79ddb41 trial.
- authoritative-gap-service-profile-20260910 and wrapper: diagnostics ONLY.
- target-write-receipt-20260910: ba56290; return-round-20260910:364d417.
Other previous observers/ordinary comparators remain, not cleanup targets.

No sudo, host shaping, outside-root work or /mnt/storage use. Owned Docker only.
No deletion in this transaction. User's seven-line
docs-dev/LIVE_OWNER_FRONTIER_WORK_BOUND.md remains untouched/unstaged.
Use exact intermediary commits; docs-dev needs exact force-add. PROGRESS is an
ignored continuity journal, not force-added. AGENTS.md is immutable.

Telegram failure milestone sent09:43UTC; next nonurgent after10:44UTC.
Meaningful measured milestones/blockers only; commentary within60s, minute
lab/build polling. Before compaction preserve exact active session, next
decision, source/binary identities and adverse/open outcomes. Continue, do not
end execution because a candidate or checkpoint closed.
