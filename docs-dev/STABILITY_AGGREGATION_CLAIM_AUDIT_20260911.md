# Stability and aggregation: bounded claim audit

2026-09-11. Read-only evidence review; **no new test, build or traffic result**.
Scope: challenge preceding conclusions against preserved captures and the
current release plan. Runtime candidate is 04f1e56; older source identities below
are intentional. Active topology cells are pending evidence, not presumed passes.

The user's triage governs: a speed deficit within 20% or an isolated gap under 3s
does not justify tuning in this batch. Preserve such observations without
calling them harmless; repeated disruption, sustained collapse, failed recovery,
corruption, deadlock and unreclaimed ownership remain relevant severe risks.

## 1. Native competition explains mixed-mode weakness

**Supported narrowly; an exclusive or unavoidable explanation is unproven.**
On ordinary 011/shared 500, adding three external raw TCP downloads to QUIC-only
raises echo p95 from 155.291 to 412.535ms and reduces QUIC goodput from 429.451
to 179.994Mbps. Shared queue and native RTT also rise, without any MPP TCP carrier
in that session. The raw sidecar receives 284.129Mbps in its own, nonidentical
window; do not sum those whole rates as an exact common-window total.
[Exact evidence and limits](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md#native-competition-discriminator-external-tcp-reproduces-common-delay).

This establishes a material native/shared-contention contribution, not that
MPP allocation, copies or local service contribute nothing. Ordinary mixed
398–404Mbps versus single-mode 429/443Mbps is within the user's speed triage;
that average and subsecond tails alone do not select another queue/rank fix.
**Cheapest remaining check:** the already-declared current-source protocol-
isolated cuts. A sustained split-link collapse despite serviceable singleton
cuts would falsify a blanket shared-bottleneck explanation, not identify its fix.

## 2. Aggregation works, so healthy capacity should prevent QoS stalls

**Healthy aggregation is supported in one envelope; the implication is false.**
Earlier b0 ordinary 200+200 has DOWN 148.236→302.319 and UP 169.362→315.648Mbps
versus one 200Mbps cut. Both physical cuts carry substantial traffic and whole
ordered delivery exceeds one cut. This is real aggregation, not just duplicate
wire bytes. Both cuts have TCP AND QUIC; it is not TCP46+QUIC47 isolation.
[Topology, counters and complete timing](NATIVE_REFILL_INDEPENDENT_20260910.md).

The later 04f prefix diagnostic 77827 proves an assigned missing-prefix hold:
receiver R=completed-write W, client A=MAX, unassigned U=0 and a known gap coexist
with large received suffixes; the matched target write takes 1.719ms after release.
Diagnostic 19038 similarly isolates a 3.979938s receiver gap and three sampled
future-clock decisions for its exact head. Neither proves every stall's owner,
the releasing copy, or that shortening its clock would improve composed service.
[77827](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md#clipped-range-prefix-diagnostic77827-received-versus-assigned-work),
[19038](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md#exact-head-diagnostic19038-a-future-original-clock-on-the-longest-hole).

Thus healthy alternative capacity does not guarantee timely ordered prefixes.
The urgency trial's practical failure and removal must not be erased by its RED.
**Cheapest check:** finish the declared singleton/split ordinary cells and the
two protocol orientations under the same QoS/outage. Use exact settlement and
healthy/restored phases, actual cut counters, target writes and returned replies.
Neither eight carriers nor native ACK progress proves useful prefix delivery.

## 3. Lifecycle is finite / the memory incident is fixed

**Reproduced ownership corrections are supported; general leak freedom is not.**
The earlier ordinary held composition retained 1,571 server owners after 1,932
completed requests. The corrected finite run completes 958+983 mixed requests
without restart and has zero logical/admission/per-path owners after each cycle
and 68s final quiet; TCP-only 32 and QUIC-only 32 also reclaim. Final reported
Product flight and queue are zero; native carrier traffic is separately preserved.
[Failed gate](SUSTAINABILITY_CHURN_20260907.md),
[Corrected ordinary record](TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907.json).

This closes those demonstrated terminal mechanisms, not unlimited-duration
stability, exact heap attribution or the uncaptured deployed RAM incident.
RSS staying above startup is not alone a leak; finite tests are not a wall-clock
service bound. **Cheapest check:** existing restart/half-close/cancel/churn guards
on the frozen release source, reusing ordinary evidence where that owner is
unchanged. If a current ordinary gate leaves suspect retention, keep the same
process alive for the existing post-load observation; killing it cannot prove
reclamation. No new long-duration stress framework is selected.

## 4. High-loss CPU caused the collapse / CPU is harmless

**Both blanket statements are unsupported.** Actual process-tick collection
reproduces startup server peaks of 101% QUIC and 138% mixed CPU at 20% loss.
Late 30→40s collapse instead has server means 3.87%/7.11% and client 1.73%/8.35%.
That falsifies sustained MPP CPU saturation for those late intervals, not the
deployed incident or every host scheduling explanation. Process-total one-core
work is real even when no single sampled thread reaches one core.
[Four ordinary cells and clock definitions](QUIC_LOSS_CPU_20260910.md).

The complete native-snapshot diagnostic bounds recorded server snapshot CPU at
62.019ms including rounding, at most 6.14% of the reproduced 1.01CPU-s startup
peak. Snapshot dominance is falsified for that capture; the remaining owner is
unattributed. Native contraction is observed, but neither its exact magnitude
nor slow reopening is proved unavoidable. Ordinary 011 loss-clear bins 20–23
remain 15–20Mbps, while H2 is already above 400Mbps; policy/rate priors differ.
**Cheapest check:** the existing frozen-current versus released loss-clear gate
with actual process ticks and complete recovery series. Only a reproduced
sustained CPU/recovery failure warrants further attribution; no snapshot rewrite
or privileged profiling workaround follows from these historical captures.

## 5. Current is nonregressing / stable for browsing, uploads and gaming

**Not yet established against the published release or across those workloads.**
The recorded d1a99ad release-control versus b9a1600 composition comparison shows
large gains in one harsh profile, but those are not current 04f. Recent a16/011/
04f comparisons are internal candidates, not a new v0.4.8 release comparison.
[Historical release comparison](PRUNED_RELEASE_COMPARISON_20260907.md).

Bulk plus 64B echo and complete upload settlement support their measured tasks;
they do not certify arbitrary browser/gaming behavior, platform compatibility,
DNS/bypass correctness or recovery after restart. **Cheapest check:** reuse
matching closed cells in the already-declared finite release gates, confirm
the exact frozen binary (target/release still contains a rejected trial), and
run only existing functional/lifecycle/platform and browser checks as planned.
Do not announce their results before they close. An incomplete upload stopped
by the guard remains incomplete; teardown reset is not autonomous failure.

## Disposition

Keep demonstrated mechanisms and bounded improvements; withhold universal
stability, inevitable mixed penalties, deployed-incident closure and current-
release nonregression claims. The topology comparison addresses a missing
premise, not an invitation to revive rejected timer/queue models.
Only severe observed failure selects its smallest causal check or correction.

## Closed current-source challenge, 2026-09-11

The planned protocol-isolation cells and selected controls are now complete;
the earlier pending statements above record their original question, not a
current absence of results. See the complete
[isolated-link record](ISOLATED_LINK_STABILITY_20260911.md), including invalid
attempts, ordinary comparisons and separately labelled diagnostics.

- **Independent download aggregation is real, within the measured envelope.**
  Current04f on two200Mbps cuts gives TCP46=179.278, QUIC47=178.857 and
  TCP46+QUIC47=309.377Mbps. All three have80/80 echoes and maximum body gaps
  below.401s. The split is72.57% above the better singleton, not a promise of
  their summed capacity. Upload completes exactly in all three cases but the
  split182.160Mbps has no material gain over181.284Mbps QUIC alone. Do not call
  both directions equally aggregating or open a small-gain tuning task.
- **Shared contention is not the whole mixed-mode explanation.** With TCP47
  unchanged at200Mbps and QUIC46 temporarily10Mbps, split DOWN QoS service is
  13.381Mbps while TCP47 alone delivers183.595Mbps in the matching phase.
  Split UP has a5.699s confirmation gap; target and native TCP progress during
  that hold prove it is not wholly a forward-service freeze. Publishedv0.4.8
  also suffers in the split controls, so this is not solely a new04f defect.
  The failure is material under the user's triage and remains unresolved.
- **The proposed recovery correction is not accepted.** Actual-producer RED
  and170 focused GREEN checks support its local nonserialized copy mechanism;
  ordinary healthy throughput drops40.37% and a3.3008s body gap appears despite
  much better QoS throughput. Trialec8cd2f is fully withdrawn by843a63f. The
  exact-G follow-up also falsifies the easy next filtering hypothesis. No
  controller gain, deadline, reserve or protocol preference is changed.
- **Lifecycle and CPU conclusions retain their limited scope.** These
  protocol-isolation results neither undo the documented exact reclamation
  evidence nor close the deployed RAM/CPU report. No new profiling result or
  permission exists. General platform/browser/restart acceptance is not inferred
  from bulk+echo tests. The release remains blocked by severe recovery service,
  not by a requirement to eliminate every small performance difference.
