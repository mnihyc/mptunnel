# Request cohort clock correction

2026-09-07 07:44 UTC; ordinary result updated 2026-09-08 00:31 +08:00.
Category: bounded model correction; ordinary acceptance withheld.
Baseline runtime `11d6f3a`; candidate changes are not yet committed.

## Why change it

The real request owner accepts pipelined OriginalData and releases its exact
ACK debt, but the staged sampler refuses numeric refresh when that cohort was
assigned before the preceding ACK. The rejection advances the same ACK fence,
so continuous service can preserve an initial estimate indefinitely. The
separate expiry entry fence can also chase rejected pre-entry cohorts.

The original intention was causal staged measurement and resistance to ACK
compression. `f4206d0` made the staged predicate unconditional when removing
its ordered-service exception. The correction retains that intent at actual
acquisition/expiry boundaries, without requiring sustained service to become
stop-and-wait. Exact RED and the earlier test-fixture failure are documented in
REQUEST_PIPELINING_OWNER_RED_20260907; the pre-implementation symbolic contract
and interleaving limits are PRODUCT_COMPLETION_OBSERVATION_MODEL_20260907.

## Exact scope and tradeoff

- A fixed entry assignment floor replaces the moving post-ACK condition.
- A valid covered cohort retains paired ACK/latest-assignment endpoints.
  Subsequent chronological samples divide their unique bytes by the slower
  endpoint interval. Assignment spacing between cohorts remains included.
- Overlapping or older assignment cohorts lose only numeric eligibility. They
  do not move valid anchors or carry discarded bytes into another numerator.
  Real Product release and qualification remain independent and unchanged.
- Existing coverage, first-sample behavior, frozen expiry, no expired-EWMA
  inheritance, maturity, exact-instance identity and copy exclusion stay.
- Only the request observer, its constant-argument caller and one RFC paragraph
  change in production. No native controller, timer, rate hint, qualification
  threshold, resource, actor or lifecycle mutation is included.

The predictable benefit is current request receipt evidence during legal
pipelining, including TCP without native delivery telemetry. It is not a
native-capacity claim or proof of faster server downloads. Including source
idle and cross-cohort assignment spacing can lower an inflated old observation;
the previous 41-ms test interval correctly becomes 140 ms. Allocation-dependent
receipt rates still cannot reveal unused capacity or ordered application
goodput. Fixed anchors remove infinite overlap-only rejection under continuing
eligible progress in unchanged scope, not every long finite refresh gap.

## Focused verification

Release build with `--locked --features lab-diagnostics --lib` and three jobs
took 3m10s. The two live-owner tests took 0.31s:

| Case | Before samples after bootstrap | After | Product debt / duplicate replay |
| --- | --- | --- | --- |
| Admitted pipeline | 2, 2, 2 (RED) | 2, 3, 4 | zero / no extra evidence |
| Same staged control | 2, 3, 4 | 2, 3, 4 | zero / no extra evidence |

The same executable then passes 21 additional focused checks in 0.01s:
all six request-evidence model tests, three epoch/expiry controls, immature
reference exclusion, duplicate ACK, sub-floor qualification, carrier receipt
non-provenance, qualification retirement, detach, exact requalification,
expired/sibling rate isolation, duplicate original/copy release, exact flight
identity, partial qualification receipt splitting and replacement-copy fencing.
Total: **23 distinct passing checks**, no ignored tests. No broad suite or
duplicate live-owner rerun was needed. Independent source/RFC review passes.

The planned ordinary TCP-upload pair is now recorded in
[its evidence](REQUEST_COHORT_ORDINARY_20260907.md): both transfers remain
incomplete at the existing observation guard; the candidate improves the local
write gap but worsens the greatest confirmation gap. This one stochastic pair
does not establish a causal gain or regression. No QUIC/mixed expansion follows.
Exact confirmation bins were discarded by the existing incomplete-accounting
probe path; the artifact preserves the remaining counter series and its limits.
The correction remains component-GREEN, ordinary acceptance withheld.
