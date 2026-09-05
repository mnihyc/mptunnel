# Current deterministic closure batch

Updated: 2026-09-05T19:38Z. Baseline: `6636091`. This ledger supersedes the
current-status labels in the historical v0.4.7 ledgers, not their evidence.
User authorized all recorded leftovers in this batch and commits afterward.
No push or release is inferred from that instruction.

## Closure rule

Every item ends as a verified root-cause correction, an evidence-backed
no-change/model constraint, or an explicit blocker. Passing a defect-asserting
diagnostic is not acceptance. A test-only checkpoint is not a production fix.
No tuning of rate/loss/freshness thresholds to manufacture healthy status.
No parameter may claim authority its measurement does not establish.

## Inventory and order

| Item | Status | Closure evidence required |
| --- | --- | --- |
| R1: blocked-write/stale-generation stream reset | Corrected,0545610 | Real actor terminates without releasing the sink for current and obsolete generations; control/lifecycle and full root suite GREEN |
| M1: active/parked loss-transaction ownership | Corrected,42d1b86 | Exact epoch dispatch and non-reused lineage identities; expiry/late ACK/CE, migration rollback, journal and native suite GREEN |
| R2: creation versus later startup enrollment | Corrected,89037a8; deliberate wire11 break | Absent STARTUP never allocates target state; retained enrollment and either initial ordinal work; versions9/10 rejected |
| R3: congested native close never reaches peer | Corrected,c6ce159; upstream PR2787 | CUBIC and BBR3 peer-event tests RED then GREEN without advancing time; no ordinary admission/timer change |
| D1: rate/share/pacing/direction presentation | Corrected,6545270 | Live TCP/QUIC counters and browser checks; interval-normalized measured shares, unequal intervals, direction, stale/idle/missing/reset/u64 precision, sorting and compact layout pass |
| N1: native QUIC probe/drain positive feedback | OPEN latency/model limitation; no accepted controller patch | Native component and forward downshift reproduce queued-RTT inflation. Dequeue-verified forward and ACK-only restoration both recover without restart. No claim that every reported recovery case is resolved |
| TCP startup/service recovery and loaded latency | Historical performance work remains OPEN | Already-native FIFO debt cannot be overtaken by a later priority frame in the same ordering domain. No new theoretical impossibility or complete recovery guarantee inferred; no replacement model is bundled |
| Cold/warm and concurrent browser down/up | Wider acceptance still OPEN | These incident tests do not replace the matched raw/V2/H2 and three-MPP-mode browser/single-stream matrix |
| Mixed allocation/flapping | Specific T06 defect released-fixed inbfac5b8 | Retain ranked-frontier repair; recorded v0.4.8 mixed/QUIC336/332-Mbit/s evidence is historical acceptance, not a new universal competitiveness claim |
| C1: MAX_DATA cadence | Corrected,5c1d288 | Sub-threshold freed prefix RED then GREEN; no byte gate; existing latest-value publication owns coalescing |
| C2/C3: partial writes and target-bound tail | Evidence-backed NO NEW RUNTIME CHANGE | Partial-write cursors retain the batch across Pending; scalar/vectored controls pass. Exact final-tail target and live frontier authority already exist. See CREDIT_CADENCE_CLOSURE.md |
| Dashboard sorting | Existing34931a5 retained and verified | Numeric ascending/descending/default order passes browser arithmetic controls; no second sorting implementation |
| Documentation/old SEEN ledger | Reconciled as historical records | Dated supersession notes point here; old32-failure labels do not describe the current2316-passing library suite |
| Android companion/upstream status | Not verified or modified in this core batch | Separate authoritative repository location/status is not established here. No Android push, upstream-merge or release claim |
| Build/evidence leftovers | Owned services stopped; generated key and sparse8GiB fixture deleted | About93MiB diagnostic evidence and root build cache retained; earlier target deletion denial was not bypassed |

Optional overlapping-contention research is not a proven required repair. Its
disposition needs a necessity argument, not implementation merely because it
was proposed. The specific deployed RAM incident still lacks a capture;
verified journal retention and queued-payload owners must not be conflated.

The existing subagents are unavailable due to their usage limit. Root can
implement and verify locally but must not claim independent review occurred.

## Final local verification

- `cargo test --locked --all-features`:2316 library tests plus6/2/6 integration
  groups passed; remaining groups have zero tests. Log `.tmp/batch-root-acceptance.log`.
- `cargo clippy --locked --all-targets --all-features -- -D warnings`:passed.
- Native `quinn-proto` release suite:450 tests plus3 doctests passed.
- Root formatting, JavaScript syntax and diff-whitespace checks passed.
- Browser `.tmp/d1-browser/verification-share-final.log`:rates/model/pacing,
  first/missing/idle/stale/reset/direction, u64 deltas, unequal intervals and
  three-state numeric sorting pass. Screenshot `output/playwright/d1-native-peer-final.png`.
- The first all-feature run exposed one obsolete byte-share string assertion;
  it was replaced with the interval-normalized measured-share assertion, not
  waived. The subsequent complete suite passes.

No push or release has occurred. These commits are verified corrections, not
a claim that all historical performance requirements or the uncaptured random
RAM incident are solved. Remaining N1 changes require an effective model
correction, not another selected gain or threshold. The specific deployed RAM
incident requires a capture to attribute its remaining owner conclusively.

Detailed urgent mechanisms and invariants remain in
`RESTART_RETENTION_RATE_FIX_PLAN.md` and its three linked diagnosis reports.
