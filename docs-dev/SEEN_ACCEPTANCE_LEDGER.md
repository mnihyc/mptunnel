# SEEN acceptance ledger

Status: internal execution ledger; production baseline `93e6284` is
release-RED, not a release or competitiveness claim.

This document freezes the defect boundary established on 2026-09-03. A
symptom already listed here is **SEEN**. On the user's later instruction, the
entire formerly held UNSEEN batch was promoted into the finite v0.4.7 SEEN
plan. A causal detail of a listed symptom remains part of that issue rather
than silently creating a renewable scope. A genuinely new issue found after
this promotion is recorded for a later candidate; it does not extend v0.4.7.
The finite execution contract is `docs-dev/V0_4_7_FINITE_CLOSURE.md`.

## Why earlier milestones were premature

The individual corrections below have real deterministic proofs. The process
failure was describing a component-green correction as though it made the
whole candidate acceptable:

- scheduler and lifecycle fixtures did not exercise cross-layer timing on a
  long-lived stream;
- aggregate throughput hid application read gaps and loaded interactive tail
  latency;
- one transport/direction did not prove the opposite direction or the mixed
  carrier case;
- the already-observed cold/warm, short-object, upload, and concurrent-browser
  cells were not retained as gates after a narrower causal defect was found;
- one randomized realization could not establish a stable tail result; and
- diagnostic builds were useful for causality but were not performance-
  comparable to ordinary release builds.

No current or future issue is closed by a broad phrase such as "works". It is
closed only as one of:

1. **fixed**: deterministic pre-change RED, focused post-change GREEN,
   independent bounded review, isolated commit, and the affected existing
   runtime gate shows no downgrade;
2. **implementation-fixed / matrix-pending**: the exact implementation defect
   satisfies RED/GREEN and review, but no existing ordinary runtime cell can
   isolate it; the wider SEEN acceptance matrix remains open;
3. **disproved**: the reported symptom is real but the proposed defect is
   impossible under the stated invariant, with a concrete counterexample or
   trace explaining the actual owner;
4. **model-constrained**: implementation matches the RFC, but the requested
   stronger behavior cannot be supplied under the stated resources; this
   rejects the candidate when the behavior is an acceptance requirement; or
5. **open**: any causal or acceptance question remains.

Only the complete frozen SEEN matrix can make a release candidate acceptable.
An isolated commit milestone never does.

Every numeric network-performance threshold in this ledger and the
corresponding runtime model—including an extra-traffic percentage—is an
advisory observation, confidence, cost, ranking, probing, or reconsideration
hint. It MUST NOT become a hard bandwidth ceiling, an ordinary-work or
recovery admission cap, or a permanent path ban. Finite copy cardinality must
come from protocol structure, not a percentage gate. Explicit memory, queue,
session, and connection maxima remain operator resource contracts rather than
inferred network thresholds.

## Frozen SEEN ledger

| Item | Scope | State | Evidence / isolated commit |
| --- | --- | --- | --- |
| SEEN-1 | Response OriginalData acquisition overrode ordinary ECF | Implementation-fixed / matrix-pending | Service-boundary RED/GREEN; `38286aa` |
| SEEN-2 | Response TCP discarded configured startup ranking prior | Implementation-fixed / matrix-pending | Typed-prior projection RED/GREEN; `a4679b5` |
| SEEN-1C | Request OriginalData acquisition overrode ordinary ECF | Implementation-fixed / matrix-pending | Direction-symmetric RED/GREEN; `842a0cc` |
| SEEN-3 | Sparse Data ACK allegedly erased receiver reorder debt | Disproved | `S - A = O + H <= W`; no code change |
| SEEN-4 | Response settlement waited on local application delivery | Implementation-fixed / matrix-pending | MPP-frontier trigger separated from local write; `444fb38` |
| SEEN-5 | Finite request replacement/ghost lifecycle | Disproved on current build | One fixed request, one attempt, complete body, no replacement; no code change |
| SEEN-6A | Persistent live-owner ACK-gap repair could renew critical authority | Safety defect fixed; authority model promoted for redesign | `0b50e9a` proves and stops renewable repair, but its hard percentage budget violates hints-only semantics; redesign with structural frontier-epoch copy cardinality under P1 |
| SEEN-6B | HTTP/3 migration left QUIC latency priority diagnostic-only | Implementation-fixed / matrix-pending | Actual Quinn priority RED/GREEN plus ordinary two-run non-downgrade; `a9450d8` cannot waive native-HOL or final matrix gates |
| SEEN-6C | QUIC-only sustained bulk/read-gap and loaded-latency mechanism | Model-constrained; encompassing acceptance remains open | Current same-native-stream gap is attributed to ordered QUIC recovery; this does not close cold/warm, recovery, upload, or concurrent behavior |
| SEEN-6D1 | TCP-only loaded interactive latency while bulk occupies every configured TCP carrier | Current model-constrained; promoted P2 decision open | Current unified matrix: 303.372 Mbps bulk but 1,417 ms interactive p95 versus 522 ms raw TCP and 538 ms V2Ray; exact diagnostics show zero MPP command wait and hundreds of KiB to MiB of already-native FIFO debt. A later priority frame cannot overtake that prefix without a hard handoff bound or another ordering domain |
| SEEN-6D2 | TCP-only cold/warm ramp, sustained single-stream service, and impairment recovery | Open; current bulk cell GREEN | The current unified matrix disproves a sustained aggregate collapse in its one realization, but the separately reported cold/warm and recovery trajectories remain to be closed by their existing gates |
| SEEN-6E | Default TCP+QUIC stability, failover, and 500 -> 10 -> 500 recovery | Captured execution matches current RFC; promoted P1 model correction open | `93e6284` fixes owner double-counting. A clean diagnostic replay still produced a 1.064233-second gap through RFC-conforming sequential repair, proving that timer tweaks are not the remaining decision |
| SEEN-6F | QUIC-only 10 -> 500 recovery and cold/warm first-object behavior | Open | The user-observed slow recovery and size-dependent startup trajectory require ordinary-build timing evidence |
| SEEN-6G | TCP, QUIC, and default single-thread plus Cloudflare-style download/upload behavior | Open | Acceptance requires stable/burstable short and sustained work; aggregate bulk goodput alone is insufficient |
| SEEN-6H | Matched raw TCP, V2Ray/Xray, and Hysteria2 comparison under the declared changing 3--10% loss schedule | Matrix measured; acceptance remains open | One current six-way ordinary-build cohort is complete and valid; MPP bulk is competitive, but SEEN-6D loaded TCP latency fails, so the cohort cannot authorize release |
| SEEN-7 | Retired peer rows, port projection, Evidence unknown/stale semantics, directional Suspect state | Retirement component fixed; P7 truthfulness gate open | Absolute retirement core `e0e70e7` plus exact native-terminal-result follow-up retained in checkpoint `3a6d0ea`; current mapping, absence, staleness, direction, sorting, and browser projection remain assigned to P7 |

The tracked tree at `a9450d8` contains no uncommitted production correction.
The non-release checkpoint `3a6d0ea` preserves the complete v10 integration
before the isolated SEEN corrections and is not itself an accepted candidate.

## Current SEEN-6E causal boundary

At `93e6284`, two valid ordinary repetitions disagreed at the application
tail: 0.812670 versus 3.580808 seconds maximum positive-read gap. The latter
pinned the 64-MiB Product window while native transports continued service, so
one favorable repetition cannot close the issue.

A focused diagnostic replay then isolated one 14,600-byte missing Product
range. The captured execution followed the current RFC: hole evidence took
571 ms to arise and 184 ms to return, fallback added 99 ms, and three distinct
TCP copies were admitted sequentially at roughly 273-, 253-, and 256-ms
intervals. Product-command wait was zero, writer handoff was 1--2 ms, and the
third domain delivered after another 251 ms, producing a 1.064233-second gap.

This proves the timer and actor-wakeup hypotheses wrong for that replay. It
does not prove every recovery execution correct, nor does it prove concurrent
copies faster under a shared bottleneck. P1 must replace the hard percentage
admission semantics with a structural live-copy identity, then adjudicate
sequential, staggered, and concurrent service against joint offered load.
`dc4853d` and `93e6284` remain exact local comparator/accounting corrections;
their broad performance benefit is not claimed. Full detail is in
`docs-dev/RECENT_SEEN_CHANGE_REFLECTION.md` and the finite closure plan.

Fixing this causal cell does not close the wider SEEN-6 matrix. The retained
matrix still requires, for TCP-only, QUIC-only, and default TCP+QUIC:

- cold and warm first-object behavior;
- one traditional single-stream transfer;
- Cloudflare-style concurrent download and upload trajectories;
- sustained behavior and recovery after a 10-to-500 or 500-to-10-to-500
  service transition, as applicable; and
- matched raw TCP, V2Ray/Xray, and Hysteria2 comparisons under the declared
  changing 3--10 percent loss schedule.

These are SEEN acceptance cells because the user reported failures in them
before this ledger was frozen. Missing evidence cannot reclassify them as new
scope. Each causal correction must remain isolated, but every cell remains
open until it is run against the ordinary candidate build.

## Promoted SEEN batch

The formerly held batch is now authorized and finite. Promotion does not
presume that each item requires production code; deterministic processing may
close one as disproved, unreachable, model-constrained, or stale fixture debt.

| Item | Promoted scope | Why it is necessary / finite closure |
| --- | --- | --- |
| SEEN-P1 | Lowest-frontier temporal service and the hard optional-repair percentage gate | Necessary: exact diagnostics show RFC-conforming sequential copies contribute observed delay, while a percentage currently denies recovery as a hard cap. Replace budget authority with structural live-copy identity, then prove or reject each service policy under coupled load. |
| SEEN-P2 | Sufficiency of native ordering domains, including a possible additional QUIC/TCP domain | Already triggered for TCP by SEEN-6D1; P1 may also trigger QUIC. Prove current-domain sufficiency, validate one clean domain design, or reject the candidate as model-constrained. |
| SEEN-P3 | Request/fixed-output typed rate hints, omitted-hint startup, and stale-path requalification | Necessary: cold/warm startup and restart-dependent 10-to-500 recovery are reported SEEN symptoms. Close with exact authority chronology and restart-free recovery. |
| SEEN-P4 | Generic score/admission conformance, uncertainty, flapping, and overlapping contention | Necessary at the carrier-neutral decision boundary for reported L4 default-path sway and marginal path swaps. Experimental L3 inherits correctness only. Overlapping latent-factor inference is processed by proving it necessary or irrelevant to v0.4.7; it is not automatically implemented. |
| SEEN-P5 | Version-10 `STREAM_ACK(..., services)`, `STREAM_MAX_DATA`, partial local writes, target-bound tail authority, and service-frontier accounting | Necessary protocol/conformance audit. The service-vector/all-stage-ledger branch is now rejected as an unproved RFC model rather than implemented; remaining items still need a reachable RED/GREEN or explicit unreachable/irrelevant proof. No compatibility branch. |
| SEEN-P6 | 32 current pre-v10/full-suite failures, including the three named stale fixtures | Necessary for release/CI truthfulness, not presumed production defects. Map each premise to the current RFC, then update stale tests or fix a reachable product mismatch. |
| SEEN-P7 | Truthful stage-local metrics, path/incarnation projection, and recovery-score observability | Necessary for the already reported stale/zero/reversed/mapping diagnostics and for causal release evidence. Unsupported values remain unavailable rather than synthesized. |
| SEEN-P8 | Final TCP, QUIC, default, short-object, single-stream, concurrent down/up, recovery, and matched-baseline matrix | Necessary release gate. Three complete valid cohorts, with at most two additional whole cohorts only for conflicting paired signs; no isolated lucky reruns. |

An evaluator or harness extension with no direct need in P1--P8 is processed as
irrelevant and rejected from v0.4.7. The existing runner may receive only the
minimum instrumentation needed to execute an already-listed gate.

Previously focused-green components remain conditional evidence rather than
release acceptance: native QUIC authority/recovery, application-limited
Startup exit, explicit initial-rate plumbing, the bounded pre-FINAL v10
prefix, terminal admission/requalification lifecycle, absolute carrier
retirement, request-side native TCP precedence, and native QUIC loss/ECN
visibility. Their existing proofs are retained in `PROGRESS.md`; each is still
subject to the ordinary SEEN-6 composition matrix above.

## Deterministic next step

1. Commit the explicit reflection and finite plan without production changes.
2. Process the exact atomic order T01--T14 in
   `docs-dev/V0_4_7_FINITE_CLOSURE.md`. The RFC authority boundary precedes
   typed rate/unit, advisory score, and admission; all precede P1 because
   owner/alternate projections consume them.
3. Preserve each earlier focused GREEN as a non-downgrade gate. Do not tune a
   timer, percentage, score, or test condition to manufacture a later pass.
4. A failed or unresolved required package rejects v0.4.7; it cannot create a
   new package or an isolated lucky rerun.
