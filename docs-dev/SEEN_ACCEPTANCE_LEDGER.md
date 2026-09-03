# SEEN acceptance ledger

Status: internal execution ledger; not a release or competitiveness claim.

This document freezes the defect boundary established on 2026-09-03. A
symptom already listed here is **SEEN**. A newly discovered independent defect
or model change is **UNSEEN** and may be recorded, but must not be implemented
without explicit approval. A causal detail of a SEEN symptom is not a new
issue by itself; an independently applicable authority or architecture is.

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
   stronger behavior needs a separately named model change; or
5. **open**: any causal or acceptance question remains.

Only the complete frozen SEEN matrix can make a release candidate acceptable.
An isolated commit milestone never does.

## Frozen SEEN ledger

| Item | Scope | State | Evidence / isolated commit |
| --- | --- | --- | --- |
| SEEN-1 | Response OriginalData acquisition overrode ordinary ECF | Implementation-fixed / matrix-pending | Service-boundary RED/GREEN; `38286aa` |
| SEEN-2 | Response TCP discarded configured startup ranking prior | Implementation-fixed / matrix-pending | Typed-prior projection RED/GREEN; `a4679b5` |
| SEEN-1C | Request OriginalData acquisition overrode ordinary ECF | Implementation-fixed / matrix-pending | Direction-symmetric RED/GREEN; `842a0cc` |
| SEEN-3 | Sparse Data ACK allegedly erased receiver reorder debt | Disproved | `S - A = O + H <= W`; no code change |
| SEEN-4 | Response settlement waited on local application delivery | Implementation-fixed / matrix-pending | MPP-frontier trigger separated from local write; `444fb38` |
| SEEN-5 | Finite request replacement/ghost lifecycle | Disproved on current build | One fixed request, one attempt, complete body, no replacement; no code change |
| SEEN-6A | Persistent live-owner ACK-gap repair could renew critical authority | Fixed | Zero/partial/full cumulative-budget RED/GREEN; `0b50e9a` |
| SEEN-6B | HTTP/3 migration left QUIC latency priority diagnostic-only | Fixed | Actual Quinn priority RED/GREEN plus ordinary two-run non-downgrade; `a9450d8` |
| SEEN-6C | QUIC-only sustained bulk/read-gap and loaded-latency mechanism | Model-constrained; encompassing acceptance remains open | Current same-native-stream gap is attributed to ordered QUIC recovery; this does not close cold/warm, recovery, upload, or concurrent behavior |
| SEEN-6D | TCP-only cold/warm ramp, sustain, loaded latency, and recovery | Open; some bulk cells GREEN | Existing bulk/startup/gap improvement does not yet prove the complete reported timing and concurrency behavior |
| SEEN-6E | Default TCP+QUIC stability, failover, and 500 -> 10 -> 500 recovery | Open; current causal priority | `53d9ab5` restores the bounded live-owner frontier invariant but remains runtime-RED; the next isolated correction is the proven owner-versus-alternate completion race |
| SEEN-6F | QUIC-only 10 -> 500 recovery and cold/warm first-object behavior | Open | The user-observed slow recovery and size-dependent startup trajectory require ordinary-build timing evidence |
| SEEN-6G | TCP, QUIC, and default single-thread plus Cloudflare-style download/upload behavior | Open | Acceptance requires stable/burstable short and sustained work; aggregate bulk goodput alone is insufficient |
| SEEN-6H | Matched raw TCP, V2Ray/Xray, and Hysteria2 comparison under the declared changing 3--10% loss schedule | Open | No release-wide competitiveness conclusion has yet satisfied the frozen comparison contract |
| SEEN-7 | Retired peer rows, port projection, Evidence unknown/stale semantics, directional Suspect state | Retirement fixed; remaining current claims disproved | Absolute retirement core `e0e70e7` plus exact native-terminal-result follow-up retained in checkpoint `3a6d0ea`; later projection audit and focused tests found no additional defect |

The tracked tree at `a9450d8` contains no uncommitted production correction.
The non-release checkpoint `3a6d0ea` preserves the complete v10 integration
before the isolated SEEN corrections and is not itself an accepted candidate.

## Current SEEN-6E causal boundary

The ordinary mixed recovery artifact contains a real 1.688-second positive-read
callback gap during the QUIC impairment window. Healthy TCP transmits tens of
megabytes during that interval, so lack of alternate service, host pressure,
and continued fresh placement onto QUIC are excluded. The current diagnostic
trace shows:

1. original placement moves from QUIC to TCP shortly after the abrupt rate
   cut;
2. a lower Data Sequence range already committed to QUIC remains unresolved;
3. the receiver therefore cannot release later TCP-delivered ranges;
4. QUIC is withdrawn from new placement after the existing three-recovery-
   interval rule; and
5. stale-owner repair is then admitted on TCP and contiguous delivery resumes.

This is a Product head-of-line and recovery-latency failure, not a congestion-
controller throughput failure. Shortening the stale timer, treating a rate
sample as packet loss, or bypassing the cumulative optional-traffic ledger is
not a justified fix. History and symbolic review identified an intended
one-frontier-quantum live-owner race that `0b50e9a` accidentally removed while
correctly bounding cumulative optional repair. Commit `53d9ab5` restores that
component invariant, but its ordinary mixed-recovery repetitions remain
unstable, so it is an intermediary checkpoint rather than an accepted
correction.

The next exact trace proves a narrower timing-model defect: before the owner
fallback, the implementation compares an alternate's predicted delivery with
the fallback timer rather than comparing alternate and owner delivery for the
same frontier. That correction can remove only the timer remainder (about
89 ms in the captured run); it cannot solve the preceding evidence delay or
native ordered-service wait. The proof and commit verdicts are recorded in
`docs-dev/RECENT_SEEN_CHANGE_REFLECTION.md`.

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

## Held UNSEEN batch

These items are recorded for later decision and are not authorized for code,
RFC, configuration, public documentation, or release changes:

- 31 deterministic pre-v10/full-suite fixture expectations that remain test or
  conformance debt, not proven production defects;
- exact all-stage carrier-work and peer-service-frontier accounting;
- the RFC `STREAM_ACK(..., services)` service-frontier wire/input mismatch;
- exact `STREAM_MAX_DATA` publication cadence and partial local-write
  consumption semantics;
- a temporal service guarantee for the lowest outstanding Data Sequence range
  after abrupt owner service collapse;
- multiple native ordering domains for one logical QUIC stream;
- generic scheduler score/admission terms that may not match RFC 15.1's typed
  authority model;
- request/fixed-output typed `RateHint` cleanup outside SEEN-2;
- repeatability and benefit of stale-path requalification reinjection; and
- exact target-bound queue authority for the currently target-unbound generic
  request/response live-tail path; and
- evaluator or harness features beyond what is necessary to execute the
  already-frozen acceptance cells.

Previously focused-green components remain conditional evidence rather than
release acceptance: native QUIC authority/recovery, application-limited
Startup exit, explicit initial-rate plumbing, the bounded pre-FINAL v10
prefix, terminal admission/requalification lifecycle, absolute carrier
retirement, request-side native TCP precedence, and native QUIC loss/ECN
visibility. Their existing proofs are retained in `PROGRESS.md`; each is still
subject to the ordinary SEEN-6 composition matrix above.

## Deterministic next step

1. Correct the bounded SEEN-6E owner-versus-alternate completion race,
   including symmetric deterministic RED/GREEN proofs and independent diff
   review.
2. Commit that correction alone, then rerun only its ordinary mixed recovery
   gate; do not tune a timer or score to manufacture a pass.
3. Keep SEEN-6E visibly open unless both the component proof and affected
   ordinary runtime gate pass without a goodput or traffic-expansion downgrade.
4. Continue the remaining frozen SEEN-6 acceptance cells one at a time; a
   newly exposed causal defect is still SEEN only when it is necessary to
   explain one of those already-reported failures.
5. Record independent findings in the held UNSEEN batch, but do not implement
   them without consent.
