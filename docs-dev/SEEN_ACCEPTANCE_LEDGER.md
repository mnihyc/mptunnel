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
- one randomized realization could not establish a stable tail result; and
- diagnostic builds were useful for causality but were not performance-
  comparable to ordinary release builds.

No current or future issue is closed by a broad phrase such as "works". It is
closed only as one of:

1. **fixed**: deterministic pre-change RED, focused post-change GREEN,
   independent bounded review, isolated commit, and the affected existing
   runtime gate shows no downgrade;
2. **disproved**: the reported symptom is real but the proposed defect is
   impossible under the stated invariant, with a concrete counterexample or
   trace explaining the actual owner;
3. **model-constrained**: implementation matches the RFC, but the requested
   stronger behavior needs a separately named model change; or
4. **open**: any causal or acceptance question remains.

Only the complete frozen SEEN matrix can make a release candidate acceptable.
An isolated commit milestone never does.

## Frozen SEEN ledger

| Item | Scope | State | Evidence / isolated commit |
| --- | --- | --- | --- |
| SEEN-1 | Response OriginalData acquisition overrode ordinary ECF | Fixed | Service-boundary RED/GREEN; `38286aa` |
| SEEN-2 | Response TCP discarded configured startup ranking prior | Fixed | Typed-prior projection RED/GREEN; `a4679b5` |
| SEEN-1C | Request OriginalData acquisition overrode ordinary ECF | Fixed | Direction-symmetric RED/GREEN; `842a0cc` |
| SEEN-3 | Sparse Data ACK allegedly erased receiver reorder debt | Disproved | `S - A = O + H <= W`; no code change |
| SEEN-4 | Response settlement waited on local application delivery | Fixed | MPP-frontier trigger separated from local write; `444fb38` |
| SEEN-5 | Finite request replacement/ghost lifecycle | Disproved on current build | One fixed request, one attempt, complete body, no replacement; no code change |
| SEEN-6A | Persistent live-owner ACK-gap repair could renew critical authority | Fixed | Zero/partial/full cumulative-budget RED/GREEN; `0b50e9a` |
| SEEN-6B | HTTP/3 migration left QUIC latency priority diagnostic-only | Fixed | Actual Quinn priority RED/GREEN plus ordinary two-run non-downgrade; `a9450d8` |
| SEEN-6C | QUIC-only bulk/read-gap and loaded-latency acceptance | Model-constrained, not release-wide GREEN | Throughput is within the frozen boundary; remaining same-stream gap is proven native ordered QUIC recovery |
| SEEN-6D | TCP-only ramp/sustain and loaded latency | Partly GREEN, still release-blocking | Bulk/startup/gap improve; loaded interactive tail exposes the held all-stage service-frontier boundary |
| SEEN-6E | Mixed TCP+QUIC stability and 500 -> 10 -> 500 recovery | Open; current priority | Healthy TCP carries later offsets while an earlier QUIC-owned Product range blocks contiguous delivery |
| SEEN-7 | Retired peer rows, port projection, Evidence unknown/stale semantics, directional Suspect state | Disproved as current defects | Lifecycle/projection audit and focused tests; no code change |

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
not a justified fix. The remaining proof must decide whether an already
specified recovery authority failed to execute, or whether satisfying the
stronger latency requirement needs a new temporal frontier-race authority.

## Held UNSEEN batch

These items are recorded for later decision and are not authorized for code,
RFC, configuration, public documentation, or release changes:

- exact all-stage carrier-work and peer-service-frontier accounting;
- a temporal service guarantee for the lowest outstanding Data Sequence range
  after abrupt owner service collapse;
- multiple native ordering domains for one logical QUIC stream;
- generic scheduler score/admission terms that may not match RFC 15.1's typed
  authority model;
- repeatability and benefit of stale-path requalification reinjection; and
- acceptance coverage that the existing runner does not already provide,
  including matched cold/warm, upload, and object-size trajectories.

## Deterministic next step

1. Finish the existing SEEN-6E trace attribution without editing production.
2. If an existing RFC authority is not exercised, add one deterministic RED,
   make the smallest symmetric correction, review it independently, commit it
   alone, and rerun only the affected existing gates.
3. If implementation already matches the RFC, leave SEEN-6E visibly open and
   present the exact new model and tradeoff for consent. Do not tune a timer or
   score to manufacture a pass.
4. Do not enter the held UNSEEN batch until that consent boundary is resolved.
