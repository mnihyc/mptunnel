# Placement-wait liveness: bounded verdict

Date: 2026-09-07 UTC. Category: pre-implementation model review.
No runtime, timer, gain, budget or configuration change is approved here.

## What the existing deadline can prove

Let `h` identify one uncommitted source head, `o` its exact currently live
lower-OriginalData owner, and `D_o` that owner's already-armed absolute
recovery deadline. The smallest defensible wait contract is:

1. Defer only this fresh placement decision, not the actor, ACK/FIN handling,
   startup settlement, repair, requalification or other runnable streams.
   Retain no speculative writer reservation or Product credit.
2. Name an exact live preferred output and its actually blocked resource.
   Arm its capacity/lifecycle/evidence notifications, recheck, then park.
   A score change alone is not a progress event.
3. Capture the existing deadline once for this head, bounded further only by
   an already-defined evidence expiry. Owner completion, invalidation,
   replacement or loss ends the wait; a rate update or new frontier owner
   cannot renew it. If no suitable finite owned boundary exists, do not wait.
4. On expiry, stop advisory deferral of this head. Use current valid immediate
   admission, or genuine resource blocking if no operation can commit.
   Expiry does not grant credit, guarantee native service or authorize a copy.

Conditional on fair actor service and an alternative whose exact resource
transaction succeeds when visited, this gives finite-head progress. Existing
recovery and capacity wakes suffice for that limited claim; no additional
numeric timer is needed. The deadline is a reevaluation boundary, not a
promise that preferred-path capacity returns by then.

A useful completion advantage remains conditional on compatible forecast
support. An exact receipt observation is not future capacity. See
[completion-evidence obligation](COMPLETION_EVIDENCE_NEXT_OBLIGATION_20260907.md)
and [rate-scope audit](RESPONSE_PLACEMENT_RATE_SCOPE_AUDIT_20260907.md).

## Realizable counterexamples: three TCP carriers and one QUIC carrier

These are schedules permitted by the current ownership/readiness model, not
new lab measurements. Carrier labels can be exchanged without changing them.

| Schedule | Result |
| --- | --- |
| QUIC owns the lower prefix and has a temporarily full writer; TCP1 is ready with comparable slower observed service. QUIC becomes writable before the captured deadline and genuinely completes the head earlier. | A bounded wait can improve ordered completion for this supported case. It is not inherently vacuous. |
| For every new head, QUIC becomes writable after some positive delay smaller than that head's remaining deadline. Each head then commits to QUIC. TCP1 remains known/slower; TCP2 and TCP3 remain idle, active, unknown and admitting. | Every head progresses and every wait expires or wakes correctly, yet TCP2/3 receive no OriginalData forever. One may actually offer independent useful capacity. Mere eligibility and frequent wakes do not provide discovery. |
| QUIC loses service after deferral. Its existing deadline becomes due while TCP1 remains admitting. | A nonrenewable wait releases the head to current immediate admission. Renewing against a successor owner/deadline would lose this proof. Queued native work may still delay delivery; the wait contract cannot cancel it. |
| An active unknown TCP output is never chosen; its writer is already empty and its positive initial Product envelope remains unused. | It need not generate another capacity edge, qualification ACK or recovery deadline. Event-driven eligibility cannot manufacture the first informative transmission. |

If *any* relevant ready unknown alternative vetoes Defer across the complete
candidate set, the second schedule cannot starve it *because of a new wait*.
That only preserves whatever opportunity current immediate placement already
provides. It does not prove that placement selects every unknown output.
Restricting the veto to the two selected comparable candidates silently loses
this protection.

Consequently fresh-comparable-only Defer may help an all-supported subset,
but leaves the demonstrated unknown/portable-prior branch unresolved. It is
not an implementation-ready fix for that observed case.

## Can existing startup or requalification own discovery?

No, not under their present contracts:

- Startup opens each declared unresolved attachment once and freezes exact
  membership. It expressly does **not** promise OriginalData on every retained
  attachment; FINAL restores ordinary placement. Reopening this one-shot plan
  or treating admission as a measured trial would undo that separation.
- Product qualification begins only after the first exact OriginalData commit.
  A positive unqualified envelope permits that commit; it does not schedule it.
  Waiting for qualification to arrange its own first sample is circular.
- Requalification's existing cyclic cursor fairly visits *stale* attachments,
  conditional on retained bytes and visit-admitting resources. Its non-owning
  copy receipt restores `Active(q=0)`, not Product qualification, capacity or
  an OriginalData allocation opportunity. Idle active unknown siblings are
  outside this ring. Marking them stale or treating copied proof as unique
  Product evidence would change already-proven authority rules.

These boundaries are explicit in [startup readiness](RESPONSE_STARTUP_READINESS_MODEL.md)
and RFC sections 8.1, 10.2, 15.1 and the stale-requalification transaction.
Preserve maintenance opportunities while waiting; do not commandeer their
identities or call them sustained discovery.

## Minimal missing obligation and decision

Stronger discovery needs an opportunity obligation that survives source-head
turnover: under continuing demand, finite stable membership, settling trials
and fair exact admission, a continuously eligible unknown output cannot be
bypassed indefinitely. This is qualitative attempt fairness, not a new proposed
numeric budget and not a wall-clock/full-capacity guarantee.

None of the existing owners above supplies that obligation for ordinary
OriginalData. Reusing their storage does not prove semantic ownership.
A history-free repeated preference cannot distinguish an untested fast path
from an identically observed slow one. Trying the latter can itself add ordered
latency; therefore universal cost-free discovery and guaranteed use of hidden
capacity cannot both be inferred from those observations.

**Verdict:** existing deadlines/events are sufficient for conditional
per-head wait termination, insufficient for cross-head discovery. Do not
implement Defer as the current unknown-path correction, add an arbitrary
budget, restore static best-path selection, or infer protocol preference.
A narrower supported-evidence wait is separately possible, but does not close
the demonstrated issue and is not approved by this review.
