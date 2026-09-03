# Recent SEEN change reflection

Status: internal design and acceptance record. This is not a release claim.

## Why earlier milestones were premature

The recent corrections were not all wrong, but their acceptance labels were.
Several component-level RED/GREEN proofs were reported as if they closed the
cross-layer performance symptom. They did not exercise the complete timing
surface: cold and warm start, short objects, sustained single-stream work,
concurrent download/upload, impairment recovery, and application-visible
gaps in all three carrier variants. Random aggregate goodput also hid long
ordered-delivery stalls.

The corrected rule is deterministic:

1. Freeze the already-SEEN symptom and its acceptance cell.
2. Capture an exact causal trace.
3. State the invariant and symbolic counterexample before production code.
4. Demonstrate the exact pre-change RED without sleeps or random timing.
5. Apply one causal correction and obtain focused GREEN.
6. Obtain independent bounded review.
7. Run the affected ordinary-build runtime cell. Diagnostic builds establish
   causality only; they do not establish performance.

A component commit is a milestone, not acceptance. A randomized runtime cell
needs repeated independent realizations and application-visible timing, but
that statistical gate must not be used to invent or tune the model.

## Commit verdicts

| Commit | Intended correction | Evidence boundary | Verdict |
| --- | --- | --- | --- |
| `38286aa` | Stop response acquisition state from overriding ordinary ECF placement. | Exact service RED/GREEN; full runtime matrix remains open. | Retain; matrix-pending. |
| `a4679b5` | Keep a configured TCP startup-rate hint bound to the exact response output rather than replacing it with untyped telemetry. | Correctness RED/GREEN is strong. With the hint omitted, the 350.75 Kbit/s discovery prior remains until qualified Product evidence, so cold/warm TCP performance still needs explicit scrutiny. | Retain as the RFC correction; performance-pending. Redesign the typed native-evidence adapter if the runtime gate fails rather than restoring untyped telemetry. |
| `842a0cc` | Apply the same ordinary ECF rule to request OriginalData and remove acquisition-order arbitration. | Direction-symmetric RED/GREEN; runtime composition remains open. | Retain; matrix-pending. |
| `444fb38` | Publish MPP ACK/startup control after Product acceptance without waiting for a blocked local application write. | Exact blocked-sink RED/GREEN; it changes no scheduler or congestion control. | Retain; matrix-pending. |
| `0b50e9a` | Bound renewable live-owner repair by cumulative optional authority and stop a proven repair flood. | Anti-amplification core is correct, but deleting the exhausted-credit frontier floor introduced non-monotonic liveness. | Retain only with its bounded liveness successor; never accept this commit alone. |
| `a9450d8` | Apply the selected latency priority to the actual Quinn stream. | Exact native-priority RED/GREEN and two ordinary non-downgrade runs. | Retain; accepted component. |
| `53d9ab5` | Restore one bounded live-owner frontier opportunity and preserve its exact owner, target, epoch, and wake lifecycle. | Component proofs are real, but three ordinary mixed-recovery runs remained unstable. The 28-file commit combines several mechanisms in one causal family, which weakens attribution. | Retain as an intermediary checkpoint only; runtime acceptance is RED. |

There is no evidence for a blanket revert. `a4679b5` is the strongest bounded
candidate for response-side TCP startup underestimation when no initial-rate
hint is configured. `0b50e9a` plus `53d9ab5` is a correctness composition, not
a sustained-performance solution. None of these changes directly explains a
native QUIC congestion-rate collapse.

## Current SEEN-6E proof boundary

For the captured blocking range `[900003768, 900069304)`, the original QUIC
placement was not caused by stale evidence: its predicted completion was
887.996 ms versus 891.256 ms for the best TCP candidate. The authoritative
gap became visible 1.318 s after assignment. At that observation:

```text
owner QUIC completion     1378.287 ms
best TCP completion        781.328 ms
owner fallback remaining    about 89 ms
```

The current early-repair rule compares alternate delivery with the time at
which fallback authority fires:

```text
max(now, loss_at) + S_alt < fallback_at
```

This compares delivery with a timer, not with owner delivery. The bounded
advisory race for frontier `M` is:

```text
max(now, loss_at) + S_alt(M) < now + S_owner(0), with M already in D_owner
```

The loss and fallback epochs remain evidence/liveness hints; they are not rate
caps or path-capacity limits. Before fallback, a measured completion advantage
can advance optional repair. At or after fallback, the existing bounded
liveness authority remains available even without that advantage. If either
comparable projection is absent, the fallback hint remains the conservative
evaluation point.

The owner's exact OriginalData debt already contains `M`; charging `M` again
would count the same work twice. Portable native telemetry can leave later
owner offsets in `D_owner`, so `S_owner(0)` is a conservative total-outstanding
projection rather than an exact byte-position oracle. It may authorize bounded
optional repair early; it cannot cap ordinary work or prove native failure.

For the trace above, the corrected comparison can remove only the remaining
approximately 89 ms wait. It cannot remove the preceding 1.318 s evidence
delay or guarantee that a copy already admitted behind native ordered work
will overtake it. Once all existing native ordering domains contain the same
missing range, stronger service guarantees require a separately approved
architecture change; they are not a timing-threshold tweak.

The immediate-prior-path hypothesis is disproved for this trace. That TCP path
had 488,534 queued bytes versus one 65,536-byte quantum, so existing queue
hysteresis would still have selected QUIC. Replacing the exact lower owner
with last-selected stickiness would be an UNSEEN scheduler policy with no
dominance proof and is excluded from this transaction.
