# Bounded restart, retention and rate correction plan

Execution update2026-09-05T19:38Z: this original plan is historical.
`CURRENT_BATCH_CLOSURE.md` records the committed corrections, tests and
remaining open limitations. N1's persistent-restoration inference was narrowed
by actual dequeue evidence; no failed controller experiment was accepted.

Date: 2026-09-05T17:04Z. Base: `6636091` (local accepted corrections atop
v0.4.8). Scope: the currently reported server restart, RAM/CPU and QUIC rate
incidents, including directional QoS. This is a plan, not release acceptance.
No additional production changes have been made during this diagnosis turn.

## Evidence and transaction boundaries

| ID | Confirmed defect | Bounded correction | Required proof |
| --- | --- | --- | --- |
| R1 | The real relay consumes a stream-terminal reset while a Product write is blocked, but defers terminal settlement behind that write. The same nested ingress statically discards stale-generation terminal errors. | Classify authenticated stream-terminal outcomes before successful-open deferral or task-generation filtering at every additional-open ingress. Preserve successful ordinary-open ordering and transient retries. | Reset terminates without unblocking the sink, including obsolete task generations; success still waits for correct ordering; TCP/QUIC and fresh-sibling controls remain green. |
| M1 | Native retained-loss evidence expires while the owning controller is parked; migration rollback restores its stale undo transaction. Both same-IP and new-network variants reproduce. | Apply terminal ownership events to each extant owning controller copy using exact epoch/transaction identity. Cover expiry and late-ACK completion; do not feed ordinary ACK/RTT evidence to the wrong path. | Expiry/late ACK before rollback cannot resurrect undo authority; partial transactions remain eligible until their last evidence disappears; the existing journal compaction and valid-spurious-undo controls pass. |
| R2 | After server state loss, a previously unbound candidate can send STARTUP first and recreate target state for an already accepted logical stream. Both TCP and QUIC reproduce. | Separate initial target creation from later startup enrollment explicitly. Only creation may allocate absent state; later enrollment/ordinary recovery requires retained ownership or returns a stream-terminal result. | Genuine initial creation on any ranked ordinal succeeds; late enrollment after state loss never reconnects a target at old offsets; retained-state enrollment and frozen plan/FINAL semantics survive. This requires a deliberate wire/RFC decision, not an ordinal heuristic. |
| D1 | Capacity-model estimates are presented under a delivery-rate name; Quality normalizes those estimates rather than measuring traffic share. Legacy pacing projection can raise literal native pacing. | Keep native capacity, literal pacing, interval native delivery and unique Product goodput distinct. Calculate traffic share only from comparable directional byte deltas; show metric direction explicitly and retain honest freshness/unavailability. | The reproduced 395-model/9.5-delivery case displays both accurately; upload/download cannot be swapped; unchanged native scheduling authority; unavailable is not zero; stale values cannot gain freshness through polling. |
| N1 | Real asymmetric MPP reproduces approximately 395 Mbit/s native bandwidth while a 10-Mbit/s shaper delivers approximately 9.5 Mbit/s. Native component tracing also reproduces retained high bandwidth, queued RTT growth and ProbeRTT state interaction. | Correct the identified native probe/drain evidence interaction only after documenting its invariant and attribution. A probe must not bootstrap a larger flight allowance from its own queued RTT or indefinitely protect a disproven rate. Do not change loss allowance, introduce an application-goodput clamp or add a second MPP congestion controller. | Fixed directional high→low→high service, with dequeue service verified, adapts without restart or growing latency; repeat with ACK-direction restriction separately. Retain erasure, idle/app-limited, reordering/fast-tail and genuine-loss recovery controls. |

## Execution order

1. R1 and M1 are independently actionable local corrections with existing
   counterexamples. Make each a separate tested commit; neither depends on
   native rate tuning or changing the wire format.
2. Resolve N1's exact native model invariant and R2's creation/enrollment
   wire semantics as separate documented decisions. Do not bundle them or
   label a diagnostic as a fix. The current N1 diagnosis does not authorize
   blanket rollback of accepted BBR corrections.
3. D1 is an independent telemetry/UI correction. It must not hide N1 by
   simply replacing a high capacity number with low application goodput.
4. Run only the listed affected controls and the bounded end-to-end cases.
   Record each failure's cause. Commit accepted fixes separately; do not
   release or claim all-green while any of these reproduced defects remains.

## Asymmetric scope

The first executed case restricts S→C service only, with a fast C→S ACK path
and unequal one-way propagation delay. It already answers the request to
stop relying solely on symmetric links. The complementary case restricts
the return/ACK path only. A third, combined case is warranted only if those
two reveal a concrete interaction. These are discrimination tests, not a
new broad benchmark campaign. They use only owned local endpoints.

## Why these changes are justified, and what must not return

- R1 descended from legitimate startup progress during a blocked Product
  write (`444fb38`); terminal authority is stronger than successful-open
  ordering. Do not remove the progress mechanism.
- R2 comes from v10's creation/enrollment conflation (`3a6d0ea`). Do not
  reinterpret ordinary recovery as creation, reuse lost target offsets, or
  reject valid first-path ordinals.
- M1 is the active-only terminal-notification boundary inherited from the
  BBR3 integration (`d5a7413`) and migration composition. The accepted
  bounded journal collector is not the cause and should remain intact.
- D1 descends from an intentionally single native scheduling authority and
  stale-evidence retention. Those purposes remain valid; telemetry must
  expose their meaning rather than manufacture current measured delivery.
- N1 combines a retained bandwidth maximum with a bandwidth/RTT-derived
  drain budget and protected probe samples. Those mechanisms individually
  preserve useful capacity during intentional low sending. Their composed
  behavior must still adapt when service really falls. The report records
  the executed trace and the limits of upstream attribution.

The specific random deployed RAM/CPU event cannot be conclusively identified
without incident measurements. The original journal retention defect and
its remaining ownership branch are concrete, but they do not prove every
future RSS increase is the same leak. Queued payload memory in N1 is another
resource owner and must not be relabeled as journal retention. The 64-MiB
journal bound is not a bound on the entire process.

Detailed evidence: `SERVER_RESTART_STREAM_STATE_DIAGNOSIS.md`,
`QUIC_LOSS_JOURNAL_RETENTION_DIAGNOSIS.md`, and
`QUIC_RATE_DISPLAY_DIAGNOSIS.md` in this directory. No unrelated optimization
or historical backlog item is silently declared resolved by this plan.
