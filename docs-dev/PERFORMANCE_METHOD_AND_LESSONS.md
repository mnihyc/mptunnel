# Performance method and lessons

Updated: 2026-09-08 00:27 +08:00. Category: requested global retrospective and
execution method. No runtime change, new experiment or release acceptance.
CURRENT_CLOSURE_PLAN remains the active scope/next-action ledger; this document
specifies how to execute it. Historical evidence remains in CHANGE_DISPOSITION_20260907,
PERFORMANCE_REFLECTION_20260907 and REVIEW_AND_PRACTICAL_ACCEPTANCE.

## Mandatory execution contract

The user explicitly adopted this method on 2026-09-08. Read this document and
CURRENT_CLOSURE_PLAN before each new diagnosis/fix/acceptance transaction and
after compaction. Do not modify AGENTS.md. Record the following in the active
transaction before changing runtime or running an experiment:

`issue / observed failure / competing causes / exact question / existing
evidence / falsifier / smallest next action / acceptance and stop conditions`.

Advance only when the preceding stage supplies its evidence; otherwise retain
its honest unresolved or rejected disposition. A justified deviation must be
recorded before taking it. This contract governs the workflow, not a promise
that random networks produce deterministic timings. Reuse an existing model
or capture instead of creating another document/test for the same question.

## Actual progress, not number of fixes

- Native reordered-packet/history/classification failures have real encrypted
  packet counterexamples and a jitter-only 0.585 -> 185.313 Mbps observation.
  This is a useful speed result for that case, not universal recovery proof.
- Restart, exact copy/qualification, mailbox wake, executor cooperation and
  terminal/journal ownership defects have concrete mechanisms and focused
  checks. Finite churn (1941 mixed plus 64 single-mode requests) now reclaims
  owners. The uncaptured deployed RAM/CPU incident remains not fully attributed.
- Late valid STARTUP after FINAL is now refused at attachment scope instead of
  destroying the shared carrier; runtime commit `11d6f3a` has 34 focused checks.
- Request sampling has a real pipelining RED: samples [2,2,2] despite unique
  ACK progress. The uncommitted paired-clock candidate gives [2,3,4] with 23
  focused checks and independent review. Ordinary performance is not accepted.
- Rejected static ranking, relative ACK dictionary, ready-feedback batching,
  initial uncooperative actor, absolute-delay reordering and isolated raw-byte
  hysteresis deletion are not future implementation obligations. Their reasons,
  patches and adverse observations are retained rather than silently revived.

Latest completed mixed current-snapshot down/up observations are 85.556/64.602
Mbps with 4.423/5.054-second gaps and 19.293-second upload drain. These are the
500-Mbps asymmetric changing-loss/QoS/outage profile, not clean-link ceilings.
The newer TCP-upload sampler pair ends at the existing observation guard:
control 109313995/168296448 confirmed/locally-accepted bytes in 85.895780s;
candidate 133561732/191758336 in 85.563214s. Neither upload completes. Maximum
confirmation gap worsens 1.004911 -> 2.021790s while local write gap improves
7.147240 -> 2.042942s. This is mixed/incomplete evidence, not a 22.7% speed gain
or conclusive causal regression. No QUIC/mixed candidate runs follow it yet.
Both raw result directories are under
`./.tmp/reflection/results/tcp-combined-up-request-cohort-{control,candidate}-0907/`.

## Primary objective and proof boundary

Optimize receiver-confirmed, ordered user service over time: short-object
completion, sustained useful bytes, read/confirmation gaps, loaded latency,
restart-free recovery, finite resource ownership and wire/CPU/memory cost.
Neither estimated capacity, local socket acceptance nor a selected average
is this objective. Correctness and performance are joint requirements; a
known corruption, deadlock or ownership leak is not a useful optimization.

For byte u with usable arrival A(u), ordered completion through x depends on
the latest arrival among every u<=x. A fast suffix cannot compensate for a
missing prefix. For a declared approximately constant assignment-to-ACK delay
tau, an R-bit/s pipeline needs roughly R*tau/8 bytes of authority; this is
necessary, not sufficient. 500 Mbps at 100 ms is 6.25 MB. Conversely Q bytes ahead
under constant C-bit/s nonpreemptive service require8Q/C seconds; queue scope
and location must be observed before treating this as a delay measurement.

These calculations expose incompatible requirements before code, without
claiming clairvoyant optimality under unannounced future network changes.
Finite state/work or eventual service is not a useful wall-clock bound.

## One closed-loop transaction

1. **Choose the highest-impact existing failure.** Define affected workload,
   direction, carrier set, interval and expected user-visible behaviour. Link
   it to an existing owner; a new hypothesis does not automatically add scope.
2. **Inspect origin and alternatives.** Read the introducing commit and RFC.
   State the useful original intention, violated premise, and competing
   explanations. Do not assume BBR, external QoS, platform behaviour or the
   lab is responsible from a dashboard label.
3. **Construct the smallest system model.** Follow source readiness -> exact
   assignment -> native queue/service -> receipt -> ordered delivery ->
   feedback -> next assignment. State byte domain, direction, incarnation,
   lifetime, wake owner, irreversible work and estimator feedback. Distinguish
   resource permission from placement choice and observation from capacity.
4. **Predict before implementing.** Give one reachable counterexample, a
   falsifier, expected practical benefit, cost and likely regression case.
   Check sparse work, unknown paths, shared contention, failure/recovery and
   executor service where they affect this mechanism. Formal scope must match
   the claim; a correct local equation is not a sustained allocator.
5. **Prove reachability cheaply.** Use real producer/admission/ACK paths and a
   control before changing runtime. A fixture that fails before its intended
   assertion is not Product RED. If existing captures suffice, do not rerun.
6. **Implement the smallest coherent correction.** One independently
   attributable mechanism; coupled lifecycle pieces belong in the same model.
   Prefer removing false ownership or duplicate feedback over adding knobs.
   Change the RFC only where its contract is wrong; preserve proven unrelated
   semantics. Independent review checks assumptions and counterexamples, not
   only code style or agreement with a newly written test.
7. **Verify both the mechanism and composition.** Focused regressions first,
   then affected ordinary binary comparisons with full timing and completion.
   If a pair is adverse or ambiguous, stop promotion and ask one discriminating
   causal question. Do not rerun until a good number appears. Use a planned
   paired/order-reversed repeat only when it can resolve identified noise.
8. **Assign an explicit disposition.** Disproved; model-constrained; exact
   defect fixed/composition pending; practically supported within a declared
   envelope; or rejected approach. Commit isolated checkpoints with honest
   status, never equate an intermediate commit with release acceptance.

## Necessary exploration and efficient experiment ordering

Use small mechanism discriminators first, affected end-to-end controls second,
and the combined acceptance matrix last. The final matrix still includes
single 500 Mbps and independent 200 Mbps links, shared cuts, asymmetric forward/
return conditions, changing 3--10% loss, jitter, sudden QoS and blackholes,
their ablations, both directions and all three MPP carrier modes. Include
cold/warm short requests, sustained single/concurrent work, actual Cloudflare
browser experience, restart/churn and post-load ownership.

Compare raw TCP, Xray and Hysteria2 with explicit configuration/prior differences
and the same topology/load schedule. Equal netem settings are not automatically
equal realized packet loss: confirm relevant shaping/offload/accounting facts
before public comparisons. Independent random realizations are not packet-
identical controls. Host pressure is contextual evidence, not an automatic
waiver or a reason to wait for zero load; avoid build/lab contention.

Further native-controller tuning requires an actual controller-level causal
failure. Ordered allocation, recovery-copy service, evidence freshness and
platform adapters each require their own owner evidence; do not collapse them
into one bandwidth scalar. The remaining shared/independent allocation question
does not authorize latent topology inference or a universal Internet model.

## Known failure patterns become rejection conditions

- No component-green => fluent tunnel inference; no finite-bound => latency
  non-regression inference; no unchanged memory ceiling => unchanged queuing.
- No throughput-only, success-only latency, hidden failed transfers or averaging
  away stalls. Preserve warm/cold and phase histories. Cached estimate, pacing,
  ACK throughput and application delivery are separately labelled.
- No parameter/profile changes to make a candidate pass. Observation guards
  classify censoring, not product failure. Current incomplete upload probes
  omit confirmation bins; do not reconstruct them from unrelated I/O counters.
  If exact bins are essential to the next causal question, preserve existing
  observations with a minimal collection correction, not a new harness project.
- No guessed native capacity, protocol preference, lowering resource limits
  to force aggregation, permanent exploration starvation or renewable waits.
- No proof of new ordering without half-close/cancel/finalization boundaries;
  no actor-fairness claim without executor cooperation and finite handler work.
- No untested candidate stack: retain a known comparator and attributable
  source/build identities. Intermediate correctness fixes may be necessary but
  do not bypass the unchanged final timing/experience gate.
- No reopening disproved or rejected approaches without new contrary evidence;
  no unused theoretical framework as a compulsory release prerequisite.
- No large diagnostic stream unless it distinguishes current hypotheses;
  diagnostics are for causality, ordinary builds for performance. One build
  per coherent test batch, minute-scale polling and independent read-only work
  in parallel; preserve small evidence artifacts before scoped cache cleanup.

## Next decision, not another architecture expansion

Keep the sampler's proven mechanism separate from its failed ordinary acceptance.
Use the existing pair to decide the smallest remaining evidence requirement:
which exact owner holds the undelivered prefix and why it cannot progress after
service recovery. Observed native ACK progress plus large socket queues is not
an exact attribution of a two-second application gap. Do not add congestion
gains or silently promote the sampler on partial confirmed-byte totals.

Then close the existing allocation/recovery contract and affected controls
before expanding comparisons. Documentation is a decision ledger, not a
substitute deliverable. The method aims for useful conditional guarantees and
robust measured performance, not a proof of universal optimum. The known
process errors above must block acceptance even when the result looks fast.
