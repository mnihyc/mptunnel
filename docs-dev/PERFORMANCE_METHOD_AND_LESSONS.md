# Performance method and lessons

Updated: 2026-09-08 19:30 +08:00. Category: requested global retrospective and
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
evidence / benefit or information forecast / falsifier / smallest next action /
acceptance and stop conditions / actual outcome versus forecast`.

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
  ACK progress. The shelved paired-clock candidate gives [2,3,4] with 23
  focused checks and independent review. Ordinary performance is not accepted;
  its frozen executable/patch are not an implicit dependency of later fixes.
- Exact retained recovery beyond an older negative ACK horizon and unique-byte
  attribution beside partial copies have live counterexamples and focused
  checks. Their adverse/incomplete ordinary comparisons remain explicit in
  CURRENT_CLOSURE_PLAN; neither component GREEN proves practical improvement.
- Rejected static ranking, relative ACK dictionary, ready-feedback batching,
  initial uncooperative actor, absolute-delay reordering and isolated raw-byte
  hysteresis deletion are not future implementation obligations. Their reasons,
  patches and adverse observations are retained rather than silently revived.

Earlier late-STARTUP mixed down/up observations were 85.556/64.602
Mbps with 4.423/5.054-second gaps and 19.293-second upload drain. These are the
500-Mbps asymmetric changing-loss/QoS/outage profile, not clean-link ceilings.
The subsequent TCP-upload sampler pair ended at the existing observation guard:
control 109313995/168296448 confirmed/locally-accepted bytes in 85.895780s;
candidate 133561732/191758336 in 85.563214s. Neither upload completes. Maximum
confirmation gap worsens 1.004911 -> 2.021790s while local write gap improves
7.147240 -> 2.042942s. This is mixed/incomplete evidence, not a 22.7% speed gain
or conclusive causal regression. No QUIC/mixed sampler acceptance followed it.
Both raw result directories are under
`./.tmp/reflection/results/tcp-combined-up-request-cohort-{control,candidate}-0907/`.

## Primary objective and proof boundary

User clarification2026-09-08: the severe combined impairment where all
baselines perform poorly is a **diagnostic stress case**, not the final
performance environment or a throughput target. Crossing100Mbps there has
no special value. Preserve its stalls/failures for attribution, but do not
optimize its aggregate alone or publish it as general competitiveness.
Final evidence must include serviceable high-capacity links, healthy/restored
phases, heterogeneous mixed paths and independent aggregation with matched
baselines and timing/experience. Do not erase the harsh case; give each case
its declared purpose before testing.

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

### Mandatory expectation management before experiments and fixes

The user explicitly required this on 2026-09-08. Before implementing or
benchmarking a proposed correction, record a **benefit forecast** in the active
transaction. This is a prediction to test, not a promised improvement:

- **Observed impact:** identify the exact workload, direction, phase and
  user-visible loss: stalled seconds, incomplete bytes, recovery time, sustained
  goodput deficit, or retained resource growth. Include competing physical and
  baseline explanations. A slow losing copy is not the critical user interval.
- **Removable portion:** identify what this mechanism actually controls and
  what remains outside it. Estimate a plausible gain range and an upper bound
  when supported. State the assumptions and confidence basis; do not invent
  precise confidence percentages or convert operation counts into Mbps. Include
  no gain or regression when plausible. If magnitude is unknown, say so and
  choose a cheap discriminator or defer the change; do not invent a forecast
  to justify implementing it.
- **Composition and cost:** explain how the local change reaches ordered user
  delivery, first response, or recovery. Include wire amplification, CPU/RSS,
  loaded latency, sparse work and competing-path risks. Faster service at one
  stage need not improve a critical path dominated by another stage.
- **Value decision:** explain why the expected practical gain is material
  enough to pursue now. Defer small or unsupported benefits behind demonstrated
  throughput collapse, multi-second stalls, failed recovery and resource leaks.
  Evidence of a necessary correctness fix is a separate justification; do not
  market it as a speed improvement.
- **Falsifier and disposition:** predeclare what would invalidate the forecast,
  the smallest useful comparison, and the affected non-regression checks.
  Afterward compare forecast with actual full timing/completion/cost results.
  Record why the forecast held or failed, including a wrong cause, a new
  bottleneck, or unresolved measurement noise. An absent material benefit stops
  performance promotion and prompts attribution review, not favorable reruns
  or another compensating parameter.

For fixed work taking T seconds, removing an exclusively serial critical delay
d (0 <= d < T) can reduce elapsed time by at most d; the idealized goodput
gain is T/(T-d)-1. This bound requires that the delay is truly on the critical
path and creates no replacement bottleneck. Concurrent/nested elapsed totals
cannot be summed as d. For a fraction f of baseline CPU-limited critical
execution improved by factor k (0 <= f <= 1, k >= 1), the analogous Amdahl
speedup factor is 1/((1-f)+f/k); fractional gain is that factor minus one.
An arbitrary share of aggregate multicore CPU is not f. Neither equation
predicts performance from an aggregate timer.

A diagnostic experiment has an **information forecast**, not an expected speed
gain: name the competing explanations it can separate, the next decision each
outcome enables, and why existing evidence is insufficient. Do not call an
observer a performance fix or add more observation if it cannot change a
material decision.

Concrete lesson from the current native-read capture: 553 completed pending
episodes have at most 5.457ms between validated head availability and Chunk
return (p95 440us). These measurements do not justify optimizing that boundary
to solve an individual multi-second input wait. This is a per-episode bound,
not a bound on cumulative critical service or all local service; it does not
prove a transport bug.
Any next sender/recovery proposal needs its own forecast and causal evidence.

### Classify the performance shortfall before calling it a defect

Impact priority (user reinforcement, 2026-09-08): focus on sustained throughput
collapse, multi-second stalls, slow failure/recovery, failed completion and
unbounded ownership/resource growth. A change worth roughly 1Mbps or 10ms on
these high-capacity variable links is not a priority absent a larger demonstrated
consequence. These are impact examples, not new Product limits or acceptance
thresholds. Defer operation-count/micro-optimization fixes unless the measured
critical interval shows why they can materially improve the user failure.

The user reinforced this boundary on 2026-09-08: an unideal timing result
may be physical loss/queuing, a deliberate throughput/latency tradeoff, or an
implementation/model defect. Failure of the practical acceptance gate alone
does not choose among them. A local operation-count RED proves avoidable work,
not that removing it addresses the dominant user-visible delay.

Compare against matched raw TCP, VMess, Hysteria2 and, where available, MPTCP
before claiming that a delay is MPP-specific. Preserve configuration priors,
direction, shared/independent cuts, offload/accounting, offered and completed
work, timing distribution, wire traffic and resource pressure. A low-throughput
baseline with a small queue is not an equal-speed latency control. Historical
or censored baseline results are context, not a current winning comparison.

Q/C is drain time for a known queue under constant service. Without the exact
blocking byte's queue position, it is not a measured per-byte delay bound.
Moreover, a queue that is unavoidable to drain now may have been avoidable
before earlier sending decisions. Do not label all queued delay unavoidable,
or tune the lab queue away to make a candidate look healthy. Separate known
physical limits, choices on the speed/latency frontier, and a concrete causal
defect. If evidence supports only a physical limit or tradeoff, document it
without adding a model correction. Universal instantaneous optimality cannot
be proved by finite experiments; do not silently replace or waive the declared
practical acceptance gates.

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
   Preserve evidence type: a positive receipt, a receiver's authoritative
   omission, and a sender's exact retained ownership are different facts.
   One narrower evidence view must not silently gate every valid recovery path.
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
- Do not mistake preservation of an old event trace for a required authority
  boundary. The final-only MAX fold retained ACK barriers even though RFC8.4
  makes credit one shared monotonic maximum independent of byte receipt. It
  improved healthy mixed throughput but left long confirmation gaps and
  ACK-separated superseded credit work. The replacement therefore tests one
  logical state owner, not progressively wider event batching. A proposed
  per-carrier terminal watermark was withdrawn because no authority violation
  justified it; logical RESET/cancellation still closes the owner. This removes
  an unsupported dependency, not permission to reorder ACK/Data evidence or
  predict practical speed from the max algebra alone.
- More validation is not automatically safer service. The prepared-source
  candidate froze every writer's ready/occupied generation. An unselected
  loser withdrawing then invalidated the unchanged winner; two real retries
  made zero progress while the ordinary control claimed64KiB. Separate sampled
  scheduling opportunities from exact chosen ownership, refresh the former
  and fence the latter. Preserve opposite-case controls: selected withdrawal
  must refuse, and a newly observed eligible Regular must displace Backup.
  A deadlock-free lock graph alone did not establish claimant progress.
- Observe progress using its producer timestamp, not the consumer's later
  scheduling time. The actual claim/observer counterexample renewed a fallback
  anchor despite unchanged service. Same-event observation must not renew a
  deadline or overwrite newer ACK/control progress. This is a clock-domain
  correction, not authority to shorten timeouts.
- Reusing a final-only helper in an active caller is not automatically neutral.
  Compare its byte quantum, whole-frame versus positive-credit admission,
  bound versus unbound publication, and assignment-clock lifetime first. The
  response migration passed82 component checks before review exposed a64KiB
  to14600B shrink and a fresh-append renewable deadline. A passing small-frame
  fixture did not cover either countercase; keep those boundary discriminators
  instead of treating helper reuse or a test count as equivalence evidence.
- After replacing an algorithm's caller, verify that its operation-count tests
  still exercise the production path. Request structural dispatch passed485
  checks while614dc73's once-per-batch overlap test covered a now-unused helper;
  the new caller repeated the old per-frame scan. History review of the unused
  helper caught it before the lab. An actual direct-path counter then failed
  after semantic controls passed. Preserve the demonstrated work property in
  the replacement, and remove obsolete helpers/tests only after that migration.
- No untested candidate stack: retain a known comparator and attributable
   source/build identities. Intermediate correctness fixes may be necessary but
   do not bypass the unchanged final timing/experience gate.
- User-facing milestones must contain demonstrated practical outcomes, not
  merely component GREEN followed by a promise of future verification. The
  user explicitly reinforced this on2026-09-08:9720e4b improved early target
  delivery4.86→142.11MB at10s but then failed settlement with a75s return hold.
  Report that failed practical result plainly. Component/test checkpoints
  remain useful internal tracking; they are not successful speed milestones.
- No reopening disproved or rejected approaches without new contrary evidence;
  no unused theoretical framework as a compulsory release prerequisite.
- No large diagnostic stream unless it distinguishes current hypotheses;
  diagnostics are for causality, ordinary builds for performance. One build
  per coherent test batch, minute-scale polling and independent read-only work
  in parallel; preserve small evidence artifacts before scoped cache cleanup.
- Start a new delegated assignment with followup_task, which also activates an
  idle agent; send_message only updates a running assignment. Verify task
  state at ownership transitions. A queued message to a completed agent is
  not work in progress; this caused an avoidable preparation pause on09-08.

## Next decision, not another architecture expansion

CURRENT_CLOSURE_PLAN is the sole current transaction/next-action ledger; do not
repeat an earlier experiment simply because this retrospective mentions it.
Keep the sampler's proven mechanism separate from withheld ordinary acceptance.
The first use of this workflow preserved its incomplete pair, ruled out an
apparent native ACK freeze using live sockets, then used one existing-event
capture to expose the request recovery horizon/positive-frontier mismatch.
The accompanying REQUEST_COHORT_ORDINARY_20260907 artifact records this chain,
the exact 4.176371s stall, remaining attribution limits and independent audits.
It does not prove that a newly eligible repair will improve user timing.
Do not add congestion gains or promote a candidate on partial byte totals.

Then close the existing allocation/recovery contract and affected controls
before expanding comparisons. Documentation is a decision ledger, not a
substitute deliverable. The method aims for useful conditional guarantees and
robust measured performance, not a proof of universal optimum. The known
process errors above must block acceptance even when the result looks fast.
