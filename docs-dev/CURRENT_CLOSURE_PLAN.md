# Current deterministic closure plan

Updated: 2026-09-08 00:55 +08:00. Authoritative source is `./`. **No release pass.**
This is the existing REVIEW_AND_PRACTICAL_ACCEPTANCE batch, not a new inventory.
Superseded checkpoints are preserved in
[the history](CLOSURE_PLAN_HISTORY_THROUGH_20260907.md) and their linked evidence.
Execution and known-failure rejection rules are in
[the method](PERFORMANCE_METHOD_AND_LESSONS.md).
The user adopted that method as mandatory on 2026-09-08. Read it before each
transaction and after compaction; the latest explicit transaction is below.

## Current result and exact next transaction

Current runtime: `11d6f3a`, frozen ordinary executable
`./.tmp/reflection/bin/late-startup-scope-20260907/mptunnel`.
Its latest combined mixed download is 85.556 Mbps with a 4.423-second QoS gap
and one actual echo timeout. Whole-profile mirrored upload confirms every byte
at 64.602 Mbps, but has a 5.054-second confirmation gap and 19.293-second
post-load drain. Complete series and limitations are in
LATE_STARTUP_SCOPE_ORDINARY_20260907. These snapshots are not a causal A/B of
the rare attachment refusal branch and are not fluent-experience acceptance.

**Active transaction: request/upload rate refresh.** The existing sampler
requires each cohort's earliest assignment to follow the previous ACK and
advances that boundary even on rejection. Ordinary pipelining can therefore
prevent every refresh after the first. The staged predicate predates its
unconditional use introduced by `f4206d0`; no live round barrier invalidates
the counterexample. This is not attribution of the server-download stall.

1. **RED complete:** actual admission/bootstrap/ACK release gives pipelined
   samples [2,2,2], staged [2,3,4], both zero final Product debt and idempotent
   ACK replay. The earlier proof-command fixture failure is kept separate in
   REQUEST_PIPELINING_OWNER_RED_20260907. No proof flags were injected.
2. **Candidate GREEN:** fixed entry floor and paired chronological cohort
   clocks give [2,3,4] in both live cases. All 23 distinct focused checks pass;
   independent source/RFC review passes. Exact qualification, maturity,
   expiry, copy and native ownership are unchanged. Justification and tradeoff
   are REQUEST_COHORT_CLOCK_CORRECTION_20260907. Runtime remains uncommitted.
3. **Ordinary gate incomplete/adverse:** application build completed in 1m39s;
   frozen candidate is `./.tmp/reflection/bin/request-cohort-20260907/mptunnel`.
   Both TCP-upload mirrored-profile cells reached the existing 85s observation
   guard before completion. Control 109.31/168.30 MB confirmed/locally accepted;
   candidate 133.56/191.76 MB. Confirmation gap 1.005->2.022s, local write gap
   7.147->2.043s. No complete-rate or causal speed-win claim. No third run.
   Censored probes omit confirmation bins; full management/router series and
   raw probes remain in the two request-cohort result directories.
4. **Next:** exact queued/ordered-work attribution using existing evidence;
   no automatic QUIC/mixed expansion or sampler promotion. Actual early
   Product sample refresh improves, but native sockets still hold substantial
   work while native ACKs progress. That is not yet the exact gap cause. No
   generic Defer, discovery allowance, response migration or timer tuning.

### Active transaction: incomplete TCP upload drain

- **Observed failure:** baseline and sampler candidate both retain unconfirmed
  upload work after the existing 85s observation window. Candidate numerical
  refresh is proven; practical timing is not accepted.
- **Competing causes:** native queued work/actual slow transport service;
  MPP assignment into a slow ordering domain; Product feedback/actor blockage;
  target or confirmation-path backpressure. Observed socket ACK progress alone
  does not identify the application gap or eliminate those alternatives.
- **Question:** where is the already-accepted but not target-confirmed prefix
  waiting, and what observation would distinguish native service from an MPP
  progress/placement defect? No new estimator/controller hypothesis is assumed.
- **Existing evidence:** both raw probes plus 86-sample management/router/socket
  series. Exact per-confirmation bins are absent after censoring; do not invent
  them. Inspect existing evidence and producer semantics first.
- **Falsifier:** sustained exact source/target progress on either side of a
  supposed blocked stage refutes that stage's total-stop explanation; receipt
  counters without matching byte/instance scope cannot prove it.
- **Smallest next action:** preserve the interrupted pair artifact, correlate
  existing stage/instance counters, then state the remaining observation gap.
  No new lab or runtime correction until that question is explicit.
- **Stop/promotion:** no complete-rate claim or sampler acceptance from partial
  totals. If existing capture cannot identify an exact gap, report that limit
  and choose one narrow discriminator; do not launch all variants or tune a
  threshold. An independent read-only auditor checks this attribution.

**Evidence stage complete, 2026-09-08 00:31 +08:00:** the raw pair is archived in
REQUEST_COHORT_ORDINARY_20260907. Independent read-only audit confirms real
native queues, stale native publication despite live socket ACK progress, and
a one-second sampled target-write plateau with one native carrier stalled
while siblings advance. Exact logical-range ownership remains missing.

**Next discriminator, recorded before running:** use one unchanged TCP-upload
mirrored-profile capture with existing candidate executable and existing event
filter only. Question: does the first persistent server receive hole cover
already-published request work, which stable path index owns it, and was a
repair published before the hole clears? Correlate `sender_service_decision`,
`server_receive_hole`, `server_receive_delivery_stall`, `stream_ack_received`,
`tcp_sender_metrics` and `client_sender_enqueue`, retaining path error/stale
events and live socket snapshots. Event strings are present in the frozen
candidate; no build or new instrumentation is required. No source/profile or
probe changes. The same observation guard remains censoring, not a test pass.

The falsifier is an unassigned prefix (source/dispatch hold), or a prefix
already receipted while ordered service remains stopped (a later-stage hold).
Stable path-instance mapping must be checked; a replaced index cannot identify
the old owner. These events do not identify which original/copy closes the
hole or fully divide native, server routing and actor delay. Stop at that
evidence boundary if necessary; do not add another trace family automatically.
Synchronous event logging makes this a causal capture, not ordinary throughput
or sampler promotion evidence.

**Discriminator complete, 2026-09-08 00:42 +08:00:** one existing-binary trace
reproduces a 4.176371s ordered upload stall at frontier 21,364,471. Its original
64 KiB range was published 10.817s before release, with no covering repair.
Suffix assignment and ACK release continue. All complete ACKs have greatest
end at most 11,009,783; the retained negative horizon is therefore exactly that
value. Partial ACKs advance the actual mux frontier beyond it. ACK-gap recovery
cannot see the newer hole, while the retained-tail entry guard requires the
old snapshot's contiguous prefix to equal the current mux frontier and rejects
before owner age or alternate admission. Independent source/trace audits agree.
This is a recovery-evidence composition defect, not a native-loss declaration
or proof that a copy would necessarily improve the measured timing.

### Active transaction: retained recovery beyond complete-ACK horizon

- **Observed failure/question:** exact retained OriginalData above an older
  complete-ACK horizon loses both gap and tail recovery eligibility during
  supported partial-positive feedback. Why should negative evidence constrain
  a separately valid retained-owner fallback?
- **Model:** H is the negative horizon, F the actual positively acknowledged
  mux frontier. Complete `[0,1)` then partial `[1,2)` and `[3,4)` yields H=1,
  F=2, exact retained `[2,3)` on A. Even with A's original recovery deadline
  elapsed and an eligible B, stored `[0,H)` fails the tail equality against F.
  This contradicts RFC8.3's separation of local retained ownership from remote
  negative authority. Never extend H using local assignment or partial ACKs.
- **Competing correction:** cumulative feedback can advance H when its full
  ranges fit one frame, but cannot be the sole recovery dependency: legal
  fragmented snapshots remain incomplete. Inspect the sparse producer contract
  separately, without coupling two source changes into this candidate.
- **Smallest next action:** one live retained-tail RED plus its aligned-horizon
  control, before implementation. Use actual cache/original-owner commits and
  partial ACK release; prove exact F/H, owner age and alternate qualification.
  A fixture failure before the recovery assertion does not count as RED.
- **Falsifier:** if current recovery reaches the same eligible owner/target for
  stale H as for aligned H, this proposed rejection mechanism is disproved.
- **Correction boundary:** derive retained fallback authority from current F,
  exact cached owner-uniform prefix and immutable owner clock. Preserve copy
  suppression, target ranking, publication/native admission and resource scope.
  No timeout/limit/gain change or unconditional duplication.
- **Adverse case and gate:** delayed positive ACKs can cause a redundant copy
  although native delivery succeeded; shared contention can make that copy
  harmful. Focused RED/GREEN and ordinary timing/overhead gates remain required.
  No sampler or recovery candidate is accepted from this trace alone.

**RED stage complete, 00:55 +08:00:** actual cached/ordinarily committed three
4 KiB chunks, receiver-produced ACK validation and real Product release give
H=4096, F=8192, retained=4096. Exact owner age and an explicitly fixture-measured
eligible alternate are checked before the assertion. The partial-ACK case
fails only because recovery is not queued; aligned-H control queues the exact
tail and target. Build 3m09s, pair 0.20s; initial test-only integer comparison
compile failure was corrected, not counted as Product RED. Independent audit
passes with the documented boundary: component/legal-wire contract, while
the live trace supplies actual sparse-producer reachability.

**Exact next action:** revise the retained-owner fallback contract/entry point
using positive mux frontier and exact original ownership, not completeness of
an older negative snapshot. Inspect active-tail and completion-tail call sites
together so source closure is not an accidental prerequisite. Preserve the
separate authoritative-gap path and all native/copy/qualification protections.
Keep sampler and recovery changes separately attributable before any ordinary
acceptance comparison; the archived sampler diff is not automatically accepted
as a prerequisite. No new trace, congestion tuning or sparse-producer rewrite
is needed to establish this already-proven eligibility defect.

## Existing dispositions that must not be lost

- **Retained for demonstrated mechanisms:** native packet-number/reordering
  corrections, exact qualification projection, mailbox-capacity wake,
  cooperative Product actor, paired QUIC repair stream, stateless packed ACK,
  exact terminal/recipient/reconciliation cleanup. CHANGE_DISPOSITION_20260907
  and PERFORMANCE_REFLECTION_20260907 record origins, costs and adverse data;
  component correctness is not whole-performance acceptance.
- **Latest bounded correction:** `11d6f3a` refuses a valid late STARTUP after
  FINAL at attachment scope instead of retiring its shared carrier. Legal
  sender ordering plus real TCP/QUIC adapters were RED/GREEN; 34 distinct
  focused checks and independent reviews pass. No timer or rate changed.
- **Removed/rejected:** static rank prototype, stateful relative ACK codec,
  ready-feedback batching, absolute-delay reordering, wrapperless actor and
  3N1 experiments. Raw-byte hysteresis deletion also remains removed: its
  136 checks passed, but the four-cell ordinary gate failed. Exact patches
  and timing series are archived; do not revive from one favorable mean.
- **Attribution limits:** native QoS capture shows ACK progress and ordinary
  PTO recovery, not a stuck timer. Another exact prefix repair was admitted
  after 35 ms but arrived 2.737 s later with substantial shaped/native work.
  Those traces do not justify changing a timer/gain/repair quantum. Busy-fast/
  free-slow placement and unknown-path discovery remain separate open model
  obligations; a per-head wait alone does not solve discovery.
- **Sustainability:** finite mixed churn (958 then 983 requests), TCP/QUIC
  controls and post-load quiet reclaim owners with flat observed RSS. Preserve
  half-close/restart/terminal semantics. The uncaptured deployed RAM/CPU
  incident is not claimed fully attributed by those finite tests.

## Global gates, unchanged

| Order | Existing issue / gate | Completion evidence |
| --- | --- | --- |
| 1 | Mixed allocation and request/upload sampling; cold/warm startup, recovery and first-second service | Exact causal model and live RED/GREEN, then ordinary first-body, gaps, loaded latency and confirmed bytes. Preserve qualifications and native authority; no fixed protocol preference. |
| 2 | QUIC and TCP loss/jitter/QoS/blackhole recovery in both directions | Distinguish physical queue work, native reliability, Product receipts and ordered application progress. Same-request recovery after the disturbance, not only a restart or high average. |
| 3 | Independent aggregation and shared contention | Single-link 500 Mbps; independent links 200 Mbps each. Shared cuts, asymmetric directions, dynamic loss/jitter/QoS/outage combinations and ablations. No summing estimates as capacity. |
| 4 | Short, sustained, concurrent and browser experience | TCP-only, QUIC-only and default; cold/warm single-stream plus actual speed.cloudflare.com. Compare raw TCP, Xray and Hysteria2 under matched conditions. Keep failures and upload confirmations. |
| 5 | Restart and sustainability regression gate | Churn, backpressure, server restart, ownership reclamation, CPU/RSS and post-load recovery. Reopen a closed component only on concrete contradictory evidence. |
| 6 | Final practical comparison and publication | Complete timing/latency curves, wire overhead and completion alongside goodput. Update README/PERFORMANCE and release only after practical competitive gates, not compilation or selected averages. |

For each transaction: introduction intent -> causal counterexample -> bounded
model and explicit assumptions -> RED/GREEN -> affected ordinary comparison ->
isolated retained commit or documented rejection. Do not add threshold-only
fixes or pretend an unmeasured theoretical correction benefits users.

No endpoint controller can guarantee clairvoyant optimality under arbitrary
future outages or cost-free discovery of hidden capacity. Quantify physical
constraints without waiving avoidable software delay. New unrelated hypotheses
remain outside this batch unless evidence and user scope justify inclusion.

## Execution and continuity

- No build/lab is running; the single prefix diagnostic is complete/censored
  and owned products/probes are stopped, origin services retained. Fresh
  independent trace/source and fixture audits completed. The bounded test-only
  retained-tail pair is RED/control-GREEN; its deliberately failing test remains
  in the working tree and isolated archive. No recovery runtime change yet.
  Existing sampler runtime remains unaccepted. Next step is the bounded model
  correction above, not another diagnosis capture or broad test matrix.
- Use the owned Docker topology only, not host shaping or sudo. Preserve exact
  executable/profile, phase clocks, full series and adverse results. Do not
  expand test infrastructure or repeat already conclusive diagnostics.
- Commit intermediary model/source/evidence decisions. Keep user changes in
  LIVE_OWNER_FRONTIER_WORK_BOUND.md untouched. No rejected runtime overlay is
  active. Preserve useful temporary evidence before scoped cleanup.
- Telegram milestones/blockers are authorized at intervals of at least one
  hour; last milestone sent 2026-09-07 16:44 UTC. No release gate currently permits a
  push/release. Resume this exact priority after compaction.
