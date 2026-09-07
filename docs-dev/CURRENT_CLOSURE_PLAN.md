# Current deterministic closure plan

Updated: 2026-09-07 07:31 UTC. Authoritative source is `./`. **No release pass.**
This is the existing REVIEW_AND_PRACTICAL_ACCEPTANCE batch, not a new inventory.
Superseded checkpoints are preserved in
[the history](CLOSURE_PLAN_HISTORY_THROUGH_20260907.md) and their linked evidence.

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

1. Prove the live producer RED using actual admission, committed originals,
   bootstrap qualification and exact Product ACK release. Keep a staged
   control. Two small model counterexamples are RED for pipelining and a
   moving acquisition floor; four existing model controls pass. The first
   compiled live fixture stops at its handling of a legitimate proof command,
   before the intended assertion. Correct that fixture; runtime is unchanged.
2. After RED, implement only the independently reviewed model in
   PRODUCT_COMPLETION_OBSERVATION_MODEL_20260907: fixed entry floor, paired
   completed-cohort ACK/assignment clocks, chronological assignment guard.
   Reject invalid numeric cohorts without withholding real debt/qualification
   release or reusing their bytes. Preserve coverage, expiry, duplicate/copy,
   incarnation, maturity and native ownership rules.
3. Prove GREEN and affected controls, review exact RFC/source correspondence,
   then compare ordinary timing and completion before accepting a change.
   A numeric refresh fix is not automatically a throughput gain. No generic
   Defer, new discovery allowance, response-sampler migration or timer tuning.

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

- Three independent agents are available. One owns the focused test build;
  no overlapping builds/labs. Lab processes are stopped except origin services.
- Use the owned Docker topology only, not host shaping or sudo. Preserve exact
  executable/profile, phase clocks, full series and adverse results. Do not
  expand test infrastructure or repeat already conclusive diagnostics.
- Commit intermediary model/source/evidence decisions. Keep user changes in
  LIVE_OWNER_FRONTIER_WORK_BOUND.md untouched. No rejected runtime overlay is
  active. Preserve useful temporary evidence before scoped cleanup.
- Telegram milestones/blockers are authorized at intervals of at least one
  hour; last sent 2026-09-07 07:30 UTC. No release gate currently permits a
  push/release. Resume this exact priority after compaction.
