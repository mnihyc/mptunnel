# Response placement rate-scope audit

Date: 2026-09-07 UTC. Category: bounded source audit, not an accepted model
change or performance result. Read alongside
[bounded placement deferral](BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL.md) and
[T03 constraints](T03_ADVISORY_SCORE.md). No runtime changes, builds or labs
were performed for this audit.

## Outcome

Exact Product ACK events provide portable, carrier-neutral source material.
The currently published TCP and QUIC rates do not provide a compatible
drop-in completion comparison. Product evidence must not become a named
NativeOperational service rate by changing its label.

## Producer and ownership findings

- `src/runtime/stream/response/delivery.rs:981` releases newly acknowledged
  OriginalData for the exact output incarnation. Ambiguous copied ranges,
  invalidated generations and duplicate ACKs cannot manufacture path-proving
  rate evidence. Bytes are Product payload, scoped to one logical response
  stream and output, in the server-to-client direction.
- `response/data_commit.rs:149` and `response/delivery.rs:1166` record the
  assignment clock before the committed command can be dequeued. This is not
  a native transmission timestamp.
- `response/ack_clock.rs:55` integrates TCP ACK-spacing goodput after a first
  ACK seeds the clock. The existing minimum interval is 100 ms; gaps over
  two seconds reset this integral. These are existing behavior, not newly
  recommended parameters.
- `response/ack_clock.rs:173` uses a different UDP estimator: batch bytes
  divided by residence since earliest assignment, smoothed across fresh
  epochs, with a previous-rate maximum while carrier telemetry is app-limited.
  Thus equal byte and ACK traces need not yield equal Product rates.
- Example: two 64-KiB batches assigned at time zero and ACKed at 1.0 and
  1.1 seconds produce about 5.24 Mbps from TCP ACK spacing, versus 0.512 Mbps
  from the UDP residence-based smoothed estimate. This calculation identifies
  incompatible estimators; it does not establish a new active QUIC defect.
- `response/snapshot.rs:326` excludes TCP kernel and peer rate from its scalar
  baseline. Qualified Product goodput can raise the legacy scalar above its
  startup prior, but cannot change the independent typed rate authority.
- `response/snapshot.rs:573` projects native QUIC service exclusively and does
  not publish its numeric Product rate into completion scoring.
- `src/model/service_rate.rs:94` has no named TCP NativeOperational adapter.
  The approximately 351-Kbps portable startup value is a prior, not a measured
  TCP limit. Windows cannot inherit Linux-only diagnostic authority.

Freshness and qualification are separate. Product epochs freeze their expiry
at observation; cumulative exact Product volume cannot make an expired rate
fresh. `server_output_has_bulk_rate_evidence_at` accepts a matching native
QUIC shape even with StartupPrior, so that boolean is not stronger proof of
qualified NativeOperational service for a future Defer guard.

## Minimal advisory boundary and limits

A future Product-completion advisory can consume exact unique-byte progress
with one sampling/expiry contract across carrier families, retaining logical
stream, output incarnation and sender direction. Its workload must be matching
unique Product work, not native flight plus command work or unrelated flows.
It must remain separate from typed native C, admission, pacing and windows.

`(remaining Product bytes + new payload) / observed Product service` is at
most a conditional completion estimate while comparable service persists.
Allocation-limited, application-limited and reverse-feedback-delayed samples
are not physical capacity ceilings. An underallocated fast path and a truly
slow path can produce identical finite ACK traces. Small discovery trials do
not prove full capacity or independent marginal capacity.

Consequently, native QUIC capacity versus TCP per-flow goodput, kernel
`cwnd/RTT`, `max` of overlapping queue/flight stages, and unknown-as-measured
351 Kbps are not justified substitutes. A qualified-comparable-only Defer
policy avoids new discovery state but does not solve the recorded unknown
TCP branch. Discovery ownership remains the separate obligation already
identified in the proposal; no threshold, protocol preference or controller
is selected here.

Before any implementation, focused checks should cover identical TCP/QUIC
ACK traces, duplicate/copy ambiguity, replacement and expiry, workload-limited
and unknown alternatives, compatible work scope without double flow division,
and repeated-head non-starvation. These are proposed checks, not test passes.
