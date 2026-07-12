# Architecture and ownership

This map is the short guide for changing `mptunnel`. `DESIGN.md` records the
long-running design history; `RFC.md` defines protocol behavior; this document
names the current code owners and the boundaries that performance work must
preserve.

## Product and carrier layers

```text
ingress (SOCKS5 / HTTP CONNECT / TUN)
    -> product flow and MPP stream identity
    -> shared reliable-stream scheduling and offset ownership
    -> carrier command queue
       -> TCP writer and kernel TCP congestion control
       -> QUIC writer and Quinn congestion/recovery over UDP
    -> peer MPP decoding
    -> outbound target
```

The shared MPTUN layer owns product semantics: stream IDs, product offsets,
ordered delivery, repair, flow lanes, path membership, and cross-carrier
selection. TCP and QUIC own carrier semantics independently below it. A TCP ACK
and a QUIC packet ACK are therefore evidence for different controllers; neither
may release the shared product-range ledger. `STREAM_ACK` releases that ledger.

Mixed TCP+UDP is not a third carrier implementation. It is the shared product
scheduler comparing immutable evidence from two independent carrier families.
Cross-family placement must preserve the same product offset and ordering
contract as same-family multipath placement.

## Code owners

- `src/ingress/`: accepts local SOCKS5, HTTP CONNECT, and TUN traffic. It owns
  ingress parsing, not multipath placement.
- `src/outbound/`: opens the remote target or upstream proxy. It owns target
  connection policy, not carrier selection.
- `src/protocol/`: MPP wire types, authentication, and bounded codecs. A frame
  being representable here does not make it legal on every carrier or role.
- `src/runtime/relay_*`: product-flow lifecycle, local/target I/O, validation,
  repair triggers, and client-side relay coordination.
- `src/runtime/core.rs`: owns the session-shared TCP-Service request-flow count
  and its cancellation-safe registration lifetime. One active request stream
  with present work contributes once only while its exact Service is TCP; path
  attachment load and QUIC-Service demand are different ledgers.
- `src/runtime/relay_striping.rs`: owns TCP request startup, exact-owner
  graduation, and product-ACK calibration admission. It consumes the frozen
  exact calibration owner/target plus logical-flow and path evidence but does
  not redefine any of them.
- `src/runtime/sender_service.rs`: owns request-local exact calibration identity,
  frozen target/spend rollback, causal ACK boundaries, and the continuous
  per-flow ACK model. It also ranks immutable response-path snapshots and
  proposes work. It must not mutate carrier recovery state or claim product
  offsets before the reliable-path commit.
- `src/runtime/reliable_path.rs`: owns product stream attachments, exact range
  flights, ordering debt, and atomic response placement commits.
- `src/runtime/reliable_path/response_admission.rs`: owns response path evidence,
  TCP product-ACK calibration state, and scheduler snapshots.
- `src/runtime/reliable_path/response_placement.rs`: normalizes evidence scope and
  owns carrier-neutral whole-response-flow placement policy.
- `src/runtime/reliable_path/response_session.rs`: owns session-wide load,
  generation fences, bounded probe spend, and exclusive calibration leases.
- `src/runtime/reliable_path/registry.rs`: owns server stream lookup, carrier
  instance lifetime, and publication of local/peer path metrics.
- `src/runtime/reliable_path/quic_capacity_probe.rs`: admits one typed,
  token-scoped QUIC capacity request. It does not generate generic product
  evidence.
- `src/runtime/reliable_path/response_service_handoff.rs`: revalidates and
  commits whole-flow Service handoff at a clear product frontier.
- `src/runtime/path_commands.rs`: bounded, lane-separated transfer from the
  carrier-neutral scheduler to a carrier writer. Queue accounting is owned here
  and must balance on enqueue, dequeue, cancellation, and receiver drop.
- `src/runtime/tcp_path.rs` and `src/runtime/server_tcp.rs`: TCP carrier frame
  I/O. Kernel TCP remains the congestion/retransmission authority.
- `src/runtime/udp_path.rs` and `src/runtime/udp_metrics.rs`: QUIC path lifecycle
  and conversion of native carrier observations into path evidence. Application
  datagram flows are separate from reliable streams carried by QUIC.
- `src/transport/quic_carrier.rs`: the thin Quinn boundary. It owns QUIC record
  writes and coherent native ACK/congestion telemetry, not product scheduling.
- `src/runtime/bulk_admission.rs`, `multipath_model.rs`, and
  `response_ownership.rs`: carrier-neutral policy and bounded product models.

## Mutation contracts

Scheduling uses a propose/revalidate/commit pattern. The sender may rank a
snapshot without holding runtime locks. The owner then rechecks path identity,
incarnation, model generations, product frontier, proof authority, and the
ranked pending-byte credit bound before it mutates ownership. Shared carrier
queue/BIF is pressure rather than binding ownership: it may fall after ranking,
but growth outside the admitted envelope rejects the proposal.

Carrier identity is `(session, underlay, path_id, path_instance_id)`. A reused
numeric path ID is not enough to inherit flights, leases, or proof from a dead
connection. Stream attachment role and carrier instance lifetime are separate:
closing the last product stream must not silently reset a live carrier's
session-wide probe budget.

QUIC capacity proof is a transaction:

1. Reserve one bounded session/path attempt with frozen train geometry and a
   proof-validity interval distinct from its attempt deadline.
2. Admit one typed command carrying the exact token, path instance, deadline,
   validity interval, and invalidatable ownership ticket.
3. Gate ordinary connection writers and stream bounded token records plus an
   ordered finish marker without product offsets.
4. Accumulate one non-interleaved, session-bounded client epoch and require its
   exact whole-train receipt. Native ACK timing is provisional diagnostics and
   ordinary product evidence stays independent.
5. Freeze the full receipt-interval rate and expiry and release the carrier
   writer gate. A separate sent-time quarantine keeps probe-era ACKs out of
   generic product evidence through that expiry without blocking new writes.
6. Commit the exact lease before publishing the marker everywhere, resolve
   publication separately from cancellation, then retire public token metrics.

Raw capacity Data/Finish/Receipt records are not generic `SendFrame` work. Data
and Finish are legal only inside the typed server-to-client QUIC probe command;
Receipt is generated only by the client-side receiver. This keeps TCP framing,
peer roles, and ordinary QUIC batching from bypassing the lease and accounting
contract. Cancellation interrupts a partial probe write and fail-closes that
connection; abandoning a queued command cancels its ticket and reconciles its
full logical byte charge. Publication wakes cleanup with a distinct resolution.

## Performance rules

- Keep product scheduling carrier-neutral; specialize only where the carrier's
  controller exposes different evidence or mechanics.
- Batch immutable snapshots and lock once per scheduling pass. Packet/ACK and
  frame hot paths must not take session-wide locks.
- Bound queues and retained flight/proof state by existing mux resource limits.
  A one-item command may represent a large train, so cancellation and drop must
  still reconcile its full logical byte charge.
- Keep raw source staging distinct from assigned product flight. Before exact
  Service feed evidence, a switchable same-family response couples its global
  owner tail and raw queue inside one derived feed reservoir (4 MiB with
  defaults). This is a bounded product bootstrap, not a carrier congestion
  window. A current QUIC Service may graduate source and emission staging from
  either substantial uniquely owned product `STREAM_ACK` progress or a durable
  local carrier ACK-derived DATA estimate, even when the latter is app-limited.
  Neither is optional-path capacity proof; TCP uses its strict product/carrier
  evidence. After graduation and without same-path latency pressure, either
  underlay may fill one configured product envelope so its native transport
  owns the pipe.
  Mixed-family raw staging stays in a separate bounded reservoir. Request-side
  source and repair debt share one carrier-neutral product window: it starts at
  the same 4 MiB reservoir, grows on exact unambiguous OwnerData ACK turnover,
  and resets only when ordered product ownership commits an exact Service
  handoff. Active attachment-list churn is not a handoff; temporary Service
  absence retains the bound, and bulk-to-latency demotion closes it to the
  classifier reservoir. Those ACKs never set TCP or QUIC carrier capacity.
- Treat bulk receive credit as receiver-memory authority. TCP and QUIC
  `STREAM_MAX_DATA` advertise the configured product window independently of
  path proof; source staging and native carrier congestion control separately
  bound admitted and network flight. Latency QUIC retains its smaller window.
- Bound same-family striping inside that envelope. Service owns the first
  horizon; only a strictly measured same-underlay Subflow may use the remaining
  feed reservoir, and TCP or QUIC still owns its per-path emission credit. The
  weaker QUIC product-progress/carrier Service-feed predicates do not prove
  optional capacity or admit a Subflow; QUIC Subflow, handoff, and capacity
  decisions still require strict non-app-limited local carrier proof. A QUIC
  request Validation attachment graduates only when its exact path proof and a
  fresh native packet-ACK sample produced after attachment are both valid. This
  can reuse capacity established by concurrent carrier traffic but cannot bootstrap
  an otherwise idle one-flow QUIC candidate from ordered product bytes. Before a
  high-confidence additional QUIC path has durable product progress, a
  BBR-style inflight target of `2 * delivery-rate BDP` bounds product reorder
  exposure; carrier-only pacing/cwnd growth stays below that boundary.
  Low-confidence QUIC response startup remains separately epoch-bounded and
  uses native inflight credit, falling back to the delivery-rate target when no
  native window is available. Datagram goodput may rank datagram paths but never
  satisfies reliable product durability; data-plane failure invalidates both
  durable product and native-window authority for the failed association.
- Open fresh TCP request discovery only under real same-family logical
  contention. Startup and zero-spend ACK-clock calibration require at least two
  active logical bulk request flows whose exact committed Service is TCP,
  counting each stream once regardless of its path attachments. Present queued
  or outstanding request data is required; reverse bytes, idle completed
  uploads, QUIC-Service demand, and per-path load cannot substitute for this
  gate. A begun exact-owner epoch may drain after a two-to-one transition.
  No path-wide completion estimate may veto fresh request calibration until
  request-direction, provenance-bound authority exists. The ACK of all exact
  sealed startup `OwnerData`, or the exact ordered receipt ACK when it arrives
  first, establishes the candidate's calibration boundary. One explicit
  exact-instance calibration owner then spends one frozen, cumulative,
  non-refilling target, 2 MiB with default envelopes. Exact-owner, debt,
  resource, pressure, and post-boundary causality guards still apply. The
  target does not expand to a modeled pipe. Instead, exact ownership permits a
  provisional Service-derived rate and pipe only for an endpoint-only
  candidate until its continuous product-ACK model reaches ten exact samples,
  at which point its own model replaces that prior. A configured candidate
  retains its own capacity hint. This avoids serial probe work stalling the only
  data-bearing upload while still giving kernel TCP enough bounded exploration
  credit to leave slow start. QUIC stays outside product-ACK calibration and
  requires attributable post-attachment native packet-ACK evidence. One-flow
  optional-path aggregation remains unproven for both carrier families.
- Encode large probe trains incrementally. Do not allocate a vector containing
  every frame or copy the complete train solely for queue admission.
- Treat time sources explicitly. Carrier ACK timing, scheduler poll timing, and
  proof validity deadlines are different clocks and cannot be substituted.
- TCP response goodput counts only exact binding-local `OwnerData`. Its first
  product ACK establishes the clock; later bytes use a bounded ratio of bytes
  to continuous ACK wall time so callback bursts cannot discard their silence.
  It remains per-flow evidence, not TCP carrier capacity.
- Thresholds must be protocol/resource bounds or derived from live metrics.
  Lab-specific constants must not become steady-state product policy.

## Evidence and labs

`lab/run-heterogeneous-ablation.sh` is the main topology runner. `docs/LAB.md`
defines controls and result semantics; `docs/PERF.md` defines profiling;
`docs/BENCHMARKS.md` records reference interpretation. Lab families are separate
evidence tracks: clean release performance, diagnostic causal traces,
unconstrained ceilings, shaped daily links, faults, real-Internet checks, TUN,
and TCP-only/UDP-only/mixed variants.

Use a diagnostic row to prove the intended state transition before comparing
throughput. Use matched, instrumentation-free release rows for performance
claims. A poor row should first identify the violated ownership, evidence,
queue, or clock contract; repeating the same row without a new hypothesis is
not an optimization iteration.

The current accepted same-condition TCP request result is Iteration 109. Its
diagnostics-disabled, exact 18-second upload rows use two logical flows and five
500 Mbps, 180 ms, 1 ms jitter, zero-loss paths: multipath reaches 691.368 Mbps
against 314.999 Mbps single-path, or 2.195x overall. The broad `[9,18)` and
supporting `[15,18)` ratios are 2.935x and 2.409x. Client transmit shares are
37.15%, 33.44%, 4.78%, 10.47%, and 14.17%. Relative to Iteration 69 under the
same profile, multipath changes +10.38% overall, +11.77% broad, and -6.36% late;
the `[9,15)` window improves 22.02%, so the lower late burst reflects earlier
delivery rather than a hidden aggregate loss. The matched single control changes
-0.32%, +0.56%, and +3.53%. This proves the
final TCP ownership/calibration model did not silently trade away the retained
multi-flow aggregation result. It does not prove one-flow striping, QUIC or
mixed-carrier aggregation, real-Internet performance, failover, or current
MPTCP/Hysteria2 superiority.

The staged TCP-to-QUIC placement row keeps an attached UDP transport warm while
management excludes it from scheduling. This isolates proof and ownership from
connection recovery. Blackhole/reconnect rows remain a separate fault track.
