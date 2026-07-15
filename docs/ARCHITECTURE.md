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
- `src/runtime/node/`: composes configured client/server identities. Per-group
  product performance policy is passed explicitly to relay flows; carrier path
  state neither owns nor silently defaults that policy.
- `src/runtime/relay/`: owns client and server flow orchestration, carrier open
  and attach transactions, remote membership, recovery, and target I/O. It does
  not own product scheduling formulas or carrier congestion control.
- `src/runtime/stream/request.rs`: owns the serialized request product state:
  exact offset flights, ordered Service identity, startup epochs, ACK-clock
  operation, and per-instance evidence. This aggregate stays lock-free because
  one client relay task mutates it.
- `src/runtime/sender/request.rs`: owns request preparation and apply. It
  reconciles lifecycle state, captures observations, revalidates exact topology,
  claims load, commits carrier enqueue, and then publishes product state.
- `src/runtime/sender/request/scheduling.rs`: owns request path admission and
  ranking over immutable, handle-free observations. TCP startup and ACK-clock
  calibration remain distinct from QUIC native capacity below this shared
  product policy.
- `src/runtime/sender/request/{tcp_capacity,quic_capacity}.rs`: own separate
  request-direction carrier capacity transactions. They share product intents,
  not controller state or proof semantics.
- `src/runtime/stream/response.rs` and `src/runtime/stream/response/`: own the
  response binding and its session, evidence, admission, handoff, delivery, and
  commit invariants. One per-session aggregate serializes shared response state.
- `src/runtime/sender/response.rs` and `src/runtime/sender/response/`: observe,
  plan, and dispatch response work without claiming product ownership before
  the stream transaction commits.
- `src/runtime/path/{set,state,selection}.rs`: own configured carrier identity,
  shared health/load ledgers, carrier-neutral capacity budgets, coherent batch
  observation, and atomic load reservation. Protocol-specific reservation and
  proof state is composed here but owned below. The session-shared TCP-Service
  request-flow count lives in path state and counts each logical stream once,
  not each attachment.
- `src/runtime/path/commands.rs`: owns bounded, lane-separated transfer to
  carrier writers. Queue accounting must balance on enqueue, dequeue,
  cancellation, and receiver drop.
- `src/runtime/path/tcp/` and `src/runtime/path/quic/`: own independent carrier
  actors, I/O, telemetry, recovery, and native capacity evidence. Kernel TCP and
  Quinn remain their respective congestion and retransmission authorities.
  Their `capacity.rs` owners contain the complete request reservation, proof,
  rollback, and carrier-specific handoff lifecycle; TCP stays path-parallel,
  while QUIC serializes one native measurement epoch per session.
- `src/runtime/stream/{handle,registry}.rs`: own carrier-neutral stream handles,
  server stream lookup, exact carrier-instance attachment, and binding lifetime.
- `src/model/` and `src/scheduler/`: own carrier-neutral evidence vocabulary,
  bounded product models, admission, and pure ranking. They do not import live
  relay, stream, or carrier handles.
- `src/transport/`: owns framing/encryption and thin TCP, UDP, and Quinn
  adapters. Native telemetry is optional capability evidence, not product
  ownership authority.

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

Request-direction discovery uses the symmetric record transaction but keeps a
separate ownership handoff. Its token owns the session slot, stream, relay
instance, train budget, attempt deadline, and publication ticket. Receipt time,
not delayed metrics publication, must precede the attempt deadline; reservation
cleanup retains that eligible receipt for one proof-validity horizon so the
publication boundary cannot race it. Publishing the ticket is part of proof
acceptance, and exact-token cleanup cannot clear a successor session.

A fenced native tail rate grants only bounded carrier-sized product authority.
The same stream-local relay instance must then ACK one fixed product floor from
bytes sent at or after proof acceptance, and the ACK itself must precede proof
expiry. Completion preserves durable ordered ownership and serializes the next
train. Its numeric rate prior remains fresh for one proof-validity horizon after
completion, after which new native evidence may correct it. Expiry, cancellation,
or association failure erases an incomplete handoff. Product ACK bytes prove
ordered use only; QUIC packet ACKs still own capacity, pacing, and recovery.

Native QUIC measurement uses every carrier byte in the timed measurement epoch
as its numerator; the required byte count is only a proof floor. Delayed ACKs
for probe packets remain excluded by sent time from ordinary carrier evidence,
but the exclusion cutoff is the peer receipt time. The quarantine record lives
until proof expiry only to catch delayed probe-era ACKs, so ordinary packets
sent after receipt become eligible immediately.

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
- Bound same-family striping inside the configured product, reorder, and stream
  envelope. Service owns the first horizon; only a strictly measured
  same-underlay Subflow may use the remaining ordered reservoir, and TCP or QUIC
  still owns its per-path emission credit. Admission compares the candidate's
  completion time with the complete Service backlog, while receiver reorder
  exposure excludes bytes already assigned to Service. Queue and native carrier
  flight already represented in Service ETA are not charged a second time. The
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
  credit to leave slow start. QUIC stays outside product-ACK rate calibration:
  one serialized carrier-only train establishes capacity, then exact
  post-proof product ACKs establish durable ownership. TCP and QUIC keep
  independent proof clocks below the shared product window.
- Size request-side QUIC warmup from candidate-local native flight and the
  effective competing rate/RTT pipe. Request snapshots carry total flight, so
  subtract separately tracked product flight before applying the native floor.
  Product flight is shared ordering state, possibly spanning several carriers,
  and must not inflate one carrier proof.
- Response discovery is directional and permits one active sustained bulk
  response to spend the first bounded same-family startup sample. That first
  sample is the non-circular discovery bootstrap. After one measured Subflow
  exists, every later fresh candidate must finish its whole startup sample
  within the current Service completion reservoir; an already-started exact
  epoch may finish. This prevents serial cold samples from inserting a slow
  ordered prefix while retaining one-flow download aggregation.
- Once Service has strict directional bulk evidence, offset-free carrier
  discovery runs independently of product Subflow graduation. One session
  serializes the typed trains; ordinary ETA, native credit, and ordering
  admission still decide whether a proven carrier receives product.
- A live discovery lease prevents session reclaim, releases before carrier
  wake or proof publication, and bounds train send plus receipt with one
  deadline. A measured cross-family handoff outranks optional TCP discovery.
- Bulk prevalidation opens the current Service-family candidate set together
  and permits at most two opens per stream/path, with retry after independent
  path-health revalidation. Attachment is path management, not capacity proof
  or permission to place ordered product bytes.
- After exact response startup drain, endpoint-only TCP may retain the proven
  same-family Service opportunity as a temporary typed capacity prior and move
  directly to ordinary bounded Subflow work. Ten ordinary exact-ACK windows
  replace it with per-flow goodput. Configured or independently measured paths
  keep staged exact calibration as fallback. While a fallback prefix is
  serialized, Service owner assignment stops when total ordered tail reaches
  that prefix plus one Service feed reservoir, clamped to the product envelope,
  until ACK progress releases credit. Offset-free raw staging does not weaken
  this ownership limit.
- Encode large probe trains incrementally. Do not allocate a vector containing
  every frame or copy the complete train solely for queue admission.
- Treat time sources explicitly. Carrier ACK timing, scheduler poll timing, and
  proof validity deadlines are different clocks and cannot be substituted.
- TCP response goodput counts only exact binding-local `OwnerData`. Its first
  product ACK establishes the clock; later bytes use a bounded ratio of bytes
  to continuous ACK wall time so callback bursts cannot discard their silence.
  A bounded Service opportunity or completed exclusive fallback may install a
  typed path-capacity prior. Ten completed ordinary exact-ACK windows plus a
  usable continuous sample atomically replace that prior with per-flow goodput;
  ACK callbacks alone do not advance the count. QUIC does not use either TCP
  clock.
- Linux TCP carriers duplicate the exact authenticated socket descriptor before
  framing and poll the UAPI `TCP_INFO` prefix in that carrier task. Returned
  prefix length grades independent RTT, flight, queue, loss, pacing, and
  delivery capabilities; absent fields remain unknown and do not clear existing
  state. Passive samples never claim product bytes. A one-shot, offset-free
  train and exact receipt may temporarily authorize that TCP instance for bulk
  placement. Only an actual native delivery field may lift that receipt, capped
  at 2x; pacing is never delivery proof. TCP and QUIC share bounded capacity
  wire records while retaining separate typed commands, controllers, proof
  validity, and recovery behavior.
- Ordered product recovery is connection-level. After one blocking TCP owner
  PTO, mptunnel may reinject at most one modeled owner flight on another path,
  capped by the shared feed reservoir. QUIC packet recovery remains native and
  its product repair stays one bounded quantum.
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

The current accepted same-condition TCP response result is Iteration 128. Its
diagnostics-disabled one-flow 18-second pair reaches 236.774 Mbps multipath
against 112.274 Mbps single, or 2.109x overall and 2.368x in the final three
seconds; two paths carry 75.5/24.5% of material server bytes. The adjacent slow
single is the host-epoch control, so this result supersedes the earlier 1.060x
normalized gain without claiming an absolute wire ceiling. Iterations 126/127
causally show why: removing a 5.3-8.8 second exclusive endpoint-only
calibration raises diagnostic goodput 59.6% and starts ordinary alternate work
immediately after exact startup drain. Server CPU, memory, and gap cost remain
non-ideal, and QUIC, mixed-carrier, fault, real-Internet, TUN, and
external-baseline cohorts remain unproven by this row.

The separate Iteration 129 heterogeneous guard reaches 104.531 Mbps multipath
versus 110.489 Mbps on the adjacent fat single path. It improves substantially
over preserved one-flow history and reduces first-body/read-gap latency, but
only low-latency and balanced paths carry material bytes. Iterations 131-134
proved that serially giving later cold TCP paths ordered startup ownership is
not the answer: fat-path use becomes material, but read gaps reach
0.525-1.269 seconds. Those experiments were rejected. The missing boundary is
carrier-native TCP capacity evidence; product ordering must not be used as a
surrogate probe clock. QUIC already owns equivalent evidence in its native
packet-ACK controller and remains separate.

Iteration 135 verifies the negative boundary with one 200 Mbps, 20 ms Service
and four 50 Mbps, 420 ms, 10%-loss optional paths. Multipath/single is
182.247/182.777 Mbps with 0.251/0.247 second maximum gaps, and all slow
optionals remain control-only. The startup completion gate therefore prevents a
clearly worse candidate from receiving the temporary Service prior.

The staged TCP-to-QUIC placement row keeps an attached UDP transport warm while
management excludes it from scheduling. This isolates proof and ownership from
connection recovery. Blackhole/reconnect rows remain a separate fault track.
