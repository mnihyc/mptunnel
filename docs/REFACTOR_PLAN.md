# Refactor completion plan

This document tracks the endpoint of the source reorganization. It is not an
alternative architecture; `docs/ARCHITECTURE.md` is authoritative for current
ownership and `RFC.md` is authoritative for protocol behavior.

## Objective

The refactor is complete when a reader can follow one MPP range from
ingress through scheduling, one concrete TCP or QUIC carrier, peer reassembly,
and target delivery without encountering duplicate state machines, stale path
roles, or transport-specific policy in the shared layer.

The protocol endpoint is version 2:

- neutral `OPEN_STREAM(stream_id, target, demand)`;
- sequenced directional `PathUsage::{Available, Backup}`;
- local health separate from peer preference;
- available-first metric scheduling;
- connection offsets, `STREAM_ACK`, shared receive window, exact range
  attribution, and bounded reinjection; and
- separate native TCP and QUIC recovery below the shared connection.

## Source endpoint

The intended top-level domains are:

```text
src/
  protocol/             wire values, authentication, codec, range semantics
  model/                immutable evidence and bounded MPP models
  scheduler/            pure eligibility and path scoring
  transport/            encryption, TCP/QUIC adapters, optional telemetry
  runtime/
    path/{tcp,quic}/     concrete carrier actors and native evidence
    stream/              feedback, request attachments/state, registry,
                         response bindings
    sender/{request,response}/
                         MPP queues, planning, commit, dispatch
    relay/               I/O orchestration and recovery coordination
    datagram/            MPP associations and shared edge workers
    node/                composition
  ingress/               SOCKS5, HTTP CONNECT, TUN
  outbound/              direct and upstream proxy connectors
  simulator/             deterministic model experiments only
```

Response sender modules are exactly `service`, `scheduling`, `multipath`, and
`dispatch`. Response binding modules are exactly `ack_clock`, `attachment`,
`data_commit`, `delivery`, `diagnostics`, `evidence`, `session`,
and `snapshot`. Deleted pre-v2 wrapper modules are not retained.

## Ownership checks

### Protocol and model

- Wire types contain peer-owned facts only.
- No fixed attachment role exists in a stream open or runtime binding.
- `PathUsage` sequence handling rejects stale preference updates without
  changing local health.
- `TrafficClass` remains mutable work demand and is never stored as a link class.
- MPP ranges and Data ACK semantics are independent of transport family.

### Request direction

- `runtime/stream/request.rs` is a narrow facade over three serialized owners:
  `attachment`, `state`, and `flight`.
- `attachment` owns carrier membership and attachment lifetimes; `state` owns
  per-path delivery evidence and admission; `flight` owns exact range/copy
  accounting. The shared mux stream model owns the MPP receive window.
- `runtime/sender/request.rs` is the relay-facing facade and dispatch owner.
- Sender imports no concrete state or policy from relay; relay acquires paths
  and retries unchanged queued work when attachment is required.
- Request scheduling consumes snapshots; it does not hold runtime handles.
- TCP fallback capacity measurement stays in its carrier-specific owner. QUIC
  paths publish native congestion state and use ordinary bounded MPP admission;
  carrier-neutral proof validation is a pure model function.
- Reinjection selects missing MPP ranges and does not impersonate native
  retransmission.

### Response direction

- One response binding owns target-stream lifetime and neutral attachments.
- The response service owns queued work once; attachments do not duplicate the
  source queue ledger.
- Scheduling is pure, multipath planning captures generations, and dispatch
  alone performs final revalidation and carrier enqueue.
- Exact path-instance metrics and peer usage are projected into snapshots.
- There is no hidden current-path slot that turns attachment order into policy.

### Carriers

- TCP and QUIC have separate connection, writer, reader, capacity, and native
  measurement state.
- Shared carrier commands contain MPP work and identity, not one
  transport's congestion state.
- TCP kernel recovery and QUIC packet recovery remain authoritative below MPP.
- Linux socket telemetry is an optional adapter; the portable fallback remains
  operational and eligible.

### Relay and node

- Relay owns open/attach/recovery transactions, not scheduling equations.
- Every accepted carrier path is neutral connection membership.
- Server carriers reach stream/datagram services only through typed ports.
- Accepting-listener policy is carried into the exact server path instance and
  is never recovered by indexing local configuration with a peer path ID.
- Node composition pairs one session registry with one target relay owner.

## Migration order

1. **Shape and tests**: establish domain facades, move unit tests to `_test.rs`,
   and delete one-time migration helpers.
2. **MPP data state**: consolidate exact offsets, ACK effects, windows, and
   reinjection ledgers under request/response owners.
3. **Carrier boundary**: separate TCP and QUIC actors and typed evidence from
   shared scheduling.
4. **Role removal**: remove stream-open roles and attachment promotion slots;
   add directional sequenced usage.
5. **Scheduling**: make both directions available-first and metric-driven over
   coherent snapshots.
6. **Documentation**: align RFC, architecture, design, operations, and lab
   methodology with the source tree.
7. **Verification**: format, clippy, default/all-feature tests, supported target
   checks, Wine CLI/config smoke, then bounded representative labs.

Do not run performance experiments against a half-migrated binary. Compile and
unit checks begin once the source boundary is coherent; end-to-end labs begin
after correctness and cross-target checks pass.

## Static stale-code gate

Before calling the refactor complete, inspect all production modules for:

- unused facade exports and dead feature branches;
- old stream-open role vocabulary or fixed path placement;
- duplicated MPP queue, flight, window, or path metric state;
- server target tasks spawned by carrier actors;
- shared code that assumes Linux telemetry;
- MPP decisions based on interface names, path indexes, or lab rates;
- tests embedded in production files instead of sibling `_test.rs`; and
- source references to deleted modules or diagnostic events.

Delete a stale owner and its tests/docs together. Do not preserve compatibility
inside unshipped internal APIs.

## Verification matrix

The final verification order is deliberately bounded:

1. `cargo fmt --check` and line-count warning.
2. `cargo clippy --all-targets --all-features -- -D warnings`.
3. default and all-feature tests.
4. Linux/macOS/Windows target checks and available Android library check.
5. Windows CLI/config smoke under Wine; native Wintun remains a native-host
   integration test.
6. About ten representative Docker cases covering direct/single baseline,
   TCP/QUIC/mixed multipath, upload/download, latency/bulk, aggregation,
   blackhole failover, and traffic overhead.
7. Same-profile comparison with protocol-v2 repeats and separately labeled
   pre-v2 historical bests.

Any material performance downgrade reopens the owning model or data-flow step.
Do not mask it with a topology-specific threshold or continue repeating a case
without a new causal hypothesis.
