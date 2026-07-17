# Code structure rules

This is the maintained source-organization contract. `RFC.md` defines protocol
behavior and `docs/ARCHITECTURE.md` maps current owners.

## Dependency direction

Later layers may depend on earlier layers; earlier layers must not import later
ones.

1. **Protocol**: wire values, authentication inputs, frame and range semantics,
   bounded codec.
2. **Model and scheduler**: typed immutable evidence and pure decisions.
3. **Transport**: framing, encryption, TCP, Quinn/QUIC, optional host telemetry.
4. **Carrier runtime**: live TCP/QUIC actors, command queues, native evidence.
5. **Stream and datagram runtime**: MPP identity, per-direction offsets,
   Data ACK and flow control, exact flights, range attribution, reinjection,
   atomic commits.
6. **Sender**: work queues, immutable path intents, exact commit, and carrier
   command publication.
7. **Relay**: ingress/target I/O, path acquisition, and recovery coordination.
8. **Node composition**: constructs services, injects ports, owns process life.

Use narrow typed ports when a lower layer reports to a higher owner. Do not
solve dependency cycles through root re-exports, global preludes, or glob
imports.

## Protocol and carrier boundary

- `OPEN_STREAM` is neutral membership and carries only target plus initial
  demand; runtime path roles must not be added around it.
- `PATH_STATUS` is sequenced directional `Available`/`Backup` preference. Local
  health, validation, draining, and failure remain endpoint-local state.
- Wire `(underlay, path_id)` is protocol identity, not a local configuration
  index. Node composition carries accepting-listener policy into the exact
  carrier registration and response snapshot.
- `path_instance_id` fences one authenticated physical carrier. Request
  membership adds `attachment_id`; response membership adds its binding-owned
  output incarnation, and response new-data dispatch also fences the observed
  response-model generation. Status, evidence, failure, and flight commits must
  use the identity owned by their layer rather than reconstructing one from a
  path index.
- `TrafficClass` classifies mutable queued work, not links.
- `PEER_STATUS_REQUEST` and `PEER_STATUS_RESPONSE` are bounded presentation
  frames on authenticated carrier control channels. Remote status must never
  enter local scheduling, health, capacity, or failover state.
- One configured reliable TCP path has one live carrier actor. Priority classes
  share that actor; extra physical carriers require distinct protocol identity.
- TCP and QUIC retain independent congestion, pacing, and recovery state.
- Each reliable-stream direction owns independent DSN, Data ACK, and
  `STREAM_MAX_DATA` state. `STREAM_ACK` releases MPP ranges but grants no
  offset; `STREAM_MAX_DATA` grants offsets but acknowledges no range.
- Only MPP `STREAM_ACK` releases MPP ranges. Native transport ACKs are
  typed path evidence. TCP receipt/socket evidence and QUIC packet-ACK evidence
  retain separate proof state and lifetimes.

Shared policy consumes immutable carrier-neutral snapshots and emits
identity-fenced intents. TCP and QUIC may share result vocabulary and command
geometry, but never mutable controller state or proof clocks.

## Observe, decide, apply

1. **Observe** refreshes expiring state and captures one coherent snapshot. It
   does not reserve work.
2. **Decide** runs pure available-first eligibility and metric ranking without
   locks or I/O.
3. **Apply** revalidates identity, path incarnation, generations, data
   frontier, evidence, resource limits, and queue pressure before reserving,
   enqueueing, and publishing one result.

A rejected apply leaves no partial ownership. Queue and load accounting must
balance on enqueue, dequeue, cancellation, receiver drop, timeout, and task
exit.

## Modules and directories

A directory is earned by a cohesive aggregate with private invariants and
normally at least three substantive production children. Do not split files to
equalize line counts or hide a large owner behind shallow wrappers.

- A production file owns a durable policy, state machine, transaction, or
  adapter boundary that can be explained without referring to its size.
- Keep closely coupled phases together when separation would only add
  forwarding APIs or duplicate state.
- Normal depth is two domain levels. A third level is reserved for a concrete
  carrier or directional aggregate such as `path/tcp` or `stream/response`.
- Name files by ownership (`scheduling`, `evidence`, `session`, `writer`), not
  by size or chronology. Avoid `core`, `common`, `misc`, and `utils` catch-alls.
- Use `foo.rs` as the facade for `foo/`; do not add repeated `foo/mod.rs`
  facades.
- Production modules must not use `#[path = ...]` wiring.

The current response split is intentional: sender work is
`service/scheduling/multipath/dispatch`; binding state is
`ack_clock/attachment/data_commit/delivery/diagnostics/evidence/session/
snapshot`. A new child must own an invariant not already covered
by those modules.

Response new-data planning returns an ID-only target plus model generation.
`data_commit` revalidates the physical path instance, output incarnation, model
generation, and queue credit atomically before recording original flight and
publishing the carrier command. The output carrying the contiguous Data
Sequence frontier remains governed by the shared receive window and native
carrier credit. An additional output without durable unambiguous original-data
Data ACK progress is limited to one bounded startup flight; native carrier ACK
evidence cannot substitute for that Data ACK gate.

Request stream ownership is split by invariant: `attachment` owns carrier
membership, `state` owns path delivery evidence and admission, and `flight`
owns exact ranges and copies. The shared mux stream model owns receive-window
authority. Relay owns the transaction that acquires or recovers a carrier, and
sender only consumes the resulting stream-owned set. `stream/feedback` owns
connection-level Data ACK/window emission logic instantiated independently by
both relay directions.
Generic SOCKS/TUN UDP association workers live in `datagram/edge`; TUN retains
only packet-device and packet-flow concerns.

Management is split by owned boundary: `management/http` owns bounded HTTP and
browser policy, `management/schema` owns serialized contracts,
`management/projection` reads runtime owners, `management/snapshot` owns the
immutable cache/history, and `management/control` owns explicit path mutations
plus peer-request selection. `telemetry` owns exact logical product counters.
`peer_status` owns only manual request correlation; TCP and QUIC carrier actors
keep their independent control stream and writer lifetimes.

Reinjection consumes exact retained ranges. Ordinary work must pass the
cumulative extra-traffic budget. Cause-specific critical recovery may bypass
only the remaining-budget check; event sizing, exact identity, queue/flight
credit, overlap suppression, and alternate-output requirements remain in force,
and the bytes are still charged to the ledger.

## Tests

Keep one owner's tests in sibling `foo_test.rs`, included from `foo.rs` under
`#[cfg(test)]`. Split tests only along the same semantic boundaries as
production. Cross-owner workflows belong in `src/runtime/tests/` or another
clearly named integration owner.

Test support must encode a reused domain fixture or invariant. Do not retain a
helper, wrapper, or test for a one-time migration or path-renaming action.
Test-only `#[path = "foo_test.rs"]` is acceptable; production `#[path]` is not.

## Visibility and state

- Start private. Widen to `pub(super)` or a deliberate facade export only for
  a real sibling contract.
- Production imports name their source. Do not use `use super::*`; an owning
  test module may.
- Production facades re-export a small named API, never child implementation
  details. A `#[cfg(test)]` glob may expose sibling fixtures to a parent test
  owner; it is not part of the production dependency graph.
- One state machine has one owner and one synchronization strategy. A file
  split must not duplicate its lock or copy authoritative state.
- Document non-obvious lock order and transaction boundaries beside the owner.
- Do not hold runtime locks across `.await`, blocking I/O, or unbounded work.
- Hot frame/ACK paths should use actor-local state or immutable snapshots.

## Simplification rule

Delete code when supported-target inspection proves it unreachable or unused
and its tests describe no retained contract. Remove its tests, exports, docs,
and diagnostic names in the same change.

Authentication, replay protection, resource bounds, exact physical identity,
generation fences, range attribution, rollback, cancellation, and deliberate
drop order are correctness mechanisms, not optional clutter. Removing one
requires a protocol argument and focused regression coverage.

Do not encode a lab topology, interface name, benchmark rate, or host timing in
MPP policy. Thresholds come from protocol bounds, configured resource
limits, or live typed metrics. A performance patch requires a matched
experiment and a broader regression matrix.

## Platform rule

Core protocol, model, scheduling, ownership, and relay code is portable.
Platform-specific code is a narrow adapter for a host facility. Linux
`TCP_INFO` is optional evidence with a portable no-evidence fallback; it cannot
be required for path eligibility or correctness.

Packet-device acquisition and carrier-network access are separate host
capabilities. Keep target `cfg` blocks at adapter boundaries and never branch
MPP policy by operating system. Windows client/Linux server, Linux, macOS,
and Android library builds share the same protocol and scheduling model.

## Completion gate

Structural work is complete only when all of these agree:

- source tree and ownership docs contain no stale module or duplicate owner;
- default and all-feature format, clippy, compile, and test checks pass;
- supported target checks include Windows and the available Android toolchain;
- CLI/config smoke works under Wine where native Windows is unavailable, while
  clearly not claiming Wintun driver proof;
- matched single-path, multipath aggregation, and failover rows cover upload,
  download, latency, bulk, TCP, QUIC, and mixed carriers; and
- current rows are compared with retained historical bests without treating a
  changed protocol version as directly equivalent.

## Practice references

- Rust modules: <https://doc.rust-lang.org/book/ch07-05-separating-modules-into-different-files.html>
- Quinn: <https://github.com/quinn-rs/quinn#overview>
- rustls connections: <https://github.com/rustls/rustls/tree/main/rustls/src/conn>
- Tokio runtime: <https://github.com/tokio-rs/tokio/tree/master/tokio/src/runtime>
- smoltcp layering: <https://github.com/smoltcp-rs/smoltcp/tree/main/src>
