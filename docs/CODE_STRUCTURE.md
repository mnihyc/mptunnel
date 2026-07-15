# Code structure rules

This document is the maintained contract for code ownership and dependency
direction. `RFC.md` defines protocol behavior; `docs/ARCHITECTURE.md` maps the
owners that implement it. A refactor is complete only when the code, tests, and
ownership map agree with these rules.

## Dependency direction

Each later layer may depend on earlier layers in this list. An earlier layer
must not import a later one.

1. **Protocol** owns wire values, authentication inputs, frame semantics, range
   operations, and bounded codecs. It has no runtime tasks or policy.
2. **Model and scheduler** turn typed immutable inputs into decisions. They have
   no sockets, channels, locks, platform APIs, or implicit wall clock.
3. **Transport** adapts framing, encryption, TCP, and Quinn/QUIC mechanics. It
   does not own product offsets or multipath placement.
4. **Carrier** owns live TCP and QUIC path actors, carrier command queues, and
   conversion of native observations into typed evidence.
5. **Stream and datagram** own product identity, flow control, ordered offsets,
   exact flights, repair, and atomic placement commits.
6. **Relay** coordinates ingress or target I/O with product services. It does
   not redefine carrier evidence or product admission policy.
7. **Runtime and node composition** construct services, connect ports, and own
   process lifetime.

On the server, one identity owns one uniquely paired stream registry and target
relay service. Carrier actors may admit or attach streams, but they do not spawn
target relays. An accepted-stream lease keeps registry membership alive until
carrier output has closed; close and late attachment linearize under the
response binding's output lock.

When a lower actor must notify a higher service, define a narrow typed port at
the lower boundary and inject its implementation from composition. Do not solve
the dependency by a root re-export, a global prelude, or `use super::*`.

## Carrier boundary

TCP and QUIC are separate concrete authorities:

- TCP owns socket I/O and relies on kernel TCP congestion and retransmission.
  Native socket telemetry is optional evidence.
- QUIC owns Quinn connection I/O, packet ACKs, congestion, pacing, and recovery.
- Shared path authentication owns only the carrier-neutral HELLO, AUTH, and
  JOIN transitions. TCP and QUIC retain their own frame I/O and acknowledgements,
  while the server path context owns replay admission.
- Neither carrier ACK releases product ranges. Only the product `STREAM_ACK`
  authority may release the shared exact-flight ledger.

The shared layer consumes carrier-neutral typed snapshots and emits typed,
identity-fenced intents. A snapshot records direction, evidence provenance,
freshness, logical path, physical path instance, and relevant generations. An
intent identifies work and authority; it does not contain a carrier controller.
TCP and QUIC may share command geometry and result vocabulary, but not mutable
controller state or proof clocks.

## Observe, decide, apply

Scheduling and ownership changes use three explicit phases:

1. **Observe**: the owner refreshes expiring state and creates one coherent,
   immutable snapshot. Observing must not silently reserve work.
2. **Decide**: a pure function ranks snapshots and returns an ID-only selection.
   It takes no runtime lock and performs no I/O.
3. **Apply**: the lifecycle owner combines that selection with the generations
   observed for the planning pass. The state owner then reacquires its lock,
   revalidates identity, generations, evidence, limits, and queue pressure,
   reserves, enqueues, records exact flight, and publishes one coherent result.

A rejected apply leaves no partial ownership. RAII guards must make cancellation,
queue drop, timeout, and task exit reconcile reservations exactly.

## Modules and directories

A directory is earned by a cohesive aggregate with private invariants and
normally at least three production children. A small facade around one large
file is a migration smell, not architecture. Conversely, several substantial
siblings with one owner may remain a flat directory; depth is not awarded to
make peer file counts equal.

Use these balance checks during review:

- A child containing more than about 60 percent of its aggregate is a split or
  collapse review trigger, not an automatic failure.
- Line count is never a reason to create a production file. A file should own
  a durable policy, state machine, transaction, or adapter boundary that can be
  explained without referring to its size. Keep closely coupled phases in one
  substantive owner when separating them would only add forwarding APIs.
- Peer domains should have comparable granularity where responsibilities match.
  Honest TCP/QUIC or request/response asymmetry is allowed and documented.
- Normal depth is two domain levels. A third level is reserved for an earned
  concrete carrier or directional owner. Do not create generic chains such as
  `service/logic/common/helpers`.
- Name files for ownership (`planner`, `evidence`, `session`, `writer`), not size
  or chronology. Avoid catch-all `core`, `common`, `misc`, and `utils` modules.

Use the named facade convention: `foo.rs` declares children in `foo/`. Do not
introduce repeated `foo/mod.rs` facades. Production modules must not depend on
`#[path = ...]` in the final tree. Test-only sibling wiring may use `#[path]` as
described below.

## Tests

Keep one owner's tests in a sibling `foo_test.rs`, included by `foo.rs` under
`#[cfg(test)]`. Split a large test file along the same ownership boundaries as
production code. Cross-owner scenarios belong in a clearly named test folder
or crate-level integration test, not in an unrelated owner's unit tests.

Test support must encode a reused domain fixture or invariant. Do not retain a
helper, wrapper module, or path-checking test for a one-time migration action.
Test-only `#[path = "foo_test.rs"]` is acceptable; production `#[path]` is not.

## Visibility, imports, and state

- Start private. Widen to `pub(super)` or a named facade export only for a real
  sibling contract. Keep the crate's public API at deliberate top-level owners.
- Production imports name their source. Do not use glob imports or
  `use super::*`; tests may use `super::*` within their owning module.
- Facades re-export a small named API, never all child implementation details.
- One state machine has one owner. Splitting files must not split a lock or
  duplicate state merely to make files smaller.
- Document non-obvious lock order and the transaction it protects beside the
  owning state. Do not hold runtime locks across `.await`, blocking I/O, or
  unbounded computation.
- A channel owns a specific handoff and its accounting. Enqueue, dequeue,
  cancellation, receiver drop, and task exit must balance the same byte ledger.
- Hot packet, frame, and ACK paths should use local state or immutable snapshots,
  not a session-wide lock on every event.

## Deleting or simplifying code

Delete code when static inspection proves it unreachable or unused across the
supported feature and target matrix, and its tests describe no retained
contract. Remove its tests and exports in the same change.

Do not classify these as useless defensive code merely because the happy path
rarely exercises them: authentication and replay checks, resource bounds,
logical and physical identity fences, generation checks, exact range ownership,
rollback, cancellation, and deliberate drop order. Removing such a fence needs
a protocol argument, a focused regression test, and relevant lab evidence when
it can affect scheduling or throughput.

Product policy must not encode a lab topology, interface name, fixed benchmark
rate, or one host's timing. Thresholds come from protocol bounds, configured
resource limits, or live typed metrics. A performance change is accepted by a
matched experiment, not by making one recorded row pass.

## Platform rule

Core protocol, model, scheduling, and ownership code is platform-neutral.
Platform-specific code is a narrow adapter for a real host facility, such as
Linux TCP telemetry or packet-device construction. Optional telemetry must have
a typed portable fallback; it must not become an eligibility requirement.

Packet-device acquisition and carrier-network access are separate host
capabilities. A carrier-network adapter resolves each configured endpoint on a
selected native network, then may bind a source address or protect its sockets
from an Android VPN route before connect. TCP and QUIC consume the same neutral
host capability but retain separate connection and handshake algorithms;
neither scheduling policy nor stream state may inspect an interface name, file
descriptor, or OS network handle.

Windows client with Linux server, Linux, macOS, and Android are supported design
targets even when a local lab exercises fewer hosts. Keep target `cfg` blocks at
adapter boundaries and never branch product policy by operating system.

## Migration endpoint

Each structural checkpoint should format and compile with default and all
features without changing behavior. The migration endpoint additionally requires:

- no production glob facade, `use super::*`, or `#[path]` module wiring;
- ownership documentation and source paths that match the final tree;
- focused unit tests, full default/all-feature tests, and relevant integration
  tests;
- Windows cross-compilation and CLI/config smoke under Wine, plus the available
  Android and other platform checks (Wine does not prove driver integration);
- matched single-path baseline rows and multipath aggregation/failover rows for
  upload and download, latency-sensitive and bulk work, and TCP-only, QUIC-only,
  and mixed carriers; and
- comparison with retained historical best rows so a structural or behavioral
  change cannot silently accept a performance downgrade.

## Practice references

- Rust module files: <https://doc.rust-lang.org/book/ch07-05-separating-modules-into-different-files.html>
- Quinn architecture: <https://github.com/quinn-rs/quinn#overview>
- rustls connection ownership: <https://github.com/rustls/rustls/tree/main/rustls/src/conn>
- Tokio runtime ownership: <https://github.com/tokio-rs/tokio/tree/master/tokio/src/runtime>
- smoltcp protocol layering: <https://github.com/smoltcp-rs/smoltcp/tree/main/src>
