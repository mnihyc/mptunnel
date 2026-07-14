# Refactor plan

This plan applies the ownership rules in `CODE_STRUCTURE.md` to the current
tree. It is intentionally ordered by dependency and state ownership. Moving
files before their contracts are clear would only preserve the existing
coupling under new paths.

## Balance rule

Balance means comparable responsibility granularity, not equal file counts.
A directory is justified by a cohesive aggregate with private invariants;
protocol asymmetry is expected.

- Response stream logic is large and already divided among substantial
  owners. It earns one flat `runtime/stream/response/` directory, but not
  deeper `session/`, `quic/`, or `handoff/` directories.
- TCP and QUIC path directories are concrete carrier implementations with
  distinct I/O, telemetry, recovery, and actor lifecycles. Their depth is
  earned even when their file counts differ.
- Request product state is scattered across sender and relay while response
  product state has a real stream owner. That directional mismatch is a deeper
  imbalance than file count and must be corrected with peer stream owners.
- Request sender `service.rs` still contains about 96 percent of its production
  aggregate. Response admission is now a substantive peer, while response
  `planner.rs` still mixes selection, handoff, ACK-clock calibration, and
  orchestration. Both directional trees remain migration targets, but every
  child must own a real policy or transaction rather than a line-count quota.
- Thin facades, one-off helpers, and directories with no independent invariant
  are collapsed instead of being retained for visual symmetry.

## Target boundaries

### Response stream

Use a substantive `runtime/stream/response.rs` facade with one flat
`runtime/stream/response/` directory. Its production children own binding,
session coordination, load, snapshots, evidence, admission, ACK clock,
delivery, QUIC calibration, handoff, lifecycle, topology, and transaction
commit. Tests remain sibling `*_test.rs` files at the same level.

Session coordination owns one mutex and one per-session aggregate. TCP probe,
QUIC calibration, and handoff remain distinct typed operations but share an
exclusive-operation enum. Carrier-specific proof state is never unified into a
generic controller.

### Request stream

Use a peer `runtime/stream/request.rs` facade with one flat request directory.
Its binding owns product offsets, exact flights, ACK state, outstanding window,
startup epoch, evidence, repair provenance, and lifecycle. The client relay
task already serializes this state, so the binding stays single-task and
lock-free instead of copying the response side's mutex design.

The request binding replaces parallel per-instance maps with one typed subflow
aggregate and replaces independently optional ACK-clock owner/pending fields
with one exclusive operation enum. TCP and QUIC capacity controller state does
not move into this product aggregate.

### Model, scheduler, and sender

Keep request and response as peer directional owners at each earned layer.
Model owns immutable evidence and intent vocabulary. Scheduler owns pure
admission and selection over snapshots. Sender owns queues, observation,
dispatch orchestration, progress, repair, diagnostics, and distinct TCP/QUIC
capacity controllers.

Planning returns ID-only, generation-fenced intents without command senders,
binding handles, or mutable runtime state. Readiness preview must use the same
pure decision path without reserving probes, drains, or ACK-clock state. Apply
resolves the exact identity and atomically commits enqueue, exact flight, and
Service ownership; a failed apply leaves none of them published.

### Carrier paths

Keep TCP and QUIC actor trees separate beneath the path aggregate. Shared path
code owns only carrier-neutral identities, health, load, typed evidence, and
command ports. It must not import product-range ownership from stream or task
orchestration from relay.

### Relay

Relay owns ingress/target task orchestration, attach/recovery workflows, and
product I/O. Frame ranges, BDP calculations, ordering predicates, and other
pure protocol/model functions move to lower owners so carrier and stream code
do not depend on relay as a utility facade.

### Platform

OS conditionals stay at packet-device, platform-reporting, or native telemetry
adapters. Linux `TCP_INFO` is optional evidence with a portable typed fallback;
it must not gate TCP eligibility. Windows client/Linux server is the primary
cross-platform role pair, while macOS and Android remain explicit design and
verification targets.

## Migration order

1. Replace parallel response session maps with one per-session state aggregate
   and typed active operation. Preserve behavior and the single mutex.
2. Move the response mega-test into owner-specific sibling test files, then
   delete the test facade and unsafe shared inspection helpers.
3. Normalize response to `response.rs` plus flat `response/*.rs`; remove its
   production `#[path]` wiring and broad re-export chain.
4. Move pure frame, range, ordering, capacity, and path-order functions to
   protocol/model/scheduler owners. Remove path/stream/relay reverse imports.
5. Move response tests out of the request mega-test and attach them to response
   admission, selection, capacity, handoff, repair, apply, and service owners.
6. Introduce peer model/scheduler/stream request owners. Move exact flights,
   ACK/window/startup/evidence/repair state into `stream/request`, collapse its
   parallel maps, and keep the aggregate lock-free under the client relay task.
7. Split request observation, dispatch, progress, repair, diagnostics, and the
   separate TCP/QUIC capacity controllers. Move attach/fail orchestration to the
   remote-set owner, then retire `relay_striping.rs` and the request mega-files.
8. Give response planning coherent observe, decide, typed-intent, and atomic
   apply contracts. These are phase APIs, not mandatory files: keep them inside
   admission, selection, handoff, or dispatch until one phase owns an
   independently meaningful state machine. Make preview side-effect free and
   move pure capacity and placement arithmetic to model/scheduler owners without
   changing values.
9. Split shared path state and command code by identity, health, capacity
   transactions, load, queue, and writer ownership without merging TCP and QUIC
   controllers.
10. Split relay orchestration after its lower-level utilities have moved out.
11. Replace repeated `mod.rs` facades, production glob imports, runtime prelude
    leakage, and broad re-exports with named contracts.
12. Move remaining inline tests, update `ARCHITECTURE.md` to the final paths,
    then run the deferred test, target, Wine, and lab verification matrix.

## Checkpoint policy

Every structural checkpoint must preserve public behavior, format cleanly, and
pass default and all-feature production checks. Unit and integration tests are
deliberately deferred until the migration reaches a coherent endpoint, as
requested. Behavioral fixes found during migration are recorded and applied in
separate commits with focused tests and relevant lab evidence.

The response fixed TCP train/validity policy remains unchanged during ownership
moves. Its high-BDP adequacy is a later behavior checkpoint requiring matched
100-500 Mbps experiments; a structural commit cannot silently tune it.

The final checkpoint requires source tests, supported target builds, Windows
CLI/config/TCP/QUIC smoke under Wine, native packet-device follow-up where Wine
cannot provide proof, and matched historical lab comparisons. No performance
downgrade is accepted as a structural-refactor side effect.
