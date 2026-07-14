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
- Request sender `service.rs` and its test file contain nearly their entire
  aggregate. Response sender `planner.rs` also owns several unrelated policy
  stages. Those are real imbalance and must be divided by state and algorithm
  ownership.
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

### Sender

Keep request and response as directional aggregates. Each named facade owns its
service state and orchestration; children own observation, capacity evidence,
admission, selection, intent construction, dispatch, repair, and diagnostics.
Pure planning receives immutable snapshots and returns generation-fenced
intents. Runtime application revalidates and commits those intents atomically.

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
5. Split request sender by flight, ACK clock, capacity, planner, dispatch,
   repair, startup, and diagnostics ownership. Split its tests at the same
   boundaries and retire `relay_striping.rs` as a catch-all owner.
6. Split response planning into observe, decide, intent, and apply contracts.
   Keep selection pure and put queue/flight mutation only in apply owners.
7. Split shared path state and command code by health, capacity transactions,
   load, queue, and writer ownership without merging TCP and QUIC controllers.
8. Split relay orchestration after its lower-level utilities have moved out.
9. Replace repeated `mod.rs` facades, production glob imports, runtime prelude
   leakage, and broad re-exports with named contracts.
10. Move remaining inline tests, update `ARCHITECTURE.md` to the final paths,
    then run the deferred test, target, Wine, and lab verification matrix.

## Checkpoint policy

Every structural checkpoint must preserve public behavior, format cleanly, and
pass default and all-feature production checks. Unit and integration tests are
deliberately deferred until the migration reaches a coherent endpoint, as
requested. Behavioral fixes found during migration are recorded and applied in
separate commits with focused tests and relevant lab evidence.

The final checkpoint requires source tests, supported target builds, Windows
CLI/config/TCP/QUIC smoke under Wine, native packet-device follow-up where Wine
cannot provide proof, and matched historical lab comparisons. No performance
downgrade is accepted as a structural-refactor side effect.
