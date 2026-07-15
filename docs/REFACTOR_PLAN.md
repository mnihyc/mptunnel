# Refactor plan

This plan applies the ownership rules in `CODE_STRUCTURE.md` to the current
tree. It is intentionally ordered by dependency and state ownership. Moving
files before their contracts are clear would only preserve the existing
coupling under new paths.

## Source endpoint status

The ordered source migration is complete through step 11, together with the
source, test-layout, documentation, and host-build parts of step 12. The final
tree has no orphan Rust modules, inline test bodies, `mod.rs` facades,
production wildcard imports, production `#[path]` wiring, stale migration
trees, or platform branches in protocol/model/scheduler policy. Default and
all-feature suites and the all-target/all-feature host check pass.

The final semantic pass is also complete. It removed unused wire frames and
product-owned QUIC PMTU state, collapsed path admission to one valid-state enum,
made repair a separate work decision, reduced subflow epochs to their actual
membership/startup-credit authority, consolidated request startup evidence,
and restored response queue pressure to one stream owner. It also moved virtual
DRR/tail/duplication models under the simulator, consolidated ACK-flight interval
algebra and TCP path evidence, and made endpoint path/security configuration one
immutable indexed allocation shared by its sessions. Normal TCP peer departure
now follows the same carrier-lifetime transition on admission, read, and write.
This was a model and data-flow cleanup, not another source-tree migration; the
existing module boundaries remained intact.

External proof remains deliberately separate from source completion. The
Windows GNU binary builds and its CLI, config, platform report, and
Windows-client/Linux-server TCP flow work under Wine. Quinn's own Windows UDP
endpoint fails under Wine before mptunnel code because Wine rejects
`IPV6_V6ONLY` inspection on an IPv4 socket; native Windows must therefore prove
QUIC and Wintun. The installed macOS Rust targets cannot pass the dependency
build without an Apple cross C toolchain/SDK. Historical performance labs are
also a separate behavior checkpoint and were not substituted with structural
test results.

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
- Request product state now has one lock-free `stream/request.rs` owner, peer to
  the response binding. Remaining imbalance is orchestration concentrated in
  the request sender facade, not a reason to fragment the product aggregate.
- Request sending keeps one flat capability directory. TCP capacity, QUIC
  capacity, and request scheduling each own a substantive mechanism; the
  `sender/request.rs` facade retains serialized preparation and apply. It must
  shrink by moving whole evidence, dispatch, or repair capabilities, not by
  creating shallow phase directories or hundred-line helper files.
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

Use one substantive peer `runtime/stream/request.rs` owner until it has at least
three independently meaningful children. It owns product offsets, exact
flights, ACK state, outstanding window, startup epoch, exact-instance evidence,
and repair provenance. The client relay task already serializes this state, so
the aggregate stays single-task and lock-free instead of copying the response
side's mutex design.

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

Request selection stops live runtime owners at the policy boundary. One batch
path observation uses one health lock and timestamp, then joins exact attachment
instances, placement, queue readiness, and load ownership without exposing path
or carrier handles to policy. Shared admission consumes raw candidate evidence;
TCP and QUIC keep distinct native evidence below that carrier-neutral input.

The serialized sender explicitly executes `prepare -> observe -> decide ->
apply`. Preparation may reconcile lifecycle state and enqueue control evidence,
but never unique data. A decision carries the observed `RelayPathInstance` and
complete remote-set membership generation. Apply resolves that identity,
conditionally claims scheduler load, enqueues the carrier frame as the commit
point, transfers the lease, and only then publishes request state. Queue credit
is revalidated by enqueue, so a stale observation cannot partially commit.
Proof-authorized startup and calibration also carry proof ID, health generation,
and attachment epoch; apply revalidates that exact authority before claiming
load or enqueueing unique data.

### Carrier paths

Keep TCP and QUIC actor trees separate beneath the path aggregate. Shared path
code owns only carrier-neutral identities, health, load, typed evidence, and
command ports. The shared authentication adapter owns the protocol transition
and signed frame construction; concrete carriers own the reads, writes, and
activation response, and the server context owns replay admission. Path code
must not import product-range ownership from stream or task orchestration from
relay.

### Relay

Relay owns ingress/target task orchestration, attach/recovery workflows, and
product I/O. Frame ranges, BDP calculations, ordering predicates, and other
pure protocol/model functions move to lower owners so carrier and stream code
do not depend on relay as a utility facade.

Server target relay lifetime is now one flat `runtime/relay/server.rs` owner.
Each server identity constructs one registry/service pair; TCP and QUIC carrier
actors submit accepted leases through that pair instead of spawning detached
target tasks. Shared receive/range primitives remain in `relay/io.rs`. The
client side keeps two flat owners: `relay/open.rs` executes concrete TCP/QUIC
reservation, deadline, acceptance, and retry contracts, and owns cleanup until
an opened stream commits; `relay/remote.rs` owns attachment role/candidate
policy, in-flight claim exclusion, membership commit/rollback, placement
ordering, load claims, frame fan-in, and teardown. Carrier-derived PTO timing
comes directly from `model/timing.rs`; relay I/O does not import the client
control actor for either policy. Open and remote attachment enqueue fixed
request control through the reliable stream binding rather than importing the
request sender. Dependencies run from control to sender, then remote, open, and
stream; switchable response output remains a distinct placement contract.
Client relay orchestration keeps one serialized select actor in
`relay/control.rs`, while `relay/client.rs` owns its durable endpoint/FIN,
progress/ACK, recovery, and delivery aggregates. Peer STREAM_DATA application
and STREAM_ACK repair derivation commit through that owner before path policy
runs. This is one flat substantive module, not a phase directory or collection
of pass-through helpers.

The opened-stream value is itself the pending attachment transaction: it owns
both carrier cleanup and an optional scheduler-load lease. Initial and direct
Active opens acquire the lease before asynchronous I/O; successful Active
attachment transfers it to the remote path, while Repair/Validation attachment
drops the temporary lease before publishing membership. Background validation
opens are explicitly lease-free because attachment alone is not product demand.
There is no parallel `load_reserved` boolean or manual async rollback matrix.

### Platform

OS conditionals stay at packet-device, platform-reporting, or native telemetry
adapters. Linux `TCP_INFO` is optional evidence with a portable typed fallback;
its returned prefix is parsed as independent capabilities, and missing fields
remain unknown rather than measured zero. Native pacing is not delivery
authority and must not gate TCP eligibility. Windows client/Linux server is the
primary cross-platform role pair, while macOS and Android remain explicit design
and verification targets.

Carrier network access is a narrow host provider beside, not inside,
packet-device ownership. It receives the configured path and typed client-group
and path ordinals for both endpoint resolution and raw socket construction,
allowing platform hosts to keep DNS and connect on one source network or Android
`Network`. TCP and QUIC retain separate address-attempt and handshake algorithms
after that shared boundary. No OS type or branch enters model or scheduler state.

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
6. Complete the peer request product owner: exact flights, outstanding window,
   startup, ordered Service identity, exclusive ACK-clock operation, and one
   exact-instance subflow aggregate now live in `stream/request`; keep it
   lock-free under the client relay task.
7. Keep separate TCP/QUIC capacity controllers as substantive request children.
   Request scheduling now consumes immutable observations and returns
   exact-instance decisions from `sender/request/scheduling.rs`; keep it intact
   rather than splitting policy phases into shallow files. Remaining work moves
   attach/fail orchestration to relay ownership and extracts only substantive
   request evidence, dispatch, or repair capabilities from the facade.
8. Give response planning coherent observe, decide, typed-intent, and atomic
   apply contracts. These are phase APIs, not mandatory files: keep them inside
   admission, selection, handoff, or dispatch until one phase owns an
   independently meaningful state machine. Make preview side-effect free and
   move pure capacity and placement arithmetic to model/scheduler owners without
   changing values.
9. Split shared path state and command code by identity, health, capacity
   transactions, load, queue, and writer ownership without merging TCP and QUIC
   controllers.
10. Complete client/control relay orchestration after its lower-level utilities
    have moved out; client durable state and peer data/ACK application now live
    in `relay/client.rs`, and the server target-relay lifecycle is already owned.
11. Replace repeated `mod.rs` facades, production glob imports, runtime prelude
    leakage, and broad re-exports with named contracts.
12. Move remaining inline tests, update `ARCHITECTURE.md` to the final paths,
    then run the deferred test, target, Wine, and lab verification matrix.

The endpoint semantic pass also removed unowned version 1 wire state: ingress
and outbound copies on open frames, inline stream flags, weighted demand
snapshots, path-policy echoes, and frames with no producer or transition.
Endpoint-local policy and live measurements now stay with their actual owners.

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
