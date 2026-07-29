# MPTUNNEL v0.1.2 Performance/Core capability and evidence map

Status: Core implemented; Core-frozen candidate guards recorded; tagged-binary
and formal runtime acceptance incomplete

Normative authority: [`RFC.md`](../RFC.md)

Product companion: [`PRODUCT_PLAN.md`](PRODUCT_PLAN.md)

Evidence companions: [`PERFORMANCE.md`](PERFORMANCE.md) and
[`LAB.md`](LAB.md)

The evidence companions record the method, retained historical evidence, and
current measurements. The Core-frozen no-feature protocol-v4 candidate has
targeted representative guards, but later Product-only work changes the full
binary identity. A tagged-binary guard and the fixed repeated matched matrix
remain incomplete; these documents therefore make no broad competitiveness
claim.

## 1. Authority and frozen model

`RFC.md` is authoritative for MPP wire behavior, ownership, scheduling,
recovery, datagrams, migration, security, and platform neutrality. If code,
tests, this document, or a lab assumption conflicts with the RFC, the non-RFC
artifact is wrong.

The v0.1.2 performance model is frozen. Performance work MUST NOT change:

- service quanta, startup windows, evidence sample floors, ordering or flight
  envelopes;
- RTO/PTO derivation, recovery multipliers, stale-path thresholds, retry
  timing, handoff timing, keep-alive timing, or authentication timing;
- available/backup eligibility, completion ranking, admission, reinjection, or
  failover rules;
- datagram attempt count, TTL behavior, fragmentation contract, or reassembly
  lifetime;
- resource-limit defaults, extra-traffic defaults, BBR gains, pacing geometry,
  or congestion-controller behavior; or
- lab topology, netem profiles, load duration, fault timing, accounting,
  competitor configuration, or sampling rules.

These values are not an optimization search space. The stable model adapts to
arbitrary Internet conditions only through typed live observations: RTT,
variation, jitter, locally sourced delivery evidence, native queue and flight
state, loss/ECN when known, MPP Data ACK progress, confidence, freshness, and
current demand. No rule may inspect an operating system, interface name,
endpoint, path ordinal, benchmark case, configured laboratory rate, or topology
label.

A user-configured hard resource envelope remains valid Product input.
Competitive evidence uses one declared canonical configuration for candidate
and controls; configuration is never changed per row to improve a result.

Protocol v4 is final for this release:

- TCP uses TLS 1.3 and negotiates no ALPN.
- QUIC negotiates standard `h3`; each carrier stream is an ordinary
  full-duplex HTTP/3 `POST /`. HTTPS authority equals negotiated SNI, so QUIC
  path groups use a DNS TLS identity even when carrier endpoints are literal
  IP addresses.
- Reliable MPP frames use HTTP/3 DATA.
- `DGRAM_DATA` uses the RFC 9297 HTTP Datagram extension after encrypted
  per-request opt-in.
- The evidence identity is
  `tcp-tls13-no-alpn+quic-h3-post-data-rfc9297`.
- Versions 1 through 3 and every superseded carrier presentation are rejected.
  There is no compatibility codec, private ALPN, draft listener, or custom
  TLS-like protocol.

## 2. Product/Core isolation

Performance/Core owns only the MPP session and data plane:

| Core owns | Product owns |
| --- | --- |
| MPP v4 codec, authentication transcript, identities, and bounds | CLI, TOML, API, persistence, and reload |
| Stream offsets, Data ACK, shared receive window, FIN, reset, and detach | Routing, DNS, ingress, and destination policy |
| Datagram identity, TTL, feedback, retries, and reassembly | Outbound and independent-server selection |
| TCP and QUIC carrier adapters and typed observations | Balancer probes, circuits, stickiness, and new-flow retry |
| Within-session path scheduling, aggregation, and recovery | TUN/VPN host policy and platform service integration |
| Exact carrier, attachment, and model-generation fences | Credentials, principals, and Product admission policy |
| Core diagnostics and controlled performance evidence | Dashboard and operator presentation |

Product selects exactly one outbound leaf or MPP session for a new flow. Core
may then select only carriers belonging to that session. A Product balancer
never receives path IDs, congestion state, queue state, Data ACK state, or
reinjection state, and Core never receives route configuration, DNS objects,
balancer scores, UI state, or platform policy.

The boundary carries a normalized protocol target, demand, immutable admission
permit, hard resource envelope, and prepared carrier capabilities. Core
returns flow outcomes, bounded diagnostics, and coarse session readiness.
Diagnostics never become scheduling authority.

Full-stack proxy, TUN, and platform cases are cross-track integration evidence;
they do not transfer Product ownership into Core.

## 3. Required invariants

The implementation preserves every invariant in RFC Section 16:

1. One data byte keeps one connection-level sequence identity across all
   copies.
2. Only MPP Data ACK releases retained MPP ranges.
3. Duplicate delivery never creates duplicate rate evidence.
4. Each direction has one shared receive window across all attachments.
   Attachment acceptance is credit-neutral and never recomputes that window
   from a carrier or demand class.
5. TCP and QUIC retain independent native congestion control and recovery.
6. Available paths are considered before backup paths.
7. Path usage, local health, and application demand remain independent.
8. Numeric path IDs never replace physical carrier-instance identity.
9. Scheduling observes immutable state and revalidates before commit.
10. Reinjection is evidence-driven, bounded, and never creates new offsets.
11. No fixed attachment role determines later placement.
12. Optional platform telemetry is never a correctness dependency.
13. Carrier-instance and stream-attachment lifetimes use separate fences.
14. A datagram keeps one directional identity across retries and executes at
    the target at most once while retained.

These invariants are not optimization candidates.

## 4. Implemented v0.1.2 Core

### 4.1 Reliable data plane

- Request and response directions own independent offset, Data ACK, window,
  FIN, reorder, retained-range, and flight state.
- `STREAM_MAX_DATA` grants offsets but proves no delivery; `STREAM_ACK` proves
  data-level delivery but grants no offsets.
- Out-of-order and duplicate copies are bounded and reassembled once in
  sequence.
- Original and reinjected flights retain exact range and attachment/output
  identities.
- Observe/decide/apply transactions revalidate carrier instance, attachment
  incarnation, output incarnation, model generation, frontier, proof, and
  queue credit.
- Queue, flight, load, measurement, registry, and resource claims reconcile
  exactly once on success, failure, timeout, or cancellation.
- Neutral attachments allow the same logical stream to use TCP, QUIC, or both
  without assigning primary or repair roles.
- At zero live attachments, break-before-make retention preserves the logical
  stream, stops new source reads, rotates reconnect attempts, and obeys one
  absolute retention deadline.

### 4.2 Aggregation and scheduling

- TCP-only, QUIC-only, and mixed TCP+QUIC carrier sets use one carrier-neutral
  scheduling contract. Mixed transport is not a third transport.
- Eligibility removes failed, draining, stale, flow-control-blocked, and
  enqueue-blocked candidates.
- Schedulable available paths form the first set; backup paths are considered
  only when that set is empty.
- Immutable snapshots carry exact identity, health, usage, demand, RTT,
  variation, jitter, delivery/pacing evidence, queue, native flight, MPP
  flight, loss/ECN, confidence, freshness, and active load.
- Completion ranking does not add overlapping carrier and MPP backlog as
  independent debt.
- The contiguous frontier owner remains bounded by shared flow control,
  Product resource envelopes, enqueue capacity, and its native controller.
- An additional unproven output receives only one bounded startup flight.
- Mature additional-path placement requires durable, unambiguous
  original-data Data ACK coverage.
- A path joins bulk work only when its modeled completion contribution exceeds
  its ordering and queue cost; a poor path is not striped merely because it
  exists.
- Demand changes the completion horizon, not the permanent role of a path.
- TCP and QUIC remain natively paced. MPP creates no aggregate congestion
  window and no second packet-recovery loop.

### 4.3 Evidence and estimation

- TCP observations use receiver-confirmed capacity receipts and optional
  telemetry from the exact socket.
- QUIC observations use Quinn packet-ACK-derived delivery, flight, loss,
  pacing, and congestion-window evidence.
- MPP Data ACK remains the only data-level delivery authority.
- App-limited, untimed, compressed, ambiguous, peer-supplied, stale-instance,
  or insufficient-volume samples cannot mint mature placement rights.
- Peer metrics are bounded advisory hints and are replaced by local evidence.
- Optional Linux/Android, Windows, and macOS TCP telemetry is normalized field
  by field. Missing fields remain unknown.
- Missing native telemetry selects the same portable model; it never changes
  eligibility or selects an operating-system policy.

### 4.4 Reliable recovery and failover

- Failure of the exact original carrier instance permits immediate bounded
  repair on a live alternative.
- A complete Data ACK may establish a gap; a partial ACK alone may not infer
  omitted ranges.
- Later ACK evidence uses the RFC's fixed TCP RACK or QUIC time threshold only
  when an alternative can beat owner recovery.
- ACK silence waits the original carrier's RTO/PTO.
- A contiguous tail receives at most one bounded probe per owner recovery
  interval without progress.
- New request placement excludes a non-progressing owner after the RFC's fixed
  stale threshold while native carrier recovery continues.
- Reinjection covers retained unacknowledged ranges only and suppresses
  overlapping queued copies.
- Ordinary repair consumes the cumulative extra-traffic ledger. A critical
  failure, authoritative gap, or tail event may use only its one bounded
  exception quantum, which remains charged to the ledger.
- Measured survivors are preferred, but an authenticated live survivor
  preserves liveness when no measured alternative exists.
- Established streams can reattach across TCP and QUIC after an outage without
  changing stream identity or replaying acknowledged bytes.

### 4.5 Datagrams

- Datagram identity is `(session, flow, direction, datagram_id)` and remains
  unchanged across TCP/QUIC attempts.
- TCP and QUIC selection uses their own observations and the remaining
  absolute TTL.
- Before feedback, a request may make at most the RFC-defined two attempts on
  distinct configured paths.
- Matching request feedback forbids later replay because target processing may
  already have begun.
- The receiver forwards a retained request identity to the target at most once
  and keeps bounded response replay state.
- QUIC payloads use native RFC 9297 datagrams; reliable HTTP/3 DATA carries
  flow open, close, and feedback.
- The Quarter Stream ID binds native data to its authenticated request stream.
- Fragmentation uses Quinn's current maximum datagram size, the fixed 29-byte
  MPP envelope, and at most 64 fragments.
- Zero-length datagrams, pre-open overtaking, duplicate fragments, malformed
  metadata, incomplete reassembly, expiry, route count, packet count, flow
  count, reassembly count, and retained bytes are bounded.
- TCP carriage remains a UDP-blocked fallback; a lost QUIC datagram cannot
  head-of-line block unrelated flows on a reliable carrier stream.

### 4.6 Path lifecycle and migration

Three distinct mechanisms are implemented and must not be conflated:

- Logical stream failover reattaches one retained MPP stream to a surviving or
  reconnected carrier.
- A same-address IPv4 NAT port rebinding follows Quinn's existing state-clone
  path.
- A genuinely new QUIC network path starts fresh RTT, MTU, congestion,
  delivery-rate, confidence, and bulk-proof state.

The Quinn integration retains one stable connection telemetry owner while
allocating a new monotonic `path_epoch` and independent path telemetry for a
new network path. Delayed callbacks from the retired epoch cannot acknowledge
current delivery evidence or authorize current placement. The runtime
estimator discards old rate, confidence, pending sample, and bulk-proof
authority when that epoch changes.

This implements the RFC migration ownership model. It does not, by itself,
prove native Windows, macOS, or Android Wi-Fi/cellular handover; those remain
separate platform evidence cells.

### 4.7 Bounds and portability

- Frame, payload, ACK-range, path, stream, HTTP/3 request, receive-window,
  retained-range, reorder, repair, datagram, native route, reassembly, queue,
  flight, command, authentication, and global admission state are bounded.
- Resource limits are enforced before allocation or durable registration
  where possible.
- Platform-specific code is confined to socket, packet-device, route, VPN,
  protection/binding, and optional telemetry adapters.
- Core scheduling contains no operating-system, interface, route, DNS, or
  laboratory-condition branch.

## 5. Exact Quinn patch

`crates/quinn-proto/` is the complete crates.io source of `quinn-proto`
0.11.15 with registry SHA-256:

```text
4fcb935c5bec503c2f0e306bdd3e58bb9029dcb14fa8d9ac76e3a5256ac0763e
```

The root manifest requires exactly `=0.11.15` and overrides it with this path.
It is the only package under `crates/`.

The production semantic delta is limited to:

- `src/congestion.rs`
- `src/congestion/bbr/mod.rs`
- `src/congestion/bbr/bw_estimation.rs`
- `src/connection/mod.rs`
- `src/connection/packet_builder.rs`
- `src/connection/spaces.rs`
- `src/connection/pacing.rs`
- `src/connection/paths.rs`

The patch:

- stores a compact send-time delivery snapshot on each ACK-eliciting packet;
- carries that snapshot through ACK/loss processing;
- derives BBR delivery rate from the slower of send and ACK clocks;
- excludes lower app-limited samples and updates once per ACK batch;
- supplies corrected sample and packet identity to startup, round, recovery,
  ACK aggregation, and window logic;
- publishes BBR's gain-adjusted rate to Quinn's real token-bucket pacer;
- preserves fractional refill time and bounded burst capacity;
- starts minimum-RTT age from the first real sample and does not expire an
  absent timestamp; and
- adds an opt-in fresh-network-path controller hook while preserving upstream
  factory fallback and NAT-rebinding cloning.

MPTUNNEL's `InstrumentedController` in
`src/transport/quic/congestion.rs` uses that hook to keep
connection-scoped telemetry but isolate every genuine path epoch. Added BBR,
bandwidth-estimator, pacer, and path-lifecycle tests protect the delta. No
unrelated upstream source differs.

The full mirror is necessary because delivery snapshots, ACK/loss batches,
pacing, and path creation cross private Quinn internals. An upstream refresh
is a Core algorithm change: port the exact delta, run the standalone Quinn and
full MPTUNNEL suites, then reproduce the matched QUIC matrix. It is never
dependency housekeeping.

## 6. Permitted optimization

Only profile-proven implementation work is allowed:

- eliminate avoidable allocations, copies, buffer growth, and formatting;
- reuse bounded buffers without weakening ownership or lifetime fences;
- batch already-ready frames or writes without changing priority, timing,
  ACK/window emission, or cancellation behavior;
- use vectored or platform-batched I/O behind the existing capability
  adapters;
- shorten critical sections or replace hot shared locks with actor-local state
  and immutable snapshots;
- reduce redundant wakeups, polling, task creation, syscalls, and snapshot
  work;
- improve teardown and reclamation without changing deadlines; and
- accept compiler or dependency improvements only with unchanged semantics and
  matched evidence.

Every candidate needs a profile, a causal hypothesis, an
instrumentation-enabled confirmation, and an instrumentation-free adjacent
rerun. Only a proven repeated regression fails and triggers correction or
revert; inconclusive evidence is rerun and never promoted, persisted, or used
as a revert signal. One run and one universal percentage cutoff are never a
performance verdict. In particular, an isolated throughput movement around
five percent is ordinary run-to-run fluctuation unless repeated paired
evidence and causal counters establish otherwise; five percent is neither a
pass margin nor a failure cap.

Forbidden optimization includes:

- tuning any model constant, timer, threshold, gain, queue, window, retry, or
  fault schedule;
- adding topology-, interface-, endpoint-, OS-, carrier-family-, or
  lab-case heuristics;
- using native ACKs as Data ACK, pacing MPP above native transports, or
  synthesizing a congestion window;
- weakening bounds, authentication, encryption, identity fencing, or
  at-most-once behavior;
- changing competitor settings, workload duration, qdisc, accounting, or
  sample exclusion to improve a score; or
- retaining rejected experiments, compatibility paths, or diagnostic-only
  code in the shipped hot path.

The target for v0.1.2 is ordinary non-regression. A theoretically necessary
latency/stability tradeoff still requires a preregistered owner-approved
record with theoretical rationale, Pareto evidence, ablation evidence, and
the observed normalized cost. It has no universal maximum-regression cap, and
no such tradeoff may be manufactured through parameter tuning in this
frozen-model phase.

## 7. Evidence cells

`lab/performance-impact-registry.json` is the exact machine-readable
expansion. Its 29 cells group as follows:

| Group | Cells | Required scope |
| --- | ---: | --- |
| Diagnostic overhead | 1 | Matched feature-off/feature-on |
| Reliable single path | 4 | TCP/QUIC times upload/download across seven fixed profiles |
| Reliable multipath | 6 | TCP/QUIC/mixed times upload/download across homogeneous, heterogeneous, shared-bottleneck, and asymmetric profiles |
| Datagram | 4 | TCP/QUIC times single/multipath, including loss, path maximum, and oversized behavior |
| Mixed loaded | 2 | Single/multipath bulk plus interactive and datagram work |
| Fault and migration | 3 | Reliable upload/download and mixed flapping across drain, close, blackhole, collapse, spike, outage/restore, rebinding, handover, and stale-event profiles |
| TUN | 4 | Single/multipath times upload/download |
| Resource and lifecycle | 2 | Concurrency/overload and startup/idle/reload/churn/shutdown/reconnect |
| Native packaged platforms | 3 | Windows, macOS, and Android |

The protected metric sets are reliable delivery, useful path contribution,
aggregation efficiency, setup and short-flow latency, idle/loaded latency
tails, datagram quality, recovery, wire efficiency, CPU,
memory/allocation, fairness/saturation, lifecycle, full-stack overhead, mobile
behavior, and diagnostic overhead. No composite score may hide a regressed
metric.

Minimum acceptance counts are fixed:

- ordinary runtime cells: at least seven valid adjacent matched pairs;
- fault cells: at least thirty valid triggered events;
- packaged platform cells: at least seven valid pairs and thirty triggered
  events; and
- deterministic quick checks: triage only, never acceptance or veto evidence.

Acceptance reports every adjacent pair, median, spread, and preregistered
two-sided 95% paired-bootstrap interval against both the immediate accepted
parent and the historical champion. After metric direction is normalized, an
interval wholly above zero proves improvement, one wholly below zero proves
regression, and any interval containing zero is `INCONCLUSIVE`. There is no
universal percentage, per-cell speed, absolute noninferiority, or
maximum-regression margin.

`PASS` requires at least one proven intended improvement and no proven
regression; exact all-zero deterministic equivalence may also pass. Champion
promotion requires proven improvement over the champion and no proven
regression. `FAIL` means proven repeated regression. `INCONCLUSIVE` is rerun
and is never persisted, promoted, or reverted. An approved theoretical
latency/stability tradeoff may accept a proven regression as the new parent,
but never replaces the historical champion.

## 8. Matched competitor method

The fixed reference cohort is:

- direct TCP and direct QUIC ceilings;
- pinned Xray/VMess `v26.3.27` for the V2Ray-family TCP proxy comparison;
- pinned Hysteria2 `app/v2.10.0` for QUIC reliable and datagram comparison;
- Linux MPTCP only when actual subflow use is captured;
- a Multipath-QUIC implementation only after its exact source/binary is pinned
  and actual path use is captured;
- matched MPTUNNEL single-path rows as the aggregation denominator; and
- parallel direct flows as the available multi-connection ceiling.

Every comparison keeps source commit, release profile, features, security,
MTU, topology, qdisc, direction, workload, concurrency, duration, fault
schedule, CPU/memory constraints, and accounting identical where protocol
design permits. Controls and candidate run adjacently on the same valid host
snapshot. External executable hashes, versions, archive checksums, product
binary hashes, configuration hashes, topology, and raw artifacts are retained.

Receiver-delivered unique bytes are authoritative. Upload requires finalized
target accounting. Client `send()` success, carrier writes, nominal path-rate
sums, and interface counters alone are not delivered goodput. Multipath claims
require material per-path contribution; control/proof traffic is not
aggregation.

Cohorts remain separate:

- shaped, unconstrained, fault, and real Internet;
- TCP, QUIC, and mixed;
- upload and download;
- reliable, datagram, loaded mixed, TUN, and lifecycle;
- native platform and Wine compatibility execution; and
- diagnostics-enabled causality and instrumentation-free acceptance.

The lab's fixed profiles validate the same model under different conditions.
They are not a parameter sweep and never feed production constants.

## 9. Completion and claim limits

Implementation tests prove correctness and bounds; they do not prove
throughput or competitiveness. A successful runner row proves completion of
one declared case; it does not prove superiority. Historical v0.1.1 rows use
an incompatible protocol and are references only.

The current Core-frozen candidate guard identity is release profile, no
optional features, GNU/Linux x86_64 binary SHA-256
`d46eaf6a530c57cbe8802d6d9574c5b0afb406c65df5717e724e775e68e2374e`
and build-input manifest SHA-256
`12caf8a531e8c7175f45e1f0343f1235c4433977d9d495eb2e5854731a1640f4`.
On the five-equal-path, two-flow guard it recorded complete TCP and QUIC
downloads at 799.384 and 712.382 Mbps. The canonical QUIC upload delivered a
747.305 Mbps receiver-confirmed lower bound with all five paths material and a
1.027% one-second-drain tail; its ten-second diagnostic confirmed every byte.
The canonical TCP upload delivered a 559.969 Mbps receiver-confirmed lower
bound with a 0.515% tail; its diagnostic also confirmed every byte, while
approximately 98% of carrier traffic remained on two sustained owner paths.

Those rows protect the shared-credit fix and representative aggregation
behavior. Their dirty source snapshots make them descriptive rather than
formal acceptance evidence. Later Product-only routing, logging, and doctor
changes leave RFC/Core semantics unchanged but make this a different full
binary from the eventual tag. The cohort contains no adjacent competitor or
tagged-binary fault control, so it does not prove broad competitiveness or
transfer historical failover results to protocol v4. Exact rows, path shares,
identities, and host limitations are recorded in `docs/PERFORMANCE.md`.

MPTUNNEL may claim only what fresh protocol-v4 rows establish:

- one topology cannot support "faster on every Internet";
- shaped Docker is not the public Internet;
- Wine proves the portable Windows executable path, not native Windows
  kernel, Wintun, or `SIO_TCP_INFO`;
- cross-compilation proves buildability, not native performance;
- aggregate interface bytes do not prove exact wire expansion;
- an unavailable competitor or Multipath-QUIC implementation is missing
  evidence, not a pass;
- native mobile handover remains unproven until its platform cell runs;
- a competitor win cannot excuse regression against the accepted parent or
  champion.

Core is release-complete only when:

1. RFC invariants and resource bounds pass the complete deterministic suite.
2. The standalone pinned Quinn suite passes.
3. Fresh v4 matched evidence covers every affected cell at its fixed minimum
   count.
4. Candidate, parent, champion, direct, and applicable external references are
   complete.
5. Receiver delivery, latency, recovery, path use, traffic, CPU, and memory
   agree on the result.
6. Every cell is `PASS` or has an explicit approved latency/stability
   tradeoff.
7. Missing evidence and platform limitations are disclosed without
   substitution.
8. `docs/PERFORMANCE.md` identifies exact measured binaries and states only
   scoped conclusions.
9. No stale experiment, rejected optimization, draft protocol path, or
   unexplained regression remains.

Until those conditions hold, the correct status is "implemented; release
evidence incomplete," never "competitive everywhere" or "faster than V2Ray,
Hysteria, MPTCP, or Multipath QUIC."
