# MPTUNNEL progress

This is the authoritative execution and milestone ledger for the repository.
Entries record verified work, evidence, open blockers, and the next bounded
action. `docs/PRODUCT_PLAN.md` defines Product scope and acceptance;
`docs/PERFORMANCE_PLAN.md` defines Core performance methodology and acceptance.
Neither plan is a progress log, and status text in either plan does not
supersede this file.

Historical entries below are retained as evidence of the decisions made at
their recorded time. When a later entry changes an earlier decision, the later
entry is authoritative.

## 2026-07-30T04:23:01+08:00: demand-driven application-target DNS

- Name: separate target representation, routing evidence, and resolver
  transport
- Category: Product routing, destination authorization, DNS, outbound, and
  balancer semantics
- State: implementation and complete Product gate passed; unchanged-Core
  performance guard is the next bounded action
- Content:
  - retained one immutable canonical domain as the flow and routing identity;
  - made ordered routing and destination ACL classification request address
    evidence only when an applicable IP/post-resolution rule can change the
    first match;
  - preserved domains through MPP, SOCKS5, HTTP CONNECT, HTTPS CONNECT, and
    SOCKS5 UDP when no address evidence is required;
  - made direct and source-bound leaves resolve through the selected Product
    DNS plan, whose upstream transport may be system, direct, or routed;
  - kept target resolution independent from proxy-control and carrier
    bootstrap resolution;
  - authorized every address in one complete answer and made balancer
    promotion from domain to authorized literals one-way across retries;
  - preserved Reject and Drop when a provisional pre-resolution decision
    becomes terminal after DNS, including a retained silent-discard lane for
    denied UDP associations; and
  - excluded domain-only signed destination sets from address demand because
    they cannot match an IP answer.
- Correctness boundary:
  - stable domain policy does not query DNS or depend on its availability;
  - explicit IP policy fails closed through the selected DNS plan and sends
    only authorized literals to later domain-capable members;
  - a delegated proxy is an explicit target-resolution trust boundary, while
    MPP repeats the final-egress decision on the receiving node; and
  - no scheduler, congestion, pacing, recovery, carrier, wire protocol,
    transport timing, or Core resource default changed.
- Evidence:
  - warnings-denied all-target/all-feature Clippy, formatting, and whitespace
    checks passed;
  - the complete Rust suite passed: 1,385 library tests, two allocation
    contracts, all four packaged daily-use acceptance tests, and doctests;
  - durable scenarios cover stable delegation without DNS, earlier IP rules,
    provisional terminal routes, complete-answer authorization, domain-only
    signed sets, routed proxy literals, irreversible balancer promotion, and
    silent UDP denial across repeated datagrams; and
  - an independent final semantic review reported no remaining material
    correctness or performance blocker in this Product slice.
- Decision:
  - accept the Product semantics and keep the restored Core unchanged;
  - require a clean-source representative performance guard before advancing
    to the next Product capability; and
  - retain `PROGRESS.md` as the sole execution ledger.
- Next: commit this isolated Product milestone, run the standard four-case
  unchanged-Core performance guard, record the comparable evidence, and then
  continue the remaining global Product map.

## 2026-07-30T03:15:13+08:00: canonical Product configuration identities

- Name: establish one strict public identity and cross-reference vocabulary
- Category: Product configuration, management API, operations, and presentation
- State: committed and approved; complete Product and affected performance
  guards passed; the broader Product map remains open
- Source: clean commit `9a289e7c8b116772c448a3a0e6b0c7173453c924`
- Content:
  - made `name` the explicit operator-owned configuration identity, `*_id`
    protocol/credential/runtime identity, and `endpoint` the network
    listener/connector/carrier address;
  - replaced ambiguous egress references with typed `outbound` or `balancer`
    fields and stable named MPP paths, peers, inbounds, DNS plans, principals,
    credentials, publishers, and rule sets;
  - removed anonymous path indices and implicit MPP egress ownership from
    mutable Product surfaces without compatibility aliases;
  - aligned the strict TOML model, simple CLI, runtime projection, management
    API v2, dashboard, examples, public operations documentation, and lab
    configuration generation; and
  - confined runtime changes to Product metadata and lookup boundaries. No
    scheduler, congestion, pacing, recovery, carrier, protocol, timing, or
    transport-resource behavior changed.
- Product evidence:
  - the complete suite passed: 1,380 library tests, two allocation tests, and
    all four daily-use Product acceptance tests;
  - warnings-denied all-target/all-feature Clippy, formatting, whitespace,
    dashboard JavaScript syntax, shell syntax, and all 18 durable lab-runner
    contract tests passed;
  - an independent final static audit found no stale live v1 management
    endpoint, gateway collection, generic Product selector, index-based
    mutation, ambiguous cross-reference, Core hot-path drift, or material test
    weakening; and
  - the intentional `/api/v1/status` rejection test and internal/native uses of
    `selector`, `configured_index`, and `gateway` remain outside the public
    Product vocabulary.
- Performance boundary and evidence:
  - the native Linux release binary is SHA-256
    `ed931fec4b5dc9f96bbbd9235524c87a2817c0a8f385d642cef5955094f96bd7`;
  - with a clean source, valid host, identical client/server binary,
    diagnostics disabled, isolated cases, 20-second load, and two flows,
    single/equal-fat QUIC download measured `252.939`/`733.331` Mbps and
    upload measured `241.272`/`800.008` Mbps;
  - the unchanged adjacent single-upload repeat measured `243.512` Mbps;
  - all cited rows are performance-comparable and remain on the accepted
    historical plateau;
  - the single-upload duration workload retained valid target accounting but
    was labelled `loss` because its streams had not emitted terminal closure
    within the unchanged one-second drain boundary. The same label exists on
    accepted `228.443`, `231.957`, and `232.781` Mbps historical rows, so it
    is not a regression signal and no timeout or Core behavior was changed;
    and
  - raw evidence remains under ignored
    `./.tmp/lab/results/product-config-v2-20260730/` and
    `./.tmp/lab/results/product-config-v2-20260730-single-upload-repeat/`.
- Decision:
  - approve the canonical vocabulary slice with no observed performance
    downgrade;
  - retain one clean breaking configuration model with no compatibility
    aliases; and
  - keep the unchanged restored Core as the performance authority.
- Next: implement demand-driven application-target DNS as an isolated Product
  slice: preserve the canonical domain through MPP, SOCKS5, and HTTP CONNECT;
  resolve only when IP routing, destination authorization, or the selected
  native/interface egress requires an address; keep carrier/bootstrap DNS
  separate; and retain receiving-end authorization without changing Core.

## 2026-07-30T02:07:54+08:00: transactional operator observability

- Name: establish production logging and safe live configuration transactions
- Category: Product observability, configuration, and operations
- State: committed and approved; Product acceptance and the affected
  performance guard passed; the broader Product map remains open
- Source: clean commit `529971ff16986d87c1826dd977c457c2c07f9311`
- Content:
  - added one typed `[logging]` model for CLI, TOML, environment, file, and
    console output with reloadable level/format/sink configuration;
  - made opt-in Product flow open/close records sanitized and sink-generation
    paired without adding Core or per-payload logging;
  - serialized configuration compare-and-swap, pending persistence, logger
    preparation, activation, rollback, and supervised restart so one
    generation cannot overwrite another;
  - protected the canonical configuration and sidecars from lexical,
    canonical, symbolic-link, hard-link, and time-of-check/time-of-use output
    aliasing, including operational CLI commands;
  - made DNS capability ordering deterministic; and
  - made dashboard credentials persist in local storage only after
    current-generation status and health validation, with stale-response
    guards and explicit erasure.
- Product evidence:
  - the complete suite passed: 1,381 library tests, two allocation tests, and
    all four daily-use Product acceptance tests;
  - warnings-denied all-target/all-feature Clippy, formatting, shell and
    JavaScript syntax, and whitespace checks passed;
  - a real-browser run proved invalid credentials are never stored, valid
    credentials survive reload, no credential appears in the URL or document,
    Forget clears both current and legacy stores, and a delayed stale `401`
    cannot erase a newer authenticated generation; and
  - independent transaction, hot-path, dashboard, and public-documentation
    audits found no remaining concrete defect in this slice.
- Performance boundary and evidence:
  - no scheduler, congestion, pacing, recovery, carrier, protocol, timing, or
    resource-default behavior changed; disabled flow logging adds only
    lifecycle-level atomic reads outside normal payload processing;
  - the clean native Linux release binary measured here is SHA-256
    `4ee348bb5cb85e7abfca58a1f5e6d1e8df0b7bee95f09e185bbdbfd00db43f7a`;
  - with the retained clean-host Docker topology, 20-second load, two flows,
    diagnostics disabled, and isolated cases, single/five-path QUIC download
    measured `220.899`/`754.154` Mbps and upload measured
    `232.781`/`775.025` Mbps;
  - an adjacent repeat using the identical binary resolved the lower
    single-path sample and duration-bound five-path receiver closure at
    `249.676` Mbps download and `796.663` Mbps upload, both complete and
    `ok`;
  - the immediately preceding corrected-protocol cohort measured
    `241.442`/`738.897` Mbps download and `231.957`/`762.933` Mbps upload,
    while the restored champion measured `234.276`/`680.359` and
    `228.443`/`762.552` Mbps respectively; and
  - every cited row is performance-comparable with a valid clean source and
    matching client/server hashes. Raw evidence remains under ignored
    `./.tmp/lab/results/product-observability-20260730/` and
    `./.tmp/lab/results/product-observability-repeat-20260730/`.
- Decision:
  - approve this Product-only slice with no observed affected performance
    downgrade;
  - treat the first lower single-path sample and duration-bound receiver
    closure as measured run variation, not as a tuning signal; and
  - retain the unchanged Core as the performance authority while completing
    the remaining Product scope.
- Next: establish one clean, breaking public configuration vocabulary, then
  continue the remaining daily-use routing, DNS, balancer, inbound/outbound,
  VPN, and operator gaps before the full performance and transition matrix.

## 2026-07-30T00:34:43+08:00: direct version 4 conformance corrections

- Name: admit only independently reproduced protocol corrections
- Category: protocol correctness, security, lifecycle, and performance
  change control
- State: seven isolated correctness corrections approved; affected
  steady-state non-regression guard passed; the formal repeated performance
  matrix remains open
- Content:
  - reproduced and corrected zero-port IP target encoding, non-canonical
    `PEER_STATUS` error paths, and overflowing `STREAM_DATA` extents;
  - made malformed RFC 9297 Quarter Stream IDs close HTTP/3 with
    `H3_DATAGRAM_ERROR`;
  - rejected HTTP Datagrams after request-send completion and retired closed
    request associations without retaining late datagrams as future-stream
    handoff;
  - committed QUIC carrier registry admission before publishing readiness,
    with cancellation-safe rollback; and
  - made no congestion-controller, scheduler, recovery, timing, pacing,
    resource-default, or path-policy change.
- Correctness evidence:
  - every issue first failed a persistent codec, real HTTP/3 loopback, or
    runtime admission test on the restored champion and then passed after its
    isolated correction;
  - the complete current suite passed: 1,378 library tests, two Product
    allocation tests, and four daily-use Product acceptance tests;
  - warnings-denied all-target/all-feature Clippy, formatting, and whitespace
    checks passed; and
  - commits `d3575a8` through `4afcc51` retain each correction separately.
- Performance boundary:
  - the only new operation on every reliable data frame is one checked extent
    calculation on encode and decode; it adds no allocation or lock;
  - native HTTP Datagram active routing retains its existing lock, lookup, and
    queue operation, while new lifecycle work occurs on malformed, absent, or
    terminal associations;
  - QUIC admission reorders existing setup work without adding a frame or
    round trip; and
  - the release binary measured here is SHA-256
    `f1ea26a70fef4c82d483dd83fff13dd09265b76b0b853b0c9c1e205fe89f53a0`.
- Affected non-regression evidence:
  - under the same native Linux target, Rust/Cargo toolchain, Docker images,
    clean-host validity rules, 20-second load, two flows, and netem profiles
    as the restored champion, QUIC single/five-path download measured
    `241.442`/`738.897` Mbps and upload measured
    `231.957`/`762.933` Mbps;
  - the matched restored values were `234.276`/`680.359` Mbps download and
    `228.443`/`762.552` Mbps upload;
  - all four rows are performance-comparable, the source tree was clean, and
    client/server hashes match; and
  - raw manifests, qdisc state, counters, and rows remain under ignored
    `./.tmp/lab/results/correctness-conformance-20260730/`.
- Decision:
  - approve these changes for proven protocol correctness with no observed
    affected steady-state downgrade;
  - do not attribute ordinary run variation to the corrections and do not use
    a fixed percentage as a promotion threshold; and
  - keep all other later Core, migration, TCP expansion, congestion, and
    recovery work rejected until it independently reproduces a defect or
    demonstrates a gain against both its parent and the historical champion.
- Next: close the highest-value Product observability gap without entering
  Core hot paths, then resume the preregistered representative Core condition
  and transition matrix before considering any new performance mechanism.

## 2026-07-29T23:22:09+08:00: historical performance restoration

- Name: restore the last clean competitive Core before further development
- Category: performance, recovery, and change control
- State: clean champion source and binary restored; representative steady-state
  gate passed; full transition and condition matrix remains required
- Source: exact clean commit `a5a6094fc8e07456b057ddc107a0d51849d42d10`
- Binary: release SHA-256
  `9a66861a08c0922b432cb73963bc0da0304023b88be4409aeb383397a3166d92`
- Content:
  - stopped the unverified BBR2, congestion-epoch migration, TCP expansion,
    scheduler, and related Core work;
  - preserved the complete pre-restoration working state as recoverable stash
    `0e1f6ca0c945e19743371967cbeb2abf8effc58a`;
  - rebuilt the exact clean champion and compared it adjacently with the dirty
    tree using the same Linux Docker topology, 20-second load interval, two
    application flows, and single/five-equal-fat QUIC carrier cases; and
  - retained all raw generated evidence under ignored `./.tmp/lab/results/`.
- Evidence:
  - the dirty tree delivered `260.762` Mbps single-path download and `259.919`
    Mbps single-path upload, but only `337.920` Mbps five-path download and
    `575.262` Mbps five-path upload;
  - the restored champion delivered `234.276` Mbps single-path download and
    `228.443` Mbps single-path upload, plus `680.359` Mbps five-path download
    and `762.552` Mbps five-path upload;
  - an adjacent tagged control delivered `241.869`, `245.267`, `702.349`, and
    `785.376` Mbps respectively;
  - the former tree therefore had a reproduced multipath regression of about
    52% download and 27% upload, while the restored rows returned to the
    historical range.
- Decision:
  - the restored clean tree is the only active performance authority;
  - no dirty checkpoint, controller replacement, parameter change, migration
    refactor, or RFC-driven implementation change may return as a group;
  - each proposed change must be isolated, causally justified, and compared
    against both its immediate parent and this champion across every affected
    steady-state and transition cell before promotion; and
  - ordinary run variation is interpreted from repeated measurements rather
    than a strict fixed percentage cap.
- Next: run the representative TCP, QUIC, and mixed steady/transition matrix on
  the restored champion, then establish the clean RFC without changing this
  proven behavior.

## 2026-07-29T23:35:44+08:00: restored transition survival guard

- Name: current-version balanced blackhole guard on the restored champion
- Category: performance, failover, and evidence boundary
- State: representative survival guard passed; transition-performance
  acceptance remains open
- Content:
  - ran the existing TCP download, TCP upload, and mixed-traffic balanced-path
    blackhole cases once on a clean host with diagnostics disabled;
  - used the exact restored release binary
    `9a66861a08c0922b432cb73963bc0da0304023b88be4409aeb383397a3166d92`;
    and
  - retained the raw manifest, result rows, interface counters, qdisc snapshots,
    and container telemetry under ignored
    `./.tmp/lab/results/restored-champion-transition-20260729/`.
- Evidence:
  - TCP download continued at `240.273` Mbps with a `0.280` second observed
    recovery gap and no failed request;
  - TCP upload delivered a receiver-confirmed lower bound of `211.684` Mbps
    with a `1.876` second observed recovery gap;
  - mixed traffic retained all `40/40` interactive exchanges, observed a
    `0.494` second bulk recovery gap, and delivered `176/178` UDP exchanges;
    and
  - the manifest identifies a clean source tree, a valid host, protocol version
    4, diagnostics disabled, and the exact champion client/server binary.
- Decision:
  - this single guard establishes that the restored implementation survives the
    representative blackhole transitions; it is not a formal recovery-latency
    or throughput distribution and does not approve any later patch;
  - an identified issue or proposed change is only a candidate until its
    failure reproduces against this champion and the isolated change proves a
    correctness or performance gain across every affected guard; and
  - formal fault-cell acceptance still requires the preregistered repeated
    triggered-event cohort. A fixed percentage is neither a tolerance nor a
    promotion rule.
- Next: establish the clean current-version RFC without altering champion
  behavior, then re-investigate later issue candidates individually.

## 2026-07-29T23:58:53+08:00: current-version protocol model established

- Name: standards-shaped MPP version 4 authority
- Category: protocol, Core architecture, security, and change control
- State: documentation complete; focused protocol gate passed; implementation
  conformance defects remain candidates until independently reproduced and
  corrected
- Content:
  - rewrote `./RFC.md` as the current version 4 protocol specification instead
    of a development narrative;
  - separated the MPP data level from native TCP and QUIC authority and from
    Product routing, DNS, gateway balancing, VPN, configuration, and
    presentation;
  - defined session, carrier-instance, attachment-incarnation, offset, Data
    ACK, locator, evidence, demand, and recovery-interval terminology without
    deriving identity from source address or port;
  - retained the exact version 4 frame registry and carrier profiles, including
    TLS 1.3 TCP, HTTP/3 QUIC, RFC 9221 QUIC DATAGRAM, and RFC 9297 HTTP
    Datagram association semantics;
  - specified the active transport-neutral recovery formulas and explicitly
    distinguished them from native TCP RTO, QUIC PTO, RFC 8985 RACK, and RFC
    9002 loss decisions; and
  - repaired the two public cross-references affected by the new stable section
    structure. `./docs/PERFORMANCE_PLAN.md` remains an acceptance-methodology
    document and contains no progress state.
- Evidence:
  - `cargo test --locked protocol`: 41 passed, 0 failed;
  - `git diff --check`: passed;
  - the release binary remains byte-identical to the restored champion at
    SHA-256
    `9a66861a08c0922b432cb73963bc0da0304023b88be4409aeb383397a3166d92`;
    and
  - no runtime source, timing value, congestion controller, scheduler, or wire
    value changed in this milestone.
- Open conformance candidates:
  - malformed RFC 9297 Quarter Stream IDs are silently dropped instead of
    closing the HTTP/3 connection with `H3_DATAGRAM_ERROR`;
  - datagrams associated with an already closed request stream can enter the
    bounded not-yet-created-stream handoff queue;
  - QUIC can publish `SESSION_READY` before carrier-registry admission; and
  - several codec canonicality and arithmetic checks require exact failing
    reproduction before any fix is admitted.
- Decision:
  - the RFC is the protocol and Core-model authority, while this file alone is
    the execution ledger;
  - the identified discrepancies are not approved patches; each must first
    fail a persistent conformance or adversarial test on the restored champion,
    then pass with one isolated correction and the affected non-regression
    guards; and
  - no preserved later Core implementation is restored wholesale.
- Next: reproduce and resolve the smallest directly proven wire-conformance
  defects one at a time, then re-evaluate transition and performance
  candidates against the unchanged champion.

## Product maturity audit history

## 2026-07-26: v0.1.1 global product audit

- Name: competitive product maturity baseline
- Category: architecture, correctness, security, operations, experiments, UX,
  and product roadmap
- State: complete
- Baseline: clean `origin/main` at `692c6f3`; code is public `v0.1.1` at
  `faee89d` and the only later baseline change is the frozen milestone record
- Content: audited `RFC.md`, source/runtime/configuration, inbounds/outbounds,
  routing, DNS, balancing, TUN, management API/dashboard, lab/benchmarks,
  release automation, packaging, security policy, and current official
  V2Fly/Xray/Hysteria product surfaces.
- Result: the multipath transport is a strong functional foundation, but the
  product remains early-alpha for Internet service. The full evidence map,
  findings, competitor matrix, dashboard screenshots, target architecture,
  roadmap and exit gates are in `./docs/PRODUCT_MATURITY_AUDIT.md`.

## 2026-07-26: stale experiment retirement

- Name: rejected private-QUIC calibration cleanup
- Category: experiment integrity and maintainability
- State: complete
- Content: removed the rejected staged handoff case, its two Python files,
  runner knobs/helpers/dispatch, obsolete diagnostic consumers, and stale
  documentation claims. Optional unavailable external baselines now record
  explicit `skipped` evidence without masking product-case failures.
- Evidence: 81 insertions and 1,057 deletions across eight tracked files; 138
  lab tests pass; all lab/script shell files pass syntax and ShellCheck; the
  diagnostic classifier now checks every consumed event against production
  Rust source.
- Decision: rejected experiments are removed rather than retained as permanent
  skip-only cases. Accepted experiments retain exact identity, bounded cohorts,
  raw review evidence and adjacent correctness guards.

## 2026-07-26: verification

- Name: audit quality gate
- Category: reproducibility
- State: passed within documented scope
- Evidence:
  - `cargo fmt --all -- --check`
  - `cargo test --locked --all-features`: 982 passed
  - warnings-denied all-target/all-feature clippy
  - lab Python suite: 138 passed
  - benchmark crate: 4 passed
  - shell syntax and ShellCheck
  - release-version self-test
  - `git diff --check`
  - current dashboard desktop/mobile screenshots
- Limits: no Docker performance rerun, real-Internet/native-platform soak,
  independent security audit, dependency advisory scanner, formal WCAG audit,
  or reproducible release rebuild was performed.

## 2026-07-26: isolated daily-use execution contract

- Name: Product-first and Performance/Core plans
- Category: architecture, delivery planning, acceptance, and performance
  governance
- State: plan complete; implementation not started
- Content:
  - `./docs/PRODUCT_PLAN.md` is the authoritative Product track for daily-use
    routing, ACL, DNS, outbounds, gateway balancing, TUN/VPN, identity,
    lifecycle, configuration, API, presentation, profiles, and packaging.
  - `./docs/PERFORMANCE_PLAN.md` is the authoritative isolated Core track for
    correctness, native QUIC DATAGRAM, estimation, aggregation, automatic
    failover, mobility, resource optimization, and competitive proof.
  - Product gateway selection and within-session carrier scheduling are
    separate owners with a compile-time dependency boundary and narrow typed
    API.
  - The new schema/API/wire line is a clean break. There are no v0.1 aliases,
    parsers, dual implementations, or runtime migration requirements.
  - `config.toml` is canonical durable desired state and is automatically
    loaded on restart. Simple CLI input and authenticated API mutation share
    one typed configuration/validation engine. Supported persistent API
    changes use revision checks, secure atomic file replacement, immutable
    generation activation, and last-good rollback.
  - Every change is compared with both its immediate parent and retained
    historical champion across goodput, setup/tail latency, UDP, recovery,
    overhead, CPU, memory, fairness, stability, TUN, and supported-platform
    energy/resource behavior.
  - A regression can be accepted only as a preregistered, theoretically
    necessary and measured trade for latency or network stability. Measurement
    tolerance is lab noise, not a regression budget.
- Completion rule: `PRODUCT-CAPABILITY-READY` is not overall completion.
  `DAILY-USE DONE` requires packaged Product acceptance plus repeated single-path,
  aggregation, failover, MPTCP/Multipath-QUIC, V2Ray/Xray, and Hysteria
  performance gates with no open P0/P1 daily-use blocker.

## Next milestone

- Category: shared F0/F1/F2 prerequisite
- Content: first freeze the repeated multidimensional performance ledger; then
  extract the Product/Core packages with identical v3 behavior and prove
  equivalence; then make the clean provisional vNext cut and delete v3; then
  close Core correctness/security/resource blockers. Product P1 begins only on
  that F2-safe engine, before F3-F9 algorithm optimization.

## 2026-07-26: F0 measurement foundation in progress

- Name: multidimensional non-regression constitution
- Category: performance evidence and acceptance safety
- State: implementation in progress; no baseline or completion claim yet
- Implemented:
  - a versioned 29-cell/66-metric impact registry with quick, nightly, and
    release lanes;
  - affected-change declaration and acceptance validation;
  - an append-only parent/champion ledger with direction-normalized paired
    medians, deterministic paired-bootstrap inference, and non-promoting
    latency/stability tradeoff records;
  - deterministic scheduling/failure trace replay outside the shipped binary;
  - immutable-by-digest raw-evidence bundle sealing; and
  - versioned, anonymized host/source/toolchain snapshots integrated into
    manifest v2 and result row v1, with CPU/load/memory/frequency/thermal,
    external-container, compiler, Cargo.lock, and deterministic source-tree
    identity; optional `MPTUNNEL_LAB_REQUIRE_VALID_HOST=1` retains invalid
    evidence and fails closed.
- Evidence: focused declaration, ledger, replay, and bundle tests pass.
- Blocker observed: the shared VM currently has an unrelated multi-core Rust
  build and an unrelated running container. Existing timing checks measurably
  fail under that contention. Runtime rows from this period are deliberately
  not accepted as baseline evidence.
- Next: finish runner/CI/evidence-assembler integration, obtain a clean-host
  seven-pair current champion cohort plus thirty-event fault cohorts, seal and
  register it, then begin F1A.

## 2026-07-26T18:39:58+08:00: F2 authentication freshness and replay admission

- Name: per-decision authentication time and expiry-aware replay state
- Category: Core correctness and security
- State: implementation complete; focused gates passed; workspace-wide gate
  pending concurrent receive-credit integration
- Content:
  - server `SESSION_AUTH` and `PATH_JOIN` verification independently sample
    current wall time instead of inheriting the `SESSION_HELLO` sample;
  - the verified join retains both signed issue time and exact verification
    time, and replay admission uses that same decision timestamp without an
    extra clock syscall;
  - replay state is expiry-indexed, retains entries through the inclusive
    freshness boundary, refuses capacity pressure without evicting live
    entries, and uses a monotonic observed-time high-water mark so clock
    rollback cannot reopen a pruned identity;
  - replay check and insertion remain one operation under the identity
    runtime's existing mutex; and
  - process restart is explicitly the non-persistent replay boundary.
- Evidence: 3 authentication transition tests, 5 replay
  expiry/capacity/concurrency/clock/restart tests, and 1 server-context
  freshness/replay test pass; formatting and diff checks pass.

## 2026-07-26T18:54:52+08:00: Product flow, routing, and destination policy

- Name: first pure `mptunnel-product` policy generation
- Category: Product P1/P2
- State: implementation complete as an independently tested pure package;
  runtime ingress/DNS/outbound wiring remains a later Product milestone
- Content:
  - canonical strict-IDNA domain, authority, non-zero port, IPv4/IPv6, TCP/UDP,
    source, principal, inbound, and interface flow identity;
  - immutable compiled first-match routes covering exact/suffix/keyword/regex
    domains, stage-aware destination CIDR, source CIDR, destination/source
    ports, network, inbound, principal, and interface;
  - typed direct/reject/drop/reset/outbound/balancer, DNS-plan, and traffic
    intent decisions with stable rule IDs and borrowed explanations;
  - shared destination ACL generations with default denial of metadata,
    unspecified, loopback, private/ULA, link-local, and multicast addresses,
    requiring an explicit constrained `AllowRestricted` rule to opt in; and
  - pre-resolution approval, all-answer post-resolution validation, exact
    resolver-result binding, and connect-time DNS-rebinding/target-substitution
    rejection while preserving resolver candidate order.
- Boundary: dependencies are exactly `std`, `idna`, `ipnet`, and `regex`; there
  are no MPP engine/model/protocol/scheduler, path, runtime I/O, TOML, or DNS
  client imports.
- Evidence: 29 unit tests plus an allocator-instrumented integration test pass;
  10,000 warmed route and ACL classifications allocate zero heap objects;
  strict package-wide Clippy passes with warnings denied.

## 2026-07-26T19:01:03+08:00: Core safety and canonical configuration transactions

- Name: v4 TLS cut, bounded sparse state, receive credit, and persistent
  configuration reload
- Category: F2 Core safety and Product P6 control plane
- State: implementation complete for this slice; broader routing/DNS/runtime
  generation work remains in progress
- Content:
  - TCP now uses TLS 1.3 only with exact `mptunnel/4` ALPN, WebPKI
    name/time/signature validation, exact independently distributed leaf
    pinning, server certificate/key material, and separate MPP authentication;
  - sender ACKs are validated against immutable assigned-data horizons before
    mutation, authoritative complete snapshots cannot describe newer bytes,
    and retained sparse send/receive/range nodes have explicit transactional
    limits;
  - receive flow control rejects bytes above actually published credit, the
    opening flight advertises credit without another RTT, and later credit is
    committed only after its control frame enters an owned queue;
  - canonical TOML now has a content-addressed revision, strict candidate
    validation relative to its material directory, compare-and-swap conflict
    detection, external-edit preservation, bounded reads, redacted debug
    output, secure same-directory write/flush/rename, and directory flush;
  - authenticated management exposes strict TOML validate/apply endpoints,
    requires `If-Match`, replies before requesting a clean generation reload,
    and forbids API mutation of its own listener/authentication channel; and
  - file-backed planned reloads and supervised crash restarts reread validated
    disk state instead of cloning a stale in-memory graph.
- Evidence:
  - TCP TLS focused tests: 8 passed;
  - protocol v4 tests: 32 passed;
  - sparse mux tests: 28 passed;
  - config-store tests: 6 passed;
  - management config transaction test and management HTTP parser/security
    tests pass;
  - the full workspace run reached 949/951 before two test-fixture corrections;
    both corrected tests pass independently.
- Performance decision: no data-path lock or per-byte configuration work was
  added. TLS replaces the custom record layer as an explicit security and
  stability cut while retaining buffered multi-frame writes and actual
  encrypted-wire accounting. Sparse-node checks are O(1) on the normal path
  and perform an exact read-only preview only at the configured ceiling.

## 2026-07-26T19:05:59+08:00: Persistent outbound DNS runtime

- Name: generation-scoped bounded DNS resolution
- Category: Product DNS and outbound connectivity
- State: implementation and focused gates complete; full workspace gate awaits
  concurrent Product routing fixture migration
- Content:
  - each egress generation compiles one immutable runtime pool shared by its
    reliable TCP and UDP services, including distinct balancer-member policies;
  - explicit `system` and `servers` modes replace inference from an empty list;
    servers mode requires addresses, never consults hosts/system DNS, and uses
    only its configured UDP/TCP resolver endpoints;
  - positive and negative answers use one deterministic insertion-ordered
    bounded cache with configurable capacity and TTL caps, while IP answers are
    independently capped to bound entry memory and connection work;
  - distinct in-flight names are fail-fast bounded and same normalized names
    coalesce behind a cancellation-independent query task;
  - domains are strict ASCII DNS hostnames, normalized to lowercase without a
    root dot before query/cache lookup;
  - direct target and upstream proxy endpoint resolution both use the selected
    egress runtime, closing the prior proxy-host system-resolution leak;
  - TCP preserves staggered multi-address racing after Product DNS resolution,
    and the default dual-stack strategy retains both families; and
  - strict TOML exposes mode, capacity, in-flight bound, and positive/negative
    TTL caps; simple CLI exposes mode while runtime TOML transactions can
    persist every field.
- Evidence:
  - 11 focused DNS validation/cache/expiry/negative/coalescing/capacity/
    no-fallback/strategy tests pass;
  - 10 outbound TCP/UDP/proxy integration tests pass;
  - focused config model, TOML, and CLI tests pass;
  - 12 server datagram ownership tests and the affected reliable relay test
    pass; and
  - `cargo check --lib` and scoped diff checks pass.
- Deferred by scope: DoT/DoH/DoQ, split DNS, bootstrap resolution, FakeDNS, and
  DNS cache/latency telemetry remain future Product DNS slices.

## 2026-07-26T19:08:26+08:00: Product gateway balancer

- Name: bounded independent-server/outbound new-flow selection
- Category: Product P4
- State: pure Product implementation complete; runtime/TOML/API integration
  remains separate
- Content:
  - typed outbound/server members with ordered failover, round-robin,
    unbiased weighted-random with injected entropy, EWMA least-latency, and
    capacity-normalized least-load strategies;
  - bounded destination stickiness with TTL, capacity, deterministic eviction,
    and immediate health/drain invalidation;
  - passive outcomes, failure/recovery hysteresis, capped exponential backoff,
    exclusive recovery-probe claims, monotonic-time rollback protection, and a
    deterministic all-unhealthy fallback plan that cannot bypass cooldown;
  - draining and disabled members are excluded from every new-flow choice;
    generation-fenced handles bind established flows without cross-server
    migration; and
  - state is capped at 256 members and 65,536 sticky destinations with no Core,
    scheduler, path, runtime, TOML, or async dependency.
- Evidence: all 40 package unit tests and the allocator-instrumented integration
  test pass; warmed non-sticky selection adds zero heap allocations; strict
  package-wide Clippy passes with warnings denied.

## 2026-07-26T19:24:00+08:00: Local TCP Product routing integration

- Name: Product-policy routing for SOCKS5 and HTTP CONNECT
- Category: Product P2 routing and destination authorization
- State: TCP implementation and focused gates complete; full workspace gates
  await concurrent outbound-security consolidation
- Content:
  - strict `[[routing.rules]]` TOML compiles deterministic rule IDs, every
    Product matcher category, outbound/balancer/reject/drop/reset actions,
    traffic intent, explanations, and a mandatory final catch-all;
  - local proxy inbounds now require stable Product tags, while simple CLI
    profiles synthesize `socks5`, `http`, and `tun` tags;
  - rule references are type-checked against existing MPP outbound, MPP
    balancer, and local inbound tags before a runtime generation starts;
  - one immutable `Arc<ProductPolicyGeneration>` classifies normalized TCP
    flows built from target, accepted source endpoint, authenticated or
    explicit anonymous principal, inbound tag, and network;
  - the runtime maps typed Product IDs to independent `ClientPathContext`
    owners only after classification, without exposing scheduler/path state to
    the Product crate;
  - safe destination ACL authorization happens before any MPP stream open;
    reject returns SOCKS5 connection-not-allowed or HTTP 403, while drop/reset
    close without opening an MPP stream; and
  - traffic intent is mapped at the Product/Core boundary and reaches both
    initial path selection and the migrating relay.
- Evidence:
  - 16 focused strict configuration/compiler tests pass;
  - 5 focused runtime mapping, ACL, deny-action, SOCKS5, and HTTP tests pass;
  - workspace all-target check passed after integration; and
  - the hot policy decision and typed runtime lookup do not allocate after
    per-flow target normalization.
- Deferred by scope: UDP association/TUN per-flow routing, DNS post-resolution
  authorization/rebinding proofs, configurable restricted-address ACL
  overrides, and kernel-level TCP RST emission.

## 2026-07-26T20:05:00+08:00: Authenticated and TLS proxy egress

- Name: daily-use upstream proxy security and interoperability
- Category: Product outbound connectivity
- State: implementation and focused gates complete
- Content:
  - strict outbound proxy credentials support RFC 1929 SOCKS5 username/password
    authentication for both TCP CONNECT and UDP ASSOCIATE; the authenticated
    TCP control channel remains owned for the lifetime of each UDP association;
  - HTTP CONNECT and CONNECT-UDP optionally emit bounded Basic proxy
    authorization, while secret-bearing values are redacted from Debug and
    never included in validation or connection errors;
  - explicit `https-connect` performs HTTP/1.1 CONNECT over Rustls with WebPKI
    hostname and time validation, Mozilla public trust roots, optional
    operator-provided trust roots, and TLS 1.3/1.2 interoperability;
  - one absolute outbound deadline bounds proxy DNS resolution, TCP/UDP dial,
    TLS, authentication, CONNECT exchange, and SOCKS UDP relay setup;
  - request headers are capped at 16 KiB, response headers at 64 KiB and 64
    fields, SOCKS replies are fixed-bounded, and unsafe target/header bytes are
    rejected before network output;
  - strict TOML supports nested proxy auth and HTTPS identity/root fields;
    simple CLI supports proxy auth and HTTPS server-name selection; and
  - the connector enum is resolved before reliable relay entry, leaving direct
    and plain-proxy steady-state I/O on concrete `TcpStream` with no per-I/O
    connector dispatch.
- Evidence:
  - focused SOCKS5 TCP/UDP, HTTP CONNECT/CONNECT-UDP, HTTPS success and
    wrong-name rejection, DNS total-deadline, silent-proxy timeout, response
    bound, request-injection, auth redaction, TOML, and CLI tests pass;
  - all 37 outbound tests passed before the final total-deadline additions,
    followed by focused deadline and configuration passes; and
  - workspace all-target check passed at the coherent connector milestone.
- Remaining Product proxy gaps: HTTPS CONNECT-UDP/HTTP/2 MASQUE, SOCKS5 GSSAPI,
  proxy chaining, PAC discovery, and OS-native trust-store loading are not
  implemented.

## 2026-07-26T19:30:57+08:00: UDP and TUN Product routing integration

- Name: per-target SOCKS5 UDP and per-flow TUN policy binding
- Category: Product P2 routing and destination authorization
- State: implementation and focused gates complete
- Content:
  - TCP and UDP ingress use one network-parameterized Product classifier, so
    matcher, typed outbound/balancer lookup, ACL, deny handling, and traffic
    intent cannot diverge between protocols;
  - SOCKS5 UDP preserves the accepted control source, authenticated/anonymous
    principal, and inbound tag, then caches each target decision under a
    stream/queue-derived hard bound;
  - SOCKS5 UDP lane identity includes client peer, selected MPP session, and
    target, preventing packets or completions from crossing independently
    selected contexts; cached denies are silent drops before association work;
  - TUN TCP routes local-to-remote flows before opening MPP streams, while TUN
    UDP binds each new local/remote flow once to its effective target, selected
    context, and traffic class before allocating its bounded actor/queue;
  - denied TUN UDP flows are bounded and idle-expiring without MPP tasks, and
    accepted flow tasks/completions carry monotonically increasing generation
    IDs so stale completion or teardown cannot mutate a replacement flow; and
  - Product UDP intent reaches cross-underlay TCP candidate selection and is
    retained across feedback-driven reinjection; realtime/interactive UDP maps
    to the existing realtime-datagram Core class.
- Evidence:
  - 6 Product runtime tests pass, including UDP-only context/class selection and
    deny decisions without runtime bindings;
  - focused TUN routing verifies local CIDR, inbound, principal, UDP network,
    translated DNS target, selected context, traffic class, and deny behavior;
  - SOCKS5 UDP relay integration passes over both QUIC/UDP and encrypted-TCP
    carriers;
  - workspace all-target check is warning-free; and
  - all-target Clippy passes with warnings denied (existing large-enum lint
    explicitly excluded).
- Performance decision: classification occurs once per new target/flow, not per
  payload. Existing bounded queues, association failover, TTL handling, and
  payload paths remain unchanged; only cached context/class metadata is added.

## 2026-07-26T19:36:20+08:00: Server destination ACL enforcement

- Name: fail-closed server TCP/UDP destination authorization
- Category: Product outbound security
- State: implementation and focused gates complete
- Content:
  - strict per-MPP-inbound TOML compiles destination-only deny, allow, and
    scoped allow-restricted rules into one immutable generation shared by
    reliable admission, datagram admission, and final connectors;
  - the production default denies loopback, private, link-local, metadata,
    multicast, and unspecified destinations;
  - domains are resolved locally through the selected persistent egress DNS
    runtime, every capped answer must pass as one set, and direct/SOCKS/HTTP/
    HTTPS/CONNECT-UDP connectors receive only proof-bound literal addresses;
  - one absolute deadline bounds target DNS and dial/proxy work while retaining
    resolver address order and failover; policy is never called in relay payload
    loops; and
  - test runtimes opt into restricted loopback explicitly instead of weakening
    the production default.
- Evidence:
  - `cargo check --lib` and `cargo check --all-targets` passed at coherent
    checkpoints;
  - all 18 focused outbound connector tests passed, including literal TCP/UDP
    pivot denial before proxy invocation, mixed-answer DNS fail-closed before
    any dial, and a domain/CIDR/port-scoped LAN override over TCP and UDP.
- Context limitation: the MPP target-open protocol currently carries no
  original client source endpoint, authenticated application principal,
  interface, or local inbound identity. Server ACL TOML therefore exposes only
  destination fields and evaluates with explicit fixed server-side context
  sentinels; adding source/principal selectors would require authenticated
  protocol metadata rather than inferred values.

## 2026-07-26T19:52:05+08:00: Independent MPP gateway balancing

- Name: Product GatewayBalancer integration over independent MPP outbounds
- Category: Product routing, health, and flow ownership
- State: coherent implementation and focused gates complete
- Content:
  - removed `combined-mpp` path concatenation and its compatibility schema;
    each configured MPP outbound remains one independent session/context;
  - strict typed gateway members support ordered failover, round robin,
    weighted random, least latency, least load, health backoff/recovery, and
    bounded destination stickiness;
  - route classification and destination ACL authorization precede one bounded
    gateway selection per new flow; the balancer lock is never held across
    await, carrier scheduling, relay, or payload forwarding;
  - exact flow leases report pending/active load, successful-open latency, and
    explicit failed opens; abandoning a pending flow does not invent a failure
    and releases recovery-probe ownership;
  - SOCKS5, HTTP CONNECT, TUN TCP, SOCKS5 UDP target lanes, and TUN UDP flows
    retain the selected leaf and lease through their established lifetime; and
  - removed the local HTTP `protocol = "http"` alias; `http-connect` is the
    sole strict spelling.
- Evidence:
  - all-target compilation passed;
  - 20 strict config tests pass, including no path concatenation and rejection
    of legacy `combined-mpp`/HTTP spellings;
  - 6 Product router tests and 3 gateway state tests pass, covering independent
    round-robin leaves, fixed established bindings, failed-open ejection,
    recovery, and pending-drop accounting; and
  - focused SOCKS5 UDP and TUN UDP tests prove classification/selection occurs
    once per cached target/flow rather than per payload.
- Deferred boundary: legacy egress `Sequence`/`Random` groups require unified
  network-capability-aware outbound selection and are intentionally not mixed
  into this MPP-only slice.

## 2026-07-26T20:16:47+08:00: Split and encrypted DNS phase-A boundary

- Name: tagged DNS Product compiler and generation resolver
- Category: Product DNS
- State: isolated implementation complete; root integration waits for the
  unified outbound registry checkpoint
- Content:
  - a pure bounded Product model compiles tagged UDP, TCP, UDP-with-TCP,
    DoT, and DoH upstreams; every dial uses a literal bootstrap address and
    DoT/DoH structurally require a canonical authenticated TLS identity;
  - DoH is restricted to a consistent port-443 TLS/HTTP authority and a
    bounded absolute path; Hickory's HTTP/2 transport uses RFC 8484 POST;
  - exact rules win over longest label-boundary suffix rules, followed by one
    mandatory default plan, with stable rule/plan/generation/explanation
    metadata;
  - duplicate IDs/matches/references, missing references, plaintext members in
    encrypted plans, unsupported outbound transport capability, and
    DNS-dependent outbound recursion are compile errors;
  - the runtime owns persistent explicit-server connections and plan-isolated
    bounded caches, in-flight coalescing, negative caching, address-family
    policy, and answer bounds per immutable generation;
  - ordered failover shares one absolute lookup deadline, authoritative
    negative answers do not fall through, and oversized answer sets fail
    closed instead of being truncated;
  - explicit resolvers never load system resolver configuration or hosts; the
    direct backend refuses named outbound egress unless an injected connector
    builds the matching backend.
- Evidence:
  - all 48 `mptunnel-product` library tests pass, including 5 new DNS compiler
    tests for split precedence, identity/bootstrap/path constraints,
    recursion/capability checks, encrypted-only plans, duplicates, and missing
    references;
  - runtime focused tests are implemented for split metadata, per-plan cache
    isolation, coalescing, authoritative-negative fail-closed behavior,
    oversized answers, total deadline, connector refusal, and exact DoT/DoH
    transport construction;
  - root focused execution is temporarily blocked by the intentionally
    mid-edit unified outbound registry (unrelated config/node test compile
    errors), not by the isolated DNS module.
- Integration seam:
  - map strict config records to `DnsPolicySpec`, retain `Arc<CompiledDnsPolicy>`
    in a generation, and map route-carried `DnsPlanId` through
    `DnsGeneration::resolve_in_plan`;
  - implement `DnsBackendFactory` for named MPP/proxy leaves only after the
    unified registry is coherent; a missing connector is a hard error and
    never a direct fallback.

## 2026-07-26T20:58:35+08:00: Transactional Linux managed-VPN boundary

- Name: Linux VPN preparation, socket protection, readiness, and teardown
- Category: Product VPN lifecycle
- State: isolated runtime/platform contract complete; app/config generation
  wiring remains owned by the process supervisor
- Content:
  - strict platform state covers full/split includes, explicit excludes,
    local-LAN bypass, DNS capture, one TUN, and collision-checked Linux policy;
  - native routes are snapshotted before planning; MPP carriers, proxy control
    endpoints, and encrypted-DNS bootstrap IPs are resolved before any publish;
  - reconnects use an immutable carrier address snapshot, and every TCP/QUIC
    carrier plus direct/proxy target socket receives SO_MARK before connect or
    first send; bootstrap DNS remains protected by exact native host routes;
  - activation is prepare -> device handoff -> packet-worker ready -> native
    mark rule -> capture rule -> DNS, with no publishing operation in prepare;
  - shutdown retries exact unpublication before stopping the worker, then
    drops the device and retries exact cleanup; it refuses to stop the worker
    while residual capture/DNS publication remains;
  - full VPN rejects system/plaintext DNS and missing DNS capture, rejects
    zero/multiple managed TUNs, and supports direct-only use through the
    native-main SO_MARK invariant without inventing a static carrier.
- Evidence:
  - `mptunnel-platform`: 60 tests pass; strict clippy and rustdoc warnings pass;
  - focused root tests pass for injected lifecycle ordering, immutable carrier
    resolution, identity/config-generation fencing, and readiness/drop state;
  - root library check and rustdoc warnings pass at the coherent checkpoint;
    strict root clippy reports only pre-existing app/config-store size lints,
    with no VPN lifecycle/provider finding.
- Integration seam:
  - the app supervisor builds `LinuxVpnPrepareRequest`, calls
    `prepare_linux_vpn`, starts the generation with its three host providers,
    awaits `publish_when_worker_ready`, and calls `shutdown` on signal, reload,
    startup failure, or runtime exit.

## 2026-07-26T21:00:58+08:00: Unified outbound runtime boundary

- Name: one tagged MPP/native outbound registry with flow-pinned selection
- Category: Product routing, balancer, DNS, and egress
- State: coherent runtime boundary complete; integrated workspace gate remains
  with the root supervisor
- Content:
  - one immutable registry generation owns independent MPP, direct,
    bind-source, SOCKS5, HTTP CONNECT, HTTPS CONNECT, and CONNECT-UDP leaves;
  - Product routing selects a leaf or capability-filtered gateway once per
    flow, gateway retries are explicitly bounded by member count before
    commit, and the concrete MPP/native branch is pinned for its lifetime;
  - local SOCKS/HTTP/TUN TCP and UDP no longer require a fallback MPP context;
    native target and proxy sockets receive the injected pre-connect
    configurator, while proxy endpoint inventory is exposed for VPN preflight;
  - MPP inbound TCP/UDP carries its configured DNS plan into native egress,
    resolves and authorizes the complete answer set before dialing a literal,
    and rejects MPP-to-MPP chaining both in config and runtime assembly;
  - registry construction is two-stage: native leaves and balancers compile
    first, named direct/bind DNS connectors inherit their exact native socket
    policy, then one immutable DNS generation is attached; missing, MPP, and
    proxy DNS connectors fail closed.
- Performance boundary:
  - selection/retry and gateway locking occur only during flow open;
  - no routing, selector, connector-enum, or DNS dispatch enters the payload
    loop, and lint-suggested per-flow boxing was intentionally avoided;
  - established MPP Core scheduling and carrier data paths are unchanged.
- Evidence:
  - `cargo check -p mptunnel --lib` passes;
  - all 4 focused registry tests pass: local-only concrete TCP/UDP, bounded
    native failover, runtime no-chaining, and strict named DNS egress;
  - all 16 outbound connector tests pass, covering direct/proxy TCP/UDP,
    authenticated HTTPS, deadlines, response bounds, explicit-DNS isolation,
    and pre/post-resolution destination authorization;
  - the focused TUN flow-routing test passes and `git diff --check` is clean
    for the owned runtime/connector files.

## 2026-07-26T21:03:10+08:00: Single protected split-DNS owner

- Name: tagged Product DNS policy and generation-scoped runtime
- Category: Product DNS, config, and native egress protection
- State: coherent Product milestone complete
- Content:
  - the only DNS policy owner is `mptunnel-product`; it validates tagged
    system/UDP/TCP/UDP-TCP/DoT/DoH upstreams, literal bootstrap sockets,
    authenticated TLS identities, custom DoH authorities, encryption policy,
    recursion/capability constraints, and exact/longest-suffix/default plans;
  - the only DNS runtime owner is `src/dns.rs`; each immutable generation owns
    bounded per-plan positive/negative caches, in-flight coalescing, answer
    limits, ordered failover, and one total deadline;
  - explicit modes never use system resolver configuration or hosts, encrypted
    plans cannot contain plaintext members, and a named outbound without its
    injected direct/bind connector is a startup error with no direct fallback;
  - UDP, TCP, UDP-TCP, DoT, and pooled HTTP/2 DoH all apply the node/leaf
    `NativeSocketConfigurator` before connect or first send; source-bound DNS
    rejects IP-family mismatch before runtime;
  - TOML now has one strict global `[dns]` graph, route and MPP inbound actions
    carry `DnsPlanId`, simple CLI flags synthesize the same graph, and the
    duplicate legacy `src/outbound/dns.rs` cache/runtime was deleted.
- VPN seam:
  - compiled policy exposes exact literal bootstrap endpoints plus explicit
    system/encrypted-only facts for transactional full-VPN preflight and host
    bypass planning.
- Evidence:
  - `mptunnel-product`: 50 unit tests plus the warmed allocation test pass;
  - root focused suites pass: DNS 10/10, config model 14/14, TOML 20/20, CLI
    25/25;
  - `cargo check -p mptunnel --all-targets` and `git diff --check` pass at this
    milestone.

## 2026-07-26T21:12:11+08:00: Explicit runtime-generation readiness

- Name: event-driven startup barrier and truthful management health
- Category: Product lifecycle and operations
- State: runtime composition milestone complete; process-signal orchestration
  remains at the app supervisor seam
- Content:
  - every generation has an immutable control-plane identity and a one-way
    starting -> ready/stopping/failed phase signal backed by Tokio watch;
  - readiness is sealed only after all required services are registered, and
    a dropped pre-ready service fails the generation instead of decrementing
    the barrier as if startup succeeded;
  - SOCKS5 and HTTP CONNECT become ready after every configured socket is
    bound, MPP servers after every TCP/QUIC path is bound and listener actors
    are spawned, management after every HTTP listener is bound, and TUN only
    after its device framing and TCP/UDP stack primitives exist;
  - DNS compilation remains synchronous before the barrier can be sealed;
  - `/api/health` returns structured `200` only for `ready` and `503` for
    `starting`, `stopping`, and `failed`, including desired, last-good active,
    and running revisions for canonical-config generations;
  - obsolete standalone client/server management targets were removed; one
    node management composition now represents zero-or-more MPP outbounds and
    inbounds without duplicated lifecycle paths.
- Performance boundary:
  - generation observation is notification-driven, startup registration uses
    only a control-plane mutex, and no readiness branch, lock, or revision read
    enters packet, stream, scheduler, DNS-query, or relay payload loops.
- Evidence:
  - all 1,023 root library tests pass;
  - new deterministic tests cover all-ready notification, pre-ready service
    loss, stopping terminality, HTTP phase/status mapping, revision reporting,
    and occupied-listener bind failure;
  - `cargo check -p mptunnel --lib` and strict root library/test clippy pass
    for this work; the unqualified strict clippy gate still reports only the
    separately owned app/config-store large-error lint.
- App integration seam:
  - run `run_with_config_control` concurrently with
    `RuntimeConfigControl::wait_until_ready`; commit the pending configuration
    only after that await succeeds;
  - call `RuntimeConfigControl::mark_stopping` before cooperative signal or
    reload teardown so health turns `503` before listeners/TUN publication are
    withdrawn.

## 2026-07-26T21:13:59+08:00: Core F3-F7 audit and F0 gate repair

- Name: observation, datagram, scheduling, recovery, and mobility gap map
- Category: Core performance architecture and measurement enforcement
- State: audit complete; two isolated F0 tooling defects repaired; Core
  algorithms unchanged
- Content:
  - audited the implementation, tests, RFC, performance plan, historical
    evidence, simulator boundary, runner matrix, and stale experiment state for
    F3 through F7;
  - distinguished existing estimator/range/failover primitives from missing
    native QUIC DATAGRAM, unified planning, survivor-class recovery, and
    network-epoch mobility behavior;
  - retired-experiment cleanup remains complete: the rejected private QUIC
    calibration/handoff code, runner cases, tests, and diagnostic consumers
    are absent;
  - classified every production path below `crates/mpp-engine`,
    `crates/mpp-model`, and `crates/mpp-scheduler` as extracted Core requiring
    fail-closed full-matrix performance evidence;
  - advanced the performance registry contract to `2026-07-26.f0-2` and added
    library and CLI regression tests proving those paths cannot bypass a
    declaration; and
  - kept `lab/benchmarks` as an explicitly excluded, independently locked
    standalone workspace, then refreshed its dedicated lockfile against the
    extracted packages.
- Evidence:
  - `python3 -m unittest lab.test_performance_declaration`: 21 passed;
  - registry self-check: 29 cells and 66 metrics;
  - all three extracted Core path samples resolve to `full-matrix`, with no
    unclassified path and `declaration_required=true`;
  - root and standalone benchmark `cargo metadata --locked` both resolve; the
    benchmark reports its own directory as workspace root with exactly one
    member; and
  - extracted `mpp-model`, `mpp-scheduler`, and `mpp-engine` tests: 19 passed;
    the exact documented
    `cargo test --locked --manifest-path lab/benchmarks/Cargo.toml` gate: 6
    passed.

## 2026-07-26T21:18:35+08:00: Local managed-VPN DNS termination

- Name: bounded UDP/TCP DNS capture into the split-DNS generation
- Category: Product DNS and VPN data-plane edge
- State: implementation and focused tests complete
- Content:
  - managed DNS capture recognizes only the configured system-facing IPs on
    port 53; external/manual TUN resolver forwarding remains a separate mode;
  - captured queries terminate locally in the immutable split-DNS generation
    and never pass through ordinary Product routing, preventing recursive VPN
    capture;
  - the wire responder validates one standard Internet-class query, answers A
    and AAAA records, preserves transaction/question metadata, maps negative
    and runtime failures to explicit DNS response codes, and returns `NOTIMP`
    for unsupported record types;
  - UDP response size and concurrent query work are bounded, with DNS
    truncation when required; TCP uses bounded length-framed messages, idle
    deadlines, response sizes, and queries per connection; and
  - the integration adds no branch, lock, or callback to non-DNS payload
    forwarding.
- Evidence:
  - focused DNS suite: 30 passed;
  - focused TUN L4 suite: 6 passed; and
  - tests cover split-plan selection, A/AAAA family filtering, DNS error
    mapping, malformed and unsupported requests, truncation, capture-address
    matching, and a real duplex TCP DNS exchange.

## 2026-07-26T21:29:14+08:00: Explicit managed-Linux-VPN configuration

- Name: typed external/manual versus transactionally managed TUN ownership
- Category: Product configuration and Linux VPN
- State: configuration surface, validation, CLI path, and focused integration
  gates complete
- Content:
  - `TunL4Config` now defaults to explicit non-mutating `External` host
    ownership and may instead carry one `ManagedLinuxVpnConfig`; one node may
    configure at most one managed TUN owner;
  - managed configuration supports full or split capture, canonical include
    and exclude CIDRs, local-LAN bypass, local DNS capture IPs, and typed Linux
    route-table, native-rule, capture-rule, and socket-mark ownership;
  - the TUN surface compiles directly into strict
    `mptunnel_platform::VpnConfig`, including interface/address/MTU/family,
    DNS/exclude, Linux name, priority ordering, reserved-table, and nonzero
    mark validation;
  - full VPN requires local DNS capture, DNS capture addresses must match a
    configured TUN family, external `dns_resolvers` and `ipv4_gateway` cannot
    be mixed into managed ownership, and the stable unnamed managed interface
    is `mptun0`;
  - TOML uses the nested, unknown-field-rejecting
    `[inbounds.host] mode = "managed-linux"` shape; omission remains external
    mode and no compatibility aliases were added;
  - simple CLI flags cover full/split mode, include/exclude CIDRs, local LAN,
    and DNS capture; a managed CLI profile also requires a paired literal DoT
    bootstrap and authenticated server name, from which one
    `RequireEncrypted` default DNS plan is synthesized; and
  - packet and scheduler loops are unchanged: conversion and validation occur
    only while compiling a runtime generation.
- Evidence:
  - focused config-model suite: 20 passed;
  - focused TOML suite: 25 passed;
  - focused CLI suite: 30 passed;
  - managed Linux VPN request/compiler suite: 11 passed;
  - focused TUN L4 integration suite: 3 passed;
  - real binary `--check-config` probes pass for managed full+DoT and
    split+DoT profiles and fail closed for missing DoT identity or full-VPN
    DNS capture;
  - `cargo check -p mptunnel --all-targets`, touched-file rustfmt,
    `git diff --check`, and all-target clippy with separately owned
    readiness/config-store/manual-clamp warnings explicitly allowed pass.

## 2026-07-26T21:29:31+08:00: Managed-VPN generation specification

- Name: pure Linux VPN request compiler and encrypted bootstrap resolution
- Category: Product generation assembly and native-bypass correctness
- State: implementation complete; app supervision remains intentionally
  separate
- Content:
  - a synchronous, host-independent compiler maps `AppConfig` or `NodeConfig`
    to no request for external TUN ownership or one complete
    `LinuxVpnPrepareRequest` for exactly one managed TUN;
  - MPP carrier identities use the same MPP-only group ordinal and original
    path ordinal as combined runtime assembly, while every local SOCKS5,
    HTTP(S) CONNECT, and CONNECT-UDP control endpoint is retained once;
  - direct-only managed VPN is valid, inventory/resource violations carry
    precise generation errors, and the compiled request owns the strict
    platform VPN config, encrypted DNS policy, one managed-TUN count, and a
    nonzero ten-second bootstrap deadline;
  - pre-publication carrier and proxy hostname resolution now uses a temporary
    `DnsGeneration` compiled from the required encrypted direct policy under
    that same deadline; literal IPs bypass lookup and no production
    `tokio::net::lookup_host` path remains; and
  - named DNS egress fails closed before carrier bootstrap, both in normal
    generation compilation and when a hand-built prepare request reaches the
    lifecycle defense.
- Performance boundary: all compilation, DNS bootstrap, and endpoint
  resolution occurs once before host publication; packet, stream, scheduler,
  carrier, and established-flow loops are unchanged.
- Evidence:
  - managed Linux VPN focused suite: 11 passed;
  - complete root library suite: 1,057 passed;
  - `cargo check -p mptunnel --lib` and all-target test Clippy review pass with
    only separately owned pre-existing warnings;
  - touched-file rustfmt and repository `git diff --check` pass.

## 2026-07-26T21:37:01+08:00: Transactional runtime-generation lifecycle

- Name: readiness-gated activation, explicit retirement, and last-good recovery
- Category: Product lifecycle and canonical configuration
- State: lifecycle owner and managed-host integration seam complete
- Content:
  - every replacement now reuses one in-memory `Arc<CanonicalConfigStore>`;
    generation changes never reopen the canonical file or create a competing
    revision owner;
  - runtime termination has explicit reload, shutdown, and failure outcomes,
    and replacement begins only after the prior generation has retired and all
    of its owned tasks have been joined;
  - a desired revision is activated only after the complete runtime readiness
    barrier succeeds; terminal failure has priority over simultaneous
    readiness, and no activation/success presentation occurs early;
  - a candidate that fails startup before activation is rolled back durably to
    last-good and the restored configuration is restarted; process shutdown
    also rolls back any unactivated desired revision;
  - SIGINT and SIGTERM request a cooperative generation stop and wait up to ten
    seconds for service retirement before the explicit abort-and-join fallback;
  - managed VPN hosts can defer retirement, observe the stop reason, unpublish
    host-owned VPN state, authorize retirement, and then await the runtime
    outcome; ordinary external/manual generations remain pre-authorized; and
  - reload/shutdown observation is notification-driven and does not add a
    branch, lock, revision read, or allocation to packet, stream, scheduler,
    DNS-query, carrier, or relay payload loops.
- Evidence:
  - app lifecycle suite: 13 passed, covering cooperative signal teardown,
    bounded stuck teardown, single-store identity, readiness-only activation,
    simultaneous failure precedence, and failed-candidate rollback;
  - runtime readiness suite: 5 passed;
  - deferred-retirement service-owner test: 1 passed;
  - the complete library run passed 1,062 tests and exposed one concurrently
    changed DNS assertion; its corrected exact test subsequently passed; and
  - touched lifecycle files pass rustfmt and `git diff --check`.

## 2026-07-26T21:40:16+08:00: DNS through named Product outbounds

- Name: recursion-proof routed TCP, DoT, and DoH
- Category: Product DNS and outbound integration
- State: implementation and verification complete
- Content:
  - named DNS upstreams can carry TCP, DoT, and persistent HTTP/2 DoH through
    SOCKS5, HTTP CONNECT, HTTPS CONNECT, CONNECT-UDP's TCP CONNECT capability,
    or an MPP outbound;
  - the connector receives only the upstream's literal bootstrap address and
    cannot consult system DNS or the resolver generation it is constructing;
  - proxy control endpoints and every MPP carrier endpoint must be literal IPs
    for DNS egress, with runtime assembly rechecking the proof even for
    hand-built policies;
  - TOML capability compilation includes every leaf but advertises only the
    actually implemented routed-DNS TCP capability, so hostname bootstrap
    cycles and routed UDP fail during validation rather than at query time;
  - direct/bind DNS behavior and its native socket policy remain unchanged;
    Product selection occurs at backend construction/query creation and no
    Core scheduler or established payload loop was modified.
- Evidence:
  - routed-DNS acceptance: 6 passed;
  - complete DNS-focused selection: 37 passed;
  - outbound connector suite: 16 passed;
  - TOML configuration suite: 28 passed;
  - complete root library suite: 1,069 passed;
  - a real local SOCKS5 integration test proves the literal resolver bootstrap
    is sent in CONNECT and the length-framed DNS request/answer traverses that
    selected proxy rather than direct egress;
  - literal proxy TCP/DoT/DoH and literal MPP assembly tests pass, while named
    proxy/MPP control endpoints and unimplemented routed UDP fail closed;
  - `cargo check -p mptunnel --all-targets`, touched-file rustfmt, and
    `git diff --check` pass;
  - strict library Clippy passes when only the separately owned
    app/config-store result-size, app nested-condition, and TUN manual-clamp
    findings are explicitly allowed.

## 2026-07-26T21:41:50+08:00: Daily-use Product acceptance boundary

- Name: non-privileged packaged-process acceptance
- Category: Product release confidence
- State: complete for deterministic non-privileged scope
- Content:
  - the Cargo-built executable now has a black-box test proving strict nested
    TOML rejection and authenticated configuration status, validation,
    optimistic-concurrency rejection, atomic persistence, generation reload,
    listener replacement, and post-reload policy behavior;
  - a two-process loopback topology proves local SOCKS5 routing can reject,
    retry a failed native SOCKS5 balancer member before committing to a healthy
    member, and carry a real stream over authenticated TLS MPP into an explicit
    direct server egress;
  - a public config-to-runtime test proves two encrypted DoT plans, suffix
    selection metadata, default-plan selection, and the local DNS-capture wire
    response boundary without relying on public DNS availability; and
  - all fixtures use unprivileged loopback sockets and leave packet,
    scheduler, carrier, and established-flow algorithms unchanged.
- Deliberately uncovered:
  - managed TUN creation, route/rule/resolver publication, redirected UDP/53,
    rollback after host mutation, and leak tests still require a disposable
    privileged Linux namespace;
  - real Internet DoT certificate/network interoperability remains outside the
    deterministic suite; this boundary injects tagged backends after proving
    strict encrypted transport configuration;
  - release archives, installers/service registration, signatures, and
    Windows/macOS packaged binaries are not exercised by Cargo's built binary;
  - this suite does not duplicate existing lower-level QUIC/UDP,
    HTTP(S)-CONNECT, CONNECT-UDP, or crash-journal campaigns.
- Evidence:
  - `cargo test -p mptunnel --test product_daily_use_acceptance`: 3 passed;
  - focused Clippy completed with no acceptance-test warnings; its 28 warnings
    are pre-existing in separately owned app/config-store/TUN implementation;
  - touched-test rustfmt and whitespace checks pass.

## 2026-07-26T21:43:54+08:00: Cross-platform VPN architecture correction

- Name: remove Linux lifecycle knowledge from generic Product composition
- Category: Product platform boundary
- State: in progress; Linux-only app integration paused before completion
- Decision:
  - `app.rs` must coordinate one platform-neutral VPN-generation contract and
    must not name Linux controllers, socket marks, route tables, or Linux error
    types;
  - the clean managed-VPN schema is platform-neutral; optional platform tuning
    belongs in a nested platform policy rather than the generic full/split/DNS
    fields;
  - Linux, Android, Windows, and macOS are all first-class declared targets;
    capability reporting must distinguish process-managed host networking from
    Android/extension host-owned lifecycle without presenting either as a
    silent fallback;
  - current Linux RPDB planning and operations are Linux adapter types, not
    generic `VpnConfig`, `VpnPlan`, or `HostOperation` contracts; and
  - Android's owned TUN descriptor plus socket-protection callbacks and
    Windows/macOS native adapters must enter through the same runtime provider
    boundary used by Linux, while remaining outside packet and Core loops.
- Performance boundary: the correction changes only generation preparation,
  publication, and host-provider composition. It must not add work to packet,
  stream, datagram, carrier, scheduler, or established-flow paths.

## 2026-07-26T21:52:36+08:00: Platform-neutral VPN lifecycle contract

- Name: truthful Linux, Android, Windows, and macOS VPN capability boundary
- Category: Product platform lifecycle
- State: portable contract complete; non-Linux native adapters remain explicit
  integration work
- Content:
  - `ManagedVpnConfig` now contains only portable addresses, MTU, full/split
    capture, excludes, local-LAN, and DNS intent; Linux interface identity,
    RPDB policy, planning, operations, native-route snapshots, transactions,
    and errors all have explicit `Linux*` names;
  - `VpnPlatformCapabilities` classifies Linux as built-in process-managed
    two-phase activation, Android as host-owned `VpnService` establishment,
    and Windows/macOS as process-managed two-phase adapter seams;
  - every packet-device, address, route, DNS, native-socket-bypass,
    publication, and cleanup capability reports built-in, host-required,
    adapter-required, or unsupported status without fallback;
  - Android preparation truthfully returns an already-published host device,
    requires host `protect(fd)` for every native socket, and rejects a false
    two-phase claim;
  - one `VpnLifecycleAdapter` contract covers prepare, publish, unpublish,
    cleanup, and one-time native-socket protection; validated lifecycle
    requests own bounded, canonical carrier/bootstrap bypass inventories; and
  - the crate README records the concrete VpnService, Wintun/IP Helper/DNS, and
    utun/route/DNS work still required for actual OS integration.
- Performance boundary:
  - lifecycle calls occur only during generation construction/retirement or
    native socket creation before connect/first send; no packet, stream,
    scheduler, carrier, DNS-query, or relay payload loop changed.
- Evidence:
  - platform crate: 67 tests passed;
  - strict all-target Clippy passed;
  - native Linux all-target check passed;
  - real cross-target checks passed for Windows GNU, Android ARM64, and macOS
    ARM64; and
  - crate formatting and whitespace checks passed.

## 2026-07-26T21:55:42+08:00: Platform-neutral managed-VPN Product schema

- Name: remove Linux lifecycle identity from user-facing managed-VPN policy
- Category: Product configuration and CLI
- State: complete
- Content:
  - `TunHostConfig::Managed`, the local `ManagedVpnConfig`, and
    `compile_managed_vpn` now express only portable full/split capture,
    include/exclude prefixes, local-LAN bypass, addresses, MTU, and DNS
    capture intent;
  - strict TOML accepts only `mode = "managed"`; the removed
    `mode = "managed-linux"` spelling and an Android lifecycle alias are
    rejected without compatibility shims;
  - optional Linux RPDB tuning is isolated under
    `[inbounds.host.linux]`, while the portable platform configuration passed
    to the lifecycle boundary excludes Linux interface identity and policy;
  - simple CLI VPN options remain platform-neutral and do not synthesize
    Linux tuning; help text states that Android `VpnService` lifecycle is
    supplied by the host rather than owned by the process; and
  - all legacy Rust identifiers were removed from active call sites; the sole
    `managed-linux` occurrence is the strict rejection fixture.
- Performance boundary:
  - this is a configuration/compiler rename and ownership split only; packet,
    stream, datagram, carrier, scheduler, and established-flow algorithms are
    unchanged.
- Evidence:
  - `cargo check -p mptunnel --all-targets` passed;
  - config-file tests: 29 passed;
  - config-model tests: 21 passed;
  - CLI tests: 30 passed;
  - focused Clippy completed successfully; its warnings are in separately
    owned app/config-store/runtime/transport code; and
  - touched-file formatting, whitespace, and stale-identifier checks passed.

## 2026-07-26T21:58:55+08:00: Unified embedded-VPN socket protection

- Name: fail-closed pre-connect protection for every process egress socket
- Category: Product platform boundary
- State: complete
- Content:
  - one public `HostSocketProtector` contract receives a lifetime-bounded
    borrowed descriptor/socket plus remote address and purpose for MPP
    carriers and native target, proxy, and DNS TCP/UDP sockets;
  - protected carrier and native adapters invoke the callback once after
    source binding and before connect or first send, drop the socket on
    rejection, and perform no established-flow or packet-loop callbacks;
  - `run_with_vpn_host_providers` derives both adapters from one callback so
    Android `VpnService` and Apple packet-tunnel hosts cannot omit an egress
    class accidentally;
  - Linux socket marking implements the same host-protector contract and its
    carrier/native wrappers delegate through that boundary;
  - public embedding documentation defines borrowed-handle ownership,
    exact-once composition, fail-closed behavior, and the requirement that the
    underlying carrier provider must not protect independently; and
  - deterministic tests cover TCP and UDP for every native purpose, both MPP
    carrier underlays and identities, valid borrowed handles, exact callback
    counts, Linux contract conformance, and rejection before TCP connect or
    UDP emission.
- Performance boundary:
  - the callback runs only during socket construction, exactly once per socket;
    no callback, allocation, branch, or adapter was added to established-flow,
    scheduler, framing, carrier payload, DNS-query payload, or packet loops.
- Evidence:
  - native-protector tests: 5 passed;
  - carrier-network tests: 8 passed;
  - full library suite: 1,077 passed;
  - native all-target check and Windows GNU all-target cross-check passed;
  - Android and macOS full-crate cross-checks stop in `ring` before MPTunnel
    source compilation because their native C cross-compilers are absent;
  - rustdoc completed with warnings denied;
  - strict Clippy passed after allowing only separately owned
    result-size/collapsible-if/manual-clamp findings; and
  - touched-file formatting and whitespace checks passed.

## 2026-07-26T22:12:04+08:00: Casual-user release asset contract

- Name: normalized downloads separated from CI staging and provenance
- Category: Product release UX
- State: complete
- Comparison:
  - official V2Fly v5.51.2 uses stable `v2ray-<os>-<arch>.zip` names,
    adjacent `.dgst` files, and a signed `Release` manifest; representative
    archives contain the binary, configurations, geodata, and Linux systemd
    units:
    https://github.com/v2fly/v2ray-core/releases/tag/v5.51.2
  - official Hysteria v2.10.0 uses stable raw
    `hysteria-<os>-<arch>` executable names and one `hashes.txt`:
    https://github.com/apernet/hysteria/releases/tag/app%2Fv2.10.0
- Content:
  - MPTUNNEL now exposes exactly seven version-independent,
    product/OS/architecture archives plus one sorted `SHA256SUMS`;
  - target triples and versions remain build identity, not casual filenames;
  - every archive contains the binary, concise README, licenses, and
    client/server TOML examples; Linux adds a systemd unit, macOS a launchd
    template, Windows pinned Wintun files, and Android an explicit host/core
    integration notice;
  - archive order, timestamps, ownership, modes, and metadata are normalized;
  - CI package uploads are private staging, build/version evidence remains a
    separate private artifact, attestations remain separate provenance, and
    only the eight allowlisted files can reach `gh release create`; and
  - the publish job verifies exact inventory and checksums before draft
    creation and again after a fresh download before publication.
- Remaining installer gaps:
  - no Linux distribution package or transactional installer;
  - no signed/notarized macOS package or privileged helper;
  - no signed Windows MSI/MSIX or native SCM wrapper; and
  - no Android APK/AAB, AAR/JNI bridge, or example `VpnService`.
- Performance boundary:
  - packaging, workflow, tests, and documentation only; runtime and Core are
    unchanged.
- Evidence:
  - deterministic archive contract suite: 7 passed across all seven targets;
  - the real shell packager produced the same Linux archive hash twice and
    passed the strict archive verifier;
  - Actionlint, ShellCheck, Bash syntax, plist XML, Ruff, Python bytecode,
    formatting, and whitespace checks passed.

## 2026-07-26T22:25:57+08:00: Compact configuration-store errors

- Name: remove large `Result` error ABI without lint suppression
- Category: Product configuration persistence
- State: complete
- Content:
  - recovery-conflict revisions remain a public typed diagnostic record, while
    the exceptional four-revision payload is boxed inside `ConfigStoreError`;
  - display, management error mapping, exports, and focused tests follow the
    compact representation; and
  - an explicit size regression test keeps `ConfigStoreError` at or below
    Clippy's 128-byte large-result threshold.
- Performance boundary:
  - allocation occurs only when startup recovery detects conflicting durable
    state; normal configuration transactions and all runtime/Core paths are
    unchanged.
- Evidence:
  - configuration-store tests: 14 passed;
  - strict library/test Clippy passed with all warnings denied; and
  - touched-file formatting and whitespace checks passed.

## 2026-07-26T22:17:47+08:00: Fixed local/CI build ownership

- Name: native-runner build procedure
- Category: CI and release operations
- State: complete
- Decision:
  - Linux quality, runtime, integration, privileged namespace, and performance
    work runs locally;
  - Android arm64 builds run only in the pinned GitHub NDK lane;
  - macOS and Windows builds/tests run only on their native GitHub runners;
  - a source-branch push or manual CI dispatch is the authoritative
    cross-platform checkpoint and uploads no release bundle; and
  - `Release Check` may privately stage normalized packages but never
    publishes, while the tag workflow can expose only the strict eight-file
    public contract.
- Cleanup:
  - removed the local Android Dockerfile, wrapper, `.dockerignore`, stopped
    image, and empty directory because they could not reproduce native macOS
    or Windows and duplicated the authoritative GitHub lane.
- Documentation:
  - added `./docs/CI.md` and linked it from the README and operations guide;
  - added manual dispatch to the normal CI workflow; and
  - added a contract test that all three GitHub build workflows share the
    exact seven targets, NDK version, and Android API linker, while normal CI
    contains no artifact upload or release command.
- Evidence:
  - release/CI contract suite: 8 passed;
  - Actionlint and whitespace checks passed.

## 2026-07-26T22:19:29+08:00: Fail-safe managed-VPN generation retirement

- Name: preserve packet runtime until host traffic publication is removed
- Category: Product lifecycle safety
- State: complete
- Content:
  - the platform-neutral generation contract now separates publication
    removal from post-worker cleanup; generic composition cannot authorize or
    await packet-runtime retirement until unpublication succeeds;
  - failed unpublication is retried while the runtime remains owned and live,
    including reload, activation-failure, publication-failure, and process
    shutdown paths;
  - the process teardown timeout cannot abort a runtime while host routes or
    DNS may still publish traffic to it; once unpublication succeeds, normal
    bounded teardown behavior resumes;
  - readiness failure now explicitly requests safe generation retirement
    instead of depending on a concurrent worker failure;
  - cleanup runs only after publication is absent and the worker has stopped;
    Linux rejects cleanup when any publication step remains;
  - Linux prepare and device-handoff failures retry residual inert rollback
    before the lifecycle controller is released; and
  - Linux-specific state, operations, and errors remain behind
    `runtime/vpn_generation` and `mptunnel-platform`.
- Fault evidence:
  - deterministic generic tests cover start, readiness, publish, activation,
    reload, blocked retirement, cleanup failure, and teardown timeout;
  - deterministic Linux tests cover prepare failure, device handoff,
    residual unpublish rollback, post-unpublish cleanup failure, and exact
    prepare/publish/unpublish/cleanup order;
  - platform transaction tests exhaust every prepare and publish apply
    failure and retry residual rollback without losing phase ownership.
- Performance boundary:
  - all logic runs only at generation preparation, activation, reload, or
    retirement boundaries; no packet, stream, datagram, scheduler, carrier,
    DNS payload, or established-flow loop changed.
- Verification:
  - app lifecycle tests: 21 passed;
  - Linux VPN adapter tests: 15 passed;
  - Linux transaction tests: 11 passed;
  - full library suite: 1,088 passed;
  - Linux all-target check passed; and
  - strict Clippy passed after allowing only separately owned existing
    result-size/collapsible-if/manual-clamp findings.

## 2026-07-26T22:20:48+08:00: Desktop managed-VPN native primitive checkpoint

- Name: explicit Windows and macOS managed-VPN host primitives
- Category: Product platform lifecycle
- State: coherent bounded checkpoint; native CI evidence pending
- Content:
  - `./crates/mptunnel-platform` now provides one shared deterministic
    desktop route/DNS planner and an exact two-phase host-mutation
    transaction with reverse-order, retry-safe cleanup;
  - plans protect carrier endpoints, bootstrap DNS endpoints, explicit
    excludes, and optional local-LAN routes before publishing capture routes
    or DNS, while rejecting self-capture, ambiguous equal-cost native paths,
    invalid families, and conflicting DNS bypasses;
  - Windows now has a strict Wintun device factory plus IP Helper route and
    direct modern DNS mutation primitives, with explicit owned-versus-existing
    state, postcondition checks, and partial-apply rollback;
  - macOS now has a privileged-process utun factory and route primitive;
    process-level DNS configuration fails closed because a mature consumer
    VPN requires an entitled Network Extension provider; and
  - Android remains explicitly host-owned.
- Capability truth:
  - Windows and macOS managed-VPN profiles intentionally remain
    `AdapterRequired`; these primitives are not advertised as built-in until
    the root runtime bridge exists and authoritative native tests pass;
  - Windows additionally requires an explicitly supplied signed Wintun DLL;
    macOS full consumer lifecycle, DNS, suspend/resume, and full-tunnel route
    behavior remain Network Extension/runtime integration work.
- Performance boundary:
  - no packet, stream, datagram, scheduler, carrier, congestion-control, or
    core algorithm path changed; this work executes only at host lifecycle
    boundaries.
- Verification:
  - platform unit suite on Linux: 78 passed;
  - strict platform all-target/all-feature Clippy on Linux passed;
  - root library check passed;
  - scoped whitespace check passed; and
  - final Windows/macOS source requires the repository's authoritative native
    GitHub/NDK CI; no local cross-build result is treated as completion.

## 2026-07-26T22:31:40+08:00: Local Linux source checkpoint gate

- Name: fixed pre-push Linux verification procedure
- Category: CI and deployment hygiene
- State: complete locally; authoritative native GitHub matrix pending
- Content:
  - removed the incorrect local Android cross-build image and source files;
  - documented the fixed local-Linux and GitHub-native build boundaries in
    `./docs/CI.md`;
  - retained normal branch CI as build/test evidence only, with no release
    artifact upload or GitHub Release mutation;
  - accepted CDLA-Permissive-2.0 in `./about.toml` because the locked
    `webpki-roots` dependency uses it, and regenerated the tracked notice;
  - refreshed the benchmark workspace lockfile after the Product crate and DNS
    dependency extraction so its documented `--locked` gate is reproducible.
- Evidence:
  - strict all-target/all-feature Clippy passed with zero warnings;
  - full all-feature Rust suite passed, including 1,097 root library tests,
    78 platform tests, 50 Product tests, and Product acceptance tests;
  - performance registry valid: 29 cells and 66 metrics;
  - 190 lab contract tests, 6 benchmark/replay tests, and 8 release contract
    tests passed;
  - shell syntax, release version-gate self-test, Actionlint, formatting,
    whitespace, and maintainability report passed;
  - third-party notice regeneration is byte-for-byte deterministic.

## 2026-07-26T23:45:00+08:00: Consolidation and scratch-ownership correction

- Name: single-package source tree, sole Quinn override, and ignored scratch
- Category: Product boundary, repository hygiene, and release procedure
- State: implemented locally; full post-change gates and native CI pending
- Content:
  - supersedes the earlier extracted first-party crate and third-party notice
    notes above: all first-party Product, platform, protocol, scheduler, model,
    and performance owners are cohesive modules under `./src`;
  - `./crates` contains only `./crates/quinn-proto`, with an exact upstream
    baseline, a tracked standalone lockfile, a semantic patch inventory, and a
    documented rebase/update gate;
  - removed `about.toml`, its template/generator, the generated HTML notice,
    and the old `./third_party` owner; release archives intentionally contain
    no generated dependency-license page;
  - moved the tracked exhaustive configuration specimen to
    `./examples/config.reference.toml`; root `./config.toml` remains ignored,
    durable operator state and is still the no-argument auto-load path;
  - moved packaging implementation to `./packaging/tools`, current platform
    lifecycle truth to `./docs/PLATFORM.md`, and frozen v0.1.1 material to
    `./docs/audits/v0.1.1`;
  - moved deterministic replay fixtures under their benchmark owner;
  - added `./.cargo/config.toml` so all Cargo invocations use
    `./.tmp/cargo`; lab evidence, Python caches, system test scratch, package
    staging, dependency downloads, CI scratch, and local progress now also
    live only below ignored `./.tmp`;
  - release downloads remain the seven normalized platform archives plus one
    `SHA256SUMS`; raw Actions artifacts and private version evidence are not
    public release assets.
- Quinn memory correction:
  - preserved the upstream `SentPacket <= 128` byte guard;
  - compacted MPTunnel delivery sampling to delivered bytes/time plus one
    relative send interval, using existing sent-packet metadata at ACK time;
  - standalone Quinn passed 280 unit tests and 3 doctests.
- Current evidence:
  - root format/check passed;
  - root all-target/all-feature check passed;
  - root all-target/all-feature suite passed: 1,276 unit tests plus four
    durable Product acceptance/allocation tests;
  - performance registry valid: 29 cells and 66 metrics;
  - 190 lab tests and 6 deterministic benchmark/replay tests passed;
  - 8 release contract tests and the normalized Linux musl archive check
    passed;
  - Actionlint and shell syntax passed after the scratch-path conversion.
