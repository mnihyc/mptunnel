# MPTUNNEL progress

This is the authoritative execution and milestone ledger for the repository.
Entries record verified work, evidence, open blockers, and the next bounded
action. Local development plans may live in the ignored `./docs-dev/`
directory, but they are not progress logs and never supersede this file, the
public product contract, or `RFC.md`.

Historical entries below are retained as evidence of the decisions made at
their recorded time. When a later entry changes an earlier decision, the later
entry is authoritative.

## 2026-08-06T02:16:00+08:00: public browser and blackhole evidence reconciled

- Name: clean current README continuity evidence
- Category: Public performance provenance and recovery presentation
- State: accepted on clean source `fc9042e`; documentation-only reconciliation
- Audit findings corrected:
  - the published cold-browser values came from the exact code later committed
    as `fc9042e`, but their retained run was necessarily source-dirty and not
    public-comparable;
  - the earlier mixed-blackhole values no longer had their referenced raw
    artifact; and
  - process-restart acceptance proves renewed post-restart connectivity, not
    survival of an existing application flow. Same-flow retention remains the
    separately proven total-carrier-outage behavior.
- One bounded clean-source invocation ran only the periodic browser,
  continuous browser, and mixed blackhole cases with the ordinary
  non-diagnostic binary:
  - periodic load completed `90/90` requests with zero failure and zero
    deadline miss; the slowest ten-request batch was `681.436 ms`;
  - 60-second continuous load started, accepted, and completed `755/755`
    one-MiB requests, rejected none, left none incomplete, and maintained the
    requested peak concurrency of 20; and
  - the two-second path blackhole retained `278.488 Mbps` bulk service with a
    `636.401 ms` maximum receiver gap, completed `60/60` persistent TCP and
    `72/72` HTTP checks, and delivered `228/229` datagrams. The one datagram
    crossing the unavailable path remains an explicit expected loss.
- Evidence integrity: every row has `host_valid=true`,
  `performance_comparable=true`, clean tracked source, exact commit
  `fc9042e6001fca7ee8e865da53c4930dc47c1128`, and binary SHA-256
  `b5599301596f6cc8469787516184155d31a40a33c5f9d7529983a1c6a9035521`.
- Presentation corrections:
  - README transport scope now says default TCP+QUIC applies unless a transport
    is named explicitly; and
  - README and detailed evidence distinguish same-flow carrier recovery from
    new successful flows after server and client process restarts.
- No Core, Product, transport, timing, threshold, configuration, platform, or
  lab behavior changed.
- Evidence: `./.tmp/lab/results/readme-current-clean-evidence/`.

## 2026-08-06T01:54:00+08:00: ordinary multi-endpoint TCP siblings rejected

- Name: topology-derived TCP sibling usage experiment
- Category: Core aggregation model and performance acceptance
- State: rejected and fully removed; no runtime, RFC, public-documentation, or
  test behavior from the candidate is retained
- Bounded hypothesis and change:
  - tested whether every member of every explicitly regular TCP endpoint could
    remain ordinary capacity, instead of keeping correlated siblings as ready
    backups when several TCP endpoints are configured; and
  - changed only carrier usage assignment and its direct model tests and
    documentation. No score, threshold, timing, congestion controller,
    protocol frame, or platform implementation changed.
- Source verification: formatting, strict all-target/all-feature Clippy, all
  `1,471` library tests, two allocation tests, six daily-use tests, and
  doctests passed before performance acceptance. This established functional
  consistency, not performance acceptance.
- Performance rejection under the existing public five-link profile
  (`500 Mbps` per link, `20 ms` one-way delay, `10 ms` jitter, `0.5%` loss,
  two flows, 20 seconds):
  - the candidate delivered `1,066.886 Mbps` upload and an immediate focused
    repeat delivered `1,048.797 Mbps`;
  - the accepted model had previously delivered `1,383.641 Mbps`; after exact
    restoration, a clean-source, valid-host confirmation delivered
    `1,315.502 Mbps` (`1,380.180 Mbps` interval average); and
  - the restored non-diagnostic binary SHA-256 is
    `b5599301596f6cc8469787516184155d31a40a33c5f9d7529983a1c6a9035521`,
    exactly the accepted `fc9042e` build identity.
- Causal evidence: the diagnostic candidate attached all 15 TCP carriers to
  the bulk flows, put Product work on every one, and observed up to 14 TCP
  carriers with simultaneous Product work. The resulting request-direction
  fragmentation repeatedly reduced five-link upload even though five-link
  download and two-link upload could improve. Making every sibling ordinary
  is therefore not direction-neutral and is not a clean aggregation fix.
- Evidence validity: candidate rows are diagnostic experiments and are marked
  non-public-comparable because the tracked candidate was intentionally
  uncommitted; their source capture was stable, and the failure repeated. The
  restored confirmation has a clean tracked tree, valid host snapshot, exact
  receiver accounting, and `performance_comparable=true`.
- Decision: retain the established multi-endpoint primary/ready-backup model.
  Do not compensate with a new carrier cap, promotion threshold, or scheduler
  state machine without an independently coherent RFC model and matched proof.
- Evidence:
  - `./.tmp/lab/results/sibling-regular-public/`
  - `./.tmp/lab/results/sibling-regular-five-upload-repeat/`
  - `./.tmp/lab/results/sibling-regular-five-upload-diagnostic/`
  - `./.tmp/lab/results/sibling-regular-rejected-baseline-confirm/`

## 2026-08-05T23:24:22+08:00: v0.2.1 release candidate validated

- Name: immutable v0.2.1 source release
- Category: Release identity, quality gate, and repository hygiene
- State: accepted for the tag-triggered GitHub release workflow
- Scope:
  - raised the package identity and both local-package lock entries from
    `0.2.0` to `0.2.1`;
  - preserved the established seven-platform archive and tag-specific
    `version.json` contract without changing packaging or CI; and
  - replaced six Option-returning `let ... else { return None }` expressions
    with the behavior-identical `?` form required by Rust 1.96 Clippy. No
    recovery decision, threshold, timing, or data flow changed.
- Verification:
  - strict all-target/all-feature Clippy and formatting pass;
  - `1,471` library tests, two allocation tests, six packaged daily-use tests,
    all doctests, `283` patched-QUIC tests, and three patched-QUIC doctests
    pass;
  - all `213` lab contract tests, five deterministic benchmark tests, and nine
    release archive contract tests pass; and
  - shell syntax, performance registry, whitespace, self-test, and immutable
    version gates pass. The candidate is newer than `v0.2.0` and exactly
    matches `v0.2.1`.
- Environment note: the first deterministic benchmark link stopped with a
  host `SIGBUS` because the filesystem had only `46 MiB` free. Cleaning only
  the three ignored project build caches recovered `15 GiB`; the same locked
  benchmark gate then passed from a clean build.

## 2026-08-05T23:06:00+08:00: public aggregation uploads refreshed on corrected recovery

- Name: current one-, two-, and five-link request-direction evidence
- Category: Public performance evidence and documentation consistency
- State: completed on clean source `d87dc75`; README and detailed performance
  evidence now use the same current receiver-delivered values
- Results under the existing public `500 Mbps`, `20 ms` one-way delay,
  `10 ms` jitter, `0.5%` loss, two-flow, 20-second profile:
  - one physical link: `425.335 Mbps` upload;
  - two physical links: `621.237 Mbps` upload; and
  - five physical links: `1,383.641 Mbps` upload.
- The five-link repeat delivered `1,379.945` and `1,383.641 Mbps`, a `0.27%`
  difference. The older isolated `1,496.079 Mbps` value was therefore replaced
  rather than selected as a historical peak.
- Existing download and competitor rows were not rerun or changed because the
  correction is request-direction recovery and their payload paths are
  unaffected. Updated upload scaling is `1.46×` for two links and `3.25×` for
  five links relative to the current one-link upload.
- All three published runs completed with exact receiver accounting, valid
  host snapshots, clean tracked source, no command failure, and the default
  shared-transport-key TCP+QUIC profile.
- Evidence: `./.tmp/lab/results/readme-request-recovery-current/` and
  `./.tmp/lab/results/readme-request-recovery-five-repeat/`.

## 2026-08-05T22:56:00+08:00: request Data-ACK recovery regains direction-neutral flight age

- Name: exact original-flight epoch for authoritative request-gap recovery
- Category: Core recovery lifecycle and directional performance
- State: accepted; supersedes the rejected 2026-08-02 timing candidate because
  the action is now causal and its former disruption concern was retested with
  a fixed schedule
- Root cause and historical intent:
  - the request flight ledger already retained exact attachment identity and
    assignment time, and complete MPP Data ACK snapshots already supplied the
    same bounded omission authority as response snapshots;
  - request recovery discarded that assignment time and started a fresh full
    recovery interval at observation, while response recovery used original
    flight age and the established TCP `5/4 * SRTT` or QUIC `9/8 * SRTT`
    Data-ACK threshold;
  - sparse incomplete request ACKs remain intentional positive-only feedback:
    they release exact delivered ranges, never establish an omission, and may
    only fill gaps below a retained complete snapshot's horizon; and
  - the earlier `32c6ea4` attempt was correctly reverted by `584dd11` because
    its one gain had not exercised this action and its unmatched disruption
    runs regressed. It is not treated as prior acceptance.
- Bounded correction:
  - request recovery now carries the existing ledger-owned assignment epoch
    through its immutable gap observation and uses the same existing
    transport-derived Data-ACK deadline as response recovery;
  - exact unique ownership, a live original attachment, a measured distinct
    alternate that beats full recovery, retained omission authority, queue and
    flight overlap checks, native carrier backpressure, and all existing
    repair/resource envelopes remain mandatory; and
  - no frame, protocol version, threshold, scheduler score, congestion
    controller, carrier policy, capacity limit, or platform path changed.
- Causal direction evidence:
  - on the identical deterministic 20-link schedule, normal optimized download
    remained within variation at `1,235.010 Mbps` versus `1,216.058 Mbps`,
    while upload increased from `534.255` to `1,221.342 Mbps`;
  - upload delivered exactly `7,059,603,456` sink-confirmed bytes across two
    completed streams with no failures, and its upload/download ratio improved
    from `0.439` to `0.989`;
  - request logical output increased in every schedule epoch and tracked sink
    goodput, while download remained unchanged, proving a request sender
    lifecycle correction rather than probe or accounting variance; and
  - diagnostic-build goodput is excluded because hundreds of megabytes of
    per-frame trace output interfered with both directions. Those traces were
    used only to confirm that retained-authority evaluation and queue-overlap
    rejection operated as designed.
- Regression gates:
  - matched equal-fat mixed paths completed at `622.660 Mbps` download and
    `590.934 Mbps` upload;
  - TCP-only equal-fat upload delivered `676.503 Mbps` versus the latest clean
    `685.070 Mbps` reference, a `1.25%` difference inside ordinary variation;
  - under one fixed flapping schedule, candidate bulk goodput was
    `258.787 Mbps` and `250.705 Mbps` across two runs versus `205.935 Mbps`
    control, with maximum bulk gaps `2.261 s` and `0.861 s` versus `4.507 s`;
  - both candidate runs and control served `57/57` interactive requests and
    lost exactly two UDP datagrams. One candidate run had one short-request
    timeout; the repeat had zero, confirming variance rather than a persistent
    regression; and
  - all `1,471` library tests, two allocation tests, six packaged daily-use
    tests, and doctests pass. Strict Clippy is blocked only by six existing
    `question_mark` style findings in an unrelated request helper; the scoped
    lint run, formatting, and whitespace gates pass.
- Evidence: `./.tmp/lab/results/direction-gap-epoch-candidate-normal/`,
  `./.tmp/lab/results/direction-gap-epoch-candidate-gates/`,
  `./.tmp/lab/results/direction-gap-epoch-control-fixed-flap/`,
  `./.tmp/lab/results/direction-gap-epoch-candidate-fixed-flap-repeat/`, and
  `./.tmp/lab/results/direction-gap-epoch-candidate-tcp-gate/`.

## 2026-08-02T12:30:19+08:00: release identity and immutable asset contract corrected

- Name: compact versioned release contract
- Category: Release packaging, metadata, documentation, and repository hygiene
- State: accepted for subsequent releases; published v0.1.4 remains unchanged
  and immutable
- Root cause: the v0.1.2 packaging rewrite incorrectly treated `version.json`
  as private workflow evidence and deliberately changed bundle filenames to
  version-independent names. Neither change was required by the user-facing
  cleanup request.
- Corrected contract:
  - every future release contains exactly seven versioned OS/architecture
    bundles plus one public `version.json`;
  - `version.json` retains schema, product, version, tag, and source commit
    identity, then indexes all seven bundles by only `name` and immutable
    tag-specific GitHub `download_url`; compiler-version and checksum fields
    are excluded because GitHub supplies each asset digest;
  - no separate checksum manifest or per-bundle checksum sidecar is produced;
  - the project license is no longer duplicated inside every compact archive;
    the Windows-only Wintun license remains beside its bundled DLL; and
  - the publishing workflow requires an immutable published state and compares
    a downloaded `version.json` byte-for-byte with the tag/repository identity
    generated from the checked-out release source.
- Evidence:
  - all nine durable packaging contract tests pass;
  - Ruff, Bash syntax, Actionlint, Prettier, and whitespace checks pass;
  - an actual existing Linux release binary repackaged as
    `mptunnel-0.1.4-linux-amd64.tar.gz` passes the deterministic verifier with
    exactly five intended files and no project-license copy; and
  - GitHub reports repository release immutability enabled and published
    v0.1.4 as `immutable: true`; its original eight assets and tag were not
    changed.
- Cleanup: removed all ignored `./.tmp/`, root/Quinn/benchmark Cargo targets,
  Python test/lint caches, and temporary Git worktrees. Preserved ignored
  persistent `./docs-dev/` and `AGENTS.md`.
- Next bounded action: commit this exact clean correction once. A published
  release is never replaced; any corrected public bundle set requires a later
  version.

## 2026-08-02T11:21:54+08:00: complete local v0.1.4 gate passed

- Name: exact-source local release acceptance
- Category: Correctness, Product, Core, dependency, lab contract and Linux
  package verification
- State: accepted; native GitHub matrices and publication remain
- Source gates:
  - formatting and strict all-target/all-feature Clippy pass without warnings;
  - the complete root suite passes 1,511 unit/integration tests, two Product
    allocation contracts, all six packaged daily-use scenarios, and doctests;
  - the standalone maintained `quinn-proto` suite passes 282 tests and three
    doctests; and
  - the deterministic benchmark/trace suite passes all five tests.
- Infrastructure gates:
  - the performance declaration/registry is valid and all 199 lab contract,
    evidence, runner, and observation tests pass;
  - all nine release-archive contract tests, Bash syntax checks, ShellCheck,
    version-gate self-tests, actual `v0.1.4` monotonic-version check, public
    local-link checks, whitespace checks, and tracked repository-shape checks
    pass.
- Local package evidence:
  - `packaging/package-release.sh --target x86_64-unknown-linux-musl` built the
    version `0.1.4` binary through the documented package path;
  - the result is an x86-64 static PIE and the archive verifier found exactly
    the six contracted Linux package files; and
  - `.tmp/release/dist/mptunnel-linux-amd64.tar.gz` has SHA-256
    `38afcef15d0d5497ab1c6c98ea0b5c85f21cca9feefcaf036f2efc9c6ed6bf98`.
- Decision: freeze this source. Push its exact commit without changing the
  workflows, require the normal CI and non-publishing Release Check to build
  all seven targets successfully, then create and push the annotated
  `v0.1.4` tag. The tag-triggered workflow alone may publish release assets.

## 2026-08-02T11:13:34+08:00: v0.1.4 Product and release surface closed

- Name: bounded v0.1.4 Product/documentation closure
- Category: Product acceptance, public documentation, dependency and release
  metadata
- State: accepted for the final local and native-CI gates; no Product, Core,
  protocol, scheduler, congestion, timing, carrier, or platform policy changed
- Product evidence:
  - `cargo test --locked --test product_daily_use_acceptance -- --nocapture`
    passes all six packaged scenarios, covering local TCP/UDP forwarding,
    SOCKS5 and MPP egress, ordered routing, DNS/ACL policy, balancers, runtime
    validation/apply/persistence, offline new-flow rejection, process restart,
    default `config.toml` loading, and structured log output/rotation;
  - the dashboard retains a management token in same-origin local storage only
    after successful authentication and forgets it explicitly or after server
    rejection; and
  - the public configuration vocabulary uses canonical resource `name` values
    and noun-matched references (`inbounds`, `outbound`, `balancer`, and
    `dns_plan`); `_id` remains reserved for protocol, principal, and signed
    identities.
- Repository and documentation closure:
  - `Cargo.toml` and both maintained lockfiles identify version `0.1.4`;
  - public README, architecture, lab, performance, operations, and code-layout
    documents now describe MPP v5, current Product/Core ownership, bounded TCP
    carrier groups, honest matched performance evidence, logging, dashboard
    authentication, supported platforms, and the conventional release shape;
  - internal version-development plans were removed from the public `docs/`
    tree and retained locally under ignored `./docs-dev/`; `PROGRESS.md` remains
    the sole progress ledger; and
  - no tracked `third_party`, generated license report, `about.toml`, raw
    distribution directory, or temporary test-data directory exists.
- Dependency evidence: the maintained Quinn source remains pinned to newest
  stable `quinn-proto` 0.11.16 and `quinn` 0.11.11. Its documented eight-file
  production delta and upstream-refresh procedure remain intact; no dependency
  or congestion-controller change is justified for this release.
- Next bounded action: run the complete local source/package gate, commit the
  exact candidate, require the unchanged native GitHub CI and non-publishing
  Release Check matrices to pass, tag that commit as `v0.1.4`, verify the eight
  published assets from scratch, and remove only generated scratch/build
  caches.

## 2026-08-02T10:55:11+08:00: non-cumulative condition-handover lab restored

- Name: complete link-condition epochs for the mixed disruption fixture
- Category: Performance methodology and release-gate correctness
- State: accepted; no Product, Core, RFC, timing, or parameter change was
  required
- Reproduced fixture defect:
  - each seeded event changed one qdisc but retained every mutation from prior
    events, so some random histories silently accumulated blackholes or severe
    spikes across all configured carriers;
  - the case nevertheless required a persistent echo request with a five-second
    application deadline to survive that accidental all-link outage; and
  - passing seeds had avoided the cumulative outage while failing seeds had
    created it, explaining the apparent run-to-run protocol instability.
- Clean correction:
  - every event is now one complete condition epoch: restore the declared
    baseline, then apply the selected condition;
  - the default schedule contains baseline recovery plus one-link spike and
    blackhole conditions, so changing history cannot transform handover into a
    different total-outage experiment;
  - the evidence metadata records the transition model, and the existing trace
    still records exact selected modes, dwell periods, command timing, and
    command results; and
  - complete outage/restore remains a distinct lifecycle contract, including
    the durable five-second same-flow reattachment test and packaged
    offline/restart acceptance.
- Causal evidence:
  - with the exact formerly failing seed, ordered mode sequence, and hold
    sequence, the cumulative fixture delivered `99.385` Mbit/s, disconnected
    the persistent echo flow after 9/31 successes, lost 12.5% of datagrams, and
    had a 2.633-second maximum bulk gap;
  - the corrected epoch fixture delivered `199.128` Mbit/s, kept the same echo
    flow alive for 29/29 requests, completed all 38 small transfers, lost 1.8%
    of datagrams during faults, and bounded the maximum bulk gap to 0.784
    seconds; and
  - both traces completed with zero command failures and the same selected
    schedule digest `272b801d6b21742674caa7b4bf63b445852e553317e37c3f134020518d1d841a`.
- Clean release-condition confirmation:
  - clean commit `046977b` with the default condition set delivered `224.069`
    Mbit/s, bounded its maximum bulk gap to 0.717 seconds, kept the existing
    echo flow alive for 32/32 requests, completed 47/47 small transfers, and
    delivered 134/134 datagrams;
  - the complete seeded trace and one-second management/container telemetry
    are retained, and no product source or release binary changed between the
    preceding accepted Core gate and this run; and
  - the host-validity gate remained false solely because one unrelated Docker
    container was running, so this is local release engineering evidence and
    is not presented as an independently reproducible public benchmark.
- Adjacent QUIC regression check:
  - the retained historical `952c61a4...` binary and current
    `bc7bdac4...` binary were run back-to-back on the identical five-path,
    500-Mbit/s, 180-ms one-way-delay, zero-loss profile;
  - they delivered `671.356` and `648.493` Mbit/s with maximum ordered gaps of
    0.350 and 0.314 seconds respectively, placing the current binary 3.4%
    below that adjacent historical observation while improving its gap; and
  - the historical binary itself did not reproduce its earlier isolated
    `776.116` Mbit/s row. No QUIC, congestion, pacing, scheduler, recovery, or
    parameter change is justified by the apparent peak-to-current difference.
- Verification: focused flapping/runner contract tests pass (29 tests), Bash
  syntax, ShellCheck apart from its pre-existing dynamic-source information,
  formatting, and whitespace checks pass.
- Evidence: `./.tmp/lab/results/v014-reverted-final-disruption-matrix` and
  `./.tmp/lab/results/v014-flapping-epoch-fix-causal`, and
  `./.tmp/lab/results/v014-final-condition-handover`,
  `./.tmp/lab/results/v014-quic-historical-current-host`, and
  `./.tmp/lab/results/v014-quic-current-current-host`.

## 2026-08-02T10:41:00+08:00: request recovery timing change rejected by disruption A/B

- Name: request-side Data-ACK recovery epoch review
- Category: Core performance safety and release-model closure
- State: rejected and reverted; the established request recovery interval is
  authoritative for v0.1.4
- Candidate and causal check:
  - the candidate reused the original request flight epoch when evaluating an
    acknowledged gap, matching the response direction's retained-gap clock;
  - it improved one extreme TCP upload diagnostic from `72.562` to `265.850`
    Mbit/s and preserved matched stationary throughput within `0.76%`;
  - however, the candidate's intended request ACK-gap recovery action was not
    exercised in its failing mixed-carrier diagnostic: no client
    `stream_ack_received` event made ACK-gap reinjection ready, and its only two
    client reinjections were existing path-failure recovery; and
  - therefore the isolated upload result did not prove that the proposed
    lifecycle change caused the gain.
- Disruption safety evidence:
  - two clean normal candidate runs lost interactive progress after only one of
    31 requests and delivered `109.835` and `100.525` Mbit/s; a third remained
    functional at `189.183` Mbit/s;
  - two clean controls at the preceding source remained functional at
    `219.566` and `270.738` Mbit/s; and
  - matched diagnostic runs strengthened the safety signal: the control served
    29/31 interactive requests, whereas the candidate served 16/31 before
    stalling. Both were rejected by the host-validity gate, so these are causal
    engineering diagnostics rather than publishable benchmark claims.
- Decision:
  - reject the candidate rather than retain an unproved timing asymmetry fix
    with a correlated disruption regression;
  - preserve the established full request recovery interval and the already
    accepted directional-owner data flow; and
  - make no replacement timing, threshold, scheduler, congestion-control,
    carrier-range, or platform-specific change. Continue directly to the
    frozen release gates and product/documentation closure.
- Evidence: `./.tmp/lab/results/v014-final-flapping-repeat-{1,2}`,
  `./.tmp/lab/results/v014-flapping-request-recovery-diagnostic`,
  `./.tmp/worktrees/cde7382/.tmp/lab/results/flapping-current-host-repeat`,
  `./.tmp/worktrees/cde7382/.tmp/lab/results/flapping-current-host-diagnostic`,
  and commit `584dd11`.

## 2026-08-02T09:41:03+08:00: apparent TCP regression rejected by matched-profile control

- Name: historical-binary and immutable-path comparison
- Category: Core performance diagnosis and release-gate integrity
- State: no Core change is justified; continue the frozen representative and
  disruption release matrix from the clean source
- Rejected diagnosis:
  - the first clean-source preflight appeared to place single-path TCP at
    `135.866-165.630` Mbit/s, below the earlier `293.678` Mbit/s result;
  - the exact historical `952c61a44608...` executable produced only `154.606`
    Mbit/s when rerun under the current low-result fixture, proving that neither
    the demand-episode correction nor retained Data-ACK recovery created that
    difference; and
  - retained qdisc state and run manifests then exposed the incomparable
    profiles: the historical run explicitly set the fat path to `0%` loss,
    while the low-result fixtures used the maintained lab default of `1%`.
- Matched-profile evidence:
  - the current clean normal release binary delivered `285.871` Mbit/s under
    the historical `500mbit`, `180ms`, `20ms`, `0%` fat-path profile, within
    `2.7%` of the historical value and inside the explicitly accepted run
    fluctuation rather than a performance downgrade;
  - two clean-source diagnostic repeats under that same immutable profile
    delivered `347.316` and `306.187` Mbit/s, and the additional TCP carrier
    service was both admitted and productive; and
  - the existing probe trace records 0.2-second Product delivery while the
    existing management and container collectors record one-second path,
    queue, flight, logical-progress, and physical-rate evidence. They show
    buffered Product delivery bursts over a comparatively steadier physical
    service rate; this is retained as diagnosis evidence, not converted into a
    timing tweak or a new release threshold.
- Decision:
  - preserve the clean Product-demand and recovery lifecycles already proved
    by deterministic regressions;
  - compare performance only under identical recorded path conditions, and do
    not treat the normal `1%`-loss profile as a regression against a `0%`-loss
    historical control; and
  - make no parameter, timing, congestion-control, scheduler, carrier-range,
    or platform-specific change from this result.
- Evidence: `./.tmp/lab/results/v014-tcp-single-normal-paired`,
  `./.tmp/lab/results/v014-tcp-single-diagnostic`,
  `./.tmp/lab/results/v014-tcp-single-diagnostic-2`, and
  `./.tmp/worktrees/909de0a/.tmp/lab/results/old-909-current-host-tcp-single`.

## 2026-08-02T08:30:24+08:00: retained Data-ACK recovery lifecycle restored

- Name: target-serviced persistent gaps with one directional stream owner
- Category: Core recovery correctness and causal performance restoration
- State: source and correctness gates accepted; the clean-source representative
  and disruption matrix is the next release gate
- Reproduced defect:
  - response recovery serviced an authoritative multi-megabyte Data Sequence
    gap with one fixed 14,600-byte liveness quantum, then suppressed the
    accepted alternate copy with the degraded original carrier's recovery
    clock;
  - after a frontier advance, response recovery could lose its retained gap
    between the ACK event and later target eligibility, while request recovery
    could lose a due timer when no target was eligible at that exact event; and
  - Product queue and flight accounting used incomplete or stale views, which
    could either over-admit a target or delay a refill that current service
    authority allowed.
- Clean model:
  - one authoritative gap identity is retained until its lowest missing
    frontier advances or resolves; ACK receipt, timer expiry, output/path-model
    publication, and carrier-capacity release all return through the existing
    directional stream owner;
  - a measured alternate may fill only its available Product service window,
    with queue plus flight summed within native and Product domains and the
    overlapping domain totals counted once; current shared queue state is
    authoritative over an older publication;
  - once a recovery copy is accepted, its immutable repeat deadline belongs to
    that selected alternate. The original-owner silence fallback remains a
    separate one-shot authority armed only by fresh Data-ACK receipt; and
  - retained qualified delivery evidence survives a later app-limited poll,
    while an unqualified native carrier reacquires only through its current
    native congestion credit. No stale Product window becomes native send
    authority.
- Adjacent causal evidence:
  - the former fixed-quantum run delivered `60.895` Mbit/s and retained a
    greater-than-4.5-second ordered plateau; a target-service-window diagnostic
    removed that plateau and delivered `222.156` Mbit/s before the accounting
    audit;
  - after exact accounting and lifecycle ownership, three normal release runs
    delivered `208.174`, `197.043`, and `210.795` Mbit/s. Maximum ordered gaps
    were `2.250`, `0.768`, and `0.798` seconds, with bulk success in all three,
    zero interactive failures, and zero/one/zero datagram losses respectively;
  - the first run's longer ordered gap occurred after the injected fat path was
    permanently changed to 20 Mbit/s, 900 ms one-way delay, 250 ms jitter, and
    10% loss. Its 2.250-second gap was shorter than the path's measured
    2.823-4.331-second native RTT; physical traffic, logical output, and healthy
    path delivery continued before buffered ordered data was released; and
  - these runs are causal diagnostics, not publishable benchmark rows: the
    versioned host gate rejected the dirty source and one unrelated external
    container. Evidence is retained under
    `./.tmp/lab/results/v014-repair-retained-lifecycle-*` until the release
    matrix is closed.
- Verification:
  - the optimized all-feature suite passes 1,511 library tests, two allocation
    tests, six packaged daily-use acceptance tests, and doctests;
  - focused persistent-gap and tail-recovery suites pass 23 and 33 tests, and
    the durable service-accounting regression covers current queue authority;
  - strict all-target/all-feature Clippy, formatting, and whitespace checks
    pass; and
  - authoritative review plus a bounded independent read-only review found no
    remaining recovery ownership, target binding, deadline, accounting, or
    request/response symmetry discrepancy.
- Next: commit this bounded Core milestone, rebuild once from the clean source,
  then execute only the frozen stationary aggregation, latency/blackhole/link
  swap, outage/restart, and port-hopping release gates. Do not change the model
  unless a reproducible causal lifecycle or data-flow contradiction remains.

## 2026-08-02T05:32:13+08:00: reliable response demand-episode self-lock removed

- Name: directional Product-episode admission across idle transitions
- Category: Core lifecycle correctness and repeated disruption proof
- State: deterministic mixed-carrier terminal stall is corrected and accepted;
  the frozen representative and disruption matrix remains the next release
  gate
- Causal defect:
  - the Product classifier correctly ended a drained throughput-demand episode
    at its established idle boundary, but latency-oriented source admission
    continued subtracting the reliable stream's lifetime Data Sequence offset;
  - after one long response exceeded the adaptive classification boundary,
    lifetime-based credit remained permanently zero, so the target socket was
    no longer polled and no fresh demand could reclassify the stream; and
  - causal diagnostics reproduced the self-lock exactly when Product ACK
    reached the full 635,733,664-byte response frontier, pending delivery and
    queues reached zero, and the lane returned to latency-oriented service.
- Clean correction:
  - finite response-source admission is now owned by the existing directional
    Product demand tracker and its resettable episode bytes;
  - the admission and unconditional classifier retain the same existing
    adaptive boundary, and the existing idle transition alone grants a fresh
    bounded episode;
  - moving admitted response data from its queue into assigned offsets does not
    count it twice, while queued or pending delivery continues to prevent an
    idle reset; and
  - no timer, threshold, configured limit, scheduler score, transport branch,
    congestion controller, pacing rule, or platform-specific path changed.
- Repeated normal-release evidence under the frozen mixed TCP+QUIC balanced
  blackhole profile:
  - four consecutive 20-second runs delivered 306.607, 307.406, 386.169, and
    392.997 Mbit/s, with maximum bulk read gaps of 0.486, 0.791, 0.393, and
    0.567 seconds respectively;
  - all four retained continuous bulk progress through the former failure
    point, with zero interactive and small-flow failures; the earlier repeated
    5-14 second plateaus did not recur;
  - realtime datagram loss was 0%, 1.948%, 0.543%, and 0.495% during the
    injected blackhole and remains a separate frozen disruption-matrix result,
    not justification for another Core change; and
  - one-second management and container snapshots are retained under
    `./.tmp/lab/results/v014-epoch-fix-{1,2,3,4}`. Formal comparability remains
    false solely because the versioned host-validity gate detects the unrelated
    external Docker container already documented for this host.
- Verification:
  - a durable composed lifecycle regression proves historical bulk, complete
    drain, idle reset, fresh bounded credit, exact credit consumption, and
    promotion at the unchanged boundary;
  - `cargo test --locked --all-features` passes 1,507 library tests, two
    allocation tests, six packaged daily-use acceptance tests, and doctests;
  - strict all-target/all-feature Clippy, formatting, and whitespace checks
    pass; and
  - the normal release binary was rebuilt without lab diagnostics before every
    repeated performance run.
- Next: execute the already-frozen representative and disruption matrix,
  including healthy controls, whole-link condition swap, outage/restart,
  port-hopping, latency, resources, and competitor baselines; change the model
  only if a reproducible causal lifecycle or data-flow contradiction remains.

## 2026-08-02T03:59:13+08:00: elastic TCP validation restores useful parallel service

- Name: RFC-conformant `1..3` TCP carrier validation in both Product directions
- Category: Core lifecycle, causal measurement, and targeted performance proof
- State: implementation, correctness, and the preregistered single-flow-QoS
  gate are accepted; the frozen representative and disruption matrix remains
  the next release gate
- Clean model:
  - the existing Product classifier owns a continuous throughput-demand
    episode; successful work-conserving queue drains neither mint a generation
    nor withdraw an unchanged comparison;
  - each comparison freezes its ordinary membership and model-derived byte
    geometry, requires complete candidate and ordinary service coverage, and
    advances only after ordered Product-ACK receipts resolve exact candidate
    assignments;
  - a validation-only attachment is a live owner of its exact ordered Product
    flight and recovery while remaining outside ordinary scheduling; RETAIN
    atomically converts that owner into ordinary membership, while failure or
    withdrawal settles it before the existing recovery path can act; and
  - candidate flight reuses the existing unproven-path startup and mature
    Product envelopes. No scheduler score, congestion controller, timing,
    percentage, carrier-range default, or new fixed threshold was introduced.
- Reproduced root cause and correction:
  - the candidate's original flight was recorded in the shared request ledger
    while the candidate was absent from the live owner topology;
  - ordinary placement consequently observed missing original ownership before
    candidate dispatch, reinjected the work, and serialized upload validation;
  - making the validation attachment an explicit lifecycle owner corrects that
    data-flow contradiction without granting it ordinary scheduling authority;
    and
  - the one stale actor test was corrected to withdraw at a real classifier
    transition rather than a momentary queue drain; it then passed 50
    consecutive executions.
- Targeted 100-Mbit/s per-flow-QoS evidence:
  - client-to-server upload improved from 76.088 Mbit/s with `1..1` to
    121.871 Mbit/s with `1..3` in the paired root-cause run;
  - in the complete eight-case confirmation, download improved from 75.246 to
    133.130 Mbit/s and upload from 75.675 to 139.136 Mbit/s;
  - shared-bottleneck download was 158.424 versus 150.129 Mbit/s, a 5.2%
    adjacent-run difference, while upload was 158.748 versus 159.831 Mbit/s;
    no strict fluctuation cap is inferred from adjacent lab samples; and
  - every case reported zero recovery gap and zero qdisc drops. The formal lab
  host-validity gate remained false solely because an unrelated external
  Docker container was already running, so release acceptance must use a
  clean host or the exact CI environment.
- Rate-stability diagnosis:
  - the 200-ms upload trace attributes each cumulative ACK delta to its receipt
    bucket, so its zero/spike pattern is not an instantaneous-rate signal;
  - one-second interface counters reproduce the same samples 11/22 troughs in
    `1..1` controls, shared-bottleneck runs, and shaped direct/Xray/Hysteria
    traffic without carrier failure, retirement, qdisc drops, or ownership
    change; and
  - the troughs correlate with bounded sender-side qdisc backpressure and
    resume on ACK release. No MPTUNNEL change is justified unless an unshaped
    representative case reproduces instability.
- Verification:
  - `cargo test --locked --all-targets --all-features` passes 1,507 library
    tests, two allocation tests, and six packaged daily-use acceptance tests;
  - `cargo clippy --locked --all-targets --all-features -- -D warnings`,
    formatting, and whitespace checks pass; and
  - the correction is symmetric across neutral request/response Product
    ownership and contains no OS-specific branch.
- Next: commit this bounded Core milestone, then run the frozen representative
  throughput, aggregation, failover, whole-link swap, outage/restart, latency,
  resource, and competitor-baseline matrix without further model change unless
  it exposes a reproducible lifecycle or data-flow defect.

## 2026-08-02T00:41:10+08:00: maintained Quinn baseline updated to 0.11.16

- Name: exact `quinn-proto` upstream refresh with the MPTUNNEL patch intact
- Category: Core dependency provenance and performance change control
- State: source, dependency, and correctness gates accepted; matched runtime
  performance remains the next release gate
- Provenance and port:
  - the crates.io `quinn-proto-0.11.16.crate` archive has SHA-256
    `2f4bfc015262b9df63c8845072ce59068853ff5872180c2ce2f13038b970e560`;
  - the upstream source change is the rand 0.10 migration and its published
    dependency baseline, including the upstream BBR RNG change from `StdRng`
    to `Pcg32`;
  - all nonoverlapping upstream files were applied unchanged, while the four
    overlapping files retain both upstream's `RngExt`/`Pcg32` changes and the
    existing MPTUNNEL delivery-sampling plumbing; and
  - a clean source comparison leaves exactly the documented MPTUNNEL BBR,
    delivery-state, pacing, fresh-network-path, and associated regression-test
    files different from upstream 0.11.16.
- Repository contract:
  - the root manifest now pins exactly `=0.11.16` and the root, standalone
    Quinn, and benchmark lockfiles resolve that local path package;
  - `./crates` still contains only the complete `quinn-proto` mirror; and
  - the fork README and Core performance plan record the new checksum,
    semantic delta, overlap handling, and reproducible update procedure.
- Verification:
  - the standalone fork passes 282 unit tests and three doc tests;
  - `cargo test --locked --all-targets --all-features` passes 1,503 library
    tests, two allocation tests, and six packaged daily-use acceptance tests;
  - the benchmark crate passes all five deterministic tests;
  - both root and benchmark `cargo tree` output identify only local
    `quinn-proto v0.11.16`; and
  - root clippy with warnings denied, formatting, and whitespace checks pass.
- Performance boundary: the MPTUNNEL congestion, pacing, scheduling, timing,
  and protocol parameters did not change; the preregistered matched QUIC and
  full representative matrix must still prove that the upstream refresh did
  not regress observable behavior.
- Next: run the frozen representative performance, aggregation, failover,
  whole-link swap, outage/restart, latency, resource, and competitor-baseline
  gates; change code only for a reproducible root cause.

## 2026-08-02T00:30:13+08:00: server-to-client elastic TCP transaction closed

- Name: exact S2C TCP carrier validation, retention, and recovery
- Category: Core RFC conformance and dynamic carrier lifecycle
- State: implementation and correctness gates accepted; representative
  performance remains a separate release gate
- Clean model:
  - the server admits one exact unpublished S2C output from existing Product
    saturation and evaluates it with the same RFC validation state, Product
    cohorts, writer boundaries, ACK evidence, and work-zero rule as C2S;
  - candidate dispatch performs its output-capacity reservation before Product
    mutation, then records exact recoverable flight before infallible command
    publication;
  - the client publishes exact zero-authority local ownership before a positive
    wire acknowledgement, grants only S2C authority, and rolls the publication
    back if acknowledgement or handoff does not complete;
  - a retained S2C carrier enters a separate receive/feedback attachment set,
    so multiple inputs can deliver data and publish cumulative ACK/MAX_DATA
    without acquiring ordinary C2S scheduling, load, OPEN, or membership
    authority; and
  - rejection, expiry, and owner loss use the existing ordered STREAM_DETACH,
    PATH_DRAIN, exact PATH_CLOSE, native-close, and exact-flight recovery
    boundaries without a new timeout or retry loop.
- Concrete corrections found by executable validation:
  - retained-service identity now compares immutable transaction identity
    rather than the mutable Establishing/Validating phase;
  - terminal RETAIN lifecycle advancement no longer re-enters the actor loop;
  - unpublished validation outputs remain visible to exact liveness, recovery,
    and peer-usage state without becoming ordinary outputs; and
  - an eligible validation candidate supplies existing sender-drain readiness,
    preventing a queued candidate from waiting forever behind a saturation
    wake condition.
- Verification:
  - `cargo test --locked --all-targets --all-features` passes 1,503 library
    tests, two allocation tests, and six packaged daily-use acceptance tests;
  - durable tests cover pre-ACK authority, rollback, negative ordered drain,
    receive-only fan-in/feedback, unpublished-output liveness, peer usage, and
    exact registry ownership;
  - `cargo clippy --locked --all-targets --all-features -- -D warnings`,
    formatting, and whitespace checks pass; and
  - no congestion controller, scheduler score, carrier-range value, model
    threshold, or protocol timing changed in this slice.
- Next: update the maintained `quinn-proto` fork to its current compatible
  upstream while preserving and documenting the narrow MPTUNNEL patch, then
  run the frozen representative performance and disruption matrix.

## 2026-07-31T21:32:01+08:00: process logging surface aligned with real output

- Name: four-level operator logging model
- Category: Product configuration and observability
- State: accepted; configuration now exposes only behavior implemented by the
  process event system
- Correction:
  - TOML, CLI, and environment configuration accept `off`, `error`, `warn`,
    and `info`;
  - the previously accepted `debug` and `trace` values were removed because
    production has no events at either level and both behaved identically to
    `info`;
  - flow-event logging consequently requires `info`; and
  - operator and reference documentation describe the same four-level model.
- Verification:
  - focused file-schema, CLI, and packaged live-logging-update tests pass;
  - formatting and whitespace checks pass; and
  - the change adds no event call, transport branch, timer, payload work, or
    Core behavior.
- Next: complete the RFC-defined bounded TCP carrier lifecycle, beginning with
  one shared reservation/identity model used by elastic establishment and
  planned ranged-port replacement.

## 2026-07-31T21:23:30+08:00: portable daily-use Product graph accepted

- Name: packaged ingress, egress, and DNS-authority matrix
- Category: Product executable acceptance
- State: accepted; the remaining high-value portable assembly paths are
  exercised through one packaged process without adding Product behavior
- Evidence:
  - HTTP CONNECT reaches direct TCP through an explicit host override, proving
    local Product resolution and destination authorization;
  - a fixed TCP forward selects an HTTP CONNECT outbound, whose test peer
    verifies the canonical domain authority is delegated unchanged;
  - SOCKS5 UDP ASSOCIATE and a fixed UDP forward both complete through direct
    native UDP; and
  - the existing packaged matrix continues to prove routing rejection,
    SOCKS5-outbound failover, MPP-to-direct egress, offline rejection,
    configuration persistence, and process restart recovery.
- Existing nonduplicated evidence:
  - the full runtime already proves one established SOCKS5 TCP flow remains
    open through a five-second total carrier outage and resumes on the same
    logical flow after fresh QUIC attachment;
  - focused integration tests continue to cover forwarding limits and cleanup,
    HTTP/HTTPS and SOCKS connector mechanics, routed remote DNS transport,
    encrypted DNS capture, FakeDNS, and TUN L4 packet handling.
- Verification:
  - `cargo test --all-targets --all-features --quiet` passes 1,427 library
    tests, two allocation tests, and six packaged daily-use acceptance tests;
  - `cargo clippy --all-targets --all-features -- -D warnings`, formatting, and
    whitespace checks pass.
- Performance boundary: only test support and one packaged acceptance scenario
  changed; the release binary is byte-identical to the preceding accepted
  Product milestone.
- Next: correct the proven ranged-TCP port-hopping lifecycle mismatch, then
  restore the deterministic whole-link quality-swap timing gate.

## 2026-07-31T21:14:51+08:00: Product availability presentation aligned

- Name: authenticated-carrier management projection
- Category: Product observability and operator presentation
- State: accepted; one previously separate presentation authority now reads
  the same generation-owned inventory as new-flow admission
- Correction:
  - MPP outbound session state and carrier count now come from exact
    authenticated TCP/QUIC connection lifetimes, not the optional peer
    diagnostics broker;
  - the three states are `connecting` before first authentication,
    `connected` while a carrier is live, and `offline` after the last carrier
    is lost;
  - retained established-flow counts no longer overwrite carrier availability,
    while peer diagnostics remain independently reported; and
  - the dashboard presents the three availability states explicitly.
- Documentation corrections: fixed the packaged server CLI example, described
  the intentionally visible local MPP path endpoints accurately, and recorded
  logging-only live configuration updates.
- Verification:
  - the management test proves initial, connected, and offline projection while
    a diagnostic registration remains live and an established flow is retained;
  - `cargo test --all-targets --all-features --quiet` passes 1,427 library
    tests, two allocation tests, and five daily-use acceptance tests;
  - `cargo clippy --all-targets --all-features -- -D warnings`, release build,
    formatting, and whitespace checks pass; and
  - the release binary is
    `a117f6135dffb84b85b71024684f660c3efe2f4d4c574f36481b4565280b36b0`.
- Performance boundary: projection samples one already-existing Product
  inventory snapshot; no carrier, payload, scheduler, congestion, recovery,
  timing, or RFC value changed.
- Next: close only missing high-value executable Product evidence, then run the
  deterministic disruption timing and competitive-performance gates.

## 2026-07-31T21:01:02+08:00: offline new-flow admission and restart recovery accepted

- Name: authenticated-carrier Product availability
- Category: Product admission, disruption recovery, and daily-use acceptance
- State: accepted; the implementation is carrier-neutral, generation-owned,
  and absent from established-flow and Core payload paths
- Clean model:
  - each MPP client outbound owns one exact inventory containing live
    authenticated-carrier count and whether that generation has ever
    authenticated a carrier;
  - a non-cloneable RAII registration is published after authenticated
    readiness and retained by the exact TCP or QUIC connection, independently
    of diagnostic-control streams;
  - before first authentication, bounded initial establishment remains
    permitted; one or more registrations make the outbound available; loss of
    the last registration makes it offline until ordinary background
    reconciliation authenticates a replacement;
  - a new configuration generation starts with a fresh inventory, while an
    established Product flow never rechecks availability; and
  - no source address, endpoint locator, timer, polling loop, transport metric,
    path score, or payload observation enters the state transition.
- Product behavior:
  - direct MPP selection rejects a new flow before DNS/connect work while
    offline;
  - balancers skip offline MPP leaves without marking them failed, continue to
    consider initial MPP and native leaves, and return typed outbound
    unavailability only when no concrete attempt supersedes it;
  - SOCKS5 CONNECT returns `network unreachable`, HTTP CONNECT returns `503`,
    new TCP-forward/TUN TCP sockets close, and UDP is silently discarded;
  - an offline UDP lane is not retained as a policy denial, so a later
    datagram can use a newly authenticated carrier; and
  - captured local TUN DNS remains ahead of Product routing and is unchanged.
- Verification:
  - the packaged acceptance scenario proves initial use, server kill,
    authenticated-carrier withdrawal, `0x03` rejection for a new flow, server
    restart and renewed use, then client kill/restart and renewed use;
  - direct initial/offline and mixed/all-offline balancer tests pass, TCP/QUIC
    actor tests prove exact connection publication, and SOCKS5/HTTP protocol
    mappings pass;
  - `cargo test --all-targets --all-features --quiet` passes 1,427 library
    tests, two allocation tests, and five daily-use acceptance tests;
  - `cargo clippy --all-targets --all-features -- -D warnings`,
    `cargo build --release --locked --bin mptunnel`, formatting, and whitespace
    checks pass; and
  - the release binary is
    `4cdd01e846bdc4599684b75137886869146cd4e7d36847faf9ba2d9c1ee77b94`.
- Performance boundary: the only recurring operation is one mutex snapshot per
  new concrete MPP flow selection; registration changes occur only on carrier
  authentication/teardown. No per-byte/per-packet work or Core value changed.
- Next: audit and close only remaining proven daily-use Product gaps, then
  restore a deterministic whole-link quality-swap lab with separate delivery
  interruption and post-transition steady-performance timing.

## 2026-07-31T20:41:18+08:00: exact-attachment feedback and recovery accepted

- Name: retained stream feedback, exact recovery, and transition timing
- Category: Core RFC conformance and performance change control
- State: accepted; correctness, steady-state, and representative transition
  gates pass without changing a scheduler, congestion-control, pacing, window,
  carrier-range, threshold, or timing value
- Clean model:
  - cumulative `STREAM_DATA_ACK` and `STREAM_MAX_DATA` are independent retained
    state and are published on every exact live attachment;
  - publication remains pending per attachment until that attachment accepts
    the authoritative generation, including terminal stream feedback;
  - request and response recovery use exact owned ranges, the existing RFC
    retransmission interval, and capacity/membership notifications;
  - stale placement is valid only while a distinct non-stale live alternative
    exists, so a sole survivor is restored rather than excluded; and
  - the opening response grant is fenced as already published on its opening
    attachment, preventing a duplicate initial `STREAM_MAX_DATA`.
- Practical root cause and correction:
  - mixed-carrier blackhole runs had plateaued for 7--14 seconds near
    57--59 Mbps because response ACK state could be accepted only by a
    locally-live but wire-blackholed selected attachment;
  - exact retained publication removes that single-attachment authority while
    preserving one cumulative logical feedback state;
  - redundant generations are detected by logical subsumption before
    cache/flight/recovery work, retaining exact publication without a steady
    CPU regression; and
  - no unreliable datagram retransmission was added: the small datagram loss
    around an induced underlay blackhole remains the declared unreliable
    Product behavior.
- Correctness evidence:
  - `cargo test --all-targets --all-features --quiet` passes 1,421 library
    tests, two allocation tests, and four daily-use acceptance tests;
  - `cargo check --all-targets --all-features`,
    `cargo clippy --all-targets --all-features -- -D warnings`,
    `cargo build --release --locked --bin mptunnel`, formatting, and whitespace
    checks pass; and
  - durable tests cover redundant ACK subsumption, per-attachment terminal ACK
    retention on both roles, sole-survivor stale restoration, exact recovery
    suppression, and retained credit replay.
- Performance evidence:
  - two final balanced transition cohorts record mixed/TCP-download goodput of
    282.250--293.900/256.577--270.927 Mbps, local recovery of
    0.240--0.480 seconds, and application interruption of
    0.402--1.998 seconds;
  - the final representative steady matrix records TCP/QUIC single-path
    download 123.899/235.408 Mbps, upload 130.473/251.006 Mbps, multipath
    download 784.600/694.429 Mbps, and multipath upload
    514.499/748.483 Mbps;
  - duration-upload rows marked `loss` terminated their non-finalized probe
    sockets at the configured duration; their target-confirmed byte accounting
    and goodput remain valid, and no Product patch is justified by that label;
  - the release binary is
    `b8f4c276fc3167e7a2db8ddc84c80cd63597637ed10f4de20473bba36b5ce201`;
    retained source-diff evidence is
    `cc7112172103ccf6c1edfa709d0deb7c8e33f5d8cf99aaddf0c912042744efdb`;
    and
  - detailed local evidence is retained under the ignored
    `./.tmp/lab/results/core-final-balanced-transition-{1,2}` and
    `./.tmp/lab/results/core-final-representative-1` directories.
- Next: add the generation-owned Product offline admission boundary and
  restart acceptance without touching existing flows or Core behavior, then
  validate whole-link quality-swap timing with a deterministic lab whose
  convergence metric actually measures post-transition steady performance.

## 2026-07-31T15:02:46+08:00: ordered TCP retirement correctness candidate

- Name: exact carrier drain, planned replacement, and native evidence cleanup
- Category: Product carrier lifecycle, RFC correction, and Core change control
- State: correctness gates pass; this remains a performance-unaccepted
  candidate until adjacent representative labs complete
- RFC correction:
  - removed the unproved two-pre/two-assisted/two-post sample geometry,
    universal percentages, sample bytes, validation timers, and timed
    contraction from the previously recorded elastic-controller model;
  - retained only bounded admission, finite exact Product evidence, strict
    target and aggregate improvement, exact result settlement, and zero-work
    retirement; and
  - automatic elastic expansion remains disconnected. No removed controller
    rule is replaced by another threshold, timer, estimator, or vocabulary.
- Implemented Product lifecycle:
  - the client alone initiates ordered TCP `PATH_DRAIN`; the server alone emits
    the matching `PATH_CLOSE`, and wrong-direction, unsolicited, duplicate, or
    path-mismatched lifecycle frames fail closed;
  - one carrier-instance admission fence rejects fresh Product work while
    preserving every command whose queue reservation crossed the boundary;
    drain order remains retirement, control, priority, reinjection, then data;
  - the existing non-restarting session-retention deadline bounds the complete
    graceful drain operation on each endpoint; expiry closes the exact native
    carrier and enters ordinary recovery without synthesizing peer authority;
  - management disable drains exact minimum carriers, immediate re-enable
    cannot revive a draining actor, and terminal actors are replaced with
    fresh physical instances under the same logical session; and
  - planned retirement is distinct from failure, exact server detach
    completion is awaited, and Product streams retain aggregate state across
    the carrier replacement.
- Evidence and health ownership:
  - relay send, in-flight release, delivery, ACK-clock, peer usage, and
    transport observations now carry exact physical carrier identity, so late
    callbacks from a retired instance cannot mutate its replacement;
  - physical replacement clears only physical evidence while preserving
    configured policy and logical flow ownership;
  - removed separate TCP sockets and transient QUIC connections that attempted
    to infer a live carrier's health from another carrier;
  - recurring QUIC maintenance now prepares a missing durable connection but
    treats an already-ready connection as no new observation; cached RTT is not
    manufactured into fresh liveness evidence; and
  - two tests for a test-only TCP probe path were removed because production
    configured-minimum reconciliation no longer executes that path.
- Practical corrections during the gate:
  - a full-suite failure showed that a full queue after drain could be reported
    as temporary backpressure. The nonblocking reservation path now reports
    exact carrier closure while retaining a single lifecycle load on the
    ordinary successful hot path; and
  - recurring durable QUIC recovery was restored after the false-probe cleanup
    had accidentally left only the startup pass.
- Verification:
  - `cargo fmt --all`, `git diff --check`,
    `cargo check --all-targets --all-features`, and
    `cargo clippy --all-targets --all-features -- -D warnings` pass;
  - `cargo test --all-targets --all-features --quiet` passes: 1,409 library
    tests, two allocation tests, and four daily-use acceptance tests;
  - the full-stack configured-minimum scenario passes disable, ordered drain,
    immediate re-enable, exact fresh-instance replacement, and continuation of
    the same SOCKS5 Product stream; and
  - the durable QUIC scenario proves one socket/identity is reused and a ready
    carrier does not publish a false probe result.
- Performance boundary: no congestion control, pacing, scheduler formula,
  native recovery, transport window, carrier range, or timing value changed.
  No no-regression or competitive-performance claim is made yet.
- Next: commit this coherent candidate, run adjacent representative TCP/QUIC,
  TCP-range QoS, and balanced blackhole guards, then retain or revert the
  candidate from those measurements before any further protocol work.

## 2026-07-31T03:42:16+08:00: MPP v5 wire and authenticated path purpose aligned

- Name: singular carrier-validation wire and fail-closed purpose boundary
- Category: Product protocol, authentication, resource cleanup, and Core
  change control
- State: the RFC-facing wire/authentication boundary is complete; elastic TCP
  establishment, measurement, settlement, authority, and retirement remain
  disconnected, and TCP `VALIDATION` therefore remains fail-closed
- Implemented:
  - `PATH_JOIN` now carries canonical `ORDINARY = 1` or `VALIDATION = 2`
    immediately after underlay, and the purpose is covered by the v5 HMAC
    transcript and preserved by authenticated admission;
  - existing TCP and QUIC clients emit `ORDINARY`; QUIC permanently rejects an
    authenticated `VALIDATION` before replay admission, path registration,
    peer-state publication, or readiness;
  - `TCP_CARRIER_DEMAND` now names zero or one stream through its canonical
    presence byte, and `TCP_CARRIER_VALIDATE` names exactly one stream;
  - removed the plural-list codec, its list-order and allocation errors, and
    codec-only `max_streams`; Product/Core `ResourceLimits.max_streams` remains
    unchanged and continues to bound live streams and sessions; and
  - replay identity deliberately remains purpose-independent: the HMAC covers
    purpose while one path-join nonce remains one-use across both purposes.
- Necessity and retained-patch reflection:
  - the removed list states were introduced with the incomplete untagged v5
    work but are impossible in the approved RFC model, so retaining their
    helpers, bounds, and tests would preserve dead design rather than safety;
  - this fixes a concrete RFC/code/security mismatch, not a predicted network
    edge case or a performance-test workaround;
  - MPP remains v5 because released v0.1.2 used v4 and the incomplete v5 work
    has never been released; an artificial v6 cut would add no protection; and
  - no scheduler, congestion, pacing, recovery, transport window, queue,
    timing, carrier range, platform path, or steady payload operation changed.
    Ordinary admission adds one purpose byte and one HMAC transcript byte only.
- Persistent evidence:
  - exact wire-vector tests pin path-purpose position, demand presence, singular
    validation, enum values, and v5 frame kinds;
  - authentication vectors and tamper tests prove purpose is signed;
  - a real QUIC admission test sends an authenticated `VALIDATION`, observes no
    readiness or registry state, then reuses the exact session, path, and nonce
    with its valid `ORDINARY` tag and reaches readiness, proving rejection
    precedes replay insertion and publication;
  - independent wire, runtime, and minimality reviews returned `APPROVE`;
  - `cargo test --all-targets --all-features --quiet` passes: 1,410 library
    tests, two allocation tests, and four daily-use tests, with zero failures;
  - `cargo clippy --all-targets --all-features -- -D warnings`, formatting, and
    whitespace checks pass.
- Release blocker: TCP may accept `VALIDATION` only when the complete
  session-owned C2S controller can enforce candidate authority, exact target
  binding, bounded Product evidence, immutable result settlement, and ordered
  retirement as one vertical slice.
- Next: implement that C2S vertical slice without changing ordinary carrier
  scheduling or any Core performance formula, then prove gain/no-gain behavior
  in the RFC labs before enabling expansion by default.

## 2026-07-31T03:19:27+08:00: elastic TCP RFC approved after retained-change audit

- Name: destructive Core authority and post-baseline patch reflection
- Category: RFC conformance, Core architecture, and performance change control
- State: the RFC model passed independent role, lifecycle, resource, and
  implementability gates; runtime expansion, validation, retention, and
  contraction remain disconnected and make no Product or performance claim
- Retained implementation:
  - keep canonical protocol bounds, hostile-input rejection, exact QUIC
    HTTP-datagram route retirement, admission-before-readiness,
    queue-before-publication flight ownership, asynchronous-open generation
    fences, transactional observability, canonical Product identities,
    demand-driven DNS, balancer attempt lifetimes, endpoint port sets, native
    QUIC migration, and the configured-minimum TCP group owner;
  - each retained behavior covers malformed authenticated input, an ordered
    runtime race, a reload/failover transition, or an explicit Product
    requirement;
  - no retained post-v0.1.2 change alters congestion control, pacing, native
    recovery, transport windows, scheduler formulas, steady-state defaults, or
    `quinn-proto`; and
  - keep `CodecLimits.max_paths` because it bounds active peer-status frames,
    but remove codec-only `max_streams` with the obsolete plural carrier schema;
    runtime `ResourceLimits.max_streams` remains independently required.
- Removed or rejected implementation:
  - keep removed the dormant maximum-carrier topology, per-stream carrier
    verdicts, waiter-owned lifecycle, disconnected observer/service stack,
    source or locator identity, ACK-silence reopening, periodic retry, duplicate
    cleanup owners, and overlay congestion or pacing;
  - reject ordinary pre-retention scheduling and capacity-payload retention
    evidence because neither proves bounded unique-original Product gain;
  - reject provisional authority, rollback, tombstones, peer-assumed deadlines,
    post-result withdrawal, and hidden result-reason classifications; and
  - reject the earlier preserve-on-disable behavior: client-local policy cannot
    stop server-to-client use of a live carrier, so disable now requires ordered
    carrier drain and fresh instances on re-enable.
- Approved model:
  - configured-minimum carriers authenticate as `ORDINARY`; an actual elastic
    carrier authenticates as `VALIDATION`, consumes one real reservation and
    `PathId`, and has no ordinary authority before exact `RETAIN` settlement;
  - one throughput placement-to-saturation edge owns one sequential
    group-selection sequence, with one live candidate, at most one attempt per
    client-local group, and no group identity on wire;
  - a validation binds one direction of one exact target attachment and uses
    only unique-original Product work; capacity payload, duplication,
    reinjection, native ACKs, and peer metrics are never verdict evidence;
  - after the Data ACK startup floor, assisted validation uses the existing
    mature path-flight and queue model, so the cold startup bound cannot cap a
    high-BDP candidate;
  - the sender records two candidate-free pre samples, two consecutive assisted
    samples, and two candidate-free post samples; `RETAIN` requires the minimum
    assisted target and session rates to exceed their independent four-sample
    reference maxima using exact fractions and no fixed percentage or EWMA;
  - the cumulative horizon need not fit simultaneously in a stream window;
    every instantaneous flight, queue, credit, repair, reorder, window, and
    memory bound still applies;
  - immutable result plus exact result acknowledgment commits authority at
    role-correct points: C2S client result serialization/ACK receipt and S2C
    client result acceptance/ACK serialization;
  - local non-restarting establishment, validation, and Product-phase leases
    bound resources; expiry or disable enters ordered carrier retirement;
  - a NO_GAIN attempt and later admission gate have distinct state: the client
    tracks attempted local groups, while the sender gates a later generation
    until ordinary membership/policy changes or two candidate-free session
    samples establish a disjoint service interval;
  - later throughput work may provide that rearm evidence after the original
    target ends, while target change, source IP, interface, locator, elapsed
    time, backpressure, or ACK silence alone cannot reopen expansion; and
  - TCP port hopping is planned replacement with no state transfer; QUIC uses
    native rebinding/migration; random port choice, the five-minute default, and
    five-second safety floor are endpoint-local maintenance policy, never
    capacity or failure evidence.
- Independent evidence:
  - the retained-runtime gate found the abandoned observer/service stack fully
    removed and every other retained change reachable and justified;
  - the Product-evidence review rejected a one-assisted-sample verdict and
    established the two-pre/two-assisted/two-post geometry;
  - the wire/lifecycle review established exact role fences, settlement
    barriers, local leases, minimum-member overlap, and no cross-carrier state
    transfer;
  - the suppression review approved the bounded multi-group model after removal
    of the unjustified `path_probe_interval_ms` elastic timer; and
  - the final contradiction gate returned `APPROVE`.
- Release-blocking reachable gaps:
  - MPP v5 `TCP_CARRIER_*` remains codec-only and plural in source; implement
    the singular complete vertical slice or remove the version as one unit;
  - configured TCP maximum has no elastic runtime consumer;
  - ranged TCP accepts `port_hop_interval_ms`, but only QUIC consumes it; and
  - received `SESSION_CLOSE` lacks one session-wide terminal owner and may
    reconnect the same `SessionId`.
- Performance boundary:
  - this checkpoint changes only `RFC.md` and this ledger;
  - no runtime operation, timing, transport, scheduler, queue, window, bundle,
    or platform path changed; and
  - QoS gain, shared-bottleneck no-gain, disruption recovery, and the full
    representative no-regression matrix remain mandatory before retention.
- Next: checkpoint this authority, align the wire to singular fail-closed
  validation purpose, and implement one complete client-to-server Product
  lifecycle without changing the configured-minimum or ordinary payload fast
  paths.

## 2026-07-31T00:22:00+08:00: configured-minimum TCP group owner accepted

- Name: session-owned TCP configured-minimum lifecycle and retained-patch
  reflection
- Category: Product carrier lifecycle, RFC alignment, and Core change control
- State: configured-minimum ownership is complete; automatic elastic
  expansion, validation, and contraction remain disconnected
- Reflection and removals:
  - retained only the group owner, establishment-policy fence, exact
    carrier-instance publication, exact-instance probe commit, and durable
    minimum reconciliation because each closes a reachable ownership race or
    implements the configured lower bound;
  - runtime disable is now explicitly suspend/preserve control: it prevents
    new establishment and placement through group policy and health, but does
    not manufacture carrier retirement; ordered `PATH_DRAIN` remains reserved
    for actual removal, bound reduction, replacement, or elastic-candidate
    retirement;
  - removed a meaningless watch revision, the duplicate carrier-generation
    counter and test alias, an impossible probe-mode Boolean, ignored
    exact-probe result values, a duplicate disabled-success check, and a
    second impossible management 404;
  - the group signal is now a coalescing lifecycle notification, management
    selection is a tagged TCP/UDP identity, and construction mismatches are
    structural assertions rather than runtime fallbacks; and
  - no extra timer, source-address identity, actor registry, running flag,
    platform branch, or Product-stream-owned carrier classification remains.
- Implemented model:
  - one client session service reconciles only the immutable
    configured-minimum member actors; configured maximum capacity remains
    physically absent and consumes no actor, health record, or scheduler path;
  - distinct missing minimum members may authenticate concurrently, while one
    exact actor still serializes one member's connection attempt and wire
    ownership;
  - the existing path-probe interval bounds connection-attempt start rate;
    exact readiness loss wakes reconciliation, so stable loss need not wait for
    the next periodic tick and authenticate-then-drop churn cannot spin;
  - endpoint policy generation cancels and rejects stale pre-readiness work,
    while runtime disable/re-enable preserves exact nonterminal minimum
    carriers;
  - authenticated peer usage, exact health identity, and readiness become
    visible in one health-lock transaction; an old actor can clear the shared
    readiness cell only with an exact-instance compare-and-exchange; and
  - cold Product stream admission now publishes that transaction before
    writing `OPEN_STREAM`, making disable versus admission one linearized
    decision without repeated policy checks.
- Persistent acceptance:
  - one production-service integration scenario proves a `2-3` endpoint
    materializes exactly two carriers in one `SessionId`, uses distinct
    `PathId` and carrier identities, replaces one exact stable failure before
    the next periodic tick, preserves the unaffected instance, carries SOCKS5
    Product traffic, preserves exact instances across suspend/re-enable,
    suppresses replacement after disable plus exact native failure, and
    restores only the minimum after re-enable;
  - the focused scenario passes in `2.30` seconds;
  - the complete library gate passes: 1,402 tests, zero failures;
  - `cargo check --locked --all-targets` passes;
  - `cargo clippy --locked --all-targets -- -D warnings` passes;
  - all 198 persistent lab tests pass; and
  - formatting and whitespace checks pass.
- Core performance boundary:
  - no payload framing, connected writer operation, scheduler formula,
    congestion controller, pacing, recovery, window, queue, transport
    parameter, carrier range, or RFC timing constant changed;
  - configured-minimum startup count is unchanged from the former immediate
    probe behavior, and the configured maximum remains inactive;
  - all new work is confined to connection, loss, management, and periodic
    probe control paths; and
  - no new throughput claim is made by this slice; the corrected representative
    and inactive-range evidence recorded below remains the current Core
    authority.
- Explicit open boundaries:
  - RFC clauses for elastic reservations, directional validation, removal and
    bound-change drain, and bounded carrierless retention are prospective
    acceptance for the next lifecycle step, not claims about this source;
  - reflection exposed a broader existing conformance gap: a received
    `SESSION_CLOSE` currently retires the carrier but may let the same
    `ClientPathContext` reconnect with the same `SessionId`; session-terminal
    ownership must be corrected in the later session/disruption lifecycle
    step; and
  - MPP v5 elastic carrier-control frames remain runtime-unconsumed and must be
    connected by the coherent elastic lifecycle or removed before release.
- Next: implement the one session-direction aggregate elastic lifecycle,
  including bounded reservation and ordered contraction, without changing the
  proven Core; then require the causal TCP-QoS cohort and representative
  no-regression matrix before retaining it.

## 2026-07-30T22:39:23+08:00: corrected inactive-range gate passed

- Name: valid TCP QoS fixture and exact QUIC datagram terminal state
- Category: Core performance evidence, QUIC correctness, and source gate
- State: complete; clean baseline is ready for session carrier-group ownership
- Corrected inactive-range evidence:
  - clean commit `3cc2d40f9fc9793ba59dd24777dada77ba1aa581`,
    valid host, diagnostics disabled, fixed 30-second load, three synchronized
    application flows, 500 Mbps per-flow rate, zero configured loss, and the
    established fat-path propagation delay;
  - adjacent inactive `1-1`/`1-3` ranges delivered `355.455`/`356.173` Mbps
    download and receiver-confirmed `336.391`/`337.620` Mbps upload;
  - every saved child qdisc reports the intended aggregate `limit 45256p` and
    per-flow `flow_limit 15256p`, with zero drops and zero `flows_plimit`;
  - the corrected pairs therefore establish that the configured maximum has
    no throughput effect while expansion is disconnected; and
  - complete evidence is retained under
    `./.tmp/lab/results/current-3cc2d40-corrected-per-flow-20260730/`.
- QUIC terminal-state completion:
  - the first full source gate correctly rejected merely removing the unsafe
    stream-ID watermark, because a known retired request would re-enter the
    bounded pre-registration queue;
  - the router now keeps an exact retired-ID set populated only by
    generation-owned route retirement and bounded to the configured route
    capacity;
  - active-route lookup remains first, exact retired IDs drop immediately, and
    only unknown IDs enter the already bounded one-RTT handoff queue;
  - this preserves concurrent out-of-order request registration without
    retaining unbounded terminal state or inferring closure from stream-ID
    order.
- Verification:
  - both persistent request-route retirement tests pass;
  - the complete library gate passes: 1,401 tests, zero failures;
  - `cargo check --locked --all-targets` passes;
  - `cargo clippy --locked --all-targets -- -D warnings` passes;
  - all 198 persistent lab tests pass; and
  - formatting and whitespace checks pass.
- Decision:
  - historical representative performance and the inactive `1-3` boundary are
    satisfactory;
  - no performance parameter, timing, scheduler, congestion controller,
    transport default, production queue, or payload-path operation was changed
    by the fixture or exact-terminal corrections; and
  - automatic elastic TCP expansion remains disconnected until one
    session-owned carrier group can maintain minimum readiness and own every
    candidate transition.
- Next: establish that carrier-group owner without manufacturing absent paths,
  then prove its minimum-readiness lifecycle before connecting aggregate
  expansion evidence.

## 2026-07-30T22:29:53+08:00: restored performance reproduced; invalid QoS fixture isolated

- Name: post-audit representative Core gate and retained-patch correction
- Category: Core performance, QUIC correctness, and lab validity
- State: representative gate passes; inactive shared-bottleneck range is
  inert; corrected per-flow fixture rerun and complete source gate remain open
- Representative evidence:
  - clean commit `71463212bb86bd8274cbb76072d7697de4d46d68`,
    protocol v5, valid host, diagnostics disabled, fixed Linux Docker
    topology, 20-second load, and two application flows;
  - TCP single/equal-fat download delivered `126.708`/`769.951` Mbps and
    receiver-confirmed single/equal-fat upload delivered `132.318`/`534.774`
    Mbps; the exact equal-fat upload repeated at `535.664` Mbps;
  - QUIC single/equal-fat download delivered `247.703`/`743.915` Mbps and
    receiver-confirmed single/equal-fat upload delivered `228.999`/`704.738`
    Mbps; the only ambiguous exact upload repeated at `788.547` Mbps;
  - no steady-state multipath collapse reproduced. QUIC returned to the
    historical range in every direction, including the historical-best range
    on the repeated upload; and
  - raw manifests, qdisc state, configs, telemetry, and rows are retained under
    `./.tmp/lab/results/current-7146321-representative-20260730/` and
    `./.tmp/lab/results/current-7146321-multipath-upload-repeat-20260730/`.
- Inactive-range evidence:
  - the fixed shared 200 Mbps bottleneck produced adjacent `1-1`/`1-3`
    download rows of `158.558`/`158.737` Mbps and upload lower bounds of
    `154.676`/`154.969` Mbps, establishing no range-dependent effect;
  - the per-flow rows were contradictory rather than evidence of expansion:
    `318.492`/`270.391` Mbps download and `59.310`/`237.293` Mbps upload;
  - saved `tc -s -d` evidence identified the cause in the fixture: the child
    `fq` retained its default 100-packet per-flow queue on a 500 Mbps,
    360 ms-RTT path and recorded `flows_plimit` drops in every per-flow row,
    including 6,473 drops in the low upload result; and
  - that accidental queue overflow is neither configured single-flow QoS nor
    runtime behavior, so the per-flow rows are rejected and will not be used
    to justify Core changes.
- Corrections:
  - the per-flow fixture now applies its existing BDP-derived queue model to
    both the three-flow aggregate and each `fq` flow, leaving duration,
    propagation, rate, loss, concurrency, and all production parameters
    unchanged;
  - removed the QUIC HTTP-datagram router's largest-registered-stream
    watermark. Concurrent H3 request opens need not register in stream-ID
    order, so the watermark could misclassify a valid lower-ID first datagram
    as belonging to a closed stream;
  - exact generation-owned route retirement remains, while unknown routes stay
    bounded by the existing global route, byte, packet, and one-RTT expiry
    limits; and
  - documented `reserve_load` as the required commit fence against a disable
    or failure racing an asynchronous open. It remains flow-setup work, not a
    payload-path mechanism.
- Focused verification:
  - all 19 persistent lab runner-contract tests pass;
  - all seven native HTTP-datagram and route-retirement tests pass; and
  - formatting and whitespace checks pass.
- Decision:
  - no retained change after historical authority modifies congestion
    control, pacing, recovery, scheduling, windows, transport defaults, queue
    geometry, or `quinn-proto`;
  - retain independently justified wire-extent validation,
    queue-before-publication ownership, transactional telemetry, balancer
    deadlines, port migration, asynchronous-open fencing, and peer-owned
    retirement;
  - do not interpret the invalid per-flow rows as product behavior and do not
    tune the Core against them; and
  - automatic elastic TCP expansion remains disconnected.
- Next: checkpoint these two corrections, rerun the corrected four-row
  per-flow cohort from a clean source, then complete the full source gate
  before establishing session carrier-group ownership.

## 2026-07-30T21:57:16+08:00: false topology removed and result commit defined

- Name: retained-patch reflection and MPP v5 lifecycle correction
- Category: Core topology, carrier retirement, wire protocol, and performance
  change control
- State: implemented and locally verified; elastic TCP expansion remains
  inactive until the session actor implements this contract
- Supersedes:
  - the provisional maximum-slot topology retained at 20:37; and
  - any earlier implication that writing a local `PATH_CLOSE` or unacknowledged
    `RETAIN` settles a carrier lifecycle
- Prior-patch audit:
  - retained the direct v4 conformance fixes, canonical Product identities,
    transactional observability, demand-driven DNS, balancer attempt
    lifetimes, carrier port sets, native QUIC migration, exact asynchronous
    open fencing, and queue-before-publication flight ordering;
  - found no retained change to the scheduler, congestion controller, pacing,
    windows, recovery formulas, queue geometry, transport defaults, or the
    patched `quinn-proto`;
  - retained the balancer's per-member deadline as an explicit
    availability-versus-failed-member-latency trade that still requires the
    final short-flow matrix; and
  - identified the maximum-slot TCP topology as the only unjustified
    established payload-path delta.
- Topology correction:
  - configured TCP maximum remains a strict resource and future-establishment
    bound, but physically absent elastic capacity is no longer represented as
    a `PathSpec`, health record, session actor, draining path, or management
    state;
  - only configured-minimum TCP carriers enter ordinary scheduling; this
    preserves explicit minima greater than one without exposing unestablished
    candidates;
  - removed the dormant-path eligibility flag and its branches from health,
    observation, proof, delivery, reservation, failure, management, and
    scheduler paths; and
  - removed the extra health mutex and record scan previously performed by
    `automatic_bulk_path_count` on bulk payload placement.
- Retirement correction:
  - client actor teardown no longer sends its own `PATH_CLOSE`; when no owner
    remains it drops the native carrier, which is an honest native terminal
    boundary;
  - future graceful retirement remains one actor-owned
    `PATH_DRAIN`-to-peer-`PATH_CLOSE` transaction and cannot reuse the removed
    shortcut.
- RFC and wire correction:
  - attachment is bidirectional membership and feedback capability, not
    ordinary TCP payload authority; payload authority remains directional,
    while ACK, credit, FIN, reset, detach, datagram feedback, and lifecycle
    control preserve existing semantics;
  - one session has at most one active directional validation, and each
    direction has one session-scoped aggregate controller; a per-stream
    controller cannot publish session authority;
  - validation deadline, result, attachment retirement, drain, and terminal
    cleanup are actor-owned even if a caller stops waiting; and
  - added `TCP_CARRIER_RESULT_ACK` kind 41. `RETAIN` is prepared by the sender,
    applied provisionally by the receiver, and committed only after the exact
    acknowledgment, eliminating the unordered result/withdrawal race between
    TCP directions without established-path overhead.
- Evidence:
  - all 13 focused TCP-carrier/protocol/integration tests pass;
  - the configured-minimum topology and management lifecycle tests pass;
  - the complete library gate passes: 1,401 tests, zero failures;
  - formatting and whitespace checks pass; and
  - the correction removes more runtime/test lines than it adds.
- Performance boundary:
  - no Core selection formula, transport parameter, timing value, queue
    capacity, carrier count at the configured minimum, or native controller
    changed;
  - the established payload path loses one mutex acquisition, one scan, and
    dormant-eligibility branches introduced after historical authority;
  - result acknowledgment is one small control frame and one setup round trip
    only for an elastic candidate; and
  - historical performance authority remains `a5a6094` until the adjacent
    inactive-range and representative Core labs pass.
- Open lifecycle boundary:
  - configured-minimum members now have distinct lazy actors, but an idle
    native failure is currently restored by later demand or the periodic path
    probe rather than immediate group reconciliation;
  - the session carrier-group owner must therefore maintain desired minimum
    readiness and hold endpoint-level management policy independently of
    current carrier records before it may own elastic candidates; and
  - one persistent multi-carrier lifecycle acceptance must cover minimum
    readiness, distinct instances, exact failure replacement, maximum
    non-expansion, endpoint disable, and terminal cleanup together.
- Next: establish that session carrier-group owner, then implement the smallest
  client-to-server candidate lifecycle against the exact committed-result and
  terminal-retirement contract; run adjacent `1-1` versus inactive `1-3` and
  representative historical labs before enabling expansion.

## 2026-07-30T21:39:02+08:00: per-flow carrier slice rejected before commit

- Name: TCP carrier vertical-slice reflection gate
- Category: Core ownership, directional authority, and change control
- State: rejected and removed; the source tree is restored to the clean
  `2b2d36c` baseline and automatic elastic TCP expansion remains inactive
- Supersedes: no committed milestone; this entry records why the uncommitted
  implementation following the 20:37 milestone was not retained
- Rejected implementation:
  - placed aggregate service estimation and the validation verdict inside
    each reliable-stream request controller, so concurrent streams could
    mistake service shifted between streams for session-wide gain;
  - represented client-to-server authority with generic attachment/path
    eligibility, even though ordinary Product-data authority is directional;
  - allowed a validation waiter cancellation to suppress already-admitted
    lifecycle work and released the one-candidate fence after writing
    `PATH_DRAIN`, before the ordered `PATH_CLOSE` or native-failure boundary;
  - let the receiver's validation state depend on the initiating cohort
    lifetime without an independent absolute resource deadline; and
  - added repairs for result/withdrawal and cleanup races before the RFC had
    defined one deterministic commit boundary.
- Retained conclusions:
  - aggregate service and the expansion verdict belong to one long-lived
    session-direction owner; per-stream state supplies bounded exact demand,
    attachment, flight, and Data ACK observations only;
  - ordinary Product-data authority must remain separate by direction from
    the control and feedback needed to preserve existing stream semantics;
  - one unsettled candidate remains physically owned through matching
    `PATH_CLOSE` or native failure, not merely through local drain enqueue;
  - an admitted actor operation completes independently of whether its caller
    still waits for the receipt; and
  - both endpoints need a finite validation resource lifetime, so the RFC must
    define a deterministic result/withdrawal boundary before runtime code.
- Evidence:
  - the roughly 2,900-line uncommitted source/test slice was removed in full;
  - `git status --short` is empty and `git diff --check` passes;
  - `cargo check --locked --all-targets` passes from the restored baseline;
    and
  - no scheduler rule, transport parameter, timing formula, carrier count, or
    runtime Product behavior changed.
- Performance boundary: historical authority remains `a5a6094`; the rejected
  slice produced no accepted performance claim and leaves no runtime overhead.
- Next: clarify the existing RFC session-direction actor, directional
  attachment authority, result/withdrawal commit, independent deadline, and
  terminal ownership contract; only then implement the smallest
  client-to-server lifecycle and measure it adjacently.

## 2026-07-30T20:37:27+08:00: TCP expansion model reduced to reachable state

- Name: destructive RFC and prior-patch necessity audit
- Category: Core architecture, wire contract, and performance change control
- State: RFC and wire codec are coherent; automatic TCP expansion remains
  inactive and is not releasable
- Supersedes:
  - the six-stage service-validation model recorded at 10:22, 10:37, 12:27,
    and every later entry that depended on its observer lifecycle; and
  - the 20:16 next action insofar as it referred to the former v5 frame shape.
- Prior-patch classification:
  - retained as independently justified: the direct v4 protocol-conformance
    corrections, transactional observability, canonical Product identities,
    demand-driven target DNS, selection-local balancer deadlines, carrier port
    sets, native QUIC port migration, session-local TCP carrier retirement,
    exact asynchronous-open fencing, and queue-before-publication request
    flight ordering;
  - retained as a real acceptance fixture: the opt-in adjacent `1-1` versus
    `1-3` TCP QoS/shared-bottleneck lab cohort; it changes no runtime behavior;
  - provisional and still inactive: the `tcp-carriers` Product range and its
    bounded slot topology. They express the requested default `1-3`, but no
    dormant slot becomes ordinary-use eligible without the remaining
    session-level lifecycle;
  - amended rather than preserved: the original bounded-carrier RFC, the
    endpoint-scoped service RFC, and MPP v5 controls now follow the smaller
    session-direction model below; and
  - removed as redundant: the standalone validation model, per-flow writer
    registries and observers, duplicate cleanup authority, and disconnected
    runtime adapters reverted before this milestone.
- Clean model:
  - one real saturation event may admit one unsettled elastic TCP connection;
    polling, ACK silence, locator change, interface change, or native queue
    occupancy cannot manufacture demand;
  - the directional sender freezes a session-wide accepted set, bounded stream
    cohort, and ordinary aggregate-service interval;
  - candidate outstanding and cumulative original work use explicit finite
    resource bounds and must reach the existing Data ACK startup sample floor;
  - `RETAIN` requires the combined interval's lower bound to exceed the frozen
    accepted interval's upper bound. Equal, overlapping, or inconclusive
    evidence is `NO_GAIN`; stale state or ended demand is `WITHDRAWN`;
  - minimum and exact failure-replacement carriers are ordinary by
    construction; only elastic carriers require directional `RETAIN`;
  - ordinary-use authority belongs to one exact live instance and direction
    and ends on drain, failure, or session close; and
  - QUIC roaming remains native. TCP recovery replaces an exact failed carrier
    without inferring link identity from an IP address, interface, or route.
- Wire correction:
  - `TCP_CARRIER_DEMAND(request_id, stream_ids)` remains the bounded
    server-to-client demand signal;
  - `TCP_CARRIER_VALIDATE(validation_id, request_id, direction, stream_ids)`
    now relies on the authenticated carrying connection for candidate identity
    and serializes no accepted-path list, `PathId`, or cross-carrier nonce;
  - `TCP_CARRIER_RESULT(validation_id, direction, result)` repeats no
    candidate path ID; and
  - the codec rejects direction/request-ID disagreement before allocating a
    stream list.
- Evidence:
  - the RFC review found and removed the unreachable pre/reference/post
    comparison phases, repeated candidate identity, server dependence on
    client-local carrier groups, and minimum-carrier authority gap;
  - exact stale identifiers are absent from current source;
  - all 32 durable protocol codec tests pass;
  - the complete library gate passes: 1,401 tests, zero failures;
  - `cargo check --locked --all-targets`, formatting, and whitespace checks
    pass; and
  - the simplified change removes more RFC/source/test lines than it adds.
- Performance boundary:
  - no scheduler choice, congestion controller, transport parameter, timing
    formula, carrier count, or runtime placement behavior changed;
  - the normal data path gains no validation observer or phase branch; and
  - historical performance authority remains `a5a6094` until the integrated
    lifecycle passes adjacent representative labs.
- Next: implement one coherent client-to-server vertical slice under a single
  session-level carrier owner: exact candidate preparation, validation-only
  attachment outside ordinary scheduling, bounded sender evidence, result,
  directional authority, ordered drain, and terminal cleanup.

## 2026-07-30T20:16:29+08:00: disconnected TCP service stack removed

- Name: restore one reachable runtime model before adaptive-carrier work
- Category: Core architecture, performance change control, and source audit
- State: verified cleanup; MPP v5 service frames and the RFC remain design
  inputs, but adaptive TCP carrier expansion is not implemented or releasable
- Supersedes:
  - the runtime/model acceptance claims at 12:27 and 19:17; and
  - every intervening claim that the passive writer stack was a production
    integration boundary.
- Content:
  - audited every implementation commit from the restored v4 Product boundary
    through `f4a36dc` and separated independently reproduced protocol/Product
    invariants from speculative TCP-service machinery;
  - retained the v4 conformance fixes, Product logging/configuration/DNS/
    balancer/port-set work, native QUIC migration, session-local TCP close,
    asynchronous carrier-open fence, and request queue-before-publication
    ordering;
  - removed the unconstructed `TcpServiceSessionController`, its 1,552-line
    model test mirror, the passive request/response writer observers, global
    per-flow writer registry, dormant carrier authority generations, and the
    disconnected client/runtime adapters;
  - rejected an uncommitted sparse-topology rewrite because eliminating
    dormant slots had expanded into a performance-sensitive rewrite of every
    scheduler/scoring API; the proven scheduler remains unchanged;
  - rejected an uncommitted server implementation that represented
    validation feedback as a Product output and then added ordinary hot-path
    eligibility branches to filter it again; and
  - removed the per-frame clone of the fixed request path's command sender
    while preserving queue reservation, exact load-lease transfer, flight
    recording, and publication order.
- Evidence:
  - the removed committed runtime/model stack was nearly 9,000 source/test
    lines and produced 78 dead-code warnings because no production owner
    constructed its controller;
  - the same removal compiled cleanly first in an isolated ignored worktree and
    then in the primary tree with `cargo check --locked --all-targets`;
  - the complete library gate passed: 1,400 tests, zero failures;
  - 52 focused request-sender tests passed after the clone removal; and
  - formatting and whitespace checks passed.
- Performance boundary:
  - no clean post-`a5a6094` commit has a reproduced directional regression;
    `3112e2d` is the last adjacent measured v5 boundary and `ecd5b5e` was only
    the first later unmeasured runtime boundary;
  - the only retained large regression belongs to the historical dirty
    protocol-6 tree and cannot honestly be assigned to one commit; and
  - no timing, congestion controller, scheduler rule, path count, transport
    parameter, or Product behavior changed in this cleanup.
- Decision:
  - an inactive protocol feature may not leave permanent per-flow or
    per-payload branches in the runtime;
  - the next TCP-service implementation must be owned by one long-lived
    session/carrier-group actor, install temporary evidence only for an active
    bounded cohort, and keep lifecycle feedback separate from Product outputs;
  - if the v5 wire/RFC cannot support that model without duplicate authority,
    they will be revised before implementation rather than patched around; and
  - performance attribution requires adjacent clean evidence, not a suspected
    commit or a fixed percentage cap.
- Next: complete the destructive RFC/actor-boundary review, then implement one
  coherent client-to-server lifecycle through candidate open, bounded
  evidence, result, directional lease, drain, and cleanup before adding the
  symmetric server-to-client demand path.

## 2026-07-30T19:17:35+08:00: single TCP service lifecycle authority

- Name: remove duplicate lifecycle state before production integration
- Category: Core architecture correction and change-control audit
- State: verified correction; the production session actor, carrier controls,
  validation-only attachment, and candidate placement remain disconnected
- Supersedes:
  - the runtime ownership claimed by the 17:07 entry; and
  - the replay-token design claimed by the 17:24 entry.
- Content:
  - audited the complete uncommitted receiver attempt and every committed
    change from the inactive writer boundary through `caf5d1e`;
  - rejected the uncommitted 1,989-line server receiver/attachment checkpoint
    because it reconstructed one carrier lifecycle in the reliable-stream
    registry, introduced a second wrapping attachment generation beside the
    established incarnation, and had no production client producer;
  - removed the committed `ClientRequestTcpServiceActiveLifecycle` registry
    and its installation, withdrawal, acknowledgement, replay, and disarm
    state, which duplicated the model controller and writer coordinator;
  - removed the model's externally replayable cleanup token. A long-lived
    session actor will own the model lifecycle and its terminal cleanup
    directly, while child stream actors only acknowledge exact observer
    installation or removal;
  - retained the validation-only writer clock, exact flight sidecars, passive
    request/response observers, actor-minted stream snapshots, authenticated
    carrier-instance fences, dormant-candidate separation, and synchronous
    local observer stop at stream-owned fence changes; and
  - changed no scheduler policy, congestion controller, transport parameter,
    timing value, carrier count, or Product behavior.
- Evidence:
  - `cargo check --locked --all-targets` passed after the correction;
  - the complete library gate passed: 1,420 tests, zero failures;
  - formatting was unchanged and `git diff --cached --check` passed; and
  - dead-code diagnostics now expose the remaining disconnected producer
    boundary instead of hiding it behind a second lifecycle registry.
- Performance boundary:
  - this removes cold disconnected state and does not enable adaptive carrier
    behavior;
  - commit `3112e2d` remains the only inactive-runtime boundary with an
    adjacent representative performance gate; and
  - commit `a5a6094fc8e07456b057ddc107a0d51849d42d10` remains the historical
    performance authority until the integrated actor passes matched labs.
- Next: implement one long-lived session actor as the sole owner of
  `TcpServiceSessionController`, candidate deadline, child-actor receipts,
  exact TCP controls, validation-only attachment, bounded placement, result,
  and terminal cleanup; do not checkpoint another disconnected layer.

## 2026-07-30T12:27:52+08:00: bounded TCP service validation model

- Name: exact endpoint/session/direction validation authority
- Category: Core model, evidence integrity, cancellation safety, and resource
  bounds
- State: committed and independently approved; runtime evidence adapters,
  carrier admission, and retirement remain disabled and open
- Source: commit `1649bd1e61b09a3cf7b480aad1d4d70e34ff7396`
- Content:
  - implemented one session-serialized, direction-owned validation lifecycle
    with exact range, demand, carrier-instance, eligibility, stream-demand,
    attachment, and Data ACK horizon fences;
  - implemented the RFC's two pre-reference, readiness, two comparison, and
    two post-reference windows using indivisible complete Data ACK events and
    exact rational rate ordering, without a percentage threshold;
  - bounded candidate placement, outstanding work, ACK records, range history,
    stream/path cohorts, lifecycle IDs, and absolute resource lifetime;
  - made preparation and cleanup authority linear, scoped every candidate
    permit to its non-reusable lifecycle, and retained exact deadline
    `WITHDRAWN` identity through cancellation-safe observer cleanup;
  - excluded pre-install and wrong-lifecycle accepted flights without
    manufacturing evidence, while wrong-lifecycle candidate work fails closed;
    and
  - made process-wide physical carrier instance allocation fail before numeric
    identity reuse instead of wrapping.
- Evidence:
  - all 9 focused semantic tests passed;
  - the complete 1,407-test library gate passed before the final pure-model
    corrections, and the focused gate passed again afterward;
  - three independent read-only reviews approved RFC conformance, runtime API
    shape, cancellation lifetime, and false-`RETAIN` safety; and
  - formatting and whitespace checks passed.
- Performance boundary:
  - no scheduler, timing, threshold, congestion controller, carrier count,
    connection establishment, or runtime placement behavior changed;
  - automatic TCP expansion remains unavailable; and
  - commit `a5a6094fc8e07456b057ddc107a0d51849d42d10` remains the historical
    performance authority.
- Adjacent prerequisite:
  - commit `c413f79` now reserves carrier queue capacity, records exact request
    flight ownership, and only then publishes the command, matching the
    response-side atomic contract without changing queue geometry.
- Next: install the validation-only strict writer clock and exact request and
  response release sidecars, then connect the session coordinator without
  enabling carrier expansion.

## 2026-07-30T10:45:01+08:00: TCP carrier QoS evidence fixture

- Name: make beneficial and non-beneficial TCP carrier expansion measurable
  without changing Core behavior
- Category: performance methodology, reproducibility, and lab accounting
- State: committed and locally verified; no performance result or runtime
  acceptance is claimed
- Source: commit `bb8ebdfaa9d0ec46b3cc4bcb3ee4a1d26162771a`
- Content:
  - added an opt-in fixed cohort with adjacent `tcp-carriers=1-1` and `1-3`
    download/upload rows for both per-flow QoS and one shared bottleneck;
  - uses three persistent application flows, a post-connect synchronized
    measurement anchor, a 30-second load window, and client-only carrier-range
    policy;
  - established deterministic zero-configured-loss qdiscs: a 500 Mbps
    per-native-flow `fq maxrate` profile and a 200 Mbps aggregate profile, both
    with the existing fat-path propagation delay and derived queue capacity;
  - made special qdisc profiles recreate their hierarchy so per-flow state
    cannot leak into a later shared-bottleneck row;
  - records detailed qdisc counters and all new environment inputs in the
    immutable run manifest; and
  - advanced new lab-result identity to MPP v5 while retaining all recorded v4
    rows as historical evidence.
- Evidence:
  - all 198 lab unit tests passed;
  - shell syntax, ShellCheck, and whitespace validation passed;
  - both qdisc hierarchies were created in a disposable Linux container with
    `NET_ADMIN`; inspection proved the per-flow child exists only in the
    per-flow profile and is absent from the aggregate profile; and
  - no Docker performance row was run because the adaptive controller does not
    yet exist and a `1-3` maximum alone grants no extra carrier.
- Acceptance boundary:
  - repeated adjacent per-flow pairs must prove useful added service;
  - shared-bottleneck pairs must preserve ordinary service and drain an
    unhelpful candidate without churn;
  - no single row, nominal rate sum, or universal percentage margin can retain
    a carrier or establish competitiveness.
- Next: implement and causally test the pure endpoint/session/direction
  controller before connecting it to runtime carrier establishment or running
  this cohort.

## 2026-07-30T10:37:10+08:00: bounded MPP v5 control contract

- Name: implement the wire and authentication prerequisite without enabling
  adaptive TCP behavior
- Category: Core protocol, authentication separation, resource safety, and
  implementation sequencing
- State: committed and independently verified; TCP service controllers and
  runtime dispatch remain intentionally absent
- Source: clean Core commit `cc260a262947dbc45a288826aedcc46b303bb181`
- Content:
  - advanced the sole codec to MPP v5 and the MPP session and path-join
    authentication contexts to v5, with no v4 decoder, compatibility mode, or
    downgrade path;
  - kept the separately versioned authenticated TCP prelude at version 1
    because its wire transcript and security contract did not change;
  - implemented frame kinds 38 through 40 exactly as specified by `./RFC.md`,
    including the authenticated `(PathId, PATH_JOIN.nonce)` carrier-instance
    record;
  - required nonzero owner sequence IDs, canonical duplicate-free cohorts,
    nonempty validation cohorts, and the explicit empty-demand withdrawal
    encoding;
  - applied configured path and stream limits before allocation on both encode
    and decode, including the existing peer-status path vector; and
  - added only mechanical frame classification and bounded diagnostics to
    existing exhaustive consumers. TCP and QUIC actors still reject these
    frames until their RFC owners are implemented.
- Evidence:
  - 47 all-feature protocol tests passed, including stable v5 byte layouts for
    demand, validation, and result frames, v5 authentication vectors, v4
    rejection, canonicality, identifier, enum, and pre-allocation limit
    checks;
  - all 4 focused carrier-neutral resource-policy tests passed;
  - `cargo check --locked --all-targets --all-features` passed;
  - `cargo fmt --all -- --check` and whitespace validation passed.
- Performance boundary:
  - no scheduler, sender, ACK, placement, recovery, congestion controller,
    timing, carrier count, payload loop, or runtime dispatch behavior changed;
  - the historical v4 performance authority remains unchanged and is not
    relabeled as v5 evidence; and
  - no adaptive TCP behavior may be enabled until the pure controller,
    instance/demand fences, exact sender evidence, bounded work, and ordered
    drain lifecycle are implemented and causally tested.
- Next: implement the pure session/group/direction service controller from
  RFC Sections 7.2 and 15.1, then connect exact runtime events without moving
  authority into per-stream state.

## 2026-07-30T10:22:47+08:00: endpoint-scoped TCP service authority

- Name: establish the clean breaking protocol model before adaptive TCP work
- Category: Core RFC, directional evidence, bounded resources, retirement,
  and performance change control
- State: RFC committed and independently reviewed; runtime remains on the
  restored Core and MPP v4 until the ordered v5 implementation slices pass
- Source: clean commit `95b6a24dc9992e24d7ba011a66843dd08794354b`
- Content:
  - removed the complete uncommitted 3,032-line TCP expansion attempt after
    proving that one request stream could publish endpoint-wide authority,
    response provenance was inferred after receiver reordering, candidate
    credit rolled without a finite lifetime bound, and retirement could close
    with original flight outstanding;
  - established MPP v5 as a clean break with no compatibility or downgrade
    mode, leaving the TLS 1.3 TCP prelude and standard HTTP/3 QUIC presentation
    unchanged;
  - assigned TCP establishment to the client and unique-delivery verdicts to
    two independent sender-owned session/group/direction controllers;
  - retained `PATH_JOIN.nonce` as the authenticated carrier-instance token for
    cross-carrier references instead of using a locator or reusable numeric
    `PathId`;
  - defined encrypted, TCP-only demand, validation, and result frames with
    exact bounded carrier and stream cohorts; the stream list is one immutable
    response-demand snapshot, not a per-stream demand protocol;
  - bounded candidate Product work to one readiness and two comparison phases,
    at most three existing bulk scheduling horizons in total and always within
    the existing unproven-flight bound;
  - matched two pre-reference and two post-reference windows around the two
    comparison windows, used indivisible exact Data ACK events and exact
    rational rate comparison, and introduced no fixed percentage verdict;
  - made invalidation and absolute resource-lifetime expiry withdraw without a
    capacity verdict, and suppressed no-gain retries until exact cohort
    identity or the observed reference range materially changes; and
  - made peer `PATH_CLOSE` the ordered aggregate acknowledgment of a drained
    TCP carrier, with both-direction queues, attachments, Data ACKs, flights,
    proofs, validation work, and leases at zero before removal.
- Evidence:
  - three read-only architecture audits independently located the per-stream
    authority, response-provenance, candidate-instance, and drain-ordering
    defects and agreed that no `STREAM_DEMAND` or `STREAM_DETACH_ACK` frame is
    required;
  - the final hostile review required exact response cohort equality,
    race-safe withdrawal, authenticated instance references, finite deadlines,
    canonical demand lifecycle, and bounded cohort work before approval;
  - `cargo check --locked` passed on the restored clean runtime;
  - the focused existing protocol gate passed all 46 selected tests; and
  - whitespace validation passed.
- Performance boundary:
  - this milestone changes no runtime source, congestion controller, scheduler,
    recovery rule, timing parameter, transport setting, payload loop, or
    connection count;
  - commit `a5a6094fc8e07456b057ddc107a0d51849d42d10` remains the historical
    runtime performance authority; and
  - protocol-v4 measurements remain historical evidence and will not be
    relabeled as v5. Fresh matched v5 parent, champion, QoS, shared-bottleneck,
    condition, and transition rows are mandatory before enabling or releasing
    adaptive carriers.
- Decision:
  - `./RFC.md` is the sole implementation authority for this work;
  - no code from the rejected per-stream or older TCP-pool attempts may return
    by bulk restoration; and
  - each v5 slice must be isolated, causally reviewed, and verified before
    runtime behavior is enabled.
- Next: implement only the v5 authentication contexts, bounded codec, and
  three encrypted TCP carrier-control frames with permanent canonicality and
  allocation-limit coverage; then implement the pure endpoint-direction
  controller before any runtime scheduling integration.

## 2026-07-30T07:12:31+08:00: bounded TCP carrier topology

- Name: represent one configured TCP endpoint as a bounded carrier group
- Category: Product carrier lifecycle, management identity, resource
  ownership, and unchanged-Core boundary
- State: committed and correctness-approved; automatic carrier admission,
  carrier-only retirement, TCP port replacement, and the unchanged-Core
  performance guard remain open
- Source: clean commit `01771753cee53948291d1ebd4d563eea5be509ec`
- Content:
  - expanded each configured TCP endpoint to its declared maximum number of
    runtime carrier slots while preserving every configured endpoint's
    historical primary `PathId` before appending sibling slots;
  - mapped siblings back to one immutable configured endpoint, security
    credential, TLS identity, name, and configured ordinal while retaining a
    distinct runtime path index, actor, health record, session handle, and
    carrier identity for every slot;
  - made only the configured lower bound locally eligible; dormant capacity is
    represented as draining and cannot be selected, probed, published, acquire
    peer state or delivery evidence, or obtain a load reservation;
  - rejected the complete expanded slot count against the session resource
    envelope before allocating or cloning sibling state;
  - changed path management to resolve the configured TCP endpoint and apply
    one atomic action across all of its members without admitting dormant
    capacity; and
  - kept configured endpoint count distinct from active carrier count in the
    management API while exposing each visible TCP carrier's ordinal and
    configured lower and upper bounds.
- Correctness evidence:
  - one persistent path-state model test covers two configured endpoints,
    primaries-first identity, shared immutable security/TLS mapping, different
    lower bounds, dormant exclusion, load rejection, selection order, and
    pre-allocation resource rejection;
  - existing management tests now cover endpoint-wide disable/enable with two
    lower-bound carriers, dormant-state preservation, and user-facing carrier
    presentation;
  - reservations revalidate lifecycle, administrative state, and scheduler
    state under the health lock before publishing load, closing the
    disable-or-fail-after-selection race;
  - two independent read-only reviews found and verified the resource-bound,
    dormant-reservation, and endpoint-wide control corrections; and
  - the complete repository suite passed: 1,390 library tests, two warmed
    allocation contracts, four packaged daily-use Product tests, formatting,
    whitespace checks, and warnings-denied all-target Clippy.
- Performance boundary:
  - no wire field, frame, congestion controller, scheduling formula, recovery
    timer, transport parameter, payload path, or connection-establishment rule
    changed;
  - the default lower bound remains one carrier, so this milestone establishes
    no additional TCP connection; and
  - bounded dormant records add only local inventory scans and require the
    unchanged-Core representative performance guard before final approval.
- Decision:
  - approve the topology and configuration ownership as the prerequisite for
    bounded TCP expansion;
  - do not activate an extra carrier from an eligibility Boolean or native TCP
    telemetry alone; and
  - keep automatic admission in a separate exact endpoint-claim lifecycle.
- Next: implement one generation-fenced unproven-carrier claim per configured
  endpoint, carry exact rollback through authenticated open completion, and
  keep automatic admission unavailable until MPP product evidence and
  carrier-only close semantics are complete.

## 2026-07-30T06:28:03+08:00: ranged QUIC carrier port migration

- Name: migrate one authenticated QUIC carrier across its configured
  destination-port set
- Category: Product carrier lifecycle, RFC-defined QUIC migration, neutral
  transport boundary, and unchanged-Core performance
- State: committed and independently approved; focused correctness,
  cross-target, and two-cohort performance evidence passed; bounded adaptive
  TCP carriers and the broader Product map remain open
- Source: clean commit `e456b32ef9cdbd6f7168239c2dbe76f4d5683fc2`
- Content:
  - added `port-hop-interval-ms` only for ranged UDP carrier endpoints, with
    the requested five-minute default and a five-second configuration floor;
  - retained the same Quinn connection, authenticated carrier instance,
    streams, path identity, and MPP state while selecting another configured
    destination port;
  - created each candidate through the normal protected neutral socket
    provider, pinned the established server IP, and kept the previous receive
    socket until authenticated return traffic arrived on the selected port;
  - serialized migration so another interval cannot start while the current
    locator remains unconfirmed, while Quinn continues to own validation,
    recovery, congestion state, and socket release;
  - implemented a mapping-only UDP adapter: it translates exactly the
    configured canonical/selected locator pair and leaves ECN, GSO/GRO,
    source/destination IP metadata, payloads, fragmentation, and every other
    locator decision unchanged;
  - bounded runtime ownership to one abort-on-drop task and Quinn's
    current/previous socket lifecycle; and
  - kept TCP periodic replacement out of this slice because a TCP port change
    requires a new authenticated connection and therefore belongs to the
    bounded multi-carrier pool.
- Correctness and platform evidence:
  - durable parsing tests cover defaults, explicit intervals, fixed/ranged
    rejection, and the minimum;
  - the existing socket contract verifies exact bidirectional mapping and
    pass-through behavior;
  - one transport integration carries request/response traffic before and
    after two forwarded-port migrations on the same accepted Quinn connection;
  - focused tests, shell syntax, formatting, and all-target/all-feature checks
    passed;
  - independent Windows GNU and musl all-target/all-feature builds passed;
    macOS and Android use the same neutral adapter and remain assigned to their
    native GitHub CI jobs; and
  - an independent lifecycle/RFC review found no remaining correctness,
    resource, Core-boundary, or platform blocker.
- Performance evidence:
  - both accepted cohorts used the exact clean source commit, identical
    client/server binary, a valid settled host, isolated 20-second cases,
    diagnostics disabled, one flow, and five confirmed migrations per
    hopping transfer;
  - cohort one measured fixed/hopping download at
    `2805.435`/`2835.804` Mbps and fixed/hopping upload at
    `2790.870`/`2756.474` Mbps;
  - the independent repeat measured fixed/hopping download at
    `2865.815`/`2950.826` Mbps and fixed/hopping upload at
    `2811.900`/`2778.640` Mbps;
  - every transfer was exact or complete with zero recovery gap; hopping
    download maximum read gaps were `0.018` and `0.019445` seconds, and hopping
    upload retained one finalized target connection with no unexpected
    connection; and
  - hopping was positive in both download cohorts and `1.23%`/`1.18%` below
    the adjacent upload control, which is ordinary run variance rather than a
    reproducible downgrade. Raw evidence remains under ignored
    `./.tmp/lab/results/product-quic-port-hop-20260730/` and
    `./.tmp/lab/results/product-quic-port-hop-repeat-20260730/`.
- Decision:
  - approve periodic ranged QUIC destination-port migration without a new wire
    field or Core timing/scheduling rule;
  - retain Quinn and the neutral socket provider as the migration authorities;
    and
  - proceed to one RFC-defined bounded adaptive TCP carrier pool before
    periodic TCP replacement.
- Next: establish the TCP carrier identity and bounded `1..3` pool lifecycle,
  using additional native TCP capacity only after measured demand and retiring
  it when it is idle or no longer effective.

## 2026-07-30T05:51:07+08:00: carrier endpoint port-set bootstrap

- Name: select one concrete locator for each new TCP or QUIC carrier
- Category: Product carrier configuration, bootstrap DNS, managed VPN, and
  cross-platform transport boundary
- State: committed and approved; complete Product and unchanged-Core
  performance guards passed; periodic migration and the broader Product map
  remain open
- Source: clean commit `4d2a9915ecb10de58f216bb879f41a67555babc5`
- Content:
  - added a carrier-only endpoint model accepting one fixed nonzero port or one
    inclusive ascending `START-END` interval without expanding the interval;
  - retained the zero-entropy fixed-port path and used unbiased OS-entropy
    rejection sampling for ranged endpoints;
  - selected exactly once before each new physical TCP or QUIC carrier's DNS
    resolution and retained that port across every address-family attempt;
  - rejected carrier-provider answers that do not retain the selected port;
  - changed prepared managed-VPN carrier state to store resolved IP addresses,
    then materialize the selected port at runtime, preventing route publication
    from freezing a future carrier's locator;
  - kept proxy, DNS, target, and management endpoints single-port and rejected
    ranged server bind paths in both Product validation and transport bind
    boundaries; externally published range ports must forward to the fixed
    listener;
  - made doctor preserve and report the complete configured range while
    skipping a direct probe that could not validate the deployment-owned
    forwarding set; and
  - clarified RFC locator/identity ownership without adding a wire field,
    periodic timer, scheduler rule, or transport parameter.
- Product and platform evidence:
  - the complete suite passed: 1,386 library tests, two warmed allocation
    contracts, all four packaged daily-use Product tests, doctests, formatting,
    whitespace checks, and warnings-denied all-target/all-feature Clippy;
  - durable tests cover strict fixed/ranged syntax, IPv6 authority rendering,
    malformed and ambiguous rejection, CLI and TOML retention, inbound range
    rejection, Product DNS selected-port preservation, port-neutral prepared
    VPN snapshots, and doctor presentation;
  - two independent reviews found no correctness, Product, Core-boundary, or
    cross-platform blocker; and
  - the neutral implementation and Windows cross-target all-target build pass.
    macOS and Android require no platform-specific branch for this feature and
    remain covered by the neutral provider contract pending native GitHub CI.
- Performance boundary and evidence:
  - no scheduler, congestion, pacing, recovery, wire framing, native transport
    timing, payload path, or Core resource default changed;
  - the clean native Linux release binary is SHA-256
    `f97b516f27c2e4669805c1589e914ac27611aa7611f2e9ad0639cccaad973650`;
  - on a valid settled host with identical client/server binary, diagnostics
    disabled, isolated cases, 20-second load, and two flows, single/equal-fat
    QUIC download measured `238.912`/`754.002` Mbps and upload measured
    `247.307`/`705.742` Mbps;
  - single upload retained the known one-second terminal-drain `loss` label and
    did not change transport timing;
  - two immediately adjacent, unchanged-binary equal-fat upload repeats had
    exact finalized receiver accounting and measured `735.938` and `774.122`
    Mbps. The recovery to the prior accepted `776.609` Mbps level proves the
    first lower row was run variance rather than a reproducible Product-code
    regression; and
  - valid raw evidence remains under ignored
    `./.tmp/lab/results/product-carrier-portset-20260730-valid/`,
    `./.tmp/lab/results/product-carrier-portset-20260730-equal-upload-repeat/`,
    and
    `./.tmp/lab/results/product-carrier-portset-20260730-equal-upload-repeat-2/`.
- Decision:
  - approve per-establishment carrier locator selection with no reproducible
    performance downgrade;
  - retain Core and native transport authority unchanged; and
  - implement periodic QUIC locator migration separately, while TCP rotation
    waits for the authenticated bounded multi-carrier pool required by dynamic
    TCP carrier scaling.
- Next: establish the clean periodic QUIC port-migration lifecycle with the
  user-required five-minute default for ranged endpoints, preserving one QUIC
  connection and carrier instance.

## 2026-07-30T05:17:29+08:00: selection-local balancer failover deadlines

- Name: preserve configured Product stages across pre-commit failover
- Category: Product balancer, target DNS, routing, and outbound correctness
- State: committed and approved; complete Product and unchanged-Core
  performance guards passed; the broader Product map remains open
- Source: clean commit `80c557dcb653d018fe6aaf0b499569fb370e484a`
- Content:
  - replaced the registry-wide maximum timeout with independent, selected-graph
    stages, so unrelated outbounds cannot lengthen or shorten an open;
  - gave every local TCP/native UDP attempt its configured connect timeout and
    every MPP TCP attempt its Product open timeout, including each later
    balancer member and each post-resolution route group;
  - retained target DNS as one flow-level stage under the selected DNS plan:
    one failed dual-family lookup skips later IP-only members without another
    lookup, while the canonical domain may continue to a remote-resolution
    member;
  - promoted a domain to authorized addresses only once after successful DNS
    and never converted shared DNS failure into gateway-health failure;
  - kept MPP UDP honest as a deferred first-send outcome rather than inventing
    a pre-commit network-open deadline; and
  - used checked per-stage deadline construction without an arbitrary aggregate
    cap or a new production timing parameter.
- Product evidence:
  - a real silent SOCKS5 member retained its configured one-second connect
    stage, then the ordered direct successor opened successfully;
  - a two-IP-only-member DNS failure issued one dual-family flow lookup,
    preserved the domain for a SOCKS5 successor, and recorded no false member
    failure;
  - two post-resolution address groups proved that a blackholed first group
    cannot consume the second group's connect stage;
  - the complete suite passed: 1,386 library tests, two warmed allocation
    contracts, all four packaged daily-use Product tests, doctests, formatting,
    whitespace checks, and warnings-denied all-target/all-feature Clippy; and
  - two independent read-only reviews found no remaining material Product or
    performance blocker after the identified DNS-repeat, unused-budget, and
    MPP-UDP documentation issues were corrected.
- Performance boundary and evidence:
  - no scheduler, congestion, pacing, recovery, carrier, wire protocol,
    transport timing parameter, payload path, or Core resource default changed;
  - the clean native Linux release binary is SHA-256
    `f9a7bc59b26c2349d523d8286c790e49fb63df191688167abb9b29d37bc47633`;
  - on a valid settled host with matching client/server binary, diagnostics
    disabled, isolated cases, 20-second load, and two flows, single/equal-fat
    QUIC download measured `223.266`/`726.461` Mbps and upload measured
    `234.078`/`776.609` Mbps;
  - the unchanged adjacent single-download repeat measured `238.240` Mbps,
    confirming the first row as ordinary shaped-link variance rather than a
    reproducible Product-code regression; the adjacent prior accepted rows were
    `245.735`/`758.115` Mbps download and `246.364`/`761.841` Mbps upload;
  - equal-fat upload had exact finalized receiver accounting. Single upload
    delivered the complete probe-visible payload but retained the known
    one-second terminal-drain `loss` label; no timeout was changed; and
  - valid raw evidence remains under ignored
    `./.tmp/lab/results/product-balancer-deadlines-20260730-valid/` and
    `./.tmp/lab/results/product-balancer-deadlines-20260730-single-download-repeat/`.
- Decision:
  - approve selection-local Product stage ownership with no reproducible
    performance downgrade;
  - retain the Core unchanged; and
  - continue the remaining daily-use Product map.
- Next: establish a bounded endpoint port-set model and select one concrete
  locator for each new TCP/QUIC carrier before adding periodic hopping
  lifecycle behavior.

## 2026-07-30T04:23:01+08:00: demand-driven application-target DNS

- Name: separate target representation, routing evidence, and resolver
  transport
- Category: Product routing, destination authorization, DNS, outbound, and
  balancer semantics
- State: committed and approved; complete Product and affected performance
  guards passed; the broader Product map remains open
- Source: clean commit `b626dcc6181f554ed15944f4e96e7c49247b26d0`
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
- Performance boundary and evidence:
  - no scheduler, congestion, pacing, recovery, carrier, wire protocol,
    transport timing, or Core resource default changed;
  - the clean native Linux release binary is SHA-256
    `4d19573363dffa2fc6f9c99882316ff28418ee5d00d77ff8cc3410d5ea509916`;
  - with a clean source, valid settled host, matching client/server binary,
    diagnostics disabled, isolated cases, 20-second load, and two flows,
    single/equal-fat QUIC download measured `245.735`/`758.115` Mbps and
    upload measured `246.364`/`761.841` Mbps;
  - every row is performance-comparable and remains on the accepted
    historical plateau; the adjacent canonical-vocabulary guard measured
    `252.939`/`733.331` Mbps download and `241.272`/`800.008` Mbps upload;
  - both upload rows confirmed exactly the probe-visible payload at the target.
    The single-upload row retained the known duration/drain `loss` label
    because its streams did not emit terminal closure inside the unchanged
    one-second drain, not because payload was lost; and
  - the warm-build cohort was rejected by the runner because build load
    exceeded the host-validity limit. No row from it was admitted. Valid raw
    evidence remains under ignored
    `./.tmp/lab/results/product-target-dns-20260730-valid/`.
- Decision:
  - approve the Product semantics with no observed performance downgrade;
  - keep the restored Core unchanged; and
  - retain `PROGRESS.md` as the sole execution ledger.
- Next: continue the remaining daily-use Product capabilities from the global
  map while preserving the same Product/Core boundary.

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

## 2026-07-30T14:06:45+08:00: Per-carrier-group TCP service suppression authority

- Name: bounded directional no-gain suppression ownership
- Category: Core model and RFC alignment
- State: model correction complete; runtime session owner remains in progress
- Content:
  - kept one session-scoped service controller and one active validation
    authority, as required by `./RFC.md`;
  - introduced an endpoint-local TCP carrier-group identity that is never
    serialized or reconstructed from locators;
  - replaced direction-global suppression state with one current suppression
    slot per direction and carrier group, bounded by the existing configured
    carrier-inventory limit rather than a timer, retry threshold, or new
    tuning parameter;
  - identity change clears suppression only inside the affected group, while
    validation of another group cannot erase established no-gain evidence;
    and
  - preserved exact rational-rate comparison and zero candidate Product credit
    for an overlapping suppressed validation.
- Evidence:
  - extended the durable six-window/no-gain model test with an
    `group A -> unrelated group B -> unchanged group A` lifecycle;
  - the regression proves that B may activate and withdraw without changing
    A, after which A remains suppressed on the same overlapping two-window
    reference range;
  - targeted regression passed;
  - all test targets compiled; and
  - formatting and whitespace checks passed.

## 2026-07-30T14:45:02+08:00: Exact TCP service writer authority boundary

- Name: directional writer observation and authenticated carrier fencing
- Category: Core model and RFC alignment
- State: authority boundary complete; producer and carrier expansion remain disconnected
- Content:
  - placed request-direction TCP-service controls in the logical stream actor's
    existing FIFO so they cannot overtake data, attachment, or close events and
    remain deliverable while the accepted attachment set is empty;
  - established exact writer-flight observation at committed carrier writes
    and exact Data ACK settlement at the owning stream actor, preserving
    original, reinjected, pre-install, and active provenance without adding a
    scheduler timer or changing an RFC parameter;
  - replaced wrapping or zero attachment identities with checked nonzero
    incarnations and fail-closed topology invalidation when a changed
    attachment set can no longer receive a new identity;
  - bound request-direction TCP-service eligibility to the authenticated path
    identity, PATH_JOIN nonce, physical carrier instance, and a checked
    eligibility generation rather than a locator or source address; and
  - kept candidate creation, candidate binding, and the session controller
    unconnected. The existing native TCP and QUIC carrier count and behavior
    are therefore unchanged at this milestone.
- Evidence:
  - the focused TCP-service suite passed: 18 tests;
  - the complete root library suite passed: 1,418 tests;
  - all test targets compile under the locked dependency graph;
  - whitespace validation passed; and
  - no performance conclusion is recorded here. The next gate is the fixed
    representative download/upload lab set on this clean commit, compared
    with the historical champion before any producer is connected.

## 2026-07-30T15:08:25+08:00: Inactive TCP service v5 performance gate

- Name: adjacent parent/candidate guard before producer connection
- Category: Core performance change control
- State: representative inactive boundary passed; full condition and
  transition matrix remains open
- Correction:
  - supersedes the preceding entry's broad statement that native behavior is
    unchanged: carrier count, RFC timing, scheduling policy, congestion
    control, and transport parameters are unchanged, but the disconnected
    runtime authority adds measurable ordinary-path scaffolding;
  - restored the established ACK fast path so reinjected or non-evidence
    releases do not perform ambiguity searches unless an active observer needs
    their provenance; and
  - boxed the rare response observer-install payload so the inactive feature
    does not enlarge every bounded server-stream event slot.
- Verification:
  - focused TCP-service tests passed: 18;
  - complete root library suite passed: 1,418;
  - formatting and whitespace checks passed; and
  - no configuration, producer, candidate-open path, carrier-count change, or
    new RFC timing parameter is connected.
- Matched protocol-v5 evidence:
  - direct parent `e9dd008646da1377b6ecf2f69f95f02c2193f09c`, release
    binary `b001a285a82deec65848f50f8fd85686cea851e1c7027c80b9a9673867a658dd`;
  - candidate `3112e2d24ef358f80c74ad31dc7fcb78b655595c`, release binary
    `cfca58089f4d4d11a99045e7b6d25bc3114e00f8218a6dad32f633ac5f749c36`;
  - both clean sources used the same native Linux toolchain, retained Docker
    images, valid-host rules, isolated topology, diagnostics-disabled release
    profile, 20-second load, and two application flows;
  - parent/candidate single-path download measured `245.418`/`234.896` Mbps,
    while equal-fat multipath download measured `728.004`/`728.579` Mbps;
  - parent/candidate single-path upload lower bounds measured
    `231.522`/`241.582` Mbps, while exact equal-fat multipath upload measured
    `720.733`/`750.275` Mbps; and
  - the only lower candidate row was repeated adjacently: parent/candidate
    single-path download measured `228.451`/`236.733` Mbps. The two paired
    signs therefore reversed, and the two-run medians were `236.935` and
    `235.815` Mbps without a reproduced directional downgrade.
- Decision:
  - accept the inactive writer-authority boundary with no observed systematic
    performance regression;
  - do not treat the isolated fluctuation as a tuning signal or introduce a
    percentage pass margin; and
  - keep the unsafe caller-fabricated install/bind interface and every service
    producer disconnected until the logical stream actors own exact
    snapshot/freeze validation.
- Evidence:
  - `./.tmp/lab/results/adjacent-parent-e9dd008-v5-20260730/`;
  - `./.tmp/lab/results/adjacent-candidate-3112e2d-v5-20260730/`;
  - `./.tmp/lab/results/adjacent-parent-e9dd008-v5-single-repeat-20260730/`;
    and
  - `./.tmp/lab/results/adjacent-candidate-3112e2d-v5-single-repeat-20260730/`.

## 2026-07-30T15:33:18+08:00: Actor-owned request service freeze

- Name: exact request demand and attachment authority
- Category: Core runtime and RFC alignment
- State: request snapshot/install boundary complete; validation candidate
  attachment and session producer remain disconnected
- Content:
  - replaced the request observer's caller-built stream fence and attachment
    map with an opaque value minted only by the serialized logical-stream
    attachment owner;
  - resolved the accepted set from one exact configured TCP carrier group and
    re-read each authenticated `PATH_JOIN` nonce, physical carrier instance,
    directional eligibility generation, and Product attachment identity;
  - required open throughput demand, fresh queued unique data, existing
    original flight on every accepted attachment, a nonzero existing Data ACK
    horizon, and bounded accepted-set cardinality before returning a snapshot;
  - established a checked request demand generation that changes only with the
    RFC demand class or local Product open state. Polling, enqueue progress,
    ACKs, reinjection, and timers do not change it;
  - made installation synchronously rederive and compare the complete actor
    snapshot before publishing a passive observer, and mapped ordinary stale
    races to `WITHDRAWN` rather than protocol or release errors;
  - removed the caller-supplied request candidate bind operation. A later
    validation candidate must have a lifecycle-owned attachment slot outside
    the ordinary accepted Product attachment set before binding is restored;
    and
  - added no timing parameter, percentage threshold, locator inference,
    carrier open, Product placement authority, or active service producer.
- Ordinary-path cost:
  - snapshot, group traversal, authentication checks, allocation, and flight
    checks execute only for a queued service control;
  - the ordinary request loop adds only the required demand-class transition
    predicate, with demand-state mutation only when that class actually
    changes; and
  - local EOF performs one checked cold-path generation transition.
- Evidence:
  - the durable actor-authority regression proves that an unchanged exact
    group installs while a candidate eligibility change between snapshot and
    install withdraws without publishing an observer;
  - the durable demand regression proves stable generation under unchanged
    polling and exact changes for demand and EOF;
  - the complete root library suite passed: 1,420 tests;
  - focused TCP-service tests passed: 19;
  - formatting and whitespace checks passed; and
  - no performance verdict beyond the previously recorded inactive v5 gate is
    claimed. Representative performance remains a required global-map gate
    before any candidate or producer can be connected.

## 2026-07-30T15:53:36+08:00: Dormant TCP validation-candidate authority

- Name: accepted-set and candidate fence separation
- Category: Core runtime and RFC alignment
- State: authority correction complete; validation candidate attachment and
  session producer remain disconnected
- Content:
  - corrected the request actor proof to use the configured `1-2` TCP carrier
    range: slot zero is accepted Product authority while slot one is dormant
    validation capacity above the configured minimum;
  - established a separate authenticated candidate fence for a ready dormant
    slot without changing its immutable scheduler eligibility or admitting it
    to the ordinary Product attachment set;
  - retained exact physical carrier instance, `PATH_JOIN` nonce, peer
    directional availability, and checked generation identity for both
    accepted and candidate authority;
  - allowed ordered `PATH_STATUS` updates to invalidate dormant candidate
    authority while preserving their exclusion from ordinary path selection;
  - tied every client TCP authenticated-path registration to the exact
    connection owner's lifetime. Cancellation, actor exit, connection
    replacement, and local failure therefore retire the exact instance and
    cannot leave a disconnected candidate fence published;
  - made an explicit management `failed` decision invalidate dormant
    candidate authority for the same established failure cooldown used by
    active members, after which maintenance restores only a newly generated
    suspect candidate fence; and
  - made the actor snapshot require exactly one current, unattached candidate
    from the requested TCP carrier group. It no longer incorrectly requires
    that candidate to be an already accepted carrier.
- Evidence:
  - the durable endpoint-topology regression proves that an authenticated
    dormant slot remains unschedulable, retains a stable fence across an
    unchanged status, receives a new fence after withdrawal and restoration,
    and disappears when its exact connection registration is dropped;
  - the existing endpoint-management regression now proves failure
    invalidation and generated restoration for dormant capacity while
    retaining the established disabled/enabled behavior;
  - the actor snapshot/install regression now uses the real `1-2` expansion
    case and proves that a candidate withdrawal between snapshot and install
    produces `WITHDRAWN(FenceChanged)`;
  - the retained-carrier integration test passed after connection-lifetime
    ownership moved to the exact registration guard;
  - focused TCP-service tests passed: 19;
  - the complete root library suite passed: 1,420 tests; and
  - formatting and whitespace checks passed. No parameter, timer, carrier
    establishment, Product placement, or performance policy changed, and no
    new performance verdict is claimed.

## 2026-07-30T16:11:00+08:00: Actor-owned request observer invalidation

- Name: synchronous request writer freeze at actor fence changes
- Category: Core runtime and RFC alignment
- State: actor-owned request invalidation complete; external authenticated
  path-state invalidation, response symmetry, validation-only candidate
  attachment, and the session producer remain disconnected
- Content:
  - made exact-lifecycle request observer removal stop the shared writer
    coordinator before dropping actor state, while a stale lifecycle remains
    an explicit no-op against current commit authority;
  - synchronously stopped an installed observer before every actor-owned
    request demand transition, local EOF transition, successful attachment
    membership commit, exact attached-path failure, and actor exit;
  - placed attachment invalidation after all fallible candidate setup but
    before publishing the new path and attachment incarnation, so no writer
    commit can enter between a changed topology and the coordinator stop;
  - stopped path-failure authority before the asynchronous detach and close
    sequence, retaining the established early load-release behavior; and
  - threaded the existing serialized sender owner through cold attachment
    helpers rather than introducing a second authority, timer, polling loop,
    locator inference, or transport-specific implementation.
- Ordinary-path cost:
  - no scheduling, admission, timing, carrier-count, congestion-control, or
    transport parameter changed;
  - inactive removal and failure checks remain one nullable observer branch;
    topology callbacks run only on successful attachment, and EOF or demand
    invalidation runs only on the corresponding state transition; and
  - no validation candidate can yet attach and no service producer is
    connected.
- Evidence:
  - the existing observer-provenance regression now proves stale removal
    preserves active authority, exact removal stops later commits, and repeated
    exact removal is idempotent;
  - the existing path-failure cleanup regression proves coordinator stop is
    visible while detach cleanup is still blocked;
  - the existing candidate-load regression proves the actor callback occurs
    on the successful membership-commit boundary;
  - the complete root library suite passed: 1,420 tests;
  - every Cargo target compiles under the locked dependency graph; and
  - formatting and whitespace checks passed. This is a correctness boundary,
    not a new performance verdict; the historical-performance gate remains
    mandatory before any producer or candidate placement is connected.

## 2026-07-30T17:07:17+08:00: Session-owned request lifecycle authority

- Name: exact request service installation, withdrawal, cleanup, and disarm
- Category: Core runtime and RFC alignment
- State: request lifecycle registry and external invalidation complete; the
  production session producer, response symmetry, and validation-only
  candidate attachment remain disconnected
- Content:
  - established one session-owned active request lifecycle with exact
    lifecycle, coordinator, bounded carrier group, accepted and validation
    candidate path fences, and per-stream writer and observer state;
  - required one session identity and one coherent frozen actor view across
    every stream before publishing lifecycle authority, then revalidated
    authenticated carrier and eligibility generations under the established
    lock order;
  - made actor installation acknowledge the exact writer fence before
    returning its receipt, and made stale or mismatched installation controls
    harmless to a different current observer;
  - made actor-owned demand, EOF, attachment, path-failure, and exit
    transitions record their precise shared withdrawal cause before removing
    and acknowledging the local observer;
  - routed authenticated TCP carrier replacement, retirement, ordered
    `PATH_STATUS`, data-plane failure, generic TCP failure, and explicit
    management failure through the same lifecycle boundary, while leaving UDP
    path handling unchanged;
  - made terminal cleanup replayable after cancellation: only installed,
    unacknowledged observers are returned, exact actor acknowledgements settle
    them, and disarm requires a stopped coordinator plus every stream clean;
  - rejected cause-free cleanup of a live lifecycle while preserving normal
    cause-free settlement after the coordinator has already stopped;
  - made writer-registration loss fail-stop the exact current lifecycle and
    ordered relay ownership so the actor observer is destroyed before its
    registration guard acknowledges final cleanup; and
  - extended the existing durable lifecycle and management regressions rather
    than adding disposable one-condition test files.
- Ordinary-path cost:
  - no packet, acknowledgement, scheduler, congestion-control, timer,
    carrier-count, admission, or transport parameter changed;
  - the new registry lock is taken only for cold TCP authority transitions;
    the inactive actor path remains a nullable observer branch; and
  - no candidate placement or production service traffic is enabled by this
    milestone.
- Evidence:
  - the durable lifecycle regression proves foreign-session rejection, exact
    install, stale-control isolation, mandatory terminal cause, replay after
    cancellation, acknowledgement-before-receipt, stopped-and-clean disarm,
    unchanged-status preservation, candidate-fence withdrawal, accepted
    carrier replacement withdrawal, and registration-drop cleanup;
  - the durable management regression proves that an explicit authenticated
    TCP failure withdraws the active lifecycle;
  - three independent read-only reviews found the lock order and mutation
    coverage sound and confirmed the stale-install cleanup blocker was closed;
  - the complete root library suite passed: 1,420 tests;
  - every Cargo target compiles under the locked dependency graph;
  - formatting and whitespace checks passed; and
  - no new timing, threshold, parameter, or performance claim is made.
    Historical-performance restoration and the representative lab matrix
    remain mandatory before candidate placement or release.

## 2026-07-30T17:24:29+08:00: Durable terminal TCP service cleanup authority

- Name: retain exact validation authority through observer cleanup
- Category: Core model and RFC alignment
- State: pure session model complete; the production session owner, response
  symmetry, and validation-only candidate attachment remain disconnected
- Content:
  - made every installation, settled validation, withdrawal, and deadline path
    enter one durable `Cleaning` state instead of releasing session authority
    before passive writer observers have been removed;
  - made the cleanup token carry the exact session, trial, candidate,
    direction, lifecycle, installation stage, terminal verdict, withdrawal
    reason, and no-gain suppression evidence;
  - made cleanup replayable after task cancellation while rejecting stale
    acknowledgements from an earlier lifecycle;
  - kept later reservations, including the opposite direction, blocked until
    exact cleanup acknowledgement; and
  - made a reached absolute deadline override an earlier proposed
    installation-withdrawal reason without introducing a second timer.
- Ordinary-path cost:
  - no packet, acknowledgement, scheduler, congestion-control, timer,
    carrier-count, admission, transport, or runtime behavior changed;
  - this is a cold, production-disconnected state-machine correction; and
  - no performance claim is made until the session owner is connected and the
    historical and representative lab gates pass.
- Evidence:
  - the existing nine durable TCP service model tests cover retained, no-gain,
    fence-changed, demand-ended, failed-installation, installing-deadline,
    running-deadline, cross-direction, replayed-cleanup, and stale-token
    lifecycles;
  - two independent read-only reviews found the generalized cleanup automaton
    sound and the token safe for lock-ordered runtime integration;
  - the complete root library suite passed: 1,420 tests;
  - every Cargo target compiles under the locked dependency graph; and
  - formatting and whitespace checks passed.

## 2026-07-31T23:33:22+08:00: Exact TCP carrier ownership and quiescent port replacement

- Name: bounded configured-minimum TCP lifecycle with ranged-port replacement
- Category: Core runtime, RFC alignment, and disruption safety
- State: implementation and deterministic acceptance complete; elastic
  expansion and representative performance labs remain the next gate
- Content:
  - made the session TCP carrier group own every physical reservation,
    concurrently unique wire `PathId`, configured-minimum member, connection
    attempt, current instance, provisional successor, and retiring predecessor;
  - removed Product opens as implicit connection owners: one session service
    now reconciles missing configured-minimum members, and exact actor terminal
    state plus reservation release wakes replacement without locator or source
    address inference;
  - made authenticated peer status report the exact physical carrier inventory
    and reject overlapping/incomplete stable-member observations instead of
    publishing a partial logical view;
  - made attachment and native transport observations retain their exact
    physical instance so an old carrier cannot borrow a successor's health,
    capacity, or failure authority;
  - established one authoritative session Product-flow owner, independent of
    telemetry, for the complete reliable or datagram logical lifetime,
    including peer-direction work, retention, and recovery;
  - moved TCP datagram path-load ownership before carrier I/O and retained it
    with the exact attachment, matching the established reliable-open
    transaction;
  - defined planned TCP port replacement at an exact session
    Product-quiescent boundary: a spare-capacity successor authenticates first
    and commits atomically or is discarded, while a full group fences and
    drains the predecessor before restoring the selected alternate port;
  - made final logical-flow, cross-underlay load, and relay-flight release wake
    an already-overdue hop, while the configured hop interval remains only an
    eligibility boundary and never proves quiescence or usefulness; and
  - kept `1-3` as the default configured TCP carrier range and preserved the
    configured maximum across establishing, ready, and draining instances.
- Ordinary-path cost:
  - one session-flow ownership increment/decrement occurs at logical Product
    flow creation/terminal release, not per packet;
  - existing path-load and relay-flight release transactions publish a carrier
    lifecycle event only on the final session Product-quiescent transition;
  - no scheduler score, congestion controller, packet format, transport
    parameter, retransmission timing, hop interval, or performance threshold
    changed.
- Evidence:
  - the persistent session-ownership regression proves a logical Product flow
    with zero attachments and cross-underlay Product load both fence TCP
    replacement, and their final release wakes maintenance;
  - the ranged spare-capacity integration test carries Product traffic beyond
    the five-second hop interval without establishing a successor, then proves
    owner-event-driven make-before-break replacement, alternate-port
    selection, physical overlap, stable `SessionId`, fresh carrier instance,
    distinct concurrent `PathId`, and exact predecessor reservation release;
  - the maximum-one integration test uses an unrelated 60-second probe
    interval, proves no drain while Product traffic is active, then converges
    after the owner event within its bounded test deadline without exceeding
    one physical carrier;
  - the complete root library suite passed: 1,436 tests;
  - all targets and all features passed Clippy with warnings denied;
  - the standalone patched `quinn-proto` suite passed: 282 unit tests and 3
    doctests; and
  - formatting and whitespace checks passed. This milestone establishes
    correctness and lifecycle timing; it does not claim a new throughput
    verdict. Historical restoration and the representative competitive lab
    matrix remain mandatory before release.

## 2026-08-01T13:01:56+08:00: Restored source-gate baseline

- Name: clean `6aac504` source boundary and benchmark lock repair
- Category: Build reproducibility and Core acceptance
- State: complete; representative performance evidence remains the next gate
- Content:
  - rejected the incomplete elastic-runtime work after preserving it below the
    ignored `./.tmp/forensics/` tree, and restored the last clean Core
    lifecycle boundary without accepting any of its runtime or RFC changes;
  - repaired the benchmark crate's stale lock dependency for the root
    `same-file` dependency, with no dependency-version or runtime change; and
  - kept candidate expansion disconnected, so this boundary changes no packet,
    scheduler, congestion-control, admission, timing, or carrier behavior.
- Evidence:
  - formatting and strict all-target, all-feature Clippy passed;
  - the main Rust, standalone Quinn, lab Python, benchmark, and packaging test
    suites passed 1,941 tests in total;
  - the 29-cell/66-metric performance registry, seven shell syntax checks, and
    release-version self-test passed; and
  - the benchmark lock repair is exactly one dependency-list entry. No
    performance verdict is claimed until the representative lab is repeated
    without concurrent build or container load.

## 2026-08-01T13:16:59+08:00: Accepted historical-performance boundary

- Name: clean eight-row TCP/QUIC representative restoration
- Category: Core performance evidence
- State: accepted baseline; elastic TCP work may proceed behind unchanged
  ordinary-path gates
- Method:
  - built the optimized native Linux binary once, then repeated the matrix with
    `BUILD_PRODUCT=0` so the host snapshot did not contain compiler load;
  - used the documented 20-second, 500-Mbit/s, 180-ms, zero-loss equal-fat and
    high-bandwidth single-path profiles with path hints disabled; and
  - retained the first run only as diagnostics because its host snapshot
    correctly rejected residual build load (`0.731` per available CPU).
- Accepted evidence:
  - source commit `982548d`, clean source snapshot, exact client/server binary
    identity, no external containers, and valid host load (`0.201` per
    available CPU);
  - TCP/QUIC single-path downloads: `257.716` / `298.191` Mbit/s;
  - TCP/QUIC equal-fat downloads: `793.576` / `742.797` Mbit/s;
  - TCP/QUIC single-path uploads: `251.097` / `293.331` Mbit/s receiver-confirmed
    duration lower bounds with zero recovery gap; and
  - TCP/QUIC equal-fat uploads: `537.303` / `749.681` Mbit/s exact completed
    results with zero recovery gap.
- Interpretation:
  - the accepted cohort restores the retained high-performance range without
    a parameter, timing, scheduler, congestion-control, or transport change;
  - the two single-path upload rows ended by the declared duration and are
    deliberately classified as receiver-confirmed lower bounds rather than
    silently promoted to exact completions; and
  - no fixed percentage cap is inferred from one run. Later Core changes must
    repeat matched rows and diagnose timing, delivery, and host validity before
    acceptance.

## 2026-08-01T13:50:12+08:00: Frozen elastic TCP validation model

- Name: direction-neutral Product-service proof for TCP carrier retention
- Category: Core RFC and implementation boundary
- State: accepted model; runtime remains deliberately disconnected until one
  complete directional integration can preserve these authorities exactly
- Clean model:
  - one transition into sustained ordinary placement failure creates one
    admission generation, in which each eligible TCP group is attempted at
    most once without timer, queue-oscillation, or failure retries;
  - one frozen comparison key and existing startup, Data-ACK, service-pipe,
    and Product-resource geometry govern an adjacent ordinary, startup,
    assisted, and ordinary-confirmation sequence;
  - exact writer and Data-ACK boundaries form whole causal cohorts, and exact
    integer fractions require strict target-flow and aggregate-session gain
    over both adjacent ordinary controls;
  - candidate assignment, resolution, and qualified release are separate:
    assisted cohort closure seals new assignment while already-assigned flight
    may resolve before the zero-work confirmation boundary; and
  - expiry, changed authority, malformed order, ambiguous evidence, overflow,
    or insufficient coverage fail closed as `WITHDRAWN`; none authorizes a
    retry in the same admission generation.
- Corrections established before runtime work:
  - generalized the existing ACK coverage and Product-measurement envelope
    names without changing their arithmetic or callers' behavior;
  - rejected alignment that could round coverage below an ordinary service
    pipe, request-only evidence geometry, per-window extrema, post-close
    candidate assignment, and premature rejection of draining candidate
    flight; and
  - generalized the existing non-restarting session-retention resource
    lifetime to admitted pre-retain validation without adding a timer value.
- Evidence:
  - 12 focused state/geometry regressions pass, including exact overflow-safe
    rate ordering, phase contamination, shared-bottleneck redistribution,
    strict adjacent controls, bounded assignment, and post-cohort flight drain;
  - the complete root library suite passes 1,449 tests;
  - strict all-target, all-feature Clippy, formatting, and whitespace checks
    pass; and
  - two independent read-only reviews report no remaining RFC/model
    correctness blocker after the final assisted-flight correction.
- Performance boundary: the module is compiled only for tests and no runtime
  caller exists, so this milestone changes no binary behavior, packet work,
  scheduler, congestion controller, transport parameter, or timing value.
- Next: integrate one complete client-to-server validation transaction outside
  ordinary path membership, prove its exact lifecycle and unchanged disabled
  path, then repeat the same neutral owner model for server-to-client service.

## 2026-08-01T19:04:01+08:00: Accepted client-to-server elastic TCP lifecycle

- Name: RFC-owned C2S validation, exact retained-carrier adoption, and matched
  performance gate
- Category: Core runtime, TCP aggregation, and performance acceptance
- State: client-to-server direction complete and accepted; symmetric
  server-to-client authority remains the next Core milestone
- Clean model:
  - one sender-owned ordinary-saturation transition creates one bounded
    admission generation; repeated blockage, ACK silence, timers, native TCP
    state, locators, and source or interface identity create no attempt;
  - the validation-purpose connection owns one exact elastic reservation,
    fresh wire `PathId`, frozen Product comparison, target attachment
    incarnation, finite unique-original work, immutable result transaction,
    and ordered settlement without entering ordinary membership early;
  - an exact `RETAIN` acknowledgment consumes the existing attachment
    reservation and transport, then publishes one sparse health record and one
    retained actor for that exact physical instance; it creates no second
    stream ID, `OPEN_STREAM`, carrier connection, or configured-minimum actor;
  - negative results resolve candidate flight, detach the exact attachment,
    drain the carrier, and hold the physical reservation until `PATH_CLOSE` or
    native failure; exact failure removes health and registry publication
    before releasing the reservation; and
  - the original absolute retention ceiling is unchanged. Sender expiry emits
    immutable `WITHDRAWN` only when already-admitted ordered work and the result
    are immediately writable, then retires the exact carrier without extending
    the ceiling; otherwise native failure is the terminal fallback.
- Correctness evidence:
  - commit `95f09ca` is a clean source boundary with no production timer,
    percentage, EWMA, congestion-control, pacing, transport-parameter, or
    scheduler-score change;
  - the encrypted-wire lifecycle regressions cover exact retained handoff,
    negative acknowledgment plus zero-work drain, writable expiry settlement,
    sparse publication cleanup, configured-range reconciliation, active-path
    failure, and exact attachment adoption;
  - strict all-target, all-feature Clippy passes with warnings denied; and
  - the locked all-feature suite passes 1,475 library tests, 2 persistent
    allocation tests, 6 packaged daily-use acceptance tests, and doctests.
- Matched performance evidence:
  - the first post-link cohort was rejected because its host-load snapshot was
    invalid; no result from it was promoted to acceptance;
  - the accepted clean-source full cohort used the historical 20-second,
    500-Mbit/s, 180-ms, zero-loss profiles with path hints and lab
    instrumentation disabled, a valid host snapshot, the same Rust toolchain,
    cached lab images, and binary SHA-256
    `200925426eca8c6559bd77591bd6154251b4b75e8d586df16a8fee780ea31a04`;
  - TCP single/equal-fat downloads were `235.657` / `782.653` Mbit/s and
    uploads were `261.277` / `527.816` Mbit/s, all with zero recovery gap; an
    isolated valid single-download confirmation was `251.149` Mbit/s with
    normal startup and read-gap timing, showing that the lower full-cohort row
    was run variation rather than a reproduced structural regression;
  - QUIC single/equal-fat downloads were `311.466` / `740.593` Mbit/s and
    uploads were `294.788` / `740.783` Mbit/s, all with zero recovery gap; the
    equal-fat upload's duration lower bound was independently confirmed as an
    exact two-stream completion at `741.383` Mbit/s; and
  - TCP equal-fat throughput remains in the accepted historical range, TCP
    single upload improved, and untouched QUIC controls remain in range. No
    fixed percentage cap or parameter adjustment is inferred from individual
    samples.
- Evidence paths:
  - `./.tmp/lab/results/candidate-95f09ca-representative-2/`
  - `./.tmp/lab/results/candidate-95f09ca-tcp-single-confirm-1/`
  - `./.tmp/lab/results/candidate-95f09ca-quic-equal-upload-confirm-1/`
- Next: reuse the same direction-neutral owners for server-to-client demand,
  result authority, exact attachment adoption, and settlement; then run the
  disruption/recovery and representative performance gates before any Product
  surface or documentation phase.

## 2026-08-01T20:12:51+08:00: Accepted S2C response-ownership foundation

- Name: dormant exact response ACK provenance and validation-only output
- Category: Core runtime ownership
- State: accepted bounded foundation; no S2C demand, verdict, or wire actor is
  connected yet
- Clean model:
  - optional response Product-ACK capture mirrors the established request
    ledger and preserves exact unambiguous and ambiguous OriginalData
    subranges, while the ordinary ACK result and call path remain unchanged;
  - one exact TCP validation output is separately owned under the response
    attachment transaction, heap-allocated only while active, absent from
    ordinary targets, load registration, feedback publication, and membership
    generations;
  - finite validation data uses the existing typed bounded carrier command,
    records exact Product flight before command publication, and cannot be
    promoted while any candidate flight remains unresolved; and
  - exact negative settlement, path retirement, or stream closure invalidates
    candidate evidence, while successful promotion preserves the same carrier
    instance and output incarnation and publishes ordinary membership once.
- Performance boundary:
  - no timer, threshold, parameter, scheduling score, congestion controller,
    transport behavior, or wire format changed;
  - the capture API has no production caller, and the inactive response path
    allocates no validation state and retains the original ACK result type; and
  - representative throughput is deferred until the complete S2C transaction
    is connected, so no benchmark claim is inferred from this dormant slice.
- Evidence:
  - strict formatting, whitespace, and all-target/all-feature Clippy passed
    with warnings denied;
  - the locked all-feature suite passed 1,480 library tests, 2 allocation
    tests, 6 packaged daily-use acceptance tests, and doctests;
  - the standalone patched Quinn suite passed 282 unit tests and 3 doctests;
  - the 29-cell/66-metric performance registry, 198 lab tests, 5 deterministic
    benchmark tests, 9 packaging tests, shell syntax checks, and release-version
    self-test passed; and
  - persistent response tests prove ordinary invisibility, exact one-shot
    promotion, bounded typed dispatch, unresolved-flight exclusion,
    mixed-overlap ACK segmentation, and terminal cleanup.
- Next: connect one client-side S2C validation owner to the existing retained
  carrier authority without adding policy or timing, then add the symmetric
  server sender owner and the existing wire transaction in separate green
  commits.

## 2026-08-01T20:36:31+08:00: Accepted shared client validation ownership

- Name: one client session transaction across fresh and retained-direction
  TCP carrier validation
- Category: Core runtime ownership
- State: accepted bounded foundation; S2C demand, sender verdict, and wire
  execution remain deliberately disconnected
- Clean model:
  - the existing client session service now owns one direction-neutral
    validation transaction and one monotonic client-issued validation-ID
    sequence, while its established C2S workload and evidence owner remains
    direction-specific;
  - a fresh C2S candidate and an opposite-direction validation on any retained
    carrier exclude each other through that same session transaction;
  - retained authority is represented per direction and exact physical
    instance, and a negative result preserves already-acknowledged authority in
    the other direction; and
  - exact removal, lease abandonment, endpoint drain, and replacement release
    validation ownership after the retained-registry lock is dropped, so stale
    actors cannot settle or depublish a replacement instance.
- Performance boundary:
  - no wire frame, timer, threshold, resource bound, scheduling decision,
    congestion controller, pacing rule, transport parameter, or platform path
    changed;
  - existing C2S production behavior is mechanically preserved behind the
    shared transaction enum; and
  - the new retained-direction entry points have no production caller yet, so
    representative throughput remains deferred until the complete S2C
    transaction is connected.
- Evidence:
  - strict formatting, whitespace, and all-target/all-feature Clippy passed
    with warnings denied;
  - the locked all-feature suite passed 1,485 library tests, 2 persistent
    allocation tests, 6 packaged daily-use acceptance tests, and doctests;
  - the standalone patched Quinn suite passed 282 unit tests and 3 doctests;
  - the 29-cell/66-metric performance registry, 198 lab tests, 5 deterministic
    benchmark tests, 9 packaging tests, shell syntax checks, and release-version
    self-test passed; and
  - persistent lifecycle tests prove cross-role mutual exclusion and shared ID
    sequencing, exact directional retain/no-gain semantics, stale replacement
    fencing, and endpoint-drain cancellation.
- Review: the authoritative implementation audit and one independent read-only
  audit found no actionable ownership, lock-order, C2S-preservation, or RFC
  7.2/15.1 discrepancy.
- Next: establish the server-owned S2C demand and Product comparison owner over
  the already-separated response evidence/output seams, without connecting
  transport execution or adding policy/timing.

## 2026-08-01T21:08:48+08:00: Accepted server-owned S2C transaction model

- Name: monotonic response demand and exact sender-owned validation admission
- Category: Core runtime ownership
- State: accepted bounded foundation; carrier publication, client execution,
  and response-sender wire integration remain the next Core slice
- Clean model:
  - one server session owner serializes the continuous response-demand episode,
    successful ordinary placement, exact ordinary saturation, monotonic demand
    publication, and one exact validation admission;
  - request withdrawal and supersession preserve one nonzero server-owned
    sequence, while repeated saturation, a changed ordinary set under an
    unchanged membership generation, stale requests, candidate aliasing, and
    concurrent validation cannot mint authority;
  - admission freezes the target, complete directional Product-workload
    identities, ordinary output lifetimes and service pipes, stable policy
    generations, and the established RFC geometry without choosing or opening
    a carrier; and
  - optional response Data-ACK receipt routing is inactive behind one atomic
    read, becomes bounded only for the exact active validation, and counts
    candidate originals only under exact unambiguous carrier, attachment, range,
    and output-incarnation provenance with checked conversion and accumulation.
- Performance boundary:
  - the module has no production caller and changes no wire frame, scheduling
    decision, timer, threshold, congestion controller, pacing rule, transport
    parameter, platform path, or packet behavior;
  - no performance claim is inferred from a dormant ownership slice; and
  - the non-test dead-code scope is attached only to this unconnected module
    and remains disabled in its tests, to be removed when the following wire
    slice consumes it.
- Evidence:
  - 6 persistent RFC ownership regressions pass, covering monotonic publication
    and withdrawal, stable-generation supersession, inconsistent membership
    rejection, complete-workload freeze, bounded exact receipt routing,
    one-candidate admission, and exact candidate provenance;
  - strict formatting, whitespace, and all-target/all-feature Clippy pass with
    warnings denied;
  - the locked all-feature suite passes 1,491 library tests, 2 persistent
    allocation tests, 6 packaged daily-use acceptance tests, and doctests;
  - standalone patched Quinn passes 282 unit tests and 3 doctests;
  - the 29-cell/66-metric performance registry, 198 lab contracts, 5
    deterministic benchmark tests, 9 packaging contracts, shell syntax, and
    release-version self-test pass; and
  - the authoritative implementation review plus one independent quick
    read-only audit found no remaining RFC 7.2/15.1 ownership discrepancy.
- Next: connect this owner to one registry-scoped server session service, exact
  ready-carrier demand publication, the shared client validation-ID owner, and
  the existing directional authority transaction without adding policy or
  timing.

## 2026-08-01T21:22:39+08:00: Accepted session-level S2C demand receipt

- Name: monotonic server demand across every client TCP carrier actor
- Category: Core runtime ownership
- State: accepted bounded wire slice; demand execution and response-sender
  saturation publication remain deliberately disconnected
- Clean model:
  - ordinary, retained, and dedicated-validation TCP actors apply the same
    authenticated server demand to one client-session owner rather than to a
    target stream or carrier-local coordinator;
  - the nonzero server sequence is monotonic across concurrent carriers: older
    requests and exact duplicates are idempotent, equal-ID content conflicts
    fail as protocol errors, and a newer explicit withdrawal remains a real
    publication;
  - accepted state and its watch publication are serialized under the same
    existing session mutex, preventing an older carrier actor from publishing
    after a concurrently accepted newer request; and
  - C2S validation no longer receives the S2C-only demand frame, while both
    client and server workload owners accept `StreamId(0)`, which is the first
    valid ID allocated by the established reliable-stream owner.
- Performance boundary:
  - no carrier is opened, closed, retained, promoted, or selected by this
    slice, and no timer, threshold, configured bound, scheduler decision,
    congestion controller, pacing rule, transport parameter, or wire encoding
    changed;
  - the demand subscriber remains dormant until the next execution slice, so
    no throughput claim is inferred from this ownership correction; and
  - server demand state and publication are now linearizable without adding a
    lock or allocation to the response data path.
- Evidence:
  - strict formatting, whitespace, and all-target/all-feature Clippy pass with
    warnings denied;
  - the locked all-feature suite passes 1,492 library tests, 2 persistent
    allocation tests, 6 packaged daily-use acceptance tests, and doctests;
  - standalone patched Quinn passes 282 unit tests and 3 doctests;
  - the 29-cell/66-metric performance registry, 198 lab contracts, 5
    deterministic benchmark tests, 9 packaging contracts, shell syntax, and
    release-version self-test pass; and
  - the authoritative implementation review plus one independent quick
    read-only audit found no remaining demand-sequence, actor-coverage,
    direction-ownership, publication-order, or first-stream discrepancy.
- Next: give the registry exactly one server-session service, bind response
  workload leases and exact ordinary-saturation observations to it, and publish
  demands through an already-ready TCP carrier before connecting client-side
  candidate execution; add no policy or timing.

## 2026-08-01T22:25:31+08:00: Accepted server S2C demand publication boundary

- Name: exact response saturation to ready-carrier demand and Product-ACK
  transaction ownership
- Category: Core runtime ownership and encrypted TCP wire integration
- State: source-accepted bounded slice; client candidate execution, server
  verdict execution, and representative performance acceptance remain next
- Clean model:
  - one weakly indexed service exists per live server MPP session; ready
    ordinary and validation-purpose TCP actors retain a subscription to that
    owner, serialize its current monotonic demand exactly once, and serialize
    later requests ahead of ordinary throughput commands without changing the
    existing bounded writer run;
  - every reliable response relay holds one lifecycle-generation workload
    lease, and every target-side realtime datagram flow holds a lease in the
    same comparison boundary; lifecycle changes invalidate the complete
    workload generation, and active realtime work withdraws or rejects TCP
    expansion;
  - the response sender recognizes only the RFC 15.1 transition from a
    successful fresh ordinary placement to exact first-authority-class
    saturation: shared credit remains, every eligible ordinary output owns
    target OriginalData, every one is enqueue-blocked, and no latency-sensitive
    work is active;
  - stable comparison generations are captured with exact response output
    instances under the response-output lock. Repeated sequence updates that
    preserve effective `AVAILABLE`/`BACKUP` authority do not create fresh
    admission authority, and mutable queue, transport, staleness, rate, timer,
    and ACK-silence evidence is excluded from those generations; and
  - response Product-ACK capture freezes the exact active validation identity
    before the first ACK mutation, preserves exact original-output provenance,
    and publishes only after the complete send-cache, binding, sender-queue,
    authoritative-ACK, recovery, and FIN transaction. A withdrawn or replaced
    validation cannot receive that earlier transaction.
- Correctness findings closed during authoritative review:
  - marking the initial watch value seen prevents a ready actor from emitting
    the same current demand twice;
  - post-read response demand is published before the same-turn sender drain,
    so the first successful placement cannot occur before its demand episode;
  - demand control cannot starve behind a continuously ready throughput queue;
  - exact validation identity fences ACK receipts across concurrent validation
    replacement; and
  - realtime datagram lifecycle is no longer absent from the S2C comparison
    owner.
- Performance boundary:
  - no RFC timing, threshold, byte geometry, percentage, congestion controller,
    pacing rule, transport parameter, scheduler score, platform branch, or wire
    encoding changed;
  - successful response scheduling retains the existing path observation and
    queue operations, adding only stable-generation capture and the separate
    response-demand owner required for later bounded expansion; inactive ACK
    capture remains one atomic fast rejection with no provenance allocation;
    and
  - no throughput acceptance is inferred from this incomplete transaction.
    The frozen global map requires representative TCP/QUIC and disruption labs
    after client execution and server result settlement are connected.
- Evidence:
  - six new persistent RFC regressions prove exact saturation and negative
    boundaries, stable authority generations, realtime lifecycle fencing,
    exact-validation ACK fencing, and encrypted ready-actor current/withdrawal
    delivery under a full throughput queue;
  - formatting, whitespace, locked all-target/all-feature checks, and strict
    Clippy pass with warnings denied;
  - the locked all-feature suite passes 1,498 library tests, 2 persistent
    allocation tests, 6 packaged daily-use acceptance tests, and doctests;
  - standalone patched Quinn passes 282 unit tests and 3 doctests; and
  - the 29-cell/66-metric performance registry, 198 lab contracts, 5
    deterministic benchmark tests, 9 packaging contracts, shell syntax, and
    release-version self-test pass.
- Review: the authoritative line-by-line review retained final editing
  authority; three independent quick read-only audits identified concrete
  starvation, transaction-fencing, and realtime-workload gaps, all reproduced
  or resolved and covered above. No subagent edited the tree.
- Next: connect the shared client validation-ID owner to fresh or retained
  S2C candidate establishment, bind the server admission to the exact existing
  response attachment, execute the established bounded comparison and immutable
  result/acknowledgment settlement, then run the representative and disruption
  performance gates before advancing to Product completion.

## 2026-08-02T16:06:41+08:00: Accepted v0.1.5 lifecycle and negotiated load gates

- Name: request-local QUIC cancellation, attachment-local refusal, and bounded
  browser/20-path load evidence
- Category: Core lifecycle correctness and release performance verification
- State: accepted implementation and persistent lab gates; clean-tree release
  comparison remains a final pre-tag gate
- Clean model:
  - canceling an incomplete HTTP/3 carrier request resets only that request;
    it does not close the shared QUIC connection or leave partially written
    DATA state available for reuse;
  - refusing a pending `OPEN_STREAM` attachment uses `STREAM_DETACH` on the
    carrying TCP or QUIC carrier, while `STREAM_RESET` remains reserved for
    terminating the logical MPP stream and all of its attachments;
  - the ordered-detach fence remains unchanged and authoritative; no timeout,
    scheduler, congestion-control, pacing, capacity, or carrier-count parameter
    changed; and
  - the periodic browser gate retains its 3-second batch SLA, while the distinct
    saturation gate admits work for exactly 60 seconds at no more than 20 live
    1 MiB requests and lets already accepted requests drain under the configured
    probe completion timeout.
- Practical cause and evidence:
  - the initial 20-by-60 run exposed 12 incomplete requests and 15 H3 internal
    errors; request-local cancellation removed every H3 internal error;
  - the remaining 8 truncated bodies correlated with attachment retries during
    ordered detach being answered by a logical `STREAM_RESET(Refused)`;
  - after correcting refusal scope, the final retained saturation result is
    `status=ok`: 570 started, 570 accepted, 570 completed, zero rejected, zero
    incomplete, peak concurrency 20, 60-second admission window, 9.5 completed
    requests/s normalized over the admission window; the retained 597,688,320
    payload bytes over the complete 65.292-second run are 73.233 Mbps exact
    elapsed goodput; and
  - persistent TCP and QUIC regressions reproduce the ordered-detach retry and
    prove an exact `STREAM_DETACH` response while the logical stream remains.
- Twenty-link variation evidence:
  - each of six cases configured 10 TCP and 10 UDP links, applied five complete
    independently seeded bandwidth/latency/jitter/loss epochs, and passed in
    both directions;
  - access (30--100 Mbps/path): 344.534 Mbps download and 210.378 Mbps upload;
  - gigabit (300--1000 Mbps/path): 1,178.811 Mbps download and 609.004 Mbps
    upload; and
  - multi-gigabit (3--10 Gbps/path): 2,261.932 Mbps download and 670.693 Mbps
    upload. The VM became the high-band execution ceiling, so these results
    establish successful completion, aggregation, and adaptation rather than a
    claim that this host can saturate the configured aggregate line rate.
- Verification:
  - 175 focused Core path tests passed before the two new boundary regressions;
  - both new TCP and QUIC attachment-refusal regressions pass;
  - 75 focused Python lab/model contracts and shell syntax pass; and
  - all retained lab result rows and every path-variation schedule report
    `status=ok` and `trace_complete=true`.
- Reproducibility note: the lab host validity record rejected comparison status
  solely because the source tree necessarily contained the in-progress v0.1.5
  changes. A clean committed-tree release gate remains required before tagging.
- Next: perform the already-approved mechanical source/test layout pass, finish
  Product/public-documentation and dependency audits, then run the clean-tree
  final release gates without further model changes.

## 2026-08-02T17:42:31+08:00: Live dashboard and public evidence hierarchy accepted

- Name: dense live Overview and baseline-first README presentation
- Category: Product observability, public documentation, and evidence scope
- State: dashboard implementation and live rendering accepted; README evidence
  structure accepted for final documentation reconciliation
- Dashboard result:
  - Overview now presents current upload/download speed, cumulative traffic,
    active flows, paths, queue/flight, delivery, inbound connections, services,
    configured inbounds/outbounds, MPP sessions, paths, and balancer readiness
    from the existing management schema;
  - history expands from a fresh load and then uses the selected 15-minute,
    1-hour, 6-hour, 24-hour, or unbounded browser window; speed is the default
    graph and cumulative traffic remains one compact alternate;
  - refresh is transactional, retaining the last complete layout while the
    next status request is in flight; the collapsible navigation state and a
    successfully authenticated token persist in same-origin browser storage;
    and
  - the final live 1600-by-1000 capture contains three active inbound reliable
    flows, one connected MPP session, two active TCP/QUIC paths, and changing
    application traffic. Responsive 768-by-900 and 390-by-844 checks had no
    console error or horizontal overflow.
- Public evidence decision:
  - the executable Docker runner exposes 196 selectable case names spanning
    direct controls, pinned Xray/Hysteria2 and kernel-MPTCP baselines,
    TCP/QUIC/mixed reliable traffic, datagrams, TUN, condition matrices,
    asymmetry, blackholes, latency changes, saturation, port migration,
    adaptive TCP carriers, 20-link variation, and browser load;
  - the 29-cell/66-metric registry is an acceptance blueprint rather than
    measured evidence, and rejected, superseded, simulator, or diagnostic
    results cannot become public performance claims;
  - README now keeps the complete same-condition external baseline table,
    preserves the five-path MPTUNNEL/MPTCP comparison boundary, and summarizes
    recovery/load evidence with compact numeric rows instead of internal case
    names or explanatory paragraphs inside tables; and
  - v0.1.5 scale/browser rows remain local capability evidence until the final
    clean-source release gate; they are not blended into external comparisons.
- Performance boundary: management projection and Core telemetry were not
  changed. History retention, graph mode, navigation state, and refresh
  transactions are browser-only; README changes make no runtime claim beyond
  the recorded evidence.
- Verification: dashboard JavaScript syntax, asset contract test, whitespace,
  1600/768/390 rendering, delayed-refresh layout stability, and live-state
  capture pass. README has no fixed release number or development-plan term.
- Next: reconcile the exhaustive configuration reference, shipped examples,
  operations guide, and remaining public documents with the implemented
  schema, then complete the mechanical test layout and final source gates.

## 2026-08-02T17:59:24+08:00: Configuration contract and Rust test layout reconciled

- Name: accepted configuration mirror and role-local test organization
- Category: Product documentation and mechanical source structure
- State: implemented and source-compiled; no protocol, Product data flow,
  scheduler, congestion, timing, capacity, or performance parameter changed
- Configuration result:
  - the exhaustive reference now mirrors outbound `credential_id`, inbound
    `credential_ids`, all resource fields, DNS transports/IP strategies and
    exact/suffix rules, routing/ACL fields, MPP inbound `dns_plan`, ranged
    TCP/QUIC hopping, and TCP `tcp-carriers=MIN-MAX` with default `1-3`;
  - the shipped client and server examples expose the operator-relevant
    `allow_peer_diagnostics` setting as a disabled comment; and
  - the README certificate command creates a non-CA server-authentication
    leaf accepted by the configured TLS verifier.
- Structure result:
  - all 186 Rust test source files now use the `tests_<owner>.rs` form;
  - 45 remaining inline test modules were moved to sibling test files without
    changing their bodies or owner module; and
  - no Rust suffix-form test file, inline `mod tests { ... }`, or singleton
    source directory remains in the root crate, Quinn mirror, benchmark crate,
    or integration-test tree.
- Evidence:
  - all three shipped TOML documents parse, the source-backed reference-config
    contract passes, and documentation whitespace checks pass;
  - `cargo fmt --all -- --check` passes;
  - the root `--all-features` library test target compiles; and
  - the Quinn mirror and benchmark-crate test targets compile independently.
- Next: remove remaining internal-only public documentation, complete the
  bounded Product/config/dependency audit, then run the final release gates.

## 2026-08-02T19:16:22+08:00: v0.1.5 source and Linux package boundary accepted

- Name: final Product, lifecycle, documentation, dependency, and package audit
- Category: release acceptance
- State: source and local package gates pass; one clean-tree representative
  TCP/QUIC measurement remains before push and tag
- Correctness boundary:
  - incomplete HTTP/3 request cancellation retires only that request stream;
    pending attachment refusal uses `STREAM_DETACH`, while logical-stream
    termination remains `STREAM_RESET`;
  - Product delivery is reserved before a QUIC write, only still-pending bytes
    are rolled back on cancellation or error, and native-ACK publication cannot
    later attribute canceled bytes;
  - no scheduler, congestion controller, pacing rule, timer, threshold,
    carrier range, transport parameter, platform policy, or wire geometry
    changed; and
  - all 195 Rust test files follow `tests_<owner>.rs`, all source test paths
    resolve, no inline test body or singleton source directory remains, and the
    move preserved every prior test except the intentional cancellation
    lifecycle replacement.
- Product and public surface:
  - README presents compact product/baseline and numeric performance tables
    without a fixed release number or internal development process; lower-bound
    upload values are not promoted to a ratio or ranking;
  - the public configuration reference mirrors the implemented credentials,
    DNS, routing, outbound security, resource, port-range, and `1-3` TCP
    carrier fields, including all three bounded chunk/range limits;
  - bounded flow detail is labeled `Shown`/`Shown I/O` and reports overflow;
    cumulative chart counters remain exact decimal strings and only a bounded
    BigInt offset is converted for canvas plotting; and
  - a fresh 1600-by-1000 live browser capture contains three active inbound
    flows, one authenticated MPP session, two TCP/QUIC paths, changing speed,
    and zero console warnings. Speed and cumulative-traffic modes both render.
- Evidence boundaries:
  - the 10-TCP/10-QUIC cases span 30--100, 300--1,000, and 3,000--10,000
    Mbps/path ranges and prove completion/adaptation, not configured-rate
    saturation or universal optimality;
  - asymmetric download placed 91.5% on the faster direction; the exact upload
    check placed 86.6% on its faster direction but is retained only as path-use
    evidence because its host state was unsuitable for throughput comparison;
  - periodic browser load completed 90/90 within its three-second batches; the
    60-second, 20-concurrent, 1-MiB closed loop completed 570/570 with no
    rejection or incomplete response; and
  - the release workflow contains no provenance/signing step and publishes only
    seven versioned bundles plus `version.json`, whose per-asset fields are
    exactly `name` and tag-specific `download_url`.
- Verification:
  - formatting and warnings-denied all-target/all-feature Clippy pass;
  - root tests pass 1,519 unit tests, 2 allocation contracts, and 6 packaged
    daily-use scenarios; maintained Quinn passes 282 tests and 3 doctests;
  - 207 lab contracts, 5 benchmark/trace tests, 9 packaging contracts, the
    29-cell/66-metric registry, version gate, Bash syntax, ShellCheck,
    Actionlint, JavaScript syntax, and whitespace checks pass; and
  - the normalized Linux amd64 archive is a static PIE reporting
    `mptunnel 0.1.5` and contains exactly five contracted files, with no project
    license copy or checksum sidecar.
- Next: commit this exact candidate, run the eight-row clean-source TCP/QUIC
  single/equal-path download/upload matrix without instrumentation, and proceed
  directly to native GitHub CI, tag, immutable release verification, and
  requested generated-cache cleanup if it remains in the accepted range.

## 2026-08-02T20:49:02+08:00: Terminal delivery and public table boundary accepted

- Name: reliable terminal-state completion and concise operator tables
- Category: release correctness, performance recovery, and presentation
- State: implementation and representative labs accepted; final workflow gate
  and one committed-tree TCP upload confirmation remain before push
- Clean model:
  - cumulative receive state below the byte cadence receives one publication at
    the existing delayed Data ACK deadline, then clears its pending state;
  - bounded carrier-queue backpressure retains client `STREAM_FIN` state and
    reuses the existing capacity notification and retry deadline at all three
    FIN publication sites;
  - pre-mutation client TCP-validation reservation backpressure remains pending,
    matching the existing server-to-client reservation boundary; and
  - no wire field, timer, threshold, scheduler, congestion controller, carrier
    range, transport parameter, or platform branch was added or changed.
- Practical evidence:
  - the previously incomplete TCP upload now completes 2/2 final targets at
    263.474 Mbps over the exact 20-second, zero-loss frozen condition;
  - the matched affected controls complete at 267.666 Mbps TCP single-path
    download and 514.668 Mbps five-path TCP upload; and
  - focused relay tests pass, including the durable sub-cadence ACK-tail
    regression; two independent source-boundary audits report no blocker.
- Product presentation:
  - public Markdown tables contain compact fields and values without prose
    description columns;
  - dashboard table headers define compound metric order and data cells contain
    short states, numbers, durations, or units instead of repeated descriptions;
  - a live 1600-by-1000 capture with three connections, two paths, one session,
    and changing speed has no console error, warning, or visible overflow; and
  - internal evidence-process prose was removed from public architecture and
    platform guidance while operator capability boundaries remain explicit.
- Evidence: `git diff --check`, dashboard JavaScript syntax, four dashboard
  contract tests, and the live browser inspection pass. The final accepted
  capture is `docs/assets/dashboard.png`.
- Next: run the complete release-quality workflow, commit the bounded follow-up,
  then run one exact clean-tree TCP upload gate before the authorized push.

## 2026-08-02T21:48:42+08:00: v0.1.5 frozen; v0.1.6 audit boundaries established

- Name: immutable v0.1.5 delivery and bounded documentation/observability repair
- Category: release evidence, maintained dependency boundary, and Product
  management presentation
- State: v0.1.5 is released and independently verified; v0.1.6 corrections are
  implemented locally and focused checks pass, with public evidence
  reconciliation and final release gates still pending
- Frozen v0.1.5 evidence:
  - commit `6e2504b8c8b511786e11b473872eb9d217c766f8` is tagged `v0.1.5`;
  - native CI run `30748787226` passed Linux amd64/arm64, Windows amd64/arm64,
    macOS amd64/arm64, Android arm64, and source-quality jobs;
  - release run `30749377077` published an immutable non-draft release with
    seven versioned platform bundles plus `version.json`;
  - every downloaded archive passed the release contract, archive-layout
    contract, and GitHub asset-digest check; and
  - the final clean committed-tree TCP upload gate completed 2/2 at 253.243
    Mbps with a valid comparable host record.
- v0.1.6 audit decisions:
  - the repository test naming convention no longer rewrites the maintained
    Quinn mirror: `./crates/quinn-proto/src` is restored byte-for-byte to the
    accepted 0.11.16 baseline commit, which retains upstream layout plus the
    eight production deviations and two MPTUNNEL BBR regressions;
  - the standalone Quinn suite passes 282 tests and three doctests;
  - Overview reuses the existing bounded peer-status result and now exposes
    direction, RTT variance, pacing/flight limits, confidence, sample age, and
    application-limited state with terse numeric cells;
  - `allow_peer_diagnostics` is presented precisely as this endpoint's local
    reply permission; automatic peer requests remain one selected session,
    completion-driven, non-overlapping, and limited to Overview or Diagnostics;
  - per-service active-flow projection is labeled `Flows shown` and its I/O is
    explicitly bounded to the shown records, while aggregate traffic remains
    exact; and
  - health requests now read the sampler cache instead of recollecting full
    Product/path/balancer state on every dashboard or probe request. Startup
    seeds the cache and the existing one-second sampler remains authoritative.
- Performance boundary: no RFC field, wire behavior, transport algorithm,
  scheduler, congestion controller, pacing rule, carrier range, timer,
  threshold, or platform branch changed. The health-cache change removes a
  duplicate management read path and cannot reduce data-plane capacity.
- Focused verification: formatting, dashboard JavaScript syntax, the durable
  dashboard contract, the health cache-identity regression, whitespace, and
  the standalone Quinn suite pass.
- Evidence audit: all requested shaped/unshaped baselines, asymmetry,
  20-carrier variation, browser load, blackhole, latency transition, flapping,
  and port-hop topologies already exist. v0.1.6 will reuse them rather than add
  duplicate cases; only clean comparable results may enter public tables.
- Next: reconcile `README.md` and `docs/PERFORMANCE.md` around one concise
  methodology and accepted numeric cohorts, diagnose the unshaped local host
  ceiling without changing Core constants, then run the bounded v0.1.6 release
  matrix.

## 2026-08-02T23:00:38+08:00: v0.1.6 management and TCP failure lifecycles accepted

- Name: stable diagnostics transport, exact failed-carrier replacement, and
  final Overview presentation
- Category: Product observability and carrier lifecycle correctness
- State: implemented and verified with real Product traffic; the existing
  evidence cohort and public performance synthesis remain before release
- Clean model:
  - peer-status requests retain the last successful carrier until that carrier
    times out or unregisters, avoiding TCP bulk head-of-line delay without a
    protocol, timeout, or data-scheduling change;
  - an exact TCP instance already fenced by Product data-plane failure now
    enters the existing terminal failure lifecycle, rather than the planned
    `PATH_DRAIN` lifecycle that may validly wait for ordered peer settlement;
  - actor termination drops the exact capacity reservation and wakes the
    configured-minimum reconciler; stale instance reports cannot terminate a
    replacement; and
  - the existing connection-attempt interval remains the churn gate. No timer,
    threshold, scheduler, congestion controller, carrier range, wire field, or
    platform branch changed.
- Practical evidence:
  - one initial peer request selected the TCP carrier under concurrent bulk
    load; after fallback selected QUIC, ten consecutive peer requests completed
    in 2--7 ms, and a second run completed all ten in the same range;
  - focused lifecycle and exact-instance tests pass, and the production-boundary
    integration replaces a stable fenced TCP minimum instance inside 1.5 s;
  - three concurrent 256 MiB SOCKS5 downloads kept both TCP and QUIC paths
    active, with zero suspect or failed paths throughout sampled traffic; and
  - the authenticated dashboard returned ten consecutive peer responses with
    HTTP 200 while exposing both server-to-client TCP and QUIC path rows.
- Presentation:
  - Overview contains active inbound connections, current speeds, bounded
    history, Product traffic, admission, services, sessions, local and peer
    paths, outbounds, and balancers in one dense layout;
  - table cells use numbers, units, identifiers, or short categorical states;
    compound headings are abbreviated and expanded only through hover text;
  - the accepted 1600-by-1000 screenshot contains three live connections, one
    session, two active paths, changing speed, and no layout gap or overflow;
    and
  - the browser console reports zero errors and zero warnings after the stable
    authenticated reload.
- Verification: warnings-denied all-target/all-feature Clippy, JavaScript
  syntax, formatting, whitespace, focused queue/registry/integration tests,
  ten peer-status HTTP requests, and real browser inspection pass.
- Next: freeze the source candidate, run the existing comparable product and
  multipath lab cohort once, reconcile public numeric evidence, then execute
  the final release-quality matrix.

## 2026-08-02T23:52:04+08:00: v0.1.6 performance and public evidence accepted

- Name: adjacent competitor/multipath cohort and local processing-ceiling
  diagnosis
- Category: release performance acceptance and public documentation
- State: accepted without a Core change; final package and release gates remain
- Comparable shaped evidence:
  - one invocation used clean commit `24fa1a2`, one prebuilt optimized binary,
    two flows for 20 seconds, disabled path hints/diagnostics, and a valid host
    snapshot with no external container load;
  - MPP/QUIC completed at `240.475/180.017` Mbps on the 500 Mbps, 180 ms
    one-way, 20 ms jitter, 1% loss path; Hysteria2 completed download at
    `96.371` Mbps and retained an incomplete upload lower bound of `115.109`
    Mbps;
  - MPP/TCP completed at `123.628/103.307` Mbps on that single path; Xray
    completed download at `209.203` Mbps, while its upload closed before
    terminal target acknowledgement and was excluded from comparison; and
  - on five equal 500 Mbps, 180 ms, zero-loss paths, MPP/TCP completed at
    `732.433/508.562` Mbps, MPP/QUIC at `629.507/707.077` Mbps, and Linux MPTCP
    at `302.101/434.432` Mbps.
- Correctness and timing evidence:
  - every MPTUNNEL row completed with exact receiver accounting, zero identity
    residual, and zero reliable recovery gap;
  - all five configured paths carried traffic, but shaped TCP upload placed
    `97.7%` on two paths and is not represented as five-way aggregation; and
  - qdisc evidence showed no unintended drop, overlimit, or retained backlog
    in the equal-path cases.
- Unshaped diagnosis:
  - direct local bridge controls reached `20.452/22.325` Gbps;
  - MPP/TCP reached `6.704/1.084` Gbps on one path and `4.014/1.665` Gbps on
    five paths; MPP/QUIC reached `2.622/2.704` and `2.728/2.676` Gbps;
  - all ten rows completed exactly with all multipath carriers used and no
    recovery gap; and
  - download and QUIC-upload rows approached the two-vCPU container cap;
    TCP upload did not remain CPU-saturated, so its lower local ceiling is not
    attributed to Docker CPU alone or converted into a production threshold.
- Public result:
  - README now puts the adjacent Xray, Hysteria2, MPTCP, and MPTUNNEL numbers
    first, combines the Product and multipath explanation, and keeps table
    cells to short labels and numeric values; and
  - `docs/PERFORMANCE.md` retains the full matched interpretation, exact
    partial-baseline boundary, unshaped ceiling, scale, continuity, and limits
    without mixing cohorts into one ranking;
  - quick-start host, port, certificate, and credential examples now match the
    shipped TOML profiles, and their schema contract test passes; and
  - source ownership and native host integration details moved from public
    docs to persistent ignored `./docs-dev/` references.
- Next: run the final bounded source/package workflow, freeze the documentation
  commit, tag and publish `v0.1.6`, verify every immutable asset and
  `version.json`, then clean generated caches before the requested follow-up.

## 2026-08-03T00:56:34+08:00: default TCP range restored to exact local-ceiling completion

- Name: exact failed-owner request recovery
- Category: RFC lifecycle correctness and release performance
- State: implemented and accepted by the focused model suite and clean default
  `1-3` carrier lab; final release gates remain
- Root cause:
  - the request actor admitted one bounded recovery quantum when an exact
    attachment disappeared, but its continuing range-recovery scan considered
    only live stale attachments;
  - a failed validation carrier could therefore leave a disjoint OriginalData
    tail without the RFC-required repeat deadline; and
  - the displayed `0.846 Gbps` failure was not the active transfer rate: the
    client accepted `14,905,638,912` bytes in 20 seconds, then one stream spent
    120 seconds awaiting completion with `36,362,391` bytes unresolved.
- Clean correction:
  - failed and live-stale OriginalData owners now share the existing exact-range
    flight ledger, MPP recovery interval, queue/flight bounds, and actor wake;
  - the redundant failed-owner attempt clock and membership-only recovery pass
    were removed; and
  - no RFC timer, threshold, scheduler, receive window, carrier range,
    congestion controller, wire field, or platform branch changed.
- Evidence:
  - the source-equivalent fixed `1-1` isolation completed `2/2` exactly at
    `6.001 Gbps`, proving the request data path already retained its historical
    local ceiling;
  - the corrected default `1-3` run delivered `16,140,075,008` bytes exactly,
    completed `2/2` with zero failures, and reached `6.286 Gbps` over
    `20.541 s`; and
  - 56 focused request-sender tests pass, including exact attachment fencing,
    disjoint-range eligibility, recovery-copy suppression, and repeat expiry.
- Next: reconcile the public numeric performance evidence, run the bounded
  source/package gates, publish and verify immutable `v0.1.6`, then clean
  generated state before the separately requested follow-up.

## 2026-08-03T02:52:30+08:00: contiguous-frontier service restored

- Name: remove the redundant Product inflight controller from the exact
  contiguous owner
- Category: RFC conformance and multipath performance
- State: implemented; focused model gates and adjacent five-path proof pass;
  elastic and shaped representative gates remain
- Practical root cause:
  - the maintained five-TCP-path upload reproduced a mid-transfer fall from
    `5.43 Gbps` to `0.43 Gbps` while all five carriers remained active, native
    queues were empty, and native delivery estimates stayed high;
  - after exact per-flow evidence matured, the scheduler replaced its startup
    prior with a roughly `0.5-0.72 MiB` Product service window and applied that
    path-local window to the exact lowest outstanding Data Sequence owner; and
  - diagnostic decisions showed that owner rejected by `inflight_limit` even
    with no native bytes in flight. No request-path staleness event occurred.
- Clean correction:
  - the admission model now distinguishes a first-ranked candidate from the
    exact `ContiguousFrontier` owner derived from the flight ledger;
  - without latency pressure, only that exact owner relies on shared MPP
    credit, carrier enqueue capacity, and native TCP/QUIC control as required
    by RFC Sections 10.3 and 15.1;
  - shared reorder, completion, receive-credit, queue, configured-resource,
    and latency-pressure bounds remain; every additional or merely ranked path
    retains its existing Product window; and
  - no timer, Mbps threshold, carrier range, congestion controller, recovery
    rule, wire field, or platform branch changed.
- Evidence:
  - all `76` admission-model, `23` request-scheduler, and `34`
    response-scheduler tests pass, including new transport-neutral frontier,
    shared-reorder, and latency-pressure regressions; and
  - the adjacent non-instrumented five-path upload completed exact accounting
    at `5.446 Gbps`, versus `2.334 Gbps` before the correction, with interval
    goodput remaining `2.684-8.070 Gbps` instead of collapsing below
    `1 Gbps`; raw evidence is under
    `./.tmp/lab/results/v016-tcp-multipath-frontier-fix/`.
- Rejected alternative: ambiguous Data ACK release was not changed because
  neither the trace nor the RFC permits treating duplicate delivery as exact
  path-progress attribution.
- Next: run one default elastic `1-3` proof and the bounded affected shaped
  matrix, then freeze public evidence and execute release gates.

## 2026-08-03T03:41:26+08:00: native enqueue boundary and final Core gate accepted

- Name: exact-frontier service with adaptive TCP carrier expansion
- Category: RFC conformance, neutral fallback, and release performance
- State: accepted; the affected Core performance gate is closed and no further
  model change is justified for v0.1.6
- Final model:
  - the exact lowest outstanding OriginalData owner is not throttled by its
    overlapping Product Data-ACK flight;
  - when native credit is available, disjoint carrier queue plus native flight
    is checked against native congestion credit plus the existing bounded feed
    quantum, so genuine enqueue saturation remains visible to RFC Section 15.1
    elastic admission;
  - when native credit is unavailable, the runtime-derived Product service
    limit remains authoritative, with the existing derived service window used
    only when that limit is absent; and
  - first-ranked and additional paths retain the prior Product, startup, ECF,
    reorder, latency, receive-credit, and configured-resource bounds. No wire
    field, timer, Mbps or percentage threshold, congestion controller, carrier
    range, or platform branch changed.
- Adaptive range evidence:
  - with a 100 Mbps per-native-TCP-flow limit, `1-1` delivered
    `75.239/78.370` Mbps and `1-3` delivered `111.686/121.669` Mbps for
    download/upload; the ranged endpoint retained two carriers and stopped
    before three;
  - with one shared 200 Mbps bottleneck, `1-1` delivered
    `157.321/156.397` Mbps and `1-3` delivered `154.044/161.143` Mbps, staying
    at the same aggregate ceiling without opening a third carrier; and
  - all eight transfers completed exactly with zero failed requests or
    streams. These measured rates are evidence, never production thresholds.
- Regression evidence:
  - unshaped five-path TCP upload completed exactly at `5.408 Gbps`, with zero
    recovery gap, all five carriers active, and interval service remaining
    healthy; the adjacent prior proof was `5.446 Gbps`;
  - equal five-path TCP completed at `783.714/502.900` Mbps and QUIC at
    `625.688/742.513` Mbps for download/upload, with zero failures and zero
    reliable recovery gap; and
  - the material TCP blackhole upload completed at `266.009` Mbps with zero
    failed streams and a `0.865 s` recovery gap. Equal-path qdiscs recorded no
    unintended drops or retained backlog.
- Portability audit: the decision consumes only carrier-neutral snapshot
  fields. Optional platform telemetry supplies native credit where available;
  the corrected Product-service path remains the neutral fallback. An
  independent boundary review found no remaining native/portable authority
  inconsistency.
- Evidence: focused admission/request/response tests pass. Raw affected runs
  are under `./.tmp/lab/results/v016-tcp-carrier-qos-native-boundary/`,
  `./.tmp/lab/results/v016-tcp-carrier-qos-native-boundary-download/`,
  `./.tmp/lab/results/v016-native-boundary-unshaped-five-upload/`, and
  `./.tmp/lab/results/v016-native-boundary-representative/`.
- Next: run the bounded source, package, and release-quality workflow; freeze
  the candidate; publish and independently verify immutable v0.1.6 assets.

## 2026-08-03T03:47:20+08:00: v0.1.6 local release-quality gate accepted

- Name: final source, Product, maintained dependency, and Linux package audit
- Category: release verification
- State: all local gates pass; the candidate is ready to commit and hand to
  the authoritative native GitHub target matrix
- Source gates:
  - `cargo fmt --all -- --check` and warnings-denied all-target/all-feature
    Clippy pass;
  - the complete all-feature suite passes `1529` library tests, `2`
    allocation regressions, and `6` daily-use Product acceptance tests;
  - the maintained `quinn-proto` 0.11.16 suite passes `282` tests and `3`
    doctests; and
  - the deterministic benchmark crate passes `5` model/replay tests.
- Product and release contracts:
  - the performance registry validates `29` cells and `66` metrics;
  - all `207` lab tests and all `9` release-archive tests pass;
  - all `7` shell programs parse, dashboard JavaScript parses, and the version
    gate self-test passes;
  - shipped configuration documents parse against the strict current schema;
    project-owned test files follow `tests_[file].rs`; and
  - a separate static audit found no stale public version, prose-description
    table, internal development vocabulary, root artifact, schema drift,
    authorship mismatch, or release-inventory mismatch.
- Linux package evidence:
  - the documented local musl path built
    `mptunnel-0.1.6-linux-amd64.tar.gz` with a static PIE amd64 binary reporting
    `mptunnel 0.1.6`;
  - archive verification passed with exactly the binary, `README.md`, client
    and server TOML, and systemd service under one versioned directory; and
  - no project license copy, third-party notice, checksum sidecar, Rust
    metadata, or extra release file is present.
- Immutability preflight: neither remote tag nor GitHub release `v0.1.6`
  exists; the release version gate accepts `v0.1.6` above frozen `v0.1.5`.
- Next: commit the exact candidate, remove the local release reminder without
  committing it, tag once, push the commit and tag, and verify GitHub's native
  seven-platform build plus `version.json` before cleaning generated state.

## 2026-08-03T15:55:23+08:00: lossy TCP upload and attachment-liveness diagnosis

- Name: forced two-carrier high-RTT upload causal audit
- Category: Core performance and RFC conformance
- State: root causes proven; no production model or parameter change accepted
- Native-loss result:
  - forced `2-2`, zero loss completed exact accounting at `331.371 Mbps`; the
    two native TCP ACK totals differed by only `0.75%` and neither carrier
    reported a retransmission-counter advance;
  - changing only random loss to `1%` completed exact accounting at
    `200.842 Mbps`; the ACK totals differed by `7.02%` and the two carriers
    reported `26/40` intervals in which their native retransmission counters
    advanced; and
  - material native rate/window divergence began near `2.8 s`, about `6.7 s`
    before mature scheduler diagnostics. Both carriers later recovered,
    remained selectable for both streams, drained their queues, and finished
    the logical flows within `2.11%`. This rejects permanent Product
    starvation as the initiating cause and establishes independent native TCP
    loss history as the primary lossy-run reduction.
- Product-lifecycle discrepancy:
  - on a live carrier, an additional `OPEN_STREAM` is written and flushed
    through the carrier's existing TCP writer under a one-PTO live deadline;
    expiration is then passed to `mark_tcp_path_failure`, classifying a
    stream-local attachment delay as path health failure;
  - a focused zero-loss upload reproduced three such timeouts at
    `5.346/5.650/6.468 s` while the carriers remained non-app-limited with
    substantial native queued work; cross-attachments completed only at
    `6.191/7.654 s`;
  - the paired download started cross-attachment at `1.646/1.739 s`, timed out
    once per path, and completed at `3.517/3.790 s`. Upload is therefore
    exposed more severely because attachment control shares the saturated
    client-to-server writer, while the download's client-to-server direction
    mostly carries control and acknowledgments; and
  - management snapshots independently caught a live, queued carrier reported
    as `failed` for roughly one second in both zero- and one-percent-loss runs.
    This conflicts with the RFC separation of liveness, congestion, and
    attachment state; a stream open or cancellation cannot classify carrier
    capacity or substitute for exact native failure.
- Correction boundary: retain established carrier liveness and directional
  authority until exact native failure, ordered drain, or session close. A
  configuration change fences new admission and begins ordered drain; it does
  not directly revoke peer-direction authority. Treat an attachment deadline
  as stream-local settlement and retry evidence, never global carrier failure.
  Placement remains work-conserving
  against current native enqueue credit and recent qualified Product service;
  no fixed utilization percentage, Mbps threshold, tolerance timer, synthetic
  congestion window, or unconditional extra carrier is justified.
- Evidence: raw paired throughput traces are under
  `./.tmp/lab/results/post-v016-forced-two-upload-zero-loss-diag/` and
  `./.tmp/lab/results/post-v016-forced-two-upload-one-loss-diag/`; focused
  attachment traces are under
  `./.tmp/lab/results/post-v016-forced-two-upload-lifecycle-diag/` and
  `./.tmp/lab/results/post-v016-forced-two-download-lifecycle-diag/`.
- Next: align the smallest neutral implementation with the RFC's existing
  lifecycle separation, then prove it with adjacent upload/download and
  lossy/healthy cells. Do not change congestion control, Product flight caps,
  carrier bounds, or timing constants as part of that correction.

## 2026-08-03T16:42:55+08:00: attachment scope and carrier-failure ownership accepted

- Name: exact carrier lifecycle boundary
- Category: Core correctness and performance preservation
- State: accepted; the practical misclassification is removed without a
  congestion, timing, capacity, validation, or carrier-range change
- RFC and ownership:
  - an expired, cancelled, refused, or locally rejected pending attachment now
    settles only that attachment and retains normal ordered `STREAM_DETACH`
    settlement when its open may have entered the carrier writer;
  - Product open/reselection code no longer publishes index-wide TCP or QUIC
    carrier failure, clears exact-instance evidence, or marks every remaining
    attachment when a logical stream ends with a retryable error;
  - an established carrier's exact instance actor remains the only publisher
    of data-plane failure; disconnected TCP actors publish pre-readiness dial,
    authentication, or join failure through the existing endpoint-generation
    fence; and
  - refusal, expiry, and gradual drain remain distinct from immediate exact
  native failure. No sibling attachment, endpoint-group carrier reservation,
  proof, flight, queue, or delivery observation is mutated by a Product-local
  timeout; the exact operation's scheduler-load lease is still settled.
- Durable evidence:
  - the new lifecycle regression gives a live exact TCP instance nonzero
    flight, queue, qualified Product delivery, and path-proof evidence, then
    injects an additional-attachment timeout and proves the complete health
    and accounting tuple plus attachment membership is unchanged;
  - all `153` relay tests pass, the exact native TLS carrier-loss migration
    integration passes, and the complete default-feature suite passes `1522`
    library tests, `2` allocation regressions, and `6` daily-use acceptance
    tests;
  - warnings-denied all-target/all-feature Clippy, formatting, shell parsing,
    and whitespace gates pass.
- Adjacent performance evidence (`500 Mbit/s`, about `333-360 ms` RTT, forced
  two TCP carriers, two bulk streams, 20-second load):
  - zero loss: `350.578 Mbps` download and `332.699 Mbps` upload; both exact
    carrier instances stayed `active` in every management snapshot;
  - one-percent random loss: `212.668 Mbps` upload with exact two-stream sink
    accounting, versus the pre-correction `200.842 Mbps` control; both carrier
    instances stayed `active` throughout;
  - the healthy one-percent download replicate delivered `168.233 Mbps` with
    both exact carrier instances continuously `active`. The first sample's
    `13.802 Mbps` was rejected as a throughput sample because the initial pair
    never became authenticated carrier instances: one pre-readiness record was
    already `suspect`, both opens reached the unchanged `9.216 s` RFC-derived
    establishment deadline, and the actor replaced them with healthy instances.
    This is recorded as cold-establishment variance, not hidden as steady-state
    carrier performance and not used to justify a timer tweak.
- Capacity interpretation: the raw shaped ceiling is `500 Mbit/s`; independent
  one-percent packet loss leaves an ideal retransmission-only ceiling below
  roughly `495 Mbit/s` before TCP/IP headers, ACK traffic, recovery stalls, and
  congestion response. RTT alone does not impose a lower ceiling when the
  native windows cover the BDP, and there is no honest protocol-independent
  exact TCP goodput target under random loss. The relevant acceptance control
  is therefore the identical historical cell: the correction preserves the
  healthy result and improves, rather than reduces, the measured lossy upload.
- Scope intentionally unchanged: native congestion control, all RFC timing
  equations, Product flight accounting, validation geometry, `1-3` default
  carrier range, gradual no-gain withdrawal, and immediate exact carrier
  failure remain as established.
- Deferred, not silently altered: RFC section 15's wording around one
  candidate per admission generation versus later unattempted endpoint groups
  remains a separate elastic-expansion question. It was not involved in the
  proven attachment failure and receives no speculative change here.
- Reproducible evidence is under
  `./.tmp/lab/results/post-attachment-scope-forced-two-fat-loss0/`,
  `./.tmp/lab/results/post-attachment-scope-forced-two-fat-loss1/`, and
  `./.tmp/lab/results/post-attachment-scope-forced-two-fat-loss1-download-replicate/`.

## 2026-08-03T18:55:11+08:00: retained-tail disruption recovery accepted

- Name: established-stream continuity after an attachment becomes silently
  unusable
- Category: RFC conformance and release performance acceptance
- State: accepted; the clean candidate is frozen while the complete fresh
  publication matrix runs
- Proven gaps and corrections:
  - a complete Data ACK snapshot remains authoritative for omissions below
    its horizon, while retained bytes beyond that horizon now remain eligible
    for the RFC Section 15.2 live-tail probe in both stream directions;
  - response FIN and initial receive-credit publication remain retained Product
    work when a carrier output is temporarily full, rather than escaping as a
    terminal `SenderServiceBlocked` stream error;
  - after one bounded recovery cycle on the current live attachments, continued
    absence of Product progress may add one unattached authenticated configured
    carrier, within the existing attachment and carrier bounds; and
  - a successful recovery attachment is retained even when the logical stream
    already has multiple attachments. Progress stops expansion. No source
    address, interface, or inferred physical-link identity participates.
- Scope unchanged: no congestion controller, native TCP/QUIC behavior, timing
  equation, Mbps or percentage threshold, carrier range, wire field, or
  platform branch changed.
- Exact continuity evidence:
  - clean source `a3e06a0` and optimized binary SHA-256
    `248aca9d788c5402c92f1c1b06384f28642e40d6f901a5dcc414f1dc1ce67278`
    ran the fixed-seed mixed TCP/QUIC handover;
  - the established TCP echo completed `53/53` requests without disconnect,
    bulk delivery remained live at `221.587 Mbps`, and the maximum bulk read
    gap was `1.117 s`;
  - one of `90` newly opened HTTP requests began inside a deliberate four-second
    carrier blackhole and exceeded its independent `2.5 s` application budget
    by `3 ms`; this is an expected operation-local deadline, not established
    stream loss, and does not justify a transport timer or lifecycle patch;
  - datagrams delivered `218/221`, consistent with their unreliable semantics
    during injected blackholes; and
  - the prior terminal `SenderServiceBlocked` warning did not recur.
- Source evidence: all `1523` default-feature library tests pass, including
  durable retained-tail, response symmetry, credit backpressure, and
  multi-attachment recovery regressions. The clean handover bundle is under
  `./.tmp/publication-candidate-20260803/.tmp/lab/results/publication-final-fixed-handover-v3/`.
- Next: finish one non-duplicated fresh baseline, QoS, scale, asymmetry,
  browser-load, continuity, and local-ceiling matrix from this exact binary;
  replace every public measured output from those bundles, then run the final
  documentation and source gates.

## 2026-08-04T03:07:52+08:00: MPP v6 bounded maximum carrier pool accepted

- Name: ordinary-carrier maximum pool and clean wire cutover
- Category: RFC conformance, lifecycle simplification, and performance
- State: accepted implementation; final clean publication matrix pending
- Superseding decision:
  - the retained-tail recovery correction remains, but the prior elastic
    candidate, validation, retain, and no-gain machinery is removed;
  - a configured TCP range now has one effective sizing value: its maximum.
    The first value remains accepted only as an explicitly obsolete grammar
    position and changes no runtime behavior;
  - every member from ordinal zero through `maximum - 1` is reconciled as an
    ordinary bidirectional carrier. Readiness does not force Product placement;
    the shared completion scheduler still chooses among currently admitted
    native carriers; and
  - exact native failure is immediate, while port rotation and configuration
    retirement retain the existing ordered drain and replacement lifecycle.
    No source address, interface identity, inferred bottleneck, throughput
    threshold, percentage threshold, or new timer controls pool membership.
- Proven behavior:
  - under an independent `100 Mbit/s` per-TCP-flow policer, one carrier reached
    `75.139/77.518 Mbps`, default `1-3` reached `224.123/215.666 Mbps`, exact
    `3-3` reached `225.158/218.047 Mbps`, and three explicit `1-1` endpoints
    reached `222.027/220.080 Mbps` for download/upload. This proves that the
    default maximum pool is equivalent to three explicit endpoints for the
    intended single-flow-QoS case;
  - under a shared `200 Mbit/s` bottleneck, all four forms stayed in the same
    broad capacity region (`151.656` through `171.505 Mbps` across directions),
    so ready surplus carriers did not create a second network capacity or a
    throughput collapse; and
  - on the host-only multi-gigabit control, exact `1-1` reached `6.713 Gbps`
    while default maximum three reached `5.407 Gbps`. Packet density increased
    about `48-53%` per GiB and client CPU per Gbit/s increased `25.5%` with
    three independent TLS/TCP flows. Two bounded batching variants left the
    result effectively unchanged (`5.370/6.534` and `5.387/6.537 Gbps` for
    maximum-three/maximum-one), so both variants were rejected and fully
    removed. The remaining difference is a native multi-flow packet-processing
    trade-off, not evidence for a Product congestion controller or hidden
    elastic policy. Users seeking the highest single-route host ceiling can
    configure `1-1`; the default retains independent-flow QoS resilience.
- Additional corrections:
  - equal-capacity endpoint startup now orders member ordinals across distinct
    endpoints instead of allowing the first configured endpoint's siblings to
    displace every other physical path;
  - response scheduling gives qualified native delivery rate precedence over
    a portable Product fallback, restoring mixed TCP/QUIC local download from
    `3.383` to `4.495 Gbps` without altering congestion control; and
  - the MPP v6 QUIC session-authentication transcript now uses the v6 domain
    label, matching the clean-break wire version and preventing cross-version
    authentication-domain reuse. The RFC and deterministic vector match.
- Verification: formatting, warnings-denied all-target/all-feature Clippy,
  `1450` library tests, `2` allocation regressions, `6` daily-use acceptance
  tests, and `23` lab-runner contract tests pass. The restart/offline acceptance
  gate now checks the actual availability contract rather than assuming one
  carrier, and passes twice with the default maximum pool.
- Next: commit this exact source as the clean performance candidate; run the
  bounded current-version product, QoS, scale, asymmetry, continuity, browser,
  and stress matrix; publish only valid final-candidate measurements.

## 2026-08-04T08:20:48+08:00: native TCP flight and shared staging model accepted

- Name: per-output native congestion authority with connection-wide source
  staging
- Category: RFC conformance and final performance acceptance
- State: accepted and committed; public evidence refresh in progress
- Root cause and minimal correction:
  - a fresh exact TCP completion sample remains a demonstrated capacity lower
    bound for ranking, but an older MPP service estimate can no longer enlarge
    one output's positive native congestion window;
  - exact MPP flight already committed to that output drains gradually and is
    never revoked by a later smaller native window; and
  - unassigned source staging is connection-wide work for several outputs. It
    uses the existing stream/repair/reorder and configured resource envelopes,
    including its configured path-flight ceiling, and is no longer charged to
    one selected output's native window. Every later DSN assignment still
    passes per-output admission and native writer backpressure.
- Diagnosis and proof:
  - using one TCP output window as the shared request-source budget reduced the
    20-link multi-gigabit upload from `545.268` to `353.409 Mbps`;
  - separating those authorities restored the targeted diagnostic to
    `730.261 Mbps`, and clean commit `ee26898` delivered
    `2,277.788/723.070 Mbps` download/upload;
  - the exact-seed flapping control retained every persistent TCP exchange and
    the final clean run delivered `159.741 Mbps`, `40/40` TCP exchanges,
    `34/37` deadline-bounded HTTP requests, and `86/91` datagrams across the
    deliberate blackholes;
  - five shaped TCP paths reached `641.250 Mbps` in the matched 20-second run
    and `805.194 Mbps` in its 30-second confirmation. All five interfaces
    carried payload. The paired durations show that the shorter result is not
    a capacity ceiling; throughput alone does not identify a causal owner; and
  - the default mixed one-path result reached `284.982/305.017 Mbps`, while the
    five-path default reached `677.370/748.829 Mbps`.
- Cross-condition acceptance:
  - independent 500-Mbps-per-flow TCP limits produced
    `355.923/347.045 Mbps` for one carrier, `886.246/823.774 Mbps` for default
    `1-3`, and `794.061/901.910 Mbps` for three explicit one-carrier endpoints;
  - the same forms stayed between `151.999` and `167.363 Mbps` behind one
    shared 200-Mbps bottleneck;
  - asymmetric multipath reached `153.998/156.716 Mbps`, with `90.7/90.3%` of
    download/upload bytes on the directionally faster link;
  - mixed blackhole and latency changes retained `60/60` TCP exchanges and all
    HTTP requests; TCP-only disruption cases delivered `200.169-288.669 Mbps`;
    and
  - browser gates passed `90/90` deadline-bounded requests and `686/686`
    continuously replaced one-MiB requests with zero rejection or failure.
- Rejected alternatives: a service-estimate-first per-path window and a
  native-rate pacing cap each violated authority boundaries or disrupted reliable traffic;
  both were fully removed. No Mbps threshold, utilization percentage, source
  address, platform branch, native congestion controller, RFC timer, or lab
  condition entered production behavior.
- Reproducible clean evidence is under
  `./.tmp/lab/results/v017-final-ee-product/`,
  `./.tmp/lab/results/v017-final-ee-product-confirm/`,
  `./.tmp/lab/results/v017-final-ee-scale/`,
  `./.tmp/lab/results/v017-final-ee-continuity-load/`, and
  `./.tmp/lab/results/v017-final-ee-topology/`.

## 2026-08-04T09:26:16+08:00: preliminary v0.1.7 evidence (superseded)

- Name: exact-candidate product and scale confirmation
- Category: release performance acceptance
- State: superseded by the later exact `a173c55` release-candidate matrix;
  retained only as predecessor comparison evidence
- Exact source: clean commit `ac227f274de08bfad1b8abb2064c15b57492b0a9`,
  with valid host snapshots and receiver accounting.
- Product evidence:
  - one lossy 500-Mbps link delivered `218.718/194.412 Mbps` over MPP/TCP
    and `277.469/282.256 Mbps` with the default TCP+QUIC configuration;
  - five shaped links delivered `736.711/534.609 Mbps` over MPP/TCP and
    `626.407/752.164 Mbps` with the default configuration;
  - local default TCP delivered `5.386/6.386 Gbps`, while local default mixed
    delivered `5.023/4.755 Gbps`; and
  - the full-load browser case completed `808/808` continuously replenished
    one-MiB requests in 60 seconds with 20 live connections, zero rejection,
    and zero failure.
- Scale evidence:
  - the 20-link access and gigabit profiles delivered
    `376.287/321.846 Mbps` and `1,482.293/609.487 Mbps`;
  - multi-gigabit download repeated at `2,014.173` and `2,022.533 Mbps`;
  - multi-gigabit upload produced one low-epoch `542.044 Mbps` sample and a
    clean `752.993 Mbps` confirmation. Its one-second trace shows the lower
    interval coinciding with deliberate six-second condition epochs and later
    recovery, not steady-link oscillation; and
  - the older `2,882.347 Mbps` download used the rejected per-flow service
    authority and is not a valid regression baseline.
- Causality controls:
  - the same-host detached-ancestor control at `4951ec7` delivered
    `4.608 Gbps` local mixed upload, below current v0.1.7's `4.755 Gbps`;
    therefore the earlier `6.173 Gbps` row was environmental variation rather
    than a shared-staging regression;
  - matched single-TCP upload samples were `161.703`, `194.412`, and
    `202.487 Mbps` under 360-ms RTT and 1% random loss. The median is published;
    and
  - the earlier five-TCP `641.250 Mbps` sample was superseded by the matched
    `736.711 Mbps` confirmation; a 30-second control had already reached
    `805.194 Mbps` with payload on all five interfaces.
- Accepted trade: the mixed latency-change throughput is measured during the
  injected disruption. Its maximum receiver gap improved from `1,995` to
  `547 ms`, with `60/60` TCP and `102/102` HTTP completion; it is not a
  healthy-link capacity regression.
- Reproducible final evidence is under
  `./.tmp/lab/results/v017-final-ac-product-valid/`,
  `./.tmp/lab/results/v017-final-ac-scale-valid/`,
  `./.tmp/lab/results/v017-final-ac-confirm-valid/`,
  `./.tmp/lab/results/v017-final-ac-tcp-upload-valid/`, and
  `./.tmp/lab/results/v017-final-ac-browser-valid/`.

## 2026-08-04T13:18:46+08:00: exact v0.1.7 performance matrix accepted

- Name: final exact-candidate publication evidence
- Category: release performance acceptance
- State: accepted; source gates passed; release transaction pending
- Exact runtime source: clean commit `a173c55c5e4a1c7ffdfa9761294e20305e23ed17`
  and optimized binary SHA-256
  `4dc313a85594745ba9d4fa6b7d843f0bb31721f4be10566872b8a5854137df43`.
  Every published MPTUNNEL row has a valid host snapshot and exact receiver
  accounting. Rebuilding after the later path-only test renames reproduced the
  same release binary SHA-256 exactly.
- Matched product results:
  - one lossy 500-Mbps route delivered `257.226/262.397 Mbps` over MPP/TCP,
    `220.280/173.353 Mbps` over MPP/QUIC, and `297.387/287.721 Mbps` with the
    default TCP+QUIC configuration;
  - five shaped routes delivered `841.572/562.796 Mbps` over MPP/TCP,
    `623.590/730.726 Mbps` over MPP/QUIC, and `639.898/804.675 Mbps` by default;
  - the default therefore reached `1.43x` direct TCP in both directions on one
    route and `1.79x/2.10x` Linux MPTCP across five routes; and
  - no baseline upload lower bound was used in a ratio.
- Carrier and topology results:
  - under independent 500-Mbps-per-flow service, `1-1`, default `1-3`, and
    three explicit `1-1` endpoints delivered `345.465/338.671`,
    `901.519/873.097`, and `744.216/890.466 Mbps`;
  - the same forms remained at the one shared 200-Mbps ceiling, delivering
    `158.931/157.099`, `164.476/172.327`, and `167.164/150.939 Mbps`;
  - a diagnostic control delivered `910.926/912.851 Mbps` for default and
    explicit three-carrier forms and showed all three exact carriers carrying
    comparable byte shares. The lower ordinary explicit download is therefore
    native high-BDP TCP growth variation, not suppressed MPP capacity; and
  - asymmetric 200/20 and 20/200-Mbps links delivered
    `144.587/141.300 Mbps`, with `90.1/90.9%` of bytes on the directionally
    faster link. No source-address heuristic participates.
- Scale and load results:
  - twenty varying TCP/QUIC links delivered `350.135/245.383 Mbps` in the
    30-100-Mbps band, `1,346.848/726.616 Mbps` in the 300-1,000-Mbps band, and
    `2,000.420/597.670 Mbps` in the 3-10-Gbps band;
  - one gigabit download ended exactly at measurement teardown and was
    retained as diagnostic evidence only; its clean confirmation delivered
    `1,346.848 Mbps`;
  - browser batches completed `90/90` inside three seconds, while the
    60-second full-load run completed `739/739` one-MiB requests with twenty
    continuously live connections and zero rejection or failure; and
  - clean local capacity was `6.362/6.581 Gbps` for MPP/TCP `1-1`,
    `5.584/6.328 Gbps` for default MPP/TCP, `2.867/2.796 Gbps` for MPP/QUIC,
    and `4.921/5.190 Gbps` for the default mixed configuration.
- Continuity results:
  - QUIC port hopping delivered `2.818/2.799 Gbps`, with `11/24 ms` maximum
    receiver gaps and no recovery gap;
  - mixed blackhole, latency-change, and repeated-change runs retained every
    persistent TCP exchange (`60/60`, `60/60`, and `48/48`) and delivered
    `108/108`, `94/94`, and `90/92` deadline-bounded HTTP requests;
  - the mixed maximum download gaps were `366 ms`, `3,310 ms`, and `1,501 ms`.
    The latency result lies within the historical `0.55-4.10 s` range for the
    deliberately injected 900-ms one-way, 10% loss epoch, while every TCP and
    HTTP exchange completed; and
  - TCP-only blackhole and latency-change cases delivered
    `272.124/274.925` and `253.904/221.276 Mbps`, respectively.
- Below-best classification:
  - lossy high-RTT QUIC repeated across `162.762`, `173.353`, and
    `185.607 Mbps` upload samples. Exact diagnostics showed a live path
    snapshot, nonzero admission, and no window starvation. Static QUIC
    authority is unchanged; local, port-hop, five-path, and default mixed QUIC
    remain healthy, so a transport-specific patch is rejected;
  - scale results span five deliberate six-second condition epochs and are not
    steady-link best-of runs. Static one-link and five-link results improved,
    so lower epoch averages do not identify a shared model failure;
  - the local `1-1` result is within three percent of its historical best; and
  - the full-load request count varies with host service time, while the actual
    acceptance contract remains zero rejection, zero failure, and twenty live
    connections for sixty seconds.
- Decision: no parameter, timer, congestion controller, platform branch, or
  scheduling rule changed after `a173c55`; every remaining lower historical
  number was classified without evidence of a production model regression.
- Public evidence now uses the exact accepted values in `README.md` and
  `docs/PERFORMANCE.md`; release-specific predecessor analysis was removed
  from public documentation.
- Final source verification: formatting, warnings-denied all-target/all-feature
  Clippy, `1,457` Core tests, `2` allocation checks, `6` daily-use acceptance
  tests, `282` standalone Quinn tests, `3` Quinn doctests, `5` deterministic
  benchmark tests, `210` lab contract tests, and `9` packaging contract tests
  passed. The registry declares `29` cells and `66` metrics; every shell and
  release-version contract check passed.
- Reproducible evidence:
  `./.tmp/v017-release-candidate/.tmp/lab/results/v017-final-a173c55-product/`,
  `v017-final-a173c55-scale/`, `v017-final-a173c55-quic/`,
  `v017-final-a173c55-tcp-qos/`,
  `v017-final-a173c55-tcp-qos-explicit-dl-confirm/`,
  `v017-final-a173c55-asymmetry/`, `v017-final-a173c55-continuity/`,
  `v017-final-a173c55-browser/`, and `v017-final-a173c55-local-1-1/` under that
  result root. Diagnostic-only evidence is under
  `v017-diagnostic-a173c55-tcp-qos-download/`.

## 2026-08-04T14:41:01+08:00: shared-carrier tail-recovery starvation corrected

- Name: bounded live-tail recovery under concurrent carrier load
- Category: post-v0.1.7 reliability correction
- State: targeted implementation and causal verification passed; clean
  non-instrumented non-regression matrix pending
- Frozen baseline: release commit
  `40de8b4f5ec43c15c3e65de456b1791bde8af548` remains tagged `v0.1.7` and
  unchanged.
- Reproduced defect: during the low-latency-path blackhole case, client stream
  `0` retained request range `704..768` for the rest of the 30-second run while
  the server remained fully acknowledged at offset `704`. Six to seven
  alternatives were attached and bulk delivery continued at `271.394 Mbps`.
- Root cause: live-tail dispatch treated carrier-wide ordered-writer backlog
  and native TCP flight as if they were direction-local stream state. Unrelated
  saturated bulk traffic could therefore keep every healthy alternate
  permanently ineligible for one already-authorized 64-byte recovery copy.
- Correction:
  - ordinary speculative Data-ACK repair still requires a drained carrier;
  - an RFC-bounded live-tail recovery still prefers a drained distinct output,
    but may use bounded reinjection queue space on a busy healthy output when
    all distinct alternatives carry unrelated work;
  - request apply now honors the same busy-carrier authority already granted
    to persistent, failed-path, and stale-path recovery; and
  - the response direction uses the same live-tail fallback. No timing,
    threshold, congestion controller, normal placement, or platform behavior
    changed.
- Evidence:
  - all `115` sender tests passed, including full request dispatch with
    writer-dequeued unrelated carrier work and the symmetric response selector;
  - the affected mixed low-latency blackhole diagnostic completed `34/34`
    persistent TCP exchanges with no failure or disconnect, matching
    `2,176/2,176` request/response bytes; and
  - each retained 64-byte tail was visibly dispatched on a distinct output and
    cumulatively acknowledged. Deliberately blackholed UDP/short HTTP probes
    keep the case-level status `loss`; they do not represent a retained TCP
    exchange failure.
- Reproducible evidence:
  `./.tmp/lab/results/post-v017-inst-lowlat-blackhole-diag/` and
  `./.tmp/lab/results/post-v017-tail-recovery-lowlat-diag/`.

## 2026-08-04T15:07:29+08:00: tail recovery accepted; open identity corrected

- Name: exact clean tail-recovery confirmation and immutable stream admission
- Category: post-v0.1.7 reliability and RFC conformance
- State: tail recovery accepted; immutable-open implementation and focused
  verification passed; affected performance matrix pending
- Clean tail-recovery evidence at commit `20908c9`:
  - saturated mixed fat service delivered `263.715 Mbps`, completed `60/60`
    persistent interactive exchanges and `87/87` short requests, and retained
    no TCP failure; this is `4.6%` above the frozen `252.070 Mbps` control;
  - the exact low-latency blackhole case delivered `280.092 Mbps` and retained
    all `28/28` persistent exchanges without disconnect; and
  - the remaining short HTTP and UDP loss occurred only inside the deliberate
    blackhole and is not retained reliable-stream loss.
- RFC discrepancy corrected: a later carrier used to serialize the mutable
  sender-local lane as a fresh `OPEN_STREAM` admission hint. The registry then
  validated only the target and allowed that hint to overwrite the peer's
  live response lane. RFC section 8.1 instead makes the first target and demand
  immutable attachment identity and keeps each live objective direction-local.
- Structural correction:
  - the logical open specification now owns the initial demand, separately
    from the live lane used to select and account a carrier;
  - TCP and QUIC serialize that immutable value on every attachment;
  - the server validates both target and initial demand and rejects only a
    mismatching pending attachment; and
  - a newly accepted output inherits the current server response lane. Only
    the server-local demand tracker can mutate that lane.
- No wire encoding, protocol version, threshold, timer, congestion control,
  path ranking, or platform branch changed.
- Verification: warnings-denied all-target/all-feature Clippy passed; all `17`
  registry tests, all `8` QUIC reliable-stream lifecycle tests, and all `38`
  TCP carrier tests passed. The
  new invariant test proves that a matching later attachment preserves a
  locally promoted response lane and that a mismatched demand changes neither
  membership nor lane. The existing response-only `2 MiB` automatic bulk
  attachment integration test also passed, proving that a latency-opened
  stream can still expand after live promotion under strict identity checks.
- Reproducible clean tail evidence:
  `./.tmp/lab/results/post-v017-tail-recovery-clean/`.

## 2026-08-04T16:20:07+08:00: directional reliable-flow corrections accepted

- Name: immutable admission and direction-owned reliable-flow evidence
- Category: post-v0.1.7 RFC conformance and performance acceptance
- State: accepted; exact affected matrix, continuity case, and full Rust suite
  passed
- Frozen baseline: release commit
  `40de8b4f5ec43c15c3e65de456b1791bde8af548` remains tagged `v0.1.7` and
  unchanged. These corrections are subsequent commits; current-branch public
  evidence reports them without rewriting the tagged tree or release assets.
- Immutable admission confirmation at commit `1260f74`:
  - asymmetric download/upload delivered `134.684/144.496 Mbps`; the clean
    download repeat delivered `150.719 Mbps`;
  - default single mixed-path download/upload delivered
    `372.587/389.789 Mbps`; and
  - paired mixed-path download/upload delivered `666.838/801.770 Mbps`.
- Direction-local model correction at commits `8c17872` and `bd2a297`:
  - request and response demand now have independent observed offsets and
    lanes on both peers;
  - the immutable opening demand seeds both directional trackers but cannot be
    overwritten by a later attachment;
  - capacity evidence belongs only to the local sender direction. The receiver
    retains carrier RTT/PTO timing without borrowing the opposite sender's
    rate or BDP; and
  - response stalls use response-path timing while request-only reinjection
    uses request-path timing.
- Scope: no protocol encoding, configuration, threshold, timer constant,
  congestion controller, path ranking rule, native transport behavior, or
  platform branch changed. Native carrier recovery remains authoritative.
- Exact clean affected matrix at commit `bd2a297`, optimized binary SHA-256
  `5bdcadfa08bd1425b51e4c5a6789837ffc793eff905ccc3fbe1edf13084c8628`:
  - asymmetric download/upload: `147.748/149.680 Mbps`;
  - default single mixed-path download/upload: `370.207/398.793 Mbps`; and
  - paired mixed-path download/upload: `662.573/794.876 Mbps`.
- `README.md` and `docs/PERFORMANCE.md` use those exact current-branch values,
  their derived `1.78/1.98×` direct and `1.85/2.08×` MPTCP ratios, and the
  measured `90.1/89.4%` asymmetric fast-link shares.
- Comparison and decision:
  - versus frozen v0.1.7, five rows improved by `2.19-38.60%`; paired upload
    was `1.22%` lower;
  - versus the strongest recent clean row for each case, every lower result was
    within `1.97%`, while asymmetric and single-path upload improved; and
  - the first otherwise-successful matrix was rejected as exact evidence
    because build load produced `0.526` load per affinity CPU against the
    versioned `0.500` host limit. The no-rebuild rerun used the identical
    binary and passed every source and host gate.
- Disruption confirmation:
  - the exact low-latency-path blackhole repeat delivered `266.094 Mbps`,
    about `5.0%` below the prior `280.092 Mbps` sample in this deliberately
    bursty workload;
  - all `37/37` persistent exchanges remained connected, with zero failure,
    and the maximum success gap improved from `2.859 s` to `1.458 s`; and
  - a preceding sample delivered `234.178 Mbps` while retaining `50/50`
    persistent exchanges and a `1.565 s` maximum success gap. The exact steady
    matrix and repeat exclude a shared throughput regression; no additional
    algorithm change is justified.
- Verification: formatting, all-target/all-feature compilation, warnings-denied
  all-target/all-feature Clippy, all `153` focused relay tests, the response-only
  automatic bulk attachment integration test, `1,459` core tests, `2`
  allocation checks, and `6` daily-use acceptance tests passed.
- Reproducible evidence:
  `./.tmp/lab/results/post-v017-immutable-open-six/`,
  `./.tmp/lab/results/post-v017-immutable-open-asym-dl-repeat/`,
  `./.tmp/lab/results/post-v017-direction-evidence-six-clean/`,
  `./.tmp/lab/results/post-v017-direction-evidence-blackhole-lowlat-clean/`,
  and
  `./.tmp/lab/results/post-v017-direction-evidence-blackhole-lowlat-repeat/`.

## 2026-08-05T10:57:00+08:00: Noise TCP and private QUIC Initial preliminary verification

- Name: carrier first-flight privacy and crypto-cost verification
- Category: isolated transport-security prototype
- State: performance hypotheses supported; current prototype rejected for RFC,
  merge, or release adoption
- Isolation: branch `verify/noise-protected-initial` starts at clean commit
  `3a5ab0e`; `main`, tags, releases, and public documentation remain unchanged.
- Frozen scope:
  - TCP TLS was replaced by
    `Noise_NKpsk0_25519_AESGCM_SHA256`, using a pinned X25519 server key, a
    separate 32-byte endpoint secret, authenticated 8-63-byte random handshake
    padding, masked handshake/record lengths, and bounded 65,535-byte records;
  - TCP MPP admission binds to the completed Noise transcript;
  - QUIC keeps standard TLS, H3, recovery, congestion control, packet sizes,
    migration, and 1-RTT keys. Only Initial header/payload keys use an
    endpoint-secret-derived input, and unauthenticated version/invalid-Initial
    response oracles are suppressed; and
  - arbitrary QUIC prefix/suffix padding was rejected because the standard
    1,200-byte Initial, random connection IDs, and encrypted padding already
    provide random bytes. A new outer shape would add a fingerprint.
- TCP performance evidence:
  - low-latency download/upload changed from `71.911/71.849 Mbps` to
    `74.192/73.300 Mbps`; first body changed from `119` to `103 ms`;
  - unconstrained download/upload changed from `6.593/6.087 Gbps` to
    `7.187/7.011 Gbps`;
  - the initially frozen fat-path sample was stronger than both later runs,
    so it was not treated as a causal regression. An unmodified same-host
    control produced `182.009/197.690 Mbps`; the adjacent Noise repeat produced
    `204.755/203.268 Mbps`; and
  - these dirty-tree samples are descriptive preliminary evidence, not the
    seven-pair release acceptance ledger.
- QUIC and mixed performance evidence:
  - fat QUIC download/upload changed from `234.799/220.730 Mbps` to
    `230.812/231.906 Mbps` (`-1.7%/+5.1%`);
  - unconstrained QUIC changed from `2.456/2.599 Gbps` to
    `2.788/2.572 Gbps` (`+13.5%/-1.0%`); and
  - fat mixed TCP+QUIC changed from `352.790/358.629 Mbps` to
    `344.707/361.264 Mbps` (`-2.3%/+0.7%`). No candidate row showed a
    practical transport downgrade beyond ordinary run variation.
- Wire and active-probe evidence:
  - the captured TCP first flight was 93 high-entropy bytes and its first
    response was 67 bytes; neither had a TLS record prefix. Plain HTTP, a
    1,555-byte TLS ClientHello, and random TCP probes received zero server
    bytes;
  - replaying the captured TCP first flight elicited a fresh 94-byte server
    response. Random padding changes lengths but does not supply freshness, so
    this is a practical replay fingerprint oracle;
  - unsupported-version, malformed public-Initial, and random short-header UDP
    probes received zero bytes; and
  - a valid protected QUIC first datagram exposed no plaintext `h3` or product
    marker, but still visibly had the QUIC long-header/fixed bits, version `1`,
    and the standard 1,200-byte Initial size. The change hides the ClientHello;
    it cannot make QUIC cease to be classifiable or blockable as QUIC.
- Rejection reasons:
  - the Noise first flight needs bounded freshness/replay enforcement before a
    server response;
  - the long-lived Noise record layer needs a deterministic synchronized key
    update before multi-gigabit production use;
  - endpoint-secret rotation and the TCP-only configuration model are not yet
    complete; and
  - private QUIC Initial keys are useful against public Initial decryption but
    do not satisfy a stronger non-QUIC-cover claim. The normative `RFC.md` is
    therefore deliberately unchanged.
- Verification: all `1,455` Core tests, all `283` Quinn tests, warnings-denied
  all-target/all-feature Clippy, all-target compilation, formatting, and diff
  whitespace checks passed. The audit corrected an ALPN test that had
  accidentally stopped at the private-Initial boundary instead of exercising
  TLS negotiation.
- Reproducible evidence:
  `./.tmp/lab/results/verify-noise-initial/` and
  `./.tmp/covert-eval/`. The same-host unmodified control was copied to
  `./.tmp/lab/results/verify-noise-initial/current-host-tls-control/` before
  its temporary detached worktree was removed.

## 2026-08-05T12:23:06+08:00: optional shared transport protection accepted

- Name: one-secret TCP Noise and private QUIC Initial integration
- Category: carrier security, protocol conformance, and performance acceptance
- State: accepted for commit; no push, tag, or release is authorized
- Configuration and security boundary:
  - each MPP outbound or inbound has one optional `transport_secret_file`;
    both endpoints read the same exact 32 raw bytes;
  - this endpoint transport secret is a distinct type and configuration field
    from every MPP client credential. Possessing it reaches only transport
    authentication; normal MPP admission still requires an authorized client
    credential;
  - omission retains the prior TLS 1.3 TCP and public RFC 9001 QUIC behavior
    with no negotiation or fallback; and
  - `tls_server_name` now has the documented MPP-only default
    `mptunnel.example`, while an explicit value remains available.
- TCP model:
  - configured TCP uses `Noise_NNpsk0_25519_AESGCM_SHA256`, random padded
    first flights, secret-masked bounded lengths, transcript-bound MPP
    admission, and bounded records;
  - the server authenticates and decrypts the client flight, validates its
    timestamp, and atomically admits its nonce to one generation-local bounded
    replay cache before writing any byte;
  - malformed, stale, replayed, and wrong-secret flights produce zero server
    bytes and never invoke Rustls or transmit the certificate; and
  - each direction rekeys deterministically before every nonzero record nonce
    divisible by `2^20`. Partial protected reads or writes make only that
    direction terminal instead of risking nonce reuse.
- QUIC model:
  - configured QUIC domain-separates a private Initial input from the endpoint
    secret and DCID while retaining native QUIC packet size, connection,
    recovery, congestion control, migration, TLS, H3, and later key spaces;
  - the server authenticates/decrypts the Initial payload before token parsing,
    exposing `Incoming`, Retry/refuse policy, TLS session creation, or any
    response. Public and wrong-secret Initials cannot elicit a certificate
    flight;
  - secretless Initial derivation still passes the original DCID directly into
    the stock RFC 9001 schedule, and stock endpoint response behavior remains
    unchanged; and
  - no outer packet-encryption shim was added. The private Initial keys already
    protect the encryptable QUIC header and complete payload; hiding invariant
    QUIC header fields would require a second protocol with extra framing,
    nonce/replay ownership, MTU cost, and a new classifier surface.
- Matched performance evidence used one optimized binary SHA-256
  `8175ec115cd4b9793e5d55b18ee75aa32f9a76d17932e6687cd010ac382c0bcf`
  and source snapshot
  `d0035213466045b629e67d085053c35747c974ad84f425c96e505c50bb4370c4`
  for both profiles:
  - TCP fat download/upload changed from `198.945/232.407 Mbps` to
    `230.396/220.212 Mbps` (`+15.8/-5.2%`);
  - TCP unconstrained download/upload changed from `6.656/5.870 Gbps` to
    `7.185/6.443 Gbps` (`+7.9/+9.8%`);
  - QUIC fat download/upload changed from `240.306/215.937 Mbps` to
    `238.954/237.677 Mbps` (`-0.6/+10.1%`); and
  - mixed TCP+QUIC fat download/upload changed from `338.662/365.099 Mbps` to
    `347.100/375.497 Mbps` (`+2.5/+2.8%`).
- Every paired transfer completed with no failure. The sole `-5.2%` sample is
  consistent with ordinary run variance and contradicts no structural limit:
  the earlier adjacent fat-path control/protected TCP pair was
  `182.009/197.690` versus `204.755/203.268 Mbps`. No algorithm or threshold
  adjustment is justified. Host validity rejected both final matrices only
  because the intentionally uncommitted source tree was dirty; the two runs
  used the same source snapshot, binary, host rules, and lab shape.
- Verification:
  - formatting, warnings-denied all-target/all-feature Clippy, and whitespace
    checks passed;
  - all `1,470` library tests, `2` allocation checks, and `6` daily-use
    acceptance tests passed;
  - all `283` standalone Quinn tests and `3` Quinn documentation tests passed;
  - performance-registry validation, `210` lab contract tests, `5`
    deterministic benchmark tests, `9` packaging tests, every lab/packaging
    shell syntax check, and the release-version-gate self-test passed; and
  - final independent narrow audits found no blocking Noise, private-Initial,
    configuration, or public-documentation inconsistency.
- The benchmark workspace lockfile was updated only with the new locked Noise
  dependency closure; an initially generated broad lockfile refresh was
  rejected and fully reversed before the minimal locked update.
- Reproducible final evidence:
  `./.tmp/lab/results/transport-protection-final-legacy/` and
  `./.tmp/lab/results/transport-protection-final-protected/`.

## 2026-08-05T13:12:17+08:00: shipped profile, routing demand, and dependency audit complete

- Name: current transport profile documentation and stable dependency refresh
- Category: product configuration, Core boundary, dependencies, and regression evidence
- State: completed locally and intentionally uncommitted; no push, tag, or release
  was performed
- Configuration and documentation:
  - shipped client, server, and reference configurations now use one separate
    raw 32-byte `transport_secret_file`; the schema remains optional so an
    explicitly secretless pair retains TLS 1.3 TCP and public QUIC Initials;
  - the README, operations guide, packaging guide, and performance guide
    describe the current shipped profile directly, without development-history
    or feature-announcement wording;
  - the laboratory now defaults to the shipped shared-secret profile and
    records its exact TCP Noise plus private-Initial QUIC carrier presentation;
    explicit `MPTUNNEL_LAB_SHARED_TRANSPORT_SECRET=0` still selects the standard
    control profile; and
  - the one-path and local `1-1` public figures are the accepted matched
    protected-profile measurements already recorded above.
- Routing/Core boundary:
  - removed the public `traffic_intent` taxonomy. `interactive` and `realtime`
    mapped to the same network-derived starts, while `background` collapsed to
    throughput on the wire and had no coherent RFC scheduling objective;
  - the replacement `initial_demand` has only `automatic` and `throughput`.
    Automatic starts reliable streams latency-oriented and datagrams
    realtime-oriented; live reliable demand remains adaptive. Throughput is an
    explicit first-byte admission hint for a route known to be bulk, never a
    fixed path choice or permanent class; and
  - removed the unreachable endpoint-local Background lane rather than retain
    duplicate Core branches. The RFC latency, throughput, and realtime demand
    values and adaptive data flow are unchanged.
- Dependencies:
  - refreshed every direct application and laboratory dependency to the newest
    stable Rust 1.96-compatible release and updated all three lockfiles;
  - retained `quinn-proto` 0.11.16, the newest upstream release, with the local
    BBR and private-Initial extensions documented in its mirror README; and
  - dry-run resolution reports no remaining compatible direct update. Older
    transitive `generic-array` and optional vendored `qlog` versions remain
    upstream-constrained and were not forced into fork-only drift.
- Post-refresh performance evidence, shared-secret profile, identical optimized
  binary:
  - initial matrix: TCP `209.660/201.245 Mbps`, QUIC
    `247.512/226.521 Mbps`, default TCP+QUIC `350.038/373.540 Mbps`, and
    local TCP `1-1` `7.620/6.201 Gbps` download/upload;
  - the two shaped TCP directions were the only samples outside ordinary
    adjacency to the accepted protected figures. A focused repeat produced
    `233.586/224.167 Mbps`, slightly above the documented
    `230.396/220.212 Mbps`, while a clean local download completed at
    `6.873 Gbps`; and
  - no dependency or encryption ceiling is supported by the evidence. The
    shaped discrepancy was ordinary loss realization, so no algorithm,
    threshold, or documentation cherry-pick was introduced. One initial local
    duration run ended with a replacement request at the measurement boundary;
    its clean repeat passed.
- Verification:
  - formatting, diff whitespace, stale-vocabulary checks, and warnings-denied
    all-target/all-feature Clippy passed;
  - all `1,470` library tests, `2` allocation checks, and `6` daily-use
    acceptance tests passed after the dependency refresh;
  - the recently completed standalone Quinn (`283` plus `3` documentation),
    lab-contract (`210`), deterministic benchmark (`5`), and packaging (`9`)
    suites remain the applicable unchanged evidence; and
  - final narrow dependency, RFC/Quinn, and public-documentation audits found
    no release blocker.
- Reproducible post-refresh evidence:
  `./.tmp/lab/results/dependency-upgrade-protected/` and
  `./.tmp/lab/results/dependency-upgrade-repeat/`.

## 2026-08-05T15:28:33+08:00: buyer-facing baseline matrix and carrier timing ownership

- Name: matched Internet-condition comparison and long-loss carrier startup
- Category: runtime lifecycle, performance evidence, and public presentation
- State: runtime correction committed as `9c2265b`; README and detailed evidence
  presentation remain intentionally uncommitted until the final selection
  proof and documentation audit finish
- Proven root cause and correction:
  - the background TCP carrier owner reused `path_probe_timeout` as the
    deadline for the complete TCP, transport-protection, MPP join, and
    readiness transaction;
  - at approximately 360 ms RTT and 10% loss, the two-second probe deadline
    terminated valid cold TCP setup before the existing RFC path-open timing
    model allowed its loss-tolerant transaction to complete;
  - a diagnostic configuration-only run raised that deadline to ten seconds:
    TCP recovered from `0.162 Mbps` with failed requests to `144.871 Mbps`, and
    default TCP+QUIC completed at `227.797 Mbps`; and
  - the correction preserves the configured timeout for health probes while
    complete TCP carrier setup now uses the existing adaptive
    `path_open_timeout` model, exactly as demand-driven stream attachment does.
- Clean, receiver-delivered 500 Mbps comparison, two downloads for 20 seconds,
  zero jitter, pinned Xray/VMess and Hysteria2 baselines:
  - approximately 40 ms RTT, 0% loss: `461.341 / 461.425 / 439.091 Mbps`;
  - approximately 40 ms RTT, 10% loss: `406.613 / 421.454 / 405.129 Mbps`;
  - approximately 360 ms RTT, 0% loss: `355.414 / 251.473 / 346.164 Mbps`; and
  - approximately 360 ms RTT, 10% loss: `25.000 / 71.960 / 225.025 Mbps`.
  Values are ordered Xray/VMess, Hysteria2, and default MPTUNNEL TCP+QUIC.
- The severe-condition MPTUNNEL cell completed with zero failed requests and
  improved from the pre-correction failed `1.085 Mbps` run without changing
  congestion control, scheduling thresholds, RFC parameters, or lab shaping.
- Verification: all `1,463` release-profile library tests passed, including
  all seven timing-model tests and the TCP carrier reconciliation integration
  coverage. Both corrected clean-source cells have valid host snapshots.
- A focused latency-versus-throughput selection run was rejected from evidence
  because 27 unrelated containers started after the host snapshot boundary;
  its host validity correctly failed and none of its values are published.
- README presentation now concentrates on baseline behavior, aggregation,
  directional path choice, and recovery. TCP pool mechanics, scale inventory,
  browser stress, and local processing ceilings remain only in the detailed
  evidence guide.
- The immutable release workflow completed successfully for every packaged
  platform and its publish-verification job; no additional release operation
  was performed.
- Reproducible evidence:
  `./.tmp/lab/results/readme-baseline-highlat-lowloss/`,
  `./.tmp/lab/results/readme-baseline-lowlat-highloss/`,
  `./.tmp/lab/results/readme-fixed-lowlat-lowloss/`, and
  `./.tmp/lab/results/readme-fixed-highlat-highloss/`.

## 2026-08-05T15:36:02+08:00: latency-versus-throughput selection proof accepted

- Name: transport-neutral simultaneous bulk and interactive path selection
- Category: scheduler evidence and public performance presentation
- State: completed; no scheduler, threshold, or RFC change was required
- Controlled topology:
  - low-latency path: `80 Mbps`, `20 ms` one-way delay, `2 ms` jitter;
  - high-throughput path: `500 Mbps`, `180 ms` one-way delay, `20 ms` jitter;
  - zero loss on both paths so this experiment isolates selection rather than
    packet-loss recovery; and
  - 30 seconds of simultaneous bulk HTTP, short HTTP, persistent TCP echo, and
    UDP traffic, with TCP and QUIC roles reversed in the second case.
- Results:
  - TCP low-latency plus QUIC high-throughput delivered `289.061 Mbps` bulk,
    `117.161 ms` interactive median, and passed `60/60` echo, `67/67` HTTP,
    and `176/176` UDP checks; and
  - QUIC low-latency plus TCP high-throughput delivered `288.886 Mbps` bulk,
    `48.296 ms` interactive median, and passed `57/57` echo, `130/130` HTTP,
    and `399/399` UDP checks.
- Interpretation: bulk exceeded the low-latency path ceiling by `3.61×` in
  both orientations while median interactive latency remained below the
  high-throughput path's approximately `360 ms` RTT. Reversing the transport
  roles preserved the result, ruling out a fixed TCP/QUIC family preference.
- An earlier 1% loss run was not reused as this selection proof: shaped loss
  caused one short-request timeout in one orientation and two expected
  unreliable-datagram losses in the other. The zero-loss rerun isolates the
  intended variable instead of weakening acceptance or tuning the product.
- Both accepted rows used a clean source tree at `671e9e8`, the same optimized
  binary, valid host snapshots, and no unrelated containers.
- Reproducible evidence:
  `./.tmp/lab/results/readme-path-selection-zero-loss/`.

## 2026-08-05T15:42:13+08:00: current blackhole recovery proof retained

- Name: active-path blackhole recovery on the default TCP+QUIC topology
- Category: disruption recovery evidence and README verification
- State: completed on clean source `ea842dd`; no product change was required
- A two-second active-path blackhole was injected during 30 seconds of mixed
  bulk HTTP, short HTTP, persistent TCP echo, and UDP traffic.
- Reliable outcomes: `243.210 Mbps` bulk goodput, `60/60` persistent TCP
  exchanges, `81/81` HTTP requests, and a `576 ms` maximum bulk delivery gap.
  Existing reliable flows remained attached and completed.
- Unreliable outcome: `199/200` UDP datagrams arrived. The one datagram lost
  while its path was blackholed is reported as such; the mixed row is not
  relabeled as globally loss-free.
- The host snapshot is valid, the source tree was clean, no unrelated
  containers were running, and the shared-transport-key profile was active.
- The prior README values were replaced rather than reused because their raw
  artifact had already been cleaned.
- Reproducible evidence:
  `./.tmp/lab/results/readme-blackhole-current/`.

## 2026-08-05T18:06:20+08:00: buyer performance evidence finalized

- Name: ordinary/adverse comparison, same-flow aggregation, and path-use proof
- Category: public performance evidence and benchmark controls
- State: completed; no protocol, scheduler, congestion-control, or timing
  change was required
- Matched one-link download conditions, pinned Xray/VMess and
  Hysteria2/Brutal baselines, default MPTUNNEL TCP+QUIC:
  - `500 Mbps`, approximately `40 ms` RTT, `10 ms` per-direction jitter,
    `0.5%` loss: `441.353 / 463.502 / 414.200 Mbps`; and
  - `500 Mbps`, approximately `280 ms` RTT, `20 ms` per-direction jitter,
    `10%` loss: `70.833 / 96.288 / 194.504 Mbps`.
  Values are ordered Xray, Hysteria2, and MPTUNNEL.
- Same-flow default MPTUNNEL scaling on repeated ordinary links:
  - one link: `414.200 / 436.133 Mbps` download/upload;
  - two links: `771.888 / 602.173 Mbps`; and
  - five links: `1,365.876 / 1,496.079 Mbps`.
  This is `1.86×/1.38×` at two links and `3.30×/3.43×` at five links.
- Linux MPTCP with five links delivered `884.667 Mbps` download. Its exact,
  receiver-complete upload run collapsed to `2.572 Mbps` under independently
  jittered/lossy paths despite observed additional subflows. It is retained in
  detailed evidence, excluded from ratios, and omitted from the buyer table
  pending independent replication.
- Asymmetric capability proof:
  - Link A was `200/20 Mbps` download/upload and Link B was `20/200 Mbps`;
  - fixed-Link-A Xray/Hysteria2 delivered `181.696/≥17.708 Mbps` and
    `188.812/≥18.808 Mbps` download/upload respectively; and
  - one MPTUNNEL MPP/TCP configuration using A+B delivered
    `153.009/151.513 Mbps`, with `91.2%` of download bytes on A and `89.6%`
    of upload bytes on B.
  The baseline upload endpoint was corrected from a direction-changing oracle
  to one fixed link in commit `a0cda09`.
- Default TCP+QUIC simultaneous workload controls:
  - ordinary `80 Mbps` link alone: `60.886 Mbps` bulk, `103/217 ms` TCP
    p50/p95, all `60/60` TCP, `45/45` HTTP, and `102/102` UDP checks;
  - adverse `500 Mbps` link alone: `98.512 Mbps` bulk, `452/1,868 ms` TCP
    p50/p95, `35/35` TCP, `9/11` HTTP, and `14/18` UDP checks; and
  - both links: `160.002 Mbps` bulk, `173/318 ms` TCP p50/p95, all `60/60`
    TCP, `53/53` HTTP, and `205/205` UDP checks.
- Evidence integrity:
  - every row used in the public documents has a valid host snapshot, clean
    source tree, no unrelated containers, and receiver-side accounting;
  - the first direction-changing asymmetric baseline upload rows were excluded
    after the comparison was corrected to one fixed baseline link;
  - one path-choice attempt was rejected immediately because its snapshot saw
    transient containers from the preceding isolated run; none of its values
    were used; and
  - baseline uploads that did not close within the completion window are
    labeled as receiver-delivered lower bounds and excluded from ratios.
- Public presentation now answers one-link competitiveness, one-to-two-to-five
  link scaling, fixed-link versus multipath directional use, simultaneous
  bulk/interactive behavior, and disruption recovery before linking to exact
  detailed evidence.
- Reproducible evidence:
  `./.tmp/lab/results/readme-final-core/`,
  `./.tmp/lab/results/readme-final-path-choice-valid/`, and
  `./.tmp/lab/results/readme-final-asymmetric-fixed-baseline/`.

## 2026-08-05T19:19:59+08:00: performance evidence lifecycle audit and static rerun

- Name: fair asymmetric startup, current static controls, and stable-before-change evidence
- Category: performance methodology and publication integrity
- State: asymmetric and static cohorts completed; the corrected dynamic cohort remains pending
- Proven methodology correction:
  - commit `a5d0cac` applies asymmetric shaping before baseline processes and MPTUNNEL carriers start;
  - the clean rerun delivered `181.902/≥17.638 Mbps` for fixed-link Xray,
    `188.888/≥18.762 Mbps` for fixed-link Hysteria2, and
    `198.504/196.630 Mbps` for MPTUNNEL using both asymmetric links;
  - matching single-fast-link MPTUNNEL controls delivered
    `183.436/180.222 Mbps`, while the two-link run placed `90.7%` of bytes on
    the directionally faster link in each direction; and
  - the prior MPTUNNEL `153.009/151.513 Mbps` rows are rejected because their
    carriers predated asymmetric shaping.
- Current clean static evidence:
  - independent 500-Mbps per-flow limits delivered
    `346.354/338.889 Mbps` with one carrier, `904.757/931.537 Mbps` with the
    default `1-3`, and `902.027/901.967 Mbps` with three explicit `1-1`
    endpoints;
  - behind one shared 200-Mbps bottleneck the same forms remained within
    `153.374-171.062 Mbps`, so extra carriers did not create capacity where
    the path had none;
  - the 60-second admission window completed `734/734` one-MiB requests with
    twenty live requests, zero rejection, and zero failure; and
  - the periodic browser case completed all `90/90` requests, but one cold
    first batch took `5.031 s`; the remaining eight batches met the `3 s`
    deadline. This row is rejected as deadline-pass evidence pending the
    focused path-selection rerun.
- Remaining dynamic evidence boundary:
  - the randomized condition schedule previously changed a link immediately
    at workload start, so it could not prove recovery after a stable interval;
  - the persistent lab now defaults to a configurable ten-second initial
    stable interval, records it in schedule identity and metadata, and rejects
    traces whose first condition change violates that interval;
  - port-hopping throughput will be accepted only with retained
    `carrier_port_migrated` events; and
  - scale and disruption rows will distinguish maximum inter-delivery gaps
    from recovery time and will not infer active-path use without evidence.
- Verification: Bash syntax, all `36` focused flapping/runner contract tests,
  and diff whitespace checks pass.
- Reproducible evidence:
  `./.tmp/lab/results/readme-asymmetric-fair-clean/` and
  `./.tmp/lab/results/public-current-static/`.

## 2026-08-05T21:09:28+08:00: finite request drain accepted

- Name: measured completion of an assigned final request tail
- Category: RFC lifecycle correctness and upload performance
- State: accepted at commit `5633a34`; public evidence reconciled; no release,
  tag, or push performed
- Proven root cause:
  - two clean multi-gigabit upload runs left one of two request streams
    incomplete for approximately 165 seconds even though alternate carriers
    remained healthy;
  - the remaining OriginalData range continued making slow Product progress,
    so the inactivity-based live-tail trigger was continually refreshed and
    never raced finite completion on a recently faster output; and
  - diagnostic accounting found a `34,144,736`-byte accepted-versus-confirmed
    request deficit and repeated unchanged cumulative Data ACKs. This was a
    finite request-drain lifecycle gap, not evidence against native congestion
    control or ordinary path placement.
- Minimal correction and preserved provenance:
  - application EOF becomes a completion decision only after queued unique
    payload drains, when `next_offset` is the assigned final offset;
  - the existing exact flight age, one MPP recovery interval, bounded repair
    quantum, repeat suppression, shared credit, queue, flight, reorder, and
    extra-traffic envelopes remain unchanged;
  - a copy is admitted only when the exact live owner and exact alternate both
    have qualified current completion evidence, the alternate has the lower
    estimated completion time outside the existing adaptive jitter and queue
    hysteresis, and that exact target still has capacity at dispatch;
  - the decision does not mark the owner stale, withdraw a carrier, alter
    ordinary placement, change ACK activity, or replace native recovery; and
  - this preserves the retained-feedback purpose of `5e1ace6`, retained-tail
    horizon of `9ab3cbc`, busy-carrier liveness of `20908c9`, exact-instance
    fencing of `622960c`, and scheduler hysteresis of `9c5a125`.
- Rejected expansion:
  - no new expiry clock was added. Dispatch re-runs the exact measured target
    selection, including current enqueue capacity; an invalid target follows
    the existing migratable-tail discard path. A second recovery clock would
    duplicate established timing without evidence.
- Performance proof from the identical optimized binary at `5633a34`:
  - the exact twice-failing 20-link multi-gigabit upload completed `2/2`
    streams with zero failures, exact `3,401,383,936`-byte receiver accounting,
    `559.959 Mbps` goodput, a `48.153 s` probe time, and a `1.453 s` maximum
    per-stream receive gap; and
  - the affected five-equal-fat-TCP upload control completed `2/2` streams
    with zero failures at `685.070 Mbps`, exact `2,691,694,592`-byte receiver
    accounting, and no recovery gap. It remains above the accepted
    `562.796 Mbps` public control, so no upload downgrade is observed.
- Previously pending dynamic evidence is accepted without another run:
  - clean commit `c321dd7` held every path stable for ten seconds before the
    first deterministic condition change and retained a complete schedule
    trace with a valid host snapshot;
  - corrected repeated changes delivered `248.291 Mbps`, retained `47/47`
    TCP checks, completed `81/83` deadline-bounded HTTP checks and `217/219`
    datagrams, and limited the receiver gap to `869 ms`; and
  - the latency/loss change delivered `235.408 Mbps`, retained `60/60` TCP
    checks, completed `93/94` HTTP checks and `241/243` datagrams, and limited
    the receiver gap to `1.489 s`.
- Verification:
  - the durable exact-owner/measured-alternate scheduler regression passes;
  - all `54` focused request-sender and `153` focused relay tests passed before
    the final queue-boundary guard, after which all targets/all features
    compiled and the focused regression passed again;
  - the optimized release build completed; formatting and diff whitespace
    checks pass; and
  - `README.md` and `docs/PERFORMANCE.md` now use the accepted fair-start
    asymmetric, stable-before-change scale/disruption, current static, and
    finite-tail candidate evidence instead of rejected predecessor values.
- Reproducible evidence:
  `./.tmp/lab/results/public-current-scale-multi-tail-fix/` and
  `./.tmp/lab/results/public-current-tail-fix-fat-upload/`, with the accepted
  stable-before-change cohort under
  `./.tmp/lab/results/public-current-dynamic/`.

## 2026-08-05T21:50:49+08:00: dynamic upload direction weakness isolated

- Name: matched request/response throughput and Data-ACK authority diagnosis
- Category: Core model audit; no implementation change
- State: a reproducible directional model weakness is confirmed; a timing-only
  change is explicitly not authorized by this diagnosis
- Test-shape correction:
  - each epoch contains the same `12,000 Mbps` aggregate rate inventory and the
    same latency, jitter, and loss inventory in both directions;
  - rate and quality are independently permuted with `direction` in the
    deterministic hash, so this is aggregate-balanced but not path-for-path
    symmetric; and
  - the identical deterministic schedule is reused by the paired download and
    upload runs, so their directional data flows remain directly comparable.
- Matched optimized-binary evidence at `e17120d`:
  - download delivered `1,216.058 Mbps`; upload delivered `534.255 Mbps` with
    exact receiver accounting, while every `39-40` carrier remained live;
  - the active sender's own average modeled path delivery was approximately
    `2,280 Mbps` for download and `1,508 Mbps` for upload. Directional path
    correlation therefore explains part of the result, but realized service
    was still approximately `53%` versus `35%` of those respective models;
  - upload repeatedly fell near zero while modeled delivery remained above
    one gigabit and queue/flight state remained below its resource envelope;
    and
  - finite upload drain added only `1.478 s` in this pair. Independent direct
    upload exceeded `20 Gbps`, while stationary ordinary, equal-fat,
    independent-carrier, and local mixed controls remained directionally
    balanced. Probe cost, sink cost, platform behavior, and generic upload
    processing are therefore rejected as causes.
- Matched Data-ACK traces:
  - diagnostic download delivered `1,206.337 Mbps`. All `18,186` response ACK
    observations were complete authoritative snapshots; `2,833` observations
    authorized bounded persistent-gap service covering `53,745` frames;
  - diagnostic upload delivered `663.181 Mbps`. Of `88,101` request ACK
    observations, `65,510` were non-authoritative sparse deltas; only one ACK
    observation directly authorized eight frames and seven retained-gap timers
    authorized another `57` frames;
  - response feedback released useful flight on every observed ACK, whereas
    only `36,682` request ACK observations released new bytes. Sparse request
    publication therefore generated over eight times as many ACK observations
    per delivered byte in this topology while providing much weaker omission
    authority; and
  - the `64 MiB` stream-flight envelope was reached in `2.30%` of recorded
    download budget transitions and `0.90%` of upload transitions. It is not
    the sustained directional limiter.
- Root-cause boundary:
  - bulk TCP request feedback deliberately publishes positive sparse deltas
    during reorder, while response feedback publishes cumulative snapshots;
  - a sparse delta releases exact positive flight but cannot advance the
    authoritative omission horizon. Request repair then waits one MPP recovery
    interval from a later authoritative observation, whereas response repair
    uses original-flight age and the Data-ACK time threshold;
  - under many heterogeneous changing paths this composition withholds ordered
    request recovery long enough to make source admission bursty. It is a Core
    feedback/recovery model asymmetry, not a Linux or native-transport issue;
    and
  - the response trace's much larger repair activity is evidence of the
    inconsistency, not authority to copy that activity blindly. A correction
    must first define direction-neutral cumulative ACK-transaction authority,
    then compare bounded recovery under identical timing and ownership rules.
- Rejected shortcut and next proof boundary:
  - do not restore `32c6ea4`: that timing-only candidate was previously
    rejected because its intended ACK-gap action did not cause the observed
    gain and it correlated with a severe mixed-disruption regression;
  - preserve exact directional sender ownership, native congestion control,
    resource envelopes, and reinjection budgets; and
  - any future correction must causally reduce incomplete/duplicate request
    feedback and close the dynamic direction gap, then preserve stationary,
    aggregation, and disruption controls before acceptance.
- Reproducible evidence:
  `./.tmp/lab/results/direction-root-cause-current/`,
  `./.tmp/lab/results/direction-root-cause-diagnostic/`, and
  `./.tmp/lab/results/direction-root-cause-diagnostic-download/`. The normal
  optimized executable was restored afterward with SHA-256
  `98bfdaa1e533d63c1e678d9b88930f5fe1cc011fcc2e95378725fa25b9ab701b`.

## 2026-08-06T00:36:57+08:00: cold browser placement corrected

- Name: authenticated cold-latency path selection
- Category: Core path selection and short-connection acceptance
- State: accepted locally; no commit, tag, release, or push performed
- Root cause:
  - evidence-free startup deliberately withheld readiness timing from generic
    path completion and capacity evidence until `PATH_PROOF`;
  - concurrent latency-sensitive flows then used active-flow fan-out and could
    be assigned to an establishing or materially slower TCP carrier; and
  - the rejected run completed `89/90` requests, timed out one request, and
    took `4.764 s` for the first batch. The previously published run exposed
    the same defect at `5.031 s`.
- Accepted correction:
  - only evidence-free latency-sensitive ordering now prefers a carrier whose
    exact instance has completed the authenticated readiness exchange;
  - authenticated candidates use their existing scheduler ETA and exact
    readiness timing for latency ranking; and
  - `PATH_PROOF`, capacity qualification, throughput ordering, admission,
    congestion control, recovery timing, and transport behavior are unchanged.
- Durable tests:
  - the existing cold TCP reservation test now proves two concurrent latency
    reservations remain on the authenticated 20 ms carrier instead of an
    authenticated 80 ms carrier or an unauthenticated 1 ms generic probe;
  - the bounded-pool rotation test now asserts its actual invariant—exactly
    one carrier owns the attachment—without assuming configured index zero
    wins authenticated RTT ranking; and
  - all `1,471` library tests, `2` allocation tests, and `6` daily-use product
    tests pass with all features. Formatting and all-target/all-feature Clippy
    with warnings denied also pass; and
  - `README.md` and `docs/PERFORMANCE.md` publish the accepted `90/90`
    periodic and `732/732` continuous short-connection results.
- Performance evidence from one unchanged optimized binary:
  - periodic browser load completed `90/90` requests with zero failures and
    zero deadline misses; the slowest batch was `1.288 s` and the slowest
    request was `1.285 s` against the `3 s` bound;
  - 60-second full load accepted and completed `732/732` one-MiB requests with
    zero rejection or incomplete requests while holding 20 live requests;
  - mixed equal-fat download and upload completed at `621.980 Mbps` and
    `703.988 Mbps`, versus the latest accepted matched control at
    `622.660/590.934 Mbps`; throughput selection cannot enter the changed
    latency-sensitive branch and no throughput downgrade was observed; and
  - blackhole recovery retained bulk service at `176.875 Mbps`, completed all
    reliable checks and HTTP checks, and limited the bulk recovery gap to
    `404 ms`. Two deliberately unreliable datagrams were lost.
- Evidence boundary:
  - host validity rejected the performance cohort only because the tested
    source patch was necessarily uncommitted; no host-condition rule failed;
  - these runs prove functional acceptance and unchanged code-path behavior,
    not a new publishable throughput baseline; and
  - reproducible artifacts are retained under
    `./.tmp/lab/results/cold-start-current-regression/`.

## 2026-08-06T03:04:00+08:00: operator logging contract matured

- Name: readable process lifecycle and safe configuration inventory
- Category: Product observability
- State: implemented and verified for the `v0.2.2` release
- Accepted contract:
  - a normal runtime's first record identifies the package version; an early
    fatal record also carries that version;
  - default text is one-line UTC RFC 3339 with fixed milliseconds, uppercase
    severity, a stable `component.event`, and a natural message;
  - startup reports the configuration source and revision, bounded topology
    counts, named outbounds and MPP paths, actual bound inbound/management
    listeners, generation readiness, reload activation, shutdown request, and
    clean stop;
  - one process-scoped, five-second background HTTPS check reports the newest
    immutable GitHub release and its canonical release URL without delaying
    runtime readiness or forwarding;
  - JSON remains newline-delimited with the existing typed envelope, and
    destination-bearing flow records remain explicitly opt-in; and
  - readiness is described factually as host-facing runtime readiness, not as
    proof that every remote outbound carrier is online. Live carrier health
    remains available through path diagnostics.
- Safety and performance boundary:
  - new lifecycle records occur only at finite control-plane transitions;
    packet, scheduler, congestion-control, retry, probe, and sampling loops are
    unchanged;
  - text and JSON share one bounded emission path; recurring fault records keep
    their existing per-call-site rate limits; and
  - redaction now accepts whitespace and quoted TOML/JSON/header forms for
    authorization, cookies, tokens, passwords, credential secrets, transport
    secrets, and private keys. Authentication scheme words are no longer
    redacted outside authorization values, and terminal controls cannot create
    extra log lines; and
  - raw TOML deserialization errors are discarded at the configuration parse
    boundary. Display, debug, source chains, startup failures, and live reloads
    retain only safe line/column context and optional unknown-field identity.
- Evidence:
  - the exact release candidate passes `cargo test --locked --all-features`:
    `1,475` library tests, `2` allocation tests, `6` packaged daily-use tests,
    and documentation tests;
  - standalone patched Quinn passes `283` tests and `3` documentation tests;
    the lab registry is valid at `29` cells and `66` metrics, all `213` lab
    contract tests pass, and all `5` deterministic benchmark/replay tests pass;
  - all `9` release-archive contract tests, shell syntax checks, and the stable
    version-gate self-test pass. Both root and benchmark lockfiles resolve the
    `0.2.2` package graph under `--locked`;
  - `cargo clippy --locked --all-targets --all-features -- -D warnings`,
    formatting, and `git diff --check` pass;
  - a live startup run became ready before the update result, reported the
    newest immutable GitHub release, and shut down cleanly. The request
    completed in approximately `295 ms` on that run; and
  - malformed inline, multiline, and ordinary-field TOML canaries are absent
    from display, debug, source-chain, and packaged fatal diagnostics while
    safe line/column and unknown-field context remain available.

## 2026-08-06T16:25:34+08:00: first-class TUN-L3 packet plane established

- Name: authenticated multipath layer-3 tunnel
- Category: Product packet service and transport-neutral data plane
- State: implemented and verified locally; no commit, tag, release, or push
  performed
- Accepted contract:
  - `protocol = "tun-l3"` binds one client packet device directly to one MPP
    outbound, while an MPP inbound may own an IPv4 and/or IPv6 pool, one server
    address per family, explicit principal allocations, and additional
    principal-owned prefixes;
  - authenticated MPP principal identity is the sole ownership authority.
    Client-to-server source addresses and server-to-client destination
    addresses are checked against the immutable plan; outer source locators
    and claimed packet identity are never trusted;
  - the packet plane is parallel to Product routing, DNS, destination ACLs,
    outbound target handling, and TUN-L4. It configures only the assigned host
    address and MTU, using `/32` and `/128`; it does not install peer or pool
    routes, DNS, forwarding, firewall, or NAT state;
  - complete IPv4 and IPv6 packets preserve headers, TTL/Hop Limit, and payload.
    TCP uses ordered carrier frames; QUIC uses one reliable lifecycle stream and
    request-associated native datagrams with a compact bounded fragmentation
    envelope;
  - TCP and QUIC attachments are equally eligible. Each direction has its own
    bounded inner-flow affinity and transport-derived flowlet reselection;
    exact failure retires only that carrier attachment. The packet plane adds
    no acknowledgment, retransmission, congestion controller, pacing loop, or
    global reorder buffer; and
  - one ordered, memory-bounded client handoff preserves accepted packet and
    lifecycle order through kernel-device backpressure. Server dispatch is
    bounded, validates ownership before delivery, and uses the same dedicated
    packet-ranking evidence rather than Product flow accounting.
- Product and platform boundary:
  - existing shipped client/server defaults remain unchanged; enabling the
    packet plane is explicit and additive;
  - Linux, Windows, macOS, and BSD use the neutral packet-device provider.
    macOS/BSD automatic route association is explicitly disabled. Android uses
    the existing host-provided descriptor boundary, and Apple Network Extension
    hosts retain their documented packet-flow adapter boundary; and
  - management reports a separate aggregate TUN-L3 service inventory without
    credentials, principals, pools, assigned addresses, or allowed prefixes.
    Repeated attachment failures now emit rate-limited operator warnings; the
    healthy packet path has no new logging work.
- Reproducible evidence:
  - an isolated unprivileged Linux namespace created distinct client and server
    network namespaces, actual kernel TUN devices, and external exact host
    routes. Before those host routes were added, both IPv4 and IPv6 peer-route
    assertions were empty;
  - TCP-only, QUIC-only, and combined TCP+QUIC profiles each carried three
    complete `1,400`-byte IPv4 packets and three complete `1,400`-byte IPv6
    packets with zero loss. QUIC-only therefore exercised native fragmentation
    and reassembly rather than falling back to TCP;
  - `cargo clippy --locked --all-targets --all-features -- -D warnings` passes;
    `cargo test --locked --all-features` passes `1,505` library tests, `2`
    allocation tests, `6` packaged daily-use tests, and documentation tests;
  - patched Quinn passes `283` tests and `3` documentation tests; deterministic
    benchmark/replay passes `5` tests; management Python contracts pass `31`
    tests; dashboard JavaScript syntax, formatting, and `git diff --check` pass;
    and
  - compile-time checks pass for native Linux, static Linux, and Windows GNU.
    Android and macOS reached native crypto build scripts but this Linux host
    lacks the Android NDK compiler and Apple SDK/toolchain; their unchanged
    platform matrix remains the full-build authority.

## 2026-08-06T17:59:33+08:00: TUN-L3 performance matrix and Product guard completed

- Name: real-packet performance and historical non-regression verification
- Category: Performance evidence
- State: documented locally; no source-model patch, commit, tag, release, or
  push performed
- Verification scope:
  - the unchanged optimized candidate passes `cargo test --locked
    --all-features`: `1,505` library tests, `2` allocation tests, `6` packaged
    daily-use tests, and documentation tests;
  - the original eight-cell TCP/QUIC guard was repeated at `500 Mbps`, `180 ms`
    one-way delay, `20 ms` jitter, and zero configured loss, then compared with
    the accepted historical rows and an exact `v0.2.2` binary control; and
  - the current default TCP+QUIC ordinary/adverse guard was repeated without
    changing path hints, production timing, congestion control, windows, or
    queue parameters. A single ambiguous two-link download cell was closed by
    an A-B-A candidate/`v0.2.2`/candidate sequence; the closing candidate
    restored `775.864 Mbps` against the accepted `771.888 Mbps`.
- Product conclusion:
  - no broad accidental Product downgrade is demonstrated: current five-link
    download/upload delivered `1,555.464/1,387.474 Mbps`, and ordinary
    one-link upload delivered `426.737 Mbps`;
  - pure-TCP and QUIC five-link download, QUIC five-link upload, adverse default
    one-link download, and default two-link upload remain unresolved diagnostic
    gaps. The matched parent was also low in the two-link upload cell, so that
    observation does not establish candidate causality; and
  - every new candidate cohort is non-publishable replacement evidence because
    the tested source snapshot is uncommitted. Later cohorts satisfied host
    load and external-container rules; source dirtiness was their sole validity
    failure. No speculative scheduler, timing, or codec patch was retained.
- Real TUN-L3 matrix:
  - ten fresh network-namespace cells used actual `1,500`-byte kernel TUN
    devices, exact externally installed peer routes, independent TCP and QUIC
    veth links, both ICMP directions and sizes, four parallel inner-TCP flows,
    and four parallel inner-UDP flows;
  - clean, ordinary, adverse, and asymmetric conditions covered `500 Mbps`
    links, `10–280 ms` RTT, `0–20 ms` per-direction jitter, `0–10%`
    per-direction loss, and opposing `20/200 Mbps` asymmetric directions;
  - TCP outer service preserved all ICMP packets but reached
    `542–1,032 ms` mean echo RTT under the adverse profile. QUIC preserved
    native packet semantics; small adverse echoes lost `20–26%` against the
    approximate `19%` round-trip loss expected from independent `10%` loss in
    each direction, while the full small/near-MTU range was `15–26%`;
  - clean inner TCP delivered `441.509/442.685 Mbps` over TCP,
    `389.672/401.550 Mbps` over QUIC, and `605.370/628.656 Mbps` over mixed
    upload/download service. Inner TCP over QUIC became loss-limited under the
    ordinary profile because it, not Product reliable MPP, owns recovery;
  - clean UDP delivered `444.297/442.034 Mbps` over TCP and
    `420.399/413.527 Mbps` over QUIC from `450 Mbps` requested per direction.
    Netem and TUN qdiscs recorded no drops, but the run does not distinguish
    wire overhead from a bounded internal attachment or QUIC queue; and
  - mixed packet affinity currently sees one TCP endpoint's three carrier
    members and one QUIC endpoint as four attachments. That topology can bias
    cold placement and is consistent with the measured `864.380/443.569 Mbps`
    clean UDP upload/download imbalance from `900 Mbps` requested, but per-flow
    carrier assignment was not captured to establish causality. No selection
    correction was accepted without proving that it preserves lossy-path
    behavior.
- Documentation and evidence:
  - `docs/PERFORMANCE.md` now contains the complete ICMP, inner-TCP, and
    inner-UDP matrix plus the historical/current guard classifications;
  - `README.md` performance claims were intentionally not changed by this
    verification pass; and
  - raw artifacts remain below `./.tmp/tun-l3-performance/evidence/` and
    `./.tmp/lab/results/tun-l3-*` for the current uncommitted audit.

## 2026-08-06T18:35:21+08:00: TCP five-link download matched ABBA closed

- Name: exact-binary TCP aggregation regression check
- Category: Performance evidence
- State: candidate downgrade not reproduced; no model or parameter change
- Method:
  - ran `v0.2.2`, candidate, candidate, then `v0.2.2` using the same
    `mptunnel_tcp_multipath_equal_fat` case, without rebuilding either binary;
  - each run used five `500 Mbps` TCP links, `180 ms` one-way delay, `20 ms`
    jitter, zero configured loss, two flows, and a 20-second transfer; and
  - both binaries were retained by SHA-256, and `target/release/mptunnel` was
    restored byte-for-byte to the candidate after the sequence.
- Evidence:
  - `v0.2.2`: `631.461/686.330 Mbps`, mean `658.896 Mbps`;
  - candidate: `755.907/704.200 Mbps`, mean `730.053 Mbps` (`10.8%` higher);
  - every run completed with zero failed requests, zero early termination,
    zero shaping drops, and material traffic on all five paths; and
  - raw records are in `./.tmp/lab/results/tcp-five-abba-*`.
- Conclusion:
  - the bounded matched comparison rejects a TCP five-link candidate
    downgrade; it does not claim a universal candidate improvement; and
  - both cohorts remain below the accepted historical `793.576 Mbps`, so that
    absolute gap is shared run/environment variance rather than evidence of a
    regression introduced by the current candidate.
