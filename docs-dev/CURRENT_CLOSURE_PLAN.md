# v0.4.9 release closure plan

Updated: 2026-09-11 09:34 +08:00. Authoritative repository: ./.
Category: user-directed release convergence. No release acceptance yet.

## Active amendment: challenge claims, prioritize severe user impact

The user now explicitly requests proving/challenging the preceding conclusions,
including TCP on link 1 and QUIC on link 2, and correcting demonstrated severe
defects. Stable browsing/downloading/uploading/gaming is the objective, not an
ideal allocator. A speed deficit within 20% or an isolated gap under 3s is not
a tuning task in this batch. These are user impact triage guidelines, not new
runtime thresholds, permission to hide recurring disruption, or proof that
larger differences are automatically defects. Preserve all timings and failures.

Next transaction (before code or experiments):

- Issue/question: does native shared-cut competition adequately explain the
  observed mixed deficit, and can protocol-isolated links provide usable
  aggregation and recovery without prolonged application stalls?
- Existing evidence: healthy shared500 mixed about398--404Mbps versus single
  modes429/443Mbps is preceding011 evidence; external TCP raises QUIC latency
  without MPP TCP scheduling. Prior independent200+200 uses TCP AND QUIC on each
  cut, not protocol isolation. Current04f has an independently observed ordered
  prefix hole; neither that nor native competition explains every result.
- Competing causes: no extra physical capacity; unequal native achievable
  service; ordered-prefix/feedback coupling; local processing/resources;
  measurement or endpoint limitations. Do not assume which dominates.
- Information forecast: using the SAME current04f executable and ordinary
  probes, TCP46 alone, QUIC47 alone and TCP46+QUIC47 reveal whether extra cut
  capacity translates to useful delivery. This can falsify a blanket shared-
  bottleneck explanation, not prove a universal allocator or promised400Mbps.
- Smallest next action: six healthy200Mbps-per-cut cells (three modes, UP and
  DOWN), then four split-mode QoS/outage cells (TCP46+QUIC47 and reversed
  TCP47+QUIC46, both directions). Retain the shipped three-TCP carrier pool
  in both singleton and split controls; one QUIC per configured QUIC endpoint.
  Healthy has zero deliberate loss/jitter and UP70/DOWN30ms. Stress changes
  only46 in the transfer direction to10Mbps at15--25s (UP uses mirror=1;
  DOWN mirror unset gives DOWN70/UP30ms) and blocks UDP30--33s. This tests placement and recovery,
  not the entire random Internet. No change to existing guards or shapers.
- Only measurement change: an explicit client-config override in the existing
  scratch runner; archive it and exact path sets. No new harness or runtime
  diagnostic overlay. Verify actual interface/connection counts and service.
- Acceptance/stop: retain full receiver-confirmed series, completion, all
  stalls, loaded echo, wire/CPU/RSS. A material failure selects its smallest
  discriminator before runtime edits; no gain tuning or favourable rerun loop.
  Isolated/shared baselines and released-v0.4.8 controls distinguish ordinary
  native limits and new regressions as needed. Existing source/platform and
  practical release gates below remain; fresh matching cells may be reused.
- First closed outcome: current04f healthy DOWN singleton TCP46=179.278Mbps,
  QUIC47=178.857, split=309.377; 80/80 echoes each, worst body gaps .401/.101/
  .274s. Independent checks verify3TCP46/1QUIC47 and both200Mbps cuts used;
  split beats the best singleton72.57%, not a universal aggregation guarantee.
  UP drivers also closed; exact results under review. Two earlier unsuffixed
  QUIC/split DOWN attempts are invalid: root launched a successor before the
  prior driver closed, causing shaping/teardown interference. INVALID.md marks
  both raw directories. Neither is an MPP failure; strict completion guards now
  precede every successor. Their serial replacements are the valid observations.
  Independent agents check topology and prior claim scope;
  root alone runs all builds and traffic. Broad research remains deferred.

## Decision: freeze scope, finish a release

The user's latest instruction stops further broad exploration and prioritizes
the next release. This plan supersedes the former open-ended optimization
sequence, not its evidence or known failures. Follow
[the execution method](PERFORMANCE_METHOD_AND_LESSONS.md), with the release
scope below. No new controller, scheduler, timing-policy, bottleneck-inference,
upstream merge, configuration knob or diagnostic framework enters this batch.

Configured GitHub release lookup confirms v0.4.8 was published on
2026-09-04T22:26:57Z. The next intended version is **v0.4.9**.
Do not republish an existing version or tag before the gates pass.

## Frozen candidate and exclusions

- Candidate runtime is **04f1e56**, also the runtime at evidence checkpoint
  e2375e9 and current reversal checkpoint **0453a40**.
- Retain the existing exact correctness/lifecycle/telemetry corrections and
  finite client ACK/MAX Input correction. The clipped repair-range correction
  preserves an existing exact-owner exclusion contract, with real producer and
  live-prefix evidence. These are candidate contents, not blanket acceptance
  of every performance interaction.
- **8596f30 ordered-credit head urgency is removed** from all three owned
  runtime/test/RFC files. Exact diff against e2375e9 is empty. Its ordinary
  comparison was essentially flat in speed and worse in continuity.
- The proposed follow-up observer was canceled before any source edit, build
  or run. Other previously rejected trials remain removed. Do not restore them.
- Current ordinary candidate executable:
  ./.tmp/reflection/bin/clipped-range-20260911/mptunnel.
  target/release/mptunnel and bin/ordered-credit-head-20260911/mptunnel are the
  REJECTED trial executable, not the frozen candidate. Always use explicit paths.
- Version/package/docs changes may follow the gates. A runtime change requires
  a reproduced release blocker, its exact cause and a bounded correction or
  withdrawal. No speculative cleanup accompanies it.

## Finite gate order

### 1. Source, declared behavior and lifecycle

Run the existing release-quality checks once on the frozen candidate: format,
Clippy with warnings denied, Rust all-feature tests, standalone locked Quinn
tests, existing lab/config/packaging contracts and release-version self-test.
Use the existing restart, exact attachment, ACK/credit, half-close, cancellation,
idle cleanup and ownership/churn checks; reuse already-preserved ordinary
evidence where the relevant source is unchanged. No new stress harness.

Block on failed integrity, deadlock, failure to recover after server restart,
unreclaimed live ownership, broken configured routing/DNS/bypass behavior, or
a real platform/build incompatibility. Fix only the identified release blocker;
do not turn a lint or platform adapter failure into a model redesign.

### 2. Fixed practical regression comparison

Compare the frozen candidate with the **published v0.4.8** control using existing
ordinary binaries/probes and containers. Build a missing control once in
project-local scratch; no environment recreation, host shaping or new harness.

The fixed set is eighteen ordinary cells:

| Profile | Modes/directions | Candidate + released control |
|---|---|---:|
| Healthy single 500 Mbps, asymmetric 70/30 ms | TCP, QUIC, mixed; UP and DOWN | 12 |
| Existing independent 200+200 Mbps profile, temporary 10 Mbps QoS and UDP blackhole | Mixed; UP and DOWN | 4 |
| Existing 20% loss then clear, single 500 Mbps | QUIC DOWN | 2 |

Keep each existing profile and observation/settlement guard unchanged. The
second row uses the declared existing no-random-loss/no-jitter QoS/outage
ablation; the last row covers the currently reported high-loss/recovery issue.
Do not silently replace either with another random profile.

Use complete receiver-confirmed timing series, startup, every write/read/
confirmation gap, settlement, existing loaded-echo latency, and wire/CPU/RSS.
Do not rank local accepted bytes or cached capacity as delivered throughput.
An initial burst followed by persistent collapse is a failure. No claim that
a short confirmation catch-up bin exceeds physical link capacity.

Also perform one existing actual-browser Cloudflare smoke on default mixed
mode for browsing/concurrent behavior. This is not a new baseline matrix or
a claim that all Cloudflare modes are optimized.

Acceptance is usable, materially nonregressing service and recovery against
the released control, not compilation alone or universally highest Mbps.
Retain known latency/efficiency limitations. Small noisy changes do not open
new optimization tasks. If a material result is genuinely ambiguous, allow one
predeclared order-reversed comparison of that affected cell only; preserve
both outcomes. No favorable rerun loop or parameter adjustment to pass.

### 3. Version, docs and platform CI

Once the source and practical gates pass, bump to v0.4.9 and update release notes,
README/PERFORMANCE with the accepted evidence and existing time-series/latency
plots. Public docs contain supported results and limitations, not rejected
experiments, internal source paths or claims of universal optimality.

Run the existing linked/package checks for Linux, Windows, macOS and Android
targets. Push the fixed candidate and monitor CI at five-minute intervals.
A source-affecting correction reruns its affected tests/cell; packaging-only
changes rerun packaging, not the entire performance matrix. Do not merge new
upstreams while waiting.

### 4. Publish and stop

Publish only after the required quality, regression and platform gates pass.
Verify release artifacts/version and summarize the actual improvements and
remaining limits. Preserve useful evidence in docs-dev before scoped scratch/
build cleanup, with explicit preflight and absolute deletion paths. No broad
deletion while evidence or an active build still needs it.

Stop after the release handoff. Further performance research is a separate
batch requiring user direction.

## Stop conditions and deferred work

A failed gate does not authorize another open-ended optimization campaign.
Identify the exact blocking regression and remove the attributable unaccepted
change, or make its smallest proven correction. If attribution needs broad
research, report that concrete release blocker and stop rather than silently
restarting exploration or claiming the release is ready.

Deferred, with no resolution claim:

- Shared mixed-mode loaded-latency/efficiency superiority over every baseline.
- Broad random/asymmetric multi-link ablations, all-mode browser comparisons,
  full MPTCP comparison and universal optimality.
- Further BBR/Brutal/startup/QoS-recovery tuning or alternate repair policies.
- Unattributed deployed RAM/CPU incident and the exact high-loss burst owner.
  Local startup one-core work is reproduced; sustained low-speed collapse was
  not CPU-saturated, and snapshot-dominant work was falsified. Profiling needs
  the already-requested explicit sudo consent. No permission workaround.
  A reproduced leak, runaway CPU or stalled recovery in the fixed gate is
  still a release blocker; the unresolved report is not declared harmless.

## Evidence and continuity

Full prior plan and decisions are preserved in:
`git show 0453a40:docs-dev/CURRENT_CLOSURE_PLAN.md`.
[Complete comparison record](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md)
retains all earlier outcomes; it is not a list of mandatory new release tasks.

Latest rejected trial: 988413952 bytes/44.573108s = 177.401 Mbps versus
176.241; worst write 1.865375 -> 2.495271s and confirmation 1.717023 ->
4.357523s. Target delivery advances only29200B over a two-second cut interval.
Sampled UP bytes/useful improves1.876008 -> 1.853830, but peak UP backlog
38.663 -> 75.378 MB and native flight61.235 -> 95.685 MB. Do not mislabel every
cost worse or attribute the entire gap to the trial. It failed useful
composition despite a real producer RED,153 focused checks and independent
review, and is not part of the candidate.

[Closed trial archive](ORDERED_CREDIT_HEAD_ORDINARY_20260911.raw.tar.gz):
21 regular files; independent input-byte verification, root complete171-line
appendix review and gzip/manifest checks done. No observer follow-up exists.

No compiler, laboratory run or subagent exploration remains active.
Root alone runs builds/labs, without overlap. Existing project Docker only;
no sudo, outside-root work or /mnt/storage use. Preserve the unrelated user
seven-line edit in LIVE_OWNER_FRONTIER_WORK_BOUND.md; AGENTS.md is immutable.
Last Telegram milestone delivered by00:28:10 UTC; next nonurgent notification
no earlier than01:28:10 UTC. Commentary stays timely; no release notification
before an actual milestone.
