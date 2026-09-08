# Current deterministic closure plan

Updated:2026-09-08 15:23 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each
transaction and after compaction. This is the active scope/decision ledger,
not a new issue inventory. Full previous ledger, including all earlier failed
approaches, remains at `git show ebad57f:docs-dev/CURRENT_CLOSURE_PLAN.md`.
Detailed evidence is linked below; shortening obsolete next-action prose does
not discard findings, adverse results or acceptance gates.

## Active transaction: native claimant owner admission

**Pre-change decision15:23:** the completed reply/cost joins below select a
reachable local service boundary, not a universal congestion explanation.
Two advisory `owner.lock()` acquisitions in `claim_prepared_request_data` can
park a native writer's executor thread while the Product actor holds its mutex
for ACK/recovery. The final Native-fenced acquisition already uses a prearmed
nonblocking try-lock; the two earlier acquisitions do not. Origin9720e4b
addressed the lock cycle, but absence of a lock cycle does not ensure responsive
native input service. An empty source check can also wait behind that owner.

**Question / competing causes:** does the actual prepared producer return
without blocking when either advisory acquisition meets retained Product
ownership? Actor work and FIFO input backpressure remain distinct causes of
the captured gaps. Claim elapsed includes contention and scheduling; it does
not measure mutex waiting separately or prove it caused every held reply.

**Model / predicted correction:** use the existing freshly prearmed try-lock
at each writer acquisition and return its existing Busy outcome on contention.
At the second cut, discard the advisory frame/receipt and retry current state
after unlock. Preserve exact registration/source/Ready/Native checks, final
fence, U→Original conservation, cancellation and all admission policy. Busy is
not evidence of a bad path or authority to select Backup. Actor-side ownership
remains serialized. No new timer, queue, coalescing, controller or threshold.

**Smallest action / falsifier:** test-only actual producer controls retain the
real Product mutex in another thread at each cut. Require Busy before release,
no committed byte/flight/charge change, unlock-before-first-poll wake and the
same lowest-source claim after release. Bound only test cleanup so the old
blocking implementation fails rather than hangs. Uncontended claims and stale
registration/terminal refusal remain opposite cases. Independently audit
fresh arming after the first unlock: reusing a pre-own-unlock wait could spin.
Only a real RED permits runtime editing. Then focused GREEN and one ordinary
candidate/parent pair on the unchanged profile, retaining full completion,
phase/gap and cost evidence. A worse/ambiguous result stops promotion; no
diagnostic-rate acceptance or unrelated model expansion.

**Pair order fixed before execution15:25:** fresh frozen ordinary9ea25e2 parent
first, then the candidate only after actual RED/GREEN/audit. Parent capture
can run during test-only fixture preparation, with no build running. This
uses host time without mixing a build into the lab; the older ordinary9ea
realization is context rather than a substituted control. No favorable retry.

**RED15:32:** actual uncontended control passes; both advisory acquisition
cases and cancellation reach the intended blocking assertion only after
semantic held-state controls and thread cleanup.1pass/3fail,1.00s;
warning-free build1m12s. Independent model/fixture review passes. Implement
only the two existing prearmed try-lock acquisitions now. Fresh ordinary
parent completes355532800B/44.964483s, maxconfirmation5.052667s; this variation
is retained alongside the earlier11s gap rather than called acceptance.

**GREEN15:34:** two advisory acquisitions changed, final fence unchanged;
542checks pass1.28s after warning-free1m12s rebuild. Independent model and
consumer/fixture reviews pass. Ordinary optimized candidate build next,
no lab overlap. No practical promotion from this checkpoint.

## Completed discriminator: remaining winning-reply service hold

**Executed15:00:** warning-free diagnostic build3m33s; all observer hooks
archived/reversed before capture. Runner0,366018560 exact bytes/43.726528s;
maxconfirmation4.375291s, maxwrite4.114246s. It does NOT reproduce the ordinary
11s gap and cannot establish better ordinary performance.4371total log lines,
44service samples; products/probes stopped. Full raw diagnostic preserved.
Exact winning-reply joins and aligned nested-cost analysis are complete in
COPY_DEBT_SERVICE_20260908. No additional runtime fix, build or lab is running.
The4.374s F639 hold is mostly before decode; F737 includes1.203s already-decoded
local residence. F821 has at least2.722s preceding reader-send-awaited overlap,
but different cost composition from F737/F68. Neither global ACK cost nor
recovery cost alone explains every hold.73 winning mux advances all reach the
local writer within4ms after mux. Native-writer owner contention is a source-
reachable mechanism to falsify next, not a conclusion from one aggregate.

**Issue / observed failure:** ordinary9ea25e2 completes254083072 exact bytes in
48.973579s, but maximum confirmation gap remains11.042148s. Candidate Rc433
holds10.001s (Unix1788849816832–1788849826833) while target/response production
continues. Separate target T230311809 holds9s at1788849821825–1788849830825.
These are real fluent-service failures, not solved by improved total Mbps.

**Competing causes / exact question:** is the winning missing reply still
before native decode, between decode and Product input/mux, or after mux at
local delivery? Native congestion/backpressure, local routing/executor service,
costly preparation/dispatch and local delivery remain alternatives. Ordinary
management/socket counters do not locate the exact frame. The older local
decode-to-mux2.226s attribution belongs to another version/realization.

**Existing evidence:** COPY_DEBT_SERVICE_20260908 retains the complete ordinary
candidate/parent pair and all raw bins/costs. PREPARED_REPLY_SERVICE_20260908
retains100 winning-frame joins and synchronous cost buckets from the prior
sparse diagnostic. Inside that prior held interval, Product debt changes
6068248B between snapshots: globally frozen recovery state is disproved.

**Smallest experiment:** reuse the archived sparse reply-stage and aggregate
observer on9ea25e2. Add nested synchronous scopes for recovery range preparation
(two phases), complete target selection, Native resolution, Product projection,
queued-copy debt, repair Apply and its fenced bookkeeping. No per-attempt logs,
new harness/profile, target-observation cache or runtime policy correction.
The reused hooks plus seven scopes passed independent review and were reversed
before running the frozen diagnostic binary. No ordinary run is active.

**Falsifier / stop / acceptance:** exact winning-frame joins precede attribution.
Prompt decode/mux rejects that local stage; prepublication/predecode delays
remain distinct. No new long gap means non-reproduction, not ordinary repair.
Nested elapsed totals are not independent CPU totals and cannot be added.
Diagnostic Mbps is never ordinary acceptance. No further code correction until
a reachable mechanism/control and a clean model justify it. If evidence selects
a different stage, follow it rather than force the recovery-cost hypothesis.

## Current source and latest practical disposition

Runtime checkpoint **9ea25e2** maintains exact additive accepted-copy debt by
attachment. RED checkpoint **3396087**, evidence checkpoint **ebad57f**.
No shelved sampler, new Native observation policy or congestion tuning is active.
The user's seven-line LIVE_OWNER_FRONTIER_WORK_BOUND.md edit is untouched and
must remain outside commits.

| Outcome | Parent b3dfef1 | Candidate9ea25e2 |
| --- | ---: | ---: |
| Exact completion |No,85s observation guard |Yes |
| Confirmed / accepted bytes |93570384 /180748288 |254083072 /254083072 |
| Elapsed seconds |85.948541 |48.973579 |
| First / max confirmation gap seconds |.467841 /66.779472 |.397566 /11.042148 |
| First / max write gap seconds |.154499 /15.796057 |.128872 /1.011139 |
| Longest sampled Rc / T hold seconds |66.001 /60.000 |10.001 /9.000 |
| Peak client / server RSS KiB |511988 /90968 |677496 /129508 |

Candidate first3s target delivery is worse, but10s delivery and settlement
improve. RSS is higher with more work. Random realizations, different duration
and unequal accepted/confirmed bytes prevent causal rate ratios or a uniform
non-downgrade claim. Parent reset follows guard teardown; its raw confirmation
bins are unavailable, not zero. Candidate49 raw bins include long zero spans.
No practical promotion or favorable third ordinary repeat.

### What the last correction actually proves

The real dispatcher emits the same4096B repair from69632 retained bytes and
performs two accepted-copy-debt queries. Fragmenting only an irrelevant Original
suffix raised visits4→130; the intended work assertion alone failed after
semantic controls passed.9ea25e2 maintains J_i=sum(retained copy bytes on exact i)
at append, ACK-fragment release and drain, replacing each full scan with one
lookup. Overlaps count separately; expiry/Native ACK do not erase debt; zero
keys retire and replacements remain distinct. Checked arithmetic is supported
by current final admission, not a new cap. No RFC quantity or policy changed.

Two independent reviews, a full-scan lifecycle oracle and538 focused checks
pass; RED/GREEN builds1m13s each, GREEN1.28s. Ordinary build3m29s. The practical
pair supports better settlement in this case, not attribution of every change
or acceptable remaining latency. See COPY_DEBT_SERVICE_20260908.md/raw.tar.gz.

A further Native observation-sharing shortcut is **not implemented**. It can
be a coherent Observe–Decide policy but is not equivalent when a path changes
mid-selection: current chosen Apply cannot recover a better omitted target.
Do not silently package it as source cleanup.

## Retained findings and boundaries

| Mechanism / checkpoint | Proven scope and remaining limitation |
| --- | --- |
| Native reordered-packet/history corrections |Real encrypted packet counterexamples; jitter-only.585→185.313Mbps. Not all network/recovery gates |
| Restart / late STARTUP / lifecycle ownership |Concrete scoped refusals, wake/reclamation checks;1941mixed+64single-mode churn reclaims owners. Deployed random RAM/CPU event not fully attributed |
| Request retained recovery011aee9 and unique ACK atoms765683b |Real ownership/attribution counterexamples. Their adverse/incomplete ordinary pairs remain; component GREEN never promoted them |
| Response retained recovery953a54f |Immutable assignment recovery, active/final admission preserved; better aggregate but worse6.080s gap in ordinary pair |
| Ranked-prefix query445011f |Exact output with fewer irrelevant visits; average improves but gaps worsen. No universal acceptance |
| Ordered direct structural recovery1436ff4 |Actual byte-order/overlap-work REDs; full allowance and independent-target service preserved. Ordinary early target service adverse |
| Prepared request ownership9720e4b |U→exact Original ownership at native claim; no premature TCP assignment.533checks/audit, then ordinary75s return hold/incomplete settlement |
| Persistent idle readinessb3dfef1 |Real all-refused two-writer retry recurrence removed;535checks/audit. Ordinary long return/target holds remain |
| Exact copy-debt index9ea25e2 |Work and conservation proved; ordinary settlement improves,11s gaps remain |
| Shelved paired-clock request sampler |Actual[2,2,2]→[2,3,4] sampling correction; ordinary incomplete/adverse, not stacked into runtime |

Preserve prepared-source conservation and exact chosen readiness epochs:
A=prepared source end, C=native claimed end, U=A−C, B=U+sum Original debt.
Claim U→O does not increase B; ACK cannot exceed C; FIN waits for U=0.
Failed protected writes retain exact ownership. Product→Native actor calls
coexist with Native→tryProduct writer calls; no blocking reverse edge or guard
across await. A deadlock-free graph alone is not a latency guarantee.

Structural recovery is globally byte-ordered but cannot let a blocked target
stop independently eligible work. Keep existing exact copy deadlines/J/Native
Apply. Do not resurrect old suffix queues, ACK-per-frame structural allowance,
renewable deadlines, scalar same-host protocol preferences or guessed capacity.
Direction-neutral response parity remains pending; do not silently treat the
request-only prepared migration as both-direction completion.

Rejected static ranking, relative ACK codec, ready-feedback batching,
wrapperless actor, absolute-delay reordering,3N1 and isolated raw-byte hysteresis
deletion remain rejected. No broader topology-inference framework, controller
retuning or new speculative inventory follows from these captures.
Original-QUIC-ignored claim was disproved by actual attachment timing.
No target/backpressure or losing-copy delay may be labelled the winning gap.

## Global gates — unchanged and not satisfied

| Order | Scope | Required evidence |
| --- | --- | --- |
|1 |Mixed allocation, upload sampling, cold/warm startup |Exact cause/model/real RED/control/audit/GREEN plus ordinary first-body/gaps/loaded latency/completion |
|2 |TCP/QUIC changing loss, jitter, QoS, blackhole/recovery |Both directions, same-request restart-free recovery; native receipt versus ordered user service |
|3 |Aggregation/shared contention |Single500Mbps, independent200Mbps each, shared cuts, asymmetric3–10%mean6 loss/jitter/QoS/outage combinations and ablations |
|4 |Experience/baselines |TCP,QUIC,default; cold/warm single/concurrent/real speed.cloudflare.com; raw TCP,Xray,Hysteria2 matched topology/configuration including failures |
|5 |Sustainability |Restart/churn, backpressure, ownership, CPU/RSS and post-load recovery; reopen only on contrary evidence |
|6 |Publication |Full timing/latency series and costs/completion with goodput; README/PERFORMANCE and release only after competitive gates |

Pinned current profile: routed/mirrored single500Mbps; upload70/20ms and
return30/5ms delay/jitter. Five-second upload loss[3,8,5,6,10,3,5,8]%mean6;
return[1,2,.5,3,2,.5,1,2]%. Upload10Mbps15–25s; UDP outage30–33s;40s load,
85s runner guard/90s probe boundary. Do not tune the profile to pass.
Random packet realizations are not identical controls; confirmation bins
above500Mbps can be buffered observation, not wire capacity.

## Execution, evidence and continuity

- Owned Docker only; no sudo, host shaping, outside-repo work or build/lab
  overlap. Products/probes stopped; origins retained. No build/lab running.
- Normal frozen candidate:`./.tmp/reflection/bin/copy-debt-20260908/mptunnel`.
  Normal parent:`./.tmp/reflection/bin/prepared-idle-20260908/mptunnel`.
  Diagnostic: `./.tmp/reflection/bin/copy-debt-service-20260908/mptunnel`.
  target/release is the diagnostic binary: do not use it as ordinary.
- Exact intermediate commits only. Preserve raw evidence before scoped cleanup.
  No deletion this turn; ample root space. User7lines must stay unstaged.
- Telegram last meaningful report before2026-09-08 07:26UTC; next nonurgent
  not before08:27UTC. Respect hourly minimum and soft-frequency advisory.
  No component-only success notification or unfinished completion claim.
- Reflection: exact symbolic conservation enabled one justified work deletion,
  but did not predict every timing phase. The ordinary pair supplies real
  settlement progress while exposing adverse early/RSS observations. Preserve
  those limits; trace the same winning-prefix stage instead of stacking a
  plausible policy shortcut or repeating until a favorable average appears.
- Universal clairvoyant optimum under arbitrary future outages is impossible;
  that does not waive avoidable delay or practical gates. Never call unfinished
  work ideal or promise cost-free capacity discovery.
