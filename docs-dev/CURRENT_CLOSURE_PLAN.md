# Current deterministic closure plan

Updated: 2026-09-08 08:03 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each new
transaction and after compaction. This is the active scope/next-action ledger,
not a new issue inventory. Detailed completed transactions remain in
`git show 89a1a63:docs-dev/CURRENT_CLOSURE_PLAN.md`, named evidence below and
PROGRESS. Older history also remains at93370dc/CLOSURE_PLAN_HISTORY_THROUGH_20260907.
Shortening this ledger discards neither failures nor gates.

## Current source and disposition

Runtime checkpoint **445011f**: remove redundant full-horizon request frontier
discovery; query the already ranked prefix once. Focused321 checks pass, but
ordinary timing is mixed/adverse. 89a1a63 preserves the comparison; 192ae8d
preserves the subsequent exact request/native service discriminator.
No observer or shelved request-sampler overlay is active. The user's seven-line
LIVE_OWNER_FRONTIER_WORK_BOUND.md addition remains untouched.

| Component | Actual proof | Practical disposition |
| --- | --- | --- |
| Native reordered-packet/history corrections | Real encrypted packet REDs; jitter-only.585->185.313Mbps | Useful declared case, not every network or recovery gate |
| Late STARTUP after FINAL,11d6f3a |34checks; refusal belongs to attachment, not shared carrier | Earlier mixed down85.556/up64.602Mbps with4.423/5.054s gaps; not fluent |
| Request retained recovery beyond old negative H,011aee9 |21checks; actual positive F/retained ownership usable when H<F | TCP incomplete; mixed70.598->49.967Mbps, no practical promotion |
| Unique ACK atoms,765683b |66checks; Original65536/copy14600 retains50936 unique proving bytes | Mixed63.923->26.050Mbps, adverse; no practical promotion |
| Response retained assignment recovery,953a54f |333checks; no-ACK/H<F and immutable head deadline, preserved active/final admission |35.608->43.611Mbps but maxgap4.799931->6.080193s; no practical promotion |
| Ranked-prefix query,445011f |321checks; equal exact output, fewer irrelevant visits |36.394->71.344Mbps but maxgap4.260012->4.627694s; no practical promotion |
| Shelved paired-clock request sampler |Actual pipelined[2,2,2]->[2,3,4];23checks |TCP incomplete/adverse, not stacked into runtime |

Random realizations and unequal accepted work prevent a simple causal
regression estimate, but do not permit promotion from an attractive average.
No third favourable trial replaces an adverse pair. An intermediate correctness/
work-bound commit is not performance acceptance.

## Completed current feedback loop

### Actual local service, not a guessed congestion issue

RESPONSE_QUIC_HANDOFF_20260908.md and its raw/trace artifacts preserve exact
response paths, stages and contrary controls:

- First ordinary953a54f comparison freezes client reply delivery while the
  server reads replies and continues target writes. This is real return delay,
  not only slow forward traffic or a stale dashboard rate.
- TCP handoff diagnostic: slowest TCP copies lose. All7 TCP winning Originals
  reach mux within12ms of decode; largest winning gaps belong to QUIC before
  attachment forwarding. Do not optimize a losing duplicate's maximum delay.
- Exact QUIC chain: all82 ordinary writes join,78 advanceF. Winning[768,782)
  accounts approximately1.852s of a1.856s plateau in local handoff/service;
  [796,810) accounts.862s of.875s. Server local write stages are<=2ms.
- The preceding11.708s send-await aggregate for[754,768) spans21.872s, not only
  its9.433s largest stall. Do not assign an aggregate to a narrower window.
  Await time includes recipient/executor service, not exclusively Pending
  channel capacity or CPU. Sampled full queues alone authorize no queue fix.
- Follow-up cost capture: exact450166784B/44.532321s/maxgap5.083127s. F54/F65
  winning plateaus account.953/.957s and1.137/1.140s inside local handoffs.
  Surrounding1s windows contain.40--.64s preparation and.13--.35s ACK handling.
  Whole preparation7.009682s includes retained helper3.140200s; whole ACK
  handling2.070443s and Dispatch1.585453s are separate outer regions.
- Largest F646 gap in that capture instead includes3.161s before Original
  publication and1.921s write-to-decode; decode->mux1ms. The early CPU/service
  observation does not explain that contrary case.

Cost observer: RESPONSE_CLIENT_COST_TRACE_20260908.patch/raw archive. It reused
September6 aggregate scopes rather than reinventing a profiler. That older
profile disproved direct ACK/flight as dominant old-tail cost and found the
since-corrected quadratic sweep. Current code did not contain that old algorithm.
Observers were frozen/reversed before runs; diagnostic rates are not acceptance.

### Exact-equivalent work deletion, not a policy tweak

53d9ab5 deliberately made recovery independent of cache storage chunks.
bfac5b8 preserved the exact ranked extent through Apply after112.6-times
unranked suffix amplification. Both intentions remain.

The old request caller discovered uniform ownership over all[F,N), then queried
the scored prefix again. Let U end the initial owner/avoid-uniform prefix and
Q be the unchanged selection limit. One query over[F,min(N,F+Q)) returns exactly
[F,min(U,F+Q)), including ordered identities and assignment maxima within that
returned prefix. A boundary beforeQ remains a shorter valid result; it is not
a new full-Q admission requirement. Old full-query timestamps were unused.
Apply already satisfies service<=scored extent.

Actual cache/flight producer + synchronous retained-helper RED:229 legal64B
chunks cost2744visits;1024chunks cost7514, with the identical14600B ranked
prefix. Only the irrelevant-suffix assertion fails after existing exact target/
short-owner-boundary controls pass. After445011f, both cases cost1372visits.
Existing4096 oracle cases pass four restriction quanta each, including zero;
4model+31requestsender+28requeststream+258relay=321distinctchecks pass.
Functional builds1m10s RED/1m08s GREEN; optimized candidate2m01s.

No quantum, clock, target, native admission, ownership, copy suppression,
controller, queue, persistent cache or RFC policy changed. Two independent
proof/diff reviews passed. Work reduction is demonstrated; the whole3.14s
helper budget is not promised savings or user-latency reduction.

## Latest ordinary comparison — promotion stopped

Exact cells:
`./.tmp/reflection/results/mixed-combined-up-frontier-scope-{control,candidate}-0908/`.
Raw archive: FRONTIER_SCOPE_ORDINARY_20260908.raw.tar.gz. Full96 raw bins and
stage/cost observations: RESPONSE_QUIC_HANDOFF_20260908.md.
Both endpoints use their cell binary, diagnostics off:
control `./.tmp/reflection/bin/response-retained-20260908/mptunnel` (953a54f),
candidate `./.tmp/reflection/bin/frontier-scope-20260908/mptunnel` (445011f).

| Outcome | Control | Candidate |
| --- | ---: | ---: |
| Exact complete bytes, no errors |214695936 |420610048 |
| Elapsed seconds |47.194034 |47.163930 |
| Whole confirmed Mbps |36.394 |71.344 |
| First confirmation seconds |.485959 |.465351 |
| Maximum confirmation gap seconds |4.260012 |4.627694 |
| First local write seconds |.149918 |.104370 |
| Maximum local write gap seconds |1.897546 |6.914621 |
| Sampled client peak RSS KiB |1159376 |464976 |
| Upload class byte delta |280364967 |572257386 |

Nearly twice the bytes and lower sampled peak RSS do not waive adverse gaps.
CPU is lifetime ps%, not interval CPU; unequal work prevents normalized cost
claims. Confirmation bins above500Mbps are buffered ACK-observation releases,
not physical wire-rate claims. No further ordinary run seeks a passing number.

## Completed discriminator and active transaction: recovery service ordering

One joint publication/writer/server/native diagnostic on445011f completed:
310640640 exact bytes/52.591374s, maxconfirmation10.010333s. Build2m14s;
hooks archived and reversed before the run. REQUEST_PREFIX_SERVICE_20260908.md,
its TRACE patch and3.6MiB raw archive retain full53bins, stage joins and costs.
No new performance candidate, no third ordinary run, no runtime correction.

Two exact server frontier waits:
- F206843545: QUIC Original local H3 acceptance1788823302404; first decoder
 1788823310534; mux10546. Actual post-outage frontier plateau2.754s;
 no prior covering repair. All51 contemporaneous TCP copies address lower
 ranges, not that missing byte. No preceding ordinary reader-send wait.
- F233705913: frontier held1788823313539--1788823325383 (11.844s).
 TCP Original publication3307963->writer/flush3317116 (9.153s); its later
 receipt loses. First covering copy is TCP3324669 with200ms D; winningQUIC
 copy3324884->decode3325367->mux3325383. No earlier accepted-copy deadline
 explains the11.130s before first covering publication.

During late hold, T equalsF; serverreplyRs1687 staysflat andRc catchesup,
excluding target-socket/held-return explanations for its main span. Qnative
Originaldebt/flight becomes0; TCPnative ACKs continue. All28499 registry
transactions are admitted normally; winning missing-range decode->mux<=16ms.
The exact cause of absent earlier head publication remains a Product question.
Later-range copies were published, but queued-range membership was not logged.

Native observer locates actual PTO backoff during the outage, with no earlier
loss-time precedence at those events. Native ACKs resume after the laterprobe;
the late11.844s pause instead outlasts QUIC recovery and includeszero native
flight. Do not shorten PTO or call it the whole rootcause. Native epochs,
client/server physical IDs and runtime/wire ordinals are separate namespaces.
The diagnostic printed232219 reinjection rows, many queued=false overlap
attempts; timing is perturbed and not ordinary acceptance. That specific event
means queue overlap after selection, not generic rejection. Future captures
must not repeat this high-volume filter without a concrete need.

### Declared RED/control before any production fix

**Issue/benefit:** ordered prefix recovery can lose finite healthy-target
service to later bytes merely because of historical qualification-entry order.
Timely ordered delivery, not another bulk-rate number, is the objective.

**Origin/intention:** per-owner stale/detached recovery predates recent fixes
(5e1ace6/622960c);3e35938 retained exact insertion-ordered qualification state.
Exact per-target queued accounting and b37bacb's no-unbound fallback prevent
duplicate authority/reset and remain justified. The live-owner fallback
intentionally excludes stale new-payload owners; its separate stale branch
must still serve retained ownership coherently. Do not requalify a stale
owner merely to make recovery possible.

**Source counterexample, now dispatch-RED:** due stale ownerA owns lower interval,
due staleB owns only a later interval, and freshC has finite actual admissionK.
B-before-A entry order lets B chargeK in drive_request_path_recovery before A
is considered; A-before-B instead lets the prefix enter service. Current live
frontier fallback excludes staleA and cannot override this ordering. Insertion
can originate from ordinary progress, not only mark_stale timing.

**Actual proof:** real cache production, exact Original flight fixture, current
target admission and actual command dispatch. With q=524288 and all ranges
fitting C's unchanged startup envelope, prefix-first control passes;
suffix-first emits q instead of0; A/B/A emits2q instead ofq on its second
dispatch. Both fail only at the intended offset assertion. Two existing
recovery controls pass. This proves dispatch ordering, not finite-credit
exhaustion, physical wire timing or the entire captured stall. The first
fixture incorrectly assumed a smaller measured C bound and failed in setup;
that was NOT Product RED. No runtime correction or tuned limit was used.

**Smallest next action:** implement/audit the declared no-structural-queue
candidate in REQUEST_PREFIX_SERVICE_20260908.md. Independent reachability and
model reviews support replacing historical owner bulk materialization with
byte-ordered metadata collected once per existing Dispatch batch. Refill within
that batch, not one actor wake/ACK per frame; do not rediscover the whole ledger
per published frame. Preserve accepted copies, independent-target work, existing
queued live repair accounting, exact Apply and full structural allowance.
Prequeued-later/newly-due-head and native-blocked independent-target controls
must join the two REDs. No candidate is accepted until focused GREEN and
ordinary timing/completion/cost review. Test/evidence checkpoint is d56ccca.

**Falsifier/stop:** if model gates prevent this ordering counterexample or both
orders serve the same lowest eligible range, reject it. The capture does not
prove all TCP remained stale; later TCP copy acceptance implies requalification
was possible. Do not attribute its entire11.844s to an unobserved queue.

**Model boundary before implementation:** choose retained byte obligation before
allocating target service, with target eligibility separate from historical
Original ownership. Do not merely sort owners by their first offset and allow
one owner's later disjoint range to jump another owner's earlier range. Preserve
exact-target admission, immutable copy suppression, ranked extents, partial
credit, independent-target work, native ownership and terminal lifecycle.
Evaluate this composition before code. Focused GREEN plus ordinary timing/
completion/cost and independent review still required; no acceptance yet.

## Established boundaries — do not reopen without contrary evidence

- Naive copy-ahead cursor/aggregate-ETA guard rejected before code: exact native
  work ahead of a queried range is unavailable; later suffix debt must not
  determine earlier-prefix rank. Earlier serialized14600B repair pairs occurred
  inside10Mbps QoS, not500Mbps spare-capacity proof.
- Initial QUIC ignored false positive closed:187TCP Originals/12.19MB in25ms
  had only one logical attachment; QUIC was opening, attached91ms/data93ms.
  Initial-owner ablation improves early target bytes but not all gaps and
  worsens first confirmation; no permanent protocol preference accepted.
- Static ranking, relative ACK codec, ready-feedback writer batching,
  wrapperless actor, absolute-delay reordering,3N1 and isolated raw-byte
  hysteresis deletion remain rejected. Their records are not implementation
  obligations. No sampler overlay silently returns.
- Finite1941 mixed+64 single-mode churn reclaims owners with flat post-load
  RSS. The uncaptured deployed random RAM/CPU incident is not fully attributed.
- Ingress before probe D but actor effect after D does not authorize accepting
  retired evidence or lengthening D. Response assignment clocks are fixed;
  analogous request fresh-append scope is recorded separately, not globally waived.

## Global gates — unchanged, not satisfied by isolated GREEN

| Order | Scope | Required evidence |
| --- | --- | --- |
|1 |Mixed allocation, upload sampling, cold/warm startup |Exact cause/model/real RED/control/audit/GREEN plus ordinary first-body/gaps/loaded latency/completion |
|2 |TCP/QUIC changing loss, jitter, QoS, blackhole/recovery |Both directions, same-request restart-free recovery; native receipt versus ordered user service |
|3 |Aggregation/shared contention |Single500Mbps, independent200Mbps each, shared cuts, asymmetric3--10%mean6 loss/jitter/QoS/outage combinations and ablations |
|4 |Actual experience/baselines |TCP,QUIC,default; cold/warm single/concurrent/real speed.cloudflare.com; raw TCP,Xray,Hysteria2 matched topology/configuration including failures |
|5 |Sustainability |Restart/churn, backpressure, ownership, CPU/RSS and post-load recovery; reopen only on contrary evidence |
|6 |Publication |Full timing/latency series and cost/completion with goodput; README/PERFORMANCE and release only after competitive gates |

Pinned current diagnostic profile: routed/mirrored single500Mbps; upload70/20ms
delay/jitter, return30/5ms;5s upload loss[3,8,5,6,10,3,5,8]%mean6, return
[1,2,.5,3,2,.5,1,2]%. Upload10Mbps15--25s, UDP outage30--33s,40s load,
85s runner observation guard/90s probe boundary. Censoring is not complete
throughput. Random realizations are not packet-identical; do not reuse old
aggregate300/200 defaults as200each. Do not tune this profile to pass.

## Execution and continuity

- Owned Docker only; no sudo, host shaping, outside-repo work or build/lab
  overlap. All products/probes are stopped; origin services retained.
- No diagnostic overlay/build/lab. The declared structural-recovery candidate
  is being implemented; its test migration and focused verification are pending.
  Commit exact intermediate dispositions;
  preserve small evidence before scoped cache cleanup. No deletion this turn.
  User seven-line edit must remain outside commits.
- Telegram milestone authorization: no more often than hourly. Last sent about
  2026-09-07 23:10UTC; next nonurgent not before2026-09-08 00:10UTC. Respect advisory;
  ordinary gaps remain open, so do not send a completion claim.
- Reflection: local work proof was useful and enabled a deletion, but did not
  predict every network interval. The ordinary pair moved the worst observed
  holding stage; this is why total Mbps and isolated GREEN cannot close the
  loop. Follow the exact stage, without bundling unrelated fixes or discarding
  adverse evidence. The joint trace isolates publication/native/input stages;
  historical per-owner allocation is now the bounded RED/control question,
  not proof that one new correction will explain every captured pause.
- Universal clairvoyant optimality under arbitrary future outages is impossible;
  that does not waive avoidable delays or any practical gate. Never label
  unfinished work ideal or promise cost-free capacity discovery.
