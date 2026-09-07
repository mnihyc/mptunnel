# Current deterministic closure plan

Updated: 2026-09-08 04:22 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each
transaction and after compaction. This is the active scope and next-action
ledger, not a new issue inventory. Completed transaction detail is preserved
in the named evidence below and in git checkpoint `93370dc`:
`git show 93370dc:docs-dev/CURRENT_CLOSURE_PLAN.md`.
Older history is CLOSURE_PLAN_HISTORY_THROUGH_20260907. Shortening this ledger
does not discard failed experiments, authorizations, or global gates.

## Current runtime and ordinary results

Current isolated runtime: `765683b`; ordinary executable
`./.tmp/reflection/bin/ack-atoms-20260908/mptunnel`.
No diagnostic or request-sampler overlay is active. The user's seven-line
LIVE_OWNER_FRONTIER_WORK_BOUND.md addition remains untouched.

| Checkpoint / scope | Proven | Ordinary disposition |
| --- | --- | --- |
| `11d6f3a`, late STARTUP after FINAL | 34 focused checks; refusal belongs to attachment, not shared carrier | Retained comparator. Mixed combined down85.556Mbps /4.423s gap; up64.602Mbps /5.054s gap and19.293s drain. Not fluent acceptance. |
| `011aee9`, retained repair beyond complete-ACK horizon | 21 checks; actual positive F and retained ownership remain usable when negative H<F; same-range accepted-copy deadline preserved | TCP pair incomplete/adverse. Mixed control70.598 vs candidate49.967Mbps; no practical promotion. |
| `765683b`, unique ACK atoms beside partial copies | 66 checks; original65536/copy14600 yields50936 unique proving bytes in both directions, without changing settlement/epoch fences | Mixed control63.923 vs candidate26.050Mbps; maxconfirmationgap6.000557 vs6.035895s. Exact completion, less work in more time. No practical promotion. |
| Shelved request cohort sampler | Actual pipelined samples[2,2,2] vs staged[2,3,4]; candidate23GREEN | TCP pair incomplete, confirmation gap worsened. Frozen candidate/patch retained; NOT implicitly stacked into current runtime. |

Unseeded pairs do not alone prove a causal regression, but adverse or incomplete
results stop promotion. No third favourable trial replaces them.
REQUEST_COHORT_ORDINARY_20260907 contains full raw timing series, identities,
cost and attribution limits; its linked raw archives preserve observations.
Late-startup ordinary detail is LATE_STARTUP_SCOPE_ORDINARY_20260907.

## Just closed: what causes the slow prefix

1. **False QUIC silence was a real byte-attribution defect.** In the retained
   requalification trace, a full65536B original ACK with only14600B copied was
   classified wholly ambiguous. Unique progress at3620ms was discarded and
   QUIC withdrawn3624ms. `765683b` corrects exact set subtraction; it does not
   claim to fix every subsequent ordered-service gap.
2. **Actual live-repair feedback serialization is proven.** Two adjacent pairs
   of14600B QUIC copies beat every intersecting TCP original/copy at receiver F.
   Native writer acceptance0--1ms; peer advance64--98ms; next disjoint copy
   waits another127--128ms for client ACK application despite L1.8--2.0MB.
   Actual cause is persistent ACK-gap recovery, not retained fallback alone.
   These pairs occur inside10Mbps QoS, not500Mbps spare-wire evidence.
3. **Initial ready-path selection false positive is closed.** One membership
   capture publishes187 contiguous TCP originals totaling12,189,643B in25ms,
   every event with exactly one attachment. QUIC was already opening, attaches
   at91ms and receives data93ms. It was not ignored while ready.
4. **Other causes remain distinguished.** Early QUIC native work is substantial
   before13s; some copies lose to TCP. At13--20s, source and ordered target
   advance3,276,800B while the leading TCP debt falls by exactly that amount,
   retaining64MiB separation. Late50--55s all222,429,184B are already written to
   the target while client Product debt remains43MB: debt is not undelivered
   payload. During UDP outage, native silence is a different stage again.

Evidence: ACK_ATOMS_SERVICE_TRACE_20260908.patch and
ACK_ATOMS_SERVICE_DIAGNOSTIC_20260908.raw.tar.gz; initial membership observer
INITIAL_PLACEMENT_TRACE_20260908.patch and
INITIAL_PLACEMENT_DIAGNOSTIC_20260908.raw.tar.gz.
All overlays were frozen and reversed before their captures. Diagnostic rates
do not pass the ordinary gate; latest membership capture has a13.500752s gap.

## Completed model decision: do not implement naive copy-ahead

Three independent read-only reviews agree. A repair-placement cursor can be
distinct from actual received F, but that distinction alone does not make
ahead copies useful or control their aggregate service cost.

- T06/`bfac5b8` deliberately preserves one ranked lowest-frontier hedge.
  It removed a demonstrated112.6-times score/Apply suffix expansion. A test
  demanding another adjacent copy before DataACK would assert a new policy,
  not a RED against that existing contract.
- Keep exact original ownership, H/F, qualified epochs, stable copy slots,
  same-range suppression and native authority. Every proposed next range would
  need its own age/rank/Apply proof; provisional or accepted copies are not
  received bytes. Cancellation/detach must expose an earlier hole again.
- Existing service admission subtracts queued+accepted copy debt. Ranking
  cannot erase that debt, nor simply add it atop native queue/flight where
  overlap exists. Already delivered but un-DataACKed copies still own Product
  resources without necessarily needing more wire service.
- An ETA guard is not an exact-prefix remedy: request/multipath.rs computes
  owner completion from ALL OriginalData debt, not native work ahead of the
  queried range. Increasing a later suffix can change an earlier-prefix
  decision. Freshness and jitter margins do not fix that scope mismatch.
- The existing fallback epoch holds one frontier. Moving it through successive
  speculative positions can overwrite an earlier immutable deadline; returning
  after cancellation must not renew that mature cause.
- At10Mbps, observed L1,811,008--1,966,080B costs at least1.449--1.573s of
  shared serialization before framing/retransmissions. Independently ranking
  repeated Q on the sole alternate can still fill L. Some old TCP prefixes are
  already received awaiting feedback, so such work can be entirely redundant.

**Disposition:** real conditional service limitation; naive cursor and
aggregate-ETA-guard forks rejected BEFORE implementation. Existing producers
support ownership/accounting, not the promised exact-range service forecast.
No quantum, timer, traffic percentage, controller, P/E or native-queue change.
No extra trace is needed to reach this decision. The existing due-F fallback
remains. A future useful policy may be a clearly declared empirical heuristic;
it must not be presented as a guarantee derived from unavailable evidence.

## Completed transaction: initial-owner causal ablation

- **Issue:** mixed upload has long ordered stalls after committing a large
  prefix to its sole initial TCP attachment. The exact trace establishes
  ownership and timing, but not how much whole-profile delay would disappear
  with another initial owner. Do not infer dominance from correlation alone.
- **Question / competing causes:** does initial irreversible TCP placement
  dominate the early slow period, or do subsequent allocation, recovery, native
  service and feedback produce similarly poor timing regardless of first owner?
- **Smallest preflight:** inspect supported configuration and the existing
  initial-open selection seam. Determine whether initial QUIC selection can
  be changed without changing later mixed membership, rate hints, qualification,
  budgets, native controllers or the network. No permanent protocol preference.
  If configuration cannot isolate it, declare the exact temporary lab-only
  intervention and its confounders before any build/run; no implicit backup
  policy, reorder of all later decisions or new harness.
- **Planned causal comparison:** one declared ordinary control/intervention
  mixed combined upload pair on current runtime, diagnostics off, initial
  owner the sole intended difference. Both retain three TCP plus one QUIC.
  Same500Mbps routed/mirrored asymmetric profile and existing receiver probe.
  Distinct frozen identities/configuration; no build/load overlap.
- **Preflight resolved, 04:03:** no config changes only initial choice. Array
  order also changes later ties; rate/backup changes persist. Use a throwaway
  one-file intervention in open_remote_stream_active, immediately after the
  existing local opening_candidates sort: move its first UDP candidate to the
  front without changing its ordinal, frozen return plan, other candidates or
  later scheduler. Guard by the existing lab-diagnostics compile feature;
  archive and reverse after freezing the executable. No product option or
  persistent runtime change. Keep server on the ordinary current executable.
  Both cells enable ONLY existing initial/open and additional-attachment events
  to verify actual first owner and fallback; no bulk or repair tracing. This
  narrowly supersedes diagnostics-off for setup identity only, and still cannot
  supply ordinary acceptance. Logs may contain later replacement setup too.
  Control that already starts on QUIC, failed intended QUIC open, missing
  required physical readiness, or different final membership limits the
  ablation; do not repeat to manufacture the desired contrast.
  The prior capture has all3TCP and physicalQUIC active143ms before its first
  OriginalData, so the91ms delay there was logical attachment, not cold native
  establishment. The fixed1s runner wait is not a readiness barrier; check
  both new cells' initial snapshots and retain their handshake costs.
- **Frozen, 04:07:** build2m03s; experimental executable
  `./.tmp/reflection/bin/initial-quic-owner-ablation-20260908/mptunnel`.
  Ten-line one-file overlay independently reviewed, archived as
  INITIAL_OWNER_ABLATION_20260908.patch and fully reversed before execution.
  Server stays on ordinary ack-atoms-20260908 in both cells. Labels
  `mixed-combined-up-initial-owner-{control,quic}-0908`; setup event filter
  `reliable_stream_open_attempt,reliable_stream_open_success,reliable_stream_open_timeout,relay_additional_path_open_spawned,relay_additional_path_open_attached,relay_additional_path_open_failed`.
  Independent review notes the existing last-alternative timeout follows
  attempt order: a failed QUIC then successful TCP is not the intended contrast.
- **Environment-invalid cell, 04:08:** control12182 hit its existing guard;
  client.log13 identifies the actual problem: the owned pinned test certificate
  expired at2026-09-07 19:49:30UTC, before this run. QUIC could not attach, so
  this is NOT a mixed comparison or an MPP throughput regression. Products and
  probe are stopped; retain raw invalidcell instead of deleting or promoting it.
  Renew only the owned certificate with the same key, CN/SAN localhost and
  critical CA:FALSE, valid30days to cover this continuing lab task. Both next
  cells use the same renewed certificate; verification remains enabled.
  Check validity and actual native/setup readiness. The previous membership
  capture at19:30UTC preceded expiry and remains valid. New pair labels
  `mixed-combined-up-initial-owner-valid-{control,quic}-0908`; this repeats an
  invalid environment attempt, not an adverse valid result to seek a pass.
- **Measurement:** complete confirmed bytes, first/max gaps and full raw bins,
  startup/attachment and native service history, drain, directional wire and
  RSS/CPU. Initial handshake time must remain visible. A different first owner
  does not by itself prove an equivalent warm native state or selection bug.
- **Falsifier / stop:** similarly long early owner-blocked timing argues against
  initial TCP commitment as the sufficient dominant cause. Improvement confined
  to the early interval supports that mechanism only, not a QUIC-first default.
  Different random realizations limit causal strength. No third run-to-pass,
  automatic controller change, copying larger suffix or matrix expansion.
- **Acceptance:** this is a diagnosis ablation, never release or policy
  acceptance. A proposed placement revision must then state its cold/unknown,
  sole-path/high-BDP, UDP-QoS, discovery and shared-cut countercases before RED
  and code. Preserve existing T03/T04b constraints unless explicitly revised
  with replacement guarantees; do not silently restore a rejected shrink.

**Pair complete, 04:13:** valid control235,470,848B/53.153263s/35.440Mbps;
QUIC-first374,734,848B/49.660557s/60.367Mbps, both exact complete. First
confirmation worsens.417902->1.551972s; maxconfirmationgap5.697254->5.256939s;
maxwritegap9.670569->4.932933s. At4s ordered target7,667,553B versus81,312,587B.
This supports a material early initial-owner effect, not a sufficient fix or
QUIC preference. Both physical sets were warm and all four logical attachments
established. Intervention later records a TCP session-closed warning, without
an observed physical-identity replacement; retain that distinction.
Full series and cost: INITIAL_OWNER_ABLATION_20260908.md and its raw archive,
which also preserves the explicitly excluded expired-certificate cell.

**Separate return stage proven:** intervention4--6s target advances7,041,888B,
server reads sink replies171->262B, but client delivers no reply beyond67B.
At43--47s target advances9,473,237B to the complete total; replies1166->1347B
are read by server while client remains942B. Thus confirmation zeros are not
all forward-upload silence. At22--23s a different actual forward plateau has
64MiB source/target separation despite1,656,000B QUIC native ACK progress.
Existing snapshots do not identify exact holding ranges or native winners.

## Active transaction: response retained-frontier symmetry preflight

- **Issue:** the same mixed request has long return delivery stalls after the
  server has read small acknowledgement bodies, including outside deliberate
  QoS. This affects confirmation/interactive service, not only bulk estimates.
- **Question / competing causes:** response queue/native/client ordering may
  hold an already-published prefix; or response retained recovery still rejects
  current positive F when its historical complete-ACK horizon is behind.
  Source inspection finds the generic active tail still requires historical
  completeness/prefix equality, while its exact-owner helper is final-only.
  The request correction011aee9 changed this family; RFC15.2 now explicitly
  permits active retained-owner recovery independent of EOF or old completeness.
- **Smallest action:** independent source/history/producer review to determine
  whether a legal response ACK sequence can actually yield H<F at those gates.
  Check actual positive frontier, stored snapshot and callers; no full trace,
  runtime correction or claim that the captured67-byte stall has this cause.
  If reachable, construct one real response admission/ACK-ledger RED plus an
  aligned-H control before changing production. Existing request tests are not
  proof of symmetric response behavior.
- **Falsifier:** if response producer/state invariants require H>=F there, or
  another active retained fallback already reaches the same exact prefix and
  target, this proposed rejection is not a defect. Do not patch an unreachable
  branch or change freshness/quantum to manufacture eligibility.
- **Preservation / gate:** actualF and negativeH remain distinct; exact original
  age, copy/deadline/target identity, ranked range, qualification and Native
  admission stay. Component RED would prove a contract mismatch only, not
  attribute all return stalls. Symmetric mechanism controls and the affected
  ordinary timing/cost comparison still precede practical promotion.

Exact network/probe profile remains unchanged: upload70/20ms delay/jitter;
return30/5ms. Five-second loss epochs upload[3,8,5,6,10,3,5,8]% (mean6),
return[1,2,.5,3,2,.5,1,2]%; upload10Mbps at15--25s, UDP outage30--33s.
40s load, existing85s runner observation guard and90s probe completion boundary;
censoring is not complete throughput. Netem random realizations are not identical.
Single500Mbps is a configured link limit, not a per-confirmation-window bound.
Do not reuse the runner's old aggregate300/200 defaults for the required200each.

## Existing dispositions and attribution limits

- Retain demonstrated native reordered-packet/history corrections, exact
  qualification, mailbox-capacity wake, cooperative actor, equivalent
  live-owner frontier sweep, paired QUIC repair, stateless ACK, and exact
  terminal/reconciliation cleanup. CHANGE_DISPOSITION_20260907 and
  PERFORMANCE_REFLECTION_20260907 preserve origins, costs and adverse data.
- Static score prototype, stateful relative ACK codec, ready-feedback batching,
  absolute-delay reordering, wrapperless actor, 3N1 and isolated raw-byte
  hysteresis deletion remain rejected/removed. Do not revive by another name.
- Queued original waiting behind a native domain and accepted repair awaiting
  actual wire/peer service are distinct. One earlier repair was accepted in35ms
  but arrived2.737s later; publication is not delivery.
- Captured probe3 reached client mailbox before D but actor application followed
  expiry. RFC15.2 and `3d68243` intentionally fence effects at application.
  This observation does not license lengthening D, accepting a retired probe
  or relabelling ingress as effect. Actor delay's ordinary contribution remains
  unquantified; it is separate from initial ownership and the partial-copy fix.
- Finite1941 mixed requests plus64 single-mode controls reclaim owners with
  flat observed post-load RSS. Preserve restart/half-close/terminal semantics.
  The uncaptured deployed random RAM/CPU incident is not fully attributed.

## Global gates — unchanged, not satisfied by an isolated GREEN

| Order | Scope | Required evidence |
| --- | --- | --- |
| 1 | Mixed allocation, request/upload sampling, cold/warm startup | Exact cause, conditional model, real RED/control, independent audit, focused GREEN; ordinary first-body, gaps, loaded latency, complete bytes. |
| 2 | TCP/QUIC QoS, loss, jitter, blackhole and recovery | Both directions; same-request restart-free recovery. Separate native work, Product receipt and ordered application service. |
| 3 | Aggregation/shared contention | Single500Mbps, independent200Mbps each; shared cuts and asymmetric changing3--10%mean6 loss/jitter/QoS/outage combinations plus ablations. |
| 4 | Actual user experience | TCP, QUIC and default; cold/warm, single/concurrent and actual speed.cloudflare.com. Raw TCP, Xray and Hysteria2 under matched conditions, including failures and upload confirmations. |
| 5 | Sustainability | Restart/churn, backpressure, ownership reclamation, CPU/RSS and post-load recovery; reopen components only on contrary evidence. |
| 6 | Publication | Complete timing/latency curves, overhead and completion alongside goodput. README/PERFORMANCE and release only after practically competitive gates. |

## Execution and continuity

- Owned Docker topology only; no sudo, host shaping, outside-repo work or
  build/lab overlap. Products/probes are currently stopped; origins retained.
- Read method/current plan after compaction. Continue the one active transaction,
  not a new broad audit. Commit isolated evidence/model/code dispositions.
  No component-green => whole performance inference; no average hides stalls.
- Preserve useful artifacts before scoped cache cleanup. No deletion this
  transaction; no unaccepted runtime overlay or user edit belongs in a commit.
- Telegram milestones authorized no more often than hourly; last sent
  2026-09-07 about20:01UTC (initial membership and rejected repair fork).
  Respect its soft frequency advisory: nonessential messages can wait.
- Universal clairvoyant optimality under arbitrary future outages is impossible.
  This does not waive avoidable software delays or any practical acceptance
  gate. Never describe unfinished work as ideal or promise cost-free discovery.
