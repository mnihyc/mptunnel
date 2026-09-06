# Current deterministic closure plan

Updated: 2026-09-06 15:38 UTC. Baseline source: `7189e69`; evidence checkpoints:
`282b71f`, `5d52914`, `9f15ffd`, `c44ecee`, `5e604d1`, `efa8181`. This is the active continuation of REVIEW_AND_PRACTICAL_ACCEPTANCE,
not a new SEEN/UNSEEN inventory. No release is accepted yet.

## Active transaction — 2026-09-06 15:38 UTC

The historical execution entries below are evidence, not simultaneous tasks.
Latest owner: native FIFO obstruction is PROVEN and committed in e99694d.
The bounded companion candidate is integrated but UNACCEPTED. Three actual
Quinn ordering/credit tests, queue-transfer/binding checks, 302 carrier tests,
53 codec tests and the actual bidirectional companion/half-close/sibling test
pass. Functional root tests used root-only opt0 after opt3/opt1 test builds
were SIGKILLed; the compared executables are ordinary optimized release builds.
First mixed loss/jitter down comparison: control141.918Mbps/gap2.689s versus
candidate155.674Mbps/gap.384s, no failed interactive requests. Candidate still
has uneven delivery and a1.015s interactive maximum (control.685s), so this
does NOT establish non-regression. Full series/RSS are saved separately in
REPAIR_COMPANION_COMPARISON_20260906.json. The next upload comparison is
UNACCEPTABLE: control fails to drain before the existing85s runner guard;
candidate completes211,419,136bytes in63.487s with a32.816s confirmation gap.
Do not rank the control's149.007Mbps lower-bound/teardown result as a complete
transfer. Candidate26.641Mbps is not accepted. Native pair-credit cancellation
passes an actual low-limit test. Wider comparisons pause at this observed
failure: trace client original/repair assignment and exact server prefix,
distinguishing Product accounting, native service and target ACK return.
The lighter trace now proves49.324s without new original assignment while the
server has already delivered208.786MB and client feedback lags near146MB.
REPAIR_UPLOAD_FEEDBACK_BOUNDARY owns the next exact ACK-stage discriminator;
do not reinterpret the absent next assignment as native loss of that byte.
Subsequent ACK-stage traces locate5--9s before client decode, with millisecond
reader-queue handoff and subsecond Product handling in the latest follow-up.
That discriminator is complete: an exact ACK batch is contiguously acknowledged
natively44.098s before client MPP decode, while observed native loss deadlines
remain below.4s. This exact delay is client processing, not native recovery.
Small per-frame queue waits cannot exclude accumulated FIFO backlog age; the
earlier queue inference is corrected explicitly. Quiet aggregate profiles then
give230.989Mbps/gap2.064s and45.407Mbps/gap14.616s. Recovery enqueue processing
grows from2.374s to29.233s, against a46.7s second run; Product ACK transaction
cost is only2.418s there. Nested scopes overlap and must not be added.
Next split recovery frontier lookup, target selection, frame extraction and
queued-overlap/enqueue cost before selecting the algorithmic correction. No
ACK thinning, congestion gain, copy-budget, dirty-wake or timeout change is
justified yet. Exact cost, original intent and preserved semantics must lead
RED/GREEN and ordinary comparisons. Native receipt and owner-profile evidence
are committed separately; all profile rates remain diagnostic only.
REPAIR_COMPANION_UPLOAD_EVIDENCE_20260906.json preserves both outcomes and
1Hz native/Product/RSS series. No native gain, queue-cap or protocol-preference change. The simple
response ECF rollback is already rejected; do not repeat it.

1. COMPLETE for download repair and upload native boundary; ACTIVE for upload processing: read-only native offsets distinguish accepted, first-unsent and contiguous
   acknowledged bytes of the exact H3 stream. Map its first repair record to
   native offsets and to Product receipt. No controller, threshold, writer
   credit or topology change. This resolves whether repair service is blocked
   before transmission or only by native/receiver ordering.
2. Specify the smallest allocation/repair change that addresses that measured
   boundary. State independent permission, placement, discovery and committed
   native-work owners. Prove singleton, unknown-path, preferred failure,
   direction/identity, concurrent ownership and high-BDP counterexamples
   before runtime policy changes; revise affected RFC sections explicitly.
3. Component RED/GREEN then ordinary before/after timing comparisons close
   this transaction, or explain a failed candidate and remove its policy.
   Do not declare all issues fixed based on this component.
4. Continue the existing fixed order below through both-direction timing,
   aggregation, browser, sustainability and final baseline comparisons.
   Preserve500-Mbps single cuts,200-Mbps independent cuts and asymmetric
   impairments. No release until material identified deficits are resolved.

Only new evidence needed for those existing issues enters this batch. An
unsupported theoretical concern is not another production fix. Audit workers
remain unavailable; root analysis and tests do not substitute for independent
sign-off. Intermediate evidence commits preserve exact unresolved owners.

## Fixed order and closure obligations

| Order | Identified issue | Required evidence / disposition |
| --- | --- | --- |
| 1 | QUIC reordering and mixed QoS-history delivery gaps | Separate router service, native reliability and Product ordered progress on one timeline. Compare QoS-only, jitter-only, loss-only, isolated outage and combined history. Preserve duplicate safety, actual loss recovery, bounded history and existing lifecycle fixes. The archived excess-delay composition is an ablation, not an accepted baseline. |
| 2 | TCP/mixed timing, cold/warm startup, upload and return to recovered carriers | Both directions, short and sustained requests, source eligibility versus actual native delivery. No hard estimated-rate admission or fixed QUIC/TCP preference. Close with same-request recovery, first-body delay, gap series and loaded latency as well as useful bytes. |
| 3 | Shared/independent aggregation and asymmetric failures | Real routed independent cuts versus shared aggregate service, protocol/path ablations and combined disturbances. Never add carrier estimates as physical capacity. |
| 4 | Browser/Cloudflare instability and concurrency | Actual browser and local repeatable short/concurrent workloads alongside single-stream transfer. Preserve failures in the record, cold/warm distinction and upload confirmation semantics. |
| 5 | Sustainability and existing restart/retention branches | Exercise churn, backpressure and server restart with live ownership, CPU/RSS and post-load recovery. Keep proven journal/phase/close corrections; do not claim the uncaptured deployed RAM incident fully attributed. |
| 6 | Final matched acceptance and publication | Repeat affected healthy/adverse cases against raw TCP, Xray and Hysteria2; all three MPP modes and both directions. Publish current time series and latency, not selected best averages. Release only after material identified gaps are closed. |

For each issue: inspect current owner and introduction intent -> causal
counterexample -> dimensional/lifetime model and explicit assumptions -> exact
RED/GREEN -> affected ordinary end-to-end comparisons -> isolated accepted
commit or rejection. A component pass never closes the global gate.

No theoretical promise of clairvoyant optimality or zero service during an
all-path outage. Measured physical queue-drain bounds are recorded separately
from avoidable software stalls. New unrelated hypotheses stay outside this
batch until evidence and user scope justify inclusion. Already disproved items
and closed component invariants are not repeatedly reopened as new defects.

## Current execution

- User clarification: single-link nominal service is 500 Mbps in each
  direction; multiple independent links are 200 Mbps each. Keep directional
  delay/loss and temporary QoS asymmetric. Earlier 500/100 runs remain labelled
  historical diagnostics, not matched measurements for this new cohort.
- The initial QoS/jitter/loss/outage ablations and owner traces identify one
  exact response projection defect: native snapshots erase existing Product
  qualification, making faster QUIC fail the additional-output startup check.
  NATIVE_PRODUCT_QUALIFICATION_CLOSURE records the counterexample, original
  isolation intent, bounded correction and RED/GREEN obligations. Component
  proof is green, but its practical composition remains unaccepted. The next
  exact trace identifies a TCP-owned missing frontier, substantial stale-owner
  repair backlog and a native observation blind spot. Continue
  MIXED_RECOVERY_QUEUE_DIAGNOSIS before changing allocator or recovery policy.
- Adjudicated recovery refusals match occupied-copy/stale/queue state; do not
  remove those invariants to create apparent eligibility. The current RFC's
  immediately-admissible-action rule permits busy-fast/free-slow placement that
  can strand ordered progress. A model revision is possible, but the initial
  bounded-wait proposal fails unknown-capacity exploration unless it obtains
  a separate evidence/discovery owner. BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL
  explicitly records that pre-implementation constraint, not an accepted fix.
- Mixed still exhibits a 3.442-second gap without the deliberate QoS or outage
  (asymmetric variable loss and jitter retained). The matched QUIC-only case
  has a .507-second gap. Raw TCP, Xray and Hysteria2 controls are archived with
  their complete series; a poor baseline result cannot waive MPP's own gap.
  Next exact trace identifies the deferred TCP input kind during a long write,
  separating actual native delay from actor-level feedback obstruction.
- That trace now identifies Product mailbox pressure: TCP1 retains STREAM_ACK
  for 12.173 seconds until native write completion. Corresponding TCP and QUIC
  interlocks conflate mailbox capacity with an actor-ordering barrier. Next
  bounded transaction is MAILBOX_WRITE_WAKE_MODEL. Its production-interlock
  RED now turns GREEN, along with cancellation/closed-recipient checks, all
  297 carrier tests and 253 stream tests. The ordinary comparison remains
  unacceptable: mixed loss/jitter download still has sustained trickle, and
  two mixed-upload candidates give165--169 Mbps against237--260 Mbps controls.
  Endpoint isolation gives148 Mbps for old client/new server versus296 Mbps
  for new client/old server. The next trace covers every original byte and
  identifies pre-assignment gaps. Input-state tracing then captures2.633 s
  with a live source output, positive read budget and no sender retry blockage,
  but buffered input disables source reads and sender service. Next is the
  bounded/fair Product input-service model in MIXED_UPLOAD_SOURCE_GAP_DIAGNOSIS;
  preserve per-frame ACK validation, incarnation/lifecycle ordering and actual
  Product/native admission. Do not merely remove guards or tune a deadline.
  PRODUCT_ACTOR_SERVICE_MODEL now states cyclic ready service across input,
  dispatch and source reads, not a carrier-selection policy. The production
  arbitration RED retains the legacy empty-queue veto and selects Input six
  times despite all three classes being ready. Candidate removes that veto,
  preserves all three handler bodies and adds explicit RFC 10.4 separation
  from final-writer priority. Four service checks and240 other relay tests
  pass; one server FIN fixture fails before the asserted operation because its
  synthetic completed proof can be future-dated. A test-only timestamp
  correction awaits the next full verification; no production timing change.
  The first ordinary actor candidate is unacceptable:20.818 Mbps and61.501 s
  confirmation gap versus228.118 Mbps and2.280 s in its matched mailbox control.
  Wider comparison is paused. A second exact RED shows its synchronous dispatch
  bypassing exhausted executor budget when mpsc input asks to yield. The proof
  omitted executor fairness. The revision uses Tokio's existing cooperative
  boundary, not a new MPP budget or timer; five production-module tests pass.
  Ordinary revised binary is building; practical attribution remains open.
  The live trace measured resource eligibility, not the
  application socket's readable-byte count; do not overstate that observation.
  MAILBOX_WRITE_WAKE_MODEL and its full-series evidence retain the results.
  Preserve one retained frame,
  exact recipient, partial-write ownership and terminal/requalification
  barriers. Component success is not an isolated throughput improvement.
- Per-range recovery logging heavily perturbs the first upload traces; those
  rates are not acceptance numbers. Large line counts count range attempts,
  not actor iterations, and do not prove a busy loop or attribute the deployed
  RAM incident. Quieter gate traces preserve the source-read obstruction.
  Diagnostic fields are archived and removed from active runtime source.
- The cooperative revision's ordinary comparisons and796 affected tests are
  now complete. Mixed upload211.236 Mbps/gap3.125s, QUIC upload327.385/gap3.463s;
  the first actor candidate's61.5s gap does not recur in that run. Mixed
  loss/jitter-only download remains129.953/gap2.033 versus QUIC152.147/gap.659,
  both80 successful interactive probes. Combined cases still have long gaps
  and interactive failures. The cooperative client's40s RSS543148 KiB exceeds
  control335424 KiB. Keep all runtime candidates UNACCEPTED; do not use the
  fixed source-service invariant to waive ordered-frontier/resource concerns.
  The completed-proof fixture correction is test-only and independently green;
  it is not counted as a deployed performance fix. No additional native tuning.
- The next mixed loss/jitter traces establish a post-submission ordering
  obstruction, not absent QUIC service. An original frontier is assigned to
  TCP; its QUIC repair returns from H3 write in134us but first appears at
  Product receive2.376s later, after44.24MB of preceding QUIC Product payload.
  ORDERED_REPAIR_SERVICE_BOUNDARY records exact events, interpretation limits
  and the RFC boundary. Current15.1 immediate-admission and10.4 native
  nonpreemption can both be obeyed while producing this bad ordered outcome.
  Do not tune BBR, shrink buffers, force QUIC preference or erase copy slots.
  The next transaction is the allocation/discovery and irreversible-handoff
  contract; its unknown-capacity and reversed-quality cases must be settled
  before implementation. Two diagnostic-only hunks are archived and removed;
  no additional runtime behavior changed. Resource cost remains open.
  History review identifies65edae3/T04b's incomplete argument: resource
  permission invariance does not mandate immediate dispatch or establish
  ordered-latency non-regression. Keep exact resource separation, but replace
  the missing allocation choice explicitly; do not restore all old ETA/BDP
  gates or claim this single commit explains every previous regression.
- The response-only ECF restoration ablation is now complete and insufficient:
  ordinary136.161 Mbps/gap1.494s versus matched current135.348/gap3.523s, both
  80echo successes. Different random realizations do not establish a stable
  gain; both retain stalls/buffer-release bursts. A diagnostic confirms the
  restored predicate executes and still reproduces a TCP-owned hole with
  about60MB of reordered suffix. All ablation source hunks are removed; full
  series, patches and attribution are retained in ORDERED_REPAIR_SERVICE_BOUNDARY
  and RESPONSE_ECF_PLACEMENT_ABLATION_EVIDENCE_20260906.json. No simple rollback
  is accepted. Next work is the four-way permission/placement/discovery/repair
  service contract, compatible evidence and irreversible-work ownership proof.
  Revise affected RFC15.1/10.4/15.2 explicitly only after that proof; do not
  rewrite unrelated accepted ACK/lifecycle semantics or start another gain,
  queue-cap or protocol-preference sweep. Global gates above remain open.
- Native outage trace shows ordinary exponential PTO backoff, not a stuck
  timer in that capture. Physical queue drain, native reordering tolerance and
  Product-prefix stalls remain separate causes; do not collapse them into the
  newly identified qualification defect or waive the other gates.
- No new policy before attribution. Independent audit
  workers remain unavailable under their recorded usage limit; no substitute
  independent sign-off is claimed.
- Persist every verdict, exact executable/configuration, series and next owner
  in PROGRESS and committed docs-dev. Preserve evidence across compaction.
