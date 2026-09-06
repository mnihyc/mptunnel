# Current deterministic closure plan

Updated: 2026-09-06 11:18 UTC. Baseline source: `7189e69`; evidence checkpoint:
`282b71f`. This is the active continuation of REVIEW_AND_PRACTICAL_ACCEPTANCE,
not a new SEEN/UNSEEN inventory. No release is accepted yet.

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
- Native outage trace shows ordinary exponential PTO backoff, not a stuck
  timer in that capture. Physical queue drain, native reordering tolerance and
  Product-prefix stalls remain separate causes; do not collapse them into the
  newly identified qualification defect or waive the other gates.
- No new policy before attribution. Independent audit
  workers remain unavailable under their recorded usage limit; no substitute
  independent sign-off is claimed.
- Persist every verdict, exact executable/configuration, series and next owner
  in PROGRESS and committed docs-dev. Preserve evidence across compaction.
