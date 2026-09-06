# Current deterministic closure plan

Updated: 2026-09-06 07:40 UTC. Baseline source: `7189e69`; evidence checkpoint:
`fb01c1a`. This is the active continuation of REVIEW_AND_PRACTICAL_ACCEPTANCE,
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
- Next experiment: existing routed mixed/QUIC candidate versus control,
  QoS-only and QoS-with-jitter/loss ablations, using existing management
  snapshots alongside receiver gaps and router dequeue counters.
- No production patch or new policy before attribution. Independent audit
  workers remain unavailable under their recorded usage limit; no substitute
  independent sign-off is claimed.
- Persist every verdict, exact executable/configuration, series and next owner
  in PROGRESS and committed docs-dev. Preserve evidence across compaction.
