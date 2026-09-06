# Native reordering candidate — not accepted yet

Disposition: both sender-only variants are withdrawn from the production tree.
Their exact patches remain archived for controlled experiments. Component
success and higher averages did not close mixed stability or the newly proven
[receiver-history limit](QUIC_RECEIVE_HISTORY_DIAGNOSIS.md). Do not reapply a
variant as an accepted standalone fix.

2026-09-06. The time-only ablation sustained only0.579Mbps. In the preceding
native trace,1119/1153 declared-lost Data packets numbered>=100 were later
acknowledged as originals. Both packet and time triggers are implicated.
Packet numbers below100 are excluded from that ratio to avoid handshake-space
ambiguity in the temporary text trace. No change to compensation is justified.

## Superseded RACK-derived candidate

Use the evidence-adaptive reordering allowance from RFC8985 as a QUIC recovery
policy under RFC9002§6.1. This is an intended native policy improvement, not a
claim that the recommended QUIC starting values violate its RFC.

- Before authenticated late-original proof, retain existing packet and time
  loss detection exactly. No global larger initial delay or rate assumption.
- A late ACK of retained, unexpired loss evidence establishes reordering for
  its controller/path lineage. It does NOT prove that other packets in that
  recovery episode arrived, and does not undo congestion state by itself.
- After proof, packet-count alone no longer declares loss (RFC8985§6.2 step4).
  The allowance is `min(steps * min_rtt/4, smoothed_rtt)`. The first late-loss
  proof raises steps from1 to2, matching RACK's DSACK growth. Growth can happen
  once per flight: another increase requires proof about an original sent
  after the previous increase. A single ACK burst cannot inflate it repeatedly.
- Native loss deadline is the larger of its existing RFC9002 deadline and
  `latest_delivered_original_rtt + allowance`. This is a separate RACK delivery
  clock, updated by the newest original newly acknowledged in each ACK,
  including retained late originals even when the largest acknowledged packet
  number does not advance. Do not substitute QUIC's congestion RTT estimator:
  its largest-packet sampling rule need not include these deliveries. QUIC's
  RTT, ACK-delay handling, pacing and dashboard latency remain unchanged.
  Native timer scheduling, PTO,
  ECN, persistent congestion and all controller callbacks retain their owners.
- An increased allowance persists for16 recovery completions without renewed
  late-original proof, then returns to the one-quarter-minimum-RTT allowance,
  as in RACK. This ages adaptation by recovery evidence, not an arbitrary
  wall-clock timeout or offered-rate estimate.
- Store only a small fixed-size state per native path: proof/growth boundary,
  bounded multiplier, recovery completion boundary and persistence count.
  No new per-packet payload retention, wire field, user knob, packet copier,
  pacer, congestion window or MPP admission threshold.
- New network paths start untrained. Same-lineage port migration/rollback
  preserve learning; unrelated epochs must not train the replacement path.

## Symbolic guarantees and unavoidable tradeoff

With `B=latest_delivered_original_rtt` and `0<=A<=srtt`, learned loss delay is at least
the existing delay and at most `max(existing_delay, B+srtt)`. It is not an
unbounded wait. The QUIC PTO mechanism is not replaced by this deadline.
No proof implies identical starting behavior. One ACK of a declared-lost
original changes only reordering evidence, never the truth of other loss/CE.
The flight boundary prevents a packet burst from acting like many learning
rounds. Fixed-size state excludes lifetime-growing history.

This can defer *genuine* non-tail loss after a path has exhibited reordering;
no causal detector can eliminate both false loss and waiting without further
information. The practical requirement is fewer spurious recoveries and better
application speed/latency, with effective real-loss, blackhole and restoration
behavior—not merely a higher average. The RFC-derived16/4 values are not
selected to pass this lab. Changing them is outside this candidate.

## Gates

First: focused default/evidence/flight-boundary/aging/expiry/epoch/rollback and
real packet-pair tests, then native suite. Second: ordinary-build jitter-only,
loss-only, combined loss/QoS/blackhole and recovery comparisons on both MPP
QUIC and mixed. If the causal experiment has no practical benefit, remove the
candidate before widening the matrix. No public performance or release claim
until the affected gates pass. TCP, deep-queue N1 and browser/churn gates remain
in the global plan, not silently waived by this candidate.

Acceptance is conjunctive: correct ownership/model, useful throughput across
the complete series, bounded first-byte/read gaps, loaded latency including
failed/missing probes, recovery without restart and sustainable resource use.
A larger bulk average does not compensate for worse service outages or latency.
Compare these outcomes under the same conditions and ablations, not a selected
peak or an overall average that hides collapse.

## First candidate result and correction

The first implementation substituted QUIC's `max(latest_rtt, srtt)` for RACK's
delivery clock. This was not equivalent to RFC8985 section6.2 step2/step5: its
delivery sample includes newly delivered older segments even when the forward
ACK does not advance. Under reordering, selecting only new-highest packet
samples can favor the fast tail. The separate delivery clock corrects that
translation error; it is not an increased multiplier or bandwidth prior.

Before that correction, two routed jitter-only runs sustained12.222/11.825Mbps
versus0.614Mbps ordinary, but mixed sustained59.220Mbps with substantial swings.
All50 echoes succeeded in each candidate run. One native sample still showed
18.36% declared loss and a67ms smoothed RTT on the100ms nominal path. These
results establish practical benefit of detector adaptation, not acceptance or
proof that every remaining slowdown has the same cause.

With the separate delivery clock, QUIC sustained12.343Mbps, not a material
increase; mixed averaged84.747Mbps but had a4.004s read gap and30 failed echoes.
An ordinary mixed control averaged42.101Mbps with a1.277s gap and no failed
echoes (one timeout followed by29 unavailable slots). The candidate remains unaccepted; an average-speed gain cannot justify
those service failures. Native trajectory analysis must separate continued
false loss from persistent consequences of an early reduction before any
further model change.

## Observed-deadline model — proposed before implementation

The trace resolves that question: with no injected drops, the capped candidate
continues declaring about1,000--1,250 packets lost per five-second interval.
Its multiplier grows to170, but the average deadline remains102--107ms. Many
originals arrive later. The cap prevents learning from counterexamples. A unit
counterexample also fails: an original acknowledged after180ms cannot raise
the next deadline above180ms. The exact rejected candidate is archived in
`QUIC_REORDERING_RACK_CANDIDATE.patch`; it is not a production recommendation.

Replace, do not layer onto, that policy:

1. Let `D0` be the unchanged native RFC9002 loss delay. Let `L` be the largest
   observed elapsed time of a current-lineage, unexpired, newly acknowledged
   original previously declared lost, plus one native timer granule.
2. Use `D=max(D0,L)`. Before proof, `L=0` and both native detectors are unchanged.
   After proof, do not let packet-count alone overrule this settling time.
3. An ACK cohort contributes its oldest newly acknowledged retained original,
   which supplies the largest elapsed time. A maximum cannot multiply a single
   compressed ACK burst into artificial learning rounds. No second RTT
   estimator, gain multiplier, absolute operator delay or rate prior is added.
4. Expiry happens before matching. Thus each learned observation is bounded by
   the existing two-PTO retained-evidence lifetime at observation, plus timer
   granularity. A new path epoch cannot use old evidence. Same-lineage port
   migration/rollback keeps it. No per-packet storage growth is added.
5. As in RFC8985, retire adaptation after16 completed recoveries without fresh
   late-original proof. This is evidence aging, not a permanent maximum or a
   wall-clock guess. Native timers, real-loss callbacks, ECN and persistent
   congestion stay authoritative; the new delay can defer true-loss declaration.

Proof obligation: if the native default does not become larger and the next
original has the same observed delivery delay, its learned deadline is later
than delivery by at least one timer granule. No amount of fast-tail RTT bias can
undo that learned fact. The max operation is idempotent for duplicate evidence.
No valid proof implies baseline behavior. Storage remains constant per path.
No claim is made that an unseen longer delay can be predicted, or that a late
original disproves other genuine losses. This is an experimental adaptive
absolute threshold permitted by RFC9002 section6.1.2, not a claim that RFC8985
itself mandates this replacement. The tradeoff is explicitly longer genuine
   loss detection after recent deep reordering; timing, restoration and mixed
service gates are required before accepting it.

Timer qualification: unchanged PTO code does not imply an independent PTO
deadline. RFC9002 section6.2.1 and Quinn's timer both give a pending time-loss
timer precedence over PTO. Therefore an observed delay learned during a deep
queue can defer real-loss recovery after the queue disappears, until aging
retires it. Do not silently add a parallel PTO (which violates that RFC rule),
or describe the retained-evidence bound as a current-PTO guarantee. The existing
QoS-downshift/restoration and partial-loss latency cases must reject a harmful
history effect even if jitter-only throughput improves.

References: https://www.rfc-editor.org/rfc/rfc9002.html#section-6.1 and
https://www.rfc-editor.org/rfc/rfc8985.html#section-6.2 .
