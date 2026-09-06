# Upload feedback lag after the paired repair experiment

2026-09-06 15:38 UTC. Focused continuation, not an accepted runtime fix.

## Practical result and exact boundary

The first ordinary paired-repair download improves maximum gap2.689s to.384s,
but this cannot accept the candidate. Its first ordinary upload completes
exactly211,419,136bytes in63.487s with a32.816s confirmation gap. The control
also fails to drain before the existing85s observation guard. Its subsequent
EOF is caused by owned-product teardown and is not a spontaneous close proof.

The first diagnostic upload completes at225.627Mbps. It does not reproduce
the long source stall; server delivery gaps peak.415s while returned sink
confirmation gaps peak3.975s. Those two clocks must not be equated. The second,
lighter trace reproduces28.056Mbps with24.222s confirmation gaps and a49.392s
actual server delivery gap. Both traces use the same500/500Mbps asymmetric
variable-loss/jitter setup without intentional QoS or outage. Different random
realizations and observer overhead mean these are attribution, not ranking.

Every original assignment is contiguous, nonoverlapping and covers the exact
eventual target total in each trace. In trace2:

| Unix ms | Observation |
| --- | --- |
|1788705653948|Last original before the stall: QUIC0,208774253..208786253.|
|about20--60s in management series|Server Product target counter remains208786253.|
|about15--40s relative to first client ACK|Client processes mostly complete ACKs whose greatest end remains145996301; retained unacknowledged bytes stay near64MiB.|
|1788705703272|Next original is assigned to TCP1 at208786253..208791789, after49.324s without new original assignment.|
|1788705703426|Server delivery resumes,155ms after that assignment.|

During that pause, TCP carries stale-owner repairs for ranges below the
server's already-delivered prefix. Native QUIC service slowing is therefore
not alone evidence of unavailable path capacity: the next new original was
not assigned for most of the server's gap. This does not yet establish where
the latest Product feedback stopped or whether source read permissions were
correctly held by genuine outstanding-resource ownership.

The next discriminator separates receive-state publication, actual native
ACK-frame handoff, native receive, and Product ACK input. Do not change BBR,
raise a repair limit, erase retained data, or move more control to another
stream without identifying that boundary first. The broader download FIFO
finding remains valid; this upload failure does not accept or disprove its
independent-ordering capability.

## Evidence and next action

- REPAIR_COMPANION_COMPARISON_20260906.json: ordinary download series/tails/RSS.
- REPAIR_COMPANION_UPLOAD_EVIDENCE_20260906.json: both ordinary upload outcomes
  and complete1Hz source/target/native/Product/process observations.
- REPAIR_UPLOAD_FEEDBACK_TRACE_20260906.json: both diagnostic probes, exact
  source-gap edges, coverage verification, ACK windows and server gap events.
- REPAIR_UPLOAD_ACK_STAGE_DIAGNOSTICS.patch: read-only opt-in stage tracing.
  The current quic_read site observes only buffered decoder output, not the
  zero-copy decoder. Absence at that site cannot prove absent native receipt;
  positive observations and write/production stages remain usable.

## Subsequent stage results

The partial-decoder trace completes157.707Mbps with6.261s confirmation gaps;
server actual delivery gaps peak.536s. Publication and native write acceptance
advance together, while Product input trails for many seconds. Full probe and
two-second stage series are in REPAIR_UPLOAD_ACK_STAGE_EVIDENCE_20260906.json.

The all-decoder trace completes206.492Mbps with3.962s confirmation gaps. Exact
single-range ACK contents take up to9.375s from server native handoff to client
decode. The follow-up reader-queue trace completes245.798Mbps and again finds
5.333s handoff-to-decode delay. Its maximum matched decode-to-reader-queue wait
is14ms, queue-to-ordinary actor305ms, and decode-to-earliest Product input634ms.
Product matching may observe the same contents via TCP first; it is not exact
QUIC-copy attribution. There are no nonterminal writer-barrier events in this
follow-up. Records are in REPAIR_UPLOAD_ACK_RECEIVER_EVIDENCE_20260906.json.

These runs weigh against a local mailbox-only explanation for the multi-second
delay. Neither a paired-task scheduling defect nor native loss-policy defect
is accepted from these observations. The next discriminator maps the ACK batch
to exact native accepted/unsent/contiguously-ACKed offsets and observes the
held native reordering candidate's learned deadlines. That candidate may be
part of the problem and must not be protected from the same causal standard.
No scheduling preference, queue limit, congestion gain or deadline is changed.

## Native receipt separates the remaining delay

REPAIR_UPLOAD_NATIVE_RECEIPT_EVIDENCE_20260906.json captures a decisive witness:
ACK contents `[0,159705431)` are accepted at Unix1788707715368ms in native
stream4, batch offsets470108..470182. At1788707715694653us the native contiguous
ACK prefix reaches470537, covering the whole batch. MPP decodes those contents
at1788707759793ms:44.425s after handoff and44.098s after confirmed native receipt.
The largest observed native loss deadlines are.376s client and.389s server.
Do not attribute this delay to native loss policy, network QoS or a BBR gain.

Client CPU stays near one full core while RSS rises from343580KiB at5s to
505648KiB at50s. Server load declines while already-delivered Product bytes
await processing at the client. These are lifetime-average CPU samples and
live RSS, not a proven leak or CPU-hot-function profile. The long native delay
is ruled out for these exact batches; processing cost remains to be attributed.

Correction to the earlier queue interpretation: small *individual enqueue
waits* do not exclude long accumulated FIFO backlog age. Repeated40ms service
waits across a thousand accepted ACK records can age later records by40s.
The native-offset witness, not per-frame queue-wait maxima, identifies which
side of native delivery owns this delay. A sampling/aggregation error must not
be turned into a false protocol or congestion correction.

Next use aggregate synchronous-owner timings with all per-frame logs disabled.
Profile the ACK transaction, ACK-derived recovery evaluation and path recovery
pass, retaining ordinary-build controls. Distinguish algorithmic work from
observer overhead. No gain/queue/timeout adjustment or unvalidated ACK dropping.

Diagnostic binaries and raw records remain under .tmp/reflection/. Wider
acceptance is paused at this failure; no release or runtime acceptance.

## Quiet owner profile reproduces the failure

Per-frame diagnostics and native traces are disabled; existing one-second
aggregate timers alone are enabled. Two independent realizations retain the
same500/500Mbps asymmetric variable loss/jitter topology and no deliberate QoS
or outage. REPAIR_UPLOAD_OWNER_PROFILE_EVIDENCE_20260906.json saves both full
probes and synchronous component timing series.

| Observation | First profile | Repeat |
| --- | ---: | ---: |
| Exact completed upload Mbps |230.989|45.407|
| Confirmation gap seconds |2.064|14.616|
| Profile lifetime seconds |44.039|46.699|
| Path recovery total seconds |3.371|30.639|
| Within it, recovery enqueue seconds |2.374|29.233|
| Product ACK transaction seconds |5.560|2.418|
| ACK-derived recovery evaluation seconds |7.136|1.756|

The scopes nest; these totals cannot be summed as CPU. No single call exceeds
12.542ms in the failed repeat, so accumulated processing/backlog, not a single
blocking function, remains the interpretation. The repeated recovery enqueue
operation dominates the failed run and is the next bounded owner to split.
It currently extracts ranges, selects a target, rebuilds payload frames and
linearly checks each against queued repairs. Establish which suboperation
dominates before optimizing it; merely suppressing dirty wakes or treating all
zero-byte ACK outcomes as unchanged would alter valid recovery semantics.
