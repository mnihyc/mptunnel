# Upload feedback lag after the paired repair experiment

2026-09-06 14:49 UTC. Focused continuation, not an accepted runtime fix.

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

Diagnostic binaries and raw records remain under .tmp/reflection/. Wider
acceptance is paused at this failure; no release or runtime acceptance.
