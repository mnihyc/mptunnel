# Native QoS and recovery observation

Date: 2026-09-07 06:15 UTC. Category: bounded diagnosis; no runtime correction.

Evidence: `NATIVE_PTO_LATE_STARTUP_DIAGNOSTIC_20260907.json` contains the full
probe, both verbatim endpoint logs, exact invocation and41 compact snapshots.
The frozen executable is the pruned baseline plus opt-in observational hooks,
not the rejected hysteresis candidate. The hooks are archived in
`NATIVE_PTO_AND_ATTACHMENT_DIAGNOSTIC_20260907.patch` and removed from source.

## QoS stall is reproduced, not explained by a stopped ACK timer

The application has a9.859789s closed read gap from probe15.143337s to25.003127s.
Server native ACK/controller delivery continues through QoS; its14--27s
snapshots have PTO0. The native send counter remains66,161 at native21.05--24.06s
while flight drains from4,758,000 to1,402,800bytes, above a334,717byte window.
RTT reaches approximately4.57s. This is consistent with already-exposed work
draining after a rate cut, but does not identify which Product prefix blocks
application delivery or establish a native controller defect.

The diagnostic achieves43.166Mbps and one real echo timeout; these are not
ordinary-mode performance claims. No late-startup rejection or matching
mid-run physical retirement occurs. Thus that lifecycle warning is not a
necessary cause of this application stall. It remains a separate observed
issue in an earlier ordinary run.

## The subsequent outage has ordinary PTO recovery

The runner records blackout removal around Unix1788760777041ms. Its sampling
timestamps are not exact completion timestamps of the router commands.
Server probes142377/142378 emit at1788760778385ms, approximately1.3s later.
Their actual armed timer fires1.191ms after its deadline. Within291ms, two
additional live-containing ACK frames appear and PTO resets to0; retained-only
and no-known classifications do not increase. The client likewise receives
two live-containing ACK frames and resets PTO. Six earlier authenticated
server arrivals had no parsed ACK frames.

Exact ACK ranges are not logged. The evidence therefore shows live recovery
following the next scheduled probes, not that those two specific packet
numbers were acknowledged. It does not reproduce the rejected candidate's
earlier persistent post-QoS plateau. No timer shortening, alternate backoff,
or retained-ACK policy change follows.

## Reflection and next discriminator

The prior flat published ACK-byte counter was not proof of absent ACK packets.
This observation closes that ambiguity for this realization and rules out a
timer fix for it. It does not close the ordered-delivery issue. The next
existing-event capture must match original/repair dispatch offsets to the
receiver's lowest missing range and actual release. Native recovery and
Product ordering must remain separate until the same transaction connects
them. No additional parameter or model rewrite is accepted from these logs.
