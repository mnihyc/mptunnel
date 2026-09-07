# Pruned composition: release comparison and remaining timing failure

2026-09-07 UTC. Finite ordinary observations, not release acceptance.

The requested pruning did not merely preserve compilability. In the existing
changing-loss/QoS/outage profile, the pruned composition substantially improves
delivered bulk work and finite upload completion over the recorded v0.4.8
control. It has not established stable mixed-carrier experience.

| Same-profile cell | Released control | Pruned composition |
| --- | ---: | ---: |
| QUIC download, observed Mbps | 0.477 | 95.238 |
| QUIC maximum closed read gap | 5.587 s | 4.922 s |
| Mixed download, observed Mbps | 3.968 | 93.139 |
| Mixed maximum read gap | at least 9.008 s; unfinished | 3.888 s; delivery resumes |
| Mirrored mixed upload | incomplete at 85-s guard | all 676,265,984 B confirmed in 44.774 s |
| Completed upload Mbps | unavailable | 120.831 |

The released upload confirms only 59,965,175 of 114,622,464 locally accepted
bytes. Its final sink-close error follows test teardown; it is not proof of a
spontaneous protocol reset. The candidate's 6.559-second confirmation gap is
not automatically an equally long target-write gap: receipts can be delayed.

Both QUIC echo timeouts coincide with the deliberately complete three-second
UDP outage. The pruned QUIC echo survives the ten-second 10-Mbps restriction;
successful echo times return to approximately 72–152 ms in the following
five-second restored phase. Mixed instead times out during QoS. Disconnected
schedule slots are not additional failed requests, and success-only overall
p95 is not a fair comparison when the successes cover different phases.

## Baseline context and next exact owner

Unchanged raw TCP and Hysteria2 controls deliver 4.138 and 8.371 Mbps. Raw
completes 80/80 echoes with a 0.407-second maximum bulk gap. Hysteria2 has a
21.515-second closed gap beginning before QoS and one echo timeout during QoS.
Hysteria2 has explicit 500-Mbps upload/download configuration; MPP uses its
current dynamic discovery. These single realizations do not prove a general
ranking, nor license changing the competing baseline until it looks better.

Mixed's average nearly matches QUIC, but its pre-QoS series still alternates
small delivery intervals and large catch-up bursts. That is the next exact
ordered-prefix attribution target, using already-present dispatch/rank/mux
events on the unchanged executable. Physical queue work is measured separately.
No new BBR gain, priority, queue bound or allocator is justified from an average
rate, a cumulative loss number, or the mere presence of an outage-time gap.

## Evidence boundaries

- [Six-cell record](PRUNED_RELEASE_COMPARISON_20260907.json): all six probes,
  293 compact management/router samples, original errors, invocations, actual
  shaper transitions, full curves and phase summaries. Independent review
  recomputed the six totals and thirty phase summaries against raw files.
- [Baseline context](PRUNED_BASELINE_CONTEXT_20260907.json): raw/Hysteria
  probes, 82 samples and phase-specific queue context. Native network service,
  application release and upload confirmation use distinct clocks. Individual
  receiver/confirmation bins above a link rate are not faster physical service.
- Control lineage is the existing frozen `d1a99ad` artifact; candidate runtime
  checkpoint is `b9a1600`. Both print 0.4.8, so the version string alone is not
  identity proof. Diagnostics are off in these ordinary cells.
- Both use 500-Mbps directional cuts, 100-ms base RTT with asymmetric jitter,
  forward loss epochs 3/8/5/6/10/3/5/8 percent (mean six), a 15–25-s 10-Mbps cut
  and a 30–33-s UDP outage. Upload mirrors the whole directional profile.
  Same schedule does not imply identical random packet histories.

This supports a bundled-composition improvement in the measured profile, not
the contribution of each commit or all-use-case non-regression. The previously
proven finite terminal-owner cleanup remains a separate gate. Independent
200-Mbps aggregation, TCP-only recovery, broader directional/combined ablations,
browser/Cloudflare and full baseline curves still precede any release verdict.
