# Request live-gap service after the overlap-cost correction

2026-09-06 16:07 UTC. Active discriminator, not a new accepted fix.

The ordinary second candidate upload is127.882Mbps with4.582s confirmation
gaps. Between5--11s the target advances only about0.99MB. The last TCP-owned
original prefix shrinks524288 to57416bytes, while QUIC native delivery has
already acknowledged over151MB and reports12--18MiB available native window
with only12--44KB native flight. CPU declines during this interval. This is
not the same observed CPU-heavy34s overlap loop; it is the existing ordered
mixed-path/recovery issue exposed after that bounded correction.

## Next exact question

For each accepted request live-gap repair, record the actual first gap,
owner-uniform extent, common and target quantum, target service, accepted
bytes, owner assignment age/deadline, and selected target. The temporary
`request_ack_gap_commit` event fires only on actual accepted recovery batches.
Existing original/repair dispatch, staleness, and exact-admission events can
connect it to native and target progress. No new policy is installed.

Current `adaptive_reliable_relay_reinjection_bytes` maps Throughput to Latency;
that latency quantum's floor and cap meet at PATH_OPEN_SCORE_BYTES, normally
14,600bytes. T06 then preserves the ranked quantum through Apply. If successive
frontiers require separate feedback cycles of durationR, recovery of a large
hole has the conditional ceiling8*14600/R bits/s:1.168Mbps at100ms,0.584Mbps
at200ms. The trace must establish whether this serialization is what the live
case actually does. A native window is not proof of current delivered capacity.

The generic snapshot used for the common quantum comes from the relay's
lowest-ETA view; it is NOT the incoming ACK carrier's identity. Do not make a
directional-ACK attribution unsupported by the call chain. In the current
default latency-quantum calculation, rate changes do not escape the14,600-byte
geometry anyway.

## Preserve the already solved defect

bfac5b8/T06 fixed a real score/Apply mismatch:162 decisions ranked2.365MB but
published266.316MB of repair,112.6times the ranked extent. It was intended as
a bounded live-owner latency hedge, with separate stale/failure recovery.
Its exact-range, configured-slot and immutable-cause protections must remain.
Merely increasing14,600 or restoring whole-target-window publication would
repeat the old error and is not an acceptable fix.

If the trace proves the conditional service ceiling, the next model must
separate native command fairness quantum from an explicitly ranked recovery
extent and its feedback/copy lifetime. Prove the actual extent before Apply,
preserve same-range copy suppression and native credit, and consider shared
bottleneck amplification, slow-but-progressing owners, reversed path quality,
unknown capacity, and both directions. Do not implement a broader recovery
model until those invariants and the observed failure are connected.

All wider CURRENT_CLOSURE_PLAN gates remain open. The complete ordinary
comparison is in REQUEST_RECOVERY_OVERLAP_COMPARISON_20260906.json. No new
README result, release acceptance, congestion gain or protocol preference.

## Follow-up attribution, 2026-09-06 16:28 UTC

The first `live-gap-service-0906` diagnostic is incomplete: an application
reset ends it at 19.872 s, with 191,919,271 target-confirmed bytes versus
277,741,568 locally accepted. Its 822 accepted repair events establish the
14,600-byte geometry, not completed throughput or the initiating close owner.
Client reports `reliable path session closed`; subsequent server H3_NO_ERROR
alone cannot identify why the Product relay ended.

Five follow-ups did not reproduce that reset. All completed with exact target
confirmation, but gaps remain unacceptable. They are diagnostics, not release
measurements:

| Suffix of `mixed-combined-up-` | Deliberate QoS/outage | Mbps | Max confirmation gap s | Total s |
| --- | --- | ---: | ---: | ---: |
| `live-gap-close-0906` | Off | 176.011 | 3.948136 | 58.996587 |
| `live-gap-close2-0906` | Off | 209.406 | 4.737695 | 41.065452 |
| `output-unavailable-0906` | Off | 204.623 | 3.487076 | 42.460993 |
| `output-unavailable-combined2-0906` | On | 197.119 | 2.808189 | 41.284680 |
| `output-unavailable-original-events-0906` | Off | 190.013 | 11.205387 | 45.096797 |

All retain routed 500/500 Mbps service and asymmetric variable loss/jitter.
The last follow-up restores the first run's per-dispatch/ACK diagnostics as
well as terminal events; logging changes timing and these means must not be
compared as performance improvements. Raw probes, logs and one-second service
observations remain under `.tmp/reflection/results/` with the names above.

The `request_output_unavailable` event establishes that a previously selected
persistent-repair target can become stale before dispatch while every carrier
remains active. Existing dispatch correctly cancels that exact queued batch;
both traced occurrences complete normally. Do not patch that expected branch.
Another hypothesis is ruled out statically: absent measured alternate
completion produces no ready cause deadline, so it cannot newly enqueue the
speculated unbound ACK-gap fallback through this evaluator.

The initial reset remains unattributed, not fixed or excused as instrumentation.
The current narrow lifecycle check tests FIN selection when all still-live
attachments have stale payload evidence. Existing coverage checks a remaining
fresh output and a sole stale OriginalData fallback, not that terminal case.
The bounded FIN correction is subsequently proven and committed in `8e27abb`;
it does not attribute the initial reset or change live-gap recovery policy.

One attempted run failed during router setup, before any product started:
`tc qdisc replace` attempted to change an existing HTB root. The shell used
`grep -q` under `pipefail`; early consumer exit can make the producer fail and
skip deletion. The local setup check now consumes its complete input. This is
not an MPP failure, and that setup-only attempt has no performance result.

## Boundary refinement, 2026-09-06 16:58 UTC

Do not conflate the first-gap quantum with every confirmation gap. Many
consecutive trace frontiers advance by 65,536 bytes, exceeding the 14,600-byte
repair and establishing concurrent native progress. The 11.205-second gap in
the last diagnostic includes continued server target writes and reply reads;
its client return stream is stalled. Ordinary healthy uploads reproduce a
long reply tail after every upload byte reaches the target, without per-frame
diagnostics. REQUEST_FEEDBACK_DRAIN_WORK_MODEL owns the next focused CPU/stage
discriminator. Live-gap service remains unresolved, not a reason to enlarge
the ranked repair extent or to call the return-stream delay a probe artifact.
