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
