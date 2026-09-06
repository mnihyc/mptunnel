# Feedback drain after bulk upload

2026-09-06 16:58 UTC. Existing bidirectional mixed-path timing issue; diagnosis,
not an accepted new algorithm or an unrelated issue inventory entry.

## Measured boundary

In `mixed-combined-up-output-unavailable-original-events-0906`, the client
receives 1,044 bytes of target replies by the 21-second observation, then none
until the 32-second observation. During that interval the server's target
writes advance from 677,149,291 to 729,947,307 bytes, and its target reads from
1,058 to 1,310 bytes. The upload probe's 11.205-second confirmation gap therefore
is not eleven seconds of zero target upload. It includes an actual return-path
stall. Neither redefining the measured rate nor ignoring small replies fixes it.

`ObservedProductIo::poll_write` records only successful target writes;
`poll_read` records successful target reads. This is application-I/O evidence,
not a bandwidth estimate. Server scheduling records reconstruct the next
14-byte reply's offset as 1,044: accepted for ordinary QUIC at Unix
1788712011459 ms, with its Product ACK observed at 1788712022423 ms. Original
response offsets are reconstructed from the complete ordered new-data record,
not inferred from repair records that omit offsets. The resulting 10.964 s
includes all native delivery, client handling and return-ACK stages; it does
not yet uniquely attribute the slow stage.

The ordinary healthy FIN candidate independently delivers all 1,112,080,384
upload bytes to the target by the 26-second sample. All 1,742 target reply
bytes are read there too. The client remains at 1,608 reply bytes through
39 seconds, then at 1,653 until about 45 seconds. Its Product outstanding
accounting drains from 66,529,449 bytes at 26 s to 14,404,329 at 44.9 s while
native flight samples are predominantly zero. Client CPU remains busy.
The old ordinary control also has a tail delay; its repeated run has an
8.429-second confirmation gap. Thus diagnostics alone are not causing it.

Sampled native flight is not an exact per-frame native ACK proof. CPU in `ps`
is process-lifetime average, not instantaneous. The evidence nevertheless
identifies the discriminating next question: is synchronous client feedback
processing consuming this drain interval after native transport has completed?

## Why the earlier quantum hypothesis is insufficient

The live-gap trace contains 822 accepted 14,600-byte repairs, but many next
frontiers advance by 65,536 bytes through native delivery. Only selected
intervals advance exactly one repair quantum. Small batches alone cannot
attribute all observed gaps. Changing their limit could reintroduce T06's
score/Apply amplification and would not address a delayed 14-byte reply.

The previous quadratic queue-overlap scan is already removed. Full request
flight-ledger rebuilding remains a separately measured cost in older profiles,
but those profiles predate that removal and do not establish its current share.
Do not call it the new dominant root cause without measuring the current build.

## Bounded next transaction

1. Keep FIN eligibility and all other ordinary behavior fixed. Add only
   temporary synchronous duration counters for the Product ACK transaction,
   exact flight release, path recovery and ACK-gap evaluation. Existing mux
   timing gives the unique-byte subcost. No per-packet trace is needed here.
2. Use the existing healthy 500 Mbps upload followed by complete drain; retain
   both-direction application I/O and process observations. Nested times are
   not additive. Compare cost during source activity and after source EOF.
3. If one computation owns the stall, prove an equivalent work-bounded model
   before changing it. Prefer deleting repeated reconstruction/scanning where
   possible; preserve every exact copy, qualification, ACK validation and
   ownership settlement. Otherwise remove the counters and follow the next
   measured boundary rather than patching that hypothesis.

The global closure gates remain unchanged. No performance acceptance, README
claim or release follows from this localization. The earlier unexplained
mid-transfer reset is retained separately and is not attributed to this trace.

## First profile rejects direct flight-cost attribution — 2026-09-06 17:03 UTC

The healthy diagnostic completes 1,008,140,288 bytes in 41.041 s with a
9.039-second reply gap. Flight ACK release totals 9.085 s inside the 9.275 s
Product ACK subtransaction. ACK-gap evaluation totals 1.945 s and path recovery
0.733 s. However, one-second intervals ending after 26 s contain only 0.380 s
of flight release and 0.396 s of that ACK subtransaction, while the reply tail
is still stalled. The old function-level label does not include the caller's
queued-repair pruning, authoritative ACK update, or staleness work.

Therefore optimizing flight rebuilding alone is not a demonstrated fix for
the observed post-source stall. Preserve it as a measured bulk CPU cost, not
the current causal conclusion. The next bounded profile includes the complete
ACK handler, queued-repair pruning and source-admission observation. No runtime
policy or ledger algorithm has been changed. The first profile's exact probe
and per-second counters are REQUEST_FEEDBACK_DRAIN_PROFILE_20260906.json.

The second healthy diagnostic completes in 41.237 s (191.067 Mbps, 4.198 s
maximum reply gap). Its full ACK handler is 8.823 s overall but only 0.270 s
in intervals ending at or after 27 s. Repair-queue pruning is 0.029 s overall
and 0.002 s in those tail intervals. Source admission is 0.953 s overall and
0.051 s in the tail. These measurements reject queue cloning/pruning and
source-admission cost as dominant explanations of this post-source stall too.
No such algorithms are patched. Exact interval counters are retained in
REQUEST_FEEDBACK_HANDLER_PROFILE_20260906.json.

The next stage discriminator measures relay preparation before event selection
and per-kind input handling. Preparation includes any control/topology awaits
in that region: its duration must not be mislabeled CPU time. This separates
remaining relay work from time below the relay. Existing native flight samples
alone still do not establish the exact native receipt boundary.

## Preparation owner and equivalent correction — 2026-09-06 17:31 UTC

The third profile records 21.533 s of relay preparation. Intervals ending at
process-profile elapsed time >=28 s contain 11.771 s of preparation versus
0.383 s of input ACK handling, after the 25 s source phase. A repeated run's
native stack catches the busy worker in the uniform live-owner frontier
calculation called by completion-tail recovery. See
REQUEST_RELAY_STAGE_PROFILE_20260906.json for interval counts, complete probes
and the native stack; debugger-paused runs are diagnostic only.

LIVE_OWNER_FRONTIER_WORK_BOUND records the symbolic equivalence argument,
history, RED 4,196,352 visits and GREEN 12,286 visits for 2,048 chunks. The sweep
removes repeated coverage scans while preserving exact sets, vector order,
assignment maxima and ranked range authority. Seven model tests pass,
including 4,096 independent byte-oracle cases. Both direction callers still
need integration verification; this is not full mixed-path acceptance.

All temporary profiling scopes are removed from active runtime source. The
exact overlay is preserved in REQUEST_FEEDBACK_DIAGNOSTIC_OVERLAY_20260906.json.
