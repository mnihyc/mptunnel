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
