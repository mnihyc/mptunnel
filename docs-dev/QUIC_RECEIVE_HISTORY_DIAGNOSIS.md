# Native receive-history limit under reordering

2026-09-06 UTC. Diagnosis, not an accepted production fix.

The [23-case continuation evidence](reordering-continuation-evidence-20260906.json)
preserves application time series, individual echo outcomes and case caveats.

## Confirmed mechanism

Quinn's `connection/spaces.rs::Dedup` stores128 bits plus the highest packet
number. Authenticated packet numbers more than128 behind that highest number
are treated as possible duplicates and discarded before frames/ACK processing.
This is intentional bounded duplicate protection, already present in24fa1a2,
not introduced by the current lifecycle/retention corrections. The practical
limitation is its packet-count horizon, not the need for duplicate protection.

The [real native-packet diagnostic](QUIC_RECEIVE_HISTORY_DIAGNOSTIC.rs) uses
100ms RTT and sufficient sender credit to emit a short flight. It first keeps
150 packets unacknowledged so the held original has a two-byte encoded packet
number; this excludes short-number reconstruction ambiguity. No sender ACK is
processed before the receiver observation:

-64 newer packets: all65 originals arrive; held original elapsed61.28ms.
-130 newer packets:130/131 originals arrive; held original elapsed62.60ms.
-Normal one-way delay is50ms. A trace confirms the missing original successfully
  authenticated, then `Dedup` discarded PN157 with highest PN287.

The diagnostic asserts current behavior and is archived outside desired-behavior
tests. A correction must deliver that original once without accepting its
duplicate. It must not change the test to approve the discard.

## Live attribution

The observed-deadline sender candidate still has the unchanged receiver.
On the routed500/100Mbps,70/30ms,20/5ms-jitter link with no injected loss:

-Both router netem queues report zero drops in the final sample.
-The client records4,948 distinct authenticated Data-space duplicate-history
  discards. Mean overtaking distance238.2 packets, maximum845.
-All4,948 packet numbers match sender loss declarations:4,948/7,371 distinct
  declared-lost Data packets numbered>=100 in the complete trace (~67.1%).
-Application goodput16.175Mbps, maximum read gap0.763s,47 successful echoes,
  no failed echoes, success p95about800ms. Trace overhead means this run is for
  attribution, not a precise ordinary-build performance comparison.

These receiver discards turn delivered network packets into real QUIC erasures.
They cannot be repaired by pretending the sender's loss evidence is false or
increasing compensation. Some other declared losses remain unmatched; do not
attribute all of them to this one mechanism. The trace includes final draining.
The reproducible probe logs and complete interval series are under
`.tmp/reflection/results/quic-steady-down-receive-history-trace-0906/`.

At rate R bits/s and typical packet size S bytes, a differential delay J can
produce approximately `R*J/(8*S)` overtaking packets. For500Mbps and1200bytes,
129 packets span2.477ms. Conversely, an80ms differential delay spans129 packets
at15.48Mbps. This explains the scale of the observed low-rate regime; it is not
an exact goodput ceiling independent of packet sizes or the delay distribution.

## Why sender-only candidates are not accepted

The [candidate chronology](QUIC_REORDERING_MODEL_CANDIDATE.md) separates:

1. Premature sender loss under shallow reordering, before receiver history is
   exhausted. Late-original proof demonstrates this; detector adaptation helps.
2. Receiver history exhaustion as more work is successfully offered. These
   packets receive no ACK because the receiver discarded them. Increasing the
   sender's waiting time cannot restore frames that were never processed.
3. Packet-number encoding: `PacketNumber::new` chooses width from the distance
   to the largest ACK. A one-byte number has a reconstruction half-window of128.
   A wider receiver history alone does not solve ambiguity in an already emitted
   short number. This constraint follows the codec; its live contribution has
   not yet been separately counted.

The higher mixed average is not acceptance: the full series still surges and
collapses, and some candidate runs lose interactive service. No sender-only
candidate is merged into production as a completed fix.

## Bounded next model and gates

Treat sender loss tolerance, receiver duplicate-history lifetime, and packet-
number encoding as one reordering contract with distinct owners. Before code:

1. Define the supported history horizon and its actual memory cost. A fixed
   packet count cannot be advertised as a rate-independent timing guarantee.
   Any resource setting must control real retained storage, not fake health or
   an artificial rate ceiling. Determine allocation/retirement from that model.
2. Preserve an explicit monotone retirement floor and exact once-only packet
   admission within retained history. Growing storage must never reinterpret
   forgotten packet numbers as known-unseen; memory must not grow for the entire
   connection lifetime. No bypass of duplicate protection is an acceptable fix.
3. Make sender encoding sufficient for the declared receive tolerance, with
   explicit wire-overhead and asymmetric/older-peer behavior. Do not simply
   change129 to a lab-selected larger constant or adjust only the receiver.
4. Keep the existing genuine loss/ECN and recovery owners. A learned delay can
   defer genuine loss; Quinn's time-loss timer takes precedence over PTO under
   RFC9002. Exercise a history learned during QoS followed by a healthy path and
   real loss, not only a stationary jitter throughput test.
5. Real packets: accept timely reordered originals exactly once; reject real
   duplicates before/after retirement, preserve epoch/space separation and
   bounded storage under sustained traffic. Then ordinary-build timing-series
   comparisons: jitter-only, loss-only, QoS/restoration, blackhole, QUIC/mixed,
   asymmetric upload/download and the existing browser/churn/aggregation gates.

This is the next already-reproduced performance owner, not permission to reopen
the accepted restart, close, journal, receive-credit or exact-frontier fixes.
The [global review](REVIEW_AND_PRACTICAL_ACCEPTANCE.md) and full acceptance
requirements remain in force. No release claim is made.
