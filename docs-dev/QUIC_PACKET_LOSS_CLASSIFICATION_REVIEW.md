# Individual packet truth versus recovery-episode undo

2026-09-06. Existing native loss/jitter performance investigation. The accounting
defect is reproduced and component-corrected; practical composition remains
unaccepted. The early hypothesis and pre-code model below are retained history,
not a claim that every remaining deployed performance symptom is attributed.

## Question and intended safety boundary

The packet-lifetime correction eliminates the observed missing-send-evidence
decision class. Remaining diagnostic lower-bound actions are budget-driven.
Native declarations in those traces exceed the configured erasure mean, but
declaration populations and physical packet populations are not equivalent.
That observation alone cannot justify changing any loss policy.

Source inspection finds a more specific potential loss of information:
`acknowledge_retained_losses` recognizes an individual retained late original,
but `finish_spurious_loss_detection` calls the controller only after every
packet in its transaction is acknowledged. BBR3 uses that callback for both
native undo and compensation-record reclassification. The all-members rule
was introduced to prevent one reordered packet undoing a genuine congestion
response for other lost packets. That protection is correct and must remain.

Consider two original packets A and B declared lost in the same recovery
transaction. A later arrives and is unambiguously acknowledged within retained
proof lifetime; B really was lost. Native undo remains forbidden because B is
not disproven. Yet A's individual loss declaration is disproven. With the
current notification granularity, A can remain an ordinary loss in the
compensation journal until finalized together with B.

The proposed semantic distinction, subject to reproduction, is:

- Individual retained packet proof may correct only that packet's loss class.
- Such correction supplies neither a bandwidth sample nor native window undo.
- Whole-transaction native undo continues to require every member's proof,
  unchanged controller lineage, no newer/conflicting raw authority and all
  existing expiry/ECN/disqualification checks.
- Genuine B and unrelated packets retain their class and congestion authority.
- Existing bounded replay, rather than an additive refund, would own any
  correction of already-consumed compensation epochs.

No larger compensation percentage, longer retention, new data queue or new
controller episode is implied. The immediate test uses the real QUIC packet
engine with one held original, one actually dropped original and younger
delivered packets. It must establish that both declarations share an episode,
that A really reaches the peer and is acknowledged, that B remains unresolved,
and inspect actual BBR3 record classes without editing its state. Only then
decide whether this is a real accounting defect and whether RFC wording needs
to separate these authorities explicitly.

## Actual counterexample — 2026-09-06 19:17 UTC

The production packet-engine test is RED at the intended assertion, after
all reachability premises pass. Data packets 7 and 8 have the same retained
RecoveryTransactionId(1). Both are initially ordinary losses. The held packet
7 reaches the peer (five PINGs received: four younger originals plus 7) and
is acknowledged while 8 remains actually missing. No controller-wide undo
occurs, correctly. The actual BBR3 journal still records both as non-exempt
ordinary losses. No controller state is edited; its inspection helper exists
only in test builds. This proves the accounting mismatch, not its exact share
of deployed throughput loss.

The native whole-episode guard predates the compensation journal (d5a7413).
The later compensation replay (61c2059) reused that notification as its class
correction input. These authorities have different granularity. Preserve the
original guard for native undo; do not make it an individual truth gate.

## Correction model before implementation

Transport-retained packet proof is the sole owner of a record's ability to
change loss class. Each journal record has a pending-packet-proof bit until
that exact native packet is either acknowledged or expires. The existing
controller-lineage, packet-space and packet-number identity scopes the bit.
Neither a partial episode expiry nor ECN erases the truth of other retained
packets: those events still disqualify native undo for the entire episode.
An individually expired packet remains ordinary/raw as previously classified;
a later ACK cannot resurrect it. An unexpired acknowledged packet becomes
proven-spurious, regardless of whether another member actually was lost.

The native transport reports packet terminals in an ACK/expiry batch to both
extant matching controller copies. A batch is applied before another budget
decision can consume it. The controller does not invent delivery bytes, RTT,
bandwidth samples, congestion-window credit or a native undo from these
notifications. Native whole-episode completion and disqualification callbacks
retain their original rollback conditions and ordering.

The journal's separate list of retained undo transactions can then be removed:
it must not be a surrogate for individual packet-proof lifetime. Prefix folding
uses the per-record pending bit, the existing open loss cohort and callback
batch. Full native undo still has its existing exact snapshot/transaction
identity; journal history does not become another native-undo authority.
The allocation ceiling, checked arithmetic, RawOnly absorption, chronological
replay and release of finalized prefixes remain unchanged. No retention
duration, percentage, controller gain, queue size or wire protocol changes.

For epoch recurrence F, changing A's class replays F with only A's ordinary
loss debit removed. B's debit and every immutable delivered counter remain
unchanged. Clamping and envelope rebasing are replayed in their original order;
adding credit directly is not equivalent. Native state is not rolled back by
this replay. Thus the correction cannot cancel genuine B's earlier congestion
response, but can prevent later decisions from spending a disproven A again.

Batch application must not scan the entire journal once per acknowledged
packet. Build one transient identity lookup for K terminals and scan the N
retained records once, then perform one existing bounded replay. Work is
O(K log K + N log K + retained replay), not O(K*N) repeated replay; there is no
new persistent queue. Every mutable record has a still-live native proof or
an existing finite current cohort. Rolling overlaps do not pin finalized
prefixes, preserving the previous RAM/CPU correction.

Required controls before practical acceptance: A/B partial proof; duplicate
and wrong identity; partial expiry followed by a younger valid ACK; ECN with
valid packet truth but forbidden undo; active/parked/fresh lineage dispatch;
same-ACK real loss; exact replay across clamps/rebases; rolling expiry and
zero/finite journal authority. Existing full-episode undo tests remain.
After component checks, repeat ordinary affected timing comparisons. More
accurate allowed-loss accounting can permit more service and must still be
tested for loaded latency; no universal performance theorem is claimed.

## Component status — 2026-09-06 19:31 UTC

The actual A/B packet-engine counterexample is GREEN. The 468-test native
suite passes, including unchanged full-episode undo, genuine congestion,
exact replay and journal resource controls. A further targeted migration-copy
test passes: packet terminals after ECN reach both same-lineage copies, never
a fresh lineage or another packet-number space, and do not change native
windows. The earlier partial-expiry case now asserts distinct expired-old and
acknowledged-young packet terminals while whole-episode undo remains forbidden.

Native disqualification tests verify that unexpired packet truth remains live
after ECN or episode abandonment, expired truth cannot revive, duplicates and
wrong-space terminals do not change budget state, B remains an ordinary loss,
and replay matches the canonical classification oracle without adding delivery
bytes or changing the native window/bandwidth. Callback-only retention fixtures
now deliver both packet expiry and episode disqualification, as production
does; this is not a relaxation of the rolling-prefix/resource assertions.

The transaction-retention collection is deleted. Its allocation charge is
removed because the allocation no longer exists; the per-record pending bit
is included automatically in size_of(record). Resource ceilings are unchanged.
Batch lookup still adds work on late-ACK paths; its bounded complexity is not
an empirical CPU non-regression proof. Root adapter checks and ordinary
before/after timing comparisons remain required. No practical acceptance or
release is declared from these component results.

## Ordinary comparison and retention review — 2026-09-06 19:53 UTC

All 469 native tests, 25 MPP adapter checks and strict all-target/all-feature
Clippy pass.
The 8,192-round rolling-expiry check retains three records/two epochs for
three native proofs, with capacities four/four; finalization releases both
allocations. This preserves the previously fixed prefix-collection bound.
It is not a proof of total process RSS under arbitrary application load.

Packet terminals remain distinct from discarded live-send metadata. Actual
loss, including one without a native undo identity, retains a native packet
record that later ACK/expiry can settle. ECN-only handling does not create an
individual loss record. Key discard leaves existing lost-packet proof for the
normal expiry path. Retry cannot have prior loss declarations in its replaced
Initial space: a processed authenticated Initial/Retry already forbids Retry,
and native loss detection requires an earlier ACK. No additional key-discard,
Retry or controller-reset policy is introduced by this correction.

QUIC_PACKET_CLASS_COMPARISON_20260906.json preserves ten complete probes and
one-second observations, using ordinary optimized frozen binaries on the same
held composition. This is the existing asymmetric-jitter 500-Mbps discriminator,
not the final baseline/browser/aggregation matrix. Combined runs have random
downstream 3--10% loss with mean 6%, unequal reverse loss, and downstream
500→10→500 Mbps at seconds 15/25, without the separate UDP outage.

| Case | Control mean / candidate mean(s), Mbps | Control post25–40 / candidate(s), Mbps | Control maximum read gap / candidate(s), s |
| --- | --- | --- | --- |
| QUIC QoS | 124.890 / 113.447, 122.099 | 177.194 / 147.509, 167.225 | 2.583 / 3.966, 2.884 |
| Mixed QoS | 78.922 / 108.806, 73.184 | 80.464 / 167.262, 104.638 | 3.114 / 4.720, 3.780 |
| QUIC steady | 170.675 / 181.650 | not applicable | 0.339 / 0.505 |
| Mixed steady | 137.414 / 154.024 | not applicable | 0.417 / 0.500 |

All 50 scheduled echoes succeed in each steady run. Steady candidate/control
p95 is 327/275 ms (QUIC) and 312/253 ms (mixed); maxima are 428/372 and
693/839 ms. QoS still times out the initial interactive connection; subsequent
unavailable slots are not independent failed connections. Mixed startup and
ordered delivery remain visibly bursty. Some one-second application bins
exceed the shaper rate because the receiver releases previously buffered bytes;
they are not wire service measurements and are not clipped or advertised.

Verdict: individual accounting is correct in the tested owner model, but the
observed ordinary differences do not prove a universal gain or latency
non-regression. The higher mixed recovery rate does not excuse its startup,
read gaps or the QUIC comparison. Keep an isolated intermediate checkpoint,
not a release acceptance. No compensation, expiry, queue or gain change is
justified from these averages.

Next exact owner stays the existing mixed ordered-progress problem. In the
steady candidate at about four seconds, QUIC has natively ACKed 54.1 MB while
the application has received only 5.0 MB; TCP queues retain about 5.5 MB and
QUIC has about 37 KB in flight. This separates native arrival from ordered
Product delivery, but does not yet identify which lower range or repair
decision obstructs it. Trace that exact frontier and its cause-specific repair
authority before changing scheduling. Do not infer a startup FINAL failure
from these one-second snapshots or undo the ranked-extent/copy protections.
