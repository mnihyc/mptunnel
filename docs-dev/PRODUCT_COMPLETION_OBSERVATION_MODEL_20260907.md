# Product completion observation: fixed entry floor, paired cohort clocks

2026-09-07 UTC. Bounded model proposal for the existing request sampling owner;
no implementation, new experiment, native-capacity authority or performance
acceptance follows. Read with [the next obligation](COMPLETION_EVIDENCE_NEXT_OBLIGATION_20260907.md)
and [the existing scope audit](RESPONSE_PLACEMENT_RATE_SCOPE_AUDIT_20260907.md).

## First correct the existing reachable request defect

`request_evidence.rs:138-151` requires every covered cohort's earliest assignment
to follow the previous cohort's ACK, then advances that ACK fence even when it
rejects the sample. The live caller in `request/multipath.rs:2925` always
requests this staged-window condition. It is not the sustained-flow contract.

For coverage F, assign cohort n at nD and acknowledge it at R+nD, with 0<D<R.
After the first sample, every cohort has nD < R+(n-1)D and is rejected forever.
This is ordinary pipelining, not an invalid receipt or absence of service.
For R=100ms/D=50ms, the default ordinary TCP F=1MiB (retained acquisition
target=2MiB) needs only a few cohorts within the configured 64MiB resources.
These numbers demonstrate reachability; none is a proposed parameter.

The runtime does not force a stop-and-wait reset: a successful first sample
marks ACK-clock proof/capacity admission and clears its Owner; reconciliation
also clears proven Owners. Acquisition selection excludes already-proven
paths. Ordinary Data commits continue without reopening an acquisition Owner
(`multipath.rs:2111-2150,2744-2750,2948-2978`; `scheduling.rs:790-803`).
Exact ACK release still works while numeric refresh fails. A first epoch can
therefore fail to mature or expire without replacement during healthy load.
This is a source-proved request/upload defect, not attribution of the user's
server-to-client download plateau or a measured speed improvement.

History: the predicate already described a **staged** cohort in the July14
`cf38cb41` extraction. `f4206d0` removed the ordered-service exception and
changed the live argument from `!is_ordered_service` to unconditional `true`.
`5aaf9b3` only renamed its coverage helper; `3a6d0ea` added frozen expiry;
`ed2ad17` only test-gated the provenance helper in this model. Do not attribute
the predicate's introduction to recent CI cleanup or undo unrelated fixes.

## One concrete observation contract

Scope K = logical stream + exact output/attachment incarnation + original
sender direction. For one already-existing covered Product cohort i, retain:

- B_i: newly ACK-released, unique, path-proving OriginalData payload bytes;
- a_i/b_i: earliest/latest assignment time of exactly those bytes;
- r_i: local observation time of the ACK completing the cohort.

Keep existing coverage, qualification and expiry policies. No new time window,
byte threshold, smoothing gain, idle timer or probing traffic is proposed.
Assignment is Product commitment, **not native transmission**. Duplicate,
copied/ambiguous, invalid-generation or replaced-owner evidence remains excluded
by the existing exact-flight release before this observer runs.

Separate two clocks that currently share an inappropriate moving condition:

1. **Entry floor e:** an actual staged-acquisition or expired-epoch boundary.
   Freeze it once. Discard numeric cohorts with a_i<e without advancing e.
   An unseeded first observation uses a_i as its starting boundary. The first
   valid sample keeps the existing bootstrap elapsed max(r_i-e,b_i-a_i), or
   r_i-a_i when unseeded. It does not manufacture established capacity.
2. **Completed-cohort anchors:** after a valid cohort, retain the pair (r_i,b_i).
   For the next chronological cohort j require a_j>=b_i, not a_j>=r_i. Then:

   `elapsed_j = max(r_j-r_i, b_j-b_i)`

   `G_j = 8 * B_j / elapsed_j`.

Use the existing PathRateSample time representation floor; it is not a path
property. The assignment delta spans corresponding completed-cohort endpoints,
not merely the tiny assignment span inside the latest ACK batch. A reversed or
interleaved assignment cohort cannot establish this interval: discard only its
numeric observation, retain the last valid anchors, and do not carry discarded
bytes into the next numerator. Never move an assignment anchor backwards.
Real delivery and qualification release are unaffected by numeric rejection.

The entry floor remains effective for its epoch. Expiry starts a new observer
once, discarding predecessor pending timing and rate averaging; later rejected
cohorts cannot move that floor. New numeric publication retains the existing
frozen absolute expiry. Repeated polls, cumulative volume and old SACKs cannot
renew it. Replacement/direction change cannot reuse either anchor.

### Finite obstruction, not a wall-clock recovery guarantee

This argument does not assume FIFO Product receipts. At a fixed valid
assignment anchor b_i, or entry floor e, only finitely many eligible unique
OriginalData bytes were assigned before that boundary and remain unACKed.
Each covered cohort rejected for crossing that fixed boundary consumes at
least one of those old bytes. Later assignments use a monotonic clock and
cannot replenish that set; duplicate, copied/ambiguous and revoked-owner
receipts cannot create eligible unique bytes. Discarding a numeric cohort
keeps the fixed boundary and valid paired anchors rather than moving either.

Consequently, within one unchanged scope/epoch, with continuing eligible
unique progress sufficient to complete further cohorts, overlap alone cannot
reject every future refresh indefinitely. The old moving-ACK fence lacked
this property: each rejection created a newer obstruction. This is a finite
progress argument, not a FIFO, native-capacity or elapsed-time guarantee.
An old byte mixed into each large new cohort can cause many rejected cohorts;
arbitrarily delayed receipts, sparse/sub-coverage work and scope changes can
still leave long gaps. No useful wall-clock bound follows from these
observations, and this correction does not establish full throughput recovery.

## Meaning and conditional forecast

G is achieved **Product receipt service for K**, including allocation and return
ACK effects. It is not native capacity, an independent bottleneck estimate or
proof of continuously offered work. Source/assignment idle time remains in the
interval. A low sample cannot distinguish a slow carrier from a fast carrier
given little work, and a high sample cannot rule out compressed feedback.

For W bytes of matching unique outstanding Product work plus a proposed prefix,
`8W/G` is only a scenario forecast **if that per-flow receipt service persists
under continuing supply and unchanged contention/feedback**. Do not add native
flight as new Product work, divide by active-flow count, replace native C, or
change credit/pacing. Existing lower-prefix completion is a separate dependency:
later receipts can increase this observation while ordered delivery remains
blocked. Unknown lower-owner completion makes total ordered completion unknown.
Writer reopening time is also not inferred from G. Thus this contract alone
does not authorize busy-fast Defer or establish discovery/non-starvation.

## Symbolic checks and bounded RED

- Equal TCP/QUIC eligible cohorts have identical G. Two64KiB cohorts assigned
  at zero and ACKed at1.0/1.1s give the same second interval100ms and5.243Mbps;
  native telemetry cannot alter the arithmetic. Response projection migration
  is not part of the immediate request correction.
- Compressed ACKs1ms apart with completed assignment endpoints50ms apart use
  50ms, not the latest cohort's tiny internal span. If both endpoint clocks are
  compressed, these observations do not reveal true forward-link capacity.
- Source idle contributes to both endpoint spans where applicable; it cannot
  become a capacity collapse or justify starving the next burst.
- A cohort mixing assignment before b_i with a new suffix is not a valid
  consecutive send-clock interval. Reject/reanchor-by-next-valid-progress as
  specified above; do not turn its old prefix into new high-rate evidence.
- Missing lower Product frontier plus later exact receipts is possible. G
  measures those receipts, not frontier motion or application goodput.
- Direction, replacement, duplicate/copy ambiguity and post-expiry entry retain
  their existing independent fences; no fabricated ACK or proof is introduced.

Canonical owner RED: use an actual default context/exact TCP attachment and
normal Product commits for a real bootstrap cohort. Its exact ACK first
establishes proof. Then pipeline further disjoint F-sized cohorts through
actual admission, split into legal frames and drain only writer commands.
Keep their real Product debt outstanding until live `apply_product_ack` on
their exact ranges. Subsequent samples must refresh the epoch despite
assignments preceding the previous ACK. Also assert ordinary work is still
admissible and no acquisition Owner reappears. This avoids assuming that a
cold unqualified path may publish three coverage cohorts. The initial fixture
failed to recognize a legitimate path-proof command; that was a fixture
failure, not Product RED. After checking that command explicitly, the actual
owner fixture reproduced the defect: pipelined sample counts `[2,2,2]` with
refresh `[true,false,false]`, versus staged `[2,3,4]` and three refreshes.
Both released all exact Product debt and left no acquisition Owner. This
confirms the numeric defect, not an ordinary throughput benefit or release pass.
Separate compression/interleaving and expiry controls prevent a blanket
`true` to `false` bypass from passing as the clean correction.
