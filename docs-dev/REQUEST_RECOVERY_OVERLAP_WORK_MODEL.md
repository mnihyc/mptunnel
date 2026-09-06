# Request recovery overlap work — bounded algorithm correction

2026-09-06 15:44 UTC. Candidate, not release acceptance.

## Evidence and original intent

The quiet mixed-upload repeat completes 45.407 Mbps with a 14.616 s confirmation
gap. Its client spends 29.233 s rebuilding recovery work in 46.699 s. A finer
completed profile gives 33.012 Mbps with a 14.965 s gap. The overlap/enqueue loop
consumes 34.143 s, against 0.879 s frame extraction and 0.383 s target selection.
The exact native ACK witness separately locates 44.098 s after confirmed native
receipt but before MPP decoding. These findings support accumulated client
processing backlog, not another BBR/reordering threshold change.

The queue overlap rule predates the current companion experiment. History
691a59a2 extracted its ownership in July; f4206d0b retained separate critical
and ordinary repair lanes. Its intent is correct: already queued repair must
not gain a duplicate intent merely because another recovery observation fires.
The defect is repeated linear discovery of the same occupied byte intervals.
With Q queued repairs and R candidate frames, one pass can inspect Q * R extents.
With repeated feedback this makes catch-up slower as retained backlog grows,
delaying the very ACK processing that would release that backlog.

## Exact model and equivalence

Let U be the union of existing queued repair half-open intervals across both
repair lanes at the start of one serialized recovery batch. A candidate f is
already queued iff its nonempty byte interval intersects U. Normalize U once;
its intervals then have increasing, disjoint starts and ends. Find the first
interval with end > f.start. It overlaps iff start < f.end. This is exactly the
existing strict half-open overlap predicate, including partial overlap and
touching boundaries. No byte is split, silently ACKed or removed.

Request recovery obtains candidates from ReliableSendStream's normalized
range/cache intersection. Those candidates are pairwise disjoint in increasing
offset order: retained chunks never overlap, and normalized requested ranges
never overlap. Therefore an earlier newly queued candidate cannot change the
overlap outcome of a later candidate in the same batch. Snapshot decisions
equal the current sequential queue-and-check decisions. Source ownership is
serialized; no other queue mutation interleaves this synchronous operation.

Cost becomes O(Q log(Q+1) + R log(Q+1)) plus normal enqueue work, rather than
O(Q * R).
The temporary interval vector belongs only to this call; there is no durable
index to invalidate on ACK, close, migration or cancellation. Empty queues and
ordinary single-frame queries keep their semantics. Memory cost is O(Q+R)
temporary numeric data, not additional retained payload or transport credit.

## Scope and obligations

Only the request path-recovery batch uses the new snapshot operation. Preserve
target choice, repair byte limit, exact attachment/copy checks, lane priority,
optional-traffic accounting, dispatch-time validation and dirty/wake causes.
This does not authorize ACK thinning or a no-change claim based on released
bytes alone. ACK ledger rebuilding remains a separate measured cost; do not
mix its optimization into this transaction.

Before change, an operation-count test must fail because the real batch helper
revisits queued extents. After change, it must visit them once. A separate
semantic test compares decisions with existing sequential queue checks using
real mux-generated disjoint candidates, partial ACK holes, overlapping queued
repairs, both lanes and touching boundaries. Then affected sender tests and
ordinary matched upload/download timing, interactive latency and RSS must pass.
No claim that this one correction closes every global performance issue.

## RED and reflection

The production batch helper initially uses the old repeated linear predicate.
`recovery_batch_inspects_existing_queue_extents_once` fails deterministically:
2,098,176 inspections for 2,048 existing intervals, rather than 2,048. The
sequential semantic comparison passes on that same build. Command:

```sh
cargo test --release -j1 --config 'profile.release.package.mptunnel.opt-level=0' --lib recovery_batch_ -- --nocapture
```

The candidate replaces only that predicate's implementation and the request
batch call site. RFC pre-commit overlap suppression is unchanged. This is an
implementation-complexity defect, not justification to weaken the RFC's copy
ownership model. Earlier service-class fairness proofs were insufficient to
establish practical latency: one fair turn can still perform quadratic work.
The previous ordinary upload control also stalled, so the companion capability
is not the sole introduction of this old cost. Neither fact accepts the held
composition or establishes the deployed RAM incident's exact root cause.

Temporary native/ACK/owner diagnostic hooks have been archived and removed.
All 243 affected sender tests pass, including both new proofs. The structural
test now visits exactly 2,048 queued extents. Tests used root-package opt0 to
avoid the previously observed test-build SIGKILL; ordinary comparisons use an
optimized release executable. Those comparisons remain pending; no runtime
or global acceptance yet.
