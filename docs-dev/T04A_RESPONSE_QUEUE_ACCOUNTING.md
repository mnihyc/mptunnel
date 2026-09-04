# T04a response queue accounting

Status: focused GREEN. This closes only the duplicate response completion
projection. It makes no whole-tunnel performance claim.

## Owner model

For one carrier command queue at one observation instant, define:

```text
R = bytes reserved but not yet published into a bounded channel
Q = bytes still in bounded channel queues
W = bytes dequeued by a writer but not yet released
P = total command charge retained by ReliablePathCommandQueueMetrics
N = independently observed native sender queue
```

The queue owner establishes:

```text
P = R + Q + W
0 <= W <= P
```

Reservation adds bytes to `P`. Commit transfers the same charge into a queue
envelope. Dequeue transfers that charge to the writer and changes its stage
from `Q` to `W`, but does not release `P`. Writer commit, failure, or receiver
drop releases the exact outstanding charge once. Bounded channel capacity may
therefore reopen at dequeue while byte ownership correctly remains in `P`.

The response path snapshot already projects `P` into `queue_bytes`. Where a
native queue is independently observed, the existing producer has already
combined the disjoint native and command work. The completion projection may
retain an exact `N` floor, but it must not add `W` again:

```text
correct response queue work = max(snapshot.queue_bytes, exact N floor)
incorrect                     = correct response queue work + W
```

`W` is a stage-location diagnostic, not an additional service stage. Adding it
would assign two remaining-service charges to the same bytes.

## Why the defect existed

The original queue owner retained one inclusive pending charge through writer
release. A later change added `writer_pending_bytes` to distinguish dequeued
private writer work for reinjection ordering while deliberately retaining the
inclusive total. The non-release v10 checkpoint removed the hard writer-idle
gate and attempted to preserve that work as soft completion evidence by adding
`W` to the snapshot. The composition overlooked that `P` was still present.
Neither the original queue lifetime nor native telemetry was defective.

## Exact falsifier and correction

The focused fixture enqueues 4,096 and 8,192 bytes. After the first command is
dequeued but before release, `R=0` and:

```text
P = 12,288 bytes
W =  4,096 bytes
old completion projection = 16,384 bytes
```

The corrected projection remains 12,288 bytes. The same fixture proves that
channel admission reopens independently, writer release reduces `P` to 8,192
exactly once, and a larger exact native queue remains a floor rather than a
reason to add `W`.

The implementation changes one arithmetic projection. It does not release
charges at dequeue, resize a queue, change admission, subtract and re-add
concurrently sampled values, alter Product flight, change request-direction
accounting, or reinterpret native metrics. Request completion already treats
its carrier pending value as one floor and needs no corresponding change.

## Evidence

- Pre-change RED: `t04a_response_completion_counts_dequeued_writer_charge_once`
  reported 16,384 instead of 12,288 bytes.
- Post-change GREEN: the same exact lifecycle fixture passes.
- Affected response scheduling module: 51/51 tests pass.
- Independent owner and test audits agree that `W` is a subset of `P` and
  must be omitted from completion arithmetic.
