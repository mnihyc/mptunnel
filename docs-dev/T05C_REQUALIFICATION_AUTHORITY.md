# T05c requalification authority

Status: frozen symbolic model for T05c, with the receipt-deadline slice proved
first. This document does not claim runtime acceptance or release readiness;
deadline construction and queue ownership remain separate follow-ons.

## Decision and scope

Requalification is non-delivering attachment maintenance. It copies a bounded
piece of retained `OriginalData` to test one stale exact attachment, but it
does not own Product sequence space, advance Product delivery, or supply rate
authority. T05a's configured-member publication slots and T05b's Product
recovery theorem therefore do not become requalification identity.

T05c answers only these questions:

1. what authorizes and bounds one probe;
2. when its exact receipt may affect attachment qualification; and
3. whether the optional extra-traffic percentage has any authority over it.

It does not choose ordinary Product placement, recovery service order,
cross-stream scheduling priority, native congestion control, a rate estimate,
or a retry interval. Those remain their existing independently owned models.

## Historical intent and defect

Commit `3e35938` introduced exact data-bearing requalification so a stale path
could prove service and recover without a process restart. Its single pending
transaction, finite cyclic target cursor, retained-data requirement, retry
deadline, and exact non-reused probe ID prevent a health label or unrelated
return path from reviving Product admission.

That model froze an absolute deadline, but the implementation tested it only
when the next selection pass ran. Both request and response receipt paths
instead accepted a matching probe after the deadline. Actor scheduling could
therefore choose the semantic result: a timer/selection event first made the
receipt stale, while an ACK event first revived the attachment. This is an
exact lifecycle defect, not a performance threshold or timer-tuning problem.

The RFC already says that expiry wins at the deadline and a late ACK is a
stale no-op. The implementation must be corrected to that model; changing the
deadline or adding grace would merely preserve the race with a new constant.

## Probe authority and extent

For one stream send direction, let:

- `T` be one current authenticated, policy-eligible stale exact attachment;
- `P = (stream_id, probe_id, offset, payload_bytes)` be a fresh non-reused
  exact probe tuple bound to `T`;
- `R` be an authenticated current same-session attachment returning the ACK;
- `B` be a positive retained `OriginalData` source extent;
- `Q` means the exact bounded carrier-command reservation can commit; and
- `q` be the configured structural probe quantum.

One probe may be published only when no other probe is pending in that stream
direction and `T`, `B`, and `Q` remain valid at commit. The return
attachment `R` authenticates reverse session service only; `P`, not `R`,
selects `T`.

The structural quantum is:

```text
q = max(1, min(path_open_score_bytes, bulk_feed_quantum, max_repair_bytes))
L = min(q, bytes(B))
```

The explicit floor makes `q` positive; retained-source and final resource
validation still decide whether any positive `L` can commit. `L` is finite.
The established quantum is deliberately large enough to be a useful
data-bearing acquisition sample while remaining inside the ordinary feed,
repair, retained-data, command-queue, later native-pacing, and single-pending
bounds.
Changing it to a token byte or enlarging it from a throughput experiment would
be a new unproved policy, not a correction.

Before T05c the wrappers compute, for remaining optional credit `C`:

```text
min(q, max(C, q)) = q
```

for every `C`, including zero. The optional percentage therefore has no
behavioral authority over target eligibility or probe extent. The clean model
keeps `q` directly and charges the exact accepted `L` to the existing
accepted-recovery accounting. The percentage may change the reported target
or ratio only; it cannot suppress, shrink, or enlarge a requalification probe.

## Exact receipt transaction

For receipt adjudication, take the already stored immutable deadline `D` as an
input and define:

```text
X = (stream_direction, P, T, D)
```

The normative publication transaction should obtain `D` with checked addition
at its defined publication boundary. Whether both concrete directions already
do so is a separate construction proof; it cannot change how a representable
stored `D` is adjudicated here.

The receipt-authority interval is half-open:

```text
valid(X, now) = exact_current(X) && now < D
```

Within the serialized request ledger or response output lock:

1. authenticate `R` as a current attachment of the named stream and session;
2. resolve the exact non-reused `P` to the exact current `T`;
3. preserve the existing revoked Product-admission precondition;
4. compare the receipt time with frozen `D`; and
5. apply exactly one transition.

If `now < D`, the exact target enters `Acquiring`, with zero qualification
evidence; fresh uniquely owned `OriginalData` and its exact Product ACK are
still required to reach `Qualified`. If `now >= D`, including equality, the
target atomically returns to `Stale { retry_at: D }`, the receipt returns false,
and Product admission remains revoked. A mismatch changes nothing.

The cursor advanced at publication and is not rewound by either outcome. The
ordinary successor wake may immediately select the next due target. A later
lazy expiry pass is idempotent. This linearization removes all dependence on
whether an ACK, deadline, or actor wake becomes runnable first.

## Bounds and non-claims

At most one exact probe is pending per stream direction. One publication adds
at most `q` accepted payload bytes. Under persistent probe loss, the current
retry model permits at most one such publication per completed/expired
transaction, and every accepted payload remains accounted. After publication,
the bounded carrier command pipeline owns the accepted work. If and when its
writer hands the frame to TCP or QUIC, that native transport owns subsequent
service and recovery. Expiry retires proof authority; it cannot pretend
already accepted downstream work was never admitted, and no Product
acknowledgement is fabricated.

These are cardinality and work bounds, not a wall-clock delivery theorem. Zero
native service, zero reverse service, permanent higher-priority starvation,
absent retained bytes, or exhausted identifiers can prevent requalification.
Whether many streams' maintenance work can unfairly precede ordinary Product
work is a service-ordering question and must be adjudicated separately with an
exact reachable counterexample; it cannot change this transaction's identity,
extent, or deadline semantics.

This receipt slice also does not decide whether a command already committed to
the bounded carrier queue must gain a removable cancellation identity. The
current runtime treats that commit as accepted pipeline work, while older RFC
text describes cancellation of a still-removable reservation. That mismatch
requires its own queue-ownership proof; accepting a late receipt cannot be its
substitute.

## Required exact evidence

Before production changes, request and response REDs must prove:

- the same exact receipt succeeds immediately before `D`;
- it fails exactly at `D`, leaves the target stale, and does not reactivate
  Product admission;
- it fails after `D` with the same state;
- a mismatch or unattached return carrier remains a no-op; and
- the next attempt uses a fresh probe ID and preserves the advanced cursor.

Focused GREEN must retain exact-probe/fresh-OriginalData qualification,
same-session sibling return, one-pending cardinality, finite cursor traversal,
queue/native admission, detach/terminal fencing, and exact accepted-byte
accounting. Separate request and response zero/default/large-percentage cases
must publish and account the same `L`; this proves the no-sizing-change verdict
rather than selecting a new constant.
