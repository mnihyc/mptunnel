# T05 structural recovery authority

Status: frozen symbolic model for the T05 implementation transactions. This
document does not claim runtime acceptance or release readiness.

## Decision and scope

T05 replaces a cumulative percentage as Product recovery authority with an
exact live-copy identity. It is deliberately split:

1. **T05a -- identity foundation.** Add an authenticated, sender-assigned
   configured-member slot which survives carrier replacement. Track unresolved
   Product recovery by exact range and that stable slot. Keep the existing
   percentage guard during this transaction.
2. **T05b -- authority demotion.** Once T05a is independently GREEN, remove the
   percentage from recovery readiness, byte extent, and final enqueue
   admission. Retain exact traffic accounting and expose the percentage only
   as an advisory cost/rank input.
3. **T05c -- requalification adjudication.** Treat non-delivering
   `STREAM_REQUALIFY_DATA` separately. Its identity is an exact attachment and
   probe ID, not a Product range and recovery slot. Current code already
   preserves one minimum probe quantum after percentage exhaustion; no change
   is justified until its own transaction proof says otherwise.

T05 does not choose sequential, staggered, or concurrent recovery service.
That is T06. T05 changes neither TCP nor QUIC congestion control, native
recovery, pacing, Product windows, queue bounds, nor exact-incarnation Apply
fences.

## Why the current mechanism exists

The original cumulative duplicate ledger was introduced to prevent optional
repair from becoming an unbounded second congestion window. That objective is
valid. Later persistent-gap service widened one attempt to a measured Product
window and a deterministic replay then produced 434,790,952 repair bytes where
the old cumulative envelope was 108,847,604 bytes. The hard percentage guard
stopped that renewal. A later one-attempt epoch restored a small liveness floor,
but retained the percentage as ordinary recovery authority.

The defect is therefore not the existence of wire accounting or an
anti-renewal invariant. It is using an operator traffic preference to decide
whether a structurally bounded recovery copy may exist. With Product,
lifecycle, and native state held fixed, changing a percentage currently changes
wake eligibility, service extent, and final enqueue success. That violates the
project-wide rule that numeric performance thresholds are hints.

Deleting the guard alone is unsafe. Request recovery currently identifies an
attachment by `RelayPathInstance`; response recovery identifies a carrier by
`(CarrierPathKey, incarnation)`. Both are exact Apply identities, but both are
reminted by replacement. The requester already knows a stable configured
member through `RelayPathKey`; the responder receives no corresponding peer
member identity. A local listener ordinal cannot substitute because multiple
peer TCP members may use the same listener. T05a must close that protocol gap
before T05b can remove the old guard.

## Identity model

For one sender, define a configured ordering-domain slot:

```text
d = (session, underlay, configured member slot)
```

The client assigns the member slot from its immutable configured carrier-set
index. It is unique within `(session, underlay)`, stable across port changes and
physical carrier replacement, and authenticated as part of `PATH_JOIN`.
`PathId`, physical carrier incarnation, and stream attachment incarnation remain
separate exact-lifetime identities; none may be used as the stable slot.

For a logical-stream send direction `s`, exact retained Product range `r`, and
configured slot `d`, the live-copy key is:

```text
X = (session, direction, stream, r, d)
```

Range identity is interval based. Partial Product ACK clips a live key to its
unacknowledged remainder; complete Product ACK retires it. Stream or session
termination retires it. A timer, metric publication, queue removal, port hop,
or incarnation change cannot retire or mint it.

A planned replacement may temporarily overlap its predecessor. Both map to the
same `d`, so at most one may own a live copy of the same `r`. The successor may
replace that copy only after exact terminal evidence proves the predecessor can
no longer coexist as a delivering native copy. Exact incarnation still fences
the final writer reservation and commit; stable slot identity does not let a
successor inherit stale Native authority.

## Recovery authority

For candidate target `t`, let:

- `M(s,r)` mean `r` is retained and still missing at the Product layer;
- `T(s,r)` mean the cause-specific immutable recovery clock is due;
- `E(t)` mean `t` is live, policy-eligible, and distinct from every current
  owner of `r`;
- `d(t)` be `t`'s authenticated configured slot;
- `V(s,r,d)` mean no unresolved live copy of `r` occupies slot `d`;
- `K_t` be exact target Product repair headroom after queued and accepted debt;
- `Q(s,r,t)` be the cause-specific retained frontier/service extent; and
- `N(t)` mean the exact queue/native reservation can commit.

The Boolean authority is:

```text
A(s,r,t) = M(s,r) && T(s,r) && E(t) && V(s,r,d(t))
             && K_t > 0 && N(t)
```

The admitted byte extent is:

```text
L(s,r,t) = min(K_t, bytes(r), Q(s,r,t))
```

The optional extra-traffic percentage does not occur in either expression.
It may contribute an advisory action cost after structurally eligible actions
exist, and the exact ledger continues to report resulting wire amplification.
It cannot turn `A` false or reduce `L`.

Exact carrier failure retains its existing cause-bounded correctness authority.
Zero configured repair/path-flight limits, exhausted `K_t`, owner-set overlap,
missing retained data, invalid lifecycle, and failed queue/native reservation
remain legitimate hard negatives.

## Symbolic obligations

### Percentage invariance

For fixed structural and lifecycle state `S` and any configured percentages
`p1` and `p2`:

```text
A(S, p1) = A(S, p2)
L(S, p1) = L(S, p2)
```

Only advisory cost and reported amplification may differ. T05b must prove this
in both Product directions at cause reachability and at final enqueue.

### Live-copy cardinality

Let `D_s` be the finite configured slot set eligible for direction `s`. Vacancy
is consumed atomically with final target validation. Because one `(s,r,d)` key
can have at most one live owner:

```text
live_copies(s,r) <= |D_s|
```

Port hopping and carrier replacement preserve `d`, so they cannot increase the
bound. Two physical carriers temporarily sharing one replacement slot still
consume one key. Product ACK clipping cannot increase cardinality.

This is a live-copy bound, not a false finite-delivery theorem. An unbounded
sequence of definitive terminal carrier failures may require an unbounded
cumulative number of attempts to preserve liveness. T05 does not cap that
sequence by a percentage and does not promise progress when every eligible
native domain supplies zero service.

### Atomicity

Decide may inspect a snapshot, but Apply must reserve the physical writer,
revalidate exact target Product headroom and incarnation authority, reserve the
stable-slot range key, and then commit the frame. Any failed validation refunds
both reservations. Recording the slot key after native commit would permit two
actors to pass vacancy concurrently; recording it before a fallible reservation
without rollback would leak authority.

### Direction symmetry

Request and response recovery use the same slot/range theorem. Their concrete
path and attachment identities differ, but neither direction may fall back to
peer `PathId`, listener ordinal, rate evidence, or percentage credit as
authority.

## Required exact evidence

T05a RED/GREEN must prove:

- the new member slot is canonical wire data and covered by `PATH_JOIN` HMAC;
- changing the slot invalidates authentication;
- two current carriers cannot claim one slot except the already bounded planned
  replacement overlap;
- predecessor and successor share one recovery slot while exact incarnation
  Apply fences remain independent;
- timer, metric, queue, port, and incarnation churn cannot mint a second live
  `(s,r,d)` copy;
- Product ACK clips/releases the key; terminal stream/session releases it; and
- request and response behaviors are symmetric.

T05b RED/GREEN must hold all structural inputs fixed and vary percentage across
zero, default, and a large value for request ACK-gap, response ACK-gap, request
completion-tail, response completion-tail, and the two final enqueue paths.
Eligibility and extent must be identical. Existing exact-overlap, `K_t = 0`,
zero configured resource, invalid Native authority, and terminal-failure tests
must remain GREEN.

T05c and T06 require separate proofs and commits. A T05a or T05b result cannot
waive either one.
