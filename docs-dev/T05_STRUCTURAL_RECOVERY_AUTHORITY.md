# T05 structural recovery authority

Status: frozen symbolic model for the T05 implementation transactions. This
document does not claim runtime acceptance or release readiness.

## Decision and scope

T05 replaces a cumulative percentage as Product recovery authority with an
exact current-attachment publication identity. It is deliberately split:

1. **T05a -- identity foundation.** Add an authenticated, sender-assigned
   configured-member slot which survives carrier replacement. Track unresolved
   Product recovery by exact range and that stable slot. Keep the existing
   percentage guard during this transaction.
2. **T05b -- authority demotion.** Once T05a is independently GREEN, remove the
   percentage from recovery readiness, byte extent, and final enqueue
   admission. Retain exact traffic accounting. This transaction does not
   invent a new ranking consumer for the percentage: until a separately
   proved scheduler policy exists, it is an accounting/diagnostic target only.
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

The defect is therefore not the existence of accepted-recovery accounting or an
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
configured slot `d`, the publication key is:

```text
X = (session, direction, stream, r, d)
```

Range identity is interval based. Partial Product ACK clips a publication key
to its unacknowledged remainder; complete Product ACK retires it. The
serialized removal of its exact attachment from current Product scheduling
membership ends that attachment's publication ownership. Stream or session
termination retires it. A timer, metric publication, queue observation, port
change, or incarnation change alone cannot retire or mint it.

A planned replacement may temporarily overlap its predecessor. Both map to the
same `d`, so at most one current attachment may own publication authority for
the same `r`. The successor may acquire that authority only after the
predecessor leaves current Product attachment membership. That removal is
serialized with final Apply: if Apply wins, it records and publishes before
removal; if removal wins, later Apply cannot find the exact attachment and
fails. Exact incarnation still fences the final writer reservation and commit;
stable slot identity does not let a successor inherit stale Native authority.

Attachment removal is not a claim that the peer has settled every preceding
byte. That claim is impossible without Product acknowledgement: queued,
locally flushed, or in-progress predecessor data may still arrive after
authority transfers. Such an arrival is a legal duplicate of the same Product
offset and is deduplicated by the receiver. Its accepted Product recovery work
remains in cumulative accepted-recovery accounting; that counter does not prove
physical serialization. The structural invariant bounds current
publication owners, not unknowable packets already in the network.

An intermediate model used the whole carrier's sticky terminal signal as this
boundary because response membership removal can precede native writer drain.
That model is rejected. Writer drain matters to physical settlement, not to
future Product publication, and a QUIC operation-local attachment can be
removed while its carrier remains healthy indefinitely. Requiring carrier
terminal would therefore strand the configured slot. The shared Apply/removal
serialization is both the weaker sufficient proof and the only boundary that
handles carrier-local and operation-local lifetimes uniformly.

## Post-T05b recovery authority

T05a installs and proves the identity terms below while deliberately retaining
the current percentage guard. T05b removes that percentage from authority and
extent so the complete expression becomes normative only after both slices.

For candidate target `t`, let:

- `M(s,r)` mean `r` is retained and still missing at the Product layer;
- `T(s,r)` mean the cause-specific immutable recovery clock is due;
- `E(t)` mean `t` is live, policy-eligible, and distinct from every current
  owner of `r`;
- `d(t)` be `t`'s authenticated configured slot;
- `V(s,r,d)` mean no current attachment publication owner for `r` occupies
  slot `d`;
- `S(s,r)` mean no accepted ReinjectedData copy overlapping `r`, whose exact
  attachment remains in current Product membership, retains an unexpired
  immutable suppression deadline `D`;
- `K_t` be exact target Product repair headroom after queued and accepted debt;
- `Q(s,r,t)` be the cause-specific retained frontier/service extent; and
- `N(t)` mean the exact queue/native reservation can commit.

The Boolean authority is:

```text
A(s,r,t) = M(s,r) && T(s,r) && E(t) && V(s,r,d(t)) && S(s,r)
             && K_t > 0 && N(t)
```

The admitted byte extent is:

```text
L(s,r,t) = min(K_t, bytes(r), Q(s,r,t))
```

The optional extra-traffic percentage does not occur in either expression.
The exact ledger continues to report accepted recovery work and its ratio to
unique acknowledged Product bytes. A future advisory use may rank only among
the same finite eligible actions under fair traversal; it may not rank recovery
against a permanent "do nothing" result or otherwise create starvation. T05b
adds no such use. The percentage cannot turn `A` false or reduce `L`.

Demotion preserves the existing positive-credit cause branch for every value,
including zero. It does not instead force every action through the later
over-credit floor epoch: that epoch was introduced as a liveness floor for the
old cumulative guard, and promoting it to a universal recovery clock would be
a new staggered-service policy belonging to T06. Existing cause clocks,
exact-range overlap suppression, target capacity, stable-slot vacancy, and
native admission still apply. The epoch remains observable for existing
successor wake calculation until T06 adjudicates service order.

The retained live-owner frontier-floor epoch `G_s` and the accepted-copy
suppression deadline `D` are different observations. Eligible live-owner work
may record `G_s` at provisional Product-queue acceptance. After percentage
demotion, `G_s` does not gate a due cause, does not alter `A` or `L`, and does not
create or release publication ownership. Final writer Apply records `D` with
the accepted ReinjectedData flight. While an overlapping current copy has an
unexpired `D`, `S(s,r)` suppresses that range globally across the current slot
set. After expiry another eligible structurally vacant slot may be evaluated;
expiry never releases `J_d(t)` or makes the accepted copy's own slot vacant.

The retained counter records payload accepted into Product recovery work (and
immediately published requalification payload), not proved physical wire
serialization. Cancellation does not refund it. Public diagnostics therefore
may compare this exact accepted-work accounting with the configured target,
but must not relabel it as actual wire bytes.

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

Only the reported accounting target and accepted-work ratio may differ in T05b.
A later, separately proved advisory scheduler may change finite candidate order
but not reachability or extent. T05b must prove invariance in both Product
directions at cause reachability and at final enqueue.

### Publication-owner cardinality

Let `D_s` be the finite configured slot set eligible for direction `s`. Vacancy
is consumed atomically with final target validation. Because one `(s,r,d)` key
can have at most one current attachment publication owner:

```text
current_publication_owners(s,r) <= |D_s|
```

Port hopping and carrier replacement preserve `d`, so they cannot increase the
bound. Two physical carriers temporarily sharing one replacement slot still
consume one publication key while both remain current attachments. Serialized
removal of the owning attachment transfers that key; its draining physical
attempt remains separately accounted. Product ACK clipping cannot increase
cardinality.

This is not a bound on delayed physical packets and not a false finite-delivery
theorem. An unbounded sequence of definitive attachment or carrier failures
may require an unbounded cumulative number of attempts to preserve liveness.
T05 does not cap that sequence by a percentage and does not promise progress
when every eligible native domain supplies zero service.

Stable per-slot Product repair debt is the interval union of unresolved ranges
owned by current attachments. Merely replacing an incarnation or recording
the same range twice cannot enlarge that debt while the predecessor remains in
current membership. Physical attempt bytes are deliberately counted
separately and never collapsed.

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
- distinct configured members get distinct underlay-local slots, while a
  bounded planned predecessor/successor overlap retains the same slot;
- predecessor and successor share one recovery slot while exact incarnation
  Apply fences remain independent;
- timer, metric, queue, port, and incarnation churn cannot mint a second
  current `(s,r,d)` publication owner;
- Product ACK clips/releases the key; serialized exact attachment removal
  transfers it and its existing membership notification wakes the Product
  owner; terminal stream/session releases it; and
- request and response behaviors are symmetric.

## Wire-version decision

The public `v0.4.6` tag speaks wire version 9. Wire version 10 exists only in
the untagged `v0.4.7` candidate, so T05a completes that unreleased canonical
layout in place: it adds the mandatory slot to `PATH_JOIN` and to its existing
v10 authentication transcript. There is no deployed v10 compatibility format
to preserve, no fallback decoder, and no reason to mint a second provisional
version. A peer using the earlier candidate layout fails closed on frame
length/authentication rather than being silently assigned an invented slot.

T05b RED/GREEN must hold all structural inputs fixed and vary percentage across
zero, default, and a large value for request ACK-gap, response ACK-gap, request
completion-tail, response completion-tail, and the two final enqueue paths.
Eligibility and extent must be identical. Existing exact-overlap, `K_t = 0`,
zero configured resource, invalid Native authority, and terminal-failure tests
must remain GREEN.

T05c and T06 require separate proofs and commits. A T05a or T05b result cannot
waive either one.
