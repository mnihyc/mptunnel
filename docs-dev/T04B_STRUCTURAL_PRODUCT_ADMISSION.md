# T04b: structural Product admission

Status: focused implementation GREEN; independently audited GO (2026-09-04)

## Exact defect

Two production-owner tests hold all exact/configured authority constant and
change only inferred scheduling values:

- On the response path, a queue-blocked lower owner leaves one enqueueable
  additional output. Changing only rate, RTT, jitter, loss, confidence, and
  active-flow observations changes the result from that output to no output.
  The exact diagnostic reason is `ecf_no_completion_gain`.
- On the request path, a live contiguous owner has unchanged lifecycle,
  writer readiness, Product debt, `P`, cross-path debt, and configured limits.
  Changing only its observed rate changes the result from that owner to
  `Blocked`. The exact diagnostic reason is `reorder_budget`, derived from the
  inferred BDP.

These are admission defects, not merely bad rankings: both outcomes leave no
ordinary Product action despite an enqueueable action and unchanged resource
headroom.

The separate `completion_horizon` denial is not a production root cause.
Every production non-additional constructor either has zero cross-path debt or
sets `best == candidate`. Because its horizon is then the candidate ETA plus
nonnegative serialization and absorption, `candidate_eta > horizon` is
impossible. Its old unit test manufactured a tuple no production owner can
construct.

## Authority model

For pending OriginalData quantum `N`, let:

- `O` be exact stream-wide unique OriginalData debt;
- `O_i` be the portion assigned to exact output `i`;
- `W` be the configured stream Product resource envelope;
- `P_i` be the configured Product envelope of output `i`;
- `E_i` be the bounded acquisition envelope of an unqualified additional
  output; and
- `L_i` be `P_i` for a first/live-frontier/qualified output and `E_i` for an
  unqualified additional output.

Ordinary Product resource admission is exactly:

```text
O + N <= W
O_i + N <= L_i
```

followed by the existing exact lifecycle/incarnation, receive-credit,
bounded-command reservation, and native-writer revalidation. Configured
repair, reorder, stream, and path-flight limits define `W`, `P_i`, and `E_i`;
native TCP/QUIC backpressure remains the final carrier authority.

Let `e` contain ETA, rate, RTT, jitter, loss, confidence, app-limited state,
flow count, Suspect state, and inferred BDP. For fixed structural state
`sigma`, ordinary admission must satisfy:

```text
A(sigma, N, e1) = A(sigma, N, e2)
```

Those observations may produce a finite candidate order but cannot change
the Boolean resource predicate. If the best-ranked action cannot obtain its
real reservation, the next structurally eligible same-tier action remains
available.

## Why the old model failed

`282b8e1` introduced a BLEST/ECF-style completion veto and adaptive two-BDP
reorder limit to reduce receive holes. Later commits extended it across
same-underlay candidates, compared against the exact lower Product tail, and
made it carrier-neutral. Those changes served the old model consistently.

The authority model later changed: exact Data-ACK-clocked `W/P/E` now bounds
unique Product ownership, while actual command reservations and native
controllers bound carrier work. The older ETA/BDP veto remained in the
ordinary request and response selectors, so one advisory estimate became a
second Product gate. Under a blocked lower owner, a pessimistic estimate could
therefore suppress the only action capable of progress. Under a low inferred
rate, the adaptive BDP could likewise shrink configured headroom and stall a
live frontier owner.

## Bounded correction

1. Ordinary request and response selection use only structural Product
   resource admission. ETA remains in their existing rank and hysteresis.
2. Stream-wide admission compares exact candidate debt plus exact other-path
   debt and `N` against configured `W`; inferred BDP is not a hard sublimit.
3. The dead completion-horizon denial is removed from Product admission.
4. The request ACK-clock measurement-start policy keeps its transaction-local
   completion comparison. Declining that optional annotation falls through to
   the already-selected ordinary Data action and therefore does not deny
   Product work.

No replacement threshold or protocol preference is introduced.

## Non-regression argument

Removing an inferred veto cannot enlarge exact exposure: every accepted
commit still proves `O + N <= W` and `O_i + N <= L_i`, then reserves and
revalidates the exact writer/native authority. The old receive-hole concern
remains bounded by `W/P/E`; when several outputs admit, the existing advisory
rank still prefers the predicted earlier completion. Only a failed real commit
allows the next structural candidate to run.

This transaction must preserve lifecycle/policy/backup/stale tiers, exact
output and attachment incarnations, Product qualification, Data-ACK-only
release, receive credit, command capacity, native congestion control and
pacing, optional measurement bounds, reinjection accounting, apply-time
refund/reselection, and L3 behavior.

One adjacent RFC/code mismatch is deliberately excluded: the current
calculation of `E_i` may consume native credit whereas the newer RFC defines a
portable configured acquisition envelope. T04b treats the published `E_i` as
opaque so this correction cannot silently alter startup authority.

## Focused evidence

The two pre-change owner-level falsifiers are GREEN after the correction:

- response selection retains the sole structurally admissible output when
  only completion observations become adverse; and
- request selection retains the exact contiguous owner when only inferred
  rate falls and configured `W/P` still admit the quantum.

The complete admission module (82 tests), request scheduling module (45
tests), response scheduling module (52 tests), and all-target compilation are
GREEN. The existing request ACK-clock tests confirm that an optional
measurement annotation remains subordinate to ordinary Data.

An independent broad audit also found that untouched parent `88957df` fails
`stale_output_recovery_falls_through_exhausted_target_reserve`. Exact Apply
inspection showed that its bare UDP test outputs have no activation-scoped
Native authority stamp and are intentionally rejected by the current QUIC
fence. The reserve fixture predates that fence. This is a stale test fixture,
not a Product-admission or recovery-authority defect; the carrier-neutral
reserve test uses TCP outputs while dedicated Native tests cover UDP Apply.
