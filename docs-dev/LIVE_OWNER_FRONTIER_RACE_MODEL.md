# Live-owner frontier race model

Status: component-level design and proof for SEEN-6E. This document does not
claim runtime acceptance or release readiness.

## Defect boundary and history

MPP leaves congestion, pacing, packet recovery, and ordered delivery inside
each native TCP or QUIC transport. Product ordering is independent: a later
range delivered by a healthy carrier cannot advance the contiguous Product
frontier while an earlier range remains owned by a degraded but live carrier.
The mixed 500 -> 10 -> 500 trace demonstrates exactly that state.

Commit `3f40ca9` allowed one adaptive frontier quantum to race after one owner
recovery interval. That was the intended Product-liveness escape. Commit
`476f10e` incorrectly expanded the exception to a whole measured service
window, making repeated repair capable of becoming a second congestion window.
Commit `0b50e9a` correctly returned that larger service window to cumulative
optional credit, but it also removed the original one-quantum floor whenever
credit was exhausted. Stronger authoritative-gap evidence could therefore
provide less liveness than a silent contiguous tail. This non-monotonic result
is the SEEN-6E implementation defect addressed here.

The correction is not a shorter timer, a path-score preference, or a new
congestion controller. It restores one bounded Product-frontier race while
retaining the cumulative anti-amplification rule introduced by `0b50e9a`.

## State and transition model

For one logical-stream send direction and one serialized evaluation, define:

- `Q`: one immutable direction-level repair quantum captured for target
  comparison in this evaluation;
- `C`: remaining cumulative optional-reinjection credit;
- `H`: bytes in the exact lowest uncovered Product frontier;
- `O(x)`: the complete set of live exact OriginalData output incarnations
  covering byte `x`;
- `A(x)`: the complete set of exact output incarnations with unresolved
  OriginalData or accepted ReinjectedData covering byte `x`;
- `U`: the maximal retained contiguous prefix from the lowest uncovered byte
  on which `O(x)` is the same non-empty singleton and `A(x)` is unchanged;
- `M = min(Q, H, U)`: the common frontier extent used to rank alternatives;
  and
- `G`: one non-accumulating live-owner over-credit frontier-floor token.

The `K_t` and `A_t` quantities below are candidate-specific only after the
common frontier is defined. When an attempt is accepted, `R` is the immutable
MPP recovery interval captured for that batch. For independently bound frames,
`R` is the maximum interval of every frame actually accepted; an earlier target
cannot renew the shared token while a slower accepted target remains inside
its recovery interval.

Cache chunks are serialization slices, not scheduling authority: adjacent
chunks do not end `U` while their exact `O(x)` and `A(x)` sets remain equal.
For every OriginalData assignment span `j` intersecting `M`, retain its
immutable assignment time `a_j` and applicable owner interval `R_o,j`. The
absolute owner boundary is:

```text
T_f = max_j(a_j + R_o,j)
```

The early loss boundary is aggregated the same way from its applicable
per-span interval. Therefore post-fallback service begins only after every
constituent of `M` has matured; taking a latest assignment time never borrows
authority from an older byte. A hole in retained data, ambiguous OriginalData
ownership, a non-live owner, or any change in either exact identity set ends
`U` before that byte.

Every candidate is ranked against the same `M`; otherwise target-specific
quantum selection is circular because target choice depends on payload size
while payload size depends on the chosen target. For a target-bound batch,
selection yields exact target `t`, its adaptive repair quantum `A_t`, and its
current Product repair capacity `K_t`. Apply uses:

```text
F_t = min(K_t, A_t, M)
```

It may therefore shrink the ranked frontier prefix, but may never enlarge or
skip it. The final target-bound service limit is additionally capped by `U`.
Bytes beyond `M` can be admitted only from cumulative optional credit: because
`F_t <= M`, `L > M` implies `C > M` and therefore those suffix bytes have no
over-credit component. Every suffix slice must still be retained and
unacknowledged, keep the same exact avoidance set, exclude target `t`, and pass
fresh exact target Product/native admission. The first failed, overlapping,
or identity-changing slice stops the prefix; it cannot be skipped. Candidate-
specific quantum ranking is an optional future model, not a correctness
premise here.

The pre-existing generic tail path is target-unbound at Product-queue time.
For that path, `A_u` is the bounded preassignment event quantum and `K_u` is
the global queue/event resource bound:

```text
F_u = min(K_u, A_u, H)
```

Queued unbound work is conservatively charged against eligible targets, and
native dispatch enforces the actual selected target's exact `K_t` before wire
commitment. Making every generic tail target-bound is a separate architecture
change, not part of this correction. In the common equations below, `(K,F)`
means `(K_t,F_t)` for a bound batch and `(K_u,F_u)` for an unbound batch.

For a bound transaction before the original-owner recovery boundary, the
service limit is:

```text
L = min(K, U, C)
```

At or after that boundary, when `G` is available:

```text
L = min(K, U, max(C, F))
D = max(0, L - C)
```

where `D` is the part that crosses remaining optional credit. Before the owner
boundary or while `G` is consumed, `L = min(K, U, C)` and `D = 0`. The
target-unbound path substitutes its already bounded retained prefix for `U`.
Every accepted byte, including `D`, is still charged to the cumulative
duplicate ledger.

Successful serialized Product-queue acceptance of a live-owner gap or tail
batch at or after `T_f` while `G` is available consumes it, even if `C` paid
for the complete batch. Optional-funded acceptance before `T_f`, or while `G`
is already closed, neither requires nor renews the token. This is the
scheduling-attempt boundary, not a claim that a native writer has transmitted
the batch. The accepted attempt fixes `next = accepted_at + R`. Repeated
evaluation, queue removal, copy expiry, target replacement, metric publication,
and a gap/tail evidence transition do not alter `next`. If native dispatch
later abandons the queued attempt, retaining the consumed token is conservative:
it creates no wire amplification and delays a successor by at most the captured
`R`. Once `next` is due, one later accepted attempt fixes a new
`accepted_at + R`; missed intervals do not accumulate. Native dispatch still
revalidates exact target service and carrier admission before wire commitment.

Only contiguous unique Data-ACK frontier progress restarts the full quiet
interval. Sparse suffix ACKs do not. Exact terminal carrier failure is a
different cause with separate bounded correctness authority and neither
consumes nor waits for `G`.

A live gap has two cause stages: an early completion-winning deadline `T_e`
that can spend optional credit, and the original-owner recovery boundary
`T_f` that also enables the frontier floor. If `T_e` is due while `C = 0` and
`T_f` is still future, the retained cause deadline is `T_f`; consuming or
exhausting optional credit must not discard the later floor wake. New ACK
funding is itself an actor event and reevaluates the earlier optional stage.

A retained recovery cause has two authorization branches. Optional-funded
service is eligible at its cause deadline `T_c` whenever `C > 0`. The
over-credit floor is eligible only at the owner fallback `T_f` and token
deadline `T_g`:

```text
T_optional = T_c                       when C > 0
T_floor    = max(T_f, T_g)             when T_g exists
T_floor    = T_f                       otherwise
T          = min(active future branches)
```

There is no wake when no cause is retained. If `T <= now`, eligibility is
retained as explicit due state rather than converted into a past timer.

## Symbolic obligations

### Capacity safety

`L <= K` follows directly from the outer `min`. For a target-bound batch, `K`
is exact at Product-queue admission. For a target-unbound tail, `K_u` is only a
global queue/event bound; native dispatch separately enforces the actual
target's exact `K_t`. In neither case can repair bypass queue reservation, the
MPP flow window, or native sender admission.

### Bounded extra traffic

Because `L <= max(C, F)`:

```text
D = max(0, L - C)
  <= max(0, max(C, F) - C)
  <= F
```

For a bound batch, `F_t <= M <= U`, `F_t <= M <= Q`, and `F_t <= A_t`; for an unbound batch,
`F_u <= A_u` and `F_u <= H`. Thus exhausted or partial credit can be crossed
by at most one bounded frontier quantum per accepted recovery epoch. The rule
is `max(C, F)`, never
`C + F`; optional credit and the frontier floor cannot be counted twice.

### Evidence monotonicity

For `0 < C < F`, the rule yields `L = min(K, F)`, not an all-optional request
for `F` that can enqueue zero. Reclassifying the same lowest frontier from a
silent tail to an authoritative gap therefore cannot remove already-earned
liveness. It also cannot create another opportunity because both observations
share `G`.

### Non-renewal and non-accumulation

Only a batch accepted while the token is available consumes `G` and fixes the
captured recovery interval. Contiguous unique Data-ACK frontier progress may
postpone the next availability using that already captured interval, but it
does not consume, renew, or mint an attempt. No other observational input
mutates the deadline. Before that deadline, another over-credit acceptance is
impossible; optional-funded acceptance remains possible and does not renew the
token. Polling after several elapsed intervals represents one available token,
not a count; the first accepted retry sets exactly one successor deadline from
its actual acceptance time.

Therefore target churn, actor wakes, queue churn, and evidence-shape churn
cannot reproduce the renewable critical-repair flood fixed by `0b50e9a`.

### Progress response

After an accepted attempt, contiguous unique Data-ACK frontier progress at
time `p` sets the next opportunity no earlier than `p + R`. This cannot move
an existing deadline earlier. A sparse suffix ACK changes neither the
contiguous frontier nor the token clock, so it cannot indefinitely postpone a
still-blocked lower range.

### Liveness under stated premises

If the recovery cause is due, `G` is available, `K > 0`, `F > 0`, the exact
range remains retained and unacknowledged, and a distinct native output
accepts its reservation, then `L >= F > 0` even when `C = 0`. Consequently one
frontier quantum is committed. The model makes no finite liveness claim when
there is no eligible distinct output, no Product capacity, no retained exact
range, or the native sender cannot accept work.

### Wake correctness

`T_c` is sufficient for optional-funded service; `max(T_f, T_g)` is the least
time satisfying both frontier-floor predicates. Conjoining optional work with
`T_g` delays valid service, while using `min(T_f, T_g)` for the floor can
evaluate before one authority exists. Filtering a past deadline can lose an
already-due evaluation or produce a re-arm loop. Explicit due state preserves
both branches without either failure.

## Directional and cause symmetry

Request and response senders each own one epoch for their own Product
direction. ACK-gap, contiguous-tail, and live FIN-tail observations in that
direction use the same authority and consume the same epoch. A target-bound
accepted attempt derives each frame's interval from its selected alternate
snapshot and uses their maximum for the batch; an unbound tail derives `R`
from the then-observed original-owner snapshot. Terminal failed-owner repair
remains isolated in both directions.

A response live FIN-tail that was ranked and sized from exact target `t` is a
target-bound transaction. Its queued cause carries that exact output
incarnation and a finite captured validity interval; Apply and native dispatch
may shrink or reject it but cannot silently retarget it. Target disappearance,
incarnation replacement, or expiry removes the queued intent and triggers a
fresh evaluation rather than leaving an impossible queue head.

## Required evidence before acceptance

Component acceptance requires deterministic tests for zero, partial, and full
credit; gap/tail/FIN transitions; target and queue churn; sparse suffix versus
contiguous ACK progress; wake conjunction; and terminal-failure isolation in
both directions. Runtime acceptance remains separate: the ordinary fixed-
request mixed 500 -> 10 -> 500 case must reduce the previously observed
1.688-second positive-read gap without goodput regression or a repair-traffic
burst. The wider frozen SEEN matrix remains open after this component passes.
