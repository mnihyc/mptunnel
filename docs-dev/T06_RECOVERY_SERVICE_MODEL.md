# T06 live-owner recovery service

Status: implemented v0.4.8 candidate. The focused model and causal runtime
gates are green; uninstrumented matched performance acceptance remains pending.

## Scope and observed counterexample

T06 decides the extent of one recovery transaction while an OriginalData owner
is still live. It does not change recovery timing, target ranking, native
TCP/QUIC congestion control, ordinary placement, Product windows, operator
traffic hints, stale-output handoff, or exact-failure recovery.

Exact v0.4.7 diagnostics reproduced the response/download mixed TCP+QUIC
regression and split Product work by cause. One response stream placed 831.741
MiB of OriginalData on TCP and 37.813 MiB on QUIC, while accepting 410.56 MiB
of Product repair. Persistent Data-ACK gaps caused 253.98 MiB and stale-output
recovery caused about 154.8 MiB. QUIC-only sustained about 313--341 Mbit/s in
matched uninstrumented runs, whereas mixed service sustained about 203--217
Mbit/s. This is not evidence of a Quinn or QUIC congestion-control failure.

The 162 persistent-gap decisions ranked at most 2,365,200 bytes (2.256 MiB) of
lowest-frontier quanta, but admitted 266,315,936 bytes (253.98 MiB) of Product
repair: 112.6 times the ranked extent. The first decision ranked 14,600 bytes,
then exposed 10,667,416 bytes of target service. In later decisions one
evaluation admitted as many as 104 frames. This score/Apply mismatch is the
bounded defect.

## Why the defect exists

The cumulative optional-repair percentage was originally introduced after a
full-window renewal replay generated 434,790,952 repair bytes against a
108,847,604-byte envelope. Preventing an unbounded second congestion window
was a valid objective, but making an operator traffic preference decide
recovery reachability and extent was not.

T05a therefore added exact range/configured-slot publication identity, and
T05b correctly removed the percentage from authority. The frozen T05 model
explicitly deferred sequential, staggered, or concurrent recovery service to
T06. Current range/slot uniqueness proves a finite number of current
publication owners, but the implementation still ranks one small frontier
quantum and then appends a suffix up to the selected target's Product service
window. Removing percentage authority while leaving that suffix reopened the
known amplification family. T05 identity remains valid; its declared T06
service layer is unfinished.

## Native recovery and Product migration are different operations

For one logical-stream direction, let:

- `f` be the lowest missing Product offset;
- `r_f` be the maximal retained range at `f` with one unchanged live owner and
  unchanged accepted-copy owner set;
- `T(f)` be the existing immutable recovery cause;
- `A(f,t)` and `L(f,t)` be T05's exact structural authority and target service
  extent for target `t`;
- `H_f` be the exact lowest Data-ACK gap extent;
- `I_f` be the owner-uniform retained frontier extent; and
- `Q_f` be the existing common adaptive repair quantum captured before target
  ranking.

The already-ranked positive frontier extent is:

```text
M_f = min(Q_f, H_f, I_f)
```

A complete Data-ACK gap proves Product reordering or loss. It does not prove
that a live TCP or QUIC owner has lost native recovery authority. Live-owner
recovery is therefore one speculative latency hedge, not bulk migration:

```text
A_live(f,t) = A(f,t) && T(f)
L_live(f,t) = min(L(f,t), M_f)
```

Apply may shrink this exact prefix for current target/native capacity but may
not enlarge it or append an unscored suffix. Existing accepted-copy range/slot
identity and its immutable suppression deadline continue to serialize another
attempt for the same range. Existing cause clocks and target ordering continue
to decide when a later transaction is considered. T06 adds no timer,
percentage gate, topology assumption, or concurrent fanout.

An exact failed or detached owner has no remaining native recovery authority.
Its existing cause-bounded recovery remains immediate and may use the full T05
target service extent:

```text
L_fail(r,t) = min(K_t, bytes(r), Q_fail(r,t))
```

A declared-stale attachment has already survived the separate persistence
test and is withdrawn from new OriginalData placement. Its current bounded
handoff is unchanged in this transaction. If the post-change trace still shows
stale-output handoff as an independent dominant amplifier, it requires its own
model and proof; the downstream observation is not silently bundled here.

## Symbolic properties

### Percentage invariance

For fixed Product/lifecycle/native state and any configured percentages `p1`
and `p2`:

```text
A_live(S,p1) = A_live(S,p2)
L_live(S,p1) = L_live(S,p2)
```

The percentage remains accepted-recovery accounting and an operator hint. It
cannot enable, deny, advance, postpone, or resize recovery.

### Observe/Decide/Apply range preservation

For the exact ranked frontier `R_decide = [f, f + M_f)`, every accepted range
in that transaction obeys:

```text
R_apply subset_of R_decide
bytes(R_apply) <= M_f <= Q_f, H_f, I_f
```

Changing target headroom `K_t`, the larger owner-uniform suffix, metrics, queue
wakes, port, or carrier incarnation cannot enlarge `R_decide` after ranking.

### Bounded speculative work

One active-live transaction admits at most the existing Core service quantum
(`M_f <= 64 KiB` with default geometry), additionally bounded by `L(f,t)`.
This is a per-transaction structural bound, not a cumulative throughput cap.
It neither changes ordinary OriginalData nor claims a finite physical-wire
bound across an unbounded sequence of exact failures.

### Conditional liveness

If a range remains missing, a structurally eligible target eventually obtains
native admission, actor scheduling is fair, and at least one attempted native
domain supplies finite service, existing cause/suppression transitions permit
later finite attempts. T06 cannot promise delivery when every native domain
supplies zero service. Exact failure retains its separate immediate branch.

### No congestion-controller duplication

TCP and QUIC still own packet recovery, congestion windows, pacing, and native
retransmission. T06 neither estimates shared bottlenecks nor installs a coupled
congestion controller. Concurrent fanout remains rejected because it has no
topology-neutral non-downgrade proof.

## Required RED/GREEN evidence

Before implementation, exact tests must fail on v0.4.7 and express:

1. a due response live-gap transaction with `K_t > M_f` admits exactly `M_f`,
   not the complete target window;
2. the request direction has the same production-reachable invariant;
3. percentages zero, default, and large leave positive eligibility and exact
   extent unchanged;
4. existing same-range suppression and stable-slot publication tests remain
   green; and
5. exact path failure still admits bounded service beyond one frontier quantum
   when retained work and target capacity allow it.

Post-change runtime evidence is intentionally narrow:

- rerun the same diagnostic mixed cell and require every persistent live-gap
  accepted batch to be no larger than its captured frontier limit; the
  112.6-times suffix pattern must disappear;
- rebuild without diagnostics and compare matched QUIC-only and TCP+QUIC
  service; mixed service must not materially degrade the healthy QUIC result,
  loaded latency, or forward-edge traffic; and
- retain the focused exact-failure and QoS-recovery guards.

Failure of that focused runtime gate rejects this model and ends the
transaction. It does not authorize allocator, congestion-control, dashboard,
staleness, or TCP-HOL changes.

## Candidate evidence

The pre-implementation request and response integration tests both failed on
the v0.4.7 behavior because one ranked frontier frame expanded into a
multi-frame suffix. The shared authority helper and all active-live call sites
now apply:

```text
service_limit = min(target_service_limit, ranked_frontier_limit)
```

The same tests are green with no queued unscored suffix. Symmetric completion
tail, percentage-invariance, exact-target Apply, capacity-wake, and
multi-quantum terminal-failure tests are also green. All-feature/all-target
Clippy with warnings denied and the documentation contract suite are green.

The identical diagnostic mixed cell produced 461 accepted persistent-gap
evaluations. Every evaluation had `service_limit <= base_limit`; accepted
persistent repair fell from 266,315,936 bytes to 6,712,728 bytes (97.479
percent), and total Product repair was 7,255,320 bytes, or 0.676 percent of
1,073,742,032 OriginalData bytes. This proves removal of the observed
score/Apply amplification. Its instrumented throughput is not performance
acceptance. The remaining gate is the documented clean, uninstrumented,
matched QUIC-only/TCP+QUIC comparison.
