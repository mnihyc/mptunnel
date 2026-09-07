# Next bounded placement obligation

Date: 2026-09-07 06:44 UTC. Category: model preparation for the existing
mixed-placement issue; no new runtime algorithm or parameter accepted.

## Keep three causal transactions separate

1. The raw-byte veto overrides an otherwise satisfied timing hysteresis.
   The exact ready/qualified witness has a0.699ms advantage within19.407ms
   jitter. Its isolated deletion has a passing component proof but failed
   ordinary candidate acceptance; it remains removed. A later lifecycle fix
   does not retroactively turn that comparison into a pass.
2. Filtering out a busy fast carrier before comparing it with a free slow one
   can create avoidable ordered-prefix work. The existing RFC requires that
   immediate-admission behavior, so a wait action changes policy, not merely
   implementation. Its liveness and evidence obligations must be explicit.
3. The latest QoS repair is admitted35ms after the missing prefix appears,
   then takes2.737s to decoded receipt while substantial shared queued work
   drains. This is not evidence for a new repair-timer or congestion-gain fix.

## What a completion comparison must mean

For Product byte `u`, let `A(u)` be earliest actual receipt over its copies.
Ordered completion through `x` is `F(x) = sup(A(u): u <= x)`. Higher summed
native rates or more immediately accepted commands need not reduce `F`.

A common completion advisory must compare matching quantities: one logical
stream, exact output incarnation, sender direction, unique Product work,
observation clock and expiry. Its retrospective achieved service is not native
capacity, a future lower bound, or permission to change Product/native credit.

The current producers are not a drop-in common observation. TCP uses Product
ACK spacing; QUIC's numeric Product history uses assignment-to-ACK residence,
smoothing and an application-limited maximum. Active QUIC selection instead
uses native capacity. Two64KiB batches assigned at zero and acknowledged at
1.0/1.1s produce about5.24Mbps from the TCP spacing rule versus0.512Mbps from
the QUIC residence rule. This is a falsifiable semantic difference, not proof
that changing the unused QUIC diagnostic history would improve selection.

The next finite deliverable is an explicit **two-candidate completion-evidence
contract**, with these acceptance obligations before any implementation:

- Identical eligible Product histories have the same meaning regardless of
  TCP/QUIC labels. Define the observation denominator and sampling/expiry
  once, rather than mixing two different measures in one scalar.
- Match outstanding Product work to that per-flow service; do not divide by
  active flow count again or add overlapping native flight as new Product work.
- Duplicate/copy-ambiguous ACKs, replaced instances and old epochs cannot
  create new path-specific evidence. Unknown, stale and allocation-limited
  observations remain explicit; none becomes a measured low-rate ceiling.
- Preserve native congestion, pacing and rate-authority ownership. A Product
  completion advisory must not be relabeled NativeOperational capacity.
- Establish whether a useful wait can be supported by the compatible evidence
  actually available. If not, reject the proposed wait instead of adding an
  ungrounded sampling/window/discovery parameter to make the fixture pass.

## Conditional benefit and decisive counterexample

If a busy carrier really becomes writable in20ms and then serves64KiB at
500Mbps, completion takes about21.05ms before propagation. A free carrier
with4MB ahead at4Mbps takes8.13s. That conditional inequality justifies
considering a wait; it does not prove those future service conditions exist.

An underallocated500Mbps carrier and a genuinely slow carrier can produce the
same finite ACK history. Freshness and a normalized clock cannot distinguish
them. Even a nonrenewable deadline per head can starve the underallocated
alternative forever when another carrier reopens before every head expires.
Therefore finite-head progress is not finite discovery. Do not revive the
rejected static-rank prototype or disguise startup priors as measured rates.

Fresh-comparable-only deferral is the smallest proposed boundary without new
discovery ownership, but it leaves the observed unknown-prior branch unresolved.
It is not an all-issue solution and is not authorized here as a production fix.

## Execution order

The independently proved late-enrollment scope correction is separate and
committed as11d6f3a. Preserve its34 distinct passing controls. Freeze the
ordinary clean executable and check bidirectional timing on the unchanged
profile. Then use the common-evidence obligation above to decide the next
placement change. Do not add another timer tweak, unused scorer or new issue
inventory. Global aggregation, baselines, browser and sustainability gates
remain in CURRENT_CLOSURE_PLAN; no release or final performance claim follows.
