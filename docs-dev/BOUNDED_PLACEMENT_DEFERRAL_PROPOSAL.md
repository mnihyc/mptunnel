# Ordered placement needs a bounded wait action

2026-09-06 09:13 UTC. Pre-implementation proposal, not an accepted RFC change.
Verdict: the missing action is real, but the proposal is NOT implementation-ready.
Scope: the demonstrated busy-fast/free-slow OriginalData allocation decision.
Stale bulk handoff, native loss detection, metric observation and unrelated
closed SEEN cases remain separate. No new code or numeric knob is authorized
by this document alone.

## Why the present action model is insufficient

MIXED_RECOVERY_QUEUE_DIAGNOSIS records a new original assigned to a TCP output
with a 95.869-second estimated completion after filtering out the busy QUIC
frontier owner whose estimated completion is 2.382 seconds. Profile 7 currently
requires use of the sole immediately queue-admissible action. Thus changing
only the implementation would contradict the declared policy.

Let `A(u)` be arrival time of Product byte `u`, considering the earliest of its
physical copies. Ordered completion through offset `x` is:

`F(x) = sup(A(u) for u <= x)`.

Maximizing immediate publication or summed native rates does not minimize
`F`. For a conditional counterexample, a fast writer reopens in20 ms and then
serves64 KiB at500 Mbps; a free slow writer has4 MB ahead at4 Mbps. The next
range can finish in about21.05 ms on the fast writer versus over8.13 seconds
on the slow writer, before adding propagation. The existing rule chooses the
second action if it is the only writer accepting now. No change to transport
loss or congestion gains can correct that allocation choice.

This is conditional evidence, not a universal prediction theorem. The fast
path can fail just after observation, share a bottleneck, lose to another
flow's work, or have a stale rate. An indefinitely renewable ETA wait would
recreate the earlier restoration deadlock and is expressly excluded.

## Reference boundary

[ECF](https://api.repository.cam.ac.uk/server/api/core/bitstreams/9f216be1-4124-4f10-bbad-137e912cc7ff/content)
explicitly considers waiting for an unavailable fast MPTCP subflow instead of
using an available slower one, with remaining work and variation in its model.
Its congestion-avoidance assumptions and TCP-specific equations are not
automatically valid for MPP.
[BLEST](https://olivier.mehani.name/publications/2016ferlin_blest_blocking_estimation_mptcp_scheduler.pdf)
also explains why per-subflow queues can hold a later repair behind previous
work. These support the action distinction and queue concern, not an oracle
performance promise or copying their constants into MPP.

The Linux-native
[unsent-queue readiness option](https://www.kernel.org/doc/html/latest/networking/ip-sysctl.html#tcp-notsent-lowat-unsigned-integer)
is a separate adapter mechanism. It influences unsent queue admission and
writability, not the congestion window. No setting was changed: reducing it
alone would leave the allocation model and other platforms unresolved, and
could increase service/wakeup overhead.

## Preflight against the previously rejected T03 model

T03_ADVISORY_SCORE already rejected repeatedly choosing a static best path.
This proposal must not reintroduce that policy by repeatedly waiting for it.
In the recorded decision, the TCP estimate uses the portable approximately
351-Kbit/s compatibility rate; the kernel diagnostic rate for two siblings is
about 3.4--3.8 Mbit/s. Neither is an observed bound on the time to serve the
new range. The 95.869-second score therefore proves which advisory comparison
was available, not a 95.869-second physical service time or a safe wait budget.

A nonrenewable deadline for each source head is insufficient to prove useful
exploration. Suppose the underestimated path can provide 500 Mbit/s, but each
head waits for another path that reopens before that head's deadline. Every
head makes finite progress and the fast unknown path still receives no work
forever. This violates the previous explicit unknown-capacity obligation even
though finite-head liveness holds. Repricing a fresh TCP path at 351 Kbit/s
cannot solve this circular dependence.

The proposed defer action is consequently admissible only after establishing
compatible operational evidence for both alternatives, or a separately owned
discovery opportunity that cannot be perpetually postponed by successive
heads. No existing producer/owner has yet proved the latter contract. Missing
evidence is not equivalent to low service, and a diagnostic rate is not
silently promoted to NativeOperational authority.

The current Section 15.1 prohibition was clarified by docs-only commit
`ba177f3d`; it was not a newly introduced runtime comparison bug. A future
allocation policy permitting defer must explicitly supersede both this RFC
rule and T03's no-wait obligation, with singleton, discovery and failure
liveness replacing the stronger but performance-limiting rule. It must retain
the accepted partial-write transaction, high-BDP pipeline, exact range/slot
ownership and qualification separation. No implementation proceeds solely on
the strength of the conditional busy-fast/free-slow arithmetic below.

## Proposed ownership boundary, before choosing an equation

The sender needs typed alternatives, not `None` overloaded as every failure:

1. `Emit`: exact admitted output and immutable source prefix.
2. `Defer`: preserve that unassigned source prefix, one exact currently live
   preferred output, its evidence scope and an absolute nonrenewable deadline.
3. `Blocked`: genuine resource/lifecycle failure with its actual wake owner.

`Defer` does not assign an offset, reserve native capacity, reduce `W/P_i/E_i`,
or borrow native ACK as Product acknowledgement. It is an allocation action,
not another congestion window. It can only compare a material completion
advantage supported by current evidence; missing evidence cannot manufacture
a reason to wait. The current carrier can be either TCP or QUIC.

The first admissible implementation is restricted to a current live lower-
frontier owner with retained OriginalData. Its already-owned absolute progress
deadline and finite evidence validity are available liveness boundaries.
Capture the earlier boundary once for the source head. A later rate, native
ACK, jitter update, capacity notification, replacement or preferred-candidate
change must not extend this head's deadline. Expiry permits immediate current
ready-path selection for that head even if the preferred path still looks
better. Membership loss or evidence invalidation ends the wait earlier.
After actual source-prefix commitment, the next head may start its own choice.

This still requires proof that the selected evidence's completion comparison
uses compatible work/rate scopes and that the supposed preferred output is
structurally usable except for the named resource. Do not transplant the
Section10.2 static score as a sustained allocator. No new implementation
should be written before those producer and wake boundaries are settled.

## Symbolic obligations and practical acceptance

- Singleton preservation: without a distinct structurally live preferred
  owner, retain immediate work-conserving emission. Never wait on absent,
  drained, failed or revoked ownership.
- Finite-head liveness: if a ready survivor remains available, this head's
  advisory wait lasts no longer than its captured deadline plus actor service
  delay. Spurious wakes and changing estimates cannot renew it.
- Exact resources: successful emission still uses the existing source,
  incarnation, generation, Product credit and writer-reservation transaction.
- Event correctness: arm the actual preferred writer/membership wait, recheck
  the complete decision, then park with the same armed event and absolute
  deadline. A speculative reservation/refund must not drive self-wake loops.
- Conditional completion benefit: if the captured service bounds hold, defer
  only when waiting plus the preferred service finishes earlier than current
  emission after accounting for uncertainty. Without such a bound, say it is
  a heuristic and test its adverse countercase; do not call it a proof.
- Fairness/aggregation: no fixed protocol or same-host preference. A useful
  independent200-Mbps path must remain usable; mixed shared-cut service must
  not buy a mean-rate increase with long gaps or loaded-latency regression.

First counterexamples must cover busy-fast/free-slow, immediate fast failure,
perpetually changing estimates, no native telemetry, cold/unqualified paths,
equal-quality paths, capacity race and true singleton. Then run both request
and response implementations under the already defined QoS/jitter/loss/outage
and200-Mbps aggregation gates. Compare complete timing series. If the model
does not survive those cases, reject it rather than adding thresholds.
