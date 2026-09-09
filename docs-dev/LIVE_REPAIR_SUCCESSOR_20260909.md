# Live-owner repair successor characterization — 2026-09-09

Status: **existing model boundary characterized; no new recovery policy selected**.
One corrected production-path fixture passes. It demonstrates ACK-serialized
successor service in response `FinalDrain` while both attachments remain live.
It does not reproduce a critical network stall or establish a performance gain
from changing that boundary. The characterization is archived, not installed
as a requirement to preserve an unselected model forever.

## Question, origin and competing effects

The predeclared question in CURRENT_CLOSURE_PLAN was whether an accepted head
copy on live alternate B prevents that same available alternate from serving
a distinct, already-mature successor before Product ACK advances. Independent
producer and contract reviews agreed on the source mechanism: the producer
restarts at the lowest unacknowledged Product offset; accepted B ownership
excludes B there and ends the uniform ownership/avoidance prefix. Draining a
native command does not retire Product copy debt. Stale/failed recovery has
separate authority, so this is not a universal stream throughput ceiling.

This follows [T06](T06_RECOVERY_SERVICE_MODEL.md), not a loop typo. Its original
correction addressed162 decisions that ranked2365200B but admitted266315936B
of repair:112.6 times the ranked extent. One14600B score exposed10667416B of
target service. Preserving rank/Apply extent and exact copy ownership was
justified; undoing that protection is not a clean takeover model. The historical
matched T06 result was336.156Mbps QUIC versus332.393Mbps mixed with80/80 echoes
each. That bounded result remains provenance, not present global acceptance.

The information forecast was deliberately narrower than a speed forecast:
establish actual admission, successor maturity and remaining target service;
then distinguish a Product-frontier restriction from a fixture missing native
authority. If the exact boundary exists, assess its practical relevance before
changing it. The competing cost is real: the fixed healthy
[repair window](REPAIR_WINDOW_20260909.md) contains no winning copies and all
347856 clipped accepted-copy bytes arrive187–250ms after Originals. That
window cannot be extrapolated to outages, but it prevents treating more repair
work as an automatic improvement.

## Actual characterization and fixture corrections

The temporary test is
`accepted_live_frontier_copy_waits_for_product_ack_before_serving_its_successor`
in `src/runtime/relay/tests_server.rs`. The243-line test-only patch changes no
runtime, RFC, controller, timer, threshold or resource limit. Its actual sequence:

1. Admit65536B of Original data through `ServerResponseSenderService`, the real
   send cache and live TCP attachment A. Drain its queued command, then queue
   FIN so the existing response `FinalDrain` producer is exercised.
2. Attach a distinct TCP slot B with the existing delivery-evidence fixture.
   Age the actual Original assignment before its first deadline observation;
   use real `Instant::now()` thereafter. Neither attachment is declared stale,
   failed or detached.
3. Queue and actually dispatch the first repair to B, obtain its real native
   command, then release command pending bytes. Record its positive quantum q;
   the fixture requires that two such quanta fit in the Original extent.
4. Independently observe `[q,2q)` as mature, retained and owned only by A; check
   that B is selected for that exact frame and retains at least q service.
   B still owns q accepted-copy bytes and Product ACK remains0. Reinvoking
   the production `FinalDrain` producer queues no successor.
5. Feed only the actual first-copy payload into `ReliableRecvStream`, obtain
   its ACK, validate/apply it against the real send cache, and release exact
   binding/sender copy ownership. Product ACK becomes q and B copy debt becomes0.
   The same producer now queues and dispatches a nonempty successor at q to B.

Command dequeue is deliberately not called successful physical transmission.
The receiver/ACK is an explicit simulated event using production APIs. This
isolates logical eligibility from a full queue; it is not a timed network run.
The executable proof covers response completion-tail `FinalDrain`, not a
direct active ACK-gap, request/upload, QUIC-native or repeated-RTT performance
test. Broader source symmetry is not silently counted as additional test coverage.

The first fixture did **not** establish Product RED. It used a generic queue
as a QUIC alternate without the native authority required by real admission,
so dispatch returned `SenderServiceBlocked` before the intended successor
assertion. Its build took2m35s; result0 passed/1 failed. Independent review also
caught a clock weakness: supplying a future evaluation time while actual copy
admission/suppression uses the real clock does not characterize one coherent
current state. The corrected fixture uses distinct TCP slots and ages only
the existing Original before the first observation, with real clocks thereafter.
No fake QUIC capacity or production predicate bypass was added to obtain PASS.

Parent-run corrected log: build28.91s,1 passed/0 failed/2441 filtered,
test duration0.00s. The assertion `blocked.queued == 0` intentionally describes
current behavior; changing its expectation to1 would be a proposed policy
requirement, not proof that the current model violates its own contract.

[Raw archive](LIVE_REPAIR_SUCCESSOR_20260909.raw.tar.gz) retains
`./.tmp/reflection/live-repair-successor-characterization-0909.patch` and the
initial/fixed logs of the same basename (`-fixed-0909.log` for the correction).
Parent verification passes gzip integrity and decompressed byte comparisons.
No executable is needed for this test-only artifact. The patch is removed from
runtime test source after preservation; no regression obligation is installed.

## Conditional service bound, not attribution

If only q bytes can advance through this one alternate in each Product-ACK
cycle of duration tau, Originals make no useful progress, and no stale/failed
transition or other eligible path supplies additional service, then:

```text
repair-served goodput <= 8q / tau bits per second
q = 14600B, tau = 0.100s  => 1.168Mbps
q = 65536B, tau = 0.100s  => 5.24288Mbps
```

The examples are calculations, not measured fixture rates or fixed Product
quanta. Actor scheduling, recovery maturity and native admission can make
service slower; Original progress, additional useful paths or structural
recovery invalidate the premise. A500Mbps alternate does not remove a logical
ACK-cycle ceiling, but no existing critical timing capture proves that this
ceiling is the dominant cause of its stall. No Mbps gain is forecast from the
equation alone.

An ACK-renewed global quiet deadline `G_s` would permit continuing small/trickle
ACK progress to postpone service indefinitely, despite older mature ranges.
A one-quantum-per-RTT/token gate merely reinstalls `8q/tau` independently of
available capacity. Hard gating by traffic hints would also undo the prior
separation of operator preference from recovery authority. None is selected.

## Why an accepted-copy cursor or pipeline is not selected

An accepted copy is an additional attempt, not receipt. Treating its frontier
as the Product ACK frontier would retire unresolved data or abandon the lowest
hole incorrectly. A separate scheduling cursor could preserve receipt truth,
but requires a genuine service policy: each successor must have independently
ranked mature authority, exact range/slot debt, live target admission and a
retained oldest-head obligation with failed-copy retry. Simply appending the
target window after one successful head score reintroduces T06's defect.
Even independently legal per-quantum decisions can pipeline losing copies
across the pending extent and revive the practical amplification problem
without a literal rank/Apply mismatch. Sparse ACK splits, detach re-exposure
and bounded continuation work would also need coherent ownership.

Two network histories can present the same sender observations: A's Original
is still delayed/lost and B's future copies would win; or A has already delivered
but its ACK is still returning and B's copies would duplicate healthy service.
Native queue room, copy acceptance and missing feedback alone do not separate
them. The healthy window's Original-before-queue56–72ms is comparable to70ms
configured return propagation, not proof of faulty publication. A pipeline
can improve the former history while adding load, queue delay and redundant
delivery in the latter. This is an unresolved tradeoff, not a proof that all
pipelined designs are incorrect or that no useful improvement is possible.

Consequently the characterized boundary does not justify an accepted-copy
cursor, protocol preference, G renewal, traffic cap or speculative pipeline.
Its performance benefit must be tied to actual critical service, with the
healthy duplicate cost and existing blackhole recovery preserved.

## Historical-stall attribution correction

The7–14s citation in REPAIR_WINDOW refers to `PROGRESS.md:6377`, the2026-07-31
feedback correction: ACK/MAX publication could be accepted only by a locally
live but wire-blackholed selected attachment. `5e1ace67` retained publication
across attachments. Its cited raw results are outside the current reflection
artifact set. This is a reason to preserve independent feedback, not evidence
of the present live-owner successor ceiling.

Existing exact mixed traces also do not establish repeated head-copy/ACK cycles
as their critical cause:

- `mixed-combined-down-qualified-frontier-trace-0906/` has a5.541906s body gap.
  Head297186476 already receives full TCP and prefix QUIC repairs, followed by
  stale `no_target` decisions and another full TCP repair. Native queues hold
  megabytes; there is no proved vacant healthy target awaiting only next-q ACK.
- `mixed-combined-down-product-native-qos-diag-0907/` queues/dispatches QUIC
  repair for218414896 only35ms after exposure, then waits2.737s for reassembly
  during substantial10Mbps queued service. Prompt wakes rule out forgotten
  repair admission for that interval; precise post-admission residence remains
  unresolved. See PRODUCT_NATIVE_QOS_FRONTIER_ATTRIBUTION_20260907.
- In `mixed-combined-down-qualified-repair-refusal-0906/`, the7.153011s maximum
  application gap is27.101404–34.254415s at Product309898762, not the separate
  approximately4.108s hole242855434 analyzed in MIXED_RECOVERY_QUEUE_DIAGNOSIS.
  The actual maximum follows a caught-up ACK and new TCP Original dispatch
  (`server.log:26307–26308`); no covering repair is recorded, but healthy
  alternate eligibility/refusal at the necessary instant is not established.

All these paths are under `./.tmp/reflection/results/`. Their source versions,
observer coverage and clock domains remain distinct. None retroactively proves
a performance cause for this new `FinalDrain` characterization.

## Disposition

The actual boundary is model-constrained and now executable, not an unexplained
fixture failure. Its critical real-world impact and best tradeoff remain
unproved. Preserve T06 rank/Apply, exact ownership and structural-failure
recovery; no runtime/RFC prototype or public performance claim is selected.
The next predeclared same-build unsafe live-hedge suppression comparison is an
information experiment about cost, not a pipeline fix or release candidate.
CURRENT_CLOSURE_PLAN remains the sole next-action ledger.
