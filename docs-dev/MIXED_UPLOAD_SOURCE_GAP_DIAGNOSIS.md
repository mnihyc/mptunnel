# Mixed upload: separate assignment gaps from transport delivery gaps

2026-09-06 10:33 UTC. Continuation of MAILBOX_WRITE_WAKE_MODEL, not a new
optimization scope or accepted runtime correction. Ordinary endpoint
isolation follows the changed server; the source-ready/input-priority
obstruction is now captured below. This identifies one real sender-service
defect, not every remaining stall. No congestion-controller change is
authorized by this document.

## Trace and coverage

The first existing-feature trace uses old diagnostic client with the mailbox
candidate diagnostic server. It completes with1,182,859,264 target-confirmed
bytes. Every original byte is covered by18,275 client assignment records with
no gap, overlap or duplicate original offset. Native copies and repairs remain
distinct from these original assignments. Upload payload begins at Product
offset0; this is not the HTTP download trace's208-byte header mapping.

The diagnostic run's173.860 Mbps is not ordinary acceptance evidence. Enabling
the existing `reinjection` event produces711,553 lines, mostly attempted stale
handoff work already queued. The diagnostic control produces over1.19 million
`stale_path_reinjection queued=false` lines and167.308 Mbps, unlike its ordinary
237--260 Mbps controls. That observer cost invalidates a diagnostic speed
comparison. One log line is a per-range attempt, not necessarily one actor
iteration; do not call these counts proof of a million-iteration busy loop.
The next trace omits this noisy event without changing recovery behavior.

Common-VM Unix timestamps align assignment and server delivery. Process-local
monotonic origins differ and are not silently equated.

| Server gap s | Frontier before delivery | Original carrier | Assignment to server delivery | Attribution boundary |
| --- | --- | --- | --- | --- |
| 3.009157 | 584767531 | QUIC0 | 34 ms | Next original was not assigned during most of the gap. |
| 1.665369 | 754803915 | QUIC0 | 31 ms | Next original was not assigned during most of the gap. |
| 5.202601 | 1165030637 | TCP2 | 190 ms | Next original was not assigned during most of the gap. |

Other gaps do involve already-assigned originals and repairs. For example the
gap at1097921773 has a QUIC original and TCP repair attempts during the forced
UDP outage. Do not merge that native/ordered recovery case into the source
assignment gap. Nor does an unassigned gap by itself establish a software bug:
genuine receive credit, Product resource bounds or all native writers blocked
could correctly prevent assignment. Their actual state must be measured.

The exact matching assignment and stall records are retained in the trace
evidence. The16 ordinary comparison/endpoint results remain in
MAILBOX_WRITE_WAKE_EVIDENCE_20260906.json, and are not overwritten by traces.

## Specific pre-implementation lead: drain-to-empty input priority

`relay/control.rs` computes `inbound_frame_ready` from the deferred frame or
nonempty Product input channel, then disables both queued sender service and
new source reads while it is true. Commit9505d7f introduced the rule to process
buffered feedback before more data: ACKs, grants and lifecycle state should
affect the next assignment. This is sound intent, but an emptiness test is not
a bounded or class-aware service turn. The input channel also carries ordinary
received Product data, not exclusively lifecycle/control feedback.

Conditionally, if the input channel remains nonempty while source and native
output are ready, these guards prohibit outgoing work regardless of available
credit. A continuously replenished backlog can therefore postpone sending for
the full backlog interval. This does not prove that the measured gaps satisfy
that condition, or that arbitrary removal preserves existing lifecycle and
precommit obligations. A model correction would need finite feedback service,
within-class progress, and exact terminal/admission ordering together; a timer
or protocol preference is not a substitute.

Next expose the existing diagnostic gate states: source-output presence,
sender queue bytes, queued-send blockage, inbound readiness, ready-frame count
and prospective read budget. The source change is diagnostic-feature-only;
the production gate, recovery policy, queue sizes and timers stay unchanged.
Trace new diagnostic client against each server binary, using the same
conditions but excluding the per-range reinjection spam. Only if a real
source-ready/input-priority obstruction is captured should the model and its
RED/GREEN be designed. If another gate is responsible, record this lead as
unconfirmed and follow that exact owner instead.

## Captured gate obstruction

The quieter `mailbox-source-gates-0906` trace holds source offset999759735
from Unix1788690499627 through1788690502260, a2.633-second interval. It records
2,626 source-blocked observations with all of:

- a current source output;
- positive receive credit and positive prospective source-read budget;
- no queued-send retry blockage; and
- buffered inbound work disabling both source reads and queued sending.

At the first such observation, credit is172,608 bytes, prospective read budget
172,608 bytes, sender queue14,600 bytes, and input queue38 frames. At the last,
all prior Product flight has been acknowledged, credit is67,108,864 bytes,
prospective read budget524,288 bytes, sender queue0, and input queue129 frames.
An earlier sample legitimately has zero Product headroom and is excluded from
the2,626 input-only observations. Do not mislabel that resource boundary.

The next original is assigned to QUIC at Unix1788690502277 and reaches the
server33 ms later. The server's longest gap is2.287159 seconds because it still
had earlier in-flight bytes to deliver at the beginning of the client stall.
Thus a delivery-rate increase cannot release these still-unassigned bytes.
The sampled sender gate prevented even attempting their assignment while
the Product credit and source-read resources were available. This does not
claim that every hypothetical attempted writer reservation would succeed.

The same diagnostic client with the old server gives297.557 Mbps versus
228.481 Mbps with the changed server. These are diagnostic random trials, not
replacement ordinary acceptance. The ordinary endpoint pairs already placed
the adverse interaction at the changed server. Faster mailbox service exposes
the existing client drain-to-empty policy; reverting correct mailbox wake or
changing native congestion gains does not correct that policy.

The current RFC's final-carrier-writer class priority is not a justification
for starving Product actor source service whenever its receive queue is
nonempty. The legacy9505d7f RFC rule and implementation had intended prompt
feedback processing, but they did not establish bounded input-turn fairness.
The input channel includes payload as well as control. Any replacement must
state where each priority applies, not reuse a writer-priority clause for all
actor work.

## Required next model, before implementation

Use bounded feedback processing and an explicit fair opportunity for other
ready actor work, preserving the useful intent that already observed ACK,
credit and terminal state informs new assignments. Do not merely remove the
two boolean guards or invent a timer/queue threshold. Required obligations:

1. A continuously replenished input tail cannot indefinitely extend one
   feedback turn. No wait is required when no other work is ready.
2. Coalescing state-like ACK/MAX_DATA is permitted only after preserving exact
   Product identity, individual ACK validation against a frozen send extent,
   positive coverage, complete-snapshot negative authority, monotonic grants
   and carrier-incarnation semantics. Do not blindly copy the server coalescer
   across the client's instance-tagged input boundary.
3. Data and lifecycle boundaries retain their ordering. Known terminal or
   admission changes must still invalidate stale output commitments. Existing
   partial native/local writes, ACK ownership and final-offset behavior remain
   untouched by this arbitration correction.
4. Source/sender service retains its existing finite byte/item quantum and
   all real Product and writer admission checks. Fair opportunity is not a new
   send credit, native rate authority, congestion window or guaranteed native
   completion deadline.
5. RED must hold inbound work ready while source and an output are ready and
   show lost productive service. GREEN must demonstrate productive progress
   while retaining feedback and lifecycle processing, then repeat the same
   ordinary mixed/QUIC download/upload comparisons and the relevant terminal
   guards. A good mean alone remains insufficient.

The diagnostic field patch is archived as CLIENT_INPUT_PRIORITY_DIAGNOSTICS.patch
and removed from active source after saving the executable. No actor-policy
implementation, accepted runtime commit or release is claimed here. The
separate busy-fast/free-slow ordered-placement limitation and all global gates
remain open.
