# A pending native write does not own Product mailbox readiness

2026-09-06 09:23 UTC. Pre-implementation causal model. This is the next
bounded branch of MIXED_RECOVERY_QUEUE_DIAGNOSIS, not an allocator change.

## Reproduced cause and original intent

In `mixed-combined-down-qualified-input-deferral-0906`, TCP path 1 retains a
STREAM_ACK after a full Product mailbox at diagnostic time 12.767 s. It does
not return to that input until its write completes at 24.940 s: 12.173 s later.
Other deferred frames are STREAM_MAX_DATA, plus PATH_METRICS and STREAM_DETACH
at their normal actor boundaries. The run averages 79.518 Mbps and has a
6.028-second application gap. These observations are causal actor evidence,
not proof that this is the only cause of that application's gap.

The interlock was introduced to preserve one in-flight write/flush transaction
while processing peer feedback. One deferred frame bounds memory and preserves
carrier input order; genuine actor/lifecycle barriers must still be deferred.
However, mailbox-full and an actor-ordering barrier have different wake owners.
The current code stops input polling for either and waits only for the native
write. A Product consumer can free its mailbox without completing that write.
Consequently feedback remains parked even after its actual obstruction clears.

The same distinction is lost in the common QUIC write interlock and in client
TCP routing. This is one resource-wake model defect in corresponding branches,
not authorization to change transport recovery or lifecycle semantics.

History confirms that the common QUIC wait loop already has this conflation
in `3f40ca9` (2026-07-17). The current server TCP transaction has it in the
`3a6d0ea` v10 checkpoint. This is not evidence that the recent qualification
projection change introduced the obstruction, nor that the accepted R1
partial-write ownership correction should be reverted. R1 protects the write;
the missing mailbox wake is an orthogonal requirement.

## Required transition model

There are three routing outcomes:

- Routed: the exact frame has transferred to its Product owner.
- Mailbox pending: one retained frame, the exact recipient channel, and that
  channel's capacity wait. Native write and capacity remain independently
  polled. No later input overtakes the retained frame.
- Actor barrier: retain the frame for the existing outer actor after the
  protected native write completes. Requalification reply-credit pressure and
  terminal/lifecycle transitions retain this class unless separately proved
  safe; they must not wait for a resource only this writer can release.

Mailbox delivery must reserve the real recipient slot and transfer the frame
in the same poll/commit. It must not reserve then drop credit merely to emit a
speculative wake. Before successful transfer, cancellation when the write wins
must return the exact retained frame to ordinary actor handling, not drop or
duplicate it. Closing an old recipient must retain the caller's existing
stream-local versus carrier-terminal interpretation. No registry relookup may
silently deliver an old frame to a replacement recipient.

At most one not-yet-routed input is retained per existing interlock. The
mailbox capacity and writer transaction are unchanged. Native partial writes
are never restarted, cancelled for a capacity event, or re-encoded. Completion
evidence and writer debt remain published only at the current write+flush
boundary. No new timer, buffer size, traffic budget, rate source or scheduler
preference is needed.

## Proof obligations and acceptance

Let `Q` be the recipient mailbox, `m` the single retained frame, and `W` the
pinned write. If `Q` eventually supplies a reservation and actor service is
fair, delivery of `m` must not require completion of `W`. If `W` completes
first, `m` must remain owned exactly once for normal dispatch. If a genuine
ordering barrier is retained, later input and clean EOF must not overtake it.
These are conditional progress and ownership properties, not throughput
claims or permission to read without bounds.

Implementation may use one typed pending-mailbox object that owns the frame,
an exact recipient reservation future, and the preexisting closed-recipient
policy. The native write and that future remain independently polled. Acquire
and consume the real mailbox permit without a cancellable await between frame
transfer and send. If the write wins before that commit, cancel only the
reservation wait and recover the frame unchanged. A channel reservation is
mailbox credit, never transport service or Product ACK authority. A type-erased
permit at the existing carrier/Product port can keep server event internals
out of the carrier API; do not move unrelated lifecycle ownership to implement
the wake. There must still be at most one retained ingress frame.

First RED uses the real client QUIC routing function and production common
interlock: fill a one-slot Product mailbox; observe a second feedback frame
become pending; free the first slot while keeping the native write pending;
require that exact feedback to arrive before releasing the write. RED is
confirmed: `routed=0, retained=true` after the recipient slot was freed while
the write remained pinned. The focused release test failed in 0.10 s after
the 7m38s test build. It depends on no packet-loss or congestion assumption.
The pre-fix test is archived in MAILBOX_WRITE_WAKE_RED.patch. No runtime
correction or GREEN result is claimed yet.

The implementation must also test write-wins cancellation, receiver closure,
original input order, terminal-before-EOF, and requalification reply-credit
pressure. Corresponding TCP and QUIC, client and server producers must preserve
their lifecycle effects. Then rerun the ordinary mixed/QUIC timing case and
affected upload/failure cases. A component pass is not performance acceptance.
Do not use a mailbox fix to waive the independent busy-fast/free-slow
allocation, stale-handoff backlog, or native observation issues.

## Candidate boundary (not yet accepted)

The candidate adds `runtime/path/input.rs`: a three-way carrier input outcome
and one exact-recipient pending-mailbox object. It retains the frame separately
from a cancel-safe owned-slot reservation. Once the slot is ready, its
type-erased send permit wraps and transfers the frame without another await.
This preserves the neutral carrier/Product port rather than exposing server
event types or moving lifecycle state between layers. Allocation occurs only
after an actual full mailbox, not for ordinary successfully routed frames.

The registry returns this pending object for a full Product event queue, while
requalification reply-credit pressure remains an actor barrier. Server TCP and
the common QUIC interlock poll the mailbox independently of the pinned writer.
Client TCP retains its exact stream id and reset-retirement effect until the
pending delivery succeeds or its recipient closes. Client QUIC retains its
carrier-closed error policy; a retired server Product stream remains local and
does not fail a shared carrier. The test-only capacity-probe branch uses the
same mailbox wake, without changing its diagnostic authority.

The first `cargo check --tests --locked` completes, with only the existing
default-feature dead-code warnings. The final cancellation/closed-recipient
tests were added afterward and the focused release test build is pending.
The prior production-interlock RED, now adapted to the typed outcome, remains
the GREEN obligation. No isolated performance benefit, no independent audit
sign-off, no accepted runtime commit and no release are claimed at this point.
