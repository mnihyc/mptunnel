# Repair receive EOF and ordinary terminal ownership

2026-09-07 02:13 UTC. Existing SEEN sustainability transaction. This concerns
the explicitly held companion-stream composition, not an accepted release or
attribution of the uncaptured deployed RAM incident.

## Proven symptom and bounded attribution

SUSTAINABILITY_CHURN_20260907 records two ordinary completed-request cycles:
1,932 successful 100 KB HTTP requests, zero client owners after load, but
779 then1,571 retained server logical/admission owners. Server queues and
Product/native flight clear while RSS grows from about30 to352MiB. This is
live ownership, not only allocator page retention. Eleven component ownership
tests pass but do not establish that assembled normal termination reaches
those release operations.

The diagnostics-off fixed32-request mode ablation on the same frozen
current-ack-rtt binary completes every request: TCP retains0 server owners,
QUIC23 and mixed24 after about4.5s quiet. Clients and per-path flow counts
return to0. A separate QUIC observation retains25 owners after31s. Exact
request results, observation durations and counters belong to
CLOSED_REQUEST_ABLATION_20260907.json; these are lifecycle discriminators,
not comparative throughput rankings.

Independent code audits agree on this reachable causal chain:

1. Ordinary and repair bytes have separate native ordering domains.
2. The repair reader returns `Err(StreamFinished)` on clean native receive EOF.
3. `try_join!` propagates that error; the parent `select!` cancels the ordinary
   task even if its independently ordered final FIN/ACK is still pending.
4. The attachment guard detaches its output. Generic detach deliberately does
   not invent Product FIN or reset: the logical half-open stream is retained
   for legitimate carrier recovery.

The first actual-H3 fixture reproduces exactly
`Ready(Err(QuicCarrier(StreamFinished)))`. The initial fixture consumed native
EOF twice, which could race optional datagram-route closure on another run;
the final fixture therefore invokes the named production read-half operation
once. No sleep, throughput threshold or inferred EOF is its precondition.
GREEN and assembled ordinary checks are required before acceptance.

## Why it was introduced and why it is a defect

The held companion model addresses an observed49.326MB native predecessor
queue blocking an urgent repair. A second native ordering domain removes
that serialization prerequisite without adding physical carrier, copy or
congestion credit. That intended performance benefit does not justify merging
four independent half-stream lifetimes into one error boundary.

The implementation correctly bounded child lifetime by its parent, but
incorrectly classified normal child receive completion as attachment failure.
The prior companion test even expected EOF to cause an operation error while
checking only sibling-carrier survival. It omitted the independently ordered
terminal exchange and server admission release. Thus this is an integration
model mistake in a held candidate, not a Quinn-upstream defect or proof that
the earlier accepted ordinary half-close correction was unnecessary.

## Correction and invariant

Let O be the ordinary parent, R the repair receive half and W its send half.
Normal completion of R changes only R. It cannot establish O's terminal state
or permission to cancel W. The shared repair reader returns success only for
exact clean `StreamFinished`; the existing join keeps W alive and the ordinary
parent remains the lifetime owner. Parent retirement still cancels/reconciles
remaining child work. The read helper names an existing half-owner; it adds
no queue, actor, clock, admission rule or speculative state.

This preserves the distinction between normal EOF and reset, truncated input,
HTTP rejection, wrong stream identity or malformed frames. Those remain real
errors. The malformed-companion test preserves attachment-local error and
sibling survival coverage instead of retaining its incorrect EOF expectation.
Returning after clean EOF also does not STOP the peer's send half: Quinn's
completed receive state marks `all_data_read`, and its Drop skips STOP_SENDING.

RFC6.2 now states the directional companion EOF contract explicitly. No
congestion gain, priority, queue limit, idle timeout, copy allowance, pacing or
qualification parameter changes. Do not fix this by changing select order,
resetting every detached Product stream, swallowing all repair errors, or
adding a cleanup timer. Each would obscure or violate the actual lifetime.

## Acceptance boundaries and next checks

Run the exact H3 half-close and malformed-frame controls, existing ordinary
half-close/terminal-drain tests, split repair queue/cancellation controls, then
repeat the ordinary mode ablation and original two-cycle quiet observation.
Preserve legitimate target half-close across path outage. If a retained owner
remains after this correction, distinguish missing request FIN, missing target
EOF, final ACK publication and non-graceful companion termination before
proposing another change.

A normal-parent drop of an unused pending H3 server response can produce404;
that is not the clean-EOF branch proved here and must not be silently swallowed.
It is a source-supported possibility, not yet a second demonstrated practical
defect. Full mixed startup/recovery, aggregation, browser, asymmetric native
QoS and baseline timing acceptance remain separate open gates. No release or
whole-stack non-regression follows from the component proof.

## 02:20 UTC checkpoint

The refined single-read H3 fixture and two pair/error controls pass; seven
existing ordinary half-close, server terminal-drain/reset, split-queue close,
cancellation debt and restart/sibling controls also pass. Ten targeted checks
in total. Release lib build took3m04s; optimized lab-diagnostics executable
build took1m38s, with no simultaneous network observation. Diagnostic flags
remain off for ordinary verification. The frozen corrected binary is
./.tmp/reflection/bin/repair-half-close/mptunnel.

REPAIR_HALF_CLOSE.patch isolates today's runtime/test/RFC correction from the
older held stack; `git apply --reverse --check` passes on this composition.
The patch does not include the prerequisite companion integration or silently
accept it. The ordinary fixed-request and two-cycle post-load gates are now
running. They must establish practical reclamation before closing this item.

## 02:50 UTC ordinary result and residual attribution

The corrected fixed32-request ablations all complete and reach zero Product/
admission owners for TCP, QUIC and mixed after about4.5s. The same-process
two-cycle mixed repeat completes955 then975 requests with no errors. It clears
cycle1, but retains3 server owners after cycle2 and37.4s quiet. This materially
improves the original observation without closing the composed lifetime gate.
CLOSED_REQUEST_REPAIR_20260907.json preserves full probes and compact state.
Native carrier maintenance bytes are not logical stream ownership and need not
be zero for this gate. The ordinary repeat did not capture the3 exact flow IDs.

A separate diagnostic repeat completes956 then978 requests;13 server owners
remain after45.1s final quiet, oldest idle93.4s. Full per-PID logs were saved
before analysis. REPAIR_TERMINAL_CHURN_TRACE_20260907.json correlates all final
IDs in the one exact session, preserving84 selected event records:

- Twelve received Product request FIN. Eleven captured carrierless transitions
  already have target EOF, response FIN sent/replayed, empty sender/reinjection
  and response ACK frontier equal to the sent offset. Each has an unreconciled
  attachment generation. Two final owners lack this transition hook; do not
  infer their exact blocked await solely from the missing event.
- One has no server request FIN and a client ordinary-input mailbox-closed
  event. Its client relay has nevertheless completed successfully. It belongs
  to the separate receive-interest versus outgoing-writer retirement branch.
- Every recorded server attachment ends through ordinary success, not repair
  failure. Thus clean repair EOF correction is not the residual root cause.

Source/history inspection now identifies two bounded follow-ups. Server commit
5e1ace67 added a justified final-ACK/membership fence before reconciliation;
the actor can reconcile the final change later in that turn and then park
without reconsidering completion. Client commits3f40ca9 andda63e85 compose a
closed Product recipient error with early forwarder retirement, cancelling
still-required ordered writes. Deterministic owner-level RED tests precede
either correction. Preserve the membership fence, pending ACKs, ordinary
half-open recovery and timely forwarder retirement. No invented Product FIN,
timeout, target-read lookahead or native error suppression follows.

All temporary diagnostic source hooks were removed after freezing the trace
binary; saved overlays and full logs remain under./.tmp/reflection. The frozen
ordinary repair-half-close binary remains available for the before comparison.

## 03:23 UTC bounded lifecycle acceptance

The two independently proved residual corrections pass all30 selected final
guards. The final ordinary composition completes1941 mixed requests on one
process pair and releases every logical/admission owner after each cycle,
remaining clear after68s final quiet. RSS stays flat in that interval. Separate
final TCP-only and QUIC-only controls complete32/32 each and also reclaim.
TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907.json preserves all outcomes, exact
PIDs, flow detail and independent Product/native accounting. No idle-expiry or
product restart was used to turn a retained owner into a pass.

The reproduced normal-termination gate is now closed for this tested envelope.
This does not accept the companion's wider performance tradeoff, attribute the
uncaptured deployed incident exclusively, or close mixed allocation/recovery,
native QoS, browser and baseline timing gates. The next transaction returns to
those already documented issues; no additional speculative lifecycle patch.
