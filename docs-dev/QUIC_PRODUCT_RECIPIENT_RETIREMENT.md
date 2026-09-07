# QUIC Product-recipient retirement

Date: 2026-09-07. Scope: one demonstrated client terminal-writer cancellation
mechanism, not a general explanation of every retained server stream.

## Queue admission is not native completion

The client records its logical request FIN as sent when the carrier command
queue accepts it. Successful Product completion can therefore precede the
native writer finishing that FIN and the following ordered Detach/Close.
`close_all_ordered()` removes scheduling membership and aborts the attachment's
input forwarder before queueing ordered Detach/Close. Those queue sends do not
wait for native completion. This closes the Product receive mailbox; it does
not revoke the independent output owner.

Previously, a late valid ACK or data frame sent into that closed mailbox became
`ReliablePathSessionClosed` in QUIC. The ordinary actor, or its repair companion,
then terminated the attachment. Dropping the writer discarded still-admitted
terminal commands. Native request EOF could reach the server without the
logical `STREAM_FIN` that was still in the local queue.

The server correctly treats native EOF as attachment loss, not an invented
logical FIN. Its Product request half remains open for bounded recovery. Thus
client success, server ordinary attachment `Ok(())`, and a retained server
Product owner can coexist. The defect is the client lifetime coupling, not
the absence of a native-to-Product EOF shortcut.

## Exact evidence and its limits

The [same-process trace](REPAIR_TERMINAL_CHURN_TRACE_20260907.json) identifies
stream 1853: successful client completion with both Product halves closed is
followed by `ordinary_input` mailbox closure. The server receives no Product
FIN; its ordinary attachment ends normally, while the captured Product state
has `remote_open=true`, no pending request FIN, and fully acknowledged response
data. This ordering supports the mechanism above.

The other twelve retained streams have server FIN receipts. They must not be
relabeled as twelve more lost-FIN instances: two lack the carrierless-state
hook, and other FIN receipts occur after captured transitions. Separate
[server reconciliation tests](SERVER_TERMINAL_RECONCILIATION.md) demonstrate
two completion-order failures; they do not individually reconstruct every
remaining trace owner's exact blocking step.

## Origin, intent and correction

`3f40ca9` introduced the ordinary and writer-interlock treatment of a closed
Product receiver as attachment failure. Its apparent cleanup intent was to
stop a per-request actor whose consumer disappeared; that conflated the
consumer lifetime with the independent terminal writer lifetime.
`da63e85` subsequently introduced owned input-forwarder cancellation during
membership retirement, stopping obsolete watchers promptly. That ownership
cleanup remains valid and is preserved. The observed failure composes these
behaviors; the history alone does not establish a deployment regression date.

The correction follows the existing TCP recipient model at three QUIC sites:

- Ordinary input retires a valid frame whose Product recipient is closed.
- The writer interlock retires immediately closed or pending-then-closed input
  without cancelling its pinned native write or ordered terminal commands.
- Repair input applies the same recipient rule after normal frame validation.

This left no production constructor for the artificial `CarrierClosed`
mailbox policy. The policy enum and impossible error result were removed.
All three mailbox constructors now share one contract: `deliver()` returns
`true` only for actual recipient transfer and `false` for recipient retirement.
The four await sites preserve those owners' existing behavior. False creates
no Data ACK, receive credit, proof, Product delivery or native EOF; diagnostic
processed-frame counts are not delivery accounting.

The tradeoff is limited: late valid input with no remaining recipient is
discarded while already-owned output can finish. Native read/write errors,
rejected HTTP responses, malformed frames, exact stream validation and
terminal barriers remain authoritative. No new state, timer, queue limit,
FIN acknowledgment protocol or replay-count adjustment was added.

## Targeted verification

All three new client tests failed before correction. The native fixture sends
a late ACK followed by Ping on the same response stream; old code returns
`StreamFinished` instead of Pong. After correction, Pong proves the ACK was
processed without cancelling the writer, then actual ordered FIN/Detach/Close
completes on the same healthy connection. The two smaller tests cover direct
recipient closure and closure after mailbox pressure.

After the directly-caused policy cleanup, all 30 selected checks passed,
including those client cases, separate server reconciliation cases, existing
half-close/FIN, genuine EOF failure, malformed repair/reset, sibling/restart,
ACK-publication and TCP/QUIC queue/cancellation guards. The optimized non-test
binary also built cleanly. [Exact commands, results and timing](TERMINAL_RETIREMENT_CHECKS_20260907.json)
and the [isolated source/RFC delta](QUIC_PRODUCT_RECIPIENT_RETIREMENT_20260907.patch)
are retained. The [joint ordinary churn](TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907.json)
now also passes:1941/1941 requests on the same mixed process pair, zero retained
owners after68s quiet, plus32/32 each for TCP-only and QUIC-only. Final RSS is
stable. This verifies the demonstrated cleanup mechanisms; it is not unlimited
sustainability, performance, deployed-RAM sole-cause or release acceptance.
