# Independent terminal-regression fixture review

Date: 2026-09-07 02:55 UTC. Read-only review of the six requested tests;
no compiler, lab traffic, production edits, or claim that tests passed.

## Server reconciliation

Reviewed `server_terminal_reconciliation_fin_before_last_detach`,
`server_terminal_reconciliation_fin_after_last_detach`, and
`server_terminal_reconciliation_after_final_ack_capacity` in
`src/runtime/relay/tests_server.rs`.

The shared fixture establishes actual target read EOF using the duplex peer's
write-half shutdown, then observes both original response FIN and terminal
replay on the real command channel. It separately delivers request FIN.
Detach is explicitly required to preserve an open request half; it cannot
substitute for Product EOF. The one-slot command queue deterministically
withholds the exact final ACK's publication capacity. Releasing that slot or
detaching the last output is the final wake under test.

This correctly distinguishes completion re-evaluation from waiting for another
Product input or a retention timer. The live-output case still requires the
ACK to appear on the carrier queue. The 100ms assertion is a deadlock sentinel
for an immediately enabled state transition, not a proposed Product timeout.
Queue size one and the inert Ping occupant are deterministic resource-state
fixtures, not workload limits or protocol substitutions.

The zero-byte Product case deliberately isolates terminal scheduling. It does
not prove reclamation of previously transmitted payload, end-to-end registry
admission ownership, or native transport state; the ordinary churn repeat is
still required. No hidden false-RED premise or weakened half-close invariant
was found.

## QUIC retired Product recipient

Reviewed `quic_write_interlock_closed_product_recipient_is_retired_input`,
`quic_write_interlock_pending_product_recipient_retires_without_error`, and
`client_quic_closed_product_recipient_preserves_ordered_terminal_writer`.

The first two cover immediate recipient closure and closure after a full
mailbox has retained the exact incoming feedback frame. Both distinguish a
retired Product recipient from a failed carrier while command ownership stays
alive. A full mailbox must first produce the pending-mailbox route, so the
second test cannot accidentally pass through immediate delivery.

The third uses actual HTTP/3/native stream I/O. It sends a late matching ACK
then Ping on the same receive stream and requires the matching Pong before
submitting FIN, DETACH and ordered Close. Thus merely cancelling the writer
cannot pass: the late ACK must be processed and the independent ordered
terminal write must reach the peer, without closing the connection.

The fixture instantiates an already-open stream owner; it is not a test of
the entire authentication/open handshake. It does not suppress malformed
frames, stream-ID mismatches, actual native resets or connection failures.
Existing terminal/barrier and sibling-carrier tests remain separate guards.
No invalid premise requiring the tests to be weakened was found.

## Next observation

Wait for the frozen corrected binary, then run diagnostics-off mixed mode on
the existing clean 500/500Mbps, 70/30ms routed profile: two 25-second browser
cycles, sixteen workers, same process pair, at least 67 seconds final quiet.
Record exact remaining flow/session IDs, admission, private/public ownership
where available, queues, Product/native flight, timestamps and RSS. Native
carrier-control traffic is recorded separately, not forced to zero as a
logical-owner acceptance condition. Do not start until the build handoff.
