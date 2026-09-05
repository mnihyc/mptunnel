# Receive-credit and partial-write closure

Date:2026-09-05T19:30Z. Base6636091. This is a separate conformance correction,
not an attribution of the reported QUIC rate/recovery incident.

## C1: grant cadence — actual RFC/code mismatch

RFC8.4 already requires the retained latest grant to advance with every freed
receive prefix and to be offered before the receive actor parks. The shared
`ReliableRecvProgress::should_send_max_data` still waited for the greater of
one quarter-window and one payload batch. That older batching optimization
reduced control frames, but survived the later explicit full-window model.

The existing4096-byte-window test was corrected to the RFC contract: after
initial publication and one512-byte released prefix, publication must be due.
It fails on the old code (`.tmp/credit-cadence-red.log`). The correction compares
the new monotone grant directly with its previous value. It removes the byte
threshold and unused window-change state; it adds no timer or parameter.
The old batch-limit helper remains only as an unchanged Data ACK resource
ceiling and is renamed accordingly. Data ACK cadence itself is unchanged.

The per-attachment publication owner already coalesces blocked grants into one
latest value. Publication does not gain another unbounded queue. The tradeoff
is potentially more small MAX_DATA updates when the consumer drains small
batches, in exchange for not withholding usable receive credit. The exact
pending-publication/fanout controls and root suite are the acceptance checks;
no throughput improvement is claimed from a unit test.

## C2: partial local writes — retained ownership, no runtime change

`write_delivered_payloads` retains one scalar write_all future or one vectored
chunk-index/offset cursor across Pending. The enclosing ready batch owns all
its Bytes until write/flush succeeds, then clears that storage. An intermediate
sink write is therefore not a released MPP batch: advancing credit for the
still-retained batch would require a separate ownership transfer model.
The client constructs and pins this future once while interleaving control;
the server awaits the same ownership-preserving helper. A terminal write error
terminates the stream rather than attempting to replay a partially delivered
application prefix at new offsets.

`delivered_payload_write_preserves_partial_cursor_across_pending` checks scalar
and vectored payloads, repeated three-byte acceptance separated by Pending,
exact final byte order/count, and unchanged batch ownership. Existing ready-
batch attribution, deferred-error-prefix, blocked-write ACK/FINAL and R1 reset
tests cover the enclosing lifecycle. This is a correctness control, not proof
that smaller credit-release granularity would improve application performance.

## C3: target-bound tail — already corrected, no duplicate patch

Current response final-tail handling calls
`enqueue_live_response_final_tail_reinjection` for its selected exact alternate;
failure authority remains separate. Releasebfac5b8 already limits live-owner
accepted service to its ranked frontier. The request/response frontier tests
and final-tail predecessor/successor controls remain the relevant checks.
Old SEEN labels preceding that commit do not justify reintroducing a percentage
gate or changing the accepted repair model.
