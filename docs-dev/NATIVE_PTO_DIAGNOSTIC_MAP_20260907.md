# Temporary native QoS / PTO observer — 2026-09-07

Scope: the already observed post-QoS QUIC recovery plateau. This is an
opt-in diagnostic overlay, not a release model correction or acceptance result.
The root agent archives the overlay, freezes a diagnostic binary, then removes
the hooks before any subsequent production work.

## Activation and overhead

Only `crates/quinn-proto/src/connection/mod.rs` is instrumented. The existing
runner forwards `REFLECTION_NATIVE_TRACE=1` as
`MPTUNNEL_NATIVE_RECOVERY_TRACE=1`. The environment is read once; each native
connection has optional observation state. No timer, scheduling wakeup, payload
copy, qlog dependency, congestion mutation or recovery decision is introduced.

Existing transmit / ACK work emits at most one periodic snapshot per second.
Actual PTO and other low-frequency timeout expirations, PTO queueing and
encoded probe packets are recorded individually. Pacing, delayed-ACK and
ordinary time-threshold loss expiration counts remain visible in snapshots; individual
ACK frames are counted, not printed. Quiet connections need not produce one
snapshot every second: the observer creates no work to make that happen.

## Boundaries and interpretation

- Every row carries Unix microseconds, connection-relative microseconds,
  original destination CID, side, path generation and controller epoch.
- `on_ack_received`, after existing ACK validation and live-packet filtering,
  classifies frames as live, retained-only, or no-known. A mixed live/retained
  frame is live; retained packet totals still include its exact late proofs.
  No-known is **not** asserted to mean duplicate: expired/otherwise unretained
  packets also cannot be matched. ACKs rejected by existing validation are not
  classified. Last ACK time/space/largest identify the latest valid frame.
- `on_packet_acked` counts nonzero bytes only at the existing current-controller
  callback boundary. Old-controller/path-validation-excluded and zero-byte
  live entries do not create controller-delivery evidence. Observer counters
  are connection-cumulative; controller epoch is separately recorded.
- Snapshots show both computed PTO and the actual armed loss-detection timer,
  earliest loss time/space, last ack-eliciting send times and pending probes.
  They also show PTO count/base/ACK delay, SRTT/variance, cwnd/flight, native
  receive/transmit/authentication counters and last controller ACK time.
- `handle_timeout` counts every expired timer before the original stop/dispatch.
  Ordinary loss-time expirations are counted separately, without per-event
  output; actual PTO expiration/queueing is printed. Earliest loss time remains
  visible in snapshots, preserving the distinction between its precedence and
  conventional PTO backoff without high-volume loss logging.
- Four existing packet finalization sites call an observational wrapper that
  invokes the unchanged `PacketBuilder::finish_and_track` exactly once with
  the same arguments, then records packets inside a probe-granted datagram.
  PN, packet space, encoded byte length and ack-eliciting classification come
  from that actual builder. Padding/coalescing and early exits remain intact.
  This means native protocol emission into `Transmit`, **not** confirmed OS
  UDP submission, wire delivery or peer receipt. Coalesced packets share the
  datagram's probe grant; the individual packet space/PN is still explicit.

Array order: packet spaces are Initial / Handshake / Data; ACK classes are
live / retained-only / no-known. Timeout counters follow `Timer::VALUES`:
LossDetection, Idle, Close, KeyDiscard, PathValidation, KeepAlive, Pacing,
PushNewCid, MaxAckDelay. Optional times are connection-relative microseconds.

No build or lab was run by the hook author. Static diff checking is not a
runtime pass. The intended next discriminator is whether live ACK progress
stops while other valid ACKs arrive, which timer is actually armed/firing,
and whether the emitted recovery probes overlap the configured blackhole.
The retained-only early return alone does not establish an RFC violation.
