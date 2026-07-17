# Architecture and ownership

This document maps the current source tree and design choices to the MPP
protocol model. `RFC.md` is the wire and behavioral specification, and
`docs/CODE_STRUCTURE.md` defines repository rules.

## Layer model

```text
SOCKS5 / HTTP CONNECT / TUN ingress
    -> MPP stream or datagram identity
    -> per-direction connection sequencing, flow control, Data ACKs, and reinjection
    -> carrier-neutral path scheduler and sender queue
       -> TCP carrier -> kernel TCP congestion control and recovery
       -> QUIC carrier -> Quinn congestion control and recovery over UDP
    -> peer data-level reassembly
    -> target outbound
```

MPP unifies TCP and QUIC above their native recovery layers. It does not try to
make their packet controllers identical:

- TCP owns its byte ACKs, retransmission, congestion control, and socket queues.
- QUIC owns packet ACKs, loss detection, PTO, pacing, congestion control, and
  connection migration.
- MPP owns the stream ID, each direction's offset space, `STREAM_ACK`,
  each direction's shared receive window, exact range attribution, reinjection
  across paths, and carrier-neutral scheduling.

A native TCP ACK or QUIC packet ACK is path evidence. It never releases the MPP
connection-level flight ledger; only a `STREAM_ACK` for the MPP range does.

## Protocol-v2 model

`OPEN_STREAM` contains only `stream_id`, `target`, and initial `demand`. Opening
or attaching a stream does not assign a persistent primary, validation, or
recovery role to the path. Every accepted attachment is neutral membership in
the connection's path set.

`PATH_STATUS` carries a sequenced, directional `PathUsage` value:

- `Available`: the receiver permits ordinary use in that direction.
- `Backup`: the receiver asks the peer to use the path only after available
  paths cannot carry the work.

This follows the regular/backup preference in MPTCP and multipath QUIC. The
preference is independent of local health. Local states such as usable,
suspect, draining, or failed are never serialized as `PathUsage`.

Scheduling first considers eligible `Available` paths. Only when that set has
no schedulable output does it fall back to eligible `Backup` paths. Within the
chosen set it ranks live RTT, rate, queue, flight, loss, jitter, confidence,
and current demand. A configured local `backup` policy may be stricter than the
peer preference.

`TrafficClass` is endpoint-local, mutable demand classification for queued work. It
is not a property or role of a link. A stream may move from latency-oriented to
throughput-oriented work and back as its live demand changes without reopening
the stream or relabeling a path.

## Source owners

A module exists when it owns a durable state machine, algorithm, protocol
boundary, or adapter. File size alone does not earn a module.

- `src/protocol/`: bounded protocol-v2 codec, wire values, authentication, and
  range semantics. It owns no sockets or scheduling policy.
- `src/model/`: carrier-neutral identities, evidence, capacity, admission, ACK
  clock, connection-flight, and work models. Pure TCP proof candidate
  validation lives here; TCP runtime owns the measurement transaction.
- `src/scheduler/`: pure eligibility and completion-time ranking over immutable
  path snapshots. `src/simulator/` may reuse these formulas but owns only
  simulator-private queues.
- `src/transport/`: encryption, framing, endpoint resolution, TCP adapters,
  Quinn/QUIC adapters, and optional native telemetry. It does not own MPP
  offsets or path placement.
- `src/runtime/path/`: configured paths, health and metric publication, path
  instances, command queues, proofs, selection, and typed ports.
- `src/runtime/path/tcp/`: TCP connection actors, reads/writes, heartbeats,
  optional socket evidence, and TCP-specific capacity transactions.
- `src/runtime/path/quic/`: Quinn connection and stream actors, datagrams,
  native measurements, and native congestion-window publication.
- `src/runtime/stream/`: MPP stream handles, connection-level receive
  feedback, client request state and attachments, server registry, response
  bindings, exact attachment lifetimes, and delivery.
- `src/runtime/stream/request/`: request `attachment`, `state`, and `flight`
  owners behind the narrow `request.rs` facade. MPP receive-window authority
  remains in the shared mux stream model.
- `src/runtime/sender/request/`: request queueing, scheduling, capacity intents,
  multipath commit, and carrier dispatch.
- `src/runtime/sender/response/`: response `service`, `scheduling`,
  `multipath`, and `dispatch` phases. The service owns queued work; scheduling
  is pure; multipath owns lifecycle planning; dispatch revalidates and enqueues.
- `src/runtime/stream/response/`: response `ack_clock`, `attachment`,
  `data_commit`, `delivery`, `diagnostics`, `evidence`, `session`,
  and `snapshot` state. These are the only current response
  owners; deleted pre-v2 wrapper modules are not part of the current tree.
- `src/runtime/relay/`: ingress/target I/O, carrier open/attach transactions,
  failure recovery, and coordination with senders. The stream layer owns the
  resulting membership set; sender does not import relay state or policy.
- `src/runtime/datagram/`: MPP datagram associations, feedback, target
  workers, shared SOCKS/TUN edge workers, and carrier-neutral selection.
- `src/runtime/telemetry.rs`: exact logical product-byte, packet, and flow
  accounting at ingress/target relay boundaries. It never counts carrier
  retransmission, reinjection, or multipath copies.
- `src/runtime/peer_status.rs`: bounded correlation for manual authenticated
  peer-status requests. TCP and QUIC actors retain their own writer and metric
  ownership; the broker owns neither carrier I/O nor scheduling evidence.
- `src/runtime/management/`: cached typed snapshots, bounded HTTP transport,
  and embedded-dashboard delivery. Its one-second sampler is the only reader
  of runtime observability owners on behalf of HTTP requests.
- `src/ingress/` and `src/outbound/`: local protocol parsing and remote target
  connection policy respectively. Neither chooses MPP data paths.
- `src/runtime/node/`: constructs client, server, or combined nodes and injects
  typed ports between owners. The accepting listener carries its exact local
  path policy and startup hints into server carrier registration.

## Reliable-stream data flow

Request and response directions use different state owners and independent
sequence, Data ACK, and receive-window state, but the same connection contract.
For each direction:

1. The source allocates monotonically increasing data sequence offsets.
2. Request flight ownership retains the exact attachment identity. Response
   dispatch revalidates the path instance before enqueue, then records the
   stream-unique output incarnation only after queue reservation succeeds.
3. The receiver reassembles by data sequence offset and advances delivery once holes
   close.
4. `STREAM_ACK` acknowledges MPP ranges independently of the transport path that
   delivered the ACK.
5. The sender releases every recorded copy of an acknowledged range and updates
   local ACK-clock/admission evidence without changing advertised flow-control
   credit.
6. When data-level progress stalls and another path is eligible, the sender may
   reinject only the missing range under the configured flight and reorder
   envelopes.

Reinjection is not TCP retransmission or QUIC packet recovery. Native recovery
continues below it. The range ledger avoids treating a second copy as new
MPP data and prevents unbounded duplication.

`STREAM_ACK` and `STREAM_MAX_DATA` are separate signals. Data ACK releases
MPP ranges and flight but grants no new offset. `STREAM_MAX_DATA` grants a
new maximum offset but acknowledges no byte. Its receiver-advertised window is
shared by all attachments in one stream direction; the opposite direction has
an independent maximum. Carrier windows and congestion windows remain separate
limits.

Ordinary reinjection consumes a cumulative extra-traffic budget. Critical
path-failure, persistent authoritative Data ACK gap, and bounded live-tail
recovery may exceed the remaining cumulative budget only by a cause-specific,
event-bounded quantum. The exact range must remain unacknowledged and retained;
overlapping queued copies are suppressed, live-tail and persistent-gap work use
a distinct output, and all exception bytes remain charged against later
optional reinjection.

Recovery timing follows the evidence owner: exact path-instance failure is
immediate, an authoritative lowest missing Data Sequence frontier must persist
for three owner-carrier recovery intervals, and a contiguous live tail may send
one bounded probe after one such interval but waits three intervals before
repeating without progress. TCP uses RTO while QUIC uses PTO. Growth of the ACK
horizon above the same frontier does not restart the timer. A request carrier
with no exact Data ACK progress becomes stale for new placement after four TCP
RTOs or three QUIC PTOs when an alternative exists, without stopping its native
recovery. These are MPP data-level policies; native TCP and QUIC recovery
timers remain independent.

## Scheduling contract

Scheduling follows observe, decide, apply:

1. **Observe** captures one immutable snapshot with path key, physical path
   instance, local health, peer usage, metric provenance, freshness, queue,
   flight, and relevant generations.
2. **Decide** runs pure available-first eligibility and metric ranking. It
   returns identities and bounds, not live handles.
3. **Apply** revalidates the exact path instance, data frontier, evidence,
   generations, window, and queue credit before committing and enqueueing.

Failure or cancellation leaves no partial flight, load, or queue reservation.
RAII claims and explicit rollback balance every enqueue, dequeue, receiver
drop, timeout, and task exit.

Latency-sensitive demand prefers completion time and low queue/reorder cost.
Sustained throughput demand may use several available paths when measured
delivery opportunity exceeds the ordering and queue cost. A backup path is not
assigned an additive score penalty; it is considered in the second selection
set. This keeps preference semantics separate from metric ranking.

The immutable snapshot keeps carrier queue/flight separate from MPP
queue/Data-ACK flight. Those views may overlap, so completion ranking uses their
maximum rather than their sum. Response ranking removes the connection-wide MPP
queue shared by every candidate and keeps only exact unique data on the selected
output as its data-level completion debt.

Loss, ECN, jitter, and queue evidence can change ranking and reordering cost.
It never shrinks an MPP service quantum, creates a congestion window, or paces
a carrier; native TCP and QUIC remain the only congestion controllers.

Portable TCP follows the same boundary. Before exact product progress, one
bounded startup flight limits exploration. Once original Data ACK coverage is
durable, the shared receive/reorder windows and configured resource envelope
bound MPP work while writer/socket backpressure bounds carrier acceptance. The
measured Data ACK rate ranks completion but is not fed back as a replacement
TCP congestion window.

## Identity and ownership

Logical path identity is `(underlay, path_id)`. Physical carrier identity adds
`path_instance_id`. Evidence, flights, commands, and usage sequences cannot
cross a reconnect merely because a numeric path ID was reused.

Stream membership adds a separate incarnation. Request-side scheduling and
flight ownership carry `(path_instance_id, attachment_id)`. Response new-data
dispatch carries `(path_instance_id, output_incarnation)` plus the observed
response-model generation and revalidates all three before queue reservation.
The committed response flight retains the logical path key plus the
stream-unique output incarnation; the physical instance was the apply-time
fence, not a duplicated ledger field. Replacing a carrier invalidates physical
evidence, while detach and reattach invalidates that stream's ownership even if
the carrier itself stayed live.

One configured reliable TCP path owns one live carrier actor. `TrafficClass`
changes queue priority and scheduling demand; it never creates a second hidden
carrier with the same logical identity. TCP datagram associations use separate
short-lived sessions and keep their `PATH_STATUS` sequence locally, so they do
not replace the reliable carrier's physical identity.

The initiator chooses the wire `path_id`; the receiver treats it as opaque.
Node composition retains the accepting listener's endpoint-local configuration
ordinal through carrier registration. Startup hints and the full local
`PathPolicy` come from that listener, never from a peer-ID lookup. Local
`backup` is additionally advertised as directional `PathUsage`; the remaining
policy fields stay off wire and apply only to the local sender.

A stream attachment identifies membership and output reachability for one
exact carrier instance. It does not own the target connection or assign a
persistent data role. On the server, one session registry owns the target
stream binding; TCP and QUIC carrier actors attach to that binding through
typed ports and never create duplicate target relays.

Shared locks protect one coherent aggregate. Scheduling does not hold them
while doing I/O. Hot frame and ACK paths use local actor state or immutable
snapshots rather than a session-wide lock for each packet.

The authenticated TCP path session is its control channel. QUIC retains the
first authenticated bidirectional stream as a connection control stream and
uses later streams for product flows. Manual peer status uses these existing
channels symmetrically; it does not create a diagnostic transport or convert
remote observations into local path evidence.

The response output carrying the contiguous Data Sequence frontier is governed
by the shared MPP receive window and native carrier credit. An additional
output without durable, unambiguous Data ACK coverage receives at most one
bounded startup flight. Exact Data ACK coverage of original transmissions must
reach the startup sample floor before that additional output uses the mature
connection-window model. Native TCP ACK or QUIC packet-ACK evidence can
describe carrier service, but cannot by itself establish MPP progress.

The long-lived reliable-carrier owner publishes an unexpected loss with the
exact physical instance immediately. Relay cleanup can observe the same loss
later, but the duplicate remains retired even if a separate reachability probe
recovers logical path health. A newer authenticated carrier installation
supersedes older reports, so delayed status or teardown cannot poison its health
projection. TCP datagram associations report their own attempt failures through
the association owner rather than claiming another carrier's lifetime.

## Platform boundary

Protocol, models, scheduling, stream ownership, and relay behavior are
platform-neutral. Target-specific code is limited to host adapters:

- Linux and Android use the stable `TCP_INFO` UAPI prefix, macOS uses
  `TCP_CONNECTION_INFO`, and supported Windows versions use `SIO_TCP_INFO`.
  These adapters live under `src/transport/tcp_telemetry/` and normalize native
  units without inventing unavailable counters.
- Every native field is independently optional. Unsupported APIs, truncated
  records, and restricted hosts continue with MPP Data ACKs, configured hints,
  and carrier-neutral observations.
- Native TCP drain shortcuts require exact flight and unsent-queue counters
  from the same snapshot. Partial Windows/macOS window shape still informs
  service capacity, while reinjection uses exact MPP product flight.
- TUN acquisition and carrier-network selection are injected host
  capabilities. Android hosts must establish the VPN descriptor and protect or
  bind carrier sockets outside the catch-all route.

No scheduler eligibility rule may require native telemetry, inspect an
interface name, or branch on the operating system. Windows client with Linux
server is a primary design target; Linux, macOS, Windows, and the Android
library target must compile without changing the protocol model.

TCP and QUIC evidence remain typed by provenance. Request TCP capacity uses a
receiver-confirmed receipt and optional exact-socket telemetry; request QUIC
capacity uses fresh native packet-ACK-derived evidence and an independent proof
lifetime. For response bulk readiness, locally sourced ACK-derived carrier
evidence is authoritative for QUIC, while durable unambiguous Data ACK progress
may additionally establish a per-flow TCP MPP rate. Peer metric hints do
not mint either proof.

## Evidence rule

Deterministic simulator and benchmark gates test models, not the deployed
runtime. Runtime changes require focused tests plus matched end-to-end labs:

- single path against direct and applicable VMess/Hysteria2/MPTCP baselines;
- multipath aggregation and failover;
- upload and download;
- latency-sensitive and sustained bulk demand;
- TCP-only, QUIC-only, and mixed carriers; and
- shaped, unconstrained, fault, and separately recorded real-Internet cohorts.

Diagnostics establish causality. Instrumentation-free matched rows establish
performance. Historical protocol-v1 rows are references only and cannot prove
protocol-v2 behavior.
