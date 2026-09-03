# Architecture

This document explains how the Product layer, MPP Core, and native transports
fit together. `RFC.md` is the wire and behavioral specification.

Product routing, DNS, ingress, outbound, and operations stay above the MPP
Core. Protocol sequencing, delivery, recovery, scheduling, and carrier
lifecycle stay below that boundary.

## Layer model

The configured inbound protocols select one forwarding family. SOCKS5, HTTP
CONNECT, mixed proxy, port forwarding, TUN-L4, and MPP are L4. Experimental
`tun-l3` and `mpp-l3` select the raw IP-packet plane and do not admit L4 streams
or application datagrams. A document cannot mix the two families. Carrier
authentication and lifecycle remain shared; forwarding policy and performance
changes stay inside the selected family.

```text
L4 inbound protocols
  SOCKS5 / HTTP CONNECT / mixed proxy / port forward / TUN-L4 / MPP
    -> Product routing, DNS, admission, outbound selection
    -> MPP stream or application-datagram plane
       -> offsets, flow control, Data ACKs, bounded reinjection

L3 inbound protocols (experimental)
  TUN-L3 packet device / MPP-L3 server
    -> authenticated static address ownership + packet-flow affinity
    -> complete IPv4/IPv6 packet plane

selected MPP plane
    -> exact carrier-instance selection
       -> TCP carrier -> kernel TCP congestion control and recovery
       -> QUIC carrier -> Quinn congestion control and recovery over UDP
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

Each configured QUIC path owns its authenticated native connection. Each TCP
endpoint owns a bounded carrier group reconciled toward its configured maximum;
every authenticated member is ready for regular or backup scheduling. A lone
TCP endpoint exposes its bounded members as regular capacity. With multiple
configured TCP endpoints, each primary remains regular and its correlated
siblings are ready backups. Diagnostic reachability probes are isolated from
those durable instances. TCP and QUIC keep their own handshake and liveness
mechanics, while the path layer records only authenticated exchange timing as
path RTT, not connection setup time.

Carrier establishment may select one concrete destination port from a configured
inclusive set before resolution. Every address-family attempt for that
establishment uses the same selected port; a later physical carrier
establishment selects independently. Managed-VPN preparation therefore stores
resolved carrier IP addresses rather than freezing one selected port before
host routes are published. This local locator selection does not change MPP
path or carrier-instance identity.

TUN-L3 is deliberately separate from the Product plane. It does not create a
Product target, resolve DNS, run the TUN-L4 userspace stack, or select a normal
outbound. The authenticated principal selects a server-configured address
allocation. Host routing, forwarding, DNS, firewall policy, and NAT are outside
the packet-copy boundary.

## MPP v9 model

`OPEN_STREAM` contains only `stream_id`, `target`, and initial `demand`. Opening
or attaching a stream does not assign a persistent primary, validation, or
recovery role to the path. Every accepted attachment is neutral membership in
the connection's path set.

Initial open is a two-phase transaction. The server first publishes
`STREAM_MAX_DATA(0)` on the exact carrier, before DNS, routing, or target
connect begins. The client treats that zero as carrier admission but keeps the
logical open pending; carrier PTO no longer owns remote target latency. A
concrete attachment failure may retry another carrier with the same
`StreamId`, so the server retains one immutable opening identity and one target
task. That task queues the optional path proof after zero on the same bounded
carrier lane; proof backpressure or carrier-local failure cannot undo the
admission or duplicate the target. Target success publishes one retained
nonzero grant to every live attachment. Later attachments use zero as their
complete carrier acceptance and then inherit that retained grant.

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
limits. Initial carrier admission publishes zero; target establishment then
publishes the logical receive owner's nonzero starting credit. Later TCP or
QUIC attachments are accepted with a zero maximum, which is credit-neutral
because senders retain the greatest advertised offset; path demand can never
widen the shared window.

Optional reinjection, including persistent authoritative Data ACK-gap repair
while the exact original carrier remains live, consumes a cumulative
extra-traffic budget. Critical path-failure and bounded live-tail recovery
without an authoritative gap may exceed the remaining cumulative budget only
by a cause-specific, event-bounded quantum. The exact range must remain
unacknowledged and retained; overlapping queued copies are suppressed,
live-tail and persistent-gap work use a distinct output, and all exception
bytes remain charged against later optional reinjection.

Recovery timing follows the evidence owner. Exact path-instance failure is
immediate. A complete Data ACK establishes gaps; positive partial ACK ranges
may extend that state but cannot infer omitted ranges. Fragmented request
feedback waits one owner RTO/PTO from first authoritative gap observation.
Response feedback may use a later-ACK TCP RACK 5/4-SRTT or QUIC 9/8-SRTT time
threshold, while ACK silence waits the owner RTO/PTO. A contiguous live tail
may send one bounded probe per recovery interval without progress. A request
carrier with no exact Data ACK progress becomes stale for new placement after
four TCP RTOs or three QUIC PTOs when an alternative exists, without stopping
its native recovery.

Version 9 gives a stale exact attachment a bounded requalification lifecycle:
`Qualified -> Stale -> Requalifying -> Acquiring -> Qualified`. A non-owning
copy of retained Product bytes and its exact attachment ACK prove only that
attachment's reachability. Only fresh, uniquely owned OriginalData acknowledged
after that proof restores Product authority. The probe never delivers or owns a
Data Sequence range; retry remains bounded by retained data, queue, pacing,
flight, and the existing optional-work budget.

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

## IP-packet data flow

One logical IP tunnel attaches to every eligible authenticated TCP and QUIC
carrier. The server accepts an attachment only when that carrier's principal
has an explicit address allocation. Complete inner IPv4/IPv6 packets retain
their bytes, TTL or Hop Limit, and transport headers.

Each direction keeps independent inner-flow affinity to one exact carrier
instance. A healthy binding performs no path-health lock or ranking allocation
per packet. Exact carrier loss invalidates it immediately; an idle boundary
derived from current carrier PTO permits fresh ranking from native RTT, rate,
loss, queue, and flight evidence. Regular carriers precede backups. Queue
fullness drops only the current packet and does not trigger a duplicate or
declare the path failed. Recent packet-flow load distributes cold bursts across
otherwise equal carriers and expires with flow activity instead of cache
residency.

On the client, accepted wire frames and carrier-membership updates enter one
ordered handoff. Lifecycle entries reserve independent bounded headroom, while
each packet retains a weighted byte permit through the separately bounded
packet-device writer. Host backpressure therefore cannot invert accepted
packet/close order, overtake attachment readiness or retirement, or grow packet
memory without bound.

TCP carries packet frames in its ordered carrier framing. QUIC carries
lifecycle frames on one reliable request stream and complete packets in that
stream's native QUIC Datagram association. MPP adds no packet ACK,
retransmission, congestion window, global reorder buffer, or UDP-first policy.

## Identity and ownership

Logical path identity is `(underlay, path_id)`. Physical carrier identity adds
`path_instance_id`. Evidence, flights, commands, and usage sequences cannot
cross a reconnect merely because a numeric path ID was reused.

A ranged QUIC path may replace its local UDP socket and external destination
port while retaining the same Quinn connection and `path_instance_id`. The
established server IP is pinned for this operation, each socket crosses the
same host-network protection boundary, and the preceding socket remains
available until traffic returns through the selected port. Quinn remains the
only owner of connection IDs, migration, path validation, recovery, and native
path state. No scheduler, attachment, MPP authentication, or wire state changes.
The locator change therefore does not cold-start the QUIC carrier or create a
new MPP evidence owner. A full QUIC reconnect does: it retains configured path
hints but never predecessor measurements, queues, flight, ACK ownership, or
sample authority. A ranged TCP path chooses a port only when creating a new TCP
connection, which always creates a new physical carrier instance. One TCP group
may own one transient authenticated successor beyond its configured
current-member count.
Publication swaps the member's current exact instance before predecessor drain;
the predecessor's existing attachments then use ordinary retirement recovery.
The overlap grants no exception to the receiver's session/global path caps and
ends before another member in that group may rotate.

Stream membership adds a separate incarnation. Request-side scheduling and
flight ownership carry `(path_instance_id, attachment_id)`. Response new-data
dispatch carries `(path_instance_id, output_incarnation)` plus the observed
response-model generation and revalidates all three before queue reservation.
The committed response flight retains the logical path key plus the
stream-unique output incarnation; the physical instance was the apply-time
fence, not a duplicated ledger field. Replacing a carrier invalidates physical
evidence, while detach and reattach invalidates that stream's ownership even if
the carrier itself stayed live.

One configured TCP endpoint owns a bounded carrier group whose maximum is the
healthy target; the configuration default creates three members. Each physical
carrier has its own actor, encrypted reader/writer, `PathId`, instance identity,
attachments, queues, flight, evidence, and `PATH_STATUS` sequence. The session
group owner reconciles every missing member. A lone endpoint exposes all of its
members as regular capacity. With several endpoints, each primary is regular
and its correlated siblings are ready backups; live directional evidence ranks
members inside that eligible tier. Flow close or detach removes only its flow
or attachment and cannot create, retain, replace, or close a carrier.

Datagram failover keeps timing ownership below the carrier-neutral association.
TCP and QUIC each derive a modeled pre-feedback response timeout from their own
observations. If another ranked attempt remains, the association reserves it by
ending the first attempt without feedback after one such timeout; the final
attempt retains a three-timeout loss-tolerance budget. QUIC's attempted-path
set and the TCP attachment opener both exclude the configured path already used
by the request. The carrier-neutral association allocates one
`(session_id, flow_id, direction, datagram_id)` request identity before
selection and preserves it for every carrier attempt. The server target worker
owns an independent response-ID space and replay state. The server registry
attaches attempts to one target socket and actor, suppresses duplicate target
execution, and keeps a bounded response replay for the retry carrier. Matching
datagram feedback moves the request to admitted state, extends response waiting
to the absolute product TTL, and makes further cross-carrier replay terminal.

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

A product `STREAM_FIN` declares a final Data Sequence offset and remains
pending until the contiguous receive frontier reaches it. An offset behind any
received data is invalid, and later data cannot extend beyond an accepted final
offset. FIN does not remove the TCP or QUIC attachment, and post-FIN repair
below that offset remains valid.
`STREAM_DETACH` removes one attachment; `STREAM_RESET` terminates the logical
stream. Native TCP EOF terminates its physical carrier.

Each logical QUIC carrier stream is a full-duplex HTTP/3 `POST /` request.
The optional endpoint-wide transport secret changes only QUIC Initial key
derivation; TLS and HTTP/3 remain inside the authenticated QUIC connection.
The same file selects PSK-gated Noise instead of TLS for TCP carriers. Without
that file, the established TLS/QUIC profile is unchanged. MPP client
credentials remain a separate Product admission layer in either profile.
The gate requires HTTPS, authority equal to the negotiated TLS SNI, exactly
`/` without a query, and the canonical encrypted selector supplied by Product
credential authority before request DATA enters the bounded accepted queue or
MPP parser. QUIC path groups therefore require a DNS TLS identity, although
their carrier endpoints may be literal IP addresses. The first accepted
selector is connection-latched; later requests must match it while full
`SESSION_AUTH` and `PATH_JOIN` remain mandatory. This narrows parser exposure
but does not make H3 a cover protocol or prevent source-aware transport
fingerprinting.

Request and response DATA directions retain independent send and receive
ownership. HTTP/3 end-of-stream on one direction neither completes the MPP
stream nor substitutes for product `STREAM_FIN` or `STREAM_DETACH`; the actor
retains the writable response/request direction for final Data ACK and
teardown frames. Terminal product frames are processed before a following
clean DATA-stream EOF, while EOF inside a length-prefixed MPP record remains a
visible truncation error.

The reliable-stream actor, not a carrier, owns break-before-make retention.
At zero live attachments it preserves the existing sequence/ACK/FIN state,
stops source reads, and rotates one reconnect attempt at a time until the
absolute configured deadline. TCP heartbeat and native QUIC keep-alive/idle
timers only determine carrier liveness and never reset that logical deadline.
The client owns their bounded idle-renewal schedule; ordinary authenticated
traffic defers the current lease without changing an outstanding failure
deadline.

Shared locks protect one coherent aggregate. Scheduling does not hold them
while doing I/O. Hot frame and ACK paths use local actor state or immutable
snapshots rather than a session-wide lock for each packet.

The single authenticated TCP connection multiplexes path-control,
reliable-stream, and datagram frames. QUIC applies the selector gate, then
retains the first fully authenticated request stream as a connection-control
stream and uses later selector-matched requests for product flows. All
reliable bytes use H3 DATA; only
`DGRAM_DATA` uses RFC 9297 native QUIC DATAGRAM, associated by Quarter Stream
ID with its request stream. Reliable `OPEN_DGRAM_FLOW`, `DGRAM_CLOSE`, and
`DGRAM_FEEDBACK` preserve association lifecycle and feedback without putting
unrelated UDP payloads behind one reliable control stream. Peer status uses
the existing reliable DATA channels symmetrically; it does not create a
diagnostic transport or convert remote observations into local path evidence.

The H3 request queue, QUIC concurrent bidirectional-stream credit, native
datagram routes, active inner flow IDs, pending pre-route packets, reassembly
count, and all retained native bytes are configured bounds. The server H3
driver resolves requests in one task and applies bounded-channel backpressure;
it does not spawn one pre-admission task per request. At runtime, the
connection authentication permit is acquired before its connection task is
spawned. After authentication, concurrent product stream actors cannot exceed
the transport's
`min(max_quic_concurrent_bidi_streams, max_streams)` per connection. The bound
is per configured carrier connection and therefore multiplies across the
configured path count; it is not a process-global stream limit. Listener and
connection loops reap completed child tasks before accepting more work so
completed-task outputs cannot accumulate under sustained churn.

The response output carrying the contiguous Data Sequence frontier is governed
by the shared MPP receive window and native carrier credit. An additional
output without durable, unambiguous Data ACK coverage receives at most one
bounded startup flight. Exact Data ACK coverage of original transmissions must
reach the startup sample floor before that additional output uses the mature
connection-window model. Native TCP ACK or QUIC packet-ACK evidence can
describe carrier service, but cannot by itself establish MPP progress.

The long-lived path actor publishes loss for its exact physical instance.
Relay cleanup can observe the same loss later, but the duplicate remains
retired even if a separate reachability probe recovers logical path health. A
newer authenticated carrier installation supersedes older reports, so delayed
status or teardown cannot poison its health projection. A TCP datagram
association owns attempt timeout, retry, and attachment release; it does not
create, close, or independently report loss for a second physical TCP carrier.

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
- Retransmission counters retain their native unit: segments on Linux/Android
  and bytes on Windows/macOS. Diagnostics may compare advancement on one exact
  socket instance, but the value is never a cross-platform rate or MPP Data ACK.
- Native TCP drain shortcuts require exact flight and unsent-queue counters
  from the same snapshot. Partial Windows/macOS window shape still informs
  service capacity, while reinjection uses exact MPP product flight.
- Quinn's native UDP adapter remains the normal QUIC socket owner. A Windows
  host that reports unsupported optional Winsock facilities may use a basic
  datagram adapter without ECN, GSO, or GRO. This changes host I/O capability,
  not QUIC recovery/congestion ownership or any MPP scheduling rule.
- TUN acquisition and carrier-network selection are injected host
  capabilities. Android hosts must establish the VPN descriptor and protect or
  bind carrier sockets outside the catch-all route.
- `src/platform/` owns packet-device construction and managed-VPN generation
  adapters, including target selection and Linux host preparation/publication.
  Generic node and TUN runtime code consumes only the resulting platform
  providers and opaque packet device.

No scheduler eligibility rule may require native telemetry, inspect an
interface name, or branch on the operating system. Windows client with Linux
server is a primary design target; Linux, macOS, Windows, and the Android
library target must compile without changing the protocol model.

TCP and QUIC evidence remain typed by provenance. Request TCP capacity uses a
receiver-confirmed receipt and optional exact-socket telemetry; request QUIC
capacity uses fresh native packet-ACK-derived evidence and an independent proof
lifetime. For response bulk readiness, locally sourced ACK-derived carrier
evidence is authoritative for QUIC, while durable unambiguous Data ACK progress
may additionally establish a per-flow TCP MPP rate. A fresh exact TCP Product
rate can only raise native carrier capacity to the rate already demonstrated on
that carrier; it is never extrapolated, never lowers a native observation, and
retains carrier-capacity scope so active-flow sharing still applies. Peer
metric hints do not mint either proof.
