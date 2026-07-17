# MPTunnel Multipath Proxy Protocol (MPP) Version 2

## Status

This document defines the protocol implemented by this repository. Normative
terms such as MUST, SHOULD, and MAY are used as described by RFC 2119 and RFC
8174.

MPP borrows the connection-level sequence, Data ACK, receive-window,
reinjection, and backup-path principles of
[MPTCP](https://www.rfc-editor.org/rfc/rfc8684.html), and the directional path
usage model of
[Multipath QUIC](https://datatracker.ietf.org/doc/html/draft-ietf-quic-multipath-21).
It does not replace the congestion controller or loss recovery of either TCP
or QUIC.

Protocol version 2 is wire-incompatible with version 1. A peer MUST reject a
frame header carrying any unsupported version.

## 1. Scope

MPP carries application streams and datagrams over one or more authenticated
transport connections. Each transport connection uses either:

- TCP, with reliability, congestion control, and retransmission owned by the
  operating system TCP stack; or
- QUIC over UDP, with packetization, congestion control, loss recovery, and
  path MTU discovery owned by QUIC.

MPP owns only the data level above those transports:

- reliable-stream and datagram identities;
- an independent connection data sequence space in each direction of a
  reliable stream;
- per-direction ordered delivery, Data ACK ranges, and receive window;
- attribution of original transmissions and reinjected copies;
- selection among available carrier paths;
- bounded cross-path reinjection and failover; and
- mutable application demand.

TCP and QUIC remain separate below this boundary and are compared through
typed observations above it. Mixed TCP and QUIC is not a third transport.

## 2. Terminology

**Session**
: An authenticated MPP relationship identified by a `SessionId`.

**Transport path**
: One authenticated TCP connection or QUIC connection associated with a
  session and a `PathId`. Rust identifiers use `carrier` for this cross-transport
  abstraction; it is not a new congestion-control or recovery layer.

**Path instance**
: One live incarnation of a transport path, identified in the implementation by
  `path_instance_id`. Reusing a numeric `PathId` after a reconnect MUST NOT
  inherit state from the previous instance.

**Attachment incarnation**
: One reliable stream's membership on a path instance. Reattaching the same
  stream creates a new incarnation even when the physical transport connection
  remains live. The request owner names this `attachment_id`; the response
  binding names the analogous value `output_incarnation`.

**Reliable stream**
: One bidirectional ordered byte stream identified by a `StreamId` and target
  address, with an independent data sequence space in each direction.

**Data sequence number**
: The absolute byte offset in one direction of an MPP reliable stream.
  `STREAM_DATA.offset` is the connection data sequence number of its first
  byte. The opposite direction has an independent sequence beginning at zero.

**Data ACK**
: A `STREAM_ACK` range acknowledging MPP data in one direction. TCP ACKs and
  QUIC packet ACKs are transport evidence and MUST NOT release the MPP range
  ledger.

**Original transmission**
: The first assignment of a data-level byte range to a transport path.

**Reinjection**
: A later copy of an unacknowledged data-level byte range sent on the same or a
  different transport path.

**Available path**
: A path the receiver permits the peer to use for ordinary data.

**Backup path**
: A path the receiver prefers the peer to use only when no available path is
  schedulable. This is a directional preference, not path health.

**Demand**
: A mutable application objective: latency, throughput, or realtime. Demand
  influences scheduling but never classifies a path permanently.

## 3. Layer and Ownership Model

The protocol model is:

```text
application ingress
    MPP reliable stream or datagram
        per-direction connection offsets, Data ACK, receive window, reinjection
            available-first path scheduler
                TCP carrier controller | QUIC carrier controller
                    network
```

The MPP data level MUST be transport-neutral. It MUST NOT:

- implement a second TCP or QUIC congestion controller;
- infer transport loss solely from missing data-level progress;
- treat a TCP ACK or QUIC packet ACK as a Data ACK;
- encode local health as peer path usage;
- assign fixed Active, Service, Validation, Subflow, or Repair roles to an
  attachment; or
- tune eligibility to an operating system, interface name, laboratory rate,
  or fixed topology.

Carrier implementations MUST expose observations and bounded enqueue
capacity. They retain authority over native pacing, congestion windows,
retransmission, connection teardown, and transport errors.

## 4. Session and Path Establishment

### 4.1 Authentication

A new carrier sends, in order:

1. `SESSION_HELLO(session_id)`;
2. `SESSION_AUTH(session_id, nonce, issued_at_unix_secs, auth_tag)`;
3. `PATH_JOIN(session_id, path_id, underlay, nonce, issued_at_unix_secs,
   auth_tag)`; and
4. sequence-zero `PATH_STATUS` for that direction.

Authentication tags use HMAC-SHA256 over distinct session-authentication and
path-join contexts. The receiver MUST validate the timestamp freshness,
session identity, path identity, underlay, nonce, and tag. Replayed path-join
nonces MUST be rejected.

After acceptance, the receiver sends `SESSION_READY` and its own
sequence-zero `PATH_STATUS`. Stream or datagram work MUST NOT be admitted before
both are received. The authenticated TCP session remains its path control
channel. For QUIC, the first bidirectional stream that carried authentication
remains the connection control stream; later bidirectional streams carry
product streams or datagram flows.

The authenticated transport connection and its registration form one path
instance. Path metrics and usage sequences MUST be fenced by that physical
instance. Path proof state is owned by that path actor and keyed by its proof
identifier. Stream flights are separately fenced by the stream attachment
incarnation. Scheduler load leases account logical path use and MUST NOT be used
as proof or flight identity.

Within a session, a logical path is identified by `(underlay, path_id)`. The
path initiator selects `path_id`; the receiver MUST treat it as an opaque
protocol identifier. It is not either endpoint's path-configuration ordinal,
and the same numeric value MAY identify one TCP carrier and one QUIC carrier.
Path-scoped response frames echo the authenticated wire `path_id`.

Receiver-local policy and startup hints MUST come from the local listener that
accepted the carrier. They MUST NOT be selected by indexing local
configuration with a peer-supplied `path_id`. Local policy remains off wire;
only the directional `Available` or `Backup` preference is advertised.

### 4.2 Path lifecycle

`PATH_DRAIN(path_id)` requests graceful retirement of an MPP TCP path. No new
streams SHOULD attach after drain begins. Existing work may finish. The peer
completes retirement with `PATH_CLOSE(path_id, reason)`. The current QUIC
implementation retires the QUIC connection through its native lifecycle and
does not exchange these two MPP frames.

Local states such as active, suspect, draining, failed, disabled, and cooldown
are endpoint-local health. Except for `PATH_DRAIN` and `PATH_CLOSE`, they are
not wire values.

### 4.3 Peer diagnostics

An endpoint MAY send `PEER_STATUS_REQUEST(request_id)` on an
authenticated path control channel. The peer answers on the same channel with
`PEER_STATUS_RESPONSE(request_id, code, paths)`, where `code` is `OK`,
`DISABLED`, or `UNAVAILABLE`. A non-`OK` response MUST contain no paths.

Each returned path contains local state, directional `PathUsage`, and one typed
`PathMetrics` record. A server response MUST be filtered by the authenticated
requesting `session_id`; it MUST NOT include another session. Responses MUST
NOT contain endpoints, targets, local service tags, credentials, or local
configuration ordinals.

Peer status is diagnostic presentation only. It MUST NOT update scheduler
evidence, path health, congestion control, flow control, capacity admission,
or failover decisions. Implementations MUST bound the path count, encoded
frame, request queue, outstanding requests, and timeout. They SHOULD permit at
most one outstanding request per session. Automatic peer requests MUST occur
only after an authenticated local operator selects a finite refresh interval;
manual mode MUST send no periodic requests. An endpoint SHOULD rate-limit
accepted requests per authenticated session and
return `UNAVAILABLE` without sampling when the limit is reached. If the complete
path set cannot fit the codec limit, it MUST return `UNAVAILABLE`; a partial path
set is forbidden.

## 5. Directional Path Usage

`PATH_STATUS` contains:

```text
path_id   : u16
sequence  : u64
usage     : AVAILABLE(0) | BACKUP(1)
```

The receiver sends the status to tell the peer how the peer should use that
path for data sent toward the receiver. Therefore the two directions may have
different usage.

The sequence space belongs to one carrier instance and starts at zero.
After the handshake, an endpoint MUST accept only a status whose sequence is
strictly greater than the last accepted sequence for that instance. A stale
or duplicate sequence MUST NOT change scheduling state. A new authenticated
instance restarts at zero.

The current protocol-v2 runtime emits only the authenticated sequence-zero
status derived from listener policy. Receive paths retain the higher-sequence
fence, but no management or scheduler action currently originates a later
status. Dynamic peer-preference control therefore remains reserved until a
carrier-scoped control origin exists for both TCP and QUIC.

For ordinary data, the scheduler MUST:

1. discard paths that are locally unschedulable;
2. form the set of schedulable available paths;
3. use the backup set only if the available set is empty; and
4. rank paths within the chosen set using live metrics and current demand.

Backup preference MUST NOT be implemented as an arbitrary additive timing
penalty. An endpoint MAY locally reserve a configured path as backup even when
the peer advertises it as available. Either restriction is sufficient to keep
the path out of the ordinary available set.

Path usage is independent of authentication, path proof, liveness, congestion,
and application demand.

## 6. Reliable Streams

### 6.1 Neutral open and attachment

`OPEN_STREAM(stream_id, target, demand)` has no path role. The first accepted
open creates the reliable stream. A later open with the same `StreamId` may add
another carrier attachment only when its target exactly matches the original
target. Demand is an attachment-time hint; the endpoint may adapt its local
queued-work demand without assigning a role to the attachment.

Each accepted attachment is append-only for its live path instance. A
reannouncement on the identical live output may refresh demand. A distinct
duplicate live output for the same logical path is rejected. A replacement after
the prior attachment output closes receives a new attachment incarnation and
MUST NOT inherit flight, proof, rate, request-feedback, or load evidence.

The request owner identifies an attachment by
`(path_instance_id, attachment_id)`. The response binding uses the analogous
`(path_instance_id, output_incarnation)` identity. A response new-data dispatch
intent additionally carries the observed response-model generation; apply MUST
reject the intent if either identity or that generation changed before carrier
queue reservation and exact-flight commit. The physical identity changes when
the carrier is replaced; the attachment incarnation changes when this
stream detaches and reattaches, including on a still-live carrier.

Each sender begins its direction without implicit credit and waits for an
explicit `STREAM_MAX_DATA` or `STREAM_RESET` from that direction's receiver.
There is no role-specific implicit initial window.

### 6.2 Data sequence mapping

`STREAM_DATA(stream_id, offset, payload)` maps every payload byte to the
half-open data-level range:

```text
[offset, offset + payload.length)
```

The client-to-server and server-to-client directions maintain independent
offsets, retained ranges, Data ACK state, and flow-control limits. Frames are
interpreted in the direction in which they are received; equal numeric offsets
in opposite directions do not identify the same data.

The receiver MUST deduplicate overlapping copies, buffer out-of-order bytes
within configured bounds, and expose bytes to the application only in order.
The same range may arrive over TCP, QUIC, or both without changing its data
sequence identity.

The sender MUST retain unacknowledged ranges within its configured retention
envelope. Retention supports Data ACK processing and reinjection; it is not a
replacement for native TCP or QUIC retransmission.

### 6.3 Data ACK

`STREAM_ACK(stream_id, complete, ranges)` carries half-open ranges in the MPP
data sequence space. Every range entry MUST be non-empty. The list MAY
be empty, including for a complete snapshot before any MPP data arrives.

When `complete` is true, the range list is an authoritative snapshot of the
receiver's current received ranges and may expose a Data ACK gap. When false,
the list is partial progress and MUST NOT be used to infer a missing range.
`complete` does not mean end of stream.

Processing a Data ACK MUST be one transaction:

1. normalize and validate ranges;
2. release each newly acknowledged unique data-level byte once;
3. release every original or reinjected flight overlapping those bytes;
4. update local ACK-clock and admission evidence without changing the peer's
   advertised `max_offset`; and
5. publish exact per-path progress only when attribution is unambiguous.

If a byte was outstanding on more than one carrier, the Data ACK identifies
delivery but not which copy delivered it. The implementation MUST NOT invent
per-path delivery evidence for that range.

`STREAM_ACK` and `STREAM_MAX_DATA` are independent signals. A Data ACK releases
retained MPP data and flight and may inform local ACK-clock or admission
policy; it does not itself grant new offsets. `STREAM_MAX_DATA` grants offsets
in that direction but does not acknowledge any byte or release any flight.

### 6.4 Shared flow control

`STREAM_MAX_DATA(stream_id, max_offset)` advertises the maximum data sequence
offset the sender may assign in that direction. The maximum is shared across all
path attachments of that stream and direction. Attaching another
path does not multiply the receive window. The opposite direction has an
independent advertised maximum.

The sender MUST NOT assign new data whose end offset exceeds `max_offset`.
Transport queue capacity and congestion-window availability are additional
local constraints, not alternate MPP receive windows.

### 6.5 Completion and reset

`STREAM_FIN(stream_id, final_offset)` declares the final data sequence offset. FIN is
ordered in the same sequence space and may be sent on any live attachment.
`STREAM_DETACH(stream_id)` removes only that carrier attachment.
`STREAM_RESET(stream_id, reason)` terminates the reliable stream.

### 6.6 Break-before-make retention

An endpoint MAY retain an established reliable stream for a configured
absolute interval while it has no live carrier attachment. It preserves that
stream's data-sequence, Data ACK, receive-window, FIN, repair, and reorder
state, and MUST stop reading new application bytes while no output exists so
the configured MPP bounds and application transport backpressure remain
authoritative.

Reconnect attempts MAY rotate across configured TCP and QUIC paths but MUST
NOT extend the original no-attachment deadline. A newly authenticated
attachment resumes the same stream identity. Expiry retires the stream and its
application socket. Ordinary application idle while a live attachment exists
is not a no-attachment interval and is governed separately by native carrier
liveness.

### 6.7 Original transmission and reinjection

Before an original response range is committed, the sender revalidates the
exact path instance and attachment incarnation. The committed response flight
stores the logical path key and the stream-unique output incarnation; request
flights retain their exact attachment identity. Reinjection records another
flight for the same range and never creates new data sequence numbers.

Reinjection SHOULD use a distinct schedulable output when one exists. It is
admitted for explicit evidence such as carrier failure, a persistent Data ACK
gap, or a bounded tail condition. It MUST respect:

- the shared MPP receive window;
- transport enqueue capacity;
- the reordering envelope;
- the cumulative extra-traffic budget, except for the bounded critical
  recovery exception below; and
- exact carrier-instance and attachment identity.

MPP data-level stalls alone do not authorize unbounded duplication. Native transport
loss recovery continues independently.

Ordinary reinjection is limited by a cumulative extra-traffic budget funded by
a bounded startup allowance and unique data acknowledged by Data ACK.
A critical path-failure, persistent authoritative Data ACK gap, or live-tail
reinjection MAY proceed when that cumulative budget is exhausted only to
prevent the budget itself from deadlocking recovery. This exception MUST
remain bounded by one current event quantum, retained unacknowledged ranges,
exact flight identity, carrier queue/flight limits, repeat-delay suppression,
and a distinct output whenever the original carrier is still live. Its bytes
remain charged to the cumulative ledger, so it reduces later optional
reinjection authority.

The current recovery policy distinguishes three evidence states:

1. Failure of the exact original path instance permits immediate bounded
   reinjection on an eligible live alternative. A measured survivor SHOULD be
   preferred, but liveness is sufficient when no measured survivor remains.
2. An authoritative Data ACK snapshot that exposes the same lowest missing
   frontier must persist for three recovery intervals of the carrier that owns
   it before a target-bound reinjection flight is admitted. TCP uses its
   retransmission timeout (RTO); QUIC uses its probe timeout (PTO). Growth of
   the ACK horizon above that frontier does not reset the persistence interval.
3. A contiguous live tail with no authoritative gap may send one bounded probe
   after one owner-carrier recovery interval; another tail probe requires three
   such intervals without Data ACK progress.

These intervals are MPP local policy, not requirements imposed by MPTCP, QUIC,
or QUIC's definition of persistent congestion. Native TCP and QUIC recovery
retain their own timers throughout. Request placement stops treating a
non-progressing original attachment as eligible after four TCP RTOs or three
QUIC PTOs when another attachment is available; the original carrier remains
connected and continues native recovery.

## 7. Scheduling

The scheduler consumes immutable path snapshots and returns an intent. The
state owner revalidates the intent before enqueue and commits range ownership
only after enqueue succeeds.

A scheduling snapshot may include:

- local health and drain state;
- peer usage and local backup policy;
- smoothed RTT, RTT variation, and jitter;
- native delivery and pacing rate when available;
- MPP Data ACK progress;
- transport and MPP bytes in flight;
- transport and MPP queue bytes;
- active demand; and
- evidence confidence and freshness.

Transport queue/flight and MPP queue/Data-ACK flight are overlapping views of
one delivery pipeline. Completion-time ranking MUST NOT add the two views as
independent outstanding bytes. It uses the larger path-owned view after
excluding connection-wide MPP queue bytes that are common to every candidate.
Transport congestion windows, pacing, and native loss recovery remain
carrier-owned limits, not MPP scheduling windows.
Loss, ECN, jitter, and queue observations MAY affect path ranking and
reordering decisions. They MUST NOT reduce an MPP service quantum, synthesize
a congestion window, or pace a carrier; doing so would create a second
congestion controller above TCP or QUIC.

Within the available or backup set selected by Section 5, ordinary data SHOULD
minimize estimated completion time subject to flow control, enqueue capacity,
and reordering bounds. Latency, throughput, and realtime demand may change the
scoring horizon, but demand MUST NOT become a fixed path tag.

TCP and QUIC evidence may have different provenance. A value measured per flow
MUST NOT be treated as shared path capacity without sufficient evidence. A
configured rate is a startup prior, not proof.

The output carrying the contiguous Data Sequence frontier remains bounded by
the shared MPP receive window, enqueue capacity, and its native carrier
controller; MPP MUST NOT impose a second congestion window on that output.
Before an additional response output has durable, unambiguous Data ACK coverage
for its original transmissions, it MUST NOT own more than one bounded startup
flight. Native TCP ACK or QUIC packet-ACK evidence alone MUST NOT unlock mature
additional-path placement. Once exact original-data Data ACK coverage reaches
the configured startup sample floor, the response scheduler may use the mature
connection-window model for that output. Data ACK coverage of duplicated bytes
MUST NOT be attributed to either copy for this purpose.

The observe, revalidate, and commit sequence MUST fence carrier instance,
attachment incarnation, model generation, data frontier, proof authority,
and queue credit. If any fence changes, the sender retries from a new snapshot.

## 8. Path Evidence and Measurements

`PATH_METRICS` reports typed, directional evidence. It includes path and
underlay identity, metric epoch and age, RTT values, delivery and pacing rates,
loss and ECN observations, flight and queue values, inflight limits,
confidence, application-limited state, and sample counts. Metrics are advisory
scheduler evidence. They do not grant range ownership or change path usage.

`PATH_PROOF_DATA` and `PATH_PROOF_ACK` prove that a particular authenticated
path can carry MPP frames. Proof identity is scoped to the owning path actor.

`PATH_CAPACITY_DATA`, `PATH_CAPACITY_FINISH`, and `PATH_CAPACITY_RECEIPT` form a
bounded measurement transaction. Measurement bytes do not consume stream
offsets and do not produce Data ACK credit. The current implementation starts
these active measurements only in the client-to-server request direction and
keeps separate TCP and QUIC reservation, timing, and cleanup state. Ordinary
traffic MUST NOT interleave with an exclusive measurement epoch in a way that
corrupts its evidence.

TCP admission uses receiver-confirmed capacity receipts and, when available,
telemetry from the exact TCP socket. QUIC admission uses fresh native QUIC
packet-ACK-derived sender evidence and its own proof lifetime. Neither proof may
be substituted for the other. On the response side, only locally sourced,
ACK-derived carrier evidence may establish QUIC bulk readiness; exact
unambiguous Data ACK progress may additionally establish a per-flow TCP MPP
rate after the durable startup floor. Peer metric hints remain advisory.

Optional native telemetry may refine estimates. It MUST have a portable
fallback and MUST NOT be an eligibility requirement.

When native TCP send credit is unavailable, a Data ACK rate MUST NOT be
treated as a synthetic TCP congestion window. An unproven path receives one
bounded startup flight. After durable original-data progress, the portable
sender uses the configured product resource envelope, shared receive/reorder
limits, completion-time ranking, and socket backpressure; TCP still owns
packet flight, pacing, congestion response, and recovery.

## 9. Datagrams

`OPEN_DGRAM_FLOW(flow_id, target)` creates an MPP datagram association.
`DGRAM_DATA(flow_id, datagram_id, ttl_ms, payload)` carries one datagram.
`DGRAM_FEEDBACK(flow_id, received)` acknowledges datagram IDs as half-open
ranges. `DGRAM_CLOSE(flow_id)` closes the association.

Datagrams do not use the reliable stream data sequence space. Selection and
failover MAY compare TCP and QUIC carrier observations, but a datagram MUST be
dropped when its remaining TTL cannot cover the selected path estimate.
`DGRAM_FEEDBACK` means that an attempt was admitted to the target worker; it is
not an end-to-end delivery ACK. The current runtime assigns a new flow-local
`datagram_id` to an alternative-path attempt and provides no cross-path
exactly-once guarantee. A delayed first attempt and its retry may both reach the
target.

## 10. Wire Format

Every frame starts with a fixed ten-byte header:

```text
0..4   magic          ASCII "MPTF"
4      version        2
5      frame kind     u8
6..10  payload length u32, network byte order
```

All multibyte integers use network byte order. A decoder MUST reject invalid
magic, unsupported version, unknown kind, truncated or trailing bytes, invalid
enum values, a range entry whose start is not less than its end, zero target
ports, and configured size limit violations.

The assigned frame kinds are:

| Kind | Frame | Payload fields |
|---:|---|---|
| 1 | `SESSION_HELLO` | `session_id:u64` |
| 2 | `SESSION_READY` | none |
| 3 | `SESSION_CLOSE` | `reason:u8` |
| 4 | `PATH_JOIN` | `session_id:u64, path_id:u16, underlay:u8, nonce:16B, issued_at_unix_secs:u64, auth_tag:32B` |
| 7 | `OPEN_STREAM` | `stream_id:u64, target, demand:u8` |
| 8 | `STREAM_DATA` | `stream_id:u64, offset:u64, length:u32, bytes` |
| 9 | `STREAM_ACK` | `stream_id:u64, complete:u8, count:u16, ranges[count]` |
| 10 | `STREAM_MAX_DATA` | `stream_id:u64, max_offset:u64` |
| 11 | `STREAM_RESET` | `stream_id:u64, reason:u8` |
| 12 | `OPEN_DGRAM_FLOW` | `flow_id:u64, target` |
| 13 | `DGRAM_DATA` | `flow_id:u64, datagram_id:u64, ttl_ms:u32, length:u32, bytes` |
| 14 | `DGRAM_CLOSE` | `flow_id:u64` |
| 16 | `PING` | `nonce:u64` |
| 17 | `PONG` | `nonce:u64` |
| 18 | `SESSION_AUTH` | `session_id:u64, nonce:16B, issued_at_unix_secs:u64, auth_tag:32B` |
| 20 | `PATH_STATUS` | `path_id:u16, sequence:u64, usage:u8` |
| 21 | `PATH_DRAIN` | `path_id:u16` |
| 22 | `PATH_CLOSE` | `path_id:u16, reason:u8` |
| 23 | `DGRAM_FEEDBACK` | `flow_id:u64, count:u16, ranges[count]` |
| 24 | `PATH_METRICS` | fixed typed metric record below |
| 27 | `STREAM_FIN` | `stream_id:u64, final_offset:u64` |
| 30 | `STREAM_DETACH` | `stream_id:u64` |
| 31 | `PATH_PROOF_DATA` | `path_id:u16, proof_id:u64, length:u32, bytes` |
| 32 | `PATH_PROOF_ACK` | `path_id:u16, proof_id:u64, payload_bytes:u32` |
| 33 | `PATH_CAPACITY_DATA` | `path_id:u16, measurement_id:u64, length:u32, bytes` |
| 34 | `PATH_CAPACITY_FINISH` | `path_id:u16, measurement_id:u64, payload_bytes:u64` |
| 35 | `PATH_CAPACITY_RECEIPT` | `path_id:u16, measurement_id:u64, received_payload_bytes:u64` |
| 36 | `PEER_STATUS_REQUEST` | `request_id:u64` |
| 37 | `PEER_STATUS_RESPONSE` | `request_id:u64, code:u8, count:u16, paths[count]` |

Kinds 5, 6, 15, 19, 25, 26, 28, and 29 are reserved and MUST NOT be sent.
`PATH_DRAIN` and `PATH_CLOSE` are currently valid only on the MPP TCP path
session; QUIC paths use native connection retirement.

Each peer-status path entry consists of `state:u8`, `usage:u8`, and the fixed
typed path-metric record. State values are active `0`, suspect `1`, draining
`2`, and failed `3`; they describe only the responding endpoint's current
observation. Status code values are OK `0`, disabled `1`, and unavailable `2`.

Each `ranges[count]` entry consists of `start:u64, end:u64`.

A target begins with a type byte: domain `1`, IPv4 `2`, or IPv6 `3`. A domain
contains a `u16` UTF-8 byte length, the host bytes, and a nonzero `u16` port.
IPv4 and IPv6 contain their fixed address bytes and a nonzero `u16` port.

Demand values are latency `1`, throughput `2`, and realtime `3`. Underlay
values are TCP `1` and UDP `2`. Path-metric direction values are
client-to-server `1` and server-to-client `2`. Boolean fields use `0` for false
and `1` for true.

The `PATH_METRICS` payload is the following fixed record, in wire order:

```text
path_id:u16, underlay:u8, direction:u8, metric_epoch:u64,
metric_age_us:u32, srtt_us:u32, rttvar_us:u32, jitter_us:u32,
delivery_rate_bps:u64, pacing_rate_bps:u64, loss_ppm:u32, ecn_ppm:u32,
loss_observed:u8, ecn_observed:u8, bytes_in_flight:u64, queue_bytes:u64,
inflight_limit_bytes:u64, inflight_hi_bytes:u64, confidence_ppm:u32,
app_limited:u8, has_ack_derived_data_sample:u8, data_sample_count:u32,
data_sample_bytes:u64
```

Close-reason values are normal `0`, protocol error `1`, authentication failed
`2`, and policy rejected `3`. Stream-reset reason values are refused `1`, timed
out `2`, remote closed `3`, and policy rejected `4`.

## 11. Limits and Error Handling

Endpoints MUST enforce configured limits for frame bytes, payload bytes,
acknowledgement ranges, host length, streams, datagram flows, MPP receive
window, reordering, retained unacknowledged ranges, carrier queues, and
measurement traffic.

Protocol violations close the affected carrier or session according to scope.
A path failure MUST invalidate only state owned by that carrier instance.
The reliable stream may continue over surviving attachments and reinject only
unacknowledged ranges.

Failure publication MUST carry the exact carrier-instance identity. A delayed
status or teardown report from an older instance MUST NOT change the health,
usage, flight, or proof state established by a newer authenticated instance.

Cancellation and task teardown MUST reconcile queue bytes, flight bytes,
measurement tickets, load leases, and registry membership exactly once.

## 12. Security and Privacy

Every transport path is encrypted and authenticated before MPP data admission. Session
and path authentication use separate HMAC contexts. Nonces and freshness
windows limit replay. Target policy is enforced at the receiving endpoint and
is not delegated to the carrier.

Metrics and usage are accepted only for the authenticated path identity. A
peer cannot use `PATH_STATUS` to declare local health, bypass flow control,
grant itself capacity, or transfer state across a reconnect.

Implementations SHOULD limit diagnostic output because path metrics, targets,
and timing can expose network topology and traffic characteristics.
Peer-status responses follow the stricter rules in Section 4.3 and disclose no
target or endpoint value.

## 13. Platform Boundary

The protocol, data-level model, scheduler, and ownership rules are
platform-neutral. Platform-specific code is limited to host adapters such as
packet-device acquisition, socket binding or protection, and optional native
TCP telemetry.

QUIC packet recovery, congestion control, and timeouts remain transport-owned
when a host lacks optional UDP facilities. An implementation MAY replace its
optimized UDP adapter with basic datagram I/O when capability probing reports
unsupported ECN, packet-info, or segmentation facilities. That adapter choice
MUST NOT change MPP path eligibility, scheduling, sequencing, Data ACK, or
reinjection policy, and an implementation SHOULD report the expected
performance reduction to the operator.

Native TCP telemetry is an optional capability adapter: Linux and Android use
the stable `TCP_INFO` UAPI prefix, macOS uses `TCP_CONNECTION_INFO`, and
supported Windows versions use `SIO_TCP_INFO`. Every field is independently
optional and normalized at the adapter boundary; absent host fields MUST remain
unknown rather than becoming measured zero or delivery authority. Portable MPP
Data ACK observations remain available when native inspection is unsupported or
fails. Missing native send credit selects the capability-based portable rule in
Section 8; it does not select an operating-system policy. No scheduling
decision may branch on the operating system. A Windows client with a Linux
server is a primary target. Wine can prove CLI/configuration and portable TCP
or basic-UDP QUIC proxy behavior, but native packet-device, optimized socket,
and network integration remain separate release evidence.

A native TCP drain decision MUST require both exact bytes in flight and the
unsent sender queue from the same snapshot. A partial RTT or congestion-window
shape remains useful for ranking and service credit, but reinjection MUST use
exact MPP product-flight ownership until both native drain counters are known.

## 14. Required Invariants

A conforming implementation preserves all of these invariants:

1. One data-level byte has one sequence identity regardless of transport copies.
2. Only Data ACK releases MPP ranges.
3. Duplicate delivery does not create duplicate rate evidence.
4. Each direction's receive window is shared by all attachments of one stream,
   and the opposite direction has an independent window.
5. TCP and QUIC retain independent native recovery and congestion control.
6. Available paths are considered before backup paths.
7. Path usage, local health, and application demand are independent facts.
8. Numeric path IDs never replace carrier-instance identity.
9. Scheduling observes immutable state and revalidates before commit.
10. Reinjection is evidence-driven, bounded, and never creates new offsets.
11. No fixed attachment role determines where future data must be sent.
12. Optional platform telemetry never becomes a correctness dependency.
13. Path-instance lifetime and stream attachment incarnation are separate fences.
