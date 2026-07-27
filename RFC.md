# MPTunnel Multipath Proxy Protocol (MPP) Version 4

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

Protocol version 4 replaces the private TCP record cipher with TLS 1.3. TCP
deliberately negotiates no ALPN; QUIC negotiates the standard `h3` ALPN and
carries MPP through the HTTP/3 extension defined in Section 10. It retains the
corrected datagram identity semantics introduced during version 3 development
and is incompatible with versions 1, 2, and 3. A peer MUST reject an
unsupported carrier presentation or frame version. TCP and QUIC use one
independently configured TLS server identity and client trust policy; an MPP
application credential MUST NOT be used to derive a certificate, private key,
or certificate verifier.

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

### 3.1 Product gateway boundary

Product routing MUST be deterministic first-match policy over normalized
destination domain or IP, resolved destination IP, source IP,
destination/source port, TCP/UDP network, inbound, authenticated principal,
and pre/post-resolution stage. Implementations MUST reject configuration for
a match category that live flow construction cannot supply; silently treating
an unavailable attribute as permanently absent is not conforming behavior.

An implementation MAY place a Product gateway above independent outbound
leaves or MPP sessions. That gateway selects exactly one member while a new
flow is opening. It MUST NOT merge carriers from separate MPP sessions or use
path IDs, congestion state, queue state, reinjection state, or any other
`PathScheduler` input for member ranking.

Gateway health may use bounded Product-owned end-to-end probes and passive
target-open or completed-flow outcomes. Probe freshness, hysteresis, circuit
cooldown, recovery, drain, and manual override are Product state and do not
change MPP path health or usage.

Multiple member attempts for one flow MUST share one absolute opening
deadline. A retry is permitted only before application data or a target
datagram association has crossed the implementation's commit boundary. Once
committed, the flow remains bound to that member; a later failure is health
evidence for future flows and MUST NOT cause transparent replay.

## 4. Session and Path Establishment

### 4.1 Authentication

Carrier-session authentication is carrier-specific. Path attachment after
session authentication is common to TCP and QUIC.

A new QUIC carrier first presents the encrypted credential-derived candidate
selector defined in Section 10.2. The server MUST accept that selector before
request DATA reaches the MPP frame parser. This is a bounded parser gate, not
session or path authorization; every check below remains mandatory. The first
accepted selector is latched to that QUIC connection, and every later carrier
request on the connection MUST present the same selector.

On its first selector-accepted HTTP/3 request stream, a new QUIC carrier sends
in this order:

1. `SESSION_HELLO(session_id)`;
2. `SESSION_AUTH(session_id, credential_id, nonce, issued_at_unix_secs,
   auth_tag)`;
3. `PATH_JOIN(session_id, credential_id, path_id, underlay, nonce,
   issued_at_unix_secs, auth_tag)`; and
4. sequence-zero `PATH_STATUS` for that direction.

A new TCP carrier completes a full TLS 1.3 handshake and then sends, in this
order:

1. the fixed 131-byte TCP session-admission prelude defined in Section 10.1;
2. `PATH_JOIN(session_id, credential_id, path_id, underlay, nonce,
   issued_at_unix_secs, auth_tag)`; and
3. sequence-zero `PATH_STATUS` for that direction.

The TCP prelude semantically replaces `SESSION_HELLO` and `SESSION_AUTH`.
A TCP carrier MUST NOT send those two frames before `PATH_JOIN`. The prelude,
`PATH_JOIN`, and sequence-zero `PATH_STATUS` form one client admission flight;
TLS record boundaries and write batching do not alter that ordering.

All authentication tags use HMAC-SHA256 keyed by the named MPP application
credential. The QUIC `SESSION_AUTH` transcript is:

```text
"mptunnel session auth v4" ||
session_id:u64 ||
credential_id_length:u8 || credential_id:bytes ||
nonce:16B ||
issued_at_unix_secs:u64
```

The TCP prelude authentication transcript is:

```text
"mptunnel tcp session auth v1" ||
carrier_role:u8 = 1 ||
direction:u8 = 1 ||
tls_exporter:32B ||
session_id:u64 ||
credential_id_length:u8 || credential_id:bytes ||
nonce:16B ||
issued_at_unix_secs:u64
```

`tls_exporter` is derived from the completed TLS connection as specified in
Section 10.1. Binding the tag to that value prevents a valid TCP prelude from
being replayed on another TLS connection.

The `PATH_JOIN` transcript is common to both carrier presentations:

```text
"mptunnel path join v4" ||
session_id:u64 ||
credential_id_length:u8 || credential_id:bytes ||
path_id:u16 || underlay:u8 ||
nonce:16B ||
issued_at_unix_secs:u64
```

Integers use network byte order and every `auth_tag` is the complete 32-byte
HMAC. The receiver MUST select the named credential and reject an unknown,
revoked, or expired credential. It MUST validate timestamp freshness, session
identity, credential identity, path identity, expected underlay, nonce, and
tag. For TCP it MUST additionally validate the fixed prelude fields,
canonical padding, and TLS-exporter-bound tag.

`PATH_JOIN` MUST follow successful carrier-specific session authentication.
Its session and credential identities MUST equal those authenticated by the
QUIC `SESSION_AUTH` or TCP admission prelude. Replayed path-join nonces MUST be
rejected. An endpoint MUST NOT expose which credential lookup or
authentication check failed to an unauthenticated peer.

Credential lookup and permit issuance are authentication-time operations.
After a valid tag, the server binds an immutable principal permit to the path;
per-frame and per-byte data processing MUST NOT call credential policy.
Different credential IDs MAY overlap during rotation and MAY map to the same
principal. Every path attached to one MPP session MUST map to that same
principal.

A credential ID permanently names one principal and key for the process
lifetime. Rotation uses a new credential ID. Publishing revocation, removal,
or expiry MUST reject new authentication at publication and retire only actors
admitted through that credential at its configured absolute deadline plus
grace. It MUST NOT retire actors using an overlapping credential merely
because they share a principal. Revocation is monotonic within a process, and
a retired ID MUST NOT be reused before restart.

After acceptance, the receiver sends `SESSION_READY` and its own
sequence-zero `PATH_STATUS`. Stream or datagram work MUST NOT be admitted before
both are received. The authenticated TCP session remains its path control
channel. For QUIC, the first matching HTTP/3 request stream whose admission
succeeds remains the connection control stream; later bidirectional streams
carry product streams or datagram flows.

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

The first successful authenticated reachability validation of a logical path
SHOULD retain its transport connection as the durable path instance. This
avoids repeating transport and MPP authentication before the first product
stream. TCP and QUIC use separate carrier setup mechanisms but expose the same
lifecycle contract. Once a durable instance exists, periodic reachability
probes MUST use an isolated connection or native carrier liveness; they MUST
NOT disturb product streams. A validation RTT sample MUST cover an
authenticated request/response exchange and exclude transport connection
setup.

`PATH_DRAIN(path_id)` requests graceful retirement of an MPP TCP path. No new
streams or datagram flows SHOULD attach after drain begins. Existing work may
finish. One live TCP path is one authenticated connection actor shared by path
control, reliable-stream attachments, and datagram-flow attachments. Product
FIN, detach, reset, or `DGRAM_CLOSE` retires only the affected product state;
path and session lifecycle own physical carrier retirement. The peer completes
retirement with `PATH_CLOSE(path_id, reason)`. The current QUIC implementation
retires the QUIC connection through its native lifecycle and does not exchange
these two MPP frames.

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

The current protocol-v4 runtime emits only the authenticated sequence-zero
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

The sender retains the greatest `max_offset` observed for the direction; a
smaller value does not revoke credit. An endpoint therefore MAY acknowledge a
later attachment with a credit-neutral `STREAM_MAX_DATA(stream_id, 0)`. It
MUST NOT derive a new maximum from the attachment's demand, carrier type, or
local carrier limits. Only the logical receive owner publishes additional
credit.

The sender MUST NOT assign new data whose end offset exceeds `max_offset`.
Transport queue capacity and congestion-window availability are additional
local constraints, not alternate MPP receive windows.

### 6.5 Completion and reset

`STREAM_FIN(stream_id, final_offset)` declares the final data sequence offset
and may be sent on any live attachment. A receiver rejects an offset behind
any received data or one that conflicts with an earlier FIN. Once FIN is
accepted, later data MUST NOT extend beyond its final offset. Otherwise FIN
remains pending until the contiguous receive frontier reaches `final_offset`.
Matching duplicates are idempotent, and data or reinjection below the final
offset remains valid. `STREAM_FIN` does not remove an attachment.
`STREAM_DETACH(stream_id)` removes only that carrier attachment.
`STREAM_RESET(stream_id, reason)` terminates the reliable stream. Native TCP
EOF terminates the physical carrier and is not per-stream FIN or detach.

A native QUIC stream FIN closes only that carrier byte-stream direction. It
MUST NOT be interpreted as `STREAM_FIN`, `STREAM_DETACH`, or completion of the
MPP stream. The independently writable direction remains available until
attachment teardown for outstanding `STREAM_ACK`, `STREAM_FIN`, and
`STREAM_DETACH` frames. A native FIN at a frame boundary is a clean carrier
half-close; a native FIN inside an MPP frame is a truncation error.

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
2. A complete Data ACK snapshot may establish omitted ranges. Later positive
   partial ACK ranges extend that state but cannot establish omissions by
   themselves. Request feedback may be fragmented across paths, so its same
   lowest missing frontier waits one owner-carrier RTO/PTO from first
   authoritative observation. For response feedback, a later ACK event may
   authorize one bounded repair after the original flight exceeds TCP RACK's
   5/4-SRTT or QUIC's 9/8-SRTT time threshold, provided the alternative can
   complete before the owner RTO/PTO. ACK silence alone waits RTO/PTO.
3. A contiguous live tail with no authoritative gap may send one bounded probe
   after one owner-carrier recovery interval. Another repair requires another
   recovery interval without Data ACK progress.

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
`DGRAM_FEEDBACK(flow_id, received)` reports datagram IDs admitted as half-open
ranges. `DGRAM_CLOSE(flow_id)` closes the association.

Datagrams do not use the reliable stream data sequence space. Datagram identity
is `(session_id, flow_id, direction, datagram_id)`. Direction is implicit in
authenticated frame travel and is not encoded. Client-to-server requests and
server-to-client responses have independent ID spaces; equal numeric IDs in
opposite directions are distinct. TCP and QUIC copies or retries of one
directional datagram MUST preserve its ID and payload. A flow ID binds to one
target for its retained lifetime; reuse of that binding with a different target
or payload is a protocol violation.

Selection and failover MAY compare TCP and QUIC carrier observations, but a
datagram MUST be dropped when its remaining TTL cannot cover the selected path
estimate. `DGRAM_FEEDBACK` acknowledges directional `DGRAM_DATA` admitted by
the peer. Request feedback stops request replay and records the response route.
Response feedback permits cached response replay to retire. It is not an
end-to-end target delivery acknowledgement.

Before feedback, one request is limited to two product attempts. When a ranked,
unattempted alternative remains, the first attempt waits one modeled response
timeout derived from that carrier's observations, then MAY retry on the
alternative while TTL remains. An alternative attempt MUST use a configured
path not already used by this request; reopening the same path is not an
alternative. The final or only attempt uses three modeled response timeouts,
capped by the absolute product TTL, to tolerate ordinary loss without unbounded
replay. TCP and QUIC derive their response estimates independently; this
product rule does not replace native recovery.

After matching request-direction `DGRAM_FEEDBACK`, the sender MUST NOT replay
the request on another carrier because target processing may already have
begun. It may wait for the response until the absolute product TTL. Before
feedback, an alternative-path attempt preserves the same data-level identity. The receiver
MUST forward that identity to the target at most once, update the response route
when a carrier copy is admitted, and retain a bounded response replay until
feedback or expiry. This is at-most-once target forwarding within the retained
MPP session; it is not an end-to-end exactly-once guarantee.

Multiple bounded request identities may be outstanding on one flow. Target
responses use an independent reverse-direction ID space and need not correlate
one-to-one with requests. A duplicate request identity updates its response
route without duplicate target forwarding; a fresh request identity is
admitted independently. `DGRAM_CLOSE` detaches that carrier's flow binding.
Another carrier may reattach the same session, flow, and target during the
configured retention interval.

## 10. Wire Format

Every frame starts with a fixed ten-byte header:

```text
0..4   magic          ASCII "MPTF"
4      version        4
5      frame kind     u8
6..10  payload length u32, network byte order
```

All multibyte integers use network byte order. A decoder MUST reject invalid
magic, unsupported version, unknown kind, truncated or trailing bytes, invalid
enum values, a range entry whose start is not less than its end, zero target
ports, and configured size limit violations.

The MPP frame version is independent of TLS records. TCP carriers MUST use TLS
1.3 with no negotiated ALPN; there is no private MPP record cipher or legacy
record envelope. QUIC retains its native TLS and packet protection and MUST
negotiate exactly `h3`. Both carriers MUST authenticate the explicitly
configured TLS server identity; the server certificate and client trust anchor
are independent of the MPP application credential. After transport
authentication, both carriers still reject every unsupported MPP frame
version. Clients and servers MUST disable 0-RTT for MPP carrier connections.

### 10.1 TCP carrier presentation

The fixed TCP session-admission prelude is carrier-admission data, not an MPP
frame, and therefore does not begin with the `MPTF` frame header. It is sent
exactly once after a completed TLS 1.3 handshake. TCP MUST negotiate no ALPN
and MUST NOT accept early data.

The prelude is exactly 131 bytes:

```text
0        carrier_role              u8; client = 1
1        direction                 u8; client-to-server = 1
2        credential_id_length      u8; 1 through 64
3..67    credential_id_slot        64B
67..75   session_id                u64
75..91   nonce                     16B
91..99   issued_at_unix_secs       u64
99..131  auth_tag                  32B
```

The first `credential_id_length` bytes of `credential_id_slot` contain the
canonical credential ID. Every remaining byte in that 64-byte slot MUST be
zero. A receiver MUST reject a noncanonical credential ID, an invalid length,
or nonzero padding.

Both endpoints export exactly 32 bytes from the completed TLS 1.3 connection
using label `EXPORTER-mptunnel-tcp-admission-v1` and no exporter context. Those
bytes are `tls_exporter` in the Section 4.1 HMAC transcript. An early exporter
MUST NOT be used.

A valid prelude is followed immediately by the ordinary MPP `PATH_JOIN` and
sequence-zero `PATH_STATUS` frames. After admission, TCP carries ordinary MPP
frames directly inside TLS application data.

The listener reads exactly one complete prelude before interpreting any field.
An incomplete or rejected prelude, TLS failure, read failure, or
authentication timeout MUST be closed without application response bytes or a
carrier-specific close reason. Once the prelude has authenticated the peer,
later invalid MPP frames are ordinary authenticated protocol violations and
follow Section 11.

### 10.2 HTTP/3 carrier presentation

Each logical QUIC carrier stream is one ordinary, full-duplex HTTP/3 request
stream. Its encrypted request field section MUST contain:

```text
:method = POST
:scheme = https
:authority = configured TLS server identity
:path = /
content-type = application/octet-stream
mpp-datagram = ?1
authorization = Bearer <64 lowercase hexadecimal digits>
```

`:authority` MUST exactly equal the TLS server identity sent as SNI on that
QUIC connection. Because TLS omits SNI for an IP-address identity, a QUIC path
group MUST configure a DNS TLS server identity; its carrier endpoint remains
independent and MAY be a literal IP address. `:path` MUST be exactly `/` with
no query component. A missing SNI, origin-form target, other scheme, other
authority, or query is a nonmatching request and follows the same public
rejection behavior below.

The private `mpp-datagram` request field explicitly opts that request into the
MPP HTTP Datagram extension. It is encrypted by HTTP/3 and is not an ALPN,
HTTP/3 setting, registered upgrade token, or pre-authentication wire marker.
MPP does not use Extended CONNECT, CONNECT-UDP, WebTransport, Capsule Protocol,
the `:protocol` pseudo-header, or `capsule-protocol`.

The selector is:

```text
HMAC-SHA256(
  credential_secret,
  "mptunnel quic candidate selector v1" ||
  credential_id_length:u64 ||
  credential_id:bytes
)
```

`credential_id_length` is encoded in network byte order. The 32-byte result is
serialized as exactly 64 lowercase hexadecimal digits after the exact
`Bearer ` prefix. The request MUST contain exactly one such field. The server
MUST reject a missing, duplicate, malformed, noncanonical, expired, revoked,
or unmatched selector before exposing request DATA to the MPP parser.
Selector equality MUST use constant-time byte comparison over the bounded
active credential set.

The first accepted selector is latched to the QUIC connection. Later carrier
requests MUST present the same canonical selector and do not reconsult the
credential authority; authenticated-session retirement remains governed by
the bounded revocation grace. The selector demonstrates candidate credential
knowledge but is not channel-bound, freshness proof, session authorization,
path authorization, or replay admission. `SESSION_AUTH`, `PATH_JOIN`, and all
normal checks remain mandatory.

The first selector-accepted request stream sends, in request DATA,
`SESSION_HELLO`, `SESSION_AUTH`, `PATH_JOIN`, and sequence-zero `PATH_STATUS`
in the order required by Section 4.1. It becomes the connection control stream
only after all four records are accepted. Later matching request streams on
that authenticated connection carry product streams or datagram flows and do
not repeat connection admission.

The server MUST NOT send a successful HTTP response before application
authentication, common `PATH_JOIN` validation, replay admission, and the
sequence-zero usage advertisement succeed. After acceptance it sends a 2xx
response before response DATA containing `SESSION_READY` and its own
sequence-zero `PATH_STATUS`.

A nonmatching request, a rejected selector, or a selector-accepted request
whose MPP authentication fails receives the same marker-free `404 Not Found`
response used for an ordinary unknown resource. It MUST NOT receive an
MPP-specific status, header, body, or close reason. TLS or QUIC connections
that fail before an HTTP/3 request exists are closed using their standard
transport behavior and have no HTTP-response requirement.

All reliable MPP frames, including datagram flow open, close, and feedback,
travel in HTTP/3 DATA. Within the DATA byte stream, every MPP frame is prefixed
by its encoded length as an unsigned 32-bit network-order integer. HTTP/3 DATA
frame boundaries are independent of MPP record boundaries: one DATA frame MAY
contain several complete records, and one record MAY span received DATA
chunks. A receiver MUST apply the configured frame limit before buffering a
declared record.

Both peers MUST advertise the HTTP/3 `H3_DATAGRAM` setting before sending an
MPP HTTP Datagram. The setting is a generic capability signal; the encrypted
`mpp-datagram: ?1` request field is the per-request MPP opt-in. A QUIC
DATAGRAM carrying MPP data starts with the RFC 9297 Quarter Stream ID of the
associated client-initiated request stream, encoded as a QUIC variable-length
integer. The remaining payload is:

```text
0       extension version       u8; currently 1
1..9    flow_id                 u64
9..17   datagram_id             u64
17..21  remaining_ttl_ms        u32; nonzero
21..23  fragment_index          u16; zero based
23..25  fragment_count          u16; 1 through 64
25..29  total_payload_length    u32
29..    fragment payload
```

All multibyte fields in this envelope use network byte order. The MPP envelope
is 29 bytes per fragment. Including the Quarter Stream ID, its exact
application overhead is normally 30 bytes and at most 37 bytes per fragment;
QUIC DATAGRAM framing, packet protection, UDP, and IP overhead are additional.
The sender fragments against Quinn's current maximum datagram size and MUST
reject payloads requiring more than 64 fragments. A zero-length UDP datagram
uses one fragment with index zero and total length zero.

The sender MUST submit `OPEN_DGRAM_FLOW` on reliable DATA before it emits
native data for that flow. Native QUIC DATAGRAM can nevertheless overtake
reliable HEADERS or DATA in the network. A receiver MAY retain that first
flight for no longer than the smaller of its remaining TTL and a bounded
two-RTT handoff window; this is receiver-side reordering tolerance and MUST NOT
add a sender round trip or retransmit the datagram. Pending request routes,
per-route packets, total buffered bytes, active flow IDs, and fragment
reassemblies MUST all be bounded. Incomplete reassembly expires from the
packet's original receipt time and releases all resource charges without
waiting for another packet.

Closing a flow removes only its live-flow charge. Previously used flow IDs
MUST NOT reopen on the same request stream, while monotonically allocated new
IDs MUST remain usable for the request's lifetime. A receiver MUST silently
drop native data for a reliably closed flow and MUST bound any native data
deferred while its reliable open is still in flight.

### 10.3 MPP frame assignments

The TCP admission prelude is not a frame assignment.
`SESSION_HELLO` and `SESSION_AUTH` are used by QUIC connection admission;
TCP uses the Section 10.1 prelude instead. `PATH_JOIN` is the common
carrier-independent path-admission frame.

The assigned frame kinds are:

| Kind | Frame | Payload fields |
|---:|---|---|
| 1 | `SESSION_HELLO` | `session_id:u64` |
| 2 | `SESSION_READY` | none |
| 3 | `SESSION_CLOSE` | `reason:u8` |
| 4 | `PATH_JOIN` | `session_id:u64, credential_id, path_id:u16, underlay:u8, nonce:16B, issued_at_unix_secs:u64, auth_tag:32B` |
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
| 18 | `SESSION_AUTH` | `session_id:u64, credential_id, nonce:16B, issued_at_unix_secs:u64, auth_tag:32B` |
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

A `credential_id` begins with a `u8` byte length and contains 1 through 64
ASCII bytes. Its first byte is a lowercase letter or digit; remaining bytes
are lowercase letters, digits, `.`, `_`, or `-`. Non-canonical text MUST be
rejected rather than normalized on wire.

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

Every transport path is encrypted and authenticated before MPP data admission.
Session and path authentication use separate versioned HMAC contexts and bind
the credential ID. Nonces and freshness windows limit replay. A receiver MUST
bound concurrent pending authentications and the complete authentication
flight duration before allocating durable session or product-flow state.
Target policy is enforced at the receiving endpoint under the authenticated
principal and is not delegated to the carrier.

Metrics and usage are accepted only for the authenticated path identity. A
peer cannot use `PATH_STATUS` to declare local health, bypass flow control,
grant itself capacity, or transfer state across a reconnect.

Implementations SHOULD limit diagnostic output because path metrics, targets,
and timing can expose network topology and traffic characteristics.
Peer-status responses follow the stricter rules in Section 4.3 and disclose no
target or endpoint value.

The encrypted QUIC candidate selector prevents a source-informed client
without an active credential from reaching the MPP frame parser or eliciting
an MPP-specific response. It does not provide passive or active
indistinguishability. SNI and certificate identity, TLS and QUIC implementation
behavior, the standard `h3` ALPN, QUIC transport parameters, HTTP/3 settings,
packet sizes, timing, and endpoint response behavior remain observable or
probeable. TCP likewise exposes its ordinary TLS endpoint behavior while
keeping its binary admission and MPP records encrypted. MPTUNNEL is an
authenticated tunnel, not a cover service, and implementations MUST NOT claim
that this presentation alone defeats source-aware traffic classification.

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
server is a primary target. Native packet-device, optimized socket, and network
integration remain platform-specific release evidence.

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
14. One datagram keeps one session/flow/datagram identity across carrier retries,
    and the receiver never executes that identity twice within retained state.
