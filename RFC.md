# MPTunnel Multipath Proxy Protocol (MPP) Version 5

## 1. Status and Conventions

This document specifies MPP version 5: its wire format, carrier profiles,
data-level semantics, and transport-neutral Core requirements.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**,
**SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **NOT RECOMMENDED**, **MAY**, and
**OPTIONAL** in this document are to be interpreted as described in BCP 14
when, and only when, they appear in all capitals.

MPP uses concepts established by MPTCP, QUIC, HTTP/3, and HTTP Datagrams, but
it is a separate protocol:

- MPP stream offsets and Data ACK ranges are not MPTCP DSS mappings or
  cumulative Data ACKs.
- An MPP TCP carrier is not an MPTCP subflow.
- An MPP QUIC carrier is one QUIC connection, not a path of a Multipath QUIC
  connection.
- MPP does not implement coupled congestion control above TCP and QUIC.
- MPP's HTTP Datagram mapping is not CONNECT-UDP.

Wire version 5 is identified by the frame header in Section 12. A peer MUST
reject every unsupported frame version. This version has no downgrade or
compatibility mode.

Product routing, DNS policy, outbound selection, balancing between separate
MPP sessions, VPN device integration, configuration, management APIs,
operator presentation, packaging, and platform adapters are outside this
protocol specification. They MUST NOT redefine the identities or ownership
rules specified here.

## 2. Scope and Non-Goals

MPP carries application byte streams and datagrams over one or more
authenticated transport connections. A carrier uses one of:

- TCP protected by TLS 1.3, with reliability, congestion control,
  retransmission, and packetization owned by the TCP stack; or
- QUIC over UDP, with packetization, congestion control, loss recovery,
  address validation, and path MTU discovery owned by QUIC.

MPP owns the data level above those transports:

- authenticated session, stream, flow, and MPP Path ID namespaces;
- a separate absolute offset space for each direction of each reliable stream;
- ordered delivery, deduplication, MPP Data ACK ranges, and shared receive
  credit;
- stable identity for original transmissions and reinjected copies;
- selection among eligible carriers;
- bounded cross-carrier reinjection and failover; and
- application demand used as a mutable scheduling objective.

MPP does not:

- replace native TCP or QUIC recovery;
- impose a second congestion window or pacer above a carrier;
- derive peer, session, carrier, or bottleneck identity from an IP address or
  port;
- merge transport state belonging to different carrier instances;
- promise exactly-once execution beyond its bounded retained datagram state;
- make optional platform telemetry a correctness dependency; or
- claim passive or active indistinguishability from arbitrary Internet
  traffic.

Mixed TCP and QUIC is one MPP session using two carrier families. It is not a
third transport protocol.

## 3. Terminology

**MPP session**
: An authenticated logical association identified by a `SessionId`. One
  session can span multiple carriers.

**Carrier**
: One authenticated TCP connection or QUIC connection attached to an MPP
  session.

**Carrier instance**
: One physical transport lifetime. A reconnect creates a new instance even
  when configuration and `PathId` are unchanged.

**MPP Path ID**
: The opaque `PathId` wire label for a carrier. It is not an address,
  interface, configuration ordinal, or lifetime identity.

**QUIC network path**
: The local-address/remote-address route used by a QUIC connection as defined
  by RFC 9000. A QUIC network path can change without creating a new MPP
  carrier instance.

**Locator**
: A source or destination IP address and port. A locator is never peer,
  session, carrier, physical-link, or bottleneck identity.

**Stream attachment**
: One reliable MPP stream's bidirectional membership on one carrier instance.
  It permits the stream control and feedback required by Sections 8.3 through
  8.6, but does not by itself grant ordinary payload authority in either
  direction.

**Attachment incarnation**
: The local stale-work fence for one stream attachment. Detach followed by
  reattachment creates a new incarnation. It is not a wire identifier.

**MPP stream offset**
: An absolute byte offset in one direction of one reliable MPP stream. The
  opposite direction has an independent offset space beginning at zero.

**MPP Data ACK**
: A `STREAM_ACK` acknowledgment in an MPP stream offset space. It is distinct
  from a TCP ACK, QUIC packet ACK, and MPTCP cumulative Data ACK.

**Original transmission**
: The first assignment of an MPP stream byte range to a carrier.

**Reinjection**
: A later transmission of the same unacknowledged MPP stream byte range on the
  same or another carrier.

**Regular usage**
: The receiver's directional preference that the peer may use the carrier for
  ordinary data. The wire value is `AVAILABLE`.

**Backup usage**
: The receiver's directional preference that the peer use the carrier only
  when no regular carrier is eligible. This is not a health value.

**Eligible carrier**
: A carrier that is locally live, permitted for the direction, within shared
  flow-control bounds, and able to accept the proposed enqueue.

**Transport evidence**
: Native TCP or QUIC observations such as ACKs, RTT, loss, ECN, bytes in
  flight, pacing, and queue state.

**MPP delivery evidence**
: Unique data-level progress established by an MPP Data ACK.

**MPP recovery interval**
: A transport-derived Core estimate used only to bound MPP repair and stale
  placement. It is not the native TCP retransmission timer or QUIC PTO. The
  exact profile is defined in Section 15.2.

**Demand**
: A mutable scheduling objective: latency, throughput, or realtime. Demand is
  not a permanent carrier or attachment role.

**TCP carrier group**
: The client-local configured bounds and configured-minimum member identities
  derived from one configured TCP endpoint within one MPP session. One session
  owner reconciles those minimum members and actual elastic reservations; each
  exact carrier actor owns its socket, wire ordering, readiness, drain, failure,
  and terminal release. Group identity and local capacity ordinals are never
  sent and cannot be reconstructed from locators. Unoccupied elastic capacity
  is not a carrier, attachment, health record, or scheduling state.

**Directional carrier validation**
: Bounded admission of one elastic TCP carrier for one sender direction. The
  sender compares candidate-free Product service with concurrent ordinary and
  candidate Product service using exact MPP Data ACK evidence.

**Directional ordinary-use authority**
: Permission for ordinary `STREAM_DATA` or `DGRAM_DATA` placement on one exact
  live TCP carrier in one direction. Configured-minimum TCP carriers receive it
  on readiness; an elastic TCP carrier receives it only through an acknowledged
  `RETAIN`. One bounded directional validation binding to one exact attachment
  incarnation and its exact original work are the only pre-authority Product
  exception. The binding may create the attachment when absent but never
  creates a second live attachment for the same carrier and stream. Authority
  is not receive credit, delivery acknowledgment, carrier health, or a
  permanent attachment role.

**Aggregate service interval**
: A closed interval `[lower, upper]` formed from bounded, complete,
  non-application-limited MPP service samples. It describes observed service,
  not physical bottleneck identity or transport congestion state.

**Candidate flight bound**
: The maximum unacknowledged unique-original Product bytes that one unproven
  TCP candidate may own at once before it reaches the Data ACK startup sample
  floor. It is the existing reliable unproven-path startup-flight bound and
  remains subject to shared receive credit, path flight, repair, reorder, and
  session-memory limits. After that floor, bounded validation placement uses
  the ordinary mature-path flight and queue model.

**Validation horizon**
: The finite cumulative unique-original target bytes reserved on one TCP
  candidate during one directional validation. It reuses the existing
  reliable cold-path measurement geometry. It is a cumulative work bound, not
  an amount that must be simultaneously resident; outstanding flight, queued
  work, repair, reorder debt, and memory MUST fit their instantaneous stream
  and session resource envelopes at every point.

## 4. Architecture and Authority

The ownership boundary is:

```text
application
    MPP reliable stream or datagram
        per-direction offsets, Data ACK, shared credit, bounded reinjection
            regular-before-backup carrier selection
                TCP controller | QUIC controller
                    network
```

### 4.1 MPP authority

MPP owns stream and datagram identity, offset assignment, Data ACK processing,
shared receive credit, carrier selection, bounded reinjection, and exact
data-level deduplication.

A carrier abstraction MUST expose observations and bounded enqueue capacity.
MPP MAY rank eligible carriers from those observations, but it MUST NOT:

- infer native packet loss solely from missing MPP progress;
- treat a TCP ACK or QUIC packet ACK as an MPP Data ACK;
- synthesize a transport congestion window;
- pace carrier packets;
- retransmit native transport packets;
- convert local health into peer usage;
- assign a fixed Active, Service, Validation, Subflow, or Repair role to a
  stream attachment; or
- branch scheduling semantics on an operating system, interface name,
  laboratory topology, or source locator.

### 4.2 Transport authority

TCP retains authority over TCP retransmission, congestion control, pacing,
send-queue behavior, and connection failure.

QUIC retains authority over packet numbers, connection IDs, path validation,
anti-amplification, congestion control, pacing, loss recovery, RTT state, ECN,
PMTU discovery, NAT rebinding, connection migration, and connection failure.
The QUIC congestion-control algorithm is an implementation choice outside the
MPP wire protocol; MPP MUST NOT require CUBIC, BBR, or another specific
controller.

MPP uses independent native congestion control on each carrier. It does not
implement the coupled congestion-control algorithm described by RFC 6356.
Installing such an algorithm above kernel TCP and QUIC would violate the
transport authority boundary.

### 4.3 Identity, locators, and roaming

The authenticated cryptographic connection and its registration define a
carrier instance. A source IP address, source port, destination address,
interface, or route MUST NOT define or replace that identity.

When QUIC reports that the same connection continues after NAT rebinding or
connection migration, MPP MUST preserve the carrier instance, its attachments,
and MPP state. QUIC alone decides whether to retain or reset native
path-dependent state in accordance with RFC 9000 and RFC 9002.

When a QUIC implementation establishes a new connection rather than migrating
an existing one, MPP MUST create a new carrier instance. A new TCP connection
always creates a new carrier instance. Either may attach to retained session
and stream state only through the authenticated protocol defined below.

A change hidden below an unchanged address tuple is handled by the native
transport's measurements and recovery. MPP MUST NOT manufacture a physical
link identity from the tuple.

An endpoint MAY choose a destination locator from a locally configured set
before carrier establishment. The set and its selection policy are not
serialized by MPP and MUST NOT define `PathId` or carrier-instance identity.
After transport establishment, the resulting TCP or QUIC connection follows
the identity and roaming rules above.

When a configured endpoint contains more than one destination port, each
initial carrier selects a port uniformly with process-local cryptographic
randomness. `port_hop_interval_ms` is an earliest maintenance interval, not a
failure detector, validation deadline, or capacity signal. Its Product default
is `300000` ms and values below `5000` ms are rejected, matching the established
minimum used by deployed QUIC port-hopping practice while avoiding
connection/socket churn. A hop selects a different configured port when one
exists; missed intervals coalesce into one action and never trigger catch-up
bursts.

An established QUIC connection MAY rebind through a fresh host-policy-protected
local socket and another destination port in that same configured set. Every
port in the set MUST reach the same authenticated service at the
already-established server IP. The endpoint MUST retain the QUIC connection,
and it MUST retain the preceding socket until traffic is observed through the
new locator. QUIC retains sole authority over connection migration, path
validation, recovery, and path-dependent transport state; `PathId`, the carrier
instance, its attachments, and all MPP state remain unchanged. This operation
adds no MPP wire field. Changing a TCP destination port requires a new TCP
connection and therefore a new carrier instance. TCP hopping follows the
planned replacement and retained-elastic rules in Section 7.2; the maintenance
interval never authorizes state transfer or aggressive retirement.

## 5. Session and Carrier Establishment

### 5.1 Transport authentication

Every carrier MUST complete transport protection before MPP data admission:

- TCP MUST complete TLS 1.3 with no early data and no negotiated ALPN.
- QUIC MUST complete its TLS handshake, negotiate exactly `h3`, and disable
  0-RTT for MPP carrier requests.

Both carrier families MUST authenticate the configured TLS server identity.
The certificate, private key, and client trust policy are independent from the
MPP application credential. An MPP credential MUST NOT derive or replace a
TLS certificate, private key, or certificate verifier.

### 5.2 MPP application authentication

All MPP authentication tags use HMAC-SHA256 keyed by the named application
credential. Integers use network byte order. Every tag is the complete
32-byte HMAC output.

QUIC carrier admission sends the following MPP frames, in order, on the first
selector-accepted HTTP/3 request stream:

1. `SESSION_HELLO`;
2. `SESSION_AUTH`;
3. `PATH_JOIN`; and
4. sequence-zero `PATH_STATUS`.

TCP carrier admission sends, in order:

1. the fixed TCP admission prelude in Section 6.1;
2. `PATH_JOIN`; and
3. sequence-zero `PATH_STATUS`.

The TCP prelude supplies the session authentication represented by
`SESSION_HELLO` and `SESSION_AUTH` on QUIC. A TCP carrier MUST NOT send those
two frames before `PATH_JOIN`.

The `SESSION_AUTH` transcript is:

```text
"mptunnel session auth v5" ||
session_id:u64 ||
credential_id_length:u8 || credential_id:bytes ||
nonce:16B ||
issued_at_unix_secs:u64
```

The TCP prelude transcript is:

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

The common `PATH_JOIN` transcript is:

```text
"mptunnel path join v5" ||
session_id:u64 ||
credential_id_length:u8 || credential_id:bytes ||
path_id:u16 || underlay:u8 || purpose:u8 ||
nonce:16B ||
issued_at_unix_secs:u64
```

The receiver MUST:

- resolve the canonical credential ID;
- reject an unknown, revoked, expired, or unauthorized credential;
- validate timestamp freshness, session identity, credential identity,
  `PathId`, underlay, purpose, nonce, and tag;
- reject replayed authentication and path-join nonces within the configured
  freshness boundary;
- require `PATH_JOIN` identities to equal the carrier-authenticated
  identities; and
- avoid disclosing which unauthenticated check failed.

TCP additionally validates the fixed prelude fields, canonical zero padding,
and exporter-bound tag. Credential lookup and policy evaluation occur at
authentication time; per-frame and per-byte processing MUST use the immutable
permit established by that decision.

All carriers attached to one session MUST resolve to the same principal.
Credential rotation MAY use multiple credential IDs for that principal. A
credential ID MUST NOT be reassigned to another principal or key during one
process lifetime.

Replay admission MUST be atomic with replay-state insertion. Live replay state
MUST NOT be evicted merely to accept another authentication. Process restart
is the persistence boundary unless an implementation explicitly provides
durable replay state.

### 5.3 Readiness and identity fences

After accepting carrier authentication, `PATH_JOIN`, and sequence-zero
`PATH_STATUS`, the receiver sends `SESSION_READY` and its own sequence-zero
`PATH_STATUS`. `PATH_JOIN` purpose is `ORDINARY` or `VALIDATION`; QUIC requires
`ORDINARY`. Product stream or datagram work MUST NOT be admitted until the
initiator has received both readiness frames. A TCP `VALIDATION` carrier
remains ineligible for ordinary Product attachment and placement after
readiness until the exact directional `RETAIN` is acknowledged under
Section 7.2. Its sole pre-authority exception is the exact directional
validation binding and finite unique-original work defined by Sections 7.2
and 15.1.

Within a session, the wire label is `(underlay, path_id)`. The initiator
selects `path_id`; the receiver treats it as opaque. The same numeric
`path_id` MAY label one TCP carrier and one QUIC carrier.

The client session allocates TCP `PathId` values across the complete MPP
session, not independently per configured endpoint. It MUST NOT assign a value
held by a locally nonterminal TCP carrier. The receiver MUST reject a second
current TCP carrier with the same `(SessionId, TCP, PathId)` without replacing
or mutating the first. A reconnect may reuse a label after the client's old
instance reaches native failure, but the peer may still reject that admission
until it observes the old terminal boundary. Such a rejected connection never
becomes a carrier and transfers no state; at most one replacement attempt is
active for that configured member. Reuse never transfers attachment, authority,
evidence, queue, or flight state.

Local listener policy MUST NOT index local configuration with a peer-supplied
`PathId`. Local policy remains off wire except for the directional
regular/backup preference.

Every carrier-scoped operation MUST be fenced by the carrier instance.
Every stream operation additionally MUST be fenced by its attachment
incarnation. Delayed work from an older carrier or attachment MUST NOT mutate
newer state that reused the same wire labels.

## 6. Carrier Profiles

### 6.1 TCP over TLS 1.3

TCP uses TLS 1.3, negotiates no ALPN, and accepts no early data. After the
handshake, the initiator sends exactly one 131-byte admission prelude before
any MPP frame:

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
canonical credential ID. Every remaining byte in the slot MUST be zero.

Both endpoints export exactly 32 bytes from the completed TLS connection using
the label `EXPORTER-mptunnel-tcp-admission-v1` and no exporter context. Those
bytes are `tls_exporter` in Section 5.2. An early exporter MUST NOT be used.

The prelude is carrier-admission data, not an MPP frame, and does not begin
with `MPTF`. It is followed immediately by `PATH_JOIN` and sequence-zero
`PATH_STATUS`. TLS record boundaries and write batching do not change this
ordering.

The listener reads one complete prelude before interpreting its fields. An
incomplete or rejected prelude, TLS failure, admission timeout, or read failure
MUST close without application response bytes or an MPP-specific close reason.
After successful authentication, ordinary MPP protocol errors follow
Section 13.

One TCP carrier instance multiplexes path control, stream attachments, and
datagram-flow attachments. `PING` and `PONG` may provide MPP-level heartbeat.
`PATH_DRAIN` and `PATH_CLOSE` request graceful retirement and are valid only on
TCP carriers. Their `path_id` MUST match the TCP carrier carrying the frame.
After drain begins, both endpoints MUST make that carrier ineligible for new
attachments and original placement while retaining receive, Data ACK,
recovery, and ordered-control processing.

Initiating or receiving `PATH_DRAIN` terminally cancels every unsettled
validation on that carrier and stops new validation placement. A crossing
result or acknowledgment has no authority and is discarded with the canceled
validation. Existing bounded Product flight is still delivered or recovered,
and its directional validation binding is cleared. An attachment created only
for that validation is detached through the ordinary attachment lifecycle
before the zero-work condition below can hold. Cancellation is carrier-actor
state and creates no tombstone or separate waiter lifecycle.

The drain responder sends `PATH_CLOSE` only after every earlier frame from the
initiator has been applied and the exact carrier has no attachment, datagram
binding, queued or retained frame, original or reinjected flight, pending Data
ACK, path proof, capacity work, carrier validation, or queued or unprocessed
carrier-demand frame. An applied session-level demand request does not belong
to the carrier that conveyed it and does not pin that carrier. All responder
frames that complete carrier-owned work MUST precede
`PATH_CLOSE` in the TCP byte stream. The initiator treats receipt of
`PATH_CLOSE`, not its own write completion or local emptiness, as the aggregate
retirement acknowledgment. It removes the carrier only after applying every
preceding responder frame and reaching the same local zero-work condition.
Native failure before that boundary uses ordinary retained-state recovery.

`TCP_CARRIER_DEMAND`, `TCP_CARRIER_VALIDATE`, `TCP_CARRIER_RESULT`, and
`TCP_CARRIER_RESULT_ACK` are also valid only on ready TCP carriers. They
coordinate bounded directional carrier validation in Sections 7.2 and 15.1;
they never grant stream credit or delivery.

`PATH_CAPACITY_DATA`, `PATH_CAPACITY_FINISH`, and
`PATH_CAPACITY_RECEIPT` are valid only on TCP carriers.

### 6.2 QUIC over HTTP/3

QUIC MUST negotiate the standard `h3` ALPN. Each MPP control, stream, or flow
channel uses an ordinary full-duplex HTTP/3 request stream whose encrypted
request field section contains:

```text
:method = POST
:scheme = https
:authority = configured TLS server identity
:path = /
content-type = application/octet-stream
mpp-datagram = ?1
authorization = Bearer <64 lowercase hexadecimal digits>
```

`:authority` MUST equal the TLS server identity sent as SNI. Because TLS omits
SNI for an IP-address identity, this carrier profile requires a DNS TLS server
identity; the network endpoint MAY remain a literal IP address. The path is
exactly `/` with no query.

The encrypted `mpp-datagram: ?1` field opts the request into the MPP use of
HTTP Datagrams. It is not an ALPN, HTTP/3 setting, registered upgrade token,
or pre-authentication cleartext marker. MPP does not use Extended CONNECT,
CONNECT-UDP, WebTransport, Capsule Protocol, the `:protocol` pseudo-header, or
`capsule-protocol`.

The candidate selector is:

```text
HMAC-SHA256(
  credential_secret,
  "mptunnel quic candidate selector v1" ||
  credential_id_length:u64 ||
  credential_id:bytes
)
```

`credential_id_length` uses network byte order. The output is exactly 64
lowercase hexadecimal digits following the exact `Bearer ` prefix. A request
MUST contain exactly one selector. The server MUST reject a missing,
duplicate, malformed, noncanonical, expired, revoked, or unmatched selector
before exposing request DATA to the MPP parser. Comparison MUST be
constant-time across the bounded active credential set.

The first accepted selector is bound to the QUIC connection. Later requests
on that connection MUST use the same selector. The selector proves candidate
credential knowledge only; it is not channel binding, freshness proof,
session authorization, path authorization, or replay admission. All Section
5 authentication remains mandatory.

The first accepted request is the carrier control stream after it completes
the Section 5 admission flight. Later accepted request streams carry reliable
stream attachments, datagram flows, or a bounded one-shot `PING`/`PONG` path
proof and do not repeat connection admission.

The server MUST NOT send a successful response before application
authentication, `PATH_JOIN`, replay admission, and sequence-zero
`PATH_STATUS` succeed. It then sends a 2xx response before response DATA
containing `SESSION_READY` and its sequence-zero `PATH_STATUS`. That 2xx
response also accepts the request's MPP HTTP Datagram extension semantics.

A nonmatching request, rejected selector, or failed MPP authentication receives
the same marker-free `404 Not Found` response as an unknown resource. It MUST
NOT receive an MPP-specific status, response field, body, or close reason.
Failures before an HTTP/3 request exists use ordinary TLS, QUIC, or HTTP/3
behavior.

MPP frames carried in HTTP/3 DATA are each prefixed by their encoded length as
an unsigned 32-bit network-order integer. HTTP/3 DATA boundaries are
independent of MPP record boundaries. A receiver MUST enforce its frame limit
before buffering a declared record.

QUIC native liveness and connection retirement remain transport-owned.
`PING` and `PONG` on a QUIC request stream may prove MPP response-direction
reachability, but they do not govern QUIC connection liveness or retirement.
`PATH_DRAIN`, `PATH_CLOSE`, all `PATH_CAPACITY_*`, and all `TCP_CARRIER_*`
frames are invalid on a QUIC carrier.

### 6.3 HTTP Datagrams

Both peers MUST have sent and received `SETTINGS_H3_DATAGRAM = 1` before
sending an MPP HTTP Datagram. The associated request stream's send side MUST
remain open while datagrams are sent.

Each datagram begins with the RFC 9297 Quarter Stream ID of its associated
client-initiated request stream, encoded as a QUIC variable-length integer.
The remaining payload is:

```text
0       extension version       u8; 1
1..9    flow_id                 u64
9..17   datagram_id             u64
17..21  remaining_ttl_ms        u32; nonzero
21..23  fragment_index          u16; zero based
23..25  fragment_count          u16; 1 through 64
25..29  total_payload_length    u32
29..    fragment payload
```

Multibyte envelope fields use network byte order. The envelope is 29 bytes per
fragment. Including the Quarter Stream ID, application overhead is normally
30 bytes and at most 37 bytes; QUIC, UDP, and IP overhead are additional.

The sender fragments against the current maximum QUIC datagram size and MUST
reject a payload requiring more than 64 fragments. A zero-length UDP datagram
uses one empty fragment with index zero and total length zero.

`OPEN_DGRAM_FLOW` MUST be submitted on reliable DATA before native datagrams
for that flow. Because native datagrams can overtake HEADERS or DATA, a
receiver MAY buffer the first flight for no longer than the smaller of its
remaining TTL and the handoff interval in Section 15.3. The buffer MUST be
bounded by routes, packets, bytes, flow IDs, and reassemblies. Fragment expiry
starts at original packet receipt and releases every resource charge.

A malformed Quarter Stream ID is handled as required by RFC 9297. Datagrams
received after the associated request stream is closed are silently dropped.
Datagrams for an unknown request association MAY be dropped or briefly
buffered within the preceding bound.

Closing a flow releases its live-flow charge. A flow ID already used on one
request stream MUST NOT reopen on that request stream; later monotonically
allocated IDs remain valid for its lifetime.

## 7. Carrier Lifecycle and Directional Usage

### 7.1 Carrier lifecycle

Carrier establishment, readiness, drain, close, native transport failure, and
local policy changes are distinct events. Local states such as active,
suspect, draining, failed, disabled, and cooldown are not peer scheduling
values.

Runtime-disable is client-local group admission control with a carrier-wide
wire consequence. It suspends the configured minimum, forbids new
establishment and validation, makes every group carrier locally ineligible for
new original placement, and requests ordered retirement of every exact carrier
in the group. Ordered `PATH_DRAIN` makes the peer stop new placement in its
direction as well, cancels unsettled validation under Section 6.1, preserves
ordinary delivery and recovery ownership for bounded existing flight, and
ends at the exact `PATH_CLOSE` boundary. Disable therefore does not pretend
that client-local policy can silently revoke server-to-client authority while
keeping the carrier usable.

Re-enabling creates a new establishment-policy generation and reconciles fresh
configured-minimum carrier instances. It does not cancel or reuse a drain
already begun, and no attachment, authority, queue, flight, or evidence
transfers from a disabled instance. An in-progress pre-readiness connection
from an older policy generation cannot publish afterward.

Removing a TCP carrier group requests that each exact carrier actor retire
through the ordered `PATH_DRAIN`/`PATH_CLOSE` procedure. Re-adding it creates
new group and policy generations and does not cancel a drain already begun.
Disable, removal, and re-add use the client-local group identity and MUST NOT
use a source address, locator, interface, `PathId`, or peer `PATH_STATUS` as
group identity.

A bound change MUST NOT retroactively reclassify a live carrier. Increasing the
minimum creates fresh minimum-member identities; decreasing it gracefully
retires selected surplus minimum members. Decreasing the maximum below occupied
physical reservations, or increasing the minimum while no reservation remains
for the new member, remains unapplied until ordered retirement makes the bound
reachable. No live carrier is hidden or force-closed merely to make
configuration state appear applied.

Product FIN, detach, reset, or `DGRAM_CLOSE` retires only the corresponding
product state. It does not implicitly retire a carrier.

On TCP, `PATH_DRAIN(path_id)` requests graceful retirement and
`PATH_CLOSE(path_id, reason)` completes it. On QUIC, native connection
lifecycle performs carrier retirement.

Sending or accepting `SESSION_CLOSE` retires the complete MPP session
identified by the carrying carrier. It MUST NOT be used for ordinary carrier
drain, replacement, or failure.

### 7.2 Bounded TCP carrier establishment

A client MAY configure an inclusive minimum and maximum for one TCP carrier
group. The minimum is durable ready capacity; capacity above it is elastic.
Both bounds MUST be positive, the minimum MUST NOT exceed the maximum, and
every establishing, ready, validating, retained, and draining carrier MUST fit
the endpoint and session resource envelopes. The Product default is `1-3`;
explicit bounds may differ within those envelopes.

One physical carrier consumes one group reservation and one `PathId` from
connect initiation. A pre-readiness admission, connection, authentication, or
policy-generation failure releases both. After readiness, only receipt of the
exact ordered `PATH_CLOSE` or exact native failure releases them. Occupied
reservations MUST NOT exceed the configured maximum. Unoccupied elastic
capacity consumes no carrier, `PathId`, actor, attachment, queue, evidence, or
health state.

While the group and session are enabled, one client session owner reconciles
the configured-minimum member identities with bounded connection attempts. A
minimum member sends `PATH_JOIN` purpose `ORDINARY` and gains both-direction
authority after authenticated readiness; it requires no performance
validation. An elastic reservation sends purpose `VALIDATION` and is never
promoted into a minimum-member identity. Distinct missing minimum members MAY
establish concurrently, but one member has at most one establishment actor.

The member identity names durable configured capacity, not a physical
connection. It normally has one current carrier instance. During a planned
make-before-break replacement it may additionally have one provisional
successor or one retiring predecessor, and both exact instances consume
reservations. Readiness atomically makes the successor current before the
predecessor begins ordered retirement. No queue, attachment, flight, evidence,
or authority is transferred between those exact instances.

Exact failure removes only the failed instance. Loss of a minimum member
authorizes one replacement for that same member; loss of a retained elastic
carrier does not. A stream open, close, cancellation, or waiter exit cannot own
minimum reconciliation or classify group capacity. The peer observes the
authenticated `PATH_JOIN` purpose but does not reconstruct client-local group
identity, member identity, or bounds.

Every additional TCP connection:

- is a new carrier instance with a fresh TLS connection, TCP prelude,
  `PATH_JOIN`, sequence-zero `PATH_STATUS`, readiness exchange, and evidence;
- uses the same `SessionId` and resolves to the same principal when it joins an
  existing MPP session;
- uses a `PathId` not concurrently used by another TCP carrier in that
  session; and
- retains independent attachments, queues, flight, transport evidence, and
  failure scope even when its configured endpoint and locator set are shared.

Exact carrier failure permits immediate replacement up to the configured
minimum without first proving aggregate benefit. It MUST NOT open every
remaining elastic slot as one failure reaction. Non-failure expansion above
the minimum requires Section 15.1 validation. At most one active directional
validation may exist in an MPP session. One saturation generation may examine
at most one exact candidate from each currently eligible client-local TCP
carrier group, strictly sequentially. Selecting an existing retained carrier
for its unauthorized direction, making a physical reservation, or starting a
connection attempt consumes that group's opportunity in the generation even if
validation does not begin. The client does not start another elastic
reservation while an unretained candidate is active; after its terminal
negative result it waits for that candidate's `PATH_CLOSE` or exact native
failure before selecting an untried group. A candidate already retained in the
opposite direction remains live after negative settlement. For
client-to-server validation, exact acknowledgment receipt is the settlement
barrier before another group; for server-to-client validation, that negative
result ends the sequence as specified below.

An acknowledged `RETAIN` ends that group-selection sequence because ordinary
service membership changed. A client-observed establishment or native failure
before validation admission MAY advance to an untried group. An acknowledged
`NO_GAIN` MAY also advance only after a cross-endpoint settlement barrier:
receipt of the exact result acknowledgment for client-to-server validation, or
receipt of `PATH_CLOSE` for an unretained server-to-client candidate. A
server-to-client `NO_GAIN` on a carrier already retained in the opposite
direction ends the sequence because acknowledgment serialization at the client
does not prove server receipt across a different connection. Every advance
also requires the exact saturation, demand, and sender generations to remain
current. Every serialized `WITHDRAWN` ends the sequence; the wire result does
not encode an unverifiable local-versus-sender failure classification.
After one acknowledged `RETAIN`, the same carrier MAY later validate the other
direction or a fresh saturation generation MAY reserve another candidate,
subject to the same session-wide validation and reservation bounds.

Active validation is the Product measurement from admission of
`TCP_CARRIER_VALIDATE` through result serialization. Waiting for the exact
result acknowledgment is bounded settlement state on that carrier, not a
second measurement or a globally synchronized active phase. One candidate is
measured or awaiting settlement at a time. Settlement does not release an
unretained candidate reservation or grant authority before the exact
acknowledgment.

An elastic candidate begins with no ordinary-use authority. An acknowledged
`RETAIN` grants authority only for the measured sender direction and exact
carrier instance. Authority never transfers to a replacement connection.

Attachment membership and ordinary-use authority are independent. On TCP,
every ordinary `STREAM_DATA` and `DGRAM_DATA` enqueue MUST revalidate authority
for its sender direction. `STREAM_ACK`, `STREAM_MAX_DATA`, `STREAM_FIN`,
`STREAM_RESET`, `STREAM_DETACH`, datagram feedback and close, and carrier
lifecycle control MAY use a ready live attachment as their existing semantics
require; doing so creates no ordinary-use authority in the opposite direction.

On TCP, an `OPEN_STREAM` that creates a new stream or `OPEN_DGRAM_FLOW` that
creates a new flow MUST use a carrier with ordinary-use authority in both
directions. A carrier authorized in only one direction may subsequently attach
that existing stream or flow and carry ordinary payload only in its authorized
direction. After `TCP_CARRIER_VALIDATE`, a validation-purpose carrier MAY
attach only the exact existing target stream named by that validation. The
validation binds the measured direction to that exact attachment incarnation,
creating the attachment if absent or using its one existing attachment. Only
the session-owned controller may reserve bounded unique-original work through
that directional binding.
Ordinary scheduling, new stream or flow creation, datagrams, reinjection, and
the opposite direction remain prohibited before their respective acknowledged
authority.

The client owns physical TCP establishment because only it knows the
configured carrier group and its bounds. The sender owns demand and delivery
evidence for its direction:

- client-to-server validation is driven by the client's aggregate sender
  state and exact request Data ACK release; and
- server-to-client validation is driven by the server's aggregate sender
  state and exact response Data ACK release.

Each sender direction owns one session-scoped validation controller and
bounded aggregate-service history. The named throughput stream supplies only
the saturation authorization and validation work. Only the session controller
may publish a verdict. It counts fully processed, unambiguous unique-original
Data ACK release from the target and every Product stream in that direction.
Candidate attribution additionally requires the exact directional validation
binding, attachment incarnation, and original-flight owner. Native TCP ACKs,
duplicated or reinjected bytes, capacity receipts, and peer metrics contribute
no Product numerator.

The controller maintains scalar generations instead of a reconstructed path
or interface identity:

- a saturation generation is created only by the successful-placement-to-
  ordinary-saturation edge in Section 15.1 and owns one bounded, sequential
  group-selection sequence as defined above;
- the target demand generation changes when the target opens, closes, changes
  demand class, or changes ordinary attachment incarnation;
- the ordinary-service generation changes when ordinary carrier membership,
  directional authority, usage class, or liveness changes;
- the workload generation changes when any other Product sender begins or ends
  original work, opens or closes, or changes demand class; and
- the sender-policy generation changes when sender-local Core policy or a
  sender-local resource envelope changes.

The saturation token records the target and demand generation before
establishment. After candidate readiness and `TCP_CARRIER_VALIDATE`, the
sender revalidates that token and then freezes the ordinary-service and
workload and sender-policy generations for measurement. Candidate
establishment and its directional validation binding do not change those
generations because they are not ordinary membership or a new Product
workload. A frozen generation changing makes the validation `WITHDRAWN`; the
transient blocked state itself is not frozen.
No source address, interface, locator, peer metric, or ACK-silence timer
substitutes for these local events.

For server-to-client demand, the server sends
`TCP_CARRIER_DEMAND(request_id, stream_id)` on any ready TCP carrier in the
session. No locator or client-local group identity is encoded. Request IDs are
nonzero and strictly increase in one server-owned session sequence; after
`u64::MAX`, the server sends no further demand in that session and the client
needs only the current request and one high-water scalar.
`stream_id = none` withdraws the current request; a present stream supersedes
every older request, names the one current response-demand target, and binds
the server's current saturation generation. That request remains current while
the client sequentially examines eligible local groups. Each exact candidate
uses a fresh validation ID on its own carrier. The server admits at most one
candidate at a time and at most its configured session `max_paths` validation
attempts under one request; the client admits at most one attempt per eligible
local group. After any `NO_GAIN` under the current request, a newer present
request is permitted only by the rearm conditions in Section 15.1; remaining
untried groups continue under the current request. Thus the request ID orders a
new sender-observed service generation without revealing a group.
Because different TCP carriers may fail or reorder session-level delivery, an
exact duplicate of the current request is idempotent, an older request is
ignored, and reuse of the current ID with different presence or stream content
is a protocol violation.

The client MAY ignore the request and MUST independently check the current
request ID, stream and session liveness, configured bounds, resources, and the
one-candidate limit before establishing a candidate. It fences the exact local
group reservation and policy when sending `TCP_CARRIER_VALIDATE`. For
client-to-server validation it revalidates that fence when serializing the
result and when accepting its exact acknowledgment. For server-to-client
validation it revalidates the fence when accepting the result and atomically
with acknowledgment serialization. The server independently revalidates only
its sender-local generations when validation arrives. Neither endpoint
reconstructs the other's configuration. A request grants no carrier,
attachment, placement, receive credit, or byte budget. Repetition or
supersession of peer requests cannot bypass the client's group-attempt,
connection-rate, occupied-reservation, or session resource bounds.

After validation-purpose readiness and before creating or binding the target
attachment, the client sends
`TCP_CARRIER_VALIDATE(validation_id, request_id, direction, stream_id)` on
the candidate carrier. The authenticated carrying connection is the candidate;
no locator, `PathId`, group description, generation, or carrier nonce is
repeated. Client-issued `validation_id` values are nonzero and strictly
increase across both directions of that exact candidate; after `u64::MAX`, the
client starts no further validation on the instance. The receiver retains only
the active record and one high-water scalar. Client-to-server validation
requires `request_id = 0`.
Server-to-client validation requires the current nonzero demand request and
its exact stream. Every other direction and request combination is a
candidate-carrier protocol violation.

Malformed, noncanonical, duplicate, or unauthorized references are protocol
violations on the candidate. A well-formed reference that became stale
because the exact carrier, saturation token, target stream, demand request, or
frozen generation changed races harmlessly to `WITHDRAWN`; it is not a peer
fault. Candidate Product placement additionally requires peer usage
`AVAILABLE` for that direction. `BACKUP` preference is never bypassed to
obtain a favorable result.

The client starts one establishment lease with the physical reservation,
bounded by `[session].retention_timeout_ms`. Connection establishment has the
earlier `path_probe_timeout_ms` deadline. Establishment-lease expiry initiates
ordered candidate retirement in the exact carrier-actor ordering domain and
therefore cancels any unsettled validation under Section 6.1. The lease also
ends at the client's first local `RETAIN` commitment—receipt of the exact
acknowledgment for client-to-server validation or serialization of the exact
acknowledgment for server-to-client validation—or at terminal candidate
retirement. It is not a retained-carrier lifetime. Admission of each
`TCP_CARRIER_VALIDATE` creates independent sender-local and receiver-local
validation leases, each bounded by the local session retention limit. A later
opposite-direction validation uses fresh local leases.

The directional sender additionally owns the transport- and work-derived
Product-phase lease in Section 15.1. Its expiry produces `WITHDRAWN` only while
the sender-local validation lease still admits result serialization. The
receiver independently acknowledges a result only while its own local lease
and exact validation state remain current. Expiry of either endpoint's local
validation lease instead initiates ordered `PATH_DRAIN`; crossing result frames
are discarded under Section 6.1. No lease restarts or extends on progress,
ACKs, demand updates, or waiter cancellation, and no endpoint assumes that its
local deadline is known by its peer. A deadline is only a resource ceiling,
not delivery or performance evidence.

The receiver discharges its validation lease after serializing the exact
acknowledgment; the sender discharges its lease after receiving that
acknowledgment. Ordered drain, native failure, or session close terminally
discharges both local records.

The sender reports one result with
`TCP_CARRIER_RESULT(validation_id, direction, result)` on the candidate
carrier. `RETAIN` means the sender established the target-flow and session
service conditions in Section 15.1. `NO_GAIN` means a complete validation did
not establish both. `WITHDRAWN` means the validation became invalid,
inconclusive, or expired and makes no capacity claim. Only the sender may emit
a result, and serialization makes that result immutable: later demand or
generation changes do not create a second result or roll it back.

The receiver accepts only an exact current result. If its candidate, direction,
validation lease, local admission policy, and bounded resource state permit the
result, it atomically applies the local result and serializes
`TCP_CARRIER_RESULT_ACK(validation_id, direction, result)` on the same
candidate. The acknowledgment repeats all result fields exactly. Unknown,
conflicting, duplicate, or noncanonical results and acknowledgments are
candidate-carrier protocol errors. A canonical exact result or acknowledgment
crossing a local policy change that requires candidate retirement, ordered
drain, local-lease retirement, or native failure is discarded with that
carrier; a local race does not turn the frame into a peer fault. When the
receiver is the client, its local admission policy includes the exact
group-policy fence from validation. When the sender is the client, the same
fence is applied at result serialization and acknowledgment acceptance.
Admission of `TCP_CARRIER_VALIDATE` reserves the receiver's bounded
result-and-acknowledgment state through its local validation lease. Ordered
drain cancels that state as specified in Section 6.1.

For `RETAIN`, acknowledgment serialization commits the receiver's directional
authority and converts the exact directional validation binding to ordinary
authority on that attachment; acknowledgment receipt commits the sender's
matching transition.
The sender MUST NOT place work beyond the finite validation horizon or admit
ordinary Product data on the candidate before receiving the exact
acknowledgment. This same rule applies in both directions; no clock
synchronization, cross-connection ordering, provisional rollback, or
post-result withdrawal exists. A sender that does not receive its exact
acknowledgment before candidate retirement obtains no ordinary authority.

For `NO_GAIN` or `WITHDRAWN`, acknowledgment settles the validation without
granting authority. New candidate placement is already stopped. The exact
directional validation binding first resolves its bounded original flight
through ordinary Data ACK or recovery, then clears. An attachment with no
remaining authority or work detaches through its ordinary lifecycle. If no
direction is already retained, the client sends `PATH_DRAIN` only after that
zero-work boundary and holds the reservation until matching `PATH_CLOSE` or
exact native failure.
Result, acknowledgment, authority transition, detachment, and any following
drain share the exact carrier-actor ordering domain. Caller cancellation cannot
omit or overtake that suffix.

Acknowledged authority lasts only for the exact live instance and direction.
Carrier drain, failure, or session close revokes it. Ending the measured demand
does not. The client retains a carrier while either direction remains
authorized; the other direction remains ineligible until separately validated.
A retained elastic carrier never satisfies or replaces a configured-minimum
member.

Before its first acknowledged `RETAIN`, a candidate carries only readiness,
bounded path proof, validation control, one exact target attachment with one
directional validation binding, its bounded unique-original target work and
Data ACK feedback, heartbeat, status, and lifecycle control. Invalidated,
withdrawn, or no-gain validation stops new candidate placement immediately;
existing flight remains governed by normal MPP delivery and recovery ownership.

Retention is not permanent resource pinning. A retained elastic carrier becomes
eligible for contraction after one continuous configured session retention
interval with no queued or in-flight carrier-exclusive Product work, active
validation, or unsettled result. Maintenance control does not reset that
interval.
Attachment existence alone does not pin the carrier: at contraction the client
first stops new placement, evacuates or detaches attachments through their
normal ordered lifecycle, then sends `PATH_DRAIN` after reaching the local
zero-work boundary. Another usable carrier must remain for every affected
direction. New Product work before drain resets the interval; demand after
drain begins uses another carrier and does not reverse that exact retirement.

Changing the destination port of a TCP carrier creates a replacement carrier;
it never migrates the existing TCP connection. Planned replacement MUST be
make-before-break for a configured-minimum member when the resource envelopes
have spare capacity: authenticate and ready the replacement, admit streams
that must continue through their normal attachment procedure, stop new
attachments and original placement on the old carrier, send `PATH_DRAIN` after
its preceding writer work, and wait for the peer's `PATH_CLOSE`. The
replacement has ordinary authority after readiness under Section 7.2. When no
spare capacity exists, replacement waits for capacity or exact quiescence
rather than forcing an active carrier closed.

The expansion verdict in Section 15.1 proves old-plus-candidate aggregate gain,
not replacement equivalence. A retained elastic carrier is therefore never
proactively replaced from that verdict. Port hopping for it takes effect after
normal contraction or exact failure, when a later connection independently
selects a port. No native transport state, carrier-attributed delivery
evidence, flight, queue, attachment, validation evidence, or ordinary-use
authority transfers to any replacement. Session-owned stream offsets, retained
ranges, and MPP Data ACK state survive and may use the replacement only through
ordinary authenticated reattachment.

### 7.3 Directional usage

`PATH_STATUS` contains:

```text
path_id   : u16
sequence  : u64
usage     : AVAILABLE(0) | BACKUP(1)
```

The receiver advertises how the peer should use the carrier for data sent
toward that receiver. The two directions are independent.

The sequence space belongs to one carrier instance and begins at zero. After
admission, an endpoint accepts only a sequence strictly greater than the last
accepted value for that instance. A stale or duplicate value MUST NOT change
scheduling state. A new authenticated carrier instance starts a new sequence
space.

For ordinary data, an endpoint MUST:

1. remove locally ineligible carriers;
2. form the regular (`AVAILABLE`) eligible set;
3. use the backup set only when the regular set is empty; and
4. rank carriers within the selected set from current evidence and demand.

Backup preference MUST NOT be represented as an arbitrary additive timing
penalty. Local policy MAY further reserve a carrier as backup.

Usage, local health, authentication, liveness, proof, congestion state, and
demand are independent facts.

## 8. Reliable Streams

### 8.1 Open and attachment

`OPEN_STREAM(stream_id, target, demand)` has no carrier role. The first
accepted open creates the stream. A later open with the same `StreamId` adds an
attachment only when the target and initial demand hint exactly match the
original values.

The wire demand value is an immutable admission hint. A sender's live
throughput, latency, or realtime objective may change from local Product and
queue state without a wire update. That sender-local state controls its
direction only and cannot overwrite the peer's objective or the initial
attachment-consistency value.

One live carrier instance may have at most one live output attachment for a
given stream. Replacing a closed output creates a new attachment incarnation.
No flight, proof, rate, feedback, queue, or load state from the old incarnation
may be inherited merely because `StreamId` and `PathId` are unchanged.

Each sender begins without implicit MPP credit and waits for
`STREAM_MAX_DATA` or `STREAM_RESET` from that direction's receiver.

### 8.2 Offset mapping and delivery

`STREAM_DATA(stream_id, offset, payload)` maps its bytes to:

```text
[offset, offset + payload.length)
```

Client-to-server and server-to-client directions maintain independent offsets,
retained ranges, acknowledgments, and receive limits. Equal numeric offsets in
opposite directions do not identify the same data.

The receiver MUST:

- reject offset arithmetic overflow and configured-limit violations;
- deduplicate overlapping copies;
- buffer out-of-order bytes only within configured bounds; and
- expose bytes to the application exactly once and in offset order.

The same range may arrive over TCP, QUIC, or both without changing identity.
The sender retains unacknowledged ranges only within its configured resource
envelope.

### 8.3 MPP Data ACK

`STREAM_ACK(stream_id, complete, ranges)` carries non-empty half-open ranges in
one directional MPP stream offset space. The list MAY be empty.

When `complete` is true, the list is an authoritative snapshot of the
receiver's retained received ranges and can establish an omitted gap. When
false, the ranges report partial positive progress and omission does not imply
a gap. `complete` does not mean end of stream.

Data ACK processing MUST be one transaction:

1. validate and normalize ranges;
2. release each newly acknowledged unique byte once;
3. release every original or reinjected flight overlapping those bytes;
4. update local delivery and admission evidence without changing receive
   credit; and
5. publish carrier-specific progress only when attribution is unambiguous.

If a byte was outstanding on multiple carriers, the Data ACK proves delivery
but not which copy delivered it. No implementation may invent per-carrier
delivery evidence for that range.

### 8.4 Shared flow control

`STREAM_MAX_DATA(stream_id, max_offset)` grants the greatest offset the sender
may assign in that direction. The maximum is shared by all attachments of that
stream and direction. Adding a carrier MUST NOT multiply it.

The sender retains the greatest observed maximum; a smaller value does not
revoke credit. A new attachment MAY therefore receive a credit-neutral
`STREAM_MAX_DATA(stream_id, 0)`. Only the logical receive owner grants new
credit.

`STREAM_ACK` releases retained data and flight but grants no new offset.
`STREAM_MAX_DATA` grants offsets but acknowledges no byte. Transport enqueue
capacity and native congestion state are additional local constraints, not
alternate MPP receive windows.

### 8.5 Completion, detach, and reset

`STREAM_FIN(stream_id, final_offset)` declares the final offset and may travel
on any live attachment. A receiver rejects an offset behind received data or
one conflicting with an earlier FIN. Data MUST NOT extend past an accepted
final offset. Otherwise FIN remains pending until contiguous delivery reaches
it. Matching duplicate FINs are idempotent.

`STREAM_DETACH(stream_id)` removes only the attachment on the carrier carrying
the frame. It is not an acknowledgment of peer-side flight or carrier
quiescence. During TCP carrier drain, an endpoint MUST retain the attachment
state needed to receive preceding frames, publish or process Data ACK, and
complete recovery until the ordered `PATH_CLOSE` boundary in Section 6.1.
`STREAM_RESET(stream_id, reason)` terminates the MPP stream.

Native TCP EOF ends the carrier and is not stream FIN or detach. Native QUIC
stream FIN closes only that native byte-stream direction and MUST NOT be
interpreted as MPP FIN, detach, or completion. A native FIN inside an MPP
record is truncation; a FIN at a record boundary is a clean native half-close.

### 8.6 Attachment loss and retention

An endpoint MAY retain a stream for one configured absolute interval while it
has no live attachment. It preserves offsets, Data ACK, receive credit, FIN,
retained transmission, and reorder state. It MUST stop accepting new
application bytes when doing so would exceed the MPP resource envelope.

A newly authenticated carrier may attach to that retained stream. Attempts to
restore attachment MUST NOT extend the original no-attachment deadline.
Expiry retires the stream and its application connection. Ordinary application
idle on a live attachment is not attachment loss.

Loss of the last carrier is not `SESSION_CLOSE`. While the MPP session or any
retained stream or datagram state remains within its original configured
absolute retention lifetime, the client session service may establish
configured-minimum replacements with the same `SessionId` and fresh carrier
instances. Reattachment uses ordinary authenticated admission and attachment;
no authority, attachment, transport evidence, queue, or flight transfers from
a failed instance. Reconnect attempts MUST NOT extend any original retention
deadline.

### 8.7 Reinjection

Reinjection preserves `StreamId`, direction, offset, length, and payload. It
creates another flight record, never new stream offsets.

Before enqueue, the sender MUST revalidate:

- the exact carrier instance;
- the stream attachment incarnation;
- the current stream offset frontier and every placement fact on which the
  decision depends;
- shared receive credit;
- queue reservation;
- proof and evidence authority; and
- the retained range identity.

Reinjection SHOULD use a distinct eligible carrier when one exists. It MAY be
admitted for exact carrier failure, an authoritative persistent Data ACK gap,
or a bounded live-tail condition. It MUST remain within:

- shared receive credit;
- native carrier enqueue capacity;
- the MPP reorder envelope;
- retained unacknowledged ranges;
- exact carrier and attachment identity;
- repeat-delay suppression; and
- the cumulative extra-traffic envelope in Section 15.

Missing MPP progress alone does not authorize unbounded copies. Native
transport recovery continues independently.

## 9. Datagrams

### 9.1 Flow and datagram identity

`OPEN_DGRAM_FLOW(flow_id, target)` creates an MPP datagram association.
`DGRAM_DATA(flow_id, datagram_id, ttl_ms, payload)` carries one datagram.
`DGRAM_FEEDBACK(flow_id, received)` acknowledges datagram IDs admitted by the
peer. `DGRAM_CLOSE(flow_id)` closes that carrier's flow attachment.

A flow ID binds one target during its retained lifetime. Reuse with another
target is a protocol violation.

Datagram IDs are directional and monotonic within a flow. The full identity is
`(session_id, flow_id, direction, datagram_id)`. Direction is implicit in
authenticated frame travel. Equal numeric IDs in opposite directions are
distinct.

Every retry or carrier copy of one identity MUST preserve its payload. Reuse
of a retained identity with another payload is a protocol violation. A
receiver MUST forward an admitted request identity to the target at most once
within retained state.

### 9.2 TTL, feedback, and retry

A datagram is dropped when its remaining TTL cannot cover the selected carrier
estimate. Forwarding and any retry consume the original absolute TTL; a carrier
change does not restart it.

`DGRAM_FEEDBACK` reports directional MPP admission, not target execution or
end-to-end delivery. Request feedback stops request replay because target
processing may have begun. Response feedback permits cached response state to
retire.

Before feedback, an alternative-carrier attempt preserves the same identity.
A duplicate request updates its retained response route without duplicate
target forwarding. Target responses use an independent reverse-direction ID
space and need not correlate one-to-one with requests.

Multiple bounded request identities may be outstanding on one flow.
`DGRAM_CLOSE` detaches that carrier binding. Another authenticated carrier may
reattach the same session, flow, and target within the configured retention
interval.

The exact bounded retry profile is specified in Section 15.

## 10. Core Scheduling Requirements

### 10.1 Observe, decide, commit

The scheduler evaluates an immutable observation and proposes a carrier. Before
enqueue, the implementation revalidates current carrier identity, attachment
identity, stream frontier, evidence provenance, and queue reservation. It
commits data-range ownership only after enqueue succeeds.

An observation may contain:

- local carrier health and drain state;
- peer usage and local backup policy;
- RTT, RTT variation, and jitter;
- native delivery and pacing rate when available;
- MPP Data ACK progress;
- native and MPP bytes in flight;
- transport and MPP queue bytes;
- current demand; and
- evidence provenance, confidence, and freshness.

The implementation MUST discard and recompute a proposal when any revalidated
identity, frontier, provenance, or reservation is stale.

### 10.2 Evidence provenance

Transport queue/flight and MPP queue/Data-ACK flight overlap in one delivery
pipeline. A completion-time estimate MUST NOT add them as independent
outstanding byte counts. Common connection-wide backlog MUST NOT be charged
once per candidate carrier.

Per-flow evidence MUST NOT be treated as shared carrier capacity without a
session-scoped controller that freezes the target demand and scalar
ordinary-service and workload generations. The controller consumes per-stream
events but owns the aggregate conclusion. A configured rate is a startup
prior, not measurement.

A locator, interface, or route cannot establish carrier capacity, marginal
benefit, or bottleneck identity.

Native ACK evidence MAY establish transport readiness within its carrier.
Only unambiguous MPP Data ACK coverage establishes unique data-level delivery
on an additional output. Data ACK coverage of duplicated bytes MUST NOT be
attributed to either copy.

Directional aggregate delivery is observed at the sender that owns original
flight provenance. A receiver-side reordered delivery callback cannot
reconstruct whether the sender classified a range as original or reinjected
and MUST NOT substitute for that authority. One complete Data ACK transaction
is one indivisible aggregate event; an implementation MUST NOT split or
truncate it to manufacture a measurement boundary.

### 10.3 No second congestion controller

RTT, loss, ECN, jitter, queue, flight, pacing, and delivery observations MAY
affect ranking, eligibility, application record or batch size, and admission
to observed native send credit and backpressure. MPP MUST NOT use them to:

- maintain an independent loss- or ECN-driven congestion window;
- install a native-packet pacer;
- throttle below native enqueue/backpressure as a substitute congestion
  controller;
- replace native retransmission; or
- make a native controller's packet-loss decision.

The output carrying the contiguous stream frontier remains bounded by shared
MPP credit, enqueue capacity, and its native controller. An unproven additional
output is limited to a bounded startup flight until sufficient unambiguous
delivery evidence exists.

### 10.4 Bounded work and fairness

All scheduling, retention, reinjection, measurements, queues, and diagnostic
work MUST have byte, item, and time bounds. Cancellation MUST reconcile each
queue reservation, flight, measurement ticket, load lease, and registry entry
exactly once.

MPP does not claim RFC 6356 coupled fairness. Each carrier remains subject to
its native controller and the network's treatment of that independent
connection.

## 11. Measurement and Diagnostic Extensions

### 11.1 Path metrics

`PATH_METRICS` carries typed directional evidence:

```text
path_id:u16, underlay:u8, direction:u8, metric_epoch:u64,
metric_age_us:u32, srtt_us:u32, rttvar_us:u32, jitter_us:u32,
delivery_rate_bps:u64, pacing_rate_bps:u64, loss_ppm:u32, ecn_ppm:u32,
loss_observed:u8, ecn_observed:u8, bytes_in_flight:u64, queue_bytes:u64,
inflight_limit_bytes:u64, inflight_hi_bytes:u64, confidence_ppm:u32,
app_limited:u8, has_ack_derived_data_sample:u8, data_sample_count:u32,
data_sample_bytes:u64
```

Metrics are advisory and scoped to the authenticated carrier instance and
direction. They grant no stream offset, flight ownership, usage, health, or
capacity.

### 11.2 Reachability and capacity

`PATH_PROOF_DATA` and `PATH_PROOF_ACK` prove that one authenticated carrier can
exchange MPP frames. Proof identity is scoped to that carrier instance.
Initiator proof IDs are nonzero and strictly increase in local allocation
order; after `u64::MAX`, that initiator sends no further proof on the instance.
The initiator retains only its bounded unexpired pending proofs.
`PATH_PROOF_ACK` echoes the initiator's ID without consuming the responder's
own sequence. A receiver MUST NOT infer arrival order from proof IDs because
QUIC request streams can reorder. Every proof-frame `path_id` equals the
carrying carrier's authenticated `PathId`.

`PATH_CAPACITY_DATA`, `PATH_CAPACITY_FINISH`, and
`PATH_CAPACITY_RECEIPT` form a bounded TCP-only measurement transaction.
Measurement payload consumes no stream offset and produces no Data ACK credit.
Measurement IDs, bytes, timers, reservation, and cleanup MUST be scoped to the
exact carrier instance. Transaction-initiator IDs are nonzero and strictly
increase in each direction of that carrier; after `u64::MAX`, that initiator
starts no further transaction on the instance. The receiver needs only the
current transaction and one high-water scalar. `PATH_CAPACITY_RECEIPT` echoes
the initiator's ID and neither consumes nor compares against the responder's
own initiation sequence. Every capacity-frame `path_id` equals the carrying
carrier's authenticated `PathId`. One transaction may span multiple ordered
`PATH_CAPACITY_DATA` frames within the frame and cumulative measurement bounds;
`PATH_CAPACITY_FINISH` declares their exact cumulative payload and one matching
receipt confirms it. Capacity evidence calibrates an already attached TCP
output; it never establishes Product delivery or elastic-carrier retention.
Before its first acknowledged `RETAIN`, a validation-purpose carrier MUST NOT
carry capacity frames.

Optional native telemetry may refine estimates. Every field is independently
optional. Absence means unknown, never measured zero, and MUST NOT make a
carrier ineligible by itself.

### 11.3 Peer status

An authenticated endpoint MAY send `PEER_STATUS_REQUEST(request_id)`. The peer
answers on the same carrier with
`PEER_STATUS_RESPONSE(request_id, code, paths)`, where `code` is `OK`,
`DISABLED`, or `UNAVAILABLE`. A non-`OK` response contains no paths.

Each path entry contains local state, directional usage, and one
`PATH_METRICS` record. A response:

- MUST include only the authenticated requester's session;
- MUST NOT contain endpoints, targets, service labels, credentials, or local
  configuration ordinals;
- MUST be bounded by path count, frame size, request queue, outstanding count,
  timeout, and rate;
- MUST return `UNAVAILABLE` rather than a partial path set when the full set
  cannot fit; and
- MUST NOT update scheduling, health, flow control, capacity, or failover
  state.

Peer status is diagnostic presentation, not delivery evidence.

## 12. Wire Format and Registry

### 12.1 Frame header

Every MPP frame begins with:

```text
0..4   magic          ASCII "MPTF"
4      version        5
5      frame kind     u8
6..10  payload length u32, network byte order
```

All multibyte integers use network byte order. A decoder MUST reject invalid
magic, unsupported version, unknown kind, truncation, trailing bytes, invalid
enum values, invalid ranges, zero target ports, arithmetic overflow, and
configured-limit violations.

The frame version is independent of TLS records, QUIC packets, and HTTP/3 DATA
frames.

### 12.2 Frame assignments

| Kind | Frame | Payload fields |
|---:|---|---|
| 1 | `SESSION_HELLO` | `session_id:u64` |
| 2 | `SESSION_READY` | none |
| 3 | `SESSION_CLOSE` | `reason:u8` |
| 4 | `PATH_JOIN` | `session_id:u64, credential_id, path_id:u16, underlay:u8, purpose:u8, nonce:16B, issued_at_unix_secs:u64, auth_tag:32B` |
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
| 24 | `PATH_METRICS` | fixed record in Section 11.1 |
| 27 | `STREAM_FIN` | `stream_id:u64, final_offset:u64` |
| 30 | `STREAM_DETACH` | `stream_id:u64` |
| 31 | `PATH_PROOF_DATA` | `path_id:u16, proof_id:u64, length:u32, bytes` |
| 32 | `PATH_PROOF_ACK` | `path_id:u16, proof_id:u64, payload_bytes:u32` |
| 33 | `PATH_CAPACITY_DATA` | `path_id:u16, measurement_id:u64, length:u32, bytes` |
| 34 | `PATH_CAPACITY_FINISH` | `path_id:u16, measurement_id:u64, payload_bytes:u64` |
| 35 | `PATH_CAPACITY_RECEIPT` | `path_id:u16, measurement_id:u64, received_payload_bytes:u64` |
| 36 | `PEER_STATUS_REQUEST` | `request_id:u64` |
| 37 | `PEER_STATUS_RESPONSE` | `request_id:u64, code:u8, count:u16, paths[count]` |
| 38 | `TCP_CARRIER_DEMAND` | `request_id:u64, present:u8, stream_id:u64 when present=1` |
| 39 | `TCP_CARRIER_VALIDATE` | `validation_id:u64, request_id:u64, direction:u8, stream_id:u64` |
| 40 | `TCP_CARRIER_RESULT` | `validation_id:u64, direction:u8, result:u8` |
| 41 | `TCP_CARRIER_RESULT_ACK` | `validation_id:u64, direction:u8, result:u8` |

Kinds 5, 6, 15, 19, 25, 26, 28, and 29 are reserved and MUST NOT be sent.

`SESSION_HELLO` and `SESSION_AUTH` are QUIC carrier-admission frames; TCP uses
the Section 6.1 prelude. `PATH_DRAIN`, `PATH_CLOSE`, kinds 33 through 35, and
kinds 38 through 41 are TCP-only. Receiving a carrier-incompatible frame is a
protocol violation.

### 12.3 Common field encodings

Each `ranges[count]` entry is `start:u64, end:u64` and represents
`[start, end)`. `start` MUST be less than `end`.

A target begins with a type:

- domain `1`: `length:u16`, UTF-8 host bytes, nonzero `port:u16`;
- IPv4 `2`: four address bytes, nonzero `port:u16`; or
- IPv6 `3`: sixteen address bytes, nonzero `port:u16`.

A credential ID begins with a `u8` length from 1 through 64. Its first ASCII
byte is a lowercase letter or digit. Remaining bytes are lowercase letters,
digits, `.`, `_`, or `-`. Receivers reject rather than normalize noncanonical
text.

Demand values are latency `1`, throughput `2`, and realtime `3`. Underlay
values are TCP `1` and UDP `2`. Path purposes are `ORDINARY = 1` and
`VALIDATION = 2`. Every directional wire field, including metrics and TCP
carrier validation, uses client-to-server `1` and server-to-client `2`.
Boolean fields use `0` or `1`.

TCP carrier result values are `RETAIN = 1`, `NO_GAIN = 2`, and
`WITHDRAWN = 3`. A carrier demand presence value is canonical Boolean; an
absent stream has only the withdrawal meaning in Section 7.2. `request_id` in
`TCP_CARRIER_DEMAND` and
`validation_id` in `TCP_CARRIER_VALIDATE`, `TCP_CARRIER_RESULT`, and
`TCP_CARRIER_RESULT_ACK` MUST be nonzero. A zero validation `request_id` has
the local-demand meaning defined in Section 7.2.

Usage values are `AVAILABLE = 0` and `BACKUP = 1`.

Close reasons are normal `0`, protocol error `1`, authentication failed `2`,
and policy rejected `3`. Stream-reset reasons are refused `1`, timed out `2`,
remote closed `3`, and policy rejected `4`.

Peer-status states are active `0`, suspect `1`, draining `2`, and failed `3`.
Peer-status codes are OK `0`, disabled `1`, and unavailable `2`.

## 13. Limits and Error Scope

Endpoints MUST enforce configured bounds for:

- frame and payload bytes;
- acknowledgment ranges and host bytes;
- pending authentications and replay entries;
- sessions, carriers, streams, attachments, and datagram flows;
- shared receive credit and retained stream ranges;
- reorder bytes and intervals;
- carrier queues and flight;
- datagram attempts, TTL, caches, fragments, and reassemblies;
- proof, capacity, metric, and peer-status work;
- TCP carrier demand requests, validations, candidate work, results,
  acknowledgments, ordinary-use authority, and drain work;
- TCP group-capacity reservations, minimum connection and reconnect attempts,
  `PathId` allocation state, removal and bound-reduction drains, and
  carrierless-session retention; and
- all teardown and no-attachment retention.

A protocol violation closes the smallest safe scope: product flow, stream,
carrier, or session according to the corrupted state. Authentication failure
must not admit durable product state. A carrier failure invalidates only state
owned by that carrier instance; a logical stream may continue on surviving
attachments.

Failure publication MUST carry exact carrier-instance identity. A delayed
status, ACK, measurement, or teardown from an older instance MUST NOT alter
newer state.

## 14. Security and Privacy

### 14.1 Authentication and replay

Transport encryption and server authentication precede MPP admission.
Versioned HMAC contexts separate QUIC session authentication, TCP
exporter-bound admission, and common path join. Nonces and freshness windows
limit replay.

The receiver MUST bound unauthenticated parsing, concurrent admission work,
credential scans, replay state, and total admission duration. It MUST compare
authentication material without data-dependent early exit that discloses the
matching credential.

Target authorization is enforced under the authenticated principal at the
receiving endpoint. A peer-supplied target, metric, usage, or `PathId` does not
grant policy or capacity.

### 14.2 Malicious evidence and resource exhaustion

Peer metrics, usage, TCP carrier demand, capacity receipts, TCP carrier
results, and result acknowledgments are authenticated input. They MUST NOT:

- grant receive credit;
- release retained data;
- establish local delivery;
- declare local health;
- bypass queue or flight bounds; or
- transfer state to another carrier instance.

A peer can request or retain only capacity inside the client's configured TCP
carrier range and the session resource envelope. The client MUST bound request
frequency, outstanding identifiers, validation work, and
retained elastic carriers independently of peer input. A validation or result
for a stale physical instance, validation, direction, target stream, demand
generation, sender-policy generation, or client-owned group-policy generation
has no authority.

Datagram replay windows, response caches, pending native datagrams,
reassemblies, and target forwarding are bounded. Reused IDs with conflicting
payloads or targets are protocol violations.

### 14.3 Traffic classification

The QUIC candidate selector prevents a party without an active credential from
reaching the MPP frame parser or eliciting an MPP-specific response. The TCP
prelude and all MPP frames are encrypted.

These properties do not provide indistinguishability. Passive observers can
still observe IP and port locators, TLS and QUIC fingerprints, SNI and
certificate identity, the standard `h3` ALPN, QUIC transport parameters,
HTTP/3 settings, packet sizes, and timing. An active observer can probe public
TLS, QUIC, and HTTP/3 behavior; a party holding an application credential can
authenticate and identify the service.

Implementations MUST NOT advertise MPP as a cover protocol or claim that its
carrier presentation defeats a source-aware classifier. Fixed private
cleartext protocol markers are avoided, but authenticated tunneling—not
traffic impersonation—is the security objective.

## 15. MPTunnel Core Profile 5

This section specifies the transport-neutral Core policy used with the wire
semantics above. It defines Core conformance, not peer interoperability.
Resource-envelope values described as configured are local bounds and are not
wire values. The timing formulas below are local MPP policy; they are not
native TCP, QUIC, MPTCP, or HTTP/3 timers.

### 15.1 Original placement

Within the regular or backup set selected by Section 7, original data minimizes
estimated completion time subject to shared receive credit, carrier enqueue
capacity, and reorder bounds.

The output carrying the contiguous frontier is governed by shared MPP credit
and its native carrier. Before an additional response output has durable,
unambiguous Data ACK coverage for original transmissions, it may own at most
one bounded startup flight. Native TCP ACK or QUIC packet-ACK evidence alone
does not unlock mature additional-output placement.

After exact original-data Data ACK coverage reaches the configured startup
sample floor, the response scheduler may use its mature completion-time model.
Duplicated bytes do not satisfy that floor for either copy.

Non-failure TCP carrier expansion is permitted only for throughput demand with
fresh queued unique original data. Shared stream credit, assigned-offset,
repair, reorder, and session-memory bounds MUST still permit another original
service quantum; exhaustion of any shared Product bound cannot justify another
carrier. Within the first nonempty regular-or-backup authority class from
Section 7, every ordinary carrier must already own original target work and
every carrier-local data queue must fail the exact reservation. No latency or
realtime sender work may be active in that session direction; such work
starting withdraws an incomplete validation. The transition from a successful
original placement to this carrier-local saturation creates one generation.
Polling the same queue, retained flight, reinjection, native buffer occupancy,
or ACK silence does not create another.

That edge records one target stream and its demand generation and authorizes
the bounded, sequential group-selection sequence in Section 7.2, with at most
one physical reservation active at a time. It also freezes `r`, the highest
current mature target-flow Product-ACK rate among exact ordinary carriers in
the selected authority class. Each eligible rate has passed the existing
original-data Data-ACK maturity and provenance gates; configured rates, native
ACK rates, capacity measurements, and peer metrics do not qualify. Without a
positive mature Product rate, no validation begins. `r` sizes bounded work and
a deadline only and is never verdict evidence.

The candidate authenticates with `PATH_JOIN` purpose `VALIDATION`, reaches
readiness, and sends `TCP_CARRIER_VALIDATE`. The sender then revalidates the
saturation token and freezes the target, ordinary-service, workload, and
sender-policy generations from Section 7.2. It creates one directional
validation binding to the exact target attachment, creating that attachment
only when absent. When an already-retained elastic carrier validates its other
direction, the existing attachment is reused and ordinary Product work in the
authorized direction remains valid; only bounded validation placement is
admitted in the direction being measured.

Candidate work reuses the established reliable cold-path geometry without
using capacity payload as Product evidence. For MPP v5, let:

- `q` be the configured Data ACK startup sample floor;
- `I = 10 * 1460` bytes be the initial service window;
- `s = 65536` bytes be one maximum reliable service quantum;
- `T` be the candidate `max(SRTT, 1 ms)`, or `333 ms` without an observation;
- `r` be the positive mature Product rate frozen at the saturation edge, in
  bits per second; and
- `p = ceil(2 * (r / 8) * T)` bytes.

The finite per-sample comparison horizon and cumulative candidate horizon are:

```text
w = max(p, I)
C = w + s
H = q + 2 * C
```

Arithmetic is checked and `q` is positive. `q`, `w`, `C`, and `H` are
cumulative work counts and need not fit simultaneously in a sliding stream
window or memory envelope. Exactly `H` cumulative unique-original target bytes
may be assigned to the candidate during that validation. During readiness, at
most the candidate flight bound is unacknowledged at once. After readiness has
exact Data ACK coverage for `q`, the two assisted samples use the existing
mature-path placement model. At every instant, outstanding candidate flight,
queue reservations, shared credit, repair state, reorder debt, stream-window
occupancy, and session memory MUST remain inside their ordinary configured
bounds. Capacity frames, duplicated bytes, reinjection, peer metrics, and
native TCP acknowledgments contribute none of the cumulative work.

A Product service sample begins with the first qualifying unique-original
target commit after a fully processed target Data ACK boundary and ends when a
fully processed MPP Data ACK transaction completes its required horizon. A
qualifying byte was committed at or after the sample start, is released once by
MPP Data ACK, and has unambiguous original-flight provenance. The elapsed time
is from that first qualifying commit through the completing processed ACK.
Target service counts qualifying target bytes; session service counts
qualifying bytes from every Product stream in that sender direction over the
same elapsed interval. A complete ACK event is applied atomically and may
overshoot a horizon; it is never split or truncated to manufacture a rate.
Zero-duration, reinjected, pre-boundary, ambiguously attributed, stale, or
application-limited observations produce no sample.

Application limitation is determined only from sender-local Product placement.
A sample is invalid if, before its horizon completes, a normal original
placement opportunity exists under the current shared bounds and ordinary
authority but the target has no fresh unassigned original byte to commit.
Carrier queue pressure, native backpressure, or unavailable shared credit is
not reclassified from peer or native `app_limited` input as Product application
limitation.

The controller first records two consecutive candidate-free reference samples,
each with at least `C` qualifying target bytes, while the target remains
continuously non-application-limited and all frozen generations remain current.
It then admits the candidate work in three contiguous parts:

1. **Readiness.** At most the candidate flight bound is outstanding at once
   until exactly `q` candidate-owned original target bytes have unambiguous
   Data ACK coverage. The completing ACK establishes the assisted-sample
   boundary and is not gain evidence.
2. **First assisted sample.** The controller assigns exactly `C` further
   candidate bytes under normal shared credit, path-flight, repair, reorder,
   queue, and session-memory bounds. Ordinary carriers remain work-conserving.
   The sample ends only after all `C` bytes have exact candidate-owned Data ACK
   coverage. Its target and session numerators contain only qualifying original
   Product bytes released during that interval.
3. **Second assisted sample.** Only after the first sample's completing ACK
   transaction has been fully processed, the controller assigns exactly
   another `C` candidate bytes and records a second sample under the same rules.

No additional candidate original byte is assigned. Candidate failure,
ambiguous ownership, or inability to finish any part is `WITHDRAWN` and uses
ordinary retained-range recovery. After the second assisted sample, candidate
flight is zero and candidate placement stops. The controller records two
further candidate-free reference samples of the same `C` horizon. The
independent minimum and maximum of all four target rates form the target
reference interval; the independent minimum and maximum of all four session
rates form the session reference interval. The independent minima and maxima of
the two assisted target rates and of the two assisted session rates form their
respective assisted intervals. Pre/post bracketing and repeated assisted gain
reject a one-sample improvement and a persistent candidate-independent level
shift; they do not establish a statistical confidence level or identify every
transient coincidence.

The candidate Product phase uses the existing transport-derived measurement
lease. Let `P = SRTT + max(4 * RTTVAR, 1 ms) + 25 ms`, with
`RTTVAR = max(jitter, SRTT / 8)` and fallback `P = 1024 ms`. Candidate `T` and
`P` are frozen after validation admission and before the first pre-reference
sample, so the complete sequence uses one geometry. Let `n` be the smallest
positive number of modeled service rounds satisfying
`I * (2^n - 1) >= H`; round one covers `I`. The phase lease is:

```text
max(1 s, P + max(n * P, ceil(H * 8 / r)) + P)
```

It starts with the first readiness-byte commit, is capped by the remaining
sender-local validation lease, never restarts, and discharges successfully
after the second assisted sample has complete coverage. The validation lease
alone bounds the reference samples, result, and acknowledgment. Neither lease
alters TCP congestion control, pacing, retransmission, socket options, or any
RFC timer.

The sender emits `RETAIN` only when:

1. the exact candidate has Data ACK coverage for all `H` assigned original
   bytes and owns no remaining Product flight;
2. the lower assisted target rate is strictly above the upper target reference
   rate;
3. the lower assisted session rate is strictly above the upper session
   reference rate; and
4. every exact candidate, saturation, demand, ordinary-service, workload,
   sender-policy, and sender-local deadline fence remains current.

For either direction, the client separately applies its current group-policy,
reservation, resource, and validation-lease fences at its role-specific local
commitment points in Section 7.2: result serialization and acknowledgment
acceptance for client-to-server validation, or result acceptance and
acknowledgment serialization for server-to-client validation.

A complete sequence that does not satisfy both strict service comparisons is
`NO_GAIN`. Ended demand, expiry, incomplete coverage, ambiguous Product
provenance, or a stale fence is `WITHDRAWN`. Comparisons use exact integer
byte/time fractions; verdict construction uses no fixed percentage, EWMA
coefficient, locator, source identity, periodic probe, or wall-clock reopening
rule. This establishes only two repeated observed target and session Product-
service improvements bracketed by candidate-free service. It cannot prove a
statistical confidence level, physical bottleneck separation, or RFC 6356
coupled fairness.

The identity of a rejected attempt and the scope that gates later expansion
are distinct. For one group-selection sequence, the client owns a bounded
attempted-group set keyed by `(SessionId, direction, sequence)`. The sequence
is the local saturation generation for client-to-server work and the current
demand `request_id` for server-to-client work; the client never reconstructs
the server's local generation. Reserving a physical candidate adds its exact
client-local group before connection start; selecting an existing retained
carrier adds its group before validation admission. That group cannot be tried
again in the sequence, while an untried group remains eligible after the
previous candidate reaches its applicable negative settlement or terminal
boundary. Group identity and the attempted-group set are never sent.

After `NO_GAIN`, the sender also owns one rejected-service admission gate keyed
only by `(SessionId, direction)`. It gates creation of a later saturation
generation or a superseding present demand request; it does not prevent the
current generation from examining an untried group. The gate retains the
largest `C` and the closed hull of the independent session reference intervals
from every `NO_GAIN` in that generation, together with the stable
ordinary-service and sender-policy generations. Each exact validation result
retains its own candidate, validation, target, and saturation identity; those
attempt identities are not used as the later-admission lookup key.

A changed sender-observed ordinary carrier set or sender-policy generation
clears the rejected-service gate because it changes the measured baseline.
Otherwise, two consecutive candidate-free, byte-disjoint session samples,
each containing at least the retained largest `C`, may clear it. Both samples
must contain non-application-limited throughput Product work under one later
stable workload generation and current ordinary-service and sender-policy
generations. Their independently formed session interval must be strictly
disjoint from the retained rejected hull:
`new_upper < old_lower` or `new_lower > old_upper`.

The later samples may be supplied by the same target or by later throughput
Product work after the rejected target ends. A target, demand, or workload
identity change alone does not clear the gate, but it cannot make measured
rearm unreachable. Clearing the gate only permits a later
successful-placement-to-saturation edge. For server-to-client work, the next
strictly newer present demand request communicates that sender-side rearm and
starts a fresh client attempted-group set; for client-to-server work, the
client owns both state transitions locally. Merely waiting, changing a locator,
or observing ACK silence is insufficient. The one-live-candidate,
one-attempt-per-group, terminal-drain, configured-range, and receiver
`max_paths` bounds prevent an authenticated peer request from creating an
unbounded connection burst.

### 15.2 Reinjection budget and timing

Ordinary reinjection is limited by cumulative extra-traffic credit funded by a
bounded startup allowance and unique bytes acknowledged by MPP Data ACK.

Exact carrier-instance failure permits immediate bounded reinjection on an
eligible live alternative. A measured survivor is preferred, but liveness is
sufficient when no measured survivor remains.

A complete Data ACK snapshot may establish omitted ranges. Later positive
partial ranges extend known progress but do not establish omissions alone.

The MPP recovery interval uses the original carrier's underlay and latest
snapshot:

- TCP: with an observation, let `SRTT = max(srtt, 1 ms)` and
  `RTTVAR = max(jitter, SRTT / 8)`. The interval is
  `max(SRTT + max(4 * RTTVAR, 1 ms), 200 ms)`. Without an observation it is
  `1 s`.
- QUIC: with an observation, use the same `SRTT` and `RTTVAR`; the interval is
  `SRTT + max(4 * RTTVAR, 1 ms) + 25 ms`. Without an observation it uses
  `SRTT = 333 ms` and `RTTVAR = 166.5 ms`, yielding `1024 ms`.

These are MPP estimates. They do not read, reset, or replace a native TCP RTO
or QUIC PTO.

For request-direction feedback whose fragments may arrive on different
carriers, the same lowest missing frontier waits one MPP recovery interval
from its first authoritative observation.

For response-direction feedback, a later MPP Data ACK event may authorize one
bounded repair after the original flight exceeds the local MPP Data-ACK
threshold:

- `5/4 * SRTT` for TCP; or
- `9/8 * SRTT` for QUIC;

provided the alternative is estimated to complete before the MPP recovery
interval. These ratios are local approximations inspired by transport
time-threshold loss detection; the TCP ratio is not RFC 8985 RACK and the QUIC
ratio is not QUIC's native RFC 9002 loss decision. ACK silence alone waits one
MPP recovery interval.

A contiguous live tail without an authoritative gap may send one bounded probe
after one MPP recovery interval. Another repair requires another full interval
without MPP Data ACK progress.

When another attachment is available, request placement stops selecting a
non-progressing original attachment after four TCP MPP recovery intervals or
three QUIC MPP recovery intervals. The carrier remains connected and native
recovery continues.

If cumulative extra-traffic credit is exhausted, an exact carrier failure,
persistent authoritative gap, or live-tail event may use one critical recovery
quantum to prevent the budget itself from deadlocking recovery. The quantum is
bounded by retained ranges, exact flight identity, queue and flight limits,
repeat-delay suppression, and a distinct output while the original carrier is
live. Its bytes remain charged and reduce later optional reinjection authority.

### 15.3 Datagram retry

Before request feedback, one datagram identity has at most two product
attempts.

When a ranked, unattempted alternative exists, the first attempt waits one
modeled response timeout derived from that carrier. It may then retry on the
alternative while the original absolute TTL remains. Reopening the same
configured carrier is not an alternative.

The only or final attempt may wait three modeled response timeouts, capped by
the absolute TTL. TCP and QUIC derive their response estimates independently.
After matching request feedback, no further request attempt is permitted.

For native HTTP Datagrams that overtake their reliable flow open, the receiver
handoff bound is:

```text
min(remaining TTL, clamp(2 * current QUIC RTT, 25 ms, 250 ms))
```

## 16. Conformance Invariants

A conforming implementation preserves all of the following:

1. One stream byte has one `(session, stream, direction, offset)` identity
   across all copies.
2. Only MPP Data ACK releases retained MPP stream ranges.
3. Duplicate delivery creates no duplicate delivery or rate evidence.
4. Receive credit is shared by all attachments in one stream direction.
5. Opposite stream directions have independent offsets, acknowledgments, and
   credit.
6. TCP and QUIC retain native congestion control and recovery authority.
7. Regular carriers are considered before backup carriers.
8. Usage, local health, authentication, and demand remain independent.
9. A locator and numeric `PathId` never replace carrier-instance identity.
10. A QUIC locator change does not create a new carrier when QUIC preserves
    the connection.
11. Scheduling observes immutable state and revalidates before commit.
12. Reinjection is evidence-driven, bounded, and preserves stream offsets.
13. No fixed attachment role determines future placement.
14. Optional native telemetry is never required for correctness.
15. Carrier-instance and attachment-incarnation lifetimes are separate fences.
16. One datagram preserves its session, flow, direction, ID, and payload across
    attempts.
17. A retained datagram request identity is forwarded to its target at most
    once.
18. Simultaneous TCP carriers within one MPP session always have distinct
    `PathId` values and distinct carrier instances.
19. Non-failure TCP carrier expansion is bounded, serialized, and retained
    only by sender-owned, session-scoped, directional Product evidence released
    by MPP Data ACK.
20. Before its first acknowledged `RETAIN`, a TCP candidate owns at most one
    target attachment with one directional validation binding and finite
    validation-original work, but no ordinary placement authority.
21. Ordinary-use authority in one direction grants no authority in the other.
22. `PATH_CLOSE` is the ordered aggregate acknowledgment of a matching
    `PATH_DRAIN`; local emptiness or write completion cannot replace it.
23. `SESSION_CLOSE` retires the complete `SessionId`; carrier drain does not.
24. Attachment membership and feedback capability alone grant no ordinary TCP
    payload authority; validation placement requires its exact bounded
    controller record.
25. In either direction, the exact `RETAIN` acknowledgment is received before
    the sender may place ordinary Product payload on the candidate.
26. Configured capacity that has not established a physical carrier is
    not an eligible carrier, path attachment, or health state.
27. One client session service reconciles each TCP carrier group; an exact
    carrier actor owns its wire lifecycle, while a stream never owns group
    capacity or replacement.
28. A configured-minimum member identity is client-local and persists across
    its successive replacement instances. A planned replacement permits only
    the bounded current/successor/predecessor overlap in Section 7.2.
    Classification of each exact carrier instance as minimum or elastic is
    immutable and is never inferred from a locator or peer path label.
29. A disabled TCP carrier group establishes no carrier and grants no new
    original placement. Every exact instance already in that group reaches
    terminal state through ordered carrier retirement; re-enable creates fresh
    instances and never cancels a drain already begun.

## 17. Relationship to Existing Standards

### 17.1 MPTCP

RFC 8684 provides the established principles of stable data identity across
subflows, a data-level acknowledgment distinct from transport ACKs, shared
connection flow control, reinjection, and backup preference.
Its Section 3.9.2 also leaves additional-subflow policy local while explicitly
supporting delayed creation for short flows, buffered-demand input, rate
limiting, and a total subflow bound. MPP therefore uses an actual blocked
throughput placement and its configured carrier/resource bounds; it imports no
universal subflow threshold. Section 3.3.8 motivates regular-to-backup
transition. Section 2.6 permits one MPTCP subflow to close through ordinary TCP
FIN/ACK without closing the MPTCP connection; MPP's ordered per-carrier wire
transaction is independently defined as `PATH_DRAIN` followed by `PATH_CLOSE`.

MPP follows those principles but is not MPTCP-conformant. Its offset space is
per direction of each MPP stream rather than one connection-level DSN space
per direction.
`STREAM_ACK` uses range snapshots and positive partial ranges rather than a
cumulative DSS Data ACK. MPP carriers are not MPTCP subflows.

RFC 6356 documents why independently controlled subflows sharing a bottleneck
can be less fair than one TCP flow. MPP cannot install coupled control above
kernel TCP, so an elastic TCP carrier is retained only after the bounded
aggregate validation in Section 15.1. That validation establishes observed
MPP service; it does not claim coupled fairness or identify a bottleneck.

### 17.2 QUIC

RFC 9000 and RFC 9002 govern each QUIC carrier's connection identity, network
paths, address validation, migration, congestion control, loss recovery, RTT,
ECN, and PMTU behavior. MPP does not redefine those mechanisms.

MPP uses multiple independent QUIC connections as carriers. It does not claim
Multipath QUIC conformance.

### 17.3 HTTP/3 and HTTP Datagrams

RFC 9114 supplies the HTTP/3 mapping and RFC 9297 supplies HTTP Datagrams,
Quarter Stream IDs, settings, association lifetime, and error behavior.

MPP defines a private encrypted request opt-in and its own datagram envelope.
It is deliberately not RFC 9298 CONNECT-UDP: it uses `POST`, MPP flow-open
frames, MPP IDs, feedback, TTL, and fragmentation.

### 17.4 Congestion-control coupling

RFC 6356 describes coupled congestion control for MPTCP subflows. MPP does not
apply that algorithm above independent TCP and QUIC carriers. This document's
fairness requirement is limited to preserving native transport authority and
bounded MPP work.

## 18. References

### 18.1 Normative

- [BCP 14: RFC 2119 and RFC 8174](https://www.rfc-editor.org/info/bcp14)
- [RFC 8446: The Transport Layer Security (TLS) Protocol Version
  1.3](https://www.rfc-editor.org/rfc/rfc8446.html)
- [RFC 9000: QUIC: A UDP-Based Multiplexed and Secure Transport](https://www.rfc-editor.org/rfc/rfc9000.html)
- [RFC 9002: QUIC Loss Detection and Congestion Control](https://www.rfc-editor.org/rfc/rfc9002.html)
- [RFC 9114: HTTP/3](https://www.rfc-editor.org/rfc/rfc9114.html)
- [RFC 9221: An Unreliable Datagram Extension to QUIC](https://www.rfc-editor.org/rfc/rfc9221.html)
- [RFC 9297: HTTP Datagrams and the Capsule Protocol](https://www.rfc-editor.org/rfc/rfc9297.html)

### 18.2 Informative

- [RFC 6356: Coupled Congestion Control for Multipath
  Transport Protocols](https://www.rfc-editor.org/rfc/rfc6356.html)
- [RFC 8684: TCP Extensions for Multipath Operation with Multiple
  Addresses](https://www.rfc-editor.org/rfc/rfc8684.html)
- [RFC 8985: The RACK-TLP Loss Detection Algorithm for
  TCP](https://www.rfc-editor.org/rfc/rfc8985.html)
- [RFC 9298: Proxying UDP in HTTP](https://www.rfc-editor.org/rfc/rfc9298.html)
