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
: Bounded admission of one elastic TCP carrier for one sender direction. It
  carries finite validation work and grants no ordinary-use authority before
  the exact `RETAIN` acknowledgment.

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

**Candidate flight bound**
: The maximum unacknowledged unique-original Product bytes that one unproven
  TCP candidate may own at once before it reaches the Data ACK startup sample
  floor. It is the existing reliable unproven-path startup-flight bound and
  remains subject to shared receive credit, path flight, repair, reorder, and
  session-memory limits. After that floor, bounded validation placement uses
  the ordinary mature-path flight and queue model.

**Validation work**
: The finite cumulative unique-original target bytes assigned to one TCP
  candidate during one directional validation. It is not an amount that must
  be simultaneously resident; outstanding flight, queued work, repair,
  reorder debt, and memory MUST fit their stream and session resource
  envelopes at every point.

**Product service cohort**
: One sender-side interval collected under one frozen directional comparison
  key and placement mode. It opens after a fully processed target-stream Data
  ACK at an exact serialized writer boundary and closes with the complete Data
  ACK transaction that reaches its target-byte coverage. Target and aggregate
  Product-service rates use identical wall boundaries and the greater of the
  writer-boundary and Data-ACK-boundary spans. A cohort is ordinary-only or
  candidate-assisted according to which carriers may receive unique-original
  placement after its opening writer boundary.

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
`PATH_DRAIN` and `PATH_CLOSE` are valid only on TCP carriers. The TCP carrier
client alone sends `PATH_DRAIN`; the TCP carrier server sends `PATH_CLOSE` only
as the response that completes that drain. Their `path_id` MUST match the TCP
carrier carrying the frame. A frame sent in the opposite direction, or a
`PATH_CLOSE` without a matching `PATH_DRAIN`, is a carrier protocol violation.
After drain begins, both endpoints MUST make that carrier ineligible for new
attachments and original placement while retaining receive, Data ACK,
recovery, and ordered-control processing.

Sending `PATH_DRAIN` at the client and receiving it at the server terminally
cancels every unsettled validation on that carrier and stops new validation
placement. A crossing result or acknowledgment has no authority and is
discarded with the canceled validation. Existing bounded Product flight is
still delivered or recovered, and its directional validation binding is
cleared. An attachment created only for that validation is detached through
the ordinary attachment lifecycle before the zero-work condition below can
hold.

The server sends `PATH_CLOSE` only after every earlier frame from the client
has been applied and the exact carrier has no attachment, datagram
binding, queued or retained frame, original or reinjected flight, pending Data
ACK, path proof, capacity work, carrier validation, or queued or unprocessed
carrier-demand frame. An applied session-level demand request does not belong
to the carrier that conveyed it and does not pin that carrier. All server
frames that complete carrier-owned work MUST precede
`PATH_CLOSE` in the TCP byte stream. The client treats receipt of
`PATH_CLOSE`, not its own write completion or local emptiness, as the aggregate
retirement acknowledgment. It removes the carrier only after applying every
preceding server frame and reaching the same local zero-work condition.
Native failure before that boundary uses ordinary retained-state recovery.

The client starts a local absolute graceful-retirement ceiling when it closes
new carrier admission and begins local drain; the server starts its own when
it receives `PATH_DRAIN`. Each uses the configured
`[session].retention_timeout_ms`, never restarts or extends it, and does not
assume that the peer's deadline is equal or synchronized. Expiry closes that
exact native TCP carrier and enters ordinary exact-failure recovery; it does
not synthesize `PATH_CLOSE`.

The same configured duration is the Product resource-lifetime ceiling for
carrierless stream retention, graceful carrier retirement, and admitted
pre-retain validation settlement. These are independent absolute lifetimes;
progress in one never restarts another. The duration is not carrier health,
delivery, Product service, contraction, or performance evidence and cannot
produce `RETAIN` or `NO_GAIN`.

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

Removing a TCP carrier group makes the client retire each exact carrier through
the ordered `PATH_DRAIN`/`PATH_CLOSE` procedure. Re-adding it creates new group
and policy generations and does not cancel a drain already begun.
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

On TCP, the carrier client requests graceful retirement with
`PATH_DRAIN(path_id)` and the carrier server completes it with
`PATH_CLOSE(path_id, reason)`. The server MUST NOT initiate `PATH_DRAIN`, and
the client MUST NOT initiate `PATH_CLOSE`. On QUIC, native connection lifecycle
performs carrier retirement.

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
reservations. The successor can become current only at the old instance's
exact Product-quiescent admission boundary. That commit atomically publishes
the successor before the predecessor begins ordered retirement. No queue,
attachment, flight, evidence, or authority is transferred between those exact
instances.

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
the minimum requires Section 15.1 validation. At most one unretained elastic
carrier and one active directional validation may exist in an MPP session.
The client starts no further elastic connection until the exact current
validation is acknowledged and its candidate is retained or reaches
`PATH_CLOSE` or exact native failure. A carrier already retained in one
direction remains live after a negative result in the other direction and MAY
later validate that direction while no other validation is active.

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
the exact current directional validation may reserve bounded unique-original
work through that binding.
Ordinary scheduling, new stream or flow creation, datagrams, reinjection, and
the opposite direction remain prohibited before their respective acknowledged
authority.

A validation-only attachment is a live owner of the exact Product flight
assigned through its directional validation binding. It MUST therefore remain
in ordered-flight and failure-recovery ownership until that binding is settled
or atomically converted to ordinary membership. It remains absent from
ordinary scheduling membership throughout validation. Unresolved work on a
live validation-only attachment is not missing-owner work; only exact native
failure or settlement may expose it to ordinary recovery.

The client owns physical TCP establishment because only it knows the
configured carrier group and its bounds. The sender owns demand and delivery
evidence for its direction:

- client-to-server validation is driven by the client's aggregate sender
  state and exact request Data ACK release; and
- server-to-client validation is driven by the server's aggregate sender
  state and exact response Data ACK release.

Each sender direction owns its validation and bounded Product-service
evidence. The named throughput stream supplies only the expansion demand and
validation work. Only that sender may publish a verdict. It counts fully
processed, unambiguous unique-original Data ACK release from the target and
every Product stream in that direction.
Candidate attribution additionally requires the exact directional validation
binding, attachment incarnation, and original-flight owner. Native TCP ACKs,
duplicated or reinjected bytes, capacity receipts, and peer metrics contribute
no Product numerator.

A validation result remains valid only while the exact candidate, target
stream and attachment, demand class, ordinary carrier membership and
eligibility, concurrent Product workload, and local admission and resource
policy used by its comparison remain unchanged. Candidate establishment and
its validation-only attachment are not ordinary membership or a new Product
workload. A change to any comparison input makes an incomplete validation
`WITHDRAWN`. No source address, interface, locator, peer metric, elapsed-time
rule, or ACK silence substitutes for those sender-owned facts.

For server-to-client demand, the server sends
`TCP_CARRIER_DEMAND(request_id, stream_id)` on any ready TCP carrier in the
session. No locator or client-local group identity is encoded. Request IDs are
nonzero and strictly increase in one server-owned session sequence; after
`u64::MAX`, the server sends no further demand in that session.
`stream_id = none` withdraws the current request; a present stream supersedes
every older request and names the one current response-demand target. It
remains current until superseded, withdrawn, or the target no longer satisfies
Section 15.1. Each exact candidate uses a fresh validation ID on its own
carrier.
Present-stream supersession is a wire ordering rule, not authority for local
demand churn. Section 15.1 admission ownership does not publish a different
target merely because that target also becomes saturated while an unchanged,
not-yet-admitted request remains current.
Because different TCP carriers may fail or reorder session-level delivery, an
exact duplicate of the current request is idempotent, an older request is
ignored, and reuse of the current ID with different presence or stream content
is a protocol violation.

The client MAY ignore the request and MUST independently check the current
request ID, stream and session liveness, configured bounds, resources, and the
one-candidate limit before establishing a candidate. It rechecks the exact
local group admission, reservation, and policy when sending
`TCP_CARRIER_VALIDATE` and at its role-specific result commitment points. The
server independently rechecks its sender-owned comparison inputs when
validation arrives. Neither endpoint reconstructs the other's configuration.
A request grants no carrier, attachment, placement, receive credit, or byte
budget. Repetition or supersession of peer requests cannot bypass the client's
connection-rate, occupied-reservation, configured-maximum, or session resource
bounds.

After validation-purpose readiness and before creating or binding the target
attachment, the client sends
`TCP_CARRIER_VALIDATE(validation_id, request_id, direction, stream_id)` on
the candidate carrier. The authenticated carrying connection is the candidate;
no locator, `PathId`, group description, local comparison state, or carrier
nonce is repeated. Client-issued `validation_id` values are nonzero and strictly
increase across both directions of that exact candidate; after `u64::MAX`, the
client starts no further validation on the instance. Client-to-server
validation requires `request_id = 0`.
Server-to-client validation requires the current nonzero demand request and
its exact stream. Every other direction and request combination is a
candidate-carrier protocol violation.

Malformed, noncanonical, duplicate, or unauthorized references are protocol
violations on the candidate. A well-formed reference that became stale
because the exact carrier, target stream, demand request, or comparison input
changed races harmlessly to `WITHDRAWN`; it is not a peer fault. Candidate
Product placement additionally requires peer usage `AVAILABLE` for that
direction. `BACKUP` preference is never bypassed to obtain a favorable result.

Connection establishment remains subject to `path_probe_timeout_ms`.
Admission of `TCP_CARRIER_VALIDATE` retains bounded sender and receiver state
until the exact result acknowledgment, ordered drain, native failure, or
session close. A sender MAY make an unsettled validation `WITHDRAWN` under its
local resource policy and is the only endpoint that can serialize that result.
A receiver that is also the carrier client cancels by ordered carrier drain. A
receiver that is the carrier server cannot send `PATH_DRAIN` or a sender-owned
result and therefore closes the exact candidate natively when its local state
expires. If result signaling or client-owned drain cannot complete, exact
native carrier failure remains the per-carrier terminal fallback;
`SESSION_CLOSE` is never synthesized for this purpose. No local resource
lifetime is known by the peer or used as delivery or performance evidence.

The sender reports one result with
`TCP_CARRIER_RESULT(validation_id, direction, result)` on the candidate
carrier. `RETAIN` means the sender established the target-flow and session
service conditions in Section 15.1. `NO_GAIN` means a complete validation did
not establish both. `WITHDRAWN` means the validation became invalid,
inconclusive, or expired and makes no capacity claim. Only the sender may emit
a result, and serialization makes that result immutable: later demand or
comparison-input changes do not create a second result or roll it back.

The receiver accepts only an exact current result. If its candidate, direction,
local admission policy, and bounded resource state permit the
result, it atomically applies the local result and serializes
`TCP_CARRIER_RESULT_ACK(validation_id, direction, result)` on the same
candidate. The acknowledgment repeats all result fields exactly. Unknown,
conflicting, duplicate, or noncanonical results and acknowledgments are
candidate-carrier protocol errors. A canonical exact result or acknowledgment
crossing a local policy change that requires candidate retirement, ordered
drain, or native failure is discarded with that
carrier; a local race does not turn the frame into a peer fault. When the
receiver is the client, its local admission policy includes the exact
group admission and reservation from validation. When the sender is the
client, the same admission and reservation are rechecked at result
serialization and acknowledgment acceptance.
Admission of `TCP_CARRIER_VALIDATE` reserves the receiver's bounded
result-and-acknowledgment state. Ordered drain cancels that state as specified
in Section 6.1.

For `RETAIN`, acknowledgment serialization commits the receiver's directional
authority and converts the exact directional validation binding to ordinary
authority on that attachment; acknowledgment receipt commits the sender's
matching transition.
The sender MUST NOT place work beyond the finite validation work or admit
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
zero-work boundary and holds the reservation until matching server
`PATH_CLOSE` or exact native failure.
Result, acknowledgment, authority transition, detachment, and any following
drain share the exact carrier ordering. Cancellation of a local operation
cannot omit or overtake that suffix.

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

Retention does not permanently pin resources. Contraction is local carrier
policy and is permitted only after the carrier is made ineligible for new
placement, has no carrier-exclusive queued or in-flight Product work, active
validation, or unsettled result, and another carrier has ordinary-use
authority for every affected direction. Attachments are evacuated or detached
through their normal ordered lifecycle; the client sends `PATH_DRAIN` after the
local zero-work boundary and matching server `PATH_CLOSE` completes retirement.
New demand after drain begins uses another carrier and does not reverse that
retirement. This specification defines no idle interval for contraction.

Changing the destination port of a TCP carrier creates a replacement carrier;
it never migrates the existing TCP connection. Planned replacement MUST be
make-before-break for a configured-minimum member when the resource envelopes
have spare capacity: authenticate a provisional successor while the predecessor
remains current, then revalidate the predecessor identity and exact
Product-quiescent state at the same transaction that publishes the successor.
If Product work was admitted during establishment, the provisional successor is
discarded and the predecessor remains unchanged. A successful commit fences
new attachment and original placement on the predecessor, publishes ordinary
authority on the successor, has the client send `PATH_DRAIN` after the
predecessor's preceding writer work, and waits for the server's `PATH_CLOSE`.

Product quiescence is an exact session work-ownership state, not an idle timer
or a traffic sample: the MPP session has no logical Product-flow owner, no
carrier has admitted Product load, relay queue, or relay flight, and the old
TCP instance still matches the configured-minimum member being replaced. A
logical reliable or datagram Product flow retains its session owner through
peer-direction traffic, retention, and recovery even when it temporarily has
no attachment. Every Product open reserves path-load ownership before carrier
I/O; its logical owner releases only when the complete Product flow becomes
terminal. An idle attachment shell grants no Product-work ownership and is
settled by the normal ordered drain. Logical Product ownership, path admission,
and the replacement commit are serialized by the same path-state transaction.
The configured hop interval only makes a replacement eligible for
consideration; it proves neither quiescence nor usefulness.

When no spare capacity exists, replacement waits for exact Product quiescence,
fences and drains the predecessor within the configured maximum, then
establishes the next carrier. It never closes an active carrier to satisfy a
hop deadline. New demand during retirement uses another schedulable carrier or
waits for minimum reconciliation; it does not reverse an ordered drain.

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
`STREAM_MAX_DATA` from that direction's receiver. A receiver that refuses only
the pending attachment sends `STREAM_DETACH` on that carrier. `STREAM_RESET`
is reserved for terminating the logical MPP stream and MUST NOT be used to
refuse an additional attachment.

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

`STREAM_ACK(stream_id, complete, ranges)` carries a list of half-open ranges in
one directional MPP stream offset space. Every listed range is non-empty; the
list itself MAY be empty.

When `complete` is true, the list is an authoritative snapshot of the
receiver's retained received ranges and can establish an omitted gap. When
false, the ranges report partial positive progress and omission does not imply
a gap. `complete` does not mean end of stream.

Negative authority from a complete snapshot extends only through the greatest
range end carried by that snapshot. The sender's current assigned offset is not
receiver evidence and MUST NOT be used as the snapshot horizon. A complete
contiguous prefix therefore proves its positive range but does not declare a
later assigned tail missing; an empty complete snapshot establishes no negative
extent. An incomplete update can fill an already authoritative gap but cannot
extend its horizon.

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

The logical receiver retains its latest cumulative received-range state until
the stream becomes terminal. At the existing Data ACK cadence, changed
cumulative state advances one local publication generation and is offered
independently to every currently live exact attachment. Forced publication of
unchanged state reuses that generation. Queue acceptance advances only that
exact attachment incarnation's publication fence; a blocked attachment remains
pending and retries on existing carrier-capacity or attachment-membership
events. A newly accepted attachment starts without a fence and receives the
retained latest state. This publication rule adds no timer, ACK threshold,
receive window, congestion signal, or carrier-delivery attribution.

Changed cumulative state that has not reached the byte cadence remains eligible
for the existing delayed Data ACK deadline on every reliable underlay. Once
that changed state is published, it no longer activates the deadline unless an
existing feedback-resend rule applies. This adds no independent timer or
threshold.

When the cumulative range set fits one frame, that frame MAY be complete. When
it requires multiple frames, every chunk MUST be incomplete positive evidence,
and an attachment advances its generation fence only after every chunk was
accepted. Received positive ranges are monotonic for the stream lifetime, so a
newer cumulative generation MAY supersede the unqueued tail of an older
generation; already accepted older chunks remain valid and idempotent. The
logical receive range ledger remains the sole range owner. Each attachment
retains only its exact-incarnation generation and next-chunk cursor.

### 8.4 Shared flow control

`STREAM_MAX_DATA(stream_id, max_offset)` grants the greatest offset the sender
may assign in that direction. The maximum is shared by all attachments of that
stream and direction. Adding a carrier MUST NOT multiply it.

The sender retains the greatest observed maximum; a smaller value does not
revoke credit. A new attachment MAY therefore receive a credit-neutral
`STREAM_MAX_DATA(stream_id, 0)`. Only the logical receive owner grants new
credit.

The logical receive owner MUST retain its greatest grant as idempotent
connection state until the stream becomes terminal. When that grant advances,
the owner MUST attempt to publish the latest value independently on every
currently live attachment. Acceptance by one attachment publishes the shared
grant and permits the receiver to admit bytes through that offset; an
attachment whose carrier queue is blocked remains pending rather than
consuming the publication. A newly attached carrier MUST receive the retained
latest value after its credit-neutral attachment acceptance. Carrier-capacity
notifications retry pending publication; this rule adds no independent timer,
window, or congestion control.

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
the frame. It also refuses a pending `OPEN_STREAM` on that carrier without
terminating an existing logical stream or any sibling attachment. It is not an
acknowledgment of peer-side flight or carrier quiescence. During TCP carrier
drain, an endpoint MUST retain the attachment state needed to receive
preceding frames, publish or process Data ACK, and complete recovery until the
ordered `PATH_CLOSE` boundary in Section 6.1.
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

Per-flow evidence MUST NOT be treated as shared carrier capacity. Only the
directional sender may combine per-stream events into an aggregate conclusion,
and only while the target demand, ordinary carrier membership and eligibility,
concurrent Product workload, and local admission and resource policy remain
unchanged. A configured rate is a startup prior, not measurement.

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
protocol violation. `PATH_DRAIN` is client-to-server only; `PATH_CLOSE` is
server-to-client only and requires a matching `PATH_DRAIN`.

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

Loss of an established QUIC connection or its HTTP/3 carrier request stream is
a failure of that exact carrier instance, not of every Product stream using the
session. Carrier recovery preserves the logical stream and its exact retained
ranges on surviving authenticated attachments. Frame-codec, authentication,
configuration, and Product protocol failures do not acquire that recovery
authority merely because they were observed through a QUIC carrier.

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
state, comparison input, or client-owned group admission has no authority.

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

An output does not become the contiguous-frontier owner merely because it is
the only output that can currently enqueue. While an unresolved lower original
range belongs to another output, the survivor remains an additional output and
MUST retain the corresponding Product flight, completion, and reorder bounds.

After exact original-data Data ACK coverage reaches the configured startup
sample floor, the response scheduler may use its mature completion-time model.
Duplicated bytes do not satisfy that floor for either copy.

A later app-limited carrier observation does not revoke an earlier qualified
delivery sample while that sample remains fresh; mature placement continues to
use its completion estimate. The app-limited observation itself creates no new
admission generation or placement authority. After qualified completion
evidence expires, the output returns to unqualified acquisition. When the
carrier exports a positive native congestion window, its unresolved original
work is bounded by that window and the existing startup-flight floor; an older
Product service window MUST NOT enlarge it. When no native window is
observable, the portable bounded Product service window remains the fallback.
Acquisition does not grant a new completion estimate or bypass shared
receive-credit, reorder, queue, or configured flight bounds.

Non-failure TCP carrier expansion is a local delayed-start policy. It MAY be
considered only for throughput demand with fresh queued unique-original data
when shared receive credit, assigned-offset, repair, reorder, and session
resource bounds permit another original service quantum. In the first
nonempty regular-or-backup authority class, every eligible ordinary carrier
MUST already own original target work and no eligible ordinary carrier may
accept the proposed enqueue. No latency or realtime work may be active in that
session direction. Only a transition from successful ordinary placement to
this condition admits an attempt; rechecking unchanged state, retained flight,
reinjection, native buffer state, elapsed time, or ACK silence does not.

The throughput-demand episode is owned by the existing Product demand
classifier, not by an implementation queue snapshot. Fresh queued
unique-original data is required at the successful-placement-to-saturation
transition, but draining that queue through successful ordinary placement does
not end an already-established episode while the classifier still reports
throughput demand. The episode ends only when the existing classifier observes
the absence of queued or pending Product work and crosses its normal idle
boundary. Implementations MUST NOT turn a momentary work-conserving queue drain
into a new admission generation or use it to withdraw an otherwise unchanged
comparison.

Finite source admission while a reliable relay is latency-oriented is scoped
to that direction's current Product demand episode. It MUST use the same
adaptive byte boundary as unconditional throughput classification. When the
classifier crosses its normal idle boundary, the new episode receives fresh
bounded admission; the reliable stream's lifetime Data Sequence offset MUST
NOT consume that credit. Queued unique-original data counts once when admitted,
moving it from the sender queue into assigned offsets does not count it again,
and pending delivery prevents the episode from being declared idle.

That transition creates one sender-owned admission generation for the
continuous demand episode and its stable ordinary membership, authority,
admission-policy, and resource-policy generations. Each eligible TCP carrier
group may be attempted at most once in it. A new demand episode, or a change to
one of those stable generations, creates fresh admission authority; a timer,
ACK silence, queue or credit oscillation, repeated blocked observation,
candidate result, or candidate connection failure does not.

For server-to-client expansion, the sender serializes target selection with
that admission authority. While a current present request has not reached
exact validation admission, saturation from another target MUST NOT replace
it. The current target or frozen generations changing may supersede or withdraw
it as already specified. After the request reaches validation and that exact
transaction settles or fails, another eligible target may be selected, but a
comparison key already issued for a target workload MUST NOT be published
again. The sender therefore retains one latest issued comparison key per live
target workload. This state is bounded by the existing workload envelope and
contains no timer, queue occupancy, or transport sample.

One MPP session has at most one unretained elastic carrier and one directional
validation at a time. The candidate authenticates with `PATH_JOIN` purpose
`VALIDATION`, reaches readiness, and sends `TCP_CARRIER_VALIDATE`. It remains
outside ordinary placement and may attach only the exact existing throughput
stream named by that validation. Its unique-original validation work is finite
and remains within the existing startup-flight, shared-credit, path-flight,
repair, reorder, queue, stream, and session bounds. Ordinary carriers remain
work-conserving, and every carrier retains native TCP congestion-control,
pacing, and recovery authority. When an already-retained elastic carrier
validates its other direction, it reuses the existing attachment; ordinary
Product work in its authorized direction remains valid.

Only unique-original Product bytes released by a fully processed MPP Data ACK
contribute service evidence. Candidate attribution additionally requires the
exact carrier instance, attachment incarnation, and original-flight ownership.
Native TCP acknowledgments, capacity traffic, duplicated or reinjected bytes,
peer metrics, locators, and source or interface identity contribute none.

An elastic carrier receives `RETAIN` only when a bounded comparison under
unchanged target demand, ordinary carrier membership and eligibility,
concurrent Product workload, and local admission and resource policy
establishes strict improvement in both target-flow and aggregate session
Product service with the candidate. The same evidence qualification and bounds
apply with and without the candidate.

At admission the sender freezes one comparison key containing the session and
direction, target stream and demand generation, exact candidate instance,
exact ordinary instances and directional authority class, ordinary
non-queue liveness and policy eligibility, complete active Product-workload
identities and lifecycle generations, and local admission-policy and
resource-policy generations. Any change to that key makes an incomplete
comparison `WITHDRAWN`. Instantaneous enqueue capacity, receive credit, flight,
queue occupancy, rate or other evidence value, source address, interface,
locator, native transport sample, peer metric, and elapsed-time observation is
not a key member. Continuous target demand and work-conserving ordinary
placement are phase invariants instead.

The sender derives the comparison geometry once, before candidate Product
service, from four existing Core quantities:

- `startup_coverage` is the reliable-path Data ACK startup sample floor;
- `rate_window` is the reliable Data-ACK rate-coverage floor;
- `measurement_envelope` is the existing reliable Product-measurement session
  envelope: the minimum of path flight, repair, reorder, stream-window, and
  session resource bounds; and
- `ordinary_pipe` is the checked sum of each frozen ordinary carrier's
  established throughput-lane data-level service window, namely two Product
  BDPs with the existing service-quantum and minimum-pipe floors, rounded up
  per carrier and limited once by `measurement_envelope`.

All quantities MUST be positive. Overflow or an envelope smaller than one
`rate_window` makes the attempt `WITHDRAWN`. `cohort_coverage` is the least
whole number of `rate_window` units that covers
`max(rate_window, ordinary_pipe)`. If that checked aligned value exceeds
`measurement_envelope`, validation is unavailable; it MUST NOT be rounded down
below the frozen ordinary pipe. This is a reuse of established scheduling and
resource geometry, not a new byte constant, percentage, or transport
parameter. Geometry is never recomputed from candidate results.

Candidate assignment reuses the Core's existing reliable-path flight model.
Before `startup_coverage` has been released by unambiguous candidate-owned
Data ACK, unresolved candidate original work MUST NOT exceed the unproven-path
startup-flight limit. Afterwards it MUST NOT exceed `measurement_envelope`,
the existing mature TCP Product-flight ceiling when the carrier exports no
native congestion window. Cumulative phase credit and instantaneous flight
credit are both enforced at every assignment. Candidate placement remains
work-conserving within those bounds; neither an entire phase credit nor the
carrier command queue is a second congestion window.

The comparison has four contiguous placement phases under the frozen key:

1. An ordinary reference cohort covers at least `cohort_coverage` qualified
   target bytes and at least `cohort_coverage` qualified ordinary-carrier
   aggregate bytes while the target remains in the same continuous throughput-
   demand episode and every eligible ordinary carrier remains work-conserving.
   This phase begins only
   after candidate readiness, validation admission, and key and geometry
   freeze; the authenticated candidate remains Product-idle throughout it.
2. Candidate startup assigns exactly `startup_coverage` cumulative
   unique-original target bytes to the candidate under the candidate-flight
   and shared resource bounds, splitting the final Product frame when needed.
   All of that work MUST resolve through unambiguous candidate-owned original
   Data ACK releases before the next phase. Native flight becoming empty does
   not substitute for consumption of those ordered Product-ACK receipts.
   Resolved repaired bytes do not contribute. These releases establish exact
   provenance and startup maturity but enter no comparison cohort.
3. A candidate-assisted cohort covers at least `cohort_coverage` qualified
   target bytes and at least `cohort_coverage` qualified ordinary-carrier
   aggregate bytes with ordinary carriers still work-conserving. Candidate-
   attributed bytes do not satisfy the latter coverage. The candidate assigns
   exactly one `cohort_coverage` and all of it MUST resolve through unambiguous
   candidate-owned original Data ACK releases inside the assisted cohort.
   Candidate cumulative validation work is bounded by the checked sum of
   `startup_coverage` and `cohort_coverage`.
4. New candidate placement stops. Confirmation begins only after the
   candidate's validation queue, original flight, recovery work, and reorder
   debt are zero. An ordinary confirmation cohort then covers at least
   `cohort_coverage` qualified target bytes and `cohort_coverage` qualified
   ordinary-carrier aggregate bytes with the candidate Product-idle and
   ordinary carriers work-conserving.

Every cohort is seeded after a fully processed target Data ACK at a serialized
writer boundary. Every unique original released by a fully processed,
unambiguous Data ACK transaction completed on or after both opening boundaries
enters the cohort, including ordinary work assigned before the boundary whose
Product service completes inside the measured ACK interval. Assignment time
is not service time and has no cohort-membership authority; exact carrier
provenance is used only for candidate attribution. The closing Data ACK
transaction is indivisible: all qualified
releases from that transaction remain in the closing cohort even when they
exceed the nominal byte coverage.
The writer span and Data-ACK span MUST yield a positive effective elapsed time.
Each cohort records target service and aggregate service by every stream in the
frozen directional workload over identical opening and closing timestamps.
The aggregate minus exact candidate-attributed bytes MUST cover one complete
`cohort_coverage`; this makes every comparison observe a causally eligible
ordinary service pipe instead of allowing fast candidate startup work to close
the assisted cohort before ordinary post-boundary work turns over.
Duplicate, reinjected, ambiguous, unqualified, foreign-workload, or
pre-boundary-completed releases may resolve ordinary state but contribute no
comparison bytes. At every phase boundary all phase-owned candidate work is
resolved before the next writer and Data-ACK boundaries are seeded.

Rates are compared as exact nonnegative byte/time fractions. Floating-point
rounding, EWMAs, percentage margins, configured rate hints, and native or peer
acknowledgments have no verdict authority. Checked integer fraction comparison
is used; overflow cannot become evidence. `RETAIN` requires all four strict
whole-cohort comparisons:

- assisted target-flow rate is greater than both reference and confirmation
  target-flow rates; and
- assisted aggregate-session rate is greater than both reference and
  confirmation aggregate-session rates.

Equality or failure of any comparison is not proven gain. Redistribution at
one shared bottleneck may improve the target stream, but cannot retain the
candidate without aggregate Product-service gain as well. The adjacent
ordinary/assisted/ordinary shape rejects a phase-local transient that does not
separate from both controls; it defines no per-window extrema or growing
sample count. It makes no statistical or counterfactual claim about an
external capacity change synchronized to the assisted phase.

A complete comparison that does not establish both improvements is `NO_GAIN`.
Ended demand, a changed comparison input, expiry, incomplete coverage, or
ambiguous provenance is `WITHDRAWN`. Candidate connection establishment uses
the existing `path_probe_timeout_ms`. Each endpoint starts an independent,
absolute session-retention ceiling when it locally admits
`TCP_CARRIER_VALIDATE`; progress never extends it and expiry grants no
authority. Sender expiry before result serialization emits `WITHDRAWN` when
the candidate remains writable, otherwise exact native failure settles it. A
receiver-side client expiry starts ordered candidate drain; a receiver-side
server expiry closes the exact candidate natively because the server cannot
initiate `PATH_DRAIN` or a sender-owned result. Expiry after immutable result
serialization retires the unacknowledged candidate without changing that
result. This version defines no new timer, percentage, EWMA coefficient,
statistical confidence threshold, or reopening interval.

`NO_GAIN`, `WITHDRAWN`, candidate failure, and result settlement do not
authorize another connection in the same admission generation. A later
attempt requires fresh generation authority and remains subject to the
connection-attempt rate, configured maximum, and session resource bounds.
Waiting, polling, ACK silence, ordinary queue oscillation, or a locator,
source, or interface change alone does not authorize it.

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

For response-direction feedback, a later MPP Data ACK event may authorize
bounded repair on one measured alternate after the original flight exceeds the
local MPP Data-ACK threshold:

- `5/4 * SRTT` for TCP; or
- `9/8 * SRTT` for QUIC;

provided the alternative is estimated to complete before the MPP recovery
interval. The repair may fill the alternate's available throughput-lane Product
service window, bounded by exact omitted ranges and the configured repair and
path-flight envelopes. Existing target flight and queued Product work consume
that window. Queue and flight are summed within the Product and native carrier
domains, while the overlapping domain totals are counted only once; one repair
quantum remains available for liveness when the window is full. This is Data
Sequence service authority, not native congestion
authority: the selected TCP or QUIC sender remains the final enqueue, pacing,
congestion, and recovery authority. These ratios are local approximations
inspired by transport time-threshold loss detection; the TCP ratio is not RFC
8985 RACK and the QUIC ratio is not QUIC's native RFC 9002 loss decision. ACK
silence alone waits one MPP recovery interval.

When a persistent-gap reinjection attempt is accepted by a selected alternate,
its repeat deadline is fixed from that alternate's observed MPP recovery
interval. The stream actor wakes at the earlier of that deadline and the
original owner's live-tail deadline. If the same authoritative gap remains, a
later attempt reselects a currently measured alternate and again remains
bounded by its available Product service window. Advancement or resolution of
the lowest missing frontier clears the prior attempt deadline. Thus a degraded
original owner cannot impose its longer recovery clock on an already accepted
reinjection attempt, while mutable later measurements cannot postpone that
attempt's deadline. ACK receipt, either recovery deadline, carrier-capacity
release, and output-model publication all return through the same stream-owner
evaluation of the retained authoritative gap. Only a newly received Data ACK
may arm the separate original-owner silence fallback; polling or another wake
cannot restart that silence clock.

A contiguous live tail without an authoritative gap may send one bounded probe
after one MPP recovery interval. Another repair requires another full interval
without MPP Data ACK progress.

When another non-stale attachment is available, original placement in either
direction stops selecting a non-progressing attachment after four TCP MPP
recovery intervals or three QUIC MPP recovery intervals. Every exact
unacknowledged OriginalData range owned by that stale attachment then becomes
connection-level reinjection work on a distinct non-stale attachment. The work
is admitted through the existing shared receive-credit, retained-range, repair,
queue, and native-enqueue bounds.

Recovery suppression is exact-range state, not one clock for the complete
owner. Acceptance of a recovery copy by an exact carrier command queue covers
only that copy's Data Sequence range for one owning-path MPP recovery interval.
It MUST NOT delay a disjoint stale-owned range that has no current recovery
copy. If the exact range remains unacknowledged when that interval expires, the
range becomes eligible again and MAY reuse the same non-stale survivor; the
survivor need only remain distinct from the stale original attachment. The
earliest current range expiry is an actor wake deadline. Thus every range is
retried no more than once per owning path's MPP recovery interval until MPP
Data ACK covers it, while the existing queue, flight, repair, and extra-traffic
bounds limit aggregate work. Exact unambiguous MPP Data ACK progress on the
stale attachment makes it eligible for original placement again. The carrier
remains connected and native recovery continues throughout.

The persistence clock is independent for every exact attachment incarnation
that owns OriginalData omitted below the authoritative Data ACK horizon.
Progress or gap repair on another attachment, and movement of the stream's
lowest missing frontier between attachments, MUST NOT restart that clock.
Only exact unambiguous OriginalData ACK progress on the same attachment
restarts it. The clock is removed when that attachment has no authoritative
outstanding OriginalData or no non-stale alternative. Attachment staleness is
stream-local; only exact carrier-instance failure is session-wide.

If cumulative extra-traffic credit is exhausted, an exact carrier failure or
persistent authoritative gap may use one bounded target Product service window
to prevent the budget itself from deadlocking recovery. A live-tail event
without an authoritative gap remains limited to one critical recovery quantum.
Both are bounded by retained ranges, exact flight identity, queue and flight
limits, repeat-delay suppression, and a distinct output while the original
carrier is live. Their bytes remain charged and reduce later optional
reinjection authority.

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
22. Server `PATH_CLOSE` is the ordered aggregate acknowledgment of a matching
    client `PATH_DRAIN`; local emptiness or write completion cannot replace it.
23. `SESSION_CLOSE` retires the complete `SessionId`; carrier drain does not.
24. Attachment membership and feedback capability alone grant no ordinary TCP
    payload authority; validation placement requires its exact bounded
    directional binding and validation work.
25. In either direction, the exact `RETAIN` acknowledgment is received before
    the sender may place ordinary Product payload on the candidate.
26. Configured capacity that has not established a physical carrier is
    not an eligible carrier, path attachment, or health state.
27. One client session service reconciles each TCP carrier group; each exact
    carrier instance has its own wire lifecycle, while a stream never owns
    group capacity or replacement.
28. A configured-minimum member identity is client-local and persists across
    its successive replacement instances. A planned replacement permits only
    the bounded current/successor/predecessor overlap and exact
    Product-quiescent commit in Section 7.2.
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
transaction is independently defined as client `PATH_DRAIN` followed by server
`PATH_CLOSE`.

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
