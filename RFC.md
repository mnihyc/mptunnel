# MPTunnel Multipath Proxy Protocol (MPP) Version 9

## 1. Status and Conventions

This document specifies MPP version 9: its wire format, carrier profiles,
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

Wire version 9 is identified by the frame header in Section 12. A peer MUST
reject every unsupported frame version. This version has no downgrade or
compatibility mode.

Product routing, DNS policy, outbound selection, balancing between separate
MPP sessions, VPN device integration, configuration, management APIs,
operator presentation, packaging, and platform adapters are outside this
protocol specification. They MUST NOT redefine the identities or ownership
rules specified here.

## 2. Scope and Non-Goals

MPP carries application byte streams, datagrams, and complete IP packets over one or more
authenticated transport connections. A carrier uses TCP or QUIC over UDP.
TCP reliability, congestion control, retransmission, and packetization remain
owned by the TCP stack. QUIC retains packetization, congestion control, loss
recovery, address validation, migration, and path MTU discovery. Section 6
defines the default carrier protection and the optional shared-transport-secret
profile; neither profile changes that native transport ownership.

MPP owns the data level above those transports:

- authenticated session, stream, datagram-flow, IP-tunnel, and MPP Path ID namespaces;
- a separate absolute offset space for each direction of each reliable stream;
- ordered delivery, deduplication, MPP Data ACK ranges, and shared receive
  credit;
- stable identity for original transmissions and reinjected copies;
- selection among eligible carriers;
- bounded cross-carrier reinjection and failover; and
- application demand used as a mutable scheduling objective.

The optional IP packet service is a distinct data plane. It forwards complete
IPv4 and IPv6 packets without terminating their transport protocols. MPP does
not acknowledge or retransmit those packets above a carrier and does not
install routes, DNS policy, firewall rules, or NAT state for them.

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
: The client-local bounded pool derived from one configured TCP endpoint within
  one MPP session. The group owns durable member ordinals from zero through
  `maximum - 1`; each exact member actor independently owns its socket, wire
  ordering, readiness, drain, failure, and terminal release. Group identity,
  maximum, and member ordinals are local configuration state and are never sent
  or inferred from locators. Member ordinal zero is the configured endpoint's
  primary member; greater ordinals are its correlated siblings.

**IP tunnel**
: One authenticated layer-3 packet service identified by an `IpTunnelId`.
  One logical tunnel may have an attachment on multiple carrier instances.
  Its assigned addresses and routed-prefix ownership come from server policy,
  never from an outer locator or an address claimed by a packet.

**IP tunnel attachment**
: One IP tunnel's membership on one exact carrier instance. Failure or ordered
  retirement of that carrier removes only that attachment.

**Packet flow**
: A direction-local stable classification derived from an inner IP packet for
  carrier affinity. It is local scheduling state and is not a wire identity.

**IP packet admission**
: The local, directional handoff that either retains one complete IP packet
  through final carrier enqueue, rejects the current packet because the
  bounded packet envelope is full, or reports that the selected exact carrier
  retired before acceptance. Admission is not delivery acknowledgment or
  congestion control.

## 4. Architecture and Authority

The ownership boundary is:

```text
application
    MPP reliable stream
        per-direction offsets, Data ACK, shared credit, bounded reinjection
            regular-before-backup carrier selection
                TCP controller | QUIC controller
                    network

application datagram | IP packet service
    bounded datagram/packet admission and carrier selection
        TCP controller | QUIC controller
            network
```

One runtime generation MUST activate exactly one forwarding family. The L4
family admits reliable streams and application datagrams and MUST NOT construct
an IP-tunnel service. The L3 family admits IP-tunnel attachments and MUST reject
reliable-stream and application-datagram opens before they enter L4 egress
services. Carrier authentication, path control, and liveness remain common.
This selection is local generation configuration, defaults to L4, and is never
serialized or inferred from traffic.

### 4.1 MPP authority

MPP owns stream, datagram, and IP-tunnel identity, offset assignment, Data ACK processing,
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
randomness. `port-rotation-interval-s` is an earliest maintenance interval,
not a failure detector, validation deadline, or capacity signal. Its Product
default is `300` seconds and values below `5` seconds are rejected, matching the
established minimum used by deployed QUIC port-hopping practice while avoiding
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
adds no MPP wire field. The port is a locator, not an evidence epoch: MPP MUST
NOT cold-start, copy, or relabel carrier evidence merely because that locator
changed. Configured startup hints remain properties of the logical path, while
the retained connection's live native state remains exclusively under QUIC's
validation and recovery rules. If QUIC instead establishes a replacement
connection, it creates a new carrier instance; only configured hints survive,
and predecessor measurements, queues, flight, ACK ownership, and sample
authority MUST NOT cross into it. Changing a TCP destination port likewise
requires a new TCP connection and therefore a new carrier instance. TCP hopping
follows the bounded pool replacement rules in Section 7.2; the maintenance
interval never authorizes state transfer or aggressive retirement.

### 4.4 Product payload-idle lifetime

An L4 endpoint MAY configure one Product payload-idle lifetime through
`[flow].idle_timeout_s`. The Core Profile default is 300 seconds; zero disables
this lifetime. It applies equally to reliable streams and application-datagram
associations, independently of the selected direct, proxy, or MPP egress.

One or more reliable-stream bytes, or one fresh application datagram including
a zero-length datagram, accepted in either direction refreshes the local
lifetime. TCP FIN, zero-length stream I/O, acknowledgements, MPP or QUIC
control traffic, carrier heartbeats, keep-alives, probes, retransmission, and
recovery MUST NOT refresh it. A half-closed reliable stream MAY continue
carrying payload in its remaining direction; the half-close alone neither
retires nor keeps the stream alive.

Deadline handling MUST linearize accepted payload with retirement. Payload
recorded before the retirement commit refreshes the lifetime; once retirement
commits, a late producer cannot revive that incarnation. Cancellation of an
owner task MUST NOT strand its admission, attachment, route, or cleanup state;
terminal cleanup remains exact and idempotent.

Expiry terminates that exact Product flow and releases its admission,
telemetry, attachment, and datagram-route ownership. On MPP egress the endpoint
uses the ordinary stream reset or datagram close path. Expiry MUST NOT by itself
mark a carrier or session failed, transfer authority, or restart a carrierless
retention epoch. Product payload-idle lifetime, carrier liveness, and
`[session].retention_timeout_s` are independent local clocks and endpoints MUST
NOT assume that peer values or deadlines are equal.

## 5. Session and Carrier Establishment

### 5.1 Transport authentication

Every carrier MUST complete transport protection before MPP data admission:

- A TCP carrier without a shared transport secret MUST complete TLS 1.3 with
  no early data and no negotiated ALPN.
- A TCP carrier with a shared transport secret MUST complete the Noise profile
  in Section 6.1 before sending the MPP admission prelude.
- QUIC MUST complete its TLS handshake, negotiate exactly `h3`, and disable
  0-RTT for MPP carrier requests. A QUIC carrier with a shared transport secret
  MUST first authenticate its Initial packets as specified in Section 6.2.

QUIC and default-profile TCP MUST authenticate the configured TLS server
identity. Shared-secret TCP mutually authenticates possession of the endpoint
transport secret; because this is a group PSK, any holder can impersonate
either transport endpoint. The certificate, private key, client trust policy,
and shared transport secret are independent from every MPP application
credential. An MPP credential MUST NOT derive or replace any of them, and a
shared transport secret MUST NOT be used as an MPP client credential.

The shared-secret profile is selected only by matching out-of-band endpoint
configuration. It has no wire negotiation, downgrade signal, or fallback. A
configured endpoint MUST use that profile exclusively; authentication failure
MUST NOT retry the default profile on the same or a new carrier attempt.

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
"mptunnel session auth v9" ||
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
transport_binding:32B ||
session_id:u64 ||
credential_id_length:u8 || credential_id:bytes ||
nonce:16B ||
issued_at_unix_secs:u64
```

The common `PATH_JOIN` transcript is:

```text
"mptunnel path join v9" ||
session_id:u64 ||
credential_id_length:u8 || credential_id:bytes ||
path_id:u16 || underlay:u8 ||
nonce:16B ||
issued_at_unix_secs:u64
```

The receiver MUST:

- resolve the canonical credential ID;
- reject an unknown, revoked, expired, or unauthorized credential;
- validate timestamp freshness, session identity, credential identity,
  `PathId`, underlay, nonce, and tag;
- reject replayed authentication and path-join nonces within the configured
  freshness boundary;
- require `PATH_JOIN` identities to equal the carrier-authenticated
  identities; and
- avoid disclosing which unauthenticated check failed.

TCP additionally validates the fixed prelude fields, canonical zero padding,
and transport-bound tag. Credential lookup and policy evaluation occur at
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

An implementation MAY retain replay authority across a clean runtime
configuration replacement only when the transport secret, TLS identity,
inbound identity, protocol settings, freshness window, and replay-capacity
policy are unchanged. This state is endpoint authority and MUST NOT be selected
or partitioned by source IP. A change to any identity or policy starts a new
boundary.

### 5.3 Readiness and identity fences

After accepting carrier authentication, `PATH_JOIN`, and sequence-zero
`PATH_STATUS`, the receiver sends `SESSION_READY` and its own sequence-zero
`PATH_STATUS`. Product stream or datagram work MUST NOT be admitted until the
initiator has received both readiness frames. Every ready carrier may
participate according to its directional usage; usage remains the receiver
preference in `PATH_STATUS`, not a carrier-admission phase.

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

### 6.1 TCP carrier protection

The default TCP profile uses TLS 1.3, negotiates no ALPN, and accepts no early
data. Both endpoints export exactly 32 bytes from the completed connection
using `EXPORTER-mptunnel-tcp-admission-v1` with no exporter context. An early
exporter MUST NOT be used. This value is the `transport_binding` in Section
5.2.

The optional shared-secret TCP profile uses
`Noise_NNpsk0_25519_AESGCM_SHA256` with the prologue
`mptunnel tcp carrier v1`. The endpoint-wide input MUST be exactly 32 random
bytes. Define:

```text
K(label, context) = HMAC-SHA256(transport_secret, label || context)
```

The Noise PSK is `K("mptunnel tcp noise psk v1", empty)`. The initiator
handshake payload is:

```text
0       transport-profile version   u8; 1
1..9    issued_at_unix_secs          u64
9..25   nonce                        16 random bytes
25..N   padding                      8 through 63 random bytes
```

The responder handshake payload contains only 8 through 63 random padding
bytes. Padding length is selected uniformly. MPP identities, credentials, and
frames MUST NOT appear in either Noise handshake payload.

Each Noise handshake message is carried as its 32-byte ephemeral key, followed
by a masked 16-bit network-order remaining length, followed by the remainder
of that Noise message. For direction label `L`, the length mask is the first
16 bits of:

```text
HMAC-SHA256(K(L, empty), L || ephemeral_key)
```

`L` is `mptunnel noise client handshake length v1` for the initiator flight
and `mptunnel noise server handshake length v1` for the responder flight. The
complete initiator Noise message length MUST be 81 through 136 bytes; the
complete responder Noise message length MUST be 56 through 111 bytes.

The responder MUST authenticate the complete initiator Noise message, validate
its version and issue time against the configured authentication freshness
window, and atomically admit its nonce to a bounded endpoint-local replay cache
before writing any bytes. Every failure before the responder flight—including
an incomplete header or body, invalid decoded length, malformed, stale, future,
duplicate, or capacity-exceeding input—MUST send no bytes. Once such a failure
is final, it MUST release authentication-work capacity immediately. A separate
endpoint-local silent-rejection budget, equal to the configured pending-
authentication capacity in this profile, retains only the rejected socket
until the original absolute authentication deadline. While that budget is
available, failures remain externally indistinguishable until the deadline. If
it is exhausted, the implementation MUST shed the rejected socket immediately,
without a response and without delaying valid authentication. A valid flight
proceeds immediately and never waits for silent-rejection capacity. Parsing,
storage, authentication work, and retained rejected sockets all remain bounded;
neither budget is selected or partitioned by source IP.
Failure of local clock or replay authority is an endpoint fault, not a peer
rejection, and MUST remain operator-visible and fatal to that admission.

Replay state is shared by every carrier created from that endpoint
configuration. It MAY survive a clean generation replacement under the exact
identity and policy conditions in Section 5.2. Process restart or an
independent process creates a new replay boundary unless the host supplies
shared durable replay state. Reusing one transport secret across independent
MPP inbounds therefore also creates independent replay boundaries and is NOT
RECOMMENDED.

Let `h` be the completed Noise handshake hash. The Noise profile's
`transport_binding` is:

```text
K("mptunnel noise admission binding v1", h)
```

Noise application data is a sequence of records. Each record carries a masked
16-bit ciphertext length followed by AES-GCM ciphertext and tag. Directional
length keys are `K("mptunnel noise client record length v1", h)` and
`K("mptunnel noise server record length v1", h)`. For record nonce `n`, the
mask is the first 16 bits of:

```text
HMAC-SHA256(direction_length_key,
            "mptunnel noise record header v1" || n:u64)
```

Plaintext is split into records of at most 65519 bytes. Empty records are
invalid; ciphertext length is 17 through 65535 bytes. Nonces begin at zero and
increase without reset. Before processing each nonzero nonce divisible by
`2^20`, the sender rekeys its outgoing Noise cipher and the receiver rekeys its
corresponding incoming cipher. An incomplete write or read makes that
direction terminal.

After either carrier-protection handshake, the initiator sends exactly one
131-byte admission prelude before any MPP frame:

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

The prelude is carrier-admission data, not an MPP frame, and does not begin
with `MPTF`. It is followed immediately by `PATH_JOIN` and sequence-zero
`PATH_STATUS`. Carrier record boundaries and write batching do not change this
ordering.

The listener reads one complete prelude before interpreting its fields. An
incomplete or rejected prelude, carrier-protection failure, admission timeout,
or read failure MUST close without application response bytes or an
MPP-specific close reason.
After successful authentication, ordinary MPP protocol errors follow
Section 13.

One TCP carrier instance multiplexes path control, stream attachments, and
datagram-flow attachments. `PING` and `PONG` may provide MPP-level heartbeat.
The TCP client alone initiates an idle heartbeat. The configured heartbeat
interval `I` is the maximum idle delay, not a periodic wire cadence. At
connection start and after each completed idle heartbeat, the client selects a
fresh cryptographically random delay uniformly from `[0.8I, I]`. Authenticated
traffic defers the current delay without drawing another value; it MUST NOT
extend an outstanding `PONG` deadline. A late wake coalesces missed timer work
into at most one probe and never emits a catch-up burst. Thus the maximum
last-activity-to-failure bound remains `I` plus the configured heartbeat
timeout.
`PATH_DRAIN` and `PATH_CLOSE` are valid only on TCP carriers. The TCP carrier
client alone sends `PATH_DRAIN`; the TCP carrier server sends `PATH_CLOSE` only
as the response that completes that drain. Their `path_id` MUST match the TCP
carrier carrying the frame. A frame sent in the opposite direction, or a
`PATH_CLOSE` without a matching `PATH_DRAIN`, is a carrier protocol violation.
After drain begins, both endpoints MUST make that carrier ineligible for new
attachments and original placement while retaining receive, Data ACK,
recovery, and ordered-control processing.

The server sends `PATH_CLOSE` only after every earlier frame from the client
has been applied and the exact carrier has no attachment, datagram
binding, queued or retained frame, original or reinjected flight, pending Data
ACK, path proof, or capacity work. All server frames that complete
carrier-owned work MUST precede
`PATH_CLOSE` in the TCP byte stream. The client treats receipt of
`PATH_CLOSE`, not its own write completion or local emptiness, as the aggregate
retirement acknowledgment. It removes the carrier only after applying every
preceding server frame and reaching the same local zero-work condition.
Native failure before that boundary uses ordinary retained-state recovery.

The client starts a local absolute graceful-retirement ceiling when it closes
new carrier admission and begins local drain; the server starts its own when
it receives `PATH_DRAIN`. For the server, receipt is the authenticated frame
decode boundary, before delivery through any bounded actor queue: writer,
routing, or queue backpressure MUST NOT defer closure of new Product admission
or the start of this ceiling. Each uses the configured
`[session].retention_timeout_s`, never restarts or extends it, and does not
assume that the peer's deadline is equal or synchronized. Expiry closes that
exact native TCP carrier and enters ordinary exact-failure recovery; it does
not synthesize `PATH_CLOSE`.

The same configured duration is the Product resource-lifetime ceiling for
carrierless stream retention and graceful carrier retirement. These are
independent absolute lifetimes; progress in one never restarts another. The
duration is not carrier health, delivery, Product service, or pool-sizing
evidence.

`PATH_CAPACITY_DATA`, `PATH_CAPACITY_FINISH`, and
`PATH_CAPACITY_RECEIPT` are valid only on TCP carriers.

### 6.2 QUIC over HTTP/3

Without a shared transport secret, QUIC Initial keys follow RFC 9001. With a
shared transport secret, define:

```text
private_initial_secret =
  HMAC-SHA256(transport_secret,
              "mptunnel quic private initial key v1")
private_initial_input =
  "mptunnel quic private initial v1" ||
  private_initial_secret || destination_connection_id
```

The RFC 9001 version-specific Initial key schedule MUST use
`private_initial_input` in place of the public destination-connection-ID input.
This changes only Initial packet protection. QUIC version, long-header shape,
connection IDs, minimum datagram size, the later TLS handshake, and every
subsequent QUIC key space remain transport-owned and otherwise unchanged.

Before authenticating a private Initial, a server MUST NOT emit Version
Negotiation, Initial close, Retry, stateless reset, TLS certificate flight, or
other response bytes. A public RFC 9001 or wrong-secret Initial is silently
dropped. After successful private Initial authentication, ordinary QUIC
validation and error behavior apply.

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
behavior after private-Initial authentication when that profile is configured.

MPP frames carried in HTTP/3 DATA are each prefixed by their encoded length as
an unsigned 32-bit network-order integer. HTTP/3 DATA boundaries are
independent of MPP record boundaries. A receiver MUST enforce its frame limit
before buffering a declared record.

QUIC native liveness and connection retirement remain transport-owned. The
QUIC client alone enables native keep-alive. Its configured interval has the
same maximum-delay and `[0.8I, I]` renewal semantics as the TCP heartbeat; an
authenticated packet or send defers the current delay, a fired keep-alive draws
the next delay, and a late wake emits no catch-up sequence. The server relies on
the client's PING and its transport ACK to keep both idle timers live.
`PING` and `PONG` on a QUIC request stream may prove MPP response-direction
reachability, but they do not govern QUIC connection liveness or retirement.
`PATH_DRAIN`, `PATH_CLOSE`, and all `PATH_CAPACITY_*` frames are invalid on a
QUIC carrier.

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

### 6.4 Native IP Packets

An admitted IP tunnel uses the same RFC 9297 request-stream association and
negotiation requirements as Section 6.3. The payload following the Quarter
Stream ID is:

```text
0       extension version       u8; 2
1..9    tunnel_id               u64
9..17   packet_id               u64
17..19  fragment_index          u16; zero based
19..21  fragment_count          u16; 1 through 64
21..25  total_payload_length    u32; nonzero
25..    fragment payload
```

Multibyte fields use network byte order. Version 2 is distinct from the
version-1 application-datagram envelope; an implementation MUST NOT reinterpret
either envelope as the other. The sender fragments against the current maximum
QUIC datagram size and MUST reject a packet requiring more than 64 fragments.

`OPEN_IP_TUNNEL` and `IP_TUNNEL_READY` MUST complete their reliable transitions
before either direction sends `IP_PACKET` on that request association. Because
a QUIC Datagram may overtake reliable DATA already submitted by its sender, a
receiver MAY retain a complete or partial version-2 packet until the matching
ready transition for no longer than:

```text
clamp(2 * current QUIC RTT, 25 ms, 250 ms)
```

That receiver-owned interval starts when the first fragment is received. It
also bounds incomplete reassembly. Route, packet, byte, association, fragment,
and reassembly counts remain bounded, and expiry releases every resource
charge. A packet for a closed or different tunnel association is silently
dropped. Native IP delivery adds no MPP acknowledgment, retransmission,
ordering, or flow control.

## 7. Carrier Lifecycle and Directional Usage

### 7.1 Carrier lifecycle

Carrier establishment, readiness, drain, close, native transport failure, and
local policy changes are distinct events. Local states such as active,
suspect, draining, failed, disabled, and cooldown are not peer scheduling
values.

Runtime-disable is client-local group admission control with a carrier-wide
wire consequence. It suspends pool reconciliation, forbids new establishment,
makes every group carrier locally ineligible for new original placement, and
requests ordered retirement of every exact carrier in the group. Ordered
`PATH_DRAIN` makes the peer stop new placement in its direction as well,
preserves delivery and recovery ownership for bounded existing flight, and
ends at the exact `PATH_CLOSE` boundary. Disable therefore does not pretend
that client-local policy can silently keep the carrier usable.

Re-enabling creates a new establishment-policy generation and reconciles fresh
bounded-pool carrier instances. It does not cancel or reuse a drain
already begun, and no attachment, authority, queue, flight, or evidence
transfers from a disabled instance. An in-progress pre-readiness connection
from an older policy generation cannot publish afterward.

Removing a TCP carrier group makes the client retire each exact carrier through
the ordered `PATH_DRAIN`/`PATH_CLOSE` procedure. Re-adding it creates new group
and policy generations and does not cancel a drain already begun.
Disable, removal, and re-add use the client-local group identity and MUST NOT
use a source address, locator, interface, `PathId`, or peer `PATH_STATUS` as
group identity.

A maximum change MUST NOT retroactively reclassify a live carrier. Decreasing
the maximum below occupied physical reservations remains unapplied until
ordered retirement makes the target reachable. No live carrier is hidden or
force-closed merely to make configuration state appear applied.

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

### 7.2 Bounded TCP carrier pools

A client configures a TCP carrier group with the current `MIN-MAX` grammar;
the Product default is `1-3`. `MIN` is obsolete: it is range-validated for the
accepted grammar but has no protocol or runtime effect. Only `MAX` controls
carrier count. A future revision may remove `MIN` or restore elastic behavior
through a proven algorithm; this revision MUST NOT use it for establishment,
scheduling, restoration, or retirement.

The maximum is the healthy pool target and the hard bound on simultaneously
establishing, ready, replacing, and draining physical carriers owned by the
group. While the group and session are enabled, one client session owner
reconciles durable member ordinals `0` through `MAX - 1` toward that target.
Missing members MAY establish concurrently. Every connection performs a fresh
TLS handshake, TCP admission prelude, `PATH_JOIN`, sequence-zero
`PATH_STATUS`, and readiness exchange. No performance comparison or
directional promotion transaction is required before readiness.

Usage follows configured topology, not a measured-throughput threshold. When
an MPP outbound contains one configured TCP carrier group, every ready member
retains that endpoint's configured usage so the bounded pool can overcome
per-flow policing or independent native TCP loss history. When the outbound
contains multiple configured TCP groups, member ordinal zero of each group
retains the endpoint's configured usage and greater member ordinals are locally
reserved as `BACKUP`. The client advertises the same sibling preference for
the peer direction. An endpoint explicitly configured as backup remains backup
for every member. Thus separately configured endpoint primaries are considered
before their correlated siblings without inferring a source address, interface,
or physical bottleneck.

One physical carrier consumes one group reservation and one session-unique TCP
`PathId` from connection initiation. Pre-readiness connection,
authentication, or policy-generation failure releases both. After readiness,
only the exact ordered `PATH_CLOSE` or exact native failure releases them.
The configured maximum bounds current carrier members. Planned maintenance may
add exactly one transient successor reservation per group; it is not a fourth
schedulable member of a three-member group and a second member cannot create a
second overlap. The endpoint's ordinary session path limit still counts that
successor, and the receiver MUST NOT relax its authenticated per-session or
global carrier admission limits for a claimed replacement.

A member ordinal names durable configured pool capacity, not a physical
connection. It normally owns one current exact carrier instance. A planned
replacement may temporarily own an authenticated successor and a retiring
predecessor under that sole group-scoped transient reservation. The client
publishes the fresh `PathId` and carrier instance only if the predecessor is
still current, then fences the predecessor from new placement and begins its
ordered drain. Product work committed before that atomic publication remains
fenced to the predecessor and follows ordinary detachment, recovery, and
reattachment; later work observes only the successor. No attachment, queue,
flight, native transport evidence, or scheduling state transfers between the
two instances.

Immutable configured startup RTT, jitter, and rate hints remain properties of
the logical member. The successor's own authenticated readiness exchange MAY
replace the configured RTT in its connection-local startup hint. Live
predecessor measurements, sample authority, and native TCP state MUST NOT be
carried into the successor.

Exact native failure removes only the failed instance and immediately wakes
pool reconciliation. Restoration never waits for a throughput observation,
source-address change, interface event, or application retry. Repeated
connection attempts remain bounded by the existing establishment policy.
Operation-local stream failure, cancellation, or timeout never owns pool
capacity and never classifies another member as failed.

Every ready member independently owns health, transport measurements, queues,
attachments, native congestion control, and failure scope. The scheduler
ranks exact carriers within the regular or backup eligible set selected by
Section 7.3 and may leave a redundant member idle. Pool membership never
forces payload duplication and introduces no group-specific pacing or
congestion controller.

When eligible carriers otherwise have equal evidence, the client's configured-
order fallback visits one member ordinal across every configured endpoint
before visiting the next member ordinal. This prevents redundant members of an
earlier endpoint from displacing distinct configured endpoints during
evidence-free startup. The order is not link identity, capacity evidence, a
traffic share, or a common-bottleneck inference; measured completion evidence
remains authoritative.

Because only `MAX` is effective, one group configured `3-3` and one group
configured `1-3` expose the same three ready carriers. Three otherwise
identical `1-1` groups instead expose three separately configured primaries:
all three retain their configured usage, while siblings in a multi-group pool
are backup capacity. A pooled range shares endpoint enablement, credentials,
and locator rotation; explicit endpoints remain independent control domains.
Three omitted default ranges request three pools of three carriers, not one
three-carrier pool.

Planned maintenance selects the earliest-due healthy member and rotates at most
one member per group at a time. A successful replacement receives a fresh
deadline, so no ordinal is systematically retained. Replacement authenticates
the sole transient successor before publishing it and before draining the
predecessor. A failed or stale successor is discarded, leaves the predecessor
authoritative, releases the transient reservation, and defers another planned
attempt by the complete maintenance interval. Failure handling remains
immediate and is not delayed by planned retirement.

A maximum change MUST preserve exact live-instance identity. Increasing it
creates fresh member ordinals. Decreasing it drains surplus members in
descending ordinal order. A change that cannot yet fit the physical envelope
remains unapplied until ordered retirement reaches the requested maximum.

Idle ready members are event-driven and retain only their bounded socket,
actor, TLS, heartbeat, and path state. An implementation MUST NOT create an
unbounded number of carriers or use a fixed Mbps, percentage, source address,
interface identity, or laboratory threshold to size the pool.
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
`STREAM_MAX_DATA` from that direction's receiver. The first logical open has
two distinct phases: exact-carrier attachment admission and Product-target
establishment. After accepting a new stream identity, the receiver MUST enqueue
`STREAM_MAX_DATA(stream_id, 0)` on the opening carrier before submitting DNS,
routing, or target-connect work. Once that admission commits, the independent
target owner queues any required path-validation challenge on the same ordered
carrier output before beginning that work. Ordinary bounded-queue backpressure
may delay the challenge, but MUST NOT revoke the admitted attachment, discard
the target owner, or allocate another `StreamId`. Carrier-local failure while
queuing the challenge remains attachment-local; path validation retains its
ordinary carrier lifecycle and retry semantics.

For the initial open, the zero grant acknowledges only the carrier attachment;
the logical open remains pending. It is not evidence that routing or target
connect succeeded. After receiving it, the sender MUST NOT charge subsequent
target-establishment delay to that carrier's PTO or publish carrier failure
because that logical work is slow. The endpoint's logical Product-open deadline
still bounds the operation. A concrete attachment refusal or carrier failure
MAY select another carrier, but every retry MUST reuse the same `StreamId`.

The receiver owns exactly one target-establishment operation for one
`(SessionId, StreamId)`. The original target, initial demand, authenticated
principal, and opening ingress remain immutable. A matching repeated
`OPEN_STREAM` while establishment is pending adds an attachment and MUST NOT
create a second target connection. When target establishment succeeds, the
receiver advances the retained grant to a nonzero value and publishes it to
every live attachment. An attachment added after establishment receives its
credit-neutral zero admission followed by the retained nonzero grant. A target
failure terminates the logical stream once across all attachments; an explicit
silent-drop policy MAY instead retire it without a refusal frame.

For an additional attachment, the zero grant is itself a complete attachment
acceptance because the sender retains the greatest logical grant already seen.
A receiver that refuses only the pending attachment sends `STREAM_DETACH` on
that carrier. `STREAM_RESET` is reserved for terminating the logical MPP stream
and MUST NOT be used to refuse an additional attachment.

Expiry, cancellation, or local rejection of a pending `OPEN_STREAM` settles
only that exact attachment attempt. If the open may already have entered the
carrier's ordered writer, settlement MUST preserve the normal
`STREAM_DETACH` ordering. The sender MAY reselect another attachment, but the
operation-local outcome MUST NOT by itself publish carrier-instance failure,
revoke carrier eligibility, discard exact-instance evidence, alter sibling
attachments, or release the carrier's endpoint-group reservation. The exact
operation's temporary scheduler-load reservation is settled normally.

Operation-local retry suppression MUST identify the exact carrier instance,
last no longer than one path-derived PTO, and apply consistently to every
attachment-selection path. After it expires, the same live carrier MAY own a
fresh attachment incarnation; a successor carrier instance bypasses it
immediately.

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
idle on a live attachment is not attachment loss; it may independently reach
the Product payload-idle lifetime in Section 4.4.

Loss of the last carrier is not `SESSION_CLOSE`. While the MPP session or any
retained stream or datagram state remains within its original configured
absolute retention lifetime, the client session service may establish
bounded-pool replacements with the same `SessionId` and fresh carrier
instances. Reattachment uses ordinary authenticated admission and attachment;
no authority, attachment, transport evidence, queue, or flight transfers from
a failed instance. Reconnect attempts MUST NOT extend any original retention
deadline.

Lack of Product progress while attachments remain live is stream-local
recovery evidence, not carrier failure. The sender first evaluates retained
ranges and the currently attached outputs. That first recovery cycle MUST NOT
infer loss beyond the receiver's authoritative complete-ACK horizon. If no
Product progress follows that bounded cycle, recovery MAY extend through the
current retained send extent and MAY attach the same logical stream to one
additional authenticated configured carrier that is not already attached. A
new recovery attachment MAY immediately carry that retained extent. At most
one such recovery attachment may be pending at a time; the configured
attachment and carrier bounds still apply, and Product progress ends the
expansion. This decision uses Product progress and exact attachment membership,
not source address, interface, or an inferred physical-link identity.

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

## 9. Datagrams and IP Packets

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

### 9.3 IP tunnel admission and ownership

`OPEN_IP_TUNNEL(tunnel_id)` attaches one authenticated carrier to a logical IP
tunnel. The server admits the request only when the carrier's authenticated
principal has an explicit allocation in that MPP inbound's address plan. It
replies with `IP_TUNNEL_READY(tunnel_id, mtu, addresses)` on that exact
attachment or `IP_TUNNEL_CLOSE(tunnel_id, POLICY_REJECTED)`.

An address plan contains server-owned IPv4 and/or IPv6 pools, the server's TUN
address in each enabled pool, and explicit principal-to-address allocations.
It MAY contain additional principal-owned prefixes for site routing. Pools and
prefixes are server configuration, not negotiated wire state. The server MUST
reject duplicate address ownership, cross-principal prefix overlap, an
allocation outside its pool, and any peer prefix containing a server address.
Credential rotation does not alter an allocation because ownership binds to
the authenticated principal, not to a credential ID or outer source address.

An MPP inbound admits at most one live logical IP tunnel for one principal.
Opening a different session or tunnel identity for that principal atomically
supersedes the previous attachment set. This is authenticated takeover for
restart and roaming; an outer source locator is neither consulted nor retained.

The ready address list contains at most one IPv4 and one IPv6 host address.
The receiver MUST NOT infer, publish, or install a route, DNS server, firewall
rule, or NAT rule from the ready frame. The MTU MUST be at least 576 and MUST
be at least 1280 when IPv6 is assigned. A client opens its packet device only
after this frame is accepted.

Every client-to-server packet MUST have a source address owned by the
authenticated principal. Every server-to-client packet MUST have a destination
address owned by that principal. The server drops a packet that fails parsing,
exceeds the negotiated MTU, or violates ownership. It MUST NOT use an outer
locator as peer identity and MUST NOT learn ownership from packet contents.

`IP_TUNNEL_CLOSE` removes only the tunnel attachment on the carrying carrier.
The logical tunnel remains available while another authenticated attachment
exists or can be established. Carrier loss has the same attachment effect
without requiring a close frame. A non-normal open rejection is local to that
exact carrier lifetime and MUST NOT be retried on the same carrier instance.
Lifecycle close delivery MUST NOT be discarded or blocked indefinitely behind
packet-payload queue pressure.

When the attachment set makes a true non-empty-to-empty transition, an endpoint
that retains the carrierless logical tunnel starts one absolute retention epoch
using `[session].retention_timeout_s`. A successful empty-to-non-empty
reattachment cancels that epoch. A failed, refused, duplicate, superseded, or
stale open or close does not start, restart, or extend it. If the deadline is
reached while the same principal, session, tunnel incarnation, retention epoch,
and empty attachment set remain current, the endpoint destroys that logical
tunnel. The server then releases the tunnel's retained authenticated-session
and principal-allocation ownership. A later attachment creates a new logical
incarnation through ordinary admission; it does not revive expired state.

### 9.4 IP packet delivery and carrier selection

`IP_PACKET(tunnel_id, packet_id, payload)` carries exactly one complete IPv4 or
IPv6 packet. `packet_id` is directional and monotonic within the logical
tunnel. It provides a bounded stale-handoff and duplicate-suppression identity;
it does not create MPP delivery acknowledgment, retransmission, ordering, or
flow control. The inner TTL or Hop Limit is forwarded unchanged.

On QUIC, `OPEN_IP_TUNNEL`, `IP_TUNNEL_READY`, and `IP_TUNNEL_CLOSE` use the
reliable request stream while `IP_PACKET` uses request-stream-associated QUIC
Datagrams and the bounded fragmentation envelope in Section 6.4. On TCP all
four frames use the carrier's ordered framing. TCP reliability therefore
applies to a TCP attachment; MPP MUST NOT add another retry or copy after the
frame is accepted by that carrier.

Each direction has one byte-bounded IP packet admission envelope shared by the
logical tunnel's attachments. Its bound MUST NOT be multiplied by attachment
count or derived from an application-stream record size. Packet lifecycle
commands use separate bounded headroom so packet pressure cannot prevent
attachment retirement or close processing.

Admission has exactly three outcomes:

- `Accepted`: the complete packet is retained until every byte has entered the
  selected carrier's final local queue;
- `Full`: the current packet is discarded before acceptance; and
- `Retired`: the exact carrier ceased to be usable before acceptance, so the
  sender MAY re-evaluate another eligible attachment once for that unaccepted
  packet.

An accepted packet MUST NOT later be displaced by a newer local packet. QUIC
therefore uses non-evicting native datagram admission: if its native send
buffer is full, the current IP packet is discarded and older queued packets
remain intact. TCP hands the frame to its existing bounded ordered carrier
queue. Recovery after either handoff belongs solely to the exact TCP or QUIC
transport. It does not create an MPP acknowledgment or authorize cross-carrier
retransmission.

Before the native QUIC datagram queue, an attachment retains at most the live
native QUIC congestion window of complete IP packets. This starts at QUIC's
initial flight, follows native path growth and contraction, and never exceeds
the shared directional byte envelope. TCP needs no duplicate handoff envelope
because its existing final ordered carrier queue performs admission. These are
packet-plane rules: they MUST NOT reuse or alter an application-stream record
count, reliable-stream window, or application-datagram admission rule.

TCP and QUIC attachments are equally eligible after authentication,
validation, readiness, usage, MTU, and queue admission. An implementation MUST
NOT select a carrier family from protocol name alone. Each direction selects
independently from current native rate, RTT, loss, congestion, queue, and
configured regular/backup evidence.

The packet scheduler SHOULD retain a healthy exact-carrier binding for one
inner flow to avoid transport-damaging reordering. It MAY reselect immediately
after exact carrier failure, retirement, MTU incompatibility, or terminal queue
loss, and MAY reselect at a flowlet boundary derived from current transport
timing. A `Full` result drops the current packet, preserves the healthy flow
binding, and does not prove path failure or authorize a duplicate. Opposite
directions have independent affinity, so asymmetric carriers may be selected
independently. Selection MAY include direction-local packet-flow load, but that
evidence MUST age with actual flow activity and MUST NOT be borrowed from
Product flow accounting. IP packet admission and its queue evidence are
independent of reliable-stream and application-datagram queues; changing them
MUST NOT change L4 proxy admission, retry, scheduling, or transport behavior.

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

Per-flow evidence MUST NOT be extrapolated into shared carrier capacity. A
fresh durable Product delivery sample on one output MAY establish only the
lower bound that the same carrier has just delivered at least that rate. It
MUST NOT be multiplied by flow count to infer aggregate capacity, used to
infer unused or marginal capacity, or retained past the transport-rate
freshness horizon. Only the directional
sender may combine per-stream events into an aggregate conclusion, and only
while the target demand, ordinary carrier membership and eligibility,
concurrent Product workload, and local admission and resource policy remain
unchanged. A configured rate is a startup prior, not measurement.

The transport-rate freshness deadline is fixed when the qualifying sample is
completed, using the RTT and RTT-variation evidence from that same sample
epoch. Later transport-shape or application-limited polls MUST NOT extend or
shorten that deadline. At or after the fixed deadline, the retained value MAY
be shown as stale diagnostic provenance but MUST NOT grant placement, pacing,
confidence, queue, window, or aggregate-rate authority. The first qualifying
sample completed at or after expiry starts a new published evidence epoch and
MUST NOT inherit the prior published epoch's sample count or bytes.

An unpublished native-delivery acquisition is not published authority and
grants none of those rights. Its publish and durable-volume floors are frozen
when its first qualified tuple is observed. A qualified tuple binds Product
bytes, sample count, and delivery-clock elapsed time from the same timed,
non-application-limited carrier observation; an implementation MAY omit an
ambiguously mixed application-limited observation but MUST NOT guess its
Product attribution. Tuples MAY aggregate only while both the network-path
epoch and the native non-application-limited delivery-clock epoch remain
unchanged. Aggregation MUST be invariant to management or scheduler polling and
to an instantaneous zero outstanding-byte snapshot. A network-path or native
delivery-clock epoch change terminates the unpublished acquisition.

Expiry clears the prior published deadline, committed confidence, and committed
byte coverage. It does not terminate a separate unpublished acquisition that
remains in the same path and native delivery-clock epochs. Such an acquisition
MAY begin before and complete after the old published deadline because it
inherits no count, bytes, freshness, or authority from the expired publication.
Only its qualifying completion starts a new freshness deadline. An
implementation MUST NOT reuse the expired publication's deadline as a hidden
acquisition timeout.

Fresh qualified native delivery evidence ranks carrier capacity. Product
per-flow completion evidence is the fallback when qualified native capacity is
unavailable; a Product sample that may itself have been limited by placement
MUST NOT cap an otherwise qualified native carrier. This provenance order is
identical in both stream directions and for every carrier underlay.

For TCP response placement, exact durable MPP Data ACK progress MAY raise a
fresh qualified native delivery observation to the demonstrated same-output
Product lower bound while both samples remain fresh. The lower bound is
completion evidence for that exact output in every placement role; changing
between leading, contiguous-frontier, and additional-output roles MUST NOT
rewrite its value or provenance. It MUST NOT be added to native delivery,
multiplied by flow count to infer aggregate capacity, or treated as proof of
unused, independent, or marginal carrier capacity. It does not replace the
native congestion controller or, by itself, bypass exact Data-ACK maturity,
native send credit and backpressure, shared receive credit, queue and flight,
completion-time, or reorder gates in Section 15. For conservative per-flow
completion ranking, an implementation MAY apportion this demonstrated lower
bound among concurrently active Product flows; that accounting does not prove
aggregate carrier capacity or bypass the additional-output admission gates.

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
metric_age_us:u32, rate_valid_for_us:u64, rate_observed:u8,
srtt_us:u32, rttvar_us:u32, jitter_us:u32,
delivery_rate_bps:u64, pacing_rate_bps:u64, pacing_rate_observed:u8,
loss_ppm:u32, ecn_ppm:u32,
loss_observed:u8, ecn_observed:u8, bytes_in_flight_observed:u8,
queue_observed:u8, bytes_in_flight:u64, queue_bytes:u64,
inflight_limit_bytes:u64, inflight_hi_bytes:u64, confidence_ppm:u32,
app_limited:u8, has_ack_derived_data_sample:u8, data_sample_count:u32,
data_sample_bytes:u64
```

The fixed `PATH_METRICS` record is 116 bytes. Offsets 24, 53, 64, and 65 from
the record start are respectively `rate_observed`, `pacing_rate_observed`,
`bytes_in_flight_observed`, and `queue_observed`; the corresponding flight and
queue 64-bit values begin at offsets 66 and 74. A peer-status path entry adds
one state byte and one usage byte and is therefore 118 bytes.

Metrics are advisory and scoped to the authenticated carrier instance and
direction. `bytes_in_flight_observed` and `queue_observed` independently state
whether their corresponding numeric field is an actual observation; a true
flag with numeric zero means observed zero, while a false flag means unknown.
An endpoint MUST NOT interpret a queue or flight value whose corresponding
flag is false as measured zero, native debt, native credit, or scheduling
authority.
`metric_age_us` is the saturating diagnostic age of the selected rate sample.
`rate_valid_for_us` is the remaining receiver-relative rate-authority budget
advertised with this record. A producer MUST derive it from the selected
sample's immutable freshness deadline, and every retained or forwarded copy
MUST only reduce it by local residence; it MUST NOT reconstruct or refresh the
budget from later RTT, RTT-variation, pacing, or application-limited shape.
A canonical value is at most `64,424,584,425` microseconds, the three-PTO
freshness budget obtained from the largest representable `srtt_us` and
`rttvar_us`; a decoder MUST reject a larger value and a local producer MUST cap
its value at this bound.
A zero budget grants no rate, pacing, or confidence authority while the raw
numeric values may remain diagnostic. Because endpoints do not share a
monotonic clock, this field is a remaining-duration grant beginning at receipt,
not a cross-host absolute deadline; transport time cannot increase the
advertised duration.

`rate_observed` is true when `delivery_rate_bps` belongs to a measured native,
Product, or generic delivery epoch and remains true after that epoch expires;
it is false for an unmeasured startup prior. Rate authority requires both
`rate_observed = true` and a nonzero remaining budget. A nonzero
`rate_valid_for_us` with `rate_observed = false` is noncanonical and MUST be
rejected. Product `has_ack_derived_data_sample`, `data_sample_count`, and
`data_sample_bytes` retain their stronger meanings and MUST NOT be synthesized
merely to mark a native TCP rate as observed. Native-carrier rate, pacing,
sample count, sample bytes, confidence, and application-limited qualification
MUST be projected from one retained immutable epoch rather than combining an
older delivery timestamp with a later shape poll.

`pacing_rate_observed` is true only when `pacing_rate_bps` is a native pacing
observation belonging to the same qualified carrier-rate epoch. When false,
the numeric field is only an internal delivery-rate fallback and MUST be shown
as unavailable and MUST NOT be attributed to native pacing.
`pacing_rate_observed = true` with `rate_observed = false` is noncanonical and
MUST be rejected. All observation flags are canonical one-byte booleans: only
zero and one are valid encodings.
They grant no stream offset, flight ownership, usage, health, or capacity.

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
output; it never establishes Product delivery, pool membership, or native
congestion authority.

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
4      version        9
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
| 38 | `OPEN_IP_TUNNEL` | `tunnel_id:u64` |
| 39 | `IP_TUNNEL_READY` | `tunnel_id:u64, mtu:u16, address_count:u8, addresses[address_count]` |
| 40 | `IP_PACKET` | `tunnel_id:u64, packet_id:u64, length:u32, bytes` |
| 41 | `IP_TUNNEL_CLOSE` | `tunnel_id:u64, reason:u8` |
| 42 | `STREAM_REQUALIFY_DATA` | `stream_id:u64, probe_id:u64, offset:u64, length:u32, bytes` |
| 43 | `STREAM_REQUALIFY_ACK` | `stream_id:u64, probe_id:u64, offset:u64, payload_bytes:u32` |

Kinds 5, 6, 15, 19, 25, 26, 28, and 29 are reserved and MUST NOT be sent.

`SESSION_HELLO` and `SESSION_AUTH` are QUIC carrier-admission frames; TCP uses
the Section 6.1 prelude. `PATH_DRAIN`, `PATH_CLOSE`, and kinds 33 through 35
are TCP-only. Receiving a carrier-incompatible frame is a
protocol violation. `PATH_DRAIN` is client-to-server only; `PATH_CLOSE` is
server-to-client only and requires a matching `PATH_DRAIN`.

Kinds 38 through 41 are valid only when the endpoint has enabled the IP packet
service. `OPEN_IP_TUNNEL` is client-to-server, `IP_TUNNEL_READY` is
server-to-client, and `IP_PACKET` and `IP_TUNNEL_CLOSE` are bidirectional.

Kinds 42 and 43 are the stream-directional requalification transaction from
Section 15.2. They are valid on TCP and QUIC and MUST name the stream attached
to the carrying authenticated carrier instance.

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

An assigned IP address begins with family `4` followed by four address bytes,
or family `6` followed by sixteen address bytes. A ready frame contains at
most one address of each family and at least one address.

Demand values are latency `1`, throughput `2`, and realtime `3`. Underlay
values are TCP `1` and UDP `2`. Directional wire fields use client-to-server
`1` and server-to-client `2`. Boolean fields use `0` or `1`.

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
- TCP pool-member actors, reconciliation and reconnect attempts, group
  reservations, `PathId` allocation, and drain work;
- TCP group removal and bound-reduction drains and carrierless-session
  retention; and
- all teardown and no-attachment retention, including carrierless logical IP
  tunnels and their authenticated-session ownership.

A protocol violation closes the smallest safe scope: product flow, stream,
carrier, or session according to the corrupted state. Authentication failure
must not admit durable product state. A carrier failure invalidates only state
owned by that carrier instance; a logical stream may continue on surviving
attachments.

An operation-local deadline, cancellation, refusal, or queue-admission
failure MUST NOT by itself be escalated into carrier-instance failure. Such an
outcome may settle or reselect the exact proposed operation, but MUST NOT by
itself revoke carrier eligibility, discard exact-instance evidence, alter
sibling attachments, or release the carrier's endpoint-group reservation. The
exact operation's temporary scheduler-load reservation is settled normally.
Only an exact carrier-scoped terminal event may publish carrier failure.

Failure publication MUST carry exact carrier-instance identity. A delayed
status, ACK, measurement, or teardown from an older instance MUST NOT alter
newer state.

Loss of an established QUIC connection or its HTTP/3 carrier request stream is
a failure of that exact carrier instance, not of every Product stream using the
session. Carrier recovery preserves the logical stream and its exact retained
ranges on surviving authenticated attachments. Frame-codec, authentication,
configuration, and Product protocol failures do not acquire that recovery
authority merely because they were observed through a QUIC carrier.

Peer abandonment of one operation-scoped HTTP/3 request-stream direction with
application code zero is an operation-local, error-free shutdown signal. It
MUST NOT alone publish carrier failure or warn as a carrier runtime error; the
connection and sibling request streams remain authoritative. A nonzero
application error, malformed/truncated frame, or terminal loss of a carrier
control stream keeps its ordinary smallest-safe-scope failure semantics.

## 14. Security and Privacy

### 14.1 Authentication and replay

Transport encryption and transport authentication precede MPP admission.
Versioned HMAC contexts separate QUIC session authentication, TCP
transport-bound admission, common path join, Noise PSK/framing, and private
QUIC Initial derivation. Nonces and freshness windows limit MPP admission
replay.

The shared transport secret MUST be generated as 256 bits of cryptographic
randomness, stored as raw bytes, and protected as endpoint key material. It is
not a password and MUST NOT be reused as an MPP credential. Noise handshake
ephemerals provide forward secrecy for completed TCP transport keys, but the
group PSK does not identify an individual client.

The receiver MUST bound unauthenticated parsing, concurrent admission work,
silent rejection retention, transport and MPP replay state, credential scans,
and total admission duration. It MUST compare authentication material without
data-dependent early exit that discloses the matching credential.

Target authorization is enforced under the authenticated principal at the
receiving endpoint. A peer-supplied target, metric, usage, or `PathId` does not
grant policy or capacity.

### 14.2 Malicious evidence and resource exhaustion

Peer metrics, usage, and capacity receipts are authenticated input. They MUST
NOT:

- grant receive credit;
- release retained data;
- establish local delivery;
- declare local health;
- bypass queue or flight bounds; or
- transfer state to another carrier instance.

A peer cannot select the client's TCP pool size or member identity. The client
MUST bound every carrier group to its configured current members plus the sole
exact planned-replacement overlap in Section 7.2, all within the local session
resource envelope and independently of peer input. The server applies its
ordinary authenticated session/global carrier caps to both instances. Input
for a stale physical instance has no authority over a current member.

Datagram replay windows, response caches, pending native datagrams,
reassemblies, and target forwarding are bounded. Reused IDs with conflicting
payloads or targets are protocol violations.

### 14.3 Traffic classification

The QUIC candidate selector prevents a party without an active credential from
reaching the MPP frame parser or eliciting an MPP-specific response. The TCP
prelude and all MPP frames are encrypted. In the optional shared-secret
profile, a public or wrong-secret probe cannot elicit TCP response bytes or a
QUIC certificate flight. All rejected TCP first-flight prefixes and contents
send no response; while bounded silent-rejection capacity is available they
remain connected through the same absolute authentication deadline. Resource
exhaustion sheds excess rejected sockets immediately rather than consuming
authentication capacity. A replayed TCP first flight is rejected before a
response while it remains in the endpoint replay cache.

These properties do not provide indistinguishability. In the default profile,
passive observers can still observe TLS and QUIC fingerprints, SNI and
certificate identity, the `h3` ALPN, QUIC transport parameters, HTTP/3
settings, packet sizes, and timing. The optional profile removes the public
TCP ClientHello and prevents public QUIC Initial decryption, but Noise X25519
ephemerals and QUIC version, header shape, connection IDs, size, and timing
remain visible. Idle liveness uses independently renewed bounded delays rather
than an exact cross-carrier period, but encrypted frame sizes and the bounded
delay distribution remain observable. A captured, still-fresh Noise first
flight can elicit a padded Noise response after a process restart or against an
independent server process; it cannot disclose a certificate, authenticate
MPP, or admit Product state. A party holding the endpoint
transport secret can identify the transport service, and a party also holding
an authorized MPP credential can authenticate as that credential.

Implementations MUST NOT advertise MPP as a cover protocol or claim that its
carrier presentation defeats a source-aware classifier. Fixed private
cleartext protocol markers are avoided, but authenticated tunneling—not
traffic impersonation—is the security objective.

## 15. MPTunnel Core Profile 7

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

That native-only fresh-data authority applies only while the lowest
outstanding frontier is live: no retained complete Data ACK proves that its
lowest range is missing. When a retained complete Data ACK omits that range,
the frontier becomes an authoritative-gap frontier. Its exact owner remains
the ordering, completion, hysteresis, and recovery reference, but fresh
originals on that owner MUST pass the ordinary Product flight and reorder
admission used by an additional output until Data ACK progress advances or
resolves the gap. Other eligible outputs are not globally paused. An
incomplete positive ACK cannot create this state, and this rule neither lowers
the configured reorder envelope nor changes native TCP or QUIC recovery.

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

Connection-wide source staging precedes DSN assignment and may contain bounded
work for several independently admitted outputs. It is therefore governed by
the shared stream, repair, reorder, and configured resource envelopes, not by
one selected output's native congestion window. For bulk work its active
allowance is the bounded sum of the current per-output admission windows for
the exact live outputs eligible for original data; with one output it is
exactly that output's window. Latency-sensitive work retains the selected
output's bounded window. Selection and allowance use one coherent view:
withdrawn or unschedulable outputs contribute nothing, non-stale outputs
precede stale recovery fallbacks, and a backup contributes only when no regular
output is eligible. Staging grants no output ownership or carrier reservation;
every assignment still passes the per-output admission and native writer checks
above.

An authenticated, admission-active attachment can precede its first
exact-instance measurement. It remains an unproven output and uses only the
configured startup prior and startup-flight bounds until that measurement is
published. Absence of measurement is not absence of an output; source
admission becomes zero only when no admission-active attachment remains.
Evidence from another carrier incarnation with the same path key MUST NOT be
substituted for the unmeasured attachment.

TCP pool establishment is owned by Section 7.2 and is independent of
instantaneous Product demand. A ready pool member enters the regular or backup
placement set defined by Sections 7.2 and 7.3. It receives no fixed share and
no special startup rate: acquisition is bounded by the existing unproven-path
flight, shared credit, queue, repair, and reorder rules above.

The scheduler evaluates every exact carrier direction independently from
current completion evidence. Qualified Product delivery may increase that
carrier's useful service window; queue, flight, RTT, loss, backup preference,
or inferior completion time may leave it without new Product work. Carrier
presence is therefore not payload allocation, and an idle member does not
cause duplicate transmission.

While the exact lower-range owner remains admitted, both request and response
placement retain it when another output's estimated completion advantage is
within current timing uncertainty plus one payload scheduling quantum of queue
uncertainty. A materially earlier completion or queue growth beyond that
quantum preempts it. This transport-neutral hysteresis prevents ownership
flapping on measurement noise without turning ownership into a fixed path
preference.

The Core does not infer a common bottleneck or condition carrier membership or
directional authority on transient comparative throughput samples. Such a
comparison cannot reliably mature a new kernel TCP flow across the full
supported bandwidth and RTT range, and it makes the physical pool depend on
one transient traffic direction. The configured maximum is the explicit,
bounded connection policy; native TCP congestion control and the ordinary
completion scheduler remain the traffic policy.

No Mbps value, utilization percentage, source address, locator, interface
identity, application flow count, laboratory condition, or fixed observation
window may create, promote, or revoke a TCP pool member. Exact native failure
changes liveness immediately; planned configuration and maintenance changes
use the gradual lifecycle in Section 7.2.
### 15.2 Reinjection budget and timing

Ordinary reinjection is limited by cumulative extra-traffic credit funded by a
bounded startup allowance and unique bytes acknowledged by MPP Data ACK.
The Product default is 10 percent. `[flow].optional_reinjection_budget_percent`
sets the local sender default and an MPP inbound/outbound performance value may
override it for that node. The value is directional and peers do not negotiate
it. It meters optional reliable MPP payload reinjection, not native transport
retransmission, MPP control or probe traffic, or the cause-bounded critical
recovery authority defined below.

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

After a Data ACK transaction in either direction leaves a retained complete
snapshot with an omitted range, define two absolute clocks from the exact
original flight's assignment epoch: `loss_at` is that epoch plus the local MPP
Data-ACK threshold, and `fallback_at` is that epoch plus the MPP recovery
interval. The thresholds are:

- `5/4 * SRTT` for TCP; or
- `9/8 * SRTT` for QUIC;

Before `fallback_at`, bounded repair on a currently measured alternate is
authorized at `loss_at` only when a copy launched at
`max(observation_at, loss_at)` is estimated to finish strictly before
`fallback_at`. If the alternate cannot win that absolute comparison, the
retained gap waits until `fallback_at`. At or after `fallback_at`, an eligible
measured distinct alternate may perform bounded repair without a completion
gain; expiration is liveness authority, not evidence that the alternate is
faster or that native recovery failed.

The repair may fill the alternate's available throughput-lane Product service
window, bounded by exact omitted ranges and the configured repair and
path-flight envelopes. Existing target flight and queued Product work consume
that window. Queue and flight are summed within the Product and native carrier
domains, while the overlapping domain totals are counted only once; one repair
quantum remains available for liveness when the window is full. This is Data
Sequence service authority, not native congestion
authority: the selected TCP or QUIC sender remains the final enqueue, pacing,
congestion, and recovery authority. These ratios are local approximations
inspired by transport time-threshold loss detection; the TCP ratio is not RFC
8985 RACK and the QUIC ratio is not QUIC's native RFC 9002 loss decision. A
multi-frame cumulative publication consists only of positive, incomplete
frames and cannot establish an omission. Later incomplete updates may fill a
gap already established below a retained complete snapshot's horizon, but
cannot extend that horizon or create negative evidence. Both directions use
the exact unique original flight's assignment epoch. For the same exact gap
and assignment, later owner observations may move `loss_at` or `fallback_at`
earlier but MUST NOT restart either clock later. Alternate eligibility and
completion are evaluated from the current target; a departed target's early
completion claim MUST NOT be inherited by its replacement. Without exact
ownership or a live measured distinct alternate no target-bound repair is
sent; when an eligible alternate cannot win early, ACK silence waits until the
one-interval `fallback_at` rather than erasing that fallback.

Recovery target ranking and commitment MUST refer to the same lowest-missing
frontier quantum. The first committed repair frame on the selected target has
the exact offset and payload extent whose estimated completion authorized that
target. After that quantum is committed, the sender may fill the remainder of
the same bounded target service window behind it. A larger coalesced batch or
whole-window throughput estimate MUST NOT replace frontier-quantum completion
as the primary target objective.

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

For the finite-drain rule below, MPP Data ACK progress means newly
acknowledged unique Product bytes. Receipt or republication of an unchanged or
subsumed Data ACK remains stream activity but does not rewrite an
OriginalData range's assignment age.

Once the sending application fixes a final offset, a remaining exact
OriginalData range also has an immutable finite-drain age from its original
assignment. After one owning-path MPP recovery interval, the sender MAY race
one bounded quantum of that range on a distinct output when both outputs have
qualified current completion evidence and the ordinary completion scheduler
estimates that the alternate will finish earlier outside its existing adaptive
jitter and queue hysteresis. Partial Data ACK progress shrinks the retained
range but does not rewrite any remaining original flight's assignment time.
This finite-tail rule does not mark the original attachment stale, withdraw it
from ordinary placement, or replace native recovery. Exact range identity,
repeat-delay suppression, shared credit, queue, flight, repair, reorder, and
extra-traffic bounds continue to apply.

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
bounds limit aggregate work.

The directional stream-attachment lifecycle is
`Qualified -> Stale -> Requalifying -> Acquiring -> Qualified`. A stale
attachment remains a sole-survivor fallback when no qualified or acquiring
attachment is schedulable, but fallback use MUST NOT clear or rewrite its
stale evidence. The carrier remains connected and native recovery continues
throughout.

Re-entry uses `STREAM_REQUALIFY_DATA` and `STREAM_REQUALIFY_ACK`. At most one
requalification transaction may be pending in one stream direction. The
sender copies one bounded retained Product quantum and transmits that copy on
the selected stale attachment. The quantum MAY have any retained OriginalData
owner, including an evidence-ineligible sole-survivor fallback owned by the
selected attachment itself. Owner qualification is not a safety input: the
probe remains non-owning and the exact probe ACK still enters only
`Acquiring`. The probe carries its stream ID, a nonzero
monotonically allocated probe ID, the copied range offset, and the bytes. It is
data-bearing for reachability, pacing, queue admission, and extra-traffic
accounting, but it does not own or deliver that Product range: it is not
inserted in the receive map, does not advance a Data ACK horizon, and does not
enter Product flight or delivery evidence. OriginalData therefore remains the
only Product owner, and a lost or reordered probe cannot create Product
head-of-line blocking or make its OriginalData owner's ACK ambiguous.

The receiver authenticates the frame under the ordinary carrier session and
returns `STREAM_REQUALIFY_ACK` on the exact carrying attachment, echoing the
stream ID, probe ID, offset, and payload length. A different attachment,
mismatched field, reused or replayed ID, `PATH_PROOF`, or generic `STREAM_ACK`
does not change qualification state. An exact probe receipt proves only
bidirectional attachment reachability and moves `Requalifying` to
`Acquiring`; it MUST NOT restore the stale attachment's prior Product delivery
rate or placement capacity. Before entering `Acquiring`, the implementation
revokes stream-local pre-stale Product authority and applies the existing
bounded new-attachment acquisition envelope. Only exact unique OriginalData
progress for work assigned to that attachment after the exact probe ACK moves
`Acquiring` to `Qualified` and rebuilds normal Product authority.

Loss of the probe leaves Product ownership unchanged. A pending transaction
expires no sooner than the existing stale-attachment recovery interval and
returns to `Stale`; that deadline is an actor wake deadline. The next attempt
uses a fresh probe ID. Probe bytes consume the existing optional extra-traffic
budget and remain charged. Budget exhaustion MUST NOT permanently prevent
re-entry: one minimum useful recovery quantum may be sent per exact stale
interval as critical recovery debt, still subject to the single-pending,
retained-range, queue, pacing, and flight bounds. Thus one stream direction
can add at most one recovery quantum instantaneously and, under persistent
probe loss, at most one quantum per stale interval over time, excluding frame
headers. Later optional reinjection authority remains reduced by that debt.

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
19. Each TCP carrier group reconciles toward its configured healthy maximum
    without exceeding it; the reserved first range value changes no behavior.
20. Every authenticated ready TCP pool member has bidirectional Product
    authority subject to directional `AVAILABLE`/`BACKUP` usage. Usage follows
    configured endpoint topology and never a throughput comparison.
21. Carrier presence never forces payload placement or duplication. The
    ordinary scheduler revalidates exact carrier health, usage, queue, flight,
    credit, and completion evidence before every commit.
22. Server `PATH_CLOSE` is the ordered aggregate acknowledgment of a matching
    client `PATH_DRAIN`; local emptiness or write completion cannot replace it.
23. `SESSION_CLOSE` retires the complete `SessionId`; carrier drain does not.
24. Configured capacity without a physical carrier is only an establishment
    candidate. It is not an authenticated carrier or path attachment and
    publishes no physical evidence; Product data follows only authentication
    and readiness.
25. One client session owner reconciles each TCP carrier group; each exact
    carrier instance has its own wire lifecycle, while a stream never owns
    group capacity or replacement.
26. A durable member ordinal persists across successive exact replacement
    instances. A planned replacement permits only the bounded
    current/successor/predecessor overlap in Section 7.2 and transfers no
    attachment, queue, flight, authority, or evidence.
27. A disabled TCP carrier group establishes no carrier and grants no new
    original placement. Every exact instance already in that group reaches
    terminal state through ordered carrier retirement; re-enable creates fresh
    instances and never cancels a drain already begun.
28. An IP packet's authenticated principal comes from carrier admission, never
    from an outer locator or a claimed inner source address.
29. IP packet delivery installs no MPP acknowledgment, retransmission, global
    reorder buffer, congestion window, route, DNS, firewall, or NAT state.
30. TCP and QUIC IP-tunnel attachments remain equally eligible; each direction
    preserves inner-flow affinity until an exact failure or a transport-derived
    flowlet boundary permits safe reselection.
## 17. Relationship to Existing Standards

### 17.1 MPTCP

RFC 8684 provides the established principles of stable data identity across
subflows, a data-level acknowledgment distinct from transport ACKs, shared
connection flow control, reinjection, bounded path management, and backup
preference. MPP uses an explicit configured carrier bound rather than a
traffic-rate threshold. Section 3.3.8 motivates regular-to-backup transition.
Section 2.6 permits one MPTCP subflow to close through ordinary TCP FIN/ACK
without closing the MPTCP connection; MPP's ordered per-carrier wire
transaction is independently defined as client `PATH_DRAIN` followed by server
`PATH_CLOSE`.

MPP follows those principles but is not MPTCP-conformant. Its offset space is
per direction of each MPP stream rather than one connection-level DSN space
per direction.
`STREAM_ACK` uses range snapshots and positive partial ranges rather than a
cumulative DSS Data ACK. MPP carriers are not MPTCP subflows.

RFC 6356 documents why independently controlled subflows sharing a bottleneck
can be less fair than one TCP flow. MPP cannot install coupled control above
kernel TCP and does not claim coupled fairness or common-bottleneck detection.
The configured pool maximum is therefore the explicit resource and concurrency
policy; each member retains native TCP congestion-control authority and the
usage-aware completion scheduler may leave redundant members idle.

### 17.2 QUIC

RFC 9000 and RFC 9002 govern each QUIC carrier's connection identity, network
paths, address validation, migration, congestion control, loss recovery, RTT,
ECN, and PMTU behavior. MPP does not redefine those mechanisms.

A model-based native QUIC controller should not necessarily use one delay
estimate for two different jobs. A raw minimum RTT is propagation evidence: it
is appropriate for the controller's minimum-delay filter and for draining the
queue during a ProbeRTT state. The flight needed during ordinary Startup,
Drain, and bandwidth-probing states is instead a service-window estimate. On a
variable-delay path, one reordered fast-tail sample can be a valid minimum RTT
while still being far below the delay that normally bounds delivered flight.
Using that sample for both jobs can make `gain * bandwidth * RTT` too small,
age the bandwidth estimate down behind the resulting underflight, and create a
self-sustaining rate collapse.

The preferred model for a BBR-family implementation therefore retains raw
minimum RTT for propagation and ProbeRTT, while ordinary flight may use a
separate packet-qualified operational RTT. Operational evidence should come
only from a controller-qualified low-flight observation—for example, initial
or idle zero-flight transmission, or packets sent after a ProbeRTT hold is
armed. These predicates qualify controller state; they do not prove that the
network queue is empty. An implementation should admit at most the newest
eligible packet from one real ACK event, require two independent observations
before departing from raw RTT, and use a bounded majority filter such as the
upper median of the latest three observations. Before that evidence exists,
raw RTT remains the fallback.

This separation is not permission to preserve a standing queue. Ordinary
flight remains the native controller's full gain-times-bandwidth-times-delay
calculation, and its existing loss, ECN, recovery, and upper-flight bounds
remain authoritative. The tradeoff is deliberate: qualified operational
delay can provision more flight than a rare fast-tail minimum, but drained
sampling, majority admission, and native congestion caps bound that choice.
It avoids treating an exceptional propagation sample as the normal service
window without changing how ProbeRTT measures propagation.

On a path where the operator deliberately authorizes an exogenous-loss
allowance, the preferred BBR-family model also separates service estimation
from its residual congestion-loss objective. Let `p0` be the sender-local
authorized loss-compensation fraction and let `q` be the controller's ordinary
residual loss objective. For one aligned delivery sample, the controller may
attribute at most `min(p0, lost / (delivered + lost))` to the allowance. It may
use that attributed fraction to correct both delivery rate and delivered
volume by the same factor. A clean sample is never inflated, and missing or
unalignable loss evidence remains conservative.

The corresponding high-loss boundary is:

```text
1 - (1 - p0) * (1 - q)
```

That expression is a population-rate policy, not permission to compare every
finite packet-timed round's point estimate independently with the boundary.
Doing so makes ordinary placement variance repeatedly look like congestion;
because the native response is multiplicative, those false decisions can
ratchet the bandwidth and flight models downward. A fixed-sample confidence
interval is not a clean repair: repeated looks invalidate its coverage, and
loss may be correlated rather than Bernoulli.

Loss-only observations also cannot distinguish an arbitrarily correlated
erasure burst from a drop-only policer. A nonzero `p0` profile therefore needs
an explicit burst envelope. Let:

```text
theta = 1 - (1 - p0) * (1 - q)
H     = 3 packet-timed operating rounds
E     = resolved volume of the preceding complete non-application-limited round
A     = H * p0 * E
B     = (1 - theta) * A
```

`A` is the product policy's tolerated displacement of authorized lost bytes.
`B` is the corresponding excess-loss credit: a lost byte also contributes
`theta` credit as part of resolved volume, and therefore consumes
`1 - theta` net credit. The first epoch uses the larger of the initial window
and the earliest-sent aligned lost packet's transmit flight until a complete
non-application-limited round supplies `E`. Three rounds is an explicit
MPTUNNEL product risk policy: at `p0 = 10%`, it permits at most `0.3 * E` lost
bytes to move across neighboring evidence rounds. It is not a BBR draft
constant, an inferred path property, or a value selected by a benchmark.

The sender maintains `C` in `[0, B]`. Every actual loss declaration has one
stable record carrying its packet number space, packet number, byte count,
aligned send evidence when available, and recovery-transaction owner. Its
class is exactly one of ordinary, raw-authority, or proven-spurious. For one
immutable epoch, `delta_ordinary_lost` is the sum of records still classified
ordinary:

```text
raw = C + theta * delta_delivered
        - (1 - theta) * delta_ordinary_lost
round_high = raw < 0
C = clamp(raw, 0, B)
```

Exactly once at an ACK-discovered packet-timed boundary, the sender consumes
the non-overlapping lifetime-counter delta already resolved before that
callback. QUIC reports losses after ending the ACK batch; those losses
therefore enter the next boundary's delta rather than an open-round point
estimate. The deferral is at most one additional packet-timed boundary and no
byte is omitted or consumed twice.

The native ordinary-loss response may run at most once for that decision. A
population policy has no authoritative per-packet crossing point, so a
`round_high` decision during ProbeBW Up uses the native beta-scaled target
rather than retaining an open-round inflight sample. `E` and `B` are rebased
only at the same boundary using `delta_delivered +
max(delta_ordinary_lost, 0)` from a non-application-limited epoch. Earned raw
credit is retained against the new bound, but a larger `B` does not itself
mint credit. Negative debt is not retained, so the first recovered
below-boundary round stops repeated multiplicative reductions.

While `p0 > 0`, Startup MAY use an aligned compensated `round_high` as the
high-loss leg of its native exit only if the exact completed epoch was
application-limited in its delivery sample and the connection is still
application-limited when the completing ACK transaction closes. Both
predicates are REQUIRED; a stale send-time application-limited watermark MUST
NOT terminate newly backlogged acquisition. A non-application-limited Startup
MUST instead use its compensated full-bandwidth plateau unless a raw-authority
decision applies. This gate supplements, and does not replace, the native
full-round recovery and discontiguous loss-event criteria. It does not alter
ProbeBW or `p0 = 0` behavior.

Startup's loss-event criterion counts discontiguous packet-number ranges from
the canonical records independently in each QUIC packet-number space; callback
interleaving across spaces is not a range boundary. If missing evidence ends
ProbeBW Up before Quinn finishes the current loss callback batch, all later
declarations in that batch belong to the same raw decision and cannot open or
charge a second loss round.

QUIC may retain multiple recovery transactions for two PTOs, and a later ACK
may prove an older or several overlapping transactions spurious. Addition is
not an exact refund after `C` has clamped or `B` has rebased. The sender
therefore keeps a bounded chronological journal from the state preceding the
earliest still-reclassifiable record. Raw attribution or late-ACK proof changes
the exact record classes and replays the immutable epoch tuples from that
checkpoint. This produces the same current `C` and `B` as if those records had
always had their final class, including overlapping transactions and a
spurious cold-start flight anchor. The journal is discarded once neither the
open loss-decision cohort nor Quinn's retained evidence can change a recorded
class.

The envelope bounds loss displacement, not response time. At constant volume
and sustained loss fraction `r > theta`, a full bucket crosses after:

```text
floor(B / ((r - theta) * E)) + 1 rounds
```

With the preferred `p0 = 10%` and `theta = 11.8%`, sustained 20%, 14%, and 12%
loss cross after approximately 4, 13, and 133 operating rounds respectively.
The long delay close to `theta` is deliberate: loss-only observations cannot
distinguish such a small excess from allowed placement or correlation.

ECN, RFC 9002 persistent congestion, and missing or unalignable evidence
bypass the envelope and the compensated Startup gate, retaining whatever raw
native Startup authority applies. The exempt record cohort is the same cohort
consumed by that raw decision, so a missing snapshot or persistent batch cannot
receive a native response and later be charged again as ordinary loss.
Persistent congestion unconditionally terminates an active ProbeBW Up once per
batch; its authority cannot be diluted by whichever packet callback happened
last. ECN without actual loss exempts no bytes. A valid late-ACK proof
reclassifies only records owned by that exact recovery transaction; newer or
unrelated same-ACK evidence remains intact. Native BBR state rollback remains
limited to the exact current transaction and is refused when its snapshot
predates an older still-open decision cohort or any newer raw authority. Final
older state already represented by the snapshot is preserved. Bucket replay
remains exact and independent of that conservative native-undo gate. NAT
rebinding retains the controller and its envelope; a genuinely new path starts
fresh. With `p0 = 0`, none of this state participates and draft BBR behavior
remains bit-for-bit authoritative.

Changing `q` is not a substitute for changing `p0`: `q` controls the residual
loss boundary where the current controller state grants that decision
authority, whereas `p0` repairs the delivery and inflight evidence that would
otherwise ratchet downward under sustained post-service erasure. It does not
override the Startup gate above. The preferred MPTUNNEL profile uses `p0 =
10%` and retains the BBR draft's `q = 2%`, producing an aggregate boundary of
11.8%. A sender may select `p0 = 0` for unmodified draft behavior. ECN,
persistent congestion, and unknown aggregate evidence continue to require the
controller's ordinary congestion response; the allowance does not create a
second MPP congestion controller.

A BBR-family implementation that uses this compensated serviced-rate estimate
should also use it for the full-bandwidth growth baseline. Retaining raw
delivered-rate samples only for that baseline makes its growth ratio depend on
changes in authorized erasure between rounds: a genuinely growing ProbeBW
round can then look like a plateau. The full-bandwidth baseline and its
existing 1.25 growth comparison therefore use compensated serviced rate
whenever the allowance has aligned loss evidence. A zero allowance preserves
the unmodified raw-rate comparison exactly. This alignment changes no
pacing/window gain, configured probe timer, or congestion threshold; by
preventing a false full-bandwidth plateau, it can change the loss-informed
timing of the current ProbeUP-to-ProbeDOWN transition.

This allowance is local traffic policy, not a measured path fact and not an
MPP protocol field. Each endpoint applies its own value only to its sending
direction; peers do not negotiate it and asymmetric values are valid.
Overstating it can classify real drop-based congestion as authorized loss and
can consume additional capacity. The preferred nonzero default is therefore
an explicit performance/fairness tradeoff, not an inference that every path
has 10% exogenous loss.

Product configuration names this sender policy
`quic_loss_compensation_percent`. A matching MPP inbound/outbound performance
value overrides `[flow]`; an explicit `loss-compensation-percent` on one QUIC
path URI overrides both. The complete resolution order is path URI, node
performance, `[flow]`, then the built-in preferred value of 10 percent.

The compensation value itself injects no packet. For reliable traffic under
an actual independent 10% post-service erasure rate, native retransmission
expands traffic by approximately `1 / (1 - 0.10) - 1 = 11.1%` relative to
delivered Product payload. If the independent MPP optional-work budget is also
fully spent at 10%, their rough combined expansion is
`(1 + 0.10) / (1 - 0.10) - 1 = 22.2%`. These are directional, workload- and
loss-dependent estimates; they exclude packet headers, control traffic, and
the bounded startup floor.

Feedback ordering remains a qualification limit. If the transport finalizes
the ACK event before delivering loss or ECN feedback caused by that same ACK,
that later feedback cannot veto the event's operational-delay vote. One vote
cannot move an established latest-three majority, and subsequent native loss
and ECN processing still binds flight; an implementation must not claim that
the vote itself was loss- or ECN-free.

This guidance is an implementation preference, not an MPP wire requirement.
It records a defect in applying a one-delay form of the BBR-family model
described by `draft-ietf-ccwg-bbr-06` to the qualified variable-delay case; it
is not a defect in RFC 9000 or RFC 9002. CUBIC, an implementation of the
published BBR draft, or another native QUIC controller remains permitted. In
every case the QUIC controller—not MPP scheduling—owns its packet window,
pacing, loss response, and recovery.

The default profile uses the public RFC 9001 Initial key schedule. The optional
shared-secret profile deliberately substitutes the private Initial input in
Section 6.2 and is therefore not Initial-key interoperable with an endpoint
that lacks the secret. After Initial authentication it retains the same QUIC
connection and native transport ownership.

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

### 17.5 Layer-3 address ownership

WireGuard's cryptokey routing associates each authenticated peer with allowed
inner addresses and prefixes, using them as a source-ownership check on receive
and a peer lookup on send. Its tunnel interface remains separate from ordinary
host route configuration. MPP's IP-tunnel service follows those ownership and
interface boundaries, but binds them to an authenticated MPP principal and may
carry packets over eligible TCP and QUIC attachments.

OpenVPN 2.6 distinguishes server address pools, stable per-client address
assignment, and client-owned internal prefixes from host routing and gateway
redirection. MPP uses only explicit principal allocations inside configured
pools; it neither leases addresses dynamically nor installs host routes.

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
- [The Noise Protocol Framework, Revision 34](https://noiseprotocol.org/noise.html)

### 18.2 Informative

- [BBR Congestion Control, Internet-Draft
  draft-ietf-ccwg-bbr-06](https://datatracker.ietf.org/doc/html/draft-ietf-ccwg-bbr-06)
- [RFC 6356: Coupled Congestion Control for Multipath
  Transport Protocols](https://www.rfc-editor.org/rfc/rfc6356.html)
- [RFC 8684: TCP Extensions for Multipath Operation with Multiple
  Addresses](https://www.rfc-editor.org/rfc/rfc8684.html)
- [RFC 8985: The RACK-TLP Loss Detection Algorithm for
  TCP](https://www.rfc-editor.org/rfc/rfc8985.html)
- [RFC 9298: Proxying UDP in HTTP](https://www.rfc-editor.org/rfc/rfc9298.html)
- [OpenVPN 2.6 Manual](https://openvpn.net/community-docs/community-articles/openvpn-2-6-manual.html)
- [WireGuard: Cryptokey Routing](https://www.wireguard.com/#cryptokey-routing)
