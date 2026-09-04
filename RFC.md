# MPTunnel Multipath Proxy Protocol (MPP) Version 10

## 1. Status and Conventions

This document specifies MPP version 10: its wire format, carrier profiles,
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

Wire version 10 is identified by the frame header in Section 12. A peer MUST
reject every unsupported frame version. This version has no downgrade or
compatibility mode.

In formulas over nonnegative quantities,
`clamp(x, low, high) = min(max(x, low), high)` and
`saturating_sub(x, y) = max(x - y, 0)`.

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

**Product ownership or Product flight**
: One exact original or reinjected MPP range retained until MPP Data ACK or its
  exact Product-terminal rule. It is logical delivery/recovery state, not
  native transport flight or queue ownership.

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
            advisory action rank and regular-before-backup selection
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

MPP owns stream, datagram, and IP-tunnel identity, offset assignment, Data ACK
processing, shared receive credit, carrier selection, bounded reinjection, and
exact data-level deduplication. Product ownership, bounded MPP queues, native
transport backpressure, and configured resource limits remain distinct
authorities. An advisory action rank does not replace any of them.

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

A locator-only migration preserves Product state, attachments, bounded MPP
queues, and the current native-controller activation when QUIC reports that
the same activation remains active. If QUIC installs or restores a different
active `PathData` or controller within the same connection, the activation is
fenced under Section 10.2 and only that activation's coherent native state may
be used. A replacement connection inherits neither native evidence, the old
carrier's MPP queue, nor an old exact-output flight. Logical stream-owned
Product ranges remain retained for exact Data ACK and recovery; they can use a
replacement only after the authenticated attachment procedure below.

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

In authentication and key-derivation transcripts, `||` means byte
concatenation, a quoted literal contributes its exact ASCII bytes without a
length prefix or terminator, and `empty` is the zero-length byte string.

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
"mptunnel session auth v10" ||
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
"mptunnel path join v10" ||
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
25..end padding                      8 through 63 random bytes
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

The first two digest octets are interpreted as one network-order `u16`. The
encoded length is the network-order `remaining_length XOR mask`; the receiver
applies the same XOR before validating and reading the remainder.

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

The first two digest octets are again interpreted as one network-order `u16`.
The encoded record field is the network-order
`ciphertext_length XOR mask`; the receiver applies the same XOR before length
validation.

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

A receiver of `PING(nonce)` returns exactly `PONG(nonce)` in the opposite
direction on the same bidirectional reliable carrier operation; a PONG grants
no Product, flow-control, delivery, or rate evidence. On a TCP carrier the
client may have at most one heartbeat PING outstanding, the server MUST NOT
originate an idle heartbeat, and only a PONG carrying that outstanding nonce
completes the heartbeat. An unsolicited or mismatched TCP heartbeat PONG is a
carrier protocol violation; failure to receive the matching PONG before expiry
of the local configured heartbeat timeout terminally fails that exact TCP
carrier. A QUIC request operation may
instead use one PING/PONG exchange as a bounded operation-local reachability
probe. Its local deadline, mismatch, or timeout fails only that probe unless
native QUIC independently declares the connection terminal.

`PATH_DRAIN` and `PATH_CLOSE` are valid only on TCP carriers. The TCP carrier
client alone sends `PATH_DRAIN`; the TCP carrier server sends `PATH_CLOSE` only
as the response that completes that drain. Their `path_id` MUST match the TCP
carrier carrying the frame. A frame sent in the opposite direction, or a
`PATH_CLOSE` without a matching `PATH_DRAIN`, is a carrier protocol violation.
After drain begins, both endpoints MUST make that carrier ineligible for new
attachments and original placement while retaining receive, Data ACK,
recovery, and ordered-control processing.

When aggregate carrier retirement withdraws attachments from multiple Product
streams, each stream's ordered detach MUST be published independently and
concurrently to that stream's bounded actor queue. Earlier events in one stream
remain FIFO-before its detach, but a full or dormant stream queue MUST NOT
withhold detach publication to another stream. Aggregate carrier retirement
completes only after every stream actor has applied its exact detach.

The server sends `PATH_CLOSE` only after every earlier frame from the client
has been applied and the exact carrier has no attachment, datagram binding,
queued or retained frame, original or reinjected Product flight, pending Data
ACK, path proof, or capacity work. All server frames that complete preceding
carrier-owned protocol work MUST precede `PATH_CLOSE` in the TCP byte stream.
The client treats receipt of
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
proof and do not repeat connection admission. The first MPP frame, together
with the authenticated physical carrier binding, unambiguously selects that
operation.

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

The carrier control request is a long-lived peer-diagnostics operation; its
lifetime is not itself the physical carrier lifetime. A clean receive FIN
exactly at an MPP-frame boundary ends only that carrier's peer-diagnostics
exchange. The receiver MUST preserve the QUIC connection, exact carrier
registration, session, sibling request streams, and Product streams or datagram
associations. Local withdrawal of the peer-diagnostics owner has the same
diagnostics-only scope.

EOF inside a record-length prefix or frame is truncation. A reset or stop,
HTTP/3 stream error, malformed or unexpected control frame, or any other
nonclean terminal failure of the control request is terminal to that exact
QUIC carrier: the endpoint closes and retires the connection while retaining
session and Product state under ordinary carrier recovery. An operation-local
EOF, reset, refusal, finish, or cancellation on any later HTTP/3 request stream
remains terminal only to that operation and MUST NOT retire the physical QUIC
carrier. Only authenticated `SESSION_CLOSE` has session scope.
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

Every endpoint-local carrier-instance identity comes from one checked,
non-reusing finite sequence. Exhaustion is an absorbing process state: already
published carriers remain authoritative, but no missing-slot, pool-growth,
replacement, or reconnect transition may start another physical establishment
attempt. Maintenance eligibility and deadlines MUST report no such work after
exhaustion. Contenders that observed the final available value concurrently
may finish bounded establishment work, but exactly one can consume that value;
every loser fails before publication and MUST NOT rearm reconciliation. Identity
exhaustion is not endpoint, authentication, congestion, or path-health evidence
and MUST NOT poison those states or enter a retry loop.

While its session and local configuration remain enabled, every configured
QUIC path owns one durable physical-carrier slot. Exact native connection close
MUST atomically fence the closed carrier instance, remove that exact slot
owner, release its authenticated and diagnostic registrations, and publish a
durable reconciliation wake. A delayed close or cleanup for instance `N` MUST
NOT alter a published instance `N+1`. Missing-slot reconciliation is carrier
lifecycle work: it MUST NOT depend on optional path measurement, active-Product
flow count, application retry, or the periodic measurement interval.

Only one unpublished QUIC successor attempt may own a slot generation at a
time. Failure, deadline cancellation, or supersession cannot publish that
candidate, but MUST leave the vacancy durably reconcilable. A later attempt is
bounded by the path-derived complete establishment-transaction clock retained
from the exact prior owner, or by the ordinary startup timing model before a
first owner exists; it does not shorten QUIC idle timeout, keep-alive, loss
recovery, or congestion control. Successful reconciliation publishes a fresh
carrier-instance identity and fresh evidence. An ordinary post-control HTTP/3
operation's reset, finish, refusal, or cancellation does not vacate the live
physical slot and does not wake carrier replacement. Section 6.2 separately
defines a nonclean carrier-control failure as exact-carrier terminal.

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

The maximum is the healthy pool target and the hard bound on current member
ordinals owned by the group. Outside one planned replacement transaction, at
most `MAX` physical carriers may be establishing, ready, or draining. During
one planned replacement transaction, the sole group-scoped transient
successor permits at most `MAX + 1` physical carriers; it does not increase
the schedulable member target. While the group and session are enabled, one
client session owner reconciles durable member ordinals `0` through `MAX - 1`
toward that target.
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
traffic share, or a common-bottleneck inference; current typed carrier service
evidence remains authoritative.

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

`OPEN_STREAM(stream_id, target, demand, return_plan)` has no permanent carrier
role. The first accepted open creates the stream. A later open with the same
`StreamId` adds an attachment only when the target and initial demand hint
exactly match the original values and its return-plan fields are valid for the
retained one-shot response-startup transaction.

The return plan freezes one requester-selected usage tier and a finite set of
candidate ordinals for the first response prefix. It owns no Product credit,
rate authority, carrier health, or later ordinary scheduling preference. Its
wire fields are:

```text
trigger_bytes       h:u64
candidate_total     n:u8
candidate_tier      AVAILABLE(0) | BACKUP(1)
phase               STARTUP(0) | ORDINARY(1)
candidate_ordinal   o:u8
```

Every plan has `n >= 1` and `o < n`. A canonical singleton has `n = 1`,
`h = 0`, `phase = STARTUP`, `o = 0`, and is ready immediately without a finalization frame. A
multipath startup plan has `n > 1` and `0 < h <= 58,400` bytes. Its initial
attachment and every candidate enrolled in that frozen round use `STARTUP`, a
distinct ordinal, and the same `(h, n, candidate_tier)` signature. An
`ORDINARY` attachment uses canonical ordinal zero, is not enrolled in the
round, and cannot settle or alter it, but carries the retained signature while
the stream exists.

The responder stores a canonical singleton as `Singleton`. It stores a
multipath plan as `Unresolved`, retaining `h`, `n`, the tier, and each enrolled
ordinal's exact output association. The first valid finalization changes that
state to `Finalized` with one immutable retained-ordinal set.

Until a multipath plan is finalized, the responder admits fresh unique
response offsets only below `h`. Data ACK does not refill this one-shot prefix.
When the requester observes a contiguous response frontier at least `h`, it
opens every still-unresolved frozen candidate at most once. After every
candidate is accepted or has failed, it serializes the ordinals that are both
accepted and still attached in strictly increasing order and publishes
`STREAM_RETURN_PLAN_FINAL` on the current logical stream attachments.

The responder accepts the first finalization only when every retained ordinal
is in range, strictly increasing, and enrolled by an exact accepted startup
attachment. In one transaction it withdraws every omitted enrolled output from
new Product placement before removing the prefix ceiling. An equal repeated
finalization is idempotent; a different repetition or a new `STARTUP`
attachment after finalization is a protocol violation. The requester retains
the finalization publication until the contiguous response frontier exceeds
`h`, or until an exact terminal declaration proves either `final_offset > h`
or contiguous receipt through that final offset. These rules preserve already
published Product and ordinary attachment-detach ordering; they do not infer
which candidate delivered duplicated bytes.

The session initiator allocates nonzero `StreamId` values with checked strict
monotonicity and never reuses one during the authenticated session. Exhaustion
fails a new logical open before publication rather than wrapping. Consequently
a well-formed delayed frame for an absent or terminal `StreamId` is stale; it
can never name a future Product stream.

The wire demand value is an immutable admission hint. A sender's live
throughput, latency, or realtime objective may change from local Product and
queue state without a wire update. That sender-local state controls its
direction only and cannot overwrite the peer's objective or the initial
attachment-consistency value.

One live carrier instance may have at most one live output attachment for a
given stream. Replacing a closed output creates a new attachment incarnation.
No flight, proof, rate, feedback, queue, or load state from the old incarnation
may be inherited merely because `StreamId` and `PathId` are unchanged.

Each endpoint-local attachment-incarnation sequence is checked and non-reusing
within its exact stream scope. Exhaustion fails that new local membership before
it mutates the attachment set, a closed predecessor, Product ownership, or
evidence. It does not revoke an existing attachment. An initiator may already
own a tentative peer-visible open and cumulative attach-control transaction;
that uncommitted carrier stream owns no local scheduling or evidence authority
and MUST follow ordinary detach-before-close retirement when local admission
fails. Such a tentative transaction does not require preallocating or recycling
a local incarnation. A receiver allocates its local output incarnation before
publishing the accepted output or mutating a closed predecessor.

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
receiver advances the retained grant from zero to the configured shared window
`W`, defined in Section 15.1, and publishes it to every live attachment. An
attachment added after establishment receives its
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

`STREAM_ACK(stream_id, complete, ranges)` carries a bounded list of
half-open Product ranges in one directional MPP stream offset space. Every
listed range is non-empty; the list itself MAY be empty.

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

That horizon limits negative range inference; it does not erase the sender's
positive local fact that current-epoch OriginalData remains retained and
unacknowledged on an exact attachment incarnation. Retained Product ownership
above the horizon remains authoritative for exact Product ACK release and
Product recovery. It MUST NOT by itself extend the Data ACK horizon, establish
a Data ACK gap, or declare native transport loss.

Before mutation, the endpoint MUST structurally validate the complete frame and
immutably look up the Product stream direction. A structurally valid ACK for an
absent or terminal Product stream is a stale no-op. It MUST NOT create, reopen,
or mutate Product state. For a live Product stream, the endpoint freezes the
exact current send extent and validates and normalizes every range against that
extent before any mutation. Data ACK processing is one transaction:

1. validate and normalize the ranges;
2. compute each newly acknowledged unique Product byte once;
3. release every original or reinjected Product flight overlapping those bytes;
4. update local delivery and admission evidence without changing receive
   credit; and
5. publish carrier-specific progress only when attribution is unambiguous.

A malformed frame or a range invalid for a live Product stream changes no
state.

If a byte was outstanding on multiple carriers, the Data ACK proves delivery
but not which copy delivered it. No implementation may invent per-carrier
delivery evidence for that range.

Processing newly received unique Product bytes marks the cumulative Data ACK
state pending and advances one local publication generation. Before the
serialized receive actor parks or yields its bounded cooperative turn, it MUST
offer the latest pending generation independently to every currently live
exact attachment. Several frames processed in that turn coalesce into the
latest cumulative state; ACK publication never waits for an application read
or another Product frame.

Forced publication of unchanged state reuses the current generation. Queue
acceptance advances only that exact attachment incarnation's publication
fence; a blocked attachment remains pending and retries on carrier-capacity or
attachment-membership wakes. A newly accepted attachment starts without a
fence and receives the retained latest state. Latest-state replacement and the
chunk cursor keep pending publication bounded. This rule adds no receive
window, congestion signal, stop-and-wait dependency, or carrier-delivery
attribution.

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

Let `a_r` be the greatest contiguous receive offset whose bytes have left the
MPP reorder/receive buffer for the target or local application. Subject to the
configured absolute stream bound, the retained grant is exactly the checked
monotone maximum of its prior value and `a_r + W`. The initial successful target
grant therefore equals `W`. Every advance of `a_r` immediately updates the
retained latest grant, and before the serialized receive actor parks or yields
its bounded cooperative turn it MUST offer that latest value on every live
attachment. Several advances in one turn coalesce; queue blockage retains one
latest pending value and the exact capacity wake retries it. Arithmetic
exhaustion grants no wrapped credit and starts no new Product admission.

Thus a consuming receiver slides the full configured window without an ACK
threshold or timer, while a target/application that stops consuming exerts
intentional bounded backpressure. Data ACK publication remains independent:
receipt can release sender Product ownership before application consumption,
and credit can advance only when receive-buffer capacity is actually freed.

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

When exact-carrier failure or retirement removes an attachment from local
scheduling, membership and scheduler load MUST be removed as one local state
transition before wire cleanup. `STREAM_DETACH` followed by local stream close
remains ordered on that carrier, but publishing that cleanup MUST NOT wait for
capacity in a bounded Product command queue or block scheduling on surviving
attachments. The carrier output owns this failed-attachment retirement. This
does not relax the ordered FIN path used for successful Product completion.

A reliable-stream owner MUST continue polling its ordered attachment-lifecycle
input until that logical stream becomes terminal. Product half-close, absence
of locally sendable payload, or zero retained flight MUST NOT suppress carrier
detach or failure processing. Lifecycle input is independent of Product work;
its exact-instance transition may be required to release carrier retirement
while the remaining Product direction stays open.

`STREAM_RESET(stream_id, reason)` terminates the MPP stream.

Product FIN, detach, reset, or logical-stream terminal cancels only work
that remains locally removable under the exact queue and reservation owners.
It does not acknowledge native transport bytes or Product delivery. Already
published Product flight remains subject to Data ACK, recovery, and terminal
cleanup under its existing owner.

Native TCP EOF ends the carrier and is not stream FIN or detach. Native QUIC
stream FIN closes only that native byte-stream direction and MUST NOT be
interpreted as MPP FIN, detach, Product completion, or physical-carrier
failure. Operation-local HTTP/3 request completion follows the attachment or
flow lifecycle for that request; only a connection-level terminal event or the
nonclean carrier-control failure defined by Section 6.2 ends the QUIC carrier.
A native FIN inside an MPP record is truncation; a FIN at a record boundary is
a clean native half-close.

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
no authority, attachment, transport evidence, queue, or Product flight
transfers from a failed instance. Reconnect attempts MUST NOT extend any
original retention deadline.

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
- the cumulative extra-traffic envelope, except for the explicitly bounded
  cause-specific critical authorities in Section 15.2.

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

Fresh `datagram_id` values range from zero through `u64::MAX - 1` inclusive.
`u64::MAX` is reserved because the half-open feedback encoding cannot represent
its exclusive end. Once the next fresh ID would be reserved, the sender MUST
retire the flow rather than wrap, reuse an ID, or send that value.

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

One scheduling action is scoped to an exact logical output, attachment
incarnation, physical carrier instance, direction, command kind, and proposed
encoded MPP work. The scheduler evaluates an immutable observation and proposes
a finite order of actions. Before enqueue, the implementation revalidates the
exact identities, stream frontier, output-admission epoch, complete rate-
authority stamp, command priority and work, and queue reservation on which the
proposal depends. Product range ownership is committed only in the same
infallible publication transaction as the accepted enqueue.

An observation may contain:

- local carrier health and drain state;
- peer usage and local operator policy;
- RTT, RTT variation, and jitter;
- one typed directional service rate when available;
- MPP Data ACK progress;
- native and MPP queue and flight diagnostics;
- current demand; and
- evidence provenance and freshness.

The implementation MUST discard and recompute a proposal when any revalidated
identity, frontier, epoch, evidence stamp, or reservation is stale. Replacing a
rate inside an old score is not revalidation.

Ranking is advisory. It cannot grant Product credit, queue capacity, native
send credit, path proof, lifecycle eligibility, or recovery authority. The
commit owner tries the frozen finite action order until one exact reservation
succeeds or every structurally eligible action fails. An unrankable action sorts
last within its structural tier; it is not thereby made ineligible. Failed and
draining carriers, forbidden command classes, missing attachments, exhausted
configured resources, and failed exact reservations remain structural
ineligibility rather than score penalties.

Peer AVAILABLE versus BACKUP and local backup policy form the outer
regular-before-backup tier defined in Section 7.3. They are not numeric timing
penalties. Other explicit operator constraints such as control-only, allow-bulk,
or allow-datagrams are likewise structural. A policy described only as costly
or expensive MUST NOT be converted into an arbitrary RTT-, loss-, or payload-
scaled delay; any implementation-defined cost requires its own declared unit
and deterministic policy order.

### 10.2 Evidence provenance and advisory rank

Transport queue/flight and MPP queue/Data-ACK flight overlap in one delivery
pipeline. Sampled counters therefore cannot be added, maximized, divided by
flow count, or relabelled as one exact physical backlog. Product ownership,
bounded MPP queues, native flight, and native send capacity retain their own
owners and terminal rules.

For one exact action a, the advisory service score is:

    S(a) = T(a) + ceil(8 * (A(a) + M(a)) / C(a))

where:

- T(a) is a nonnegative duration-valued propagation estimate for the exact
  action direction;
- M(a) is the complete encoded MPP frame size: the 10-byte MPTF header plus
  its declared payload, excluding native framing, headers, and retransmission;
- A(a) is exact local pre-native predecessor work in the same byte unit,
  ending when that work is handed to the native transport; and
- C(a) is a positive finite directional service rate in normalized-MPP bits
  per second whose scope includes that exact action.

The division rounds upward. Every addition, multiplication, and conversion is
checked. An unrepresentable score is unrankable rather than saturated into a
win. A, M, and C MUST name the same direction and declared work domain. If
comparable A cannot be proved for every candidate in one comparison, A is
omitted uniformly from that comparison. Missing evidence is not zero.

Before a finite typed C exists, an Unknown startup rate uses the portable C_0
prior defined in Section 15.1. Explicit Unlimited is an ordering-only startup
sentinel: its service-duration term is defined as zero, so its startup score is
T(a). Unlimited is not a numeric C, cannot be combined with a measured rate,
and grants no Product bytes, native send credit, pacing, window, or capacity.
The first accepted Valid NativeOperational publication replaces either startup
basis for that activation.

A excludes Product or Data-ACK flight, native flight, native retransmission,
loss, confidence, active-flow count, and health labels. C MUST NOT be divided
by active-flow count, multiplied by path or flow count, or combined with an
independent rate by addition. A Product per-flow completion rate cannot be
silently compared as physical-carrier capacity; a physical-carrier rate cannot
be silently relabelled as one flow's guaranteed share. The rate-source contract
must state the comparison scope, normalized work projection, activation
identity, and freshness.

Loss, ECN, confidence, freshness, application-limited state, and Suspect may
determine whether a typed observation is valid or may trigger reconsideration,
but they do not add independent time to S. In particular, a Suspect label alone
cannot override a finite current typed rank. Native controller and exact
reservation state continue to constrain actual transmission.

Equal scores use a canonical exact action key containing output identity,
carrier instance, attachment incarnation where applicable, direction, and
command identity. A bare reusable PathId and input order are insufficient. This
key is only deterministic ordering and conveys no topology or capacity.

Path retention uses a duration-valued uncertainty `U` separate from `S`. For a
rankable action `a`, let `J(a)` be its nonnegative jitter value from the same
exact action, direction, and timing epoch as `T(a)`; the configured value is
used before a measured value exists. Then:

    U(a) = max(J(a), 1 ms)

Switching away from incumbent `i` to challenger `c` requires
`S(i) - S(c) > U(i) + U(c)`. Jitter is not a statistical rate-confidence
interval and does not prove that an estimated percentage difference is
significant. Rate confidence may validate the typed `C`; it does not contribute
to `U`. A percentage such as ten percent is a validation comparison band or
operator hint, never a hidden path-swap threshold.

The formula is an advisory local-service ordering, not an end-to-end
application-completion estimate. It proves deterministic ordering, monotonic
response to lower T or A and higher C, and no intentional double counting of
the excluded stages. It does not prove receiver completion, independent
bottlenecks, finite native service, restart-free recovery, or superiority to a
baseline.

#### 10.2.1 NativeOperational rate authority

One exact (carrier instance, original-sender direction) owns one persistent
native scheduling-rate reducer. A native transport rate is authoritative only
through a named adapter that exports the current positive, finite, gain-free
operational bandwidth used by that same controller's live send model. This
value is denoted `B_op`. For the QuinnBbr3NativeOperationalV1 adapter it is
min(max_bw, bw_shortterm). A gain-scaled pacing rate, detached ACK-window
estimate, peer metric, Product-goodput estimate, or configured path count is
not that value.

The adapter exports one coherent observation:

```text
(E_N, I_N, kind, rate)

kind = Absent                    # no rate is present
     | Valid                     # rate = positive finite B_op
```

`Absent` is no authority event for an already initialized activation: it does
not clear a retained valid value or restore a startup prior. A new activation
with `Absent` retains only its own configured startup prior until its first
`Valid(B_op)` observation.

The transport owner publishes two distinct checked identities:

- E_N, the strictly increasing active-source activation serial; and
- I_N, the immutable controller identity within one E_N.

Every installation and every restoration of an active native PathData or
controller advances E_N, including restoration of the same object or identity.
A locator-only change that retains the exact active controller retains E_N. A
valid history may therefore be:

    (E1, I_N=A) -> (E2, I_N=B) -> (E3, I_N=A)

A proposal carrying E1 or E2 is stale after E3; equality of I_N cannot revive
it. Exhaustion fails closed without wrap or reuse.

The central reducer owns a separate checked, nonzero, non-reused revision G.
One accepted semantic change of active source, authority basis, or
NativeOperational rate advances G exactly once; an exact repetition is a no-op.
A scheduling snapshot contains the immutable reducer scope, E_N, I_N,
authority basis, normalized rate, and G. Every consumer revalidates the complete
stamp.

The current (E_N, I_N, kind, rate) MUST be read atomically from one active
native controller snapshot. Publication uses capture, coherent read, and compare-
apply against the captured G, followed by a current-E_N check. A failed
comparison discards the whole observation and rereads; it cannot pair a rate
from one activation with a later identity. Same-activation publishers are
serialized or compare-and-swap their complete captured stamp.

A transport activation switch publishes a durable wake atomically with the new
active pointer and E_N. Wakes may coalesce only to the then-current activation.
A central consumer that still names an earlier E_N parks or recomputes; it
cannot use predecessor authority on the successor activation. A briefly
published predecessor observation racing a later switch is unconsumable
because precommit also compares current transport E_N.

The successful final complete-stamp comparison and authority-dependent
ownership/enqueue commit MUST have one linearization order with native active-
pointer switching. An implementation may hold the switch fence through commit
or carry E_N into the native writer, which rejects it before ownership transfer
when it no longer names the current activation. A check followed by an unfenced
gap before commit is not precommit revalidation.

A configured initial rate is a sender-local startup prior for a fresh
activation before its first valid NativeOperational publication.
`[flow].initial_rate_mbps` supplies the optional default for every local MPP
TCP and QUIC path; it is a positive whole decimal-Mbit/s integer. An explicit
path `initial-rate-bps`, `initial-rate-kbps`, or `initial-rate-mbps` value
overrides it;
`initial-rate=unknown` explicitly suppresses it, and
`initial-rate=unlimited` selects the unbounded hint form. A path URI contains
at most one of those rate forms. Every numeric form is a positive integer and
its scaling MUST fit in `u64` bits per second. Omission at both scopes means
Unknown. The resolved prior is endpoint-local and is neither serialized nor
inferred for the peer: a client outbound controls client-to-server scheduling,
and a server inbound controls server-to-client scheduling.

On TCP the value is only an MPP scheduling prior and does not change native TCP
congestion control. On QUIC, a finite rate `R` and configured initial RTT `T`
(333 ms when omitted) use `MDS`, the maximum QUIC datagram size at controller
construction, and `IW10 = min(10*MDS, max(2*MDS, 14,720))` bytes, the RFC 9002
initial congestion window. They set the native initial window target to
`max(IW10, ceil(R*T/8))` bytes and the initial pacing target to `ceil(R/8)`
bytes per second. The configured targets seed neither BBR
bandwidth nor `max_bw` and do not otherwise change Quinn's controller model.
Configuration MUST reject a finite QUIC pair unless
`ceil(R/8) <= 2^53` bytes per second and
`ceil(R*T/8) <= u64::MAX`; it cannot round or saturate those targets silently.
This QUIC constraint does not apply to a TCP-only finite prior. Unknown and
Unlimited preserve the native BBR3 startup defaults.

For a finite QUIC prior, MPP retains that prior as scheduling authority until
the exact post-authentication native controller has supplied valid Data-space,
non-application-limited evidence in two distinct packet-timed source rounds.
The first Data-space packet after the local post-authentication
application-ready boundary fixes the packet-number floor. Pre-ready,
pre-floor, stale-revision, wrong-space, invalid, zero, unrepresentable,
application-limited, absent, and same-round records are no-ops. A fresh native
controller obtains a fresh floor; a retained clone or rollback retains its
handoff state. This qualification changes only which rate basis MPP projects;
native BBR consumes its evidence and controls window, pacing, loss, and
recovery throughout. Unknown and Unlimited bypass the qualification gate. The
first qualifying valid publication changes the MPP basis; later missing polls
or idleness do not restore the prior or erase a retained valid value. Explicit
structural invalidation is a separate fenced command, not a numeric sample. It
removes the invalid authority or terminalizes the exact carrier according to
the adapter contract; it cannot silently create a different rate source.

The adapter contract declares its carrier/direction scope, unit conversion,
activation and rollback behavior, application-limited rules, publication wake,
structural invalidation, exhaustion behavior, and finite stable-activation
publication bound `D_pub` within a declared environment that includes bounded
fair coordinator service. If one complete
`(E_N, I_N, kind, rate)` observation remains current for `D_pub`, every live
central consumer MUST receive that observation's authority revision within
`D_pub`. A faster activation or observation change waives only that
conditional deadline; it never waives the immediate current-`E_N` precommit
fence. The adapter MUST have
deterministic tests for install, restore,
same-identity clone, delayed predecessor rejection, capture/read/compare races,
wake publication, invalidation, and checked exhaustion. These obligations prove
authority chronology only; they are not a throughput theorem.

Other typed sources may be used only under their own declared scope and
freshness. A configured rate is a prior, a Product delivery rate is Product
evidence, and a TCP capacity receipt is a bounded measurement. None may be
merged with NativeOperational authority by taking an unqualified maximum.
Peer PATH_METRICS is detached diagnostic evidence and cannot serialize or
replace a local native-controller lifetime.

Native ACK evidence may establish native transport service within its carrier.
Only unambiguous MPP Data ACK coverage establishes unique Product delivery on
an output. Data ACK coverage of duplicated bytes MUST NOT be attributed to
either copy. A locator, interface, address, route, carrier family, or configured
path count establishes neither capacity nor bottleneck identity.

### 10.3 No second congestion controller

RTT, loss, ECN, jitter, queue, flight, pacing, and delivery observations MAY
affect advisory ranking, typed-evidence validity, diagnostics, and application
record or batch size. They MUST NOT shape an independent Product congestion
window. Native admission remains the bounded writer reservation and native
backpressure. MPP MUST NOT use those observations to:

- maintain an independent loss- or ECN-driven congestion window;
- install a native-packet pacer;
- throttle below native enqueue/backpressure as a substitute congestion
  controller;
- replace native retransmission; or
- make a native controller's packet-loss decision.

The output carrying the contiguous stream frontier remains bounded by shared
MPP credit, enqueue capacity, configured Product resources, and its native
controller. Additional-output placement and repair retain their exact Product
identity and bounds from Section 15.

### 10.4 Bounded work and fairness

All MPP-owned scheduling, retention, reinjection, measurement, queue, and
diagnostic allocations MUST have byte and item bounds plus one exact
cancellation or terminal owner. A time bound is REQUIRED only where this RFC
defines a timer or absolute retention lifetime. Native-owned reliable debt has
no finite service-time guarantee while its native transport remains live.
Cancellation MUST reconcile each queue reservation, Product flight,
measurement ticket, load lease, and registry entry exactly once.

The final carrier writer preserves dependency and class boundaries while work
remains MPP-owned. At every command-selection boundary it serves
dependency-ready Control, lifecycle, and Data ACK work first, then Realtime and
Latency work, due cause-bounded recovery, ordinary Throughput, and optional
repair. It re-enters arbitration after each selected command; it need not wait
for native delivery or acknowledgment and therefore imposes no one-frame
stop-and-wait. Native-capacity release exposes pending higher-class work before
another lower-class command is handed off. Priority MUST NOT overtake an
earlier protocol prerequisite.

This priority cannot preempt bytes already accepted by a shared TCP socket,
QUIC stream, kernel queue, or other native FIFO. No lower-class command still
owned by MPP may be selected ahead of dependency-ready higher-class work, but
the latter may wait behind bounded mandatory MPP predecessors and bounded
already-native debt. Core states no finite time bound for native debt at zero
service. Within one class, positive quanta from continuously ready streams
receive weakly fair turns and a blocked stream owns no writer turn. Persistent
higher-priority overload may starve lower classes; Core makes no contrary
capacity claim.

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
queue 64-bit values begin at offsets 66 and 74. A peer-status path entry is
exactly `state:u8`, then `usage:u8`, then the 116-byte `PATH_METRICS` record,
and is therefore 118 bytes.

Metrics are advisory and scoped to the authenticated carrier instance and
direction. `bytes_in_flight_observed` and `queue_observed` independently state
whether their corresponding numeric field is an actual observation; a true
flag with numeric zero means observed zero, while a false flag means unknown.
An endpoint MUST NOT interpret a queue or flight value whose corresponding
flag is false as measured zero, native debt, native credit, or scheduling
authority.
`metric_age_us` is the saturating diagnostic age of the selected rate sample.
`rate_valid_for_us` is the remaining receiver-relative diagnostic freshness
budget advertised with this record. A producer MUST derive it from the selected
sample's immutable freshness deadline, and every retained or forwarded copy
MUST only reduce it by local residence; it MUST NOT reconstruct or refresh the
budget from later RTT, RTT-variation, pacing, or application-limited shape.
A canonical value is at most `64,424,584,425` microseconds, the three-PTO
freshness budget obtained from the largest representable `srtt_us` and
`rttvar_us`; a decoder MUST reject a larger value and a local producer MUST cap
its value at this bound.
A zero budget marks the rate, pacing, and confidence stale while the raw
numeric values may remain diagnostic. Because endpoints do not share a
monotonic clock, this field is a remaining diagnostic horizon beginning at receipt,
not a cross-host absolute deadline; transport time cannot increase the
advertised duration.

The receiving peer MUST NOT install, reconstruct, refresh, or downshift a local
advisory rate or NativeOperational value from this detached record; only the
producer's exact local evidence owner may publish such local authority.
`rate_observed` is true when `delivery_rate_bps` belongs to a measured native,
Product, or generic delivery epoch and remains true after that epoch expires;
it is false for an unmeasured startup prior. A nonzero
`rate_valid_for_us` with `rate_observed = false` is noncanonical and MUST be
rejected. Producer-side freshness requires both `rate_observed = true` and a
nonzero remaining budget. Product `has_ack_derived_data_sample`, `data_sample_count`, and
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
receipt confirms it.

The transaction is an optional diagnostic extension, not a Core Product
acquisition phase. Core Profile 7 MUST NOT automatically initiate it as a
prerequisite for ordinary placement, qualification, carrier readiness, or
pool reconciliation. A receipt proves only ordered ingress of the declared
measurement payload on that exact carrier and direction. It establishes no
Product or scheduling completion-rate authority. A pending transaction or
receipt MUST NOT be a logical eligibility prerequisite or hold exclusive
writer ownership; after each bounded diagnostic frame, the writer MUST return
to ordinary arbitration so Product, Data ACK, control, and lifecycle work
retain their normal priority. An implementation MAY retain the raw interval
result for diagnostics, but Core attachment Product evidence comes only from
the exact Product rules in Sections 15.1 and 15.2.

Optional native telemetry may refine estimates. Every field is independently
optional. Absence means unknown, never measured zero, and MUST NOT make a
carrier ineligible by itself.

### 11.3 Peer status

An authenticated endpoint MAY send `PEER_STATUS_REQUEST(request_id)`. The peer
answers on the same carrier with
`PEER_STATUS_RESPONSE(request_id, code, paths)`, where `code` is `OK`,
`DISABLED`, or `UNAVAILABLE`. A non-`OK` response contains no paths.

Each path entry uses the exact 118-byte order defined in Section 11.1: local
`state:u8`, directional `usage:u8`, then one `PATH_METRICS` record. A response:

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
4      version        10
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
| 7 | `OPEN_STREAM` | `stream_id:u64, target, demand:u8, trigger_bytes:u64, candidate_total:u8, candidate_tier:u8, phase:u8, candidate_ordinal:u8` |
| 8 | `STREAM_DATA` | `stream_id:u64, offset:u64, length:u32, bytes` |
| 9 | `STREAM_ACK` | `stream_id:u64, complete:u8, range_count:u16, ranges[range_count]` |
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
| 37 | `PEER_STATUS_RESPONSE` | `request_id:u64, code:u8, count:u16, paths[count]`, each path `state:u8, usage:u8, PATH_METRICS:116B` |
| 38 | `OPEN_IP_TUNNEL` | `tunnel_id:u64` |
| 39 | `IP_TUNNEL_READY` | `tunnel_id:u64, mtu:u16, address_count:u8, addresses[address_count]` |
| 40 | `IP_PACKET` | `tunnel_id:u64, packet_id:u64, length:u32, bytes` |
| 41 | `IP_TUNNEL_CLOSE` | `tunnel_id:u64, reason:u8` |
| 42 | `STREAM_REQUALIFY_DATA` | `stream_id:u64, probe_id:u64, offset:u64, length:u32, bytes` |
| 43 | `STREAM_REQUALIFY_ACK` | `stream_id:u64, probe_id:u64, offset:u64, payload_bytes:u32` |
| 49 | `STREAM_RETURN_PLAN_FINAL` | `stream_id:u64, retained_count:u8, retained_ordinals[retained_count]` |

Kinds 5, 6, 15, 19, 25, 26, 28, 29, and 44 through 48 are reserved and
MUST NOT be sent. A receiver rejects them as unknown kinds.

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
to the carrying authenticated carrier instance. For kind 42 the carrying
attachment is the proved forward target. For kind 43 the carrying attachment
is only authenticated return service; the sender's exact pending tuple names
the forward target. A reusable path ID or the ACK carrier cannot substitute
for either exact attachment incarnation.

Kind 49 carries the one-shot response-startup finalization from Section 8.1.
It is valid only for an existing reliable stream and may be sent on its current
attachments. The retained ordinals are strictly increasing, each is less than
the retained `candidate_total`, and each names an exact startup attachment
enrolled under the same frozen return-plan signature. An empty retained set is
valid. The first valid frame is absorbing; an equal duplicate is idempotent and
a different duplicate is a protocol violation.

### 12.3 Common field encodings

Each `ranges[range_count]` entry is `start:u64, end:u64` and represents
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
Return-plan phase values are `STARTUP = 0` and `ORDINARY = 1`.

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
- output-admission and attachment epochs, queue reservations, and Product
  flight records;
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

Loss of an established QUIC connection or non-clean terminal loss of its
HTTP/3 carrier-control request stream is a failure of that exact carrier
instance, not of every Product stream using the session. Carrier recovery
preserves the logical stream and its exact retained ranges on surviving
authenticated attachments. Frame-codec, authentication, configuration, and
Product protocol failures do not acquire that recovery authority merely
because they were observed through a QUIC carrier.

Peer abandonment of one operation-scoped HTTP/3 request-stream direction with
application code zero is an operation-local, error-free shutdown signal. It
MUST NOT alone publish carrier failure or warn as a carrier runtime error; the
connection and sibling request streams remain authoritative. A nonzero
application error, malformed/truncated frame, or non-clean terminal loss of a
carrier-control stream keeps its ordinary smallest-safe-scope failure
semantics.

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

Peer metrics, usage, and capacity-measurement receipts are authenticated input.
They MUST NOT:

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

Within the regular or backup set selected by Section 7, ordinary original-data
placement uses the advisory action rank in Section 10.2 subject to shared
receive credit, configured Product resources, current attachment/output
lifecycle, reorder bounds, and exact writer reservation. The rank does not
claim receiver completion time and cannot deny the only action whose
authoritative owners admit the command.

The output carrying the contiguous frontier is governed by shared MPP credit
and its native carrier. Before an additional output in either stream direction
has durable, unambiguous Data ACK coverage for original transmissions, it may
own at most one bounded startup flight. Native TCP ACK or QUIC packet-ACK
evidence alone does not unlock mature additional-output placement.

That live-contiguous treatment applies only while no retained complete Data ACK
proves the lowest outstanding range missing. When a complete Data ACK omits
that range, the frontier becomes an authoritative-gap frontier. Its exact owner
remains the ordering, hysteresis, and recovery reference, but fresh originals
on that owner use the additional-output Product and reorder position until Data
ACK progress advances or resolves the gap. Other eligible outputs are not
globally paused. An incomplete positive ACK cannot create this state, and this
rule changes neither configured reorder resources nor native recovery.

An output does not become the contiguous-frontier owner merely because it is
the only output that can currently enqueue. While an unresolved lower original
range belongs to another output, the survivor remains an additional output and
retains the corresponding Product flight and reorder bounds.

For an active output, `q_i = 0` means unqualified and `q_i = 1` means
qualified. After current-generation unambiguous OriginalData Data ACK coverage
reaches the configured positive qualification floor, the output gains durable
qualification `q_i = 1` and its configured `P_i` assignment authority.
Duplicated Product bytes satisfy neither the volume floor nor a
carrier-specific delivery sample. NativeOperational evidence remains
independently scoped under Section 10.2. An application-limited native
observation has only the update effect
declared by that native-controller adapter and neither creates nor revokes
Product qualification.

Reliable OriginalData uses separate Product-resource, lifecycle, and native-
transport authorities. Its Product byte authority is independent of traffic
class and TCP or QUIC underlay. Let:

    I_0 = 10 * 1460 = 14,600 Product payload bytes

    W   = min(configured stream window,
              configured repair window,
              configured reorder window)

    P_i = min(W, configured path-flight window)

    E_i = min(P_i,
              max(I_0,
                  configured qualification floor
                  + maximum atomic Product quantum))

W is the shared logical-stream Product resource envelope. P_i is exact output
i's configured Product envelope for unique bytes awaiting MPP Data ACK. I_0 is
portable startup Product geometry, not a claim about TCP MSS, QUIC PMTU,
native congestion window, or achieved capacity. E_i is the Product risk cap
for an unqualified additional output. Native windows, native send credit,
pacing, or connection-wide QUIC limits cannot enlarge these Product resources;
they remain independently enforced below them. Publication of zero P_i is
complete negative output authority.

Traffic class controls arbitration priority, path activation, and maximum
atomic service quantum. It MUST NOT select a smaller W, P_i, initial reliable
receive grant, or Data-ACK release rule. The initial STREAM_MAX_DATA authority
is W; later grants are monotonic under Section 8. Advertising that authority
does not allocate W bytes and bypasses no stream, repair, reorder, sparse-node,
queue, or native-writer bound.

These Product envelopes are Data-ACK-clocked resource windows. If tau is the
elapsed time from assignment until advancing Data ACK can release authority, a
sole output can sustain no more than 8*P_i/tau bits per second from P_i alone,
all outputs together no more than 8*W/tau from W alone, and an unqualified
additional output no more than 8*E_i/tau until qualification. A profile
claiming Product rates therefore declares `R`, the claimed rate for a sole
output, and `R_aggregate`, the claimed aggregate rate across outputs. It needs
`P_i` at least `ceil(R*tau/8)` for the sole-output case and `W` at least
`ceil(R_aggregate*tau/8)` for the aggregate case over its claimed feedback-
delay envelope. This is a necessary resource condition, not a throughput
promise.

Let O be the stream's unique OriginalData debt and O_i the subset assigned to
exact output i. The effective assignment envelope is:

    L_i = P_i  for the first owner when O = 0;
          P_i  for the exact owner of a live contiguous frontier;
          P_i  for a qualified additional output;
          E_i  for an unqualified additional output.

An authoritative-gap frontier and a sole currently enqueueable survivor whose
lower range belongs to another output are additional outputs under this rule.
For an exact pending OriginalData quantum of N bytes, commitment requires
O+N <= W and O_i+N <= L_i, plus shared receive credit, structural eligibility,
reorder authority, and a real bounded writer-command reservation. The complete
quantum must fit; there is no overshoot exception.

After obtaining the writer reservation, the sender revalidates the exact
output and attachment incarnations, output-admission epoch, current position
and qualification, W, P_i, E_i, receive credit, and source frontier. It then
records Product ownership before publishing the command. Failed revalidation
refunds the uncommitted reservation and changes no Product range. Data ACK or
terminal Product cleanup releases O and O_i exactly once; native ACK does not.

Ordinary numeric order uses only the typed action terms in Section 10.2.
Sampled native queue, flight, loss, ECN, confidence, application-limited state,
active-flow count, and Suspect label may validate typed evidence or diagnose
service, but do not add independent score penalties and MUST NOT multiply or
divide a physical carrier rate. They neither shrink nor enlarge W, P_i, E_i, or
shared receive credit.

The selected TCP or QUIC writer, its bounded command admission, native socket
or stream backpressure, pacing, congestion control, and recovery remain final
native transport authority. An advisory score MUST NOT install another Product
congestion gate above that writer. A native ACK may reopen native admission but
cannot release Product debt. Recovery authority remains separately bounded and
cannot mint fresh OriginalData.

Attachment membership is not active path demand. A carrier-open transaction
may publish one prospective load claim while asynchronous I/O is outstanding,
but releases it when the attachment commits, fails, is cancelled, or is
rejected. A current attachment publishes active Product demand exactly while it
owns un-Data-ACKed unique OriginalData. ReinjectedData does not create or retain
that demand. Detach removes demand synchronously before asynchronous wire
cleanup; old-incarnation Product debt remains available for ACK and recovery
and MUST NOT be projected into a same-key physical successor.

Fresh OriginalData is reserved in the shared bounded carrier command queue
before Product flight is published. That queue is the staging resource and
reservation linearization point for Product actors sharing one native writer.
Its pending-byte accounting is resource state, not renewable send credit. The
writer may continue through a bounded sequence of reserved commands without
waiting for a native ACK, but re-enters class and dependency arbitration after
each command. Control and ReinjectedData retain their priority admission while
the common queue and configured Product envelopes bound aggregate memory and
ordering debt.

Current local controller application-limited state is separate from the
application-limited provenance of a qualified rate epoch. Retaining, replacing,
or invalidating evidence MUST NOT rewrite that native state, and peer telemetry
MUST NOT supply it. Native admission participates symmetrically in both
directions through exact writer reservation and native backpressure. There is
no request-only QUIC tie-break.

Connection-wide source staging precedes stream-offset assignment and may
contain bounded work for several independently admitted outputs. It is
governed by the shared stream, repair, reorder, and configured resource
envelopes, not by one selected output's native congestion window. For reliable
bulk work, one coherent view first selects the eligible output tier and then
sets the allowance to:

    min(W, sum(P_i for each output in that selected tier))

Withdrawn, inactive, unschedulable, or zero-P_i outputs contribute zero.
The exact tier order is non-stale Regular, non-stale Backup, stale Regular,
then stale Backup; only the first nonempty eligible tier contributes. With one
eligible output the allowance is exactly P_i. Traffic class may keep source
reads and each atomic service turn smaller, but it does not replace this
Product byte authority. Staging grants no output ownership or native
reservation; every assignment still passes the exact checks above.

An authenticated admission-active attachment may precede its first exact-
instance measurement. It remains unproven and uses only configured startup
priors and startup-flight bounds. Absence of measurement is not absence of an
output; source admission is zero when the coherent selected tier contains no
current eligible output with a positive Product envelope. Evidence from
another carrier incarnation with the same path key MUST NOT be substituted.

The Core startup score uses the following portable priors only where a more
specific configured or typed observation is absent:

    M_0   = I_0 + 30 = 14,630 normalized MPP bytes
    RTT_0 = 333 ms
    T_0   = RTT_0 / 2
    J_0   = RTT_0 / 2
    C_0   = 8 * M_0 / RTT_0, approximately 351 kbit/s

A low or missing C orders alternatives but does not rate-limit, window-limit,
or pace the sole admitting carrier. Missing evidence is never measured zero.

Each output-admission epoch is checked and non-reusing within its exact output
incarnation. Attachment admission creates the initial epoch. Revocation makes
that epoch non-admitting before asynchronous cleanup. Exact requalification
activates only the already-advanced successor epoch with q_i reset; it does not
inherit predecessor Product qualification, rate evidence, queue reservation,
or byte authority. A delayed predecessor event cannot revive the successor.
Exhaustion leaves the output non-admitting until attachment replacement.
Output or direction terminal revokes its current epoch without acknowledging
retained Product.

An OriginalData flight is evidence-eligible only when it was committed under
the named current output-admission epoch and exact attachment incarnation while
that output was admission-active and non-stale, and no later exact lifecycle
transition has invalidated that evidence. The bit is retained with the exact
flight and is cleared when that output enters stale/requalification authority;
it cannot be reconstructed from a current output that merely reuses the same
path key. Evidence eligibility grants no Product or native authority.

For each active unqualified output, one Product qualification generation begins
when the serialized stream owner commits its first current-epoch OriginalData
quantum to that output. The commit freezes the exact positive configured
qualification floor F_i and exact positive maximum atomic Product quantum
N_i^max used by that commit and by E_i above. F_i MUST be representable by the
implementation's range-index and byte-measure integer types. A later commit in
the generation MUST present the same frozen pair; a mismatch fails without
mutation. The commit also records the exact output-admission epoch and a tagged
prefix of that OriginalData before publishing the already-reserved command.
The generation grants no rate, credit, recovery, pacing, or native authority.

Let T_i be the normalized set of nonempty disjoint outstanding tagged ranges,
M_i its byte measure, and V_i the exact uniquely Data-ACKed tag volume. Before
mutating the generation for an already admitted quantum, the implementation
MUST prove 0 < N <= N_i^max, that the range is fresh non-reinjected
OriginalData for this exact output and epoch, and that it overlaps no current
tag. It then tags only the deterministic prefix of length:

    x_i = min(N, F_i - V_i - M_i)

when the subtraction is positive. Only unambiguous MPP Data ACK coverage of
that exact current-generation OriginalData moves tag weight from M_i to V_i.
Reinjection, ambiguous coverage, or terminal cleanup removes overlapping
outstanding tag weight without increasing V_i. Let items(T_i) be the number of
retained ranges. Because every normalized integer range is nonempty, every
transition is clipped to the exact epoch and tagged ranges and preserves:

    items(T_i) <= M_i <= F_i
    0 <= V_i + M_i <= F_i

A first/frontier commit may carry an admitted surplus beyond the remaining tag
deficit; that surplus is useful Product but not qualification evidence. A
rejected parameter, range, overlap, or authority check changes no generation
state. No fallible step may remain between qualification/Product metadata
mutation and publication of the already-reserved command.

Accepted reinjection and Data ACK application for one stream direction share a
serialized order. Exact qualification metadata is recorded before publishing
the corresponding original command, and only an opaque receipt carried by the
exact overlapping OriginalData flight can release a tag. If reinjection is
recorded first, it removes overlapping current tags before a later ACK can
verify them. If exact ACK application occurs first, it may verify the still-
unique tag because a later duplicate cannot have caused that earlier ACK. An
ACK received earlier but applied after reinjection is conservatively ambiguous.
Native-carrier concurrency does not relax this Product-level serialization.

When V_i reaches F_i, q_i becomes one even if the same ACK has no usable timing
sample. Qualification is durable only for that active output incarnation and
epoch. Rate changes, usage, rank, and application-limited observations preserve
it. Stale or Requalifying entry, detach, or exact incarnation replacement
revokes it, advances the output epoch once for that inactive interval, and
resets q_i, V_i, and M_i without erasing unresolved Product debt. After exact
requalification, only a later current-incarnation OriginalData commit can
begin a new generation; predecessor tags cannot be reused.

The retained OriginalData debt invariants are:

    sum(O_i over all exact original owners) = O
    O <= min(W, configured reorder authority)
    O_i <= P_i

E_i is a prospective commit ceiling, not a retroactive debt invariant. Every
new unqualified-additional commit must leave O_i within its then-current E_i.
A first/frontier or previously qualified output may retain O_i greater than a
later E_i after a role transition, qualification revocation, or configured
envelope reduction. Those transitions preserve exact debt under P_i, W, and
reorder authority and admit no new unqualified-additional OriginalData until
Data ACK or terminal cleanup restores current E_i headroom.

Every ordinary positive quantum freezes a finite candidate order by structural
tier, Section 10.2 score, uncertainty, and canonical action identity. It tries
each candidate until one real writer reservation and every Product authority
succeed, then ends after that one commitment. It does not allocate equal shares.
Backup is considered only after every regular candidate fails the exact
commit. A backup uses the same L_i rule and one backup commitment never promotes
it ahead of regular candidates for a successor quantum.

Each exact writer-admission resource owns a checked monotonic capacity
generation. Reservation acquisition or refund, dequeue, close, policy or
class-limit change, and an applied native-ready event advance it when the
transition can change a positive reservation result. A captured proposal
revalidates that generation at commit. Exhaustion invalidates captured
proposals and prevents new reservation on that resource while preserving
dequeue, refund, and terminal cleanup; replacement uses a fresh resource
identity. This generation is a race detector, not byte credit or rate evidence.

A zero-commit regular pass records the membership, lifecycle, Product-
authority, and writer-capacity generations it exhausted. A backup proposal is
valid only while those generations remain unchanged. An external advance
before backup reservation restarts the regular pass. The generation advance
caused by the successful backup reservation is part of that same transaction
and cannot invalidate itself.

A zero-commit scan drops every temporary reservation, arms the relevant source,
Product-authority, writer-capacity, topology, and terminal wakes, rechecks exact
state, and then parks. It cannot spin or treat advisory queue readiness as a
reservation. Additions wait for a successor attempt; removal or replacement is
skipped by exact revalidation.

A positive ordinary commitment is durable successor work. While staged bytes
remain and one exact candidate currently satisfies Product resources, receive
credit, reorder authority, and writer readiness, the direction reconsiders
higher-priority work and attempts a successor quantum without waiting for an
unrelated socket, timer, ACK, or topology event. One actor turn may end after a
bounded number of commits for cooperative fairness, but publishes one coalesced
self-wake before yielding when that same exact predicate remains true. A raced
predicate makes the finite scan arm exact wakes and park; it cannot self-wake
again without positive work.

The response-startup return plan in Section 8.1 is a separate finite one-shot
prefix transaction. Its unresolved prefix ceiling does not refill on Data ACK,
does not become a recurring Product window, and does not alter later ordinary
ranking after finalization.

TCP pool establishment remains owned by Section 7.2 and independent of
instantaneous Product demand. A ready member enters the regular or backup set,
receives no fixed traffic share, and uses the same Product, rank, queue, and
native authorities as every other carrier. No Mbps value, transient utilization
percentage, source address, locator, interface identity, application-flow
count, laboratory condition, or fixed observation window may create, promote,
or revoke a TCP pool member. Exact native failure changes liveness immediately;
planned changes follow Section 7.2.

Core does not infer a common bottleneck from path membership or transient
comparative throughput. It makes no claim that more carriers aggregate
capacity, that two carriers are independent, or that one scalar group can model
overlapping shared resources. Such inference is outside Core Profile 7.

### 15.2 Reinjection budget and timing

Ordinary optional reliable payload is limited by cumulative extra-traffic
credit funded by a bounded startup allowance and unique bytes acknowledged by
MPP Data ACK.
The Product default is 10 percent. `[flow].optional_reinjection_budget_percent`
sets the local sender default and an MPP inbound/outbound performance value may
override it for that node. The value is directional and peers do not negotiate
it. It meters optional repair reinjection, including persistent authoritative
gap repair while the exact original carrier remains live, and stale-path
requalification payload. It does not meter native transport retransmission,
MPP control frames, or the cause-bounded critical recovery authority defined
below.

Exact carrier-instance failure permits immediate bounded reinjection on an
eligible live alternative. A measured survivor is preferred, but liveness is
sufficient when no measured survivor remains.

A complete Data ACK snapshot may establish omitted ranges. Later positive
partial ranges extend known progress but do not establish omissions alone.

The MPP recovery interval uses the original carrier's underlay and latest
snapshot. When that snapshot contains an observation, let `srtt` and `jitter`
be its nonnegative directional smoothed-RTT and jitter durations:

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
snapshot with an omitted range, define two absolute clocks for every exact
OriginalData assignment span participating in the candidate extent:
`loss_at_j` is that span's immutable assignment epoch plus the local MPP
Data-ACK threshold, and `fallback_at_j` is that epoch plus the MPP recovery
interval. The aggregate candidate clocks are the maximum corresponding
absolute deadlines, as specified below. The thresholds are:

- `5/4 * SRTT` for TCP; or
- `9/8 * SRTT` for QUIC;

For a candidate extent, let `loss_at` be the maximum applicable `loss_at_j`
and `fallback_at` the maximum applicable `fallback_at_j`. If any participating
span lacks `loss_at_j`, the aggregate `loss_at` is absent and pre-fallback
speculation is disabled. No earlier than `loss_at` and before `fallback_at`,
a sender MAY offer the exact frontier as
bounded speculative repair on a currently measured, distinct alternate, but
only from already-funded optional credit and the existing target, queue,
flight, and retained-range bounds defined below. Section 10.2 ranks eligible
alternate repair actions using one common captured positive frontier payload,
also defined below. That rank
neither scores the already-accepted owner with a zero-size action nor proves
that an alternate will complete before the owner. Exercising or declining this
optional opportunity is local policy and grants no additional traffic
authority. Without a current measured alternate there is no pre-fallback
target-bound candidate; the independent owner fallback remains retained.

At or after `fallback_at`, an eligible measured distinct alternate may perform
bounded optional repair without an owner-completion comparison. Crossing
`fallback_at` makes the first shared live-owner attempt token eligible. It
neither proves native failure nor waives optional credit beyond the exact
frontier floor defined below. These loss and fallback comparisons are
scheduling hints; they do not cap native rate, ordinary Product placement, or
path capacity. Later attempts follow the same shared-token rule.

The repair uses exact target `t`'s current published Product envelope `P_t`,
already bounded by shared `W` and the configured repair and path-flight
envelopes. Recovery MUST NOT reconstruct `P_t` from an unscoped carrier rate or
from a native-window observation of an older epoch. Let `O_t` be exact
un-DataACKed OriginalData on that target and let `A_t^r` be one
repair-admission quantum. This symbol is distinct from the unqualified
OriginalData Product envelope `E_i` in Section 15.1.
`B_t` is queued ReinjectedData bound to exact target `t`; `U_s` is
target-unbound queued ReinjectedData in the current stream and direction; and
`J_t` is every un-DataACKed ReinjectedData byte already accepted by exact
target `t`. Repair bound to another target is excluded. Raw OriginalData
staging, control work, other streams, aggregate path-health Product flight,
and sampled native queue or packet flight are excluded from the repair
accounting below.
The target's Product repair capacity is:

```text
repair_cap_t = max(saturating_sub(P_t, O_t), A_t^r)
R_t = B_t + U_s + J_t
K_t = repair_cap_t - R_t                 (saturating at zero)
```

Those excluded categories do not contribute to `R_t`.

For one logical-stream send direction `s`, let `C_s` be remaining cumulative
optional-reinjection credit after the minimum-useful-attempt rule. Let `G_s` be
its single non-accumulating live-owner attempt token, shared by authoritative-
gap and contiguous-tail recovery. For byte `x`, let `O_s(x)` be the complete
set of live exact OriginalData output incarnations covering it and let
`A_s(x)` be the complete set of exact output incarnations with unresolved
OriginalData or accepted ReinjectedData covering it. Let `V_s` be the byte
length of the maximal retained contiguous prefix from the lowest uncovered
byte on which `O_s(x)` is the same non-empty singleton and `A_s(x)` is
unchanged. A cache or
application-write boundary alone does not end `V_s`; a retained-data hole,
ambiguous owner, non-live owner, or exact identity-set change does. Let `H_s`
be the byte length of the uncovered portion of the exact lowest retained
Product frontier, let `Q_s^r` be the direction's common immutable repair
quantum captured for target ranking, and let
`M_s = min(Q_s^r, H_s, V_s)`. After selecting a bound target,
Apply defines:

```text
F_t^r = min(K_t, A_t^r, M_s)
```

Thus Apply may shrink the ranked frontier for exact target capacity but may
never enlarge or skip it, and the complete bound service prefix is capped by
`V_s`. Target-unbound repair has no independent Product-capacity grant. Before
target binding, its queued extent is bounded by the retained lowest frontier,
captured common repair quantum, retained repair debt, configured repair and
path-flight envelopes, and, for optional work, `C_s`. Queued target-unbound
ReinjectedData `U_s` is conservatively included in `R_t` for every eligible
target. At dispatch, after reserving the actual writer command, Apply
recomputes that exact target's `K_t` while excluding only the current front
intent and commits the complete frame only when its payload fits; otherwise it
cancels the reservation and reevaluates. Queueing unbound work neither creates
nor preserves target service authority.

For every OriginalData assignment span `j` intersecting `M_s`, retain its
immutable assignment time `a_j` and applicable owner recovery interval
`R_o,j`. The exact owner-fallback boundary is
`fallback_at = max_j(a_j + R_o,j)`; the early loss boundary is aggregated from
its corresponding absolute per-span deadlines. Therefore post-fallback
authority begins only after every byte in the ranked prefix has matured.

No earlier than `loss_at` and before `fallback_at`, a permitted speculative
authoritative-gap attempt has `L_t = min(K_t, V_s, C_s)`. At or after
`fallback_at`, it has:

```text
L_t = min(K_t, V_s, max(C_s, F_t^r))
D_t = max(0, L_t - C_s) <= F_t^r
```

If `G_s` is consumed, or the owner boundary has not elapsed, the over-credit
floor is unavailable but funded work remains
`L_t = min(K_t, V_s, C_s)` and has
`D_t = 0`. Thus the frontier floor replaces, and never adds to, optional
credit; it does not suspend cumulative optional service. A full persistent-gap
target window remains optional and cumulative only within the same exact
identity-uniform prefix. Since `F_t^r <= M_s`, any accepted suffix after
`F_t^r`
has `D_t = 0` and is funded entirely by cumulative credit. Every suffix slice
MUST remain retained and unacknowledged, keep the same exact identity sets,
exclude frozen target `t`, and pass fresh exact Product and native admission;
the first overlap, rejection, or identity change ends the prefix without
skipping ahead. Exact terminal carrier failure uses separate cause-bounded
critical authority, is not capped by `C_s`, and remains charged against later
optional work.

Successful admission of a live-owner gap or tail repair batch to the
serialized Product queue at or after `fallback_at` while `G_s` is available --
optional, partly critical, or critical -- consumes `G_s`. Optional-funded
service admitted before `fallback_at` or while that token is already closed
neither requires nor renews it. A later abandoned
native-writer attempt therefore retains a consumed token until its fixed
interval expires. Queue removal, copy expiry, target
replacement or churn, gap/tail reclassification, metric or evidence
publication, and actor reevaluation cannot renew it. With no contiguous unique
Data-ACK frontier progress, exactly one successor token becomes available only
after the accepted attempt's fixed MPP recovery interval; missed intervals do
not accumulate. Contiguous unique Data-ACK frontier progress restarts a full
no-progress interval and cannot move an existing deadline earlier. Sparse
suffix ACK release does not restart it. Gap and tail observations cannot each
spend a token in the same interval.

Global retained repair debt and configured resource ceilings further cap
`K_t`.

When ordinary target headroom is full, `A_t^r` is one single outstanding
emergency reserve for the exact directional recovery target. It is not renewed
per range, timer expiry, evaluation, actor wake, or native queue drain.
Target-unbound `U_s` reduces every eligible target's `K_t` until assignment; it
does not mint a separate reserve. After assignment it consumes only its exact
target's reserve. The actual bounded writer-command reservation is the native
admission boundary. After obtaining that reservation, the serialized Product
actor MUST revalidate `K_t` while excluding the current front intent from
`B_t`/`U_s`, record the accepted exact copy in `J_t`, and only then commit the
writer reservation. Failed revalidation drops the reservation without
recording the copy.
If the highest-ranked target has neither ordinary service headroom nor
emergency reserve, recovery MUST continue through the remaining eligible
regular-before-backup targets and block only when none can admit the frontier.
Recovery debt, flight, and deadlines MUST NOT transfer between exact targets.
This is Data Sequence service authority, not native congestion
authority: the selected TCP or QUIC sender remains the final enqueue, pacing,
congestion, and recovery authority. These ratios are local approximations
inspired by transport time-threshold loss detection; the TCP ratio is not RFC
8985 RACK and the QUIC ratio is not QUIC's native RFC 9002 loss decision. A
multi-frame cumulative publication consists only of positive, incomplete
frames and cannot establish an omission. Later incomplete updates may fill a
gap already established below a retained complete snapshot's horizon, but
cannot extend that horizon or create negative evidence. Both directions use
the immutable epochs of every exact OriginalData assignment span in the
candidate extent. For the same exact gap and assignment spans, later owner
observations may move a threshold-derived deadline earlier but MUST NOT restart
an assignment epoch or aggregate clock later. Alternate eligibility and the
advisory action rank are evaluated from the current target; a departed target's
rank MUST NOT be inherited by its replacement. Exact ownership and a live
measured distinct alternate are required for target-bound live-owner repair;
otherwise ACK silence waits until the one-interval `fallback_at` rather than
erasing that fallback.

Recovery target ranking and commitment MUST refer to the same lowest-missing,
identity-uniform frontier. Decide ranks candidate targets using the common
captured payload `M_s`; after selection, Apply may shrink the ranked frontier
floor only to exact `F_t^r`. A separately funded suffix may extend total service
to `L_t` only under the rules below.
The first committed repair frame has the same lowest offset, normalized
frontier identity, output incarnation, and writer-capacity generation used by
the frozen Section 10.2 target order, and its payload MUST NOT exceed `M_s`.
If that frame
cannot be committed because it overlaps queued or recent
repair work, the evaluation MUST stop without publishing later omitted ranges.
After the frontier quantum is committed, the sender may fill the remainder of
the same bounded effective target service window `L_t` behind it only while
the frozen target remains absent from the unchanged `A_s(x)` set. A larger
coalesced batch or whole-window throughput estimate MUST NOT replace the exact
frontier-quantum carrier rank as the primary target objective.

When a target-bound live-owner gap or speculative-tail repair batch is accepted,
its immutable next-attempt deadline is fixed from the selected alternate's
observed MPP recovery interval. An accepted target-unbound tail instead fixes
the then-observed original-owner tail interval. If one serialized batch admits
frames with different exact targets or owners, its fixed interval is the
maximum of every actually admitted frame's applicable interval; rejected or
overlapping suffixes contribute nothing. The shared token remains
unavailable until that deadline and requires no contiguous unique Data-ACK
frontier progress. Target or evidence changes cannot move it. Contiguous
unique Data-ACK frontier progress restarts a full interval; sparse suffix ACK
release does not. Advancement or resolution clears obsolete exact-gap
identity but does not create immediate repair authority. A later target-bound
attempt reselects a currently measured alternate and remains bounded by its
available Product service window. Thus a degraded original owner cannot impose
its longer recovery clock on an already accepted target-bound attempt, while
mutable later measurements cannot postpone any accepted attempt's deadline.
ACK receipt, recovery deadlines, carrier-capacity release, and output-model
publication all return through the same stream-owner evaluation. Polling or
another wake cannot restart either silence clock.

A contiguous live tail without an authoritative gap uses the same `G_s`.
After the owner boundary it may cross remaining optional credit by at most
the applicable bounded frontier quantum. Any larger admitted tail is funded by
`C_s`, and every target-unbound native dispatch revalidates exact `K_t`.
Admission while the token is available consumes the same opportunity as gap
repair. Another over-credit attempt requires the shared full no-progress
interval.

For the finite-drain rule below, MPP Data ACK progress means newly
acknowledged unique Product bytes. Receipt or republication of an unchanged or
subsumed Data ACK remains stream activity but does not rewrite an
OriginalData range's assignment age.

Once the sending application fixes a final offset, a remaining exact
OriginalData range also has an immutable finite-drain age from its original
assignment. After one owning-path MPP recovery interval, the sender MAY race
one bounded over-credit frontier quantum of that range on a distinct output
when both outputs have current carrier service evidence and the ordinary `S/U`
rank favors the alternate outside its uncertainty deadband. A larger suffix is
permitted only when separately funded by cumulative optional credit and bounded
by the same exact-target and identity-uniform-prefix rules above. Partial Data
ACK progress shrinks the retained range but does not rewrite any remaining
original Product flight's assignment time.
This finite-tail rule does not mark the original attachment stale, withdraw it
from ordinary placement, or replace native recovery. Exact range identity,
repeat-delay suppression, shared credit, queue, flight, repair, reorder, and
extra-traffic bounds continue to apply.

When response finite-tail Decide ranks and sizes an exact alternate output,
the serialized intent MUST carry that exact output incarnation and a finite
validity deadline through Apply and native dispatch. Dispatch revalidates its
current Product and native admission and either commits that same output or
cancels the intent for fresh evaluation; it MUST NOT silently retarget the
already-ranked batch. Exact output replacement or validity expiry removes the
intent so it cannot remain as an impossible Product-queue head.

For placement persistence, a distinct structural Product alternative exists
only when, after excluding the candidate owner, ordinary regular-before-backup
selection for the effective current lane contains a current exact, non-stale
attachment whose carrier is authenticated and ready, whose Product admission
and accepted directional usage are active, and whose local health and lane
policy permit payload scheduling. Failed, draining, retired, control-only for
a payload lane, or otherwise lane-ineligible attachments do not qualify. A
native or Product rate sample and immediately available queue, flight,
receive-credit, or pacing capacity are not part of this structural predicate;
every actual recovery commitment still MUST pass those ordinary bounds.

While that distinct-alternative predicate holds, original placement in either
direction stops selecting a non-progressing attachment after four TCP MPP
recovery intervals or three QUIC MPP recovery intervals. Every exact
unacknowledged OriginalData range owned by that stale attachment then becomes
connection-level reinjection work on a distinct non-stale attachment. This is
placement withdrawal and bounded live-tail recovery, not negative evidence
that every such byte was lost. Native recovery remains active. The work is
admitted through the existing shared receive-credit, retained-range, repair,
queue, and native-enqueue bounds.

Recovery suppression is exact-range state, not one clock for the complete
owner. Pre-commit overlap in the sender-service queue suppresses duplicate
intent only until carrier commitment. After the selected exact recovery
carrier successfully reserves its command-queue slot, the actor samples that
carrier's immutable observation, computes one MPP recovery interval, and stores
the absolute deadline `D = accepted_at + interval` with the ReinjectedData
flight before committing the reservation. This covers only that copy's Data
Sequence range. It MUST NOT delay a disjoint stale-owned range that has no
current recovery copy. Later RTT, jitter, rate-model, lane, policy, usage,
qualification, or ordered-drain changes MUST NOT move `D` or erase the exact
copy's native recovery ownership. Exact Data ACK or exact attachment detach or
failure ends that copy's retained target ownership. Deadline expiry ends only
its duplicate-suppression authority across the target set: the range becomes
eligible on another exact target, but the accepted copy remains in `J_t` and
the same exact reliable target remains ineligible for that range until Data ACK
or its terminal attachment boundary. Native queue drain, packet ACK, stream
write completion, or another timer MUST NOT release
`J_t`; TCP and QUIC already
recover the accepted bytes on that live reliable carrier. A replacement
incarnation does not inherit the old target's `J_t`. The structural alternative
predicate gates a new Product recovery commitment; it is not the lifetime of a
copy already committed. If no other exact target is eligible when `D` expires,
MPP waits for Data ACK, a target-set change, or an exact terminal event instead
of duplicating native recovery on the same survivor. The successful commitment
directly publishes `D` into the actor's
durable one-shot wake, even if `D` becomes due before the next serialized actor
observation. That next exact-state observation reconciles or cancels a future
wake after Data ACK, detach, or failure. A due one-shot is consumed by the
serialized recovery pass before awaited topology work; an unrelated ready
event cannot erase it, and consuming one due deadline MUST preserve a later
deadline for a disjoint copy. Rejection of a completed topology open MUST
transfer the exact uncommitted stream to the carrier-owned retirement mailbox,
which preserves detach-before-close ordering without making the Product actor
await bounded command capacity; completed-open ready-drain therefore cannot
hold the actor across an armed `D`. Thus every range is accepted at most once
by each exact reliable target incarnation until MPP Data ACK covers it or that
target becomes terminal, while the existing queue, exact Product flight,
repair, and extra-traffic bounds limit aggregate work.

Lifecycle and Product qualification are independent state axes. The
directional attachment lifecycle is `Active`, `Stale`, `Requalifying`, or
`Detached`; an active exact incarnation separately owns the `q_i` and
qualification generation defined in Section 15.1. Stale entry revokes that
Product qualification generation and makes `q_i = 0`. Entry into
`Requalifying` or `Detached` has the same output-local clearing rule. It does
not terminate the physical carrier or rewrite native-controller state.
Neither a later requalification nor an otherwise equal path identifier
inherits output-local Product evidence.
An exact requalification receipt moves
`Requalifying` to `Active(q_i=0)` under the already-advanced output-admission
epoch, and only the ordinary capped-volume rule can
later produce `Active(q_i=1)`. `Acquiring` is therefore a derived description
of an active unqualified attachment with a current generation, not a fifth
lifecycle state and not rate authority.

A stale attachment remains a sole-survivor fallback when no non-stale active
attachment is schedulable, but fallback use MUST NOT clear or rewrite its stale
evidence, start a qualification generation, or set `q_i`. Product drain or
ordered detachment already withdraws new placement and therefore MUST NOT
suspend retained stale-owner recovery through a distinct structural
alternative. Completion of exact detach transfers any remaining range to
exact-failure recovery without creating a Data ACK gap. The carrier remains
connected and native recovery continues throughout its owned lifetime.

Requalification arbitration owns one bounded cyclic cursor per stream
direction and never changes ordinary Product order or an attachment's
configured Regular/Backup usage. With no transaction pending, stale entry,
pending completion or expiry, retained-copy arrival, optional-budget or
critical-authority availability, writer-capacity change, topology or policy
change, and exact terminal change each trigger one pass. The pass freezes the
finite exact stale attachment incarnations that are currently authenticated,
policy-eligible, and able to carry the direction, beginning after the last
surviving cursor boundary. Locator strings and reusable path IDs are not cursor
identity. Regular and Backup attachments share this non-delivering maintenance
ring; successful proof never promotes Backup usage.

One pass visits each frozen target at most once. A removed, replaced, already
requalifying, locally blocked, or otherwise changed target is skipped without
rewind; a failed reservation is refunded; additions wait for a successor pass.
Successfully publishing one probe advances the cursor to that exact target,
moves it to `Requalifying`, and ends the pass. If no target commits, the actor
arms exact retained-data, budget/critical-authority, writer-capacity, topology,
policy, pending-deadline, and terminal wakes, rechecks, and parks. An empty
pass cannot poll or self-wake. Probe-ID exhaustion disables only new
requalification transactions for that session direction without wrapping;
ordinary Product recovery and exact cleanup continue.

Call a frozen target visit-admitting when, at its serialized visit, retained
copy identity, budget or critical authority, Product resources, and the exact
writer reservation all revalidate and that reservation succeeds.
For an initial finite eligible cohort of size `n` with no exogenous membership
or policy changes, suppose retained probe bytes and recurring budget or
critical authority exist, every still-stale target is visit-admitting when
visited, the actor and deadlines are fair, and each prior pending transaction
completes or expires. Within `n` successful probe publications, every initial
target has then either been selected or already left stale lifecycle through a
successful requalification or terminal. A failed target returns to `Stale` on
expiry after the cursor has advanced, while a successful target needs no
further attempt. If one target is locally blocked, the cursor does not let it
block the rest; if it later becomes admitting and publishes the exact capacity
wake, it is selected within `n` subsequent successful publications under the
same remaining premises. This is bounded attempt
fairness, not a delivery promise. Core claims no wall-clock recovery under zero
native or reverse service, persistent higher-priority starvation, absent
retained bytes, permanently unavailable writer authority, exhausted
identifiers, or continuously changing membership.

Re-entry uses `STREAM_REQUALIFY_DATA` and `STREAM_REQUALIFY_ACK`. At most one
requalification transaction may be pending in one stream direction. The
sender copies one bounded retained Product quantum and transmits that copy on
the selected stale attachment. The quantum MAY have any retained OriginalData
owner, including an evidence-ineligible sole-survivor fallback owned by the
selected attachment itself. Owner qualification is not a safety input: the
probe remains non-owning and the exact probe ACK enters only `Active(q_i=0)`.
The probe carries its stream ID, a nonzero
monotonically allocated probe ID, the copied range offset, and the bytes. It is
data-bearing for reachability, native service, queue admission, and extra-traffic
accounting, but it does not own or deliver that Product range: it is not
inserted in the receive map, does not advance a Data ACK horizon, and does not
enter Product flight or delivery evidence. OriginalData therefore remains the
only Product owner, and a lost or reordered probe cannot create Product
head-of-line blocking or make its OriginalData owner's ACK ambiguous.

The receiver authenticates and fully processes the frame under the ordinary
carrier session and returns `STREAM_REQUALIFY_ACK`, echoing the stream ID,
probe ID, offset, and payload length. For one session and stream direction,
define exact probe identity
`P = (stream_id, probe_id, offset, payload_bytes)`. The original sender's one
non-reused pending lookup binds `P` to one exact forward attachment incarnation
`T(P)`. The attachment `R` carrying the ACK is only authenticated reverse
service: it MUST be attached to the named stream in the same authenticated
session, but need not equal `T(P)` and MUST NOT select, replace, or inherit the
target.

The receiver owns at most one exact pending requalification receipt per stream
direction. In one finite pass it MUST attempt one identical ACK on every
currently attached authenticated reverse output whose control queue accepts it
immediately. The probe-carrying attachment MAY be visited first, but preference
is ordering only and MUST NOT end the pass. If at least one copy is accepted,
the pass completes the stream-owned receipt, no later capacity wake retries that
probe, and native reliability owns every accepted copy. If no copy is accepted,
the receipt remains with the logical stream rather than the ingress carrier.
The stream actor MUST prearm all current candidate control-capacity edges before
retry and MUST also wake on attachment membership and terminal changes.

Let `K` be the set of outputs that accept a copy in that pass and `W_ack` the
normalized encoded work of one ACK. The transaction accepts exactly
`|K| * W_ack` work, bounded by the configured live attachment count, and its
delivery delay is no greater than the minimum native service delay in `K`.
Thus fanout gives a finite recovery bound only when at least one accepting
reverse output has finite service. Core claims no such bound when every reverse
writer stalls. Queue admission alone cannot justify choosing one output: for
any deterministic single choice there is an indistinguishable execution where
that writer stalls while another queue-admitting writer has bounded service.

The receiver retains the greatest accepted exact probe tuple after publication.
A lower probe ID is a stale no-op. An equal ID with the same tuple coalesces
with a still-pending receipt and is otherwise a no-op; an equal ID with a
different tuple is a protocol violation. A greater ID supersedes an older
zero-publication pending receipt, which permits sender expiry and legal
cross-attachment reordering without unbounded replay fanout.

On ACK receipt the sender first authenticates `R`, fully validates `P`, resolves
only the exact pending `T(P)`, and then applies the dedicated requalification
effect. A different session,
unattached return carrier, mismatched field, reused ID, stale or absent target,
`PATH_PROOF`, or generic `STREAM_ACK` does not change
qualification state. Target terminal or expiry before effect application makes
the effect stale; a successor never inherits it. Return-attachment terminal
after accepted-receipt linearization cannot retroactively invalidate it.

An exact probe receipt proves only forward directional attachment reachability
plus authenticated reverse session service, activates the already-advanced
output-admission epoch described in Section 15.1, and moves only `T(P)` from
`Requalifying` to `Active(q_i=0)`. It grants no Product, qualification, rate,
usage, health, or native authority to `R`, and MUST NOT restore the target's
prior Product rate, qualification generation, or assignment authority.
Applying that exact effect retires the pending proof identity and publishes the
same successor cursor-pass wake as expiry. The first later exact
OriginalData commit on that attachment freezes a fresh `F_i` and starts the
successor generation under the bounded new-attachment acquisition envelope.
Only exact unique Data ACK coverage that advances its capped `V_i` to `F_i`
sets `q_i=1` and restores qualified `P_i` authority. One post-probe byte, a
native ACK, a capacity-measurement receipt, or a valid low-volume rate sample is
insufficient.

`STREAM_REQUALIFY_ACK` is Product-neutral recovery and settlement control. A
planned drain's Product-admission fence MUST NOT reject it while that carrier's
ordered-control queue remains open, and an accepted copy drains in order. A new
`STREAM_REQUALIFY_DATA` remains data-bearing and Product-admission-gated after
drain begins; a copy already accepted before drain is processed normally. ACK
publication neither cancels drain nor revives or qualifies its return carrier.

Loss of the probe leaves Product ownership unchanged. At successful
publication the transaction freezes the absolute
deadline `D = published_at + stale-attachment recovery interval` from that
exact attachment snapshot. The sender MUST compute `D` by checked addition
before publication; failure refunds every provisional reservation, publishes
no probe, advances the finite pass, and never saturates or wraps. Exhaustion of
the monotonic deadline domain for every candidate disables new
requalification deadlines for that exact session direction. Later metric, role, or policy
changes do not move `D`. At `D`, expiry cancels and refunds a still-removable
queued probe reservation. Work already accepted by the native transport
follows that transport's ordinary ownership and terminal path. Expiry retires
the pending proof identity, returns the target to `Stale`, publishes the next
cursor wake, and cannot leak or double-retire work. A late ACK's expired
requalification effect is a stale no-op. The next selected attempt
uses a fresh probe ID. Probe bytes consume the existing optional extra-traffic
budget and remain charged. Budget exhaustion MUST NOT permanently prevent
re-entry: one minimum useful recovery quantum may be sent per exact stale
interval as critical recovery debt, still subject to the single-pending,
retained-range, queue, pacing, and flight bounds. Thus one stream direction
can add at most one recovery quantum instantaneously and, under persistent
probe loss, at most one quantum per stale interval over time, excluding frame
headers. Later optional reinjection authority remains reduced by that debt.

The placement-persistence clock is independent for every exact attachment
incarnation that owns evidence-eligible OriginalData omitted below a complete
authoritative Data ACK horizon. Positive ACK ranges are
released before this decision, so a retained flight below that horizon is an
authoritative omission. An incomplete ACK may fill an omission below an
existing retained horizon, but cannot extend the horizon or create one. A
successful OriginalData commitment, ACK silence above the horizon, and retained
work not covered by such an omission MUST NOT arm placement withdrawal.

When the distinct-alternative predicate holds, the first stream-owner
reconciliation that observes an authoritative omitted owner arms its clock.
The predicate is evaluated using the effective current Product traffic class,
not a control, reinjection, default, or enclosing-loop lane. At arm time,
compute the owning attachment's then-current MPP recovery interval from that
exact incarnation's underlay and snapshot. The placement-persistence interval
is four such intervals for TCP and three for QUIC, and the stored deadline is
the absolute arm instant plus that interval. While the authoritative omitted
owner and predicate remain valid, scheduler polls, timer wakes, queue or
capacity notifications, or RTT, jitter, loss, and model changes MUST NOT move
that absolute deadline earlier or later.

Only a Data ACK transaction that newly acknowledges evidence-eligible
OriginalData bytes unambiguously attributable to that exact owner and output-
admission epoch may replace its deadline. The attachment carrying the
`STREAM_ACK` is
irrelevant. If authoritative omitted owner bytes and the predicate remain after
that progress, the replacement deadline is the transaction observation instant
plus a newly computed then-current placement-persistence interval; otherwise
the clock is removed. An unchanged or subsumed ACK, ambiguous duplicate or
reinjected coverage, progress attributable to another owner, native TCP or QUIC
ACKs, `PATH_METRICS`, requalification ACKs, and polling MUST NOT replace or
restart it. Progress or gap repair elsewhere and movement of the lowest missing
frontier MUST NOT restart it.

Every transition that can change the predicate -- authoritative ACK horizon,
effective directional lane, exact attachment membership or incarnation,
readiness, accepted directional usage, local health or policy, drain,
retirement, or failure -- MUST trigger stream-owner reconciliation.
While any horizon-bounded request candidate exists, the client stream owner
MUST arm path-model publication from a generation captured before observing
the predicate. A generation change is reconciliation work even when no
alternate and therefore no placement-persistence clock existed previously; an
unchanged generation remains pending and MUST NOT create a polling loop.
Reconciliation uses the direction's current lane. It preserves an existing
absolute deadline unchanged when the authoritative omitted owner and predicate
remain valid, removes the clock when the owner has no authoritative outstanding
OriginalData, the predicate is false, or the exact incarnation departs, and
arms a missing clock at that reconciliation instant when both become true.
Reconciliation itself is neither assignment nor Data ACK progress and MUST NOT
restart a surviving clock. Attachment staleness remains stream-local; only
exact carrier-instance failure is session-wide.

If cumulative extra-traffic credit is exhausted, exact terminal carrier
failure may use one bounded target Product service window so that the budget
cannot deadlock correctness recovery. While the original carrier remains
live, a persistent gap's full target window is optional, but after the owner
boundary one exact frontier quantum may cross exhausted or partial credit by
the shared-token rule. A live tail uses the same token and quantum. Every byte
remains charged. Exact retained ranges, queue, flight, distinct-output, target-
capacity, and repeat-delay bounds continue to apply.

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
2. Only MPP Data ACK or exact terminal Product cleanup releases retained MPP
   stream ranges; terminal cleanup proves no delivery and grants no progress or
   rate evidence.
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
19. Each TCP carrier group reconciles toward its configured healthy member
    maximum without exceeding it; only the sole planned-replacement overlap in
    Section 7.2 may temporarily raise its physical count to `MAX + 1`. The
    reserved first range value changes no behavior.
20. Every authenticated ready TCP pool member has bidirectional Product
    authority subject to directional `AVAILABLE`/`BACKUP` usage. Usage follows
    configured endpoint topology and never a throughput comparison.
21. Carrier presence never forces payload placement or duplication. The
    ordinary scheduler revalidates exact carrier health, usage, Product
    authority, writer-capacity generation, writer reservation, credit,
    output-admission epoch, and typed action rank before every commit.
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
    instances. A planned replacement permits only one bounded predecessor/
    successor overlap in Section 7.2 and transfers no
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
31. Negative Data ACK inference and placement-persistence candidates remain
    complete-snapshot-horizon bounded. A frozen per-owner deadline may be armed
    only for current-epoch OriginalData authoritatively omitted below that
    horizon; only newly acknowledged unique Data ACK progress unambiguously
    attributable to that exact owner may replace it.
32. Every authenticated, ready, admission-active, non-stale output owns its
    configured bounded unqualified Product authority without a capacity proof.
    Its Product generation begins with first exact current-generation
    OriginalData ownership, including when it is first or contiguous-frontier
    owner. Capped exact tags preserve `0 <= V_i + M_i <= F_i`; only unambiguous
    current-generation MPP Data ACK advancing `V_i` to the positive `F_i` may
    set durable same-incarnation `q_i=1`. Rate expiry preserves it; stale or
    requalifying entry, detach, or exact incarnation replacement revokes it.
    Stale fallback ownership neither starts acquisition nor clears stale state.
33. First and live contiguous-frontier owners retain `P_i`; passive acquisition
    cannot reduce them to `E_i`. Every new unqualified-additional commit must
    leave its exact current debt within the then-current `E_i`; role changes,
    lifecycle qualification revocation, and later `E_i` shrink preserve prior
    debt under `P_i` and shared `W` rather than retroactively reclassifying it.
    Rate expiry preserves exact-volume qualification and therefore cannot
    demote an active exact output from `P_i` to `E_i`. Concurrent outputs remain
    jointly bounded by shared receive credit, reorder, queue, native-transport,
    and ordinary recovery authority; no output owns a direction-global
    acquisition token.
34. The PATH_CAPACITY transaction is diagnostic in Core Profile 7. It
    creates no Product, lifecycle, usage, health, congestion-control, or
    scheduling-rate authority and cannot gate unrelated Product work.
35. Advisory action rank uses only a coherent propagation term, exact
    comparable pre-native predecessor work when available, the encoded action
    work, and a typed positive directional rate. It rounds service time upward,
    uses checked arithmetic, and never divides capacity by active-flow count.
36. Rank is not admission. Every finite candidate order is revalidated against
    Product, lifecycle, queue, and native authorities; an unrankable action
    sorts last but remains eligible for an exact commit attempt.
37. Equal scores use a canonical output, carrier-instance, attachment-
    incarnation, direction, and command identity. PathId and input order are
    insufficient. Incumbent hysteresis is duration-valued uncertainty separate
    from the score; a percentage hint is not a hidden eligibility gate.
38. Every active native PathData or controller installation and restoration is
    fenced by a distinct E_N, including same-identity restoration. Every
    accepted activation, basis, or NativeOperational rate change advances the
    central G. Scheduling and precommit compare the complete coherent stamp.
39. A locator-only migration that preserves the exact active controller
    preserves E_N. A replacement connection or active-controller transition
    cannot inherit a stale activation's native evidence merely because a path
    label, locator, or controller identity is equal.
40. A STREAM_ACK carries Product ranges only. Complete-frame validation and
    Product release are atomic; an absent or terminal non-reused StreamId makes
    the ACK stale and cannot resurrect Product state. Duplicated coverage
    proves no carrier attribution.
41. The response-startup return plan is one immutable finite transaction.
    Before FINAL, fresh unique response offsets cannot exceed trigger_bytes and
    ACK cannot refill that prefix. FINAL may retain only sorted enrolled
    ordinals, atomically withdraws omitted enrolled outputs before removing the
    ceiling, and is absorbing and idempotent only for an equal repetition.
42. Kinds 44 through 48 are reserved and MUST be rejected as unknown under
    version 10.
43. Stale requalification uses one finite cyclic exact-incarnation cursor and
    at most one pending proof and one stream-owned ACK publication per
    direction. The ACK carrier is authenticated return service only; the exact
    non-reused pending tuple selects the forward target. A bounded fanout pass
    may publish one identical ACK on each currently attached accepting output
    and makes no finite delivery claim when every reverse writer stalls.
44. STREAM_REQUALIFY_DATA is Product-neutral and cannot advance a receive map,
    Data ACK horizon, Product flight, or delivery evidence.
    STREAM_REQUALIFY_ACK activates only its exact still-pending target's
    already-advanced output epoch with Product qualification reset.
45. Exact carrier, attachment, output-admission, writer-capacity, proof,
    measurement, requalification, and Product identities are independent
    fences. Terminal or exhaustion clears only the state owned by that exact
    scope and never implies Product delivery.
46. MPP-owned queue and Product resources have exact byte/item bounds and
    owners. Native transport debt may have no finite service time while the
    transport remains live; local write completion is neither MPP Data ACK nor
    proof of application delivery.
47. Optional reinjection accounting is directional and every published
    optional Product byte is charged once. It never alters native
    retransmission, native congestion control, or shared receive credit.
48. Peer PATH_METRICS is detached diagnostic evidence. Unknown is not measured
    zero, stale values remain visibly stale, and peer metrics cannot replace
    local NativeOperational chronology.
49. PATH_PROOF, PATH_CAPACITY, and STREAM_REQUALIFY acknowledgments carry
    exactly the fields assigned in Section 12.2; trailing fields are invalid.
50. Every new stream and attachment uses checked non-reused identity. The v10
    return-plan transaction, Product ACK, lifecycle, and cleanup rules remain
    bounded by configured frame, path, stream, queue, and retention limits.

## 17. Relationship to Existing Standards

### 17.1 MPTCP

RFC 8684 provides the established principles of stable data identity across
subflows, a data-level acknowledgment distinct from transport ACKs, shared
connection flow control, reinjection, bounded path management, and backup
preference. MPP uses an explicit configured carrier bound rather than a
traffic-rate threshold. RFC 8684 Section 3.3.8 motivates regular-to-backup
transition.
RFC 8684 Section 2.6 permits one MPTCP subflow to close through ordinary TCP
FIN/ACK without closing the MPTCP connection; MPP's ordered per-carrier wire
transaction is independently defined as client `PATH_DRAIN` followed by
server `PATH_CLOSE`.

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
usage-aware advisory action rank may leave redundant members idle.

This Core Profile declares no named TCP NativeOperational adapter. Kernel TCP
delivery, pacing, congestion-window, and queue telemetry therefore remains
diagnostic unless a separately typed evidence source satisfies Section 10.2.
A future profile may define a native TCP adapter only by declaring that full
contract for a new carrier-incarnation and direction reducer.

### 17.2 QUIC

RFC 9000 and RFC 9002 govern each QUIC carrier's connection identity, network
paths, address validation, migration, congestion control, loss recovery, RTT,
ECN, and PMTU behavior. MPP does not redefine those mechanisms.

A proposed speed fix is to give a model-based native QUIC controller separate
propagation and service-window delay estimates: raw minimum RTT would retain
minimum-delay and ProbeRTT duties, while a larger estimate would size ordinary
Startup, Drain, and ProbeBW flight. On a variable-delay path this can avoid
using one reordered fast-tail sample to compute a small
`gain * bandwidth * RTT` flight. It is nevertheless only a hypothesis: the
endpoint cannot prove that a larger observed delay is unavoidable service time
rather than external queueing. The preferred profile therefore rejects this
change on the latency argument given below and retains raw minimum RTT for both
jobs.

A Valid `QuinnBbr3NativeOperationalV1` adapter observation exports the
controller's current gain-free **operational bandwidth component** `B_op`,
specifically `min(max_bw, bw_shortterm)`, because that is the rate component
used by the controller's live send model. It MUST NOT export only the stale-high long-term
`max_bw` filter, a gain-scaled pacing rate, or an independently smoothed ACK
window. `B_op` is not asserted to be fresh achieved goodput: it may retain or
restore a probe opportunity before a new high sample, and its declared
loss-compensation domain may exceed raw delivered rate. With a ten-percent
authorized-loss allowance, that compensation alone is bounded by
`1/(1-0.10) = 10/9`, or about `11.11%`, over the aligned raw rate. This
operational state is advisory to Core scheduling; the native controller still
enforces cwnd, pacing, and recovery.

The adapter reads `(E_N, I_N, kind, rate)` from one atomic active Quinn
`PathData`/controller snapshot. The most recently allocated validation
candidate and an underlying controller identity are insufficient: the
candidate is not necessarily active, and same-IP `from_previous` may clone
state under an equal identity before the clones diverge. Every installation
and restoration of the active pointer therefore atomically advances the
transport-owned `E_N` and publishes its durable activation wake. Failed
validation can produce `A -> B -> A`, but the two `A` activation lifetimes
remain distinct. Port rebinding that retains the exact active `PathData`/
controller activation does not create a new fence merely because the locator
changed.

After Section 10.2.1 qualification, or immediately for Unknown or Unlimited,
the export changes whenever the active native controller changes this
component. A source change resets predecessor-owned initialization and rate
evidence and compare-applies only the coherent new active snapshot, as defined
in Section 10.2. MPP adds no independent smoothing, cap, maximum with another
rate source, expiry, or recovery timer. If that complete changed observation
remains current for `D_pub` in the adapter's declared stable environment,
every live scheduling consumer receives its central-authority revision within
`D_pub`. Publication MUST detect a `B_op` change directly and cannot depend
on a detached wrapper sample-count change. This asynchronous adapter exposes
transport `E_N` separately from central `G`; snapshots and precommits compare
both, including switches completed between polls. A consumer MUST discard and
recompute an old decision when any component of its complete applicable
authority fence fails precommit equality.

The preferred model retains raw minimum RTT for propagation, ProbeRTT, and
ordinary BBR flight.  A larger packet-qualified "operational RTT" is not part
of the preferred profile.  End-to-end low-flight observations cannot prove
that delay above raw minimum RTT is propagation rather than an external shared
queue.  If a candidate controller substitutes `R_op > R_min` at bandwidth `B`
and ordinary gain `g`, its nominal flight increases by exactly
`g * B * (R_op - R_min) / 8` bytes.  No finite sample-count or order-statistic
filter bounds that increase unless it also imposes a value bound; even then it
deliberately permits additional queue.  For example, a 500 Mbit/s path and two
qualified 1 s samples can add tens of megabytes over a 10 ms raw minimum.

This is an identifiability limit, not a missing estimator tweak.  Requiring
several observations, sampling at low self-flight, or taking a latest-three
median can reject isolated noise but cannot distinguish persistent external
queueing from unavoidable service delay.  Consequently no positive
operational-RTT inflation has a symbolic latency-non-downgrade proof.  A future
opt-in experiment may explore that speed/latency frontier, but it MUST be named
as a separate empirical controller policy, disabled by the preferred profile,
and accepted only by both goodput and loaded-latency evidence.  It cannot be
used to satisfy Core conformance or the implementation gate in this document.

On a path where sender-local policy authorizes an exogenous-loss allowance,
the preferred BBR-family model separates service estimation from
its residual congestion-loss objective.  Let `p0` be the sender-local
authorized loss-compensation fraction and let `q` be the controller's ordinary
residual loss objective, with exact domains `0 <= p0 < 1` and `0 <= q < 1`.
The `p0` configuration and the controller's `q` are each represented as checked
rational fixed-point values; NaN, infinity, negative, or unit-and-above values
are invalid.

For one aligned delivery sample, let `d >= 0` and `l >= 0` be its delivered and
declared-lost bytes, and let `r_raw >= 0` be its raw delivery rate.  When
`d + l > 0`, define:

```text
a      = min(p0, l / (d + l))
d_comp = d / (1 - a)
r_comp = r_raw / (1 - a)
```

When `d + l = 0`, no compensated sample exists.  Because
`a <= l / (d + l)`, `d <= d_comp <= d + l`; compensation cannot claim more
service than the aligned resolved volume.  A clean sample has `a = 0` and is
unchanged.  Missing or unalignable loss evidence uses the raw sample and no
allowance.  Integer implementations round compensated volume and rate down
and use checked widened arithmetic; inability to represent an input or result
takes the raw-authority transition below rather than wrapping, saturating
optimistically, panicking, or dropping evidence.

Loss-only observations also cannot distinguish an arbitrarily correlated
erasure burst from a drop-only policer. A nonzero `p0` profile therefore needs
an explicit burst envelope. For exact mathematical reasoning let:

```text
theta = 1 - (1 - p0) * (1 - q)
H     = 3 packet-timed operating rounds
E     = positive resolved volume for the current envelope epoch
A     = H * p0 * E
B     = (1 - theta) * A
```

`theta` is the exact dimensionless high-loss boundary. It is a population-rate
policy, not permission to compare every finite packet-timed round's point
estimate independently with the boundary. Doing so makes ordinary placement
variance repeatedly look like congestion; because the native response is
multiplicative, those false decisions can ratchet the bandwidth and flight
models downward. A fixed-sample confidence interval is not a clean repair:
repeated looks invalidate its coverage, and loss may be correlated rather than
Bernoulli.

`A` is the product policy's tolerated displacement of authorized lost bytes.
`B` is the corresponding excess-loss credit: a lost byte also contributes
`theta` credit as part of resolved volume, and therefore consumes
`1 - theta` net credit. Before the first complete non-application-limited
round, the retained positive initial window and the identity and nonnegative
transmit flight of the earliest-sent aligned lost packet are the **cold-start
anchor**; `E` is the larger positive value supplied by that anchor. Thereafter
only the preceding complete non-application-limited round's positive resolved
volume may replace `E`. If no positive representable `E` exists, this epoch has no
compensated decision and takes raw authority.  Three rounds is an explicit
MPTUNNEL product risk policy: at `p0 = 10%`, it permits at most `0.3 * E` lost
bytes to move across neighboring evidence rounds. It is not a BBR draft
constant, an inferred path property, or a value selected by a benchmark.
Integer implementations round `A`, `B`, and every credit addition down and
round loss debits up.

The sender maintains excess-loss credit `C` in `[0, B]`.  Creating the first
valid envelope initializes `C := B`; this initial full bucket is the premise
of the response-delay formula below. Replacing an envelope first applies the
closing epoch to the old `(C, B, E)`, then derives the new positive envelope
`E'`, `A' = H * p0 * E'`, and `B' = (1 - theta) * A'`, and atomically sets
`(C, B, E) := (min(C, B'), B', E')`. Thus a smaller bound clamps retained
credit and a larger bound never mints it. While compensation remains enabled,
every loss declaration admitted to its journal has one stable record carrying
its packet number space, packet number, byte count,
aligned send evidence when available, and recovery-transaction owner. Its
class is exactly one of ordinary, raw-authority, or proven-spurious. For one
immutable epoch, `delta_delivered` is the non-compensated, non-overlapping raw
count of uniquely delivered bytes, and `delta_ordinary_lost` is the raw byte
count of records still classified ordinary in the same resolved population.
The bucket never substitutes `d_comp` for `delta_delivered`; compensation is a
separate service-rate/volume output. Thus:

At declaration, a record is `ordinary` only when its aligned evidence is
complete and no raw bypass condition owns its cohort. Missing or unalignable
evidence, persistent congestion, or the `RawOnly` resource transition makes
the affected exact cohort `raw-authority` before the native response; its
checked raw-authority generation prevents that response from being applied a
second time. A valid late ACK owned by the exact retained recovery transaction
may change either loss class to `proven-spurious`. No unrelated record,
packet-number space, callback batch, or later transaction can change it, and
no `proven-spurious` record becomes loss again. Every permitted class change
replays the bounded suffix before another controller decision observes it.

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
credit is retained and clamped as specified above. Negative debt is not
retained, so the first recovered
below-boundary round stops repeated multiplicative reductions.

While `p0 > 0`, compensated loss alone has no Startup-exit authority.  A
backlogged connection uses the compensated full-bandwidth plateau to decide
that Startup acquisition is complete; an application-limited epoch neither
proves that plateau nor forces an early exit.  ECN, persistent congestion, and
missing or unalignable evidence retain the draft controller's raw Startup-exit
authority.  This rule prevents a small, early, application-limited and lossy
burst from freezing a low bandwidth estimate immediately before backlog
arrives.  It deliberately changes only the allowance-enabled Startup decision:
ProbeBW loss response and all `p0 = 0` loss decisions retain their native
rules.  Because a nonzero allowance can also mask genuine drop congestion,
this burst policy has no universal fairness or latency-non-downgrade theorem;
it remains subject to the empirical acceptance gate below.

Startup's loss-event criterion counts discontiguous packet-number ranges from
the compensation journal's canonical records independently in each QUIC
packet-number space; callback
interleaving across spaces is not a range boundary. If missing evidence ends
ProbeBW Up before Quinn finishes the current loss callback batch, all later
declarations in that batch belong to the same raw decision and cannot open or
charge a second loss round.

QUIC may retain multiple recovery transactions for two PTOs, and a later ACK
may prove an older or several overlapping transactions spurious. Addition is
not an exact refund after `C` has clamped or `B` has rebased. The sender
therefore owns one complete replay checkpoint immediately before the earliest
still-reclassifiable record and a chronological suffix of immutable epoch
tuples plus canonical records.  The checkpoint contains every state variable
needed to replay the suffix, including `(C, B, E)`, consumed lifetime-counter
frontiers, the cold-start anchor, and raw-authority generation.  Changing raw
attribution or proving a late ACK reclassifies only the exact owned records and
replays the complete suffix. The resulting compensation replay state—`C`, `B`,
`E`, consumed counter frontiers, compensation classifications and generations,
and the cold-start anchor—is definitionally the same as processing those final
classes from the checkpoint, including overlapping transactions. This equality
does not claim rollback of native BBR state; the conservative native-undo rule
below may retain an earlier native response.

The journal has finite immutable byte and item authorities `J^B` and `J^I`.
After every insertion, reclassification, and recovery-transaction terminal,
the sender MUST advance the checkpoint through the maximal chronological
prefix for which neither an open loss-decision cohort nor retained native
late-ACK evidence can change any record class, then delete that prefix.  It
does so even while a newer reclassifiable suffix remains; waiting for the
entire suffix to become immutable permits rolling overlap to grow without
bound.  Prefix folding is exact because the checkpoint is the complete replay
state and later transitions depend on the prefix only through that state.

Before adding a record or tuple, the sender first performs that maximal-prefix
fold and proves that the new suffix fits both `J^B` and `J^I`.  If it still
cannot fit, or if any compensation counter, record identifier, or authority
generation cannot advance by checked non-reusing arithmetic, it atomically
enters `RawOnly` for the current native path epoch before classifying the
current event. If an unresolved newly declared loss cohort is present, that
cohort takes the native raw-authority path exactly once. If replay, late-ACK
reclassification, or compensation-only counter failure triggers the transition
without such a cohort, the transition synthesizes no native loss response. In
either case no new compensation record is inserted and future actual loss
cohorts use native raw semantics for that path epoch. Entering `RawOnly`
permanently invalidates and discards every
native controller-undo snapshot, open native-rollback transaction, replay
checkpoint, journal record, and compensation-only identifier for that path
epoch before the new raw response becomes visible. Later native ACK and loss
callbacks use ordinary raw controller semantics; optional diagnostic
tombstones, if retained, are separately bounded and have no class, replay, or
rollback authority. Thus no old generation can roll native state back across
the absorbing transition and no later event is required to replay arithmetic
whose representation already failed. Existing controller state is not rolled
back or described as bit-for-bit native state. The native controller's
ordinary bounded packet-number/range and recovery state remains authoritative
in `RawOnly`; it is not the discarded compensation journal. A new native path
epoch starts fresh. Resource
exhaustion therefore may conservatively lose compensation, but cannot panic,
wrap, silently omit loss, manufacture credit, or make replay state unbounded.

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
fresh. With `p0 = 0`, the compensation factor, bucket, journal, compensated
full-bandwidth baseline, and allowance-specific loss decisions do not
participate; that loss-processing path remains raw.  This statement does not
cover the independent short-term bandwidth export or any separately enabled
experimental controller policy.

Changing `q` is not a substitute for changing `p0`: `q` controls the residual
loss boundary where the current controller state grants that decision
authority, whereas `p0` repairs the delivery and inflight evidence that would
otherwise ratchet downward under sustained post-service erasure. It does not
override the Startup gate above. The preferred MPTUNNEL profile uses `p0 =
10%` and retains the BBR draft's `q = 2%`, producing an aggregate boundary of
11.8%. A sender may select `p0 = 0` for uncompensated loss behavior. ECN,
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

Feedback ordering remains a qualification limit for any future experimental
operational-RTT policy. If the transport finalizes an ACK event before
delivering loss or ECN feedback caused by that same ACK, that later feedback
cannot retroactively certify the observation as loss- or ECN-free. A new vote
can move a sliding-window order statistic, and no sample-count or median filter
repairs this causal limitation or proves a latency bound.

This guidance is an implementation preference, not an MPP wire requirement.
The loss-compensation equations and replay rules establish bounded state,
single accounting, conservative arithmetic, and exact reclassification; they
do not prove that a nonzero allowance improves every network. In particular,
loss-only evidence cannot distinguish authorized erasure from a drop-based
shared bottleneck, so the built-in 10% policy is an explicit performance and
fairness choice rather than an inferred fact. Before an allowance-enabled
controller is accepted for release, time-series tests MUST separately cover
cold and warm short transfers, sustained single- and multi-flow goodput,
loaded latency, random and burst loss below and above `theta`, ECN, genuine
drop congestion, QoS downshift and recovery without restart, and wire
amplification. QUIC-only and mixed-carrier cases MUST show no material
downgrade against the currently accepted controller under the same trace;
competitive baseline comparison is an additional product criterion, not a
symbolic theorem.

This section records a possible defect in applying an unqualified one-delay or
one-loss form of a BBR-family model described by
`draft-ietf-ccwg-bbr-06`; it is not a defect in RFC 9000 or RFC 9002. CUBIC, an
implementation of the published BBR draft, or another native QUIC controller
remains permitted. In every case the QUIC controller—not MPP scheduling—owns
its packet window, pacing, loss response, and recovery.

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
- [RFC 9001: Using TLS to Secure QUIC](https://www.rfc-editor.org/rfc/rfc9001.html)
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
