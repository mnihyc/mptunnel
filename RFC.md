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
- exact carrier-work ownership and peer-processing receipts used only for
  advisory carrier service pressure;
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
  exact Product-terminal rule. It is logical delivery/recovery state and is
  independent of the command's carrier-work token.

**Carrier-work token**
: Exact local ownership of one normalized reliable command on one physical
  carrier direction. It moves from provisional reservation through the MPP
  queue and native ownership until exact peer-processing receipt or terminal
  cleanup. It is not Product ownership, native send credit, or a packet ACK.

**Carrier-service receipt**
: An authenticated cumulative `writer_epoch` frontier proving that the peer
  completely processed exact commands on one ordered writer. It retires only
  their carrier-work tokens. It neither acknowledges a Product offset nor
  grants receive credit, qualification, pacing, or native transport authority.

**Carrier observation**
: Optional encrypted synthetic payload carried and discarded on one exact
  physical carrier direction to establish achieved-service evidence without
  creating Product ownership or delivery state. It is admitted only when the
  exact ordinary decision proves that rate evidence is the causal blocker.

**Observation channel**
: The same-fate ordered bidirectional carrier-local channel that carries one
  direction's observation grant and DATA and the reverse cumulative observation
  ACK. Its incarnation has an exact non-reused endpoint-local epoch and is
  independent of a Product stream's lifetime. The epoch is the TCP socket or
  QUIC request-stream incarnation fence; it is not an additional wire field.

**Scheduling-rate authority mode**
: The exclusive evidence contract selected by one persistent reducer for an
  exact carrier incarnation and original-sender direction. `NativeMode` admits
  only the named local controller's current gain-free operational bandwidth
  state; `ReceiptMode` admits only achieved-service lower bounds derived from
  exact peer-processing receipts.

**Ordered-writer epoch**
: A checked nonzero identity, strictly monotonically allocated by its origin
  and never reused within an authenticated session and original-sender
  direction, for one cumulatively ordered service frontier.
  One TCP carrier direction owns one epoch; every independently ordered QUIC
  HTTP/3 request-stream direction that can carry a generic service-bearing kind
  `8`, `31`, `33`, or `42` owns a distinct epoch while all of them share
  physical-carrier capacity. Carrier observation instead owns its separate
  channel processed-work coordinate.

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
            exact carrier-work ledger and advisory service-pressure rank
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

MPP owns stream, datagram, and IP-tunnel identity, offset assignment, Data ACK
processing, shared receive credit, carrier selection, bounded reinjection,
exact data-level deduplication, and exact carrier-work token lifecycle above
the native application interface. Carrier-work accounting supplies only an
advisory rank; it does not replace native transport authority.

Product and carrier-work ledgers are disjoint. MPP Data ACK may end Product
ownership without ending the original command's carrier work; a
carrier-service receipt may end carrier work without acknowledging or
delivering Product. Neither transition implies the other, and neither ledger
may be reconstructed from the other's counters.

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

A locator-only migration preserves ordered-writer epochs, exact work tokens,
and aggregate carrier `Q/Z`. If QUIC installs or restores a different active
native `PathData`/controller activation within the same connection, the switch
is fenced at its actual activation boundary under Section 10.2. NativeMode
installs only the new activation's coherent controller state; ReceiptMode
instead resets its path-scoped terms, acquisitions, and timing evidence under
the existing mode rules. Already-owned writer tokens remain. A replacement
connection transfers neither tokens nor evidence. A locator change that
retains the exact active `PathData`/controller activation clears none of them.

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

When aggregate carrier retirement withdraws attachments from multiple Product
streams, each stream's ordered detach MUST be published independently and
concurrently to that stream's bounded actor queue. Earlier events in one stream
remain FIFO-before its detach, but a full or dormant stream queue MUST NOT
withhold detach publication to another stream. Aggregate carrier retirement
completes only after every stream actor has applied its exact detach.

The server sends `PATH_CLOSE` only after every earlier frame from the client
has been applied and the exact carrier has no attachment, datagram binding,
queued or retained frame, original or reinjected Product flight, pending Data
ACK, path proof, capacity work, unreceipted local carrier-work token, or dirty
or unpublished processed service frontier in either direction. All cumulative
service entries and other server frames that complete preceding carrier-owned
work MUST precede
`PATH_CLOSE` in the TCP byte stream. The client treats receipt of
`PATH_CLOSE`, not its own write completion or local emptiness, as the aggregate
retirement acknowledgment. It removes the carrier only after applying every
preceding server frame and reaching the same local zero-work condition.
Native failure before that boundary uses ordinary retained-state recovery.
Local writer completion is not zero carrier work.

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
stream attachments, datagram flows, the one carrier-observation channel, or a
bounded one-shot `PING`/`PONG` path proof and do not repeat connection
admission. The first MPP frame, together with the authenticated physical
carrier binding, unambiguously selects that operation; an observation request
starts with `CARRIER_OBSERVE_MAX_WORK` as specified in Sections 12.2 and 15.1.

Both HTTP/3 DATA send halves of the carrier control request stream remain open
for the complete active carrier lifetime. They are the same-fate reliable
service-receipt channel defined in Section 8.3. An endpoint MUST NOT issue a
clean FIN or reset on either half while the carrier remains active. Local or
remote terminal closure of either half is the exact carrier-terminal event and
retires all writer scopes on that QUIC carrier; it is not an operation-local
writer drain.

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
carrier-instance identity and fresh evidence. An operation-local HTTP/3
request reset, finish, refusal, or cancellation does not vacate the live
physical slot and does not wake carrier replacement.

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

`OPEN_STREAM(stream_id, target, demand)` has no carrier role. The first
accepted open creates the stream. A later open with the same `StreamId` adds an
attachment only when the target and initial demand hint exactly match the
original values.

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
`W` and publishes it to
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

`STREAM_ACK(stream_id, complete, ranges, services)` carries Product ranges and
carrier-service frontiers. Product ranges are half-open in one directional MPP
stream offset space. Every listed range is non-empty; either list MAY be empty.

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
positive local fact that current-epoch, evidence-eligible OriginalData remains
retained and unacknowledged on an exact attachment incarnation. Retained
Product ownership above the horizon remains authoritative only for exact
Product ACK release and Product recovery. It neither creates nor retains
carrier/native work, which follows the independent writer-token/frontier
lifecycle, and MUST NOT by itself arm placement-persistence withdrawal, extend
the Data ACK horizon, establish a Data ACK gap, or declare native transport
loss.

Before any mutation, the endpoint MUST structurally validate the complete
frame, immutably look up and classify the Product stream direction, semantically
validate every Product range against that exact state when it is live, and
semantically validate every session-, direction-, and writer-scoped service
entry. Only after all applicable validation succeeds does one Data ACK
transaction mutate service first and Product second:

1. validate and normalize Product ranges and service frontiers;
2. apply every advancing exact carrier-service frontier before Product release;
3. compute the exact newly acknowledged unique Product coverage without yet
   releasing its owners or flight;
4. for every original owner not proved by a previously applied frontier or
   this service transaction, classify its exact token before releasing Product:
   cancel and refund a still-queued command when cumulative Product coverage
   now covers its complete Product range, retain a partly unacknowledged queued
   command without a guard, or publish the Section 15.1 ambiguous-release guard
   on an already-native-owned token and its output-admission epoch;
5. release each newly acknowledged unique byte and every overlapping original
   or reinjected Product flight exactly once;
6. update local delivery and admission evidence without changing receive
   credit; and
7. publish carrier-specific progress only when attribution is unambiguous.

If the immutable lookup classified the named Product stream direction as
absent or terminal, its structurally valid Product ranges and `complete` bit
need no live-state semantic validation and are a stale no-op after the fully
validated service entries have been applied. They MUST NOT create, reopen, or
mutate Product state. A malformed frame, an invalid service entry, or an
invalid range for a live Product stream changes no state. This validates the
whole frame atomically while still permitting a delayed `STREAM_ACK` to retire
shared-writer tokens after logical-stream teardown without granting Product
authority.

Steps 2 through 5 have one linearization order even when carrier and stream
state live in different actors. An implementation MAY use an exact two-phase
certificate, but no scheduler may observe reopened `O/W` authority between
service apply, guard publication, and Product release.

If a byte was outstanding on multiple carriers, the Data ACK proves delivery
but not which copy delivered it. No implementation may invent per-carrier
delivery evidence for that range.

Every writer direction that can serialize a generic service-bearing kind `8`,
`31`, `33`, or `42` has one checked, nonzero `writer_epoch`. Its origin allocates
epochs with strict monotonicity in the authenticated session and original-
sender direction and never reuses or wraps one. TCP has one epoch for each
physical carrier writer direction. QUIC has one epoch for each independently
ordered HTTP/3 request-stream send half that can carry one of those kinds,
including reliable attachments and carrier-control proof work; sibling request
streams MUST NOT share a frontier. The dedicated observation request stream
does not allocate this generic epoch.
`SERVICE_EPOCH(writer_epoch)` is the first service-accounting frame on that
ordered writer. A locator-only QUIC migration preserves it; writer or carrier
replacement creates a new epoch. Allocation order is origin authority; arrival
over independently ordered writers is not evidence of that order.

The receiver binds the first `SERVICE_EPOCH` to the exact native-writer
incarnation carrying it and retains only a bounded live/draining binding map.
Zero, a second epoch on one writer, or the same epoch concurrently bound to a
different live writer is a protocol violation. Exact native-writer terminal
serializes after all accepted frames from that writer and removes the binding;
no buffered frame from the terminal incarnation may apply later. The receiver
MUST NOT use a scalar retired high-water to reject a previously unseen epoch:
legal cross-writer reordering can deliver and retire epoch `e+1` before epoch
`e` first arrives. A bounded diagnostic tombstone cache MAY detect recent peer
reuse but is not correctness authority.

The origin retains its checked allocated high-water plus the bounded
live/draining epoch map. A receipt naming a live epoch is validated and applied
there. An absent epoch no greater than the origin's allocated high-water is a
stale receipt for an already terminal writer and is ignored idempotently; an
epoch above that high-water or in the wrong original-sender direction is a
protocol violation. Exhaustion fails the new writer closed before publication.
Thus no honest delayed frame can migrate to a successor native writer, while
cross-writer arrival order requires no unbounded retired set.

Within an epoch, every irrevocably serialized positive command of generic kind
`8`, `31`, `33`, or `42` occupies the next half-open interval in a cumulative
service coordinate. Before serialization, checked addition of the command's
exact normalized encoded work MUST produce a strictly greater frontier;
overflow or exhaustion terminalizes that writer without publishing or reusing
an interval. Both peers can reproduce the work; the sender maps each interval
endpoint to its exact local carrier-work token. Queue arbitration occurs before
this allocation, so a command cancelled or overtaken while still MPP-owned
creates no hole. Reinjection is a new copy and consumes a new interval even
though it retains the Product offset. Native retransmission consumes none.
Observation kind `45` uses only its Section 15.1 channel coordinate and MUST NOT
enter this one.

After complete frame validation and the command's receive-map, dedup, proof,
capacity, or requalification mutation have succeeded, the
receiver advances that exact writer's `processed_frontier`. A `STREAM_ACK`
carries a bounded vector of
`(writer_epoch, processed_frontier)` service entries in addition to its Product
ranges. The receiver transaction that first publishes Product coverage caused
by a command MUST carry the command's origin frontier in that same frame. If a
processed duplicate or reinjection creates no new Product range, an incomplete
`STREAM_ACK` with empty Product ranges and a nonempty service vector is a
service-only receipt. The dedicated ACKs listed in Section 12.2 carry their
origin epoch and cumulative frontier in the same transaction. Service entries
are cumulative and may return on any authenticated carrier of the same session.

The sender requires `received_frontier <= assigned_frontier` for the exact
epoch and applies `acknowledged_frontier = max(acknowledged_frontier,
received_frontier)`. Equal or lower received values are idempotent; a value
above the assigned frontier is a protocol violation. Advancing a frontier
retires only complete work tokens whose interval end it covers. It updates
carrier work accounting and same-output service provenance, but releases no
Product range, receive credit, `W/P_i/E_i`, qualification, pacing, native
window, or application delivery. `STREAM_ACK` itself occupies no service
coordinate and elicits no receipt, so there is no ACK recursion.

The receiver retains only the current processed frontier and a dirty
publication bit per live or draining ordered writer. Dirty state belongs to
the writer/session, not a Product stream. It is published cumulatively through
the next applicable dedicated ACK, `STREAM_ACK`, or `SERVICE_ACK`; after a
logical stream terminal, shared TCP writer state continues through
`SERVICE_ACK`. The dirty bit clears only after a same-fate reliable reverse
queue accepts a frame containing that frontier, or exact origin-writer/carrier
terminal. For TCP this queue is the opposite writer on the same TCP
connection. For QUIC it is the reverse direction of the carrier-control
request stream whose terminal normatively terminalizes that physical carrier;
an unrelated operation or attachment request stream is not sufficient. A
cross-carrier or other operation-local copy is optional acceleration and
cannot discharge dirty authority. Failure or cancellation before same-fate
acceptance retains it and retries on existing carrier-capacity or membership
wakes. Native reliability then shares the origin carrier's fate: delivery
retires the sender token, while failure of the receipt channel terminally
retires the token's complete physical-carrier scope.

Stream-owned requalification-receipt state and this ordered-writer dirty
service authority are distinct. Acceptance of a requalification ACK copy on a
sibling may complete the former, but MUST NOT discharge the latter unless the
same-fate reverse queue defined above also accepted the cumulative frontier.
The sibling copy may accelerate application of that frontier at the sender
while the receiver retains dirty authority for same-fate publication.

The sender retains its assigned and acknowledged frontiers and
the bounded ordered token deque. A later frontier subsumes a lost or duplicate
receipt. An exact writer terminal retires its remaining token ownership without
claiming service. A logical stream terminal cannot erase native-owned work or
dirty frontier state on a shared TCP writer. Publication of a service frontier
is part of the same receiver transaction and no later than Product release it
enables; otherwise Product ACK could repeatedly reopen `W` while unreceipted
carrier tokens grow without a bound.

The logical receiver retains its latest cumulative received-range state. It
retains a causally required service entry only while that exact origin writer
is live or draining; exact writer terminal or attachment-incarnation retirement
removes the entry. Thus the vector is bounded by configured concurrent writers,
not historical writer epochs. A later Product publication without a retired
origin entry remains safe: the sender already applied it, exact writer terminal
retired the carrier work and made the bound output epoch non-admitting, or step
4 publishes the exact token guard.
Processing any newly received unique Product byte marks the cumulative Data ACK
state pending and advances one local publication generation. Before the
serialized receive actor parks or yields its bounded cooperative turn, it MUST
offer the latest pending generation independently to every currently live exact
attachment. Several frames processed in that one turn coalesce into the latest
cumulative state; ACK publication never waits for a byte threshold, another
Product frame, an application read, or a timer.

Every fanout copy that could first publish those Product ranges MUST carry the
same required origin service entries; enqueueing one such copy is insufficient.
If the required entries and Product ranges do not fit one bounded frame, the
receiver splits the positive publication so every range-bearing chunk carries
all causal entries needed for its ranges; omission is forbidden. Dirty
service-only frontiers may be cumulatively batched separately. Forced
publication of unchanged state reuses the current generation. Queue acceptance
advances only that exact attachment incarnation's publication fence; a blocked
attachment remains pending and retries on carrier-capacity or
attachment-membership wakes. A newly accepted attachment starts without a
fence and receives the retained latest state. The latest-state replacement and
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

Product FIN, detach, reset, or logical-stream terminal may cancel and refund
only exact provisional or queued commands that remain safely removable before
native handoff. Cancellation clears any guard on that removable token only
after the bound Product/output epoch has become non-admitting or terminal. It
MUST NOT erase a native-owned token or its guard metadata on a shared writer or
claim peer service; writer-scoped `SERVICE_ACK` remains valid after that Product
stream terminal. Product or output terminal means an old guard no longer
suppresses a successor epoch, but the physical native token survives until
service or exact writer terminal.

An advancing Data ACK has one narrower work-conserving cancellation rule. If
its validated cumulative Product coverage covers the complete Product range of
an OriginalData command whose exact token is still queued, the ACK transaction
atomically wins removal against native handoff, refunds both writer-command and
all-stage token reservations, and retires that command before releasing its
Product owner. It publishes no guard: queued state proves that this copy never
entered native or peer service and its now-acknowledged payload has no remaining
Product purpose. If handoff linearized first, the ACK observes a native-owned
token and MUST retain and guard it. Partial command coverage cannot cancel or
split the queued command, but queued state still proves that it has not served;
the transaction retains its remaining Product owner and exact queue debt
without a guard. Handoff and ACK classification serialize on the shared
carrier-ledger generation, so the only outcomes are cancellation before a
later handoff revalidation fails, or native ownership before the ACK guards it.

Exact ordered-writer terminal atomically cancels every safely removable
provisional or queued token bound to that writer generation and retires only
its native-owned epoch tokens, proving neither Product delivery nor service,
then clamps the physical carrier `Z_c` to remaining `Q^n_c`. It makes every
bound output direction non-admitting and wakes its retained Product owners for
ordinary recovery. A TCP writer-direction terminal is also that physical
carrier-direction terminal. A QUIC attachment/request-writer terminal does not
clear sibling writer epochs or shared carrier `C/H/Q/Z`; only terminal loss of
the QUIC carrier control stream or connection does so. Exact carrier terminal
retires all its writer tokens and clears carrier ledger and evidence without
acknowledging retained Product, which survives under ordinary recovery rules.

Native TCP EOF ends the carrier and is not stream FIN or detach. Native QUIC
stream FIN closes only that native byte-stream direction and MUST NOT be
interpreted as MPP FIN, detach, Product completion, or immediate
service-writer terminal. The carrier-control exception in Section 6.2 instead
makes either half's closure a physical-carrier terminal. For every other
writer, before issuing a clean FIN the origin makes every
bound output direction non-admitting for new commands and MUST reach
`Q^p = Q^q = 0` for that writer by cancelling and refunding every uncommitted
provisional token and handing every committed queued command to native
ownership under its already-reserved all-stage authority. If it cannot, it uses the
exact reset or lifecycle/writer-terminal path rather than FIN. A locally issued
clean FIN then moves that ordered writer to `Draining`: it allocates no new
service interval and retains every native-owned token, guard, and bound output
state. The receiver
processes every preceding complete frame, retains the processed frontier and
dirty bit after observing FIN, and may terminalize its remote binding only
after same-fate reverse-queue acceptance has discharged that dirty state. The
origin terminalizes the drained writer after cumulative service has retired all
its tokens. Reset, lifecycle terminal, or carrier failure instead uses the exact terminal
cleanup above. Thus the receiver never treats FIN as permission to forget a
receipt while the origin still owns native work. A native FIN inside an MPP
record is truncation; a FIN at a record boundary is a clean native half-close
governed by this draining rule.

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
no authority, attachment, transport evidence, queue, Product flight, or carrier work transfers from
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
identity, stream frontier, output guard/epoch, the complete scheduling-rate
authority stamp, carrier evidence ordinal, shared carrier-ledger generation,
command priority and normalized work, ordered-writer generation, and queue
reservation. It creates visible provisional carrier work
only after real reservation and commits Product range ownership only in the
same infallible publication transaction.

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
identity, frontier, guard/epoch, evidence/ledger generation, provenance, or
reservation is stale. A stale authority stamp requires a new complete ranking;
substituting a newer rate into the old ranking is not revalidation.

### 10.2 Evidence provenance

Transport queue/flight and MPP queue/Data-ACK flight overlap in one delivery
pipeline. Sampled counters therefore cannot be added, maximized, or divided by
flow count to manufacture physical work. Section 15.1 instead gives every
reliable data-bearing command one exact ownership token and moves that token
between disjoint stages. Common carrier work is represented once for the
carrier direction and is visible to every logical stream sharing it.

All exact carrier work is measured as the complete encoded MPP frame: the
10-byte `MPTF` header plus its declared payload. It excludes TCP Noise record
overhead, HTTP/3 DATA framing, native packet headers, and native
retransmission. This is the common boundary both endpoints can reproduce before
carrier-specific wrapping. A candidate command's `M_c` and every exact work
token use this unit.

A rate term `C_c` is expressed in normalized-work bits per second. It MAY be
formed from a conservatively counted subset of receipted normalized work when
the subset maps one-for-one into that work. In particular, every byte of an
observation payload is contained in its encoded MPP frame, so for the same
commit/receipt interval:

```text
0 <= payload_subset <= normalized_frame_work <= actual_useful_service.
```

Dividing the payload subset by the positive interval therefore gives a valid
lower bound in the normalized-work rate domain; reconstructing variable frame
headers is unnecessary for safety, but omitting them is deliberately
conservative. An adapter that can establish neither exact normalized work nor
such a one-for-one lower-bound projection MUST omit the measurement. Missing
evidence is not measured zero.

Elapsed time used in an achieved-service denominator MUST be a conservative
upper bound, not a raw subtraction of quantized clock reads.  For a monotonic
clock whose read lies in an interval of width at most `g`, Core carries lower
and upper bounds for cumulative busy time.  Closing or observing a busy
interval adds a checked lower and upper elapsed bound; equivalently, a
floor-tick implementation may use at least one additional `g` for each elapsed
interval.  If the true cumulative busy coordinate is `T`, the maintained
bounds satisfy `T^- <= T <= T^+`.  An anchor created while the bounds are
`[T_a^-, T_a^+]` uses `T^+ - T_a^-` as its elapsed upper bound.  Merely taking
`max(raw_tick_difference, g)` is non-conforming: two endpoint quantization
errors can otherwise make the denominator smaller than real elapsed time.

For one exact carrier direction, let `B(s,t)` be distinct normalized work bytes
committed no earlier than local time `s` and covered by exact peer-processing
receipts applied by local time `t`. Let `Y(s,t)` be actual useful carrier
service in that interval and `D^+(s,t)` a conservative upper bound on its
positive elapsed duration. Every counted byte satisfies:

```text
s <= commit -> peer processing -> local receipt apply <= t
B(s,t) <= Y(s,t)
duration(s,t) <= D^+(s,t)
L(s,t) = 8 * B(s,t) / D^+(s,t)
         <= 8 * Y(s,t) / duration(s,t).
```

ACK compression cannot invalidate the inequality. `B(s,t)` MAY sum work from
different Product writers and the carrier-observation channel only when every
summand is a distinct exact token committed and service-receipted inside the
same carrier-direction interval. This is one division after summing disjoint
physical work, not a sum of independently estimated rates. Detached per-flow,
per-writer, or per-channel rate estimates MUST NOT be summed, multiplied by
flow count, copied into another carrier, or copied back into independent
per-stream rates. `L` is nevertheless only a historical achieved-service lower
bound. It is not unused capacity, marginal capacity, a future-service promise,
or evidence that another carrier is independent.

The first receipt cannot identify high-BDP capacity. For stable service `C` in
bits per second, feedback delay `tau`, and first exact normalized volume `F` in
bytes, the causal lower
bound is:

```text
L_first = C * (8 * F) / (C * tau + 8 * F).
```

Thus `L_first / C = 8F / (C * tau + 8F)`. At `F = 64 KiB`, `tau = 100 ms`, and
`C = 500 Mbit/s`, it is only about `5.2 Mbit/s`. Repeating the same small-flight
geometry does not remove that bias. Product-volume qualification, physical
carrier-rate prediction, and native congestion authority are therefore three
separate facts.

Every exact `(carrier incarnation, original-sender direction)` owns one
persistent scheduling-rate authority reducer. The reducer selects one
exclusive authority mode; Core MUST NOT take the maximum of native and
receipt-derived rate authorities. That construction has no stable downshift
order and can repeatedly fence the receipt acquisition that is intended to
recover it.

NativeMode additionally uses two logically distinct checked serials. `E_N` is
the transport-owned, strictly increasing active-source activation serial. `G`
is the central reducer's authority revision defined below. One `E_N` lifetime
begins when one exact native `PathData`/controller instance becomes active and
ends before any different instance becomes active. Every installation and
every restoration atomically advances `E_N` with the active pointer, even when
it restores the same underlying controller object, address, native-path label,
or controller identity `I_N`. `E_N` is mutable NativeMode state, not part of the
immutable carrier-direction scope. Scheduling compares it for exact equality;
its numeric value is not capacity, health, or rank evidence.
Within one `E_N`, `I_N` is immutable. Any change of active `PathData`,
controller instance, or `I_N` ends that activation and requires the next
`E_N` before the successor becomes active.

This distinction is required because a valid QUIC history may install
controller `A`, activate validation candidate `B`, and restore `A`, while a
same-IP `from_previous` transition may clone controller state under an
unchanged underlying identity and then diverge. The three activation lifetimes
are therefore distinct even when the first and third name `A`:

```text
(E1, I_N=A) -> (E2, I_N=B) -> (E3, I_N=A).
```

A delayed proposal carrying `E1` or `E2` is stale after `E3`; equality of the
first and third `I_N` cannot revive it. A locator-only change that retains the
exact active `PathData`/controller instance retains `E_N` and does not itself
change authority.

`G` is the reducer's one checked, nonzero, non-reused authority revision. It
never resets during the reducer lifetime and advances on every accepted
semantic change of active-source activation, authority basis, authority mode,
NativeMode rate, or ReceiptMode term. An exactly repeated semantic state is a
no-op and does not advance `G`; one accepted atomic transaction that changes
one or more of those fields advances it exactly once. Revision exhaustion fails
closed without wrap, alias, or a successor live stamp. A NativeMode scheduling
snapshot contains the immutable reducer scope, current mode, `E_N`, `I_N`,
authority basis and rate, and `G`; ReceiptMode carries its corresponding mode-
owned identities.

For the asynchronous adapter specified here, transport `E_N` and central `G`
MUST be separately observable and both MUST be revalidated. Every actual
native activation and restoration is therefore fenced from scheduling, not
merely discovered by later polling. A candidate installed and rolled back
entirely between polls still advances `E_N` twice, so an old decision cannot
remain valid during either activation. A future implementation with one proved
atomic cross-layer switch, reducer, and scheduling-commit transaction MAY
encode the two logical serials with one sequence, but that is not this adapter
and cannot remove either semantic comparison or wake/publication obligation.
Exhaustion of `E_N` or `G` prevents successor authority from becoming
schedulable and enters exact carrier-terminal handling; neither serial can wrap
or reuse an old value.

The transport switch transaction MUST publish a durable activation wake
atomically with the new `(active pointer, E_N)`. Wakes MAY coalesce only to the
then-current `E_N`; they cannot disappear while central authority names an
older activation. Under fair coordinator service and an activation that remains
current for the adapter's declared `D_pub`, the publisher MUST install that
activation's coherent authority snapshot in every live central consumer within
`D_pub`. Faster repeated switching voids that conditional publication bound but
not the immediate `E_N` precommit fence. A scheduler whose central `E_N` lags
the transport MUST arm the activation wake and park or recompute; it cannot use
the predecessor authority on the successor activation.

`NativeMode` is permitted only for a named local adapter whose native
controller exports its current positive, finite, gain-free operational
bandwidth `B_op`. `B_op` MUST be the rate component that the same controller
uses to construct its live send model; for the
`QuinnBbr3NativeOperationalV1` adapter it is
`min(max_bw, bw_shortterm)`. It is not a gain-scaled pacing rate,
detached ACK-window estimator, Product-goodput estimate, peer metric, or
ReceiptMode achieved-service lower bound. In particular, it may intentionally
restore a retained probe opportunity before a new high delivery sample and may
include the adapter's declared loss-compensation policy. Native congestion
window, pacing, recovery, and flow control still bound every transmitted byte.

The adapter contract MUST declare the exact carrier incarnation and original-
sender direction; the exact switch-time mechanism that creates and publishes a
fresh `E_N` for every active `PathData`/controller installation or restoration;
units and positive representable rate lattice; checked conversion into
normalized-work bits per second; raw-versus-loss-compensated service domain;
application-limited update rules that do not reinterpret a low application-
limited delivery sample as a lower service bound; structural initialization,
active-source change and rollback, explicit invalidation, revocation, and
terminal events; and its stable environment envelope. Saturation, wrap, NaN,
or infinity cannot become an authority update and MUST NOT manufacture a
maximum value. An unrepresentable checked score product makes that proposal
lose without mutation; it cannot saturate into a win.

The NativeMode adapter exports one coherent active-controller observation:

```text
(E_N, I_N, kind, rate)

kind = Absent
     | Valid                 # rate = B_op
```

An adapter MAY classify an internally available controller value as `Absent`
until a declared lifecycle qualification proves that value belongs to the
current operational epoch. Such a classifier MUST consume exact native
provenance, MUST NOT modify the native controller, and MUST declare its finite
progress bound. A detached ACK aggregate, wall-clock delay, payload heuristic,
or previously retained numeric value cannot qualify a controller output.

`Absent` means that this raw read contains no valid operational-rate
observation. On a newly accepted `E_N`, it leaves that activation on `C_0`
until its first `Valid(B_op)`. After the same activation has initialized, a
missed poll, absent optional value, post-round zero, or failed numeric
conversion is no authority event: it cannot clear the retained valid value or
restore the startup prior.

Structural invalidation is not a raw observation kind. It is a separate
explicit coordinator command carrying the exact current scope, `E_N`, `I_N`,
expected `G`, reason, and structural fence. The coordinator may accept that
command only while those values remain current under the transport activation
fence. Acceptance performs the one-way fenced NativeMode-to-ReceiptMode
transition below or exact terminal handling. A missing, zero, malformed, or
unrepresentable rate observation cannot construct or impersonate this command,
and invalidation cannot leave a knowingly invalid NativeMode value live.

The current `(E_N, I_N, kind, rate)` MUST be read atomically from the same
active-`PathData`/controller snapshot. An underlying controller identity is not
a substitute for `E_N`: a same-identity clone may own distinct and diverging
live state.
The coherent snapshot reader and current-`E_N` fence MUST be obtained as one
opaque transport-owner capability. They cannot be supplied independently and
then associated by comparing raw `E_N` or `I_N`: another connection may
legitimately issue equal numeric values, and those values have no meaning
outside their issuing fence.
Legacy RTT, flight, loss, or queue diagnostics MAY be sampled independently,
but if they participate in the same scheduling decision they MUST carry the
same current activation fence and authority revision or come from that
coherent snapshot; a mismatched bundle is discarded rather than fused.

Publication uses capture/read/compare-apply, not a caller-supplied current
stamp attached to a detached rate. The serialized coordinator first captures
its current central authority stamp, reads the transport's coherent
`(E_N, I_N, kind, rate)` observation, and then verifies that `E_N` still names
the current active pointer. It compare-applies only against the captured central
`G`; any central-stamp or current-`E_N` failure discards the entire snapshot and
retries from capture. It MUST NOT pair a value read from one activation with a
freshly read later stamp. Proposals for the same `E_N` are serialized through
one coordinator or use compare-and-swap on their captured `(E_N, G)`; a loser
discards its whole snapshot and rereads. Thus an older same-activation rate
cannot overwrite a newer accepted rate. `E_N` and `I_N` have no rate, health,
or path-order meaning.

A switch racing after the publisher's last current-`E_N` read may leave a
briefly installed central snapshot naming the predecessor, but it cannot make
that snapshot consumable: the switch has already advanced transport `E_N`,
published the durable wake, and every precommit compares that current value.
Only a central snapshot whose `E_N` still equals transport `E_N` is live
scheduling authority.

An accepted change of `E_N` advances `G` even when the underlying identity and
projected numeric rate are unchanged. It clears the predecessor activation's
controller-owned initialization and rate state and installs only the new active
state: `C_0` for `Absent`, or that activation's own `B_op` for
`Valid(B_op)`. Thus a new active instance cannot inherit MPP authority merely
from identity equality. When a retained or cloned controller becomes active,
only its current coherent observation decides the new basis; the reducer never
reuses a predecessor activation's MPP state. This source transition preserves
carrier work, Product state, and aggregate `Q/Z` under their separate rules.

This authority state machine is an implementation gate, not a universal
performance theorem. Before an adapter supplies live NativeMode authority,
deterministic transition tests MUST cover uninitialized-to-valid publication,
same-source rate replacement, distinct activation lifetimes for install,
rollback, and same-identity clone, delayed predecessor rejection, a complete
install-and-rollback between polls, failed capture/read/compare-apply retry,
serialized or compare-and-swap same-activation publication, durable activation
wake and bounded stable-activation publication, explicit structural
invalidation, checked `E_N`/`G` exhaustion, and consumer precommit rejection of
every stale complete stamp. These tests establish the reducer and publication
contract only; controller convergence remains subject to the bounded adapter
premises and empirical acceptance rules below.

Within that declared envelope, the adapter MUST document finite funded backlog
work `W_up`, a finite upshift progress/round bound `K_up` under sustained native
backlog and positive service/ACK progress, a finite downshift bound `K_down`
under continued ordinary backlog, and a finite controller-update-to-live-
consumer publication bound `D_pub`.
Loss or blackhole behavior needed for either finite bound MUST itself be
bounded; a mean loss percentage alone is insufficient. The upshift statement
quantifies only over positive representable required rates `R<C` in the
declared envelope, not over every real number. These are named adapter
obligations, not conclusions that Core derives from score arithmetic.

A configured `C_0 > 0` is only the NativeMode `StartupPrior` basis before the
adapter's first `Valid(B_op)` for the exact active source. That publication
changes the basis to `NativeOperational` and monotonically initializes that
source; thereafter `C_c = B_op`. A stable initialized source cannot revert to
`C_0` because of wall time, idleness, missing polls, application-limited
samples, or a low value. Every later changed valid controller value atomically
replaces `B_op` and advances `G`. MPP MUST NOT smooth it, cap its growth from an
earlier MPP estimate, maximize it with receipt or Product rate, or impose an
independent freshness or recovery timer. Receipt-derived Product and
observation rates remain diagnostic.

For `QuinnBbr3NativeOperationalV1`, an explicit finite QUIC startup-rate
contract enables a bounded pre-operational classifier. Omitted and Unlimited
configuration bypass it and retain the ordinary immediate projection. For one
finite-target controller lineage, let `A` be the authenticated MPP
application-ready event, `F` the first subsequently sent Data-space packet
number, and `S` BBR's immutable latest completed delivery-rate sample record.
The classifier is:

```text
PreReady
  -- A, serialized with packet processing --> AwaitFloor
AwaitFloor
  -- first subsequent Data packet F --> AwaitFirst(F)
AwaitFirst(F)
  -- eligible S1 from send-time round r1 --> Armed(F, r1)
Armed(F, r1)
  -- eligible S2 with S2.source_round > r1 --> Operational
```

Each record carries a checked nonzero controller-local revision, raw-sample
validity, selected source packet space and number, its send-time BBR round, and
its send-time application-limited bit. A record is eligible only when its
revision is new, its raw delivery sample and current `B_op` are positive and
representable, its selected source is Data with packet number at least `F`,
and BBR marked that packet non-application-limited. Pre-ready, pre-floor,
wrong-space, stale-revision, invalid, zero, unrepresentable, application-
limited, and same-round records are no-ops. They do not reset `Armed`, because
absence of evidence is not revocation.

The two distinct send-time rounds exclude Quinn's one-transmit-poll lag in its
application-limited stamp. Every packet in the first post-ready poll may carry
the preceding pre-ready `false`, but it belongs to one BBR send-time round. A
higher source round requires ACK progress and a subsequent transmit poll; a
non-application-limited selected packet there is backed by post-ready native
pressure. This proves a qualified native operational observation, not stable
capacity, spare headroom, or Product delivery. Sustained authenticated control
traffic may qualify; Product completion remains DataACK authority.

Readiness is monotonic and connection-shared. The floor, consumed-sample
revision, armed round, and operational latch are controller-lineage state. A
same-controller clone and retained rollback preserve them; a genuinely fresh
controller observes shared readiness but obtains a fresh floor and proof.
`Operational` is absorbing for the lineage. While qualification is pending,
the central reducer retains `C_0`, but native BBR still consumes every sample
and exclusively controls `bw`, `max_bw`, window, pacing, loss, and recovery.
The classifier is not another window, pacer, estimator, or traffic cap.

Under sustained backlog, positive ACK progress, and a finite per-round
feedback/recovery bound `Delta_feedback`, the conservative handoff bound is
`D_send + 3*Delta_feedback + D_pub`; the common stale-first-flight case needs
two rounds. No finite bound exists under a blackhole, unbounded loss, or an
application-limited workload, in which case retaining the explicitly
configured prior is intentional.

Every ordinary evaluation and its precommit revalidation MUST read an
immutable snapshot of the current central carrier authority and exact
authority stamp. Before commitment it MUST compare the complete captured stamp
for equality, including mode, central `(E_N, I_N, G)` in NativeMode, and the
separately observed current transport `E_N`. Failure discards the proposal and
recomputes selection from a fresh snapshot; merely rereading the rate, patching
the old proposal, or checking that the numeric value is similar is
insufficient. Any
separately required evidence ordinal, ledger generation, or reservation is
revalidated in the same transaction. A construction-time or per-stream copy
cannot be scheduling authority; it is diagnostic unless it is refreshed
through the bounded live-publication contract.

The successful final fence comparison and authority-dependent commit MUST have
one linearization order with native active-pointer switching. An implementation
may hold the switch fence through commit or carry `E_N` into the native writer,
which rejects it before ownership transfer if it no longer equals the current
activation. A check followed by an unfenced gap before commit is not
precommit revalidation.

`ReceiptMode` is selected at reducer creation when no such adapter contract
exists, or by the one fenced NativeMode revocation below. Native rate samples
and detached Product-flow rate samples are diagnostic-only for the remaining
reducer lifetime. A fresh ReceiptMode native-path scope starts with fallback
`H_R = C_0`, no active receipt term, and no acquisition. A ReceiptMode
native-path identity change fences the old term and acquisition, starts this
fresh ReceiptMode state, and advances `G`; it does not construct a new reducer
or switch authority mode. The ReceiptMode native-path identity is therefore a
subordinate equality fence for receipt evidence, not immutable reducer scope or
an ordering substitute for `G`.

ReceiptMode has exactly one optional active term `R_A > H_R`. It controls the
prediction until its absolute expiry, which is frozen from the exact
publication snapshot and is never refreshed. Otherwise the prediction is
`H_R`. Individual receipt events and detached local candidates cannot overwrite
or extend a live `R_A`. Only a disjoint `Acq_c` that independently passes the exact
ordinary decision below may supersede it in one close-and-fence transaction;
otherwise expiry settles `Z_c` under the old prediction and returns to `H_R`.
`Acq_c` may also publish in that serialized expiry transaction, but expiry by
itself cannot improve the carrier.

ReceiptMode also has at most one carrier-direction rate acquisition `Acq_c`.
When ReceiptMode evidence capacity is available, an otherwise-admitted positive
Product token MUST atomically create an absent `Acq_c` and its first suffix
anchor and receive its tag without any rate-causal prerequisite; this passive
metadata action sends no extra traffic. Evidence-capacity absence or exhaustion
leaves the Product token untagged and MUST NOT fail or block its commit. An observation token
may create and join `Acq_c` only after the exact rate-causal synthetic-admission
check in Section 15.1. A later Product commit naturally creates the disjoint
successor after publication closed its predecessor, except for the serialized
publication handoff defined below.

`Acq_c` owns a checked, non-reused acquisition generation; exact scope
`(session generation, original-sender direction, carrier incarnation,
native-path epoch, authority mode)`; carrier-ledger commit fence `f_acq`;
checked lower and upper accumulated busy-duration coordinates
`T_acq^- = T_acq^+ = 0`; one optional open-busy start `b_acq`; one optional
immutable quiescent freshness deadline `E_acq`; and a bounded ordered set of
suffix anchors.  Creation freezes one evidence feedback bound

```text
P_acq = SRTT + max(4 * RTTVAR, 1 ms) + 25 ms
```

from the exact carrier snapshot, or the startup tuple `333 ms` and `166.5 ms`,
using checked ceiling conversion to timer ticks.  Core's declared renewal
fraction is `alpha = 9/10`, so it freezes the busy-age authority horizon

```text
H_acq = ceil(P_acq / (1 - alpha)) = 10 * P_acq.
```

The normative algebra in this subsection treats `P_acq`, `H_acq`, `q_acq`,
`T_acq`, and `D_a^+` as fixed-point durations. An implementation represents
them as checked timer ticks at a positive frozen scale `G_acq` ticks per second.
Thus the integer expansion of a rate division is
`floor(8 * W_a * G_acq / D_a_ticks^+)`; dividing bytes by a bare tick count is
dimensionally invalid.

It also freezes a local anchor bound `J_acq >= 11` and spacing
`q_acq = ceil(H_acq / (J_acq - 1))` in timer ticks.  These are one operating-
envelope snapshot, not values refreshed by later RTT, polling, traffic shape,
or receipts.  An anchor is authority-live through elapsed upper bound
`H_acq` inclusive and becomes diagnostic-only above it.  `Acq_c` is carrier-
scoped rather than Product-stream- or synthetic-generation-scoped. An atomic
publication-handoff successor instead inherits the one `P_pub/H_pub` snapshot
captured by that transaction as specified below; it MUST NOT reread transport
state independently from its active term.

Ordinary creation, first-busy opening, token commit, and tagging are one
linearization: `f_acq` is the immediately preceding exact carrier-ledger
boundary and `b_acq` is the positive first token's commit time. That commit also
creates the first anchor `(f_anchor, busy-coordinate lower bound, counted
work=0)`, where `f_anchor` is the exact carrier-ledger boundary immediately
before the token's checked post-commit ordinal. Queue or actor delay before an
unselected carrier's actual commit is
therefore not misclassified as carrier service time. The sole pre-commit
exception is an atomic publication handoff: after installing `R_A`, the same
serialized transaction recomputes ordinary selection under the new rate and
may open its zero-work successor at that new fence only when positive exact
target-local backlog remains, every non-writer fact is true, and evidence state
is available. Actor/writer delay from that proved target boundary is genuine
busy time and conservatively lowers the successor rate. Cancellation,
rerouting, or loss of the target-local predicate closes an empty successor;
scope change fences it. Every later eligible commit revalidates the acquisition
fence and receives the same live tag.
After removing anchors whose elapsed upper bound exceeds `H_acq`, that commit
creates one new anchor when the set is empty or when the current busy-coordinate
lower bound is at least the last anchor's lower bound plus `q_acq`.  Thus anchor
creation follows proved busy-time resolution rather than Product frame count.
When, and only when, an exact peer-processing service frontier first retires
such a token, its normalized forward-frame work is added once to every retained
anchor whose `f_anchor` is strictly less than that token's post-commit carrier-
ledger ordinal. Product
Data ACK without exact copy service, queued cancellation, native loss,
terminal-without-service, polling, and duplicate receipt add nothing. A late
receipt for a closed acquisition may still retire carrier work but is an
acquisition semantic no-op. Observation contributes its DATA work `N + 32`,
not its `N + 68` receiver-grant charge: the reserved ACK is reverse-direction
work and is not proved by forward processing.

The busy interval remains open while at least one live acquisition-tagged token
is outstanding; or a fresh ordinary evaluation targets this exact carrier for
positive Product source/staged work with every non-writer fact satisfied and
only its actor/writer admission temporarily blocking commit; or one currently
rate-causal observation head has every non-writer authority and funding
prerequisite. A traffic-class label, work selected or committed elsewhere,
loss to another carrier by ordinary score, missing observation grant/budget,
application think time, or an empty source does not keep this carrier busy.
Writer, actor, native-flow-control, congestion, and receipt stalls while one of
the exact target-local predicates holds do keep it open and therefore cannot be
hidden from the rate denominator.

When a serialized recheck finds no such backlog and zero tagged outstanding
tokens, it closes the busy interval by adding conservative lower and upper
bounds for `t-b_acq` to `T_acq^-` and `T_acq^+` and, on the first such close
only, freezes `E_acq` at `t + 3 * P_acq`. A later eligible token before
`E_acq` reopens busy at its
commit time and excludes only the work-free idle gap; it cannot move or cancel
`E_acq`. Expiry closes and fences the acquisition even if a later busy interval
has reopened. Its old tags remain valid service ownership but become
acquisition no-ops, and a later eligible commit may create a successor. An
acquisition that stays continuously busy never arms this quiescent deadline.

Let `[T_acq^-(t), T_acq^+(t)]` include the checked elapsed bounds of a current
open interval when one exists. Anchor `a` retains its `f_anchor`,
busy-coordinate lower bound `T_a^-`, and checked counted normalized work
`W_a`. Define its conservative elapsed upper bound
`D_a^+(t) = max(timer granularity, T_acq^+(t) - T_a^-)`. While `W_a=0`, its
candidate is absent; while `D_a^+(t) <= H_acq`, it is authority-live; above
that boundary it is diagnostic-only. A positive-work authority-live anchor's
current exact suffix candidate is:

```text
r_a(t) = floor(8 * W_a / D_a^+(t))
r_acq(t) = max r_a(t) over authority-live anchors.
```

Every candidate is a separate post-anchor achieved-service lower bound; Core
maximizes rates but never sums them. Retaining every anchor until its authority
end or acquisition terminal avoids an eviction rule that repeatedly discards
the only mature recent suffix. Since live anchor lower coordinates are at least
`q_acq` apart, at most `floor(H_acq / q_acq) + 1 <= J_acq` are authority-live.
Arbitrarily small Product commands therefore cannot consume the anchor bound
before a high-BDP proof. Checked counter or coordinate exhaustion fences the
old acquisition before an otherwise-admitted Product commit; that commit may
create a zero-work successor without blocking Product. Observation may rotate
only when all of its independent optional authorities admit the new head. A
capacity contradiction despite the spacing proof is an evidence-local
internal fault: it fences acquisition and never fails Product.

Core re-evaluates current ordinary opportunities on every exact wake; the
opportunity need not be the one present when `Acq_c` began. A candidate is publishable
only when one evaluation fails with the current prediction solely because of
rate and the same evaluation, with every non-rate fact unchanged and `C_c`
replaced by `r_acq(t)`, reaches the reservation step. A positive candidate that
does not satisfy this exact comparator remains acquisition-local. It cannot
change `R_A`, close or rebase `Acq_c`, advance `f_acq`, refresh an expiry, or alter
Product authority. Thus prompt small receipts and unrelated small Product
flows add disjoint work but cannot repeatedly reset the integration interval.

Publication is one serialized transaction: revalidate the acquisition scope,
generation, exact maximizing anchor and work, busy time, authority horizon,
candidate time, and frozen ordinary snapshot; name that source acquisition's
frozen values `P_src=P_acq` and `H_src=H_acq`; capture one checked positive
`P_pub` from the current exact carrier snapshot using the complete `P_acq`
construction above, including its startup tuple and checked ceiling-to-ticks
rule, and derive
`H_pub = 10 * P_pub`; settle `Z_c` under the old prediction; install `r_acq` as
the new `R_A` with absolute lifetime `H_pub`;
close `Acq_c` and advance its exact commit fence; recompute ordinary selection
under the new rate; and, only under the exact target-local predicate above,
atomically open a zero-work successor whose first anchor stores that new fence
as `f_anchor` and whose `P_acq/H_acq` equal the same `P_pub/H_pub` before
waking. A race may prevent the later Product commit, but cannot unpublish the
carrier term or let any token from the closed acquisition enter its successor.
A successor created by this handoff tags only later commits. If the predicate
is false or evidence state is unavailable, a later eligible commit may create
the successor normally without blocking Product, but seamless active renewal
is then not claimed. Failure to represent `P_pub`, `H_pub`, the active expiry,
or successor state aborts this optional publication transaction without
changing Product or current evidence.

Active-term expiry does not close, rebase, or consume an unresolved `Acq_c`.
After returning the prediction to `H_R`, Core applies the same fresh exact
decision test to `r_acq`; it publishes only if that candidate now changes the
decision, and otherwise the acquisition continues. Likewise, a successful
same-carrier ordinary commit stops further synthetic DATA but does not destroy
`Acq_c`: that Product command's later exact service is legitimate carrier work.
`Acq_c` ends only on publication, its immutable quiescent freshness expiry,
checked acquisition-identifier or duration/work exhaustion, explicit
cancellation, or replacement/terminal of its exact session, direction,
carrier, native-path, or authority-mode scope. Exhaustion
of observation grant or optional local budget stops only synthetic admission
while ordinary Product commits can still feed `Acq_c`; one source cannot erase the
other source's accumulated legitimate work. Locator-only migration may
preserve `Acq_c` only when it preserves the declared native-path epoch.

This state is bounded by one active term, one acquisition with at most
`J_acq` authority-live suffix anchors, checked clock/work counters, and the
already-bounded exact token ledger. Its historical influence is also explicit:
an acquisition may span an earlier active term's expiry, and a term it later
publishes may remain live for one further fixed freshness lifetime. The first
quiescent deadline bounds reuse across application-idle gaps without truncating
one continuously busy high-BDP measurement. ReceiptMode
therefore provides conditional finite recovery, not unconditional stable-rate
renewal. No finite receipt horizon can continuously prove an arbitrarily high
fraction of capacity on every high-BDP path.

Authority mode is fixed at reducer creation except for one serialized explicit
structural adapter-contract-unavailable or contract-revoked event, which MAY
switch `NativeMode` to a ready `ReceiptMode` once in the same carrier-direction
reducer lifetime. It compare-applies the exact structural fence, settles `Z_c`
under the old prediction, clears native scheduling-rate authority and any
pre-switch diagnostic receipt acquisition, freezes the current carrier-ledger
commit ordinal as the earliest ReceiptMode acquisition floor, freezes
`H_R = min(C_0, C_old)` from the positive prediction in force immediately
before the switch, and advances both the authority revision and the applicable
evidence ordinal in one mutation. Thus loss of the adapter contract cannot
improve the carrier's rank by restoring the larger startup prior. A receipt
term is authority-mode-scoped and cannot be reclassified afterward merely
because the mode changed. The transaction preserves the observation
generation, queued/native work, cumulative spend, and Product state. A low
`B_op`, a missing poll, application idleness, wall time, or score change is not
contract revocation. `ReceiptMode` MUST NOT switch back within that reducer
lifetime; a new carrier incarnation constructs a new reducer and chooses
authority afresh.

The native timing projection has the same non-promotion rule. Let configured
propagation and jitter priors be `P_0` and `J_0`. The latest qualified timing
sample `(p,j)` overwrites persistent fallbacks with
`H_P = max(P_0,p)` and `H_J = max(J_0,j)`. Fresh timing uses `(p,j)`; after its
expiry the projection uses `(H_P,H_J)`. Therefore a high-delay or high-jitter
sample cannot become a better path merely by expiring, while a later genuinely
lower sample clears the caution back toward the configured priors. These
memories reset only with the exact native network-path epoch. A locator-only
port hop that preserves that epoch does not reset them.

Every indivisible evidence transaction receives one checked, non-reused
ordinal when its source event enters the serialized carrier-direction evidence
actor. All terms from that event share the ordinal. An asynchronous proposal
carries its capture ordinal and is rejected if a later carrier transaction has
committed. Ordinal exhaustion disables new optional evidence publication for
that exact carrier incarnation and clears its live ReceiptMode optional terms;
scheduling uses the specified ReceiptMode pessimistic fallback. It is a
structural failure of a NativeMode adapter's bounded live-publication contract
and MUST be handled by the fenced revocation or carrier-terminal rule above,
not by silently restoring `C_0`. It MUST NOT block the same frame's independent
service, Product, credit, or terminal transition. A `NativeMode` controller
update changes only native scheduling evidence; it does not fence,
terminalize, pace, shrink, or refund the carrier-observation excitation that
supplies its backlog. A native-controller update received in `ReceiptMode`
cannot update scheduling evidence. This evidence ordinal does not replace the
central authority revision `G`: an authority mutation advances `G` under the
rules above, while an unrelated diagnostic evidence mutation need not do so.

The freshness deadline of a detached rate, a ReceiptMode active rate, or a
timing sample is fixed from the sample's own timing epoch. Later polling,
idleness, transport shape, or application-limited state cannot move it.
NativeMode `B_op` has the exact activation-fence and authority-revision
lifetime instead of such a sample deadline. An open ReceiptMode acquisition
may cross the active term's deadline
because it has not yet published or reused that term's source. It retains only
its own post-fence exact token sum and checked acquisition generation; its
qualifying completion starts a disjoint active epoch.

A local native controller may expose a current gain-free operational bandwidth
model. It enters `C_c` only through the named NativeMode operational-bandwidth
contract above; in every other mode it is diagnostic. Pacing rate is gain-
scaled send intent and is never interchangeable with `B_op`. Raw ACK-window,
Product-goodput, receipt, and peer rate estimates are also diagnostic in
NativeMode. None can grant Product bytes, writer credit, qualification, path
independence, or confidence. Peer `PATH_METRICS` is detached evidence and
cannot select authority mode or serialize a local live-controller lifetime.

An unambiguous MPP Data ACK may establish Product qualification, but
ReceiptMode carrier rate counts a Product command only when its exact-copy
peer-processing service frontier retires the tagged carrier-work token. A
duplicated range with no exact copy receipt proves Product delivery but not
which carrier served it. It contributes no carrier rate and triggers the causal
output rule in Section 15.1. Native TCP ACKs, QUIC packet ACKs, stream writes,
queue deltas, or receiver callbacks cannot invent Product attribution.

MPP commit time precedes native departure and Core has no receiver delivery
timestamp. A sender therefore MUST NOT imitate a packet delivery sampler from
commit spacing or ACK callback spacing. Any finite-anchor approximation needs
its own proved accuracy and adaptation bound; the only Core approximation is
the explicit conservative-clock, `H_acq/J_acq/q_acq` suffix construction above.
A locator, interface, route, carrier family, active-flow count, or configured
path count establishes neither capacity nor bottleneck identity.

### 10.3 No second congestion controller

RTT, loss, ECN, jitter, queue, flight, pacing, and delivery observations MAY
affect ranking, qualification of their own typed evidence, diagnostics, and
application record or batch size. They MUST NOT shape the Product acquisition
envelope `E_i` specified in Section 15.1. Native
admission remains the exact bounded writer reservation and native backpressure;
the portable atomic Product quantum only bounds one scheduler commitment. MPP MUST NOT
use those observations to:

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

All MPP-owned scheduling, retention, reinjection, measurement, queue, and
diagnostic allocations MUST have byte and item bounds plus one exact
cancellation or terminal owner. A time bound is REQUIRED only where this RFC
defines a timer or absolute retention lifetime. Native-owned reliable debt has
no finite service-time guarantee while its native transport remains live; it
stays charged within its byte/item cap until exact peer-processing receipt or
writer/carrier terminal. Cancellation MUST reconcile each queue reservation,
flight, measurement ticket, load lease, and registry entry exactly once.

The final carrier writer MUST preserve class boundaries while work remains
MPP-owned. At every MPP command-selection boundary it serves dependency-ready
Control/lifecycle/Data ACK/carrier-service ACK/carrier-observation grant and ACK
work first, then Realtime and Latency work, then due cause-bounded recovery,
ordinary Throughput, optional repair, and carrier-observation DATA in that
order. It re-enters this arbitration after each selected
command; it need not wait for native delivery or acknowledgement and therefore
does not impose one-frame stop-and-wait. A native-capacity release exposes
pending higher-class work before another lower-class command is handed off.
Priority MUST NOT overtake an earlier protocol prerequisite: OPEN, DATA, FIN,
drain, detach, and other lifecycle fences retain their exact dependency order.

This priority cannot preempt bytes already accepted by a shared TCP socket,
QUIC connection, kernel queue, or other native FIFO. No lower-class command
that is still MPP-owned may be selected ahead of dependency-ready higher-class
work, but the latter may additionally wait behind bounded mandatory MPP
predecessor debt and a bounded amount of already-native-owned debt. Core states
no finite time bound for native debt at zero service and no one-frame native-
debt bound; either claim would require limiting handoff and BDP fill or
separating traffic classes onto independent carriers.
Within one class, positive quanta from continuously ready streams receive
weakly fair turns and a blocked stream owns no writer turn. Persistent
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

The receiving peer MUST NOT install, reconstruct, refresh, or downshift local
`C_c`, NativeMode `B_op`, or ReceiptMode `H_R/R_A` from this detached record;
only the producer's exact local evidence actor owns that authority.
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
| 7 | `OPEN_STREAM` | `stream_id:u64, target, demand:u8` |
| 8 | `STREAM_DATA` | `stream_id:u64, offset:u64, length:u32, bytes` |
| 9 | `STREAM_ACK` | `stream_id:u64, complete:u8, range_count:u16, ranges[range_count], service_count:u16, services[service_count]` |
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
| 32 | `PATH_PROOF_ACK` | `path_id:u16, proof_id:u64, payload_bytes:u32, writer_epoch:u64, processed_frontier:u64` |
| 33 | `PATH_CAPACITY_DATA` | `path_id:u16, measurement_id:u64, length:u32, bytes` |
| 34 | `PATH_CAPACITY_FINISH` | `path_id:u16, measurement_id:u64, payload_bytes:u64` |
| 35 | `PATH_CAPACITY_RECEIPT` | `path_id:u16, measurement_id:u64, received_payload_bytes:u64, writer_epoch:u64, processed_frontier:u64` |
| 36 | `PEER_STATUS_REQUEST` | `request_id:u64` |
| 37 | `PEER_STATUS_RESPONSE` | `request_id:u64, code:u8, count:u16, paths[count]` |
| 38 | `OPEN_IP_TUNNEL` | `tunnel_id:u64` |
| 39 | `IP_TUNNEL_READY` | `tunnel_id:u64, mtu:u16, address_count:u8, addresses[address_count]` |
| 40 | `IP_PACKET` | `tunnel_id:u64, packet_id:u64, length:u32, bytes` |
| 41 | `IP_TUNNEL_CLOSE` | `tunnel_id:u64, reason:u8` |
| 42 | `STREAM_REQUALIFY_DATA` | `stream_id:u64, probe_id:u64, offset:u64, length:u32, bytes` |
| 43 | `STREAM_REQUALIFY_ACK` | `stream_id:u64, probe_id:u64, offset:u64, payload_bytes:u32, writer_epoch:u64, processed_frontier:u64` |
| 44 | `CARRIER_OBSERVE_MAX_WORK` | `path_id:u16, max_charged_work:u64` |
| 45 | `CARRIER_OBSERVE_DATA` | `path_id:u16, generation_id:u64, generation_offset:u64, length:u32, bytes` |
| 46 | `CARRIER_OBSERVE_ACK` | `path_id:u16, generation_id:u64, cumulative_payload_bytes:u64, processed_observation_work:u64` |
| 47 | `SERVICE_EPOCH` | `writer_epoch:u64` |
| 48 | `SERVICE_ACK` | `service_count:u16, services[service_count]` |

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
to the carrying authenticated carrier instance. For kind 42 the carrying
attachment is the proved forward target. For kind 43 the carrying attachment
is only authenticated return service; the sender's exact pending tuple names
the forward target. A reusable path ID or the ACK carrier cannot substitute
for either exact attachment incarnation.

Kinds 44 through 46 are the carrier-directional observation transaction from
Section 15.1. Each `path_id` MUST equal the authenticated physical carrier
binding. `CARRIER_OBSERVE_MAX_WORK` is cumulative and nondecreasing; sent from A
to B, it grants B observation work toward A. It elicits no ACK. DATA has exact
normalized work `N + 32` bytes and consumes charged grant `N + 68`, reserving
one maximum 36-byte cumulative ACK. Observation payload is not Product and MUST
NOT enter a receive map, Data ACK, credit, or qualification state.

On TCP, kinds 44 through 46 use the authenticated physical carrier writers and
the socket incarnation is their channel epoch. On QUIC they are valid only on
the one client-opened observation request stream of that physical connection;
the first MPP frame in each half MUST be kind 44, including zero grant. DATA and
its ACK cannot move to the control stream, a Product request stream, or another
carrier. Receiving them in another context is a protocol violation.

`CARRIER_OBSERVE_ACK` is cumulative in two independent coordinates. The
generation payload frontier drives only live semantic evidence; the channel
processed-work frontier retires complete observation tokens across current or
retired generations. ACKs name but occupy no interval in that processed-work
coordinate and elicit no ACK. Kinds 45 and 46 MUST NOT also enter generic
Section 8.3 service accounting.

Kind 47 establishes the Section 8.3 service coordinate for the carrying
ordered writer direction. On TCP it appears once after carrier readiness and
before the first positive generic service-bearing kind `8`, `31`, `33`, or `42`
sent in that direction. On QUIC it appears once on each HTTP/3 request-stream
send half that can carry one of those kinds, after its opening or acceptance
prerequisite and before its first such command. It MUST NOT appear on the
dedicated observation request stream. A second or zero epoch on the same writer
is a protocol violation.

Kinds 8, 31, 33, and 42 are generic service-bearing frames. Each occupies its exact
normalized encoded-work interval in the carrying writer coordinate after full
processing. The corresponding kinds 9, 32, 35, and 43 carry the cumulative
service frontier that covers that command. A dedicated receipt first applies
its service entry, then applies only its proof, capacity, or requalification
semantics; it MUST NOT retire carrier work a second time. A later
cumulative frontier subsumes a lost dedicated receipt, so repeated bounded
transactions cannot leak native-owned carrier work while the writer remains
live. `PATH_CAPACITY_RECEIPT` remains diagnostic in its capacity semantics;
its service entry proves only processing of the exact forward command.

Kind 48 is a Product-neutral cumulative service-only publication. A copy is
valid on any authenticated carrier of the same session and may carry frontiers
for any exact forward writer epoch in the opposite original-sender direction.
It uses the same validation and retirement semantics as a `STREAM_ACK` service
vector, carries no Product range or stream identity, and elicits no ACK. It is
the required publication path when an origin logical stream has terminalized
but its shared writer and processed frontier remain live. Only acceptance by
the same-fate reverse queue defined in Section 8.3 discharges the receiver's
dirty authority; other copies are optional acceleration.

### 12.3 Common field encodings

Each `ranges[range_count]` entry is `start:u64, end:u64` and represents
`[start, end)`. `start` MUST be less than `end`.

Each `services[service_count]` entry is
`writer_epoch:u64, processed_frontier:u64`. The epoch is nonzero. Service
entries in one frame MUST have distinct epochs and canonical increasing epoch
order. The complete Product bit has no effect on their cumulative semantics.
`SERVICE_ACK` requires `service_count > 0`; an empty service-only publication
is noncanonical and rejected.

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
- live ordered-writer epochs, sender token deques, dirty service frontiers,
  service-vector entries, output-admission epochs and guarded-token counts,
  aggregate carrier `Q/Z`, and ledger generations;
- datagram attempts, TTL, caches, fragments, and reassemblies;
- proof, capacity, metric, and peer-status work;
- observation channels and semantic generations, ReceiptMode acquisitions and
  token tags, heads, cumulative work-token intervals, dirty ACK state, and
  channel/session/principal grant escrow and spend;
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

Peer metrics, usage, and capacity-measurement receipts are authenticated input. They MUST
NOT:

- grant receive credit;
- release retained data;
- establish local delivery;
- declare local health;
- bypass queue or flight bounds; or
- transfer state to another carrier instance.

A carrier-service entry is separately authenticated in the exact session,
original-sender direction, and ordered-writer epoch. The sender rejects a
frontier above its assigned value; an equal or lower value is idempotent. An
advancing value may retire only complete covered tokens and update exact
same-output service provenance. It MUST NOT release Product, grant credit or
qualification, rewrite usage or health, create native authority, or mutate a
different physical carrier. The complete containing frame is validated before
any service or Product mutation.

Carrier-observation work is accepted only on its exact authenticated channel
and atomically within channel, session-direction, and principal-direction
grant/escrow. One-byte heads cannot amplify receiver work or reverse ACKs
because accepted charged work includes the complete DATA frame plus one maximum
ACK. Invalid, gapped, overlapping, replayed, grant-exceeding, or wrong-channel
DATA changes no counter and elicits no ACK. Channel/session creation cannot
renew principal startup authority, and terminal refunds only unused escrow.
Observation grant or receipt MUST NOT grant Product credit, delivery,
qualification, target policy, health, native send credit, or another carrier's
authority. The principal-direction consumption ledger survives closure of its
last session within the endpoint policy epoch; otherwise reconnect alone would
replay the startup allowance.

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

Within the Regular or Backup set selected by Section 7, ordinary original-data
placement uses a carrier-scoped service-pressure rank subject to shared receive
credit, carrier enqueue capacity, and reorder bounds. The rank is advisory and
does not claim to minimize unknowable receiver completion time. Product
qualification is a consequence of ordinary placement; it does not own a
fairness cursor, force a visit, or turn an unmeasured service prediction into
fact. The optional non-delivering observation plane below can refresh
suppressed carrier evidence without assigning a Data Sequence range.

The output carrying the contiguous frontier is governed by shared MPP credit
and its native carrier. Before an additional output in either stream direction
has durable, unambiguous Data ACK coverage for original transmissions, it may
own at most one bounded startup flight. Native TCP ACK or QUIC packet-ACK
evidence alone does not unlock mature additional-output placement.

That live-contiguous treatment applies only while the lowest
outstanding frontier is live: no retained complete Data ACK proves that its
lowest range is missing. When a retained complete Data ACK omits that range,
the frontier becomes an authoritative-gap frontier. Its exact owner remains
the ordering, hysteresis, and recovery reference, but fresh originals on that
owner MUST use the additional-output Product and reorder position until Data
ACK progress advances or resolves the gap. Every live or
authoritative-gap owner still passes the Product assignment authority defined
below. Other eligible outputs are not globally paused. An
incomplete positive ACK cannot create this state, and this rule neither lowers
the configured reorder envelope nor changes native TCP or QUIC recovery.

An output does not become the contiguous-frontier owner merely because it is
the only output that can currently enqueue. While an unresolved lower original
range belongs to another output, the survivor remains an additional output and
MUST retain the corresponding Product flight and reorder bounds.

After current-generation exact tagged OriginalData Data ACK coverage reaches
the configured positive qualification floor, the output gains durable `q_i`
and its configured `P_i` assignment authority. Independently, Section 10.2 may
accept typed evidence for the shared carrier direction. Duplicated Product
bytes do not satisfy the volume floor or establish a delivery sample for either
copy; an exact same-copy service receipt remains valid carrier-work evidence.

In NativeMode, an app-limited transport observation has only the update effect
declared by the named controller contract. It neither creates a wrapper rate
nor revokes or expires `B_op`; current controller state retains scheduling
authority for its exact activation fence and authority revision. In ReceiptMode,
an app-limited native observation is diagnostic, and expiry of a qualified
receipt term returns only
to the Section 10.2 pessimistic fallback without manufacturing an improvement.
Neither observation creates a Product admission generation or placement
authority. Exact Product-volume qualification remains durable for the current
active incarnation, so a ReceiptMode rate expiry neither demotes `P_i`
assignment authority nor starts another qualification generation. Ordinary
Product placed under that authority may mature later ReceiptMode evidence
opportunistically.

Reliable OriginalData uses resource, acquisition, and native-transport
authorities that MUST remain separate. Its Product byte authority is independent
of traffic class and TCP/QUIC underlay. Let:

```text
I_0 = 10 * 1460 = 14,600 Product payload bytes

W   = min(configured stream window,
          configured repair window,
          configured reorder window)

P_i = min(W, configured path-flight window)

E_i = min(P_i,
          max(I_0,
              configured qualification floor
              + maximum atomic Product quantum))
```

`W` is the shared logical-stream Product resource envelope. `P_i` is exact
output `i`'s configured Product envelope for unique bytes awaiting MPP Data
ACK. `I_0` is the profile's immutable portable startup scoring prior, retained
from the ten-segment initial-flight geometry; it is Product payload geometry,
not a claim about the current TCP MSS, QUIC PMTU, native congestion window, or
achieved capacity. `E_i` is the class- and underlay-independent Product risk
cap needed to carry one complete qualification floor plus its possible final-
quantum rounding. A native congestion window, available congestion credit, peer flow-
control window, pacing estimate, or connection-wide QUIC limit cannot make
unique low-sequence exploration safer and therefore cannot enlarge `E_i`.
Those native authorities remain enforced by the exact writer below it. An
exact output publication of zero `P_i` is a complete negative observation and
MUST fail closed; global path discovery may use configured `P_i` only as an
advisory projection before an exact output exists.

Traffic class controls arbitration priority, path activation, and the maximum
atomic service quantum. It MUST NOT select a smaller `W`, `P_i`, initial
reliable receive grant, or Data-ACK release rule. The initial reliable
`STREAM_MAX_DATA` authority is `W`; later grants remain monotonic under Section
8. Advertising that authority does not allocate `W` bytes and does not bypass
the configured stream, repair, reorder, sparse-node, or native-writer bounds.
In particular, a latency-class or `Automatic` stream over QUIC MUST NOT begin
with a class-specific Product window: such a window is an additional Product
ACK clock above an already congestion-controlled carrier and imposes the hard
throughput ceiling `8 * window / RTT` until replenishment.

These envelopes are safety and reordering authorities, but they are also real
Data-ACK-clocked windows.  If `tau` is the elapsed time from Product assignment
until the corresponding advancing Data ACK can release authority, then a sole
output can sustain no more than `8 * P_i / tau` bits/s from `P_i` alone, and
all outputs together can sustain no more than `8 * W / tau` bits/s from `W`
alone.  An unqualified additional output is similarly bounded by
`8 * E_i / tau` until it qualifies.  A profile intended to sustain Product
rate `R` MUST therefore configure `P_i >= ceil(R * tau / 8)` for a sole-output
case and `W >= ceil(R_aggregate * tau / 8)` for the aggregate case, using the
largest feedback delay in its claimed operating envelope.  This is a necessary
authority condition, not a throughput promise: native service, receive credit,
reordering, application consumption, and loss can lower the result.  Core
claims no target throughput merely from `W`, `P_i`, or `E_i`.

Let `O` be the stream's unique OriginalData debt and `O_i` the subset assigned
to exact output `i`. The effective assignment envelope `L_i` is:

```text
L_i = P_i  for the first owner when O = 0;
      P_i  for the exact owner of a live contiguous frontier;
      P_i  for a qualified additional output;
      E_i  for an unqualified additional output.
```

An authoritative-gap frontier and a sole currently enqueueable survivor whose
lower range belongs to another output are additional outputs under this rule.
For an exact pending OriginalData quantum of `N` bytes, commitment requires
`O + N <= W` and `O_i + N <= L_i`, plus shared receive credit, current
structural eligibility, reorder authority, and a real bounded writer-command
reservation. The complete quantum MUST fit; there is no overshoot exception.
Planning is advisory.
After obtaining the writer reservation, the sender MUST revalidate the exact
output incarnation, current position and qualification, `W`, `P_i`, and `E_i`;
it then records exact Product ownership before publishing the command. Failed
revalidation refunds the uncommitted writer reservation.

Ordinary numeric order uses only the current carrier-direction service
prediction and propagation timing defined below. Sampled native queue, flight,
loss, ECN, confidence, application-limited state, and active-flow count may
qualify typed carrier evidence or diagnose service, but do not add independent
score penalties and MUST NOT multiply or divide physical carrier capacity.
They do not establish Product assignment qualification; only current-generation
exact tagged Product delivery reaching the configured qualification floor
changes an additional output from `E_i` to `P_i`.

Those observations MUST NOT shrink or enlarge `W` or `P_i`, and the advisory
score MUST NOT install another Product congestion gate above the writer. The
selected TCP or QUIC writer, its bounded command admission, native socket or
stream backpressure, pacing, congestion control, and recovery remain final
native transport authority. A native ACK can reopen that native authority, but
it cannot release `O` or `O_i`; only MPP Data ACK or terminal Product cleanup
can do so. Recovery authority `K` remains separately bounded and cannot mint
fresh OriginalData. None of these authorities bypasses shared receive credit,
reorder, queue, repair, or configured resource bounds.

Attachment membership is not active path demand. A carrier-open transaction
MAY publish one prospective load claim while asynchronous I/O is outstanding,
but MUST release it when the attachment commits, fails, is cancelled, or is
rejected. A current attachment publishes active demand exactly while it owns
un-DataACKed unique OriginalData (`O_i > 0`). ReinjectedData does not create or
retain that demand. Detach removes demand synchronously before asynchronous
wire cleanup; retained old-incarnation Product debt remains in the stream
ledger for ACK and recovery and MUST NOT be projected into a same-key physical
successor.

Fresh OriginalData is reserved in the shared bounded carrier command queue
before Product flight is published. That queue is the single staging resource
and reservation linearization point for all Product actors sharing one native
writer. Its exact pending-byte accounting is resource state, not renewable send
credit, and MUST NOT impose a smaller one-frame or one-quantum stop-and-wait
lease above the native transport. The carrier writer MAY continue through a
bounded sequence of reserved commands without waiting for a native ACK, but
re-enters class/dependency arbitration after each command; native `AsyncWrite`
or QUIC stream flow control remains backpressure authority. Control and
ReinjectedData retain their separate priority admission, while the common
queue and configured Product envelopes continue to bound aggregate memory and
ordering debt.

Current local controller application-limited state is separate from the
immutable application-limited provenance of a qualified delivery-rate epoch.
Retaining, expiring, or replacing carrier evidence MUST NOT rewrite that
current local state, and peer telemetry MUST NOT supply it. Native admission
participates symmetrically in both directions through exact writer-command
reservation and native backpressure. There is no request-only QUIC tie-break.

Core does not construct numeric native send credit from sampled queue, writer,
flight, Product counters, or their maximum. Those counters can overlap and use
different units. Instead, every reliable data-bearing command owns one exact
carrier-work token in the normalized unit of Section 10.2. The token exists in
exactly one of these disjoint stages:

```text
provisional reservation -> MPP queue -> native-owned -> peer-processed
```

An exact cancellation before native handoff retires a provisional or queued
token without claiming service. Dequeue and final writer handoff move the same
token to native ownership. A generic kind `8`, `31`, `33`, or `42` allocates its
writer-epoch interval; observation kind `45` instead allocates its exact
observation-channel work interval. Local write completion does not retire
either. The corresponding advancing authenticated frontier retires the covered
native-owned token after peer processing. Exact carrier or observation-channel
terminal may retire the tokens in its exact scope without claiming service.
Product ownership `O/O_i`, native carrier-work ownership, receive credit, and
writer reservation are intentionally distinct authorities.

Each active reliable session direction has per-stream ordinary Product
placement and Product-neutral per-carrier observation planes. Ordinary
placement is scoped to stream and attachment incarnation and assigns fresh Data
Sequence ranges. Observation is scoped to carrier/channel incarnation and
assigns none. Both create exact carrier work when they publish a data-bearing
command, but observation uses its independent cumulative processed-work
coordinate rather than a generic ordered-writer service interval.

For one exact carrier direction `c`, let `Q^p_{c,h}`, `Q^q_{c,h}`, and
`Q^n_c` be normalized work owned respectively by provisional reservations,
the MPP queue, and the native writer, where `h` is the arbitration priority.
Let `Q^t_c = Q^n_c + sum_h(Q^p_{c,h} + Q^q_{c,h})`, let `K^t_c` be the
number of tokens in all three stages, and let `K^n_c` be the native-owned
subset. Let the finite checked `N^B_c` and `N^I_c` be respectively that exact
carrier direction's immutable all-stage carrier-token byte and item
authorities. A data-bearing carrier requires `N^I_c >= 1` and `N^B_c` at least
the largest atomic positive encoded-work command that its profile permits;
otherwise configuration fails before carrier publication. These authorities
are fixed when the carrier direction is created; changing one
requires an exact replacement carrier identity rather than mutating a live
bound beneath retained debt. Let `Z_c` be predicted remaining native-owned
service work. At every linearization point:

```text
Q^p_{c,h} >= 0, Q^q_{c,h} >= 0, Q^n_c >= 0,
Q^t_c = Q^n_c + sum_h(Q^p_{c,h} + Q^q_{c,h}) <= N^B_c,
0 <= Z_c <= Q^n_c,
0 <= K^n_c <= K^t_c <= N^I_c.
```

These totals and their clock belong to the physical carrier direction. All
QUIC HTTP/3 writer epochs on one connection map into the same `Q/Z`; giving
each writer its own clock would manufacture one full `C_c` of service per
stream. Writer epochs retain exact token/frontier identity but no independent
capacity.

Queued or provisional work does not drain with wall time. Creating one exact
token of work `w` atomically reserves its eventual native-ledger byte and item
slot together with its writer-command reservation. It requires
`Q^t_c + w <= N^B_c` and `K^t_c + 1 <= N^I_c`. A failed reservation publishes
no token or Product owner and lets ordinary finite candidate selection try an
alternate carrier. Zero-coordinate `STREAM_ACK`, `SERVICE_ACK`, and dedicated
receipts own their separately bounded Control-queue reservations but no
carrier-work token, so positive token exhaustion cannot consume their
same-fate receipt authority or create ACK recursion.
An ordinary payload quantum larger than the profile's maximum fitting atomic
work is split before Product assignment; an indivisible positive protocol
command that cannot fit is rejected before publication. Neither case leaves
partial Product ownership or a provisional token.

Moving a reserved token from queue to native ownership then preserves
`Q^t_c` and `K^t_c`, increments `K^n_c`, and adds `w` to `Z_c`; it cannot lose
a second capacity race after Product publication. Cancellation before handoff,
receipt after peer processing, or exact terminal removal releases the exact
all-stage byte and item reservation and publishes the corresponding capacity
wake. The shared carrier-ledger generation covers reservation, transfer, and
release, so concurrent writers cannot each consume the same last byte or item.
At zero reverse service the separately bounded Control queue can itself fill;
Core then promises bounded backpressure, not impossible progress.

`N^B_c` and `N^I_c` are resource bounds, not another pacer or congestion
window. They MUST NOT be derived, divided, shrunk, or expanded from sampled
rate, RTT, loss, ECN, active-flow count, or controller flight; doing so would
install another transport controller. They nevertheless can impose feedback-clocked ceilings: if a token
remains charged for service-receipt delay `tau_s`, then the byte authority
alone can cap publication near `8 * N^B_c / tau_s`, and the item authority can
cap it near `N^I_c / tau_s` commands/s. A claimed operating
profile MUST size both above its maximum intended encoded-work BDP and command
rate over the claimed same-fate receipt delay.  Core claims bounded memory and
honest backpressure, not target throughput, from these authorities.

Before any later carrier-ledger event at `t`, the one shared native service
clock settles:

```text
Z_c(t) = max(0, Z_c(t0) - C_c * (t - t0) / 8).
```

There is exactly one such clock per carrier direction, not one clock per
logical output, stream, or work token. Applying an exact service receipt first
removes the covered tokens from `Q^n_c`, then sets
`Z_c = min(Z_c, Q^n_c)`. It does not subtract the covered bytes a second time.
A current-rate transition first settles through its exact boundary using the
old `C_c`, then installs the new value. The clock retains its fractional
service remainder, never creates future credit, and is invariant within one
representational quantum to splitting one interval across polls or events.

For pending priority and dependency key `k`, let `Pred^m_c(k)` be the exact set
of provisional or queued tokens that the carrier arbiter proves the command
cannot overtake: unmet dependencies, higher class, earlier same-class order,
and any cross-writer work for which no central ordering certificate proves
overtaking. Define the carrier debt ahead of that command as:

```text
D_c(k) = Z_c + sum of normalized work in Pred^m_c(k).
```

Every already-native command is included because MPP can no longer preempt it.
A lower-priority queued token may be excluded only when the shared carrier
arbiter and its generation prove that this command will hand off first. Across
independent QUIC writers, absent that proof it remains in `Pred^m_c(k)` because
native inter-stream service order is not portably observable. Handoff moves one
must-precede token from `Pred^m` to `Z_c` with its full work, so the score cannot
gain a fictitious discount. If `M_c > 0` is the pending command's exact
normalized encoded work, `C_c > 0` the Section 10.2 carrier service prediction,
and `T_c` the current one-way propagation prediction, the advisory
service-pressure score is:

```text
S_c(k, M_c) = T_c + ceil(8 * (D_c(k) + M_c) / C_c).
U_c          = max(J_c, timer granularity).
```

`S_c` is evaluated in an extended nonnegative time domain. Implementations
compare finite terms with checked widened arithmetic; if the advisory sum,
product, quotient, or duration is not representable, that candidate's score is
`+infinity` for this frozen ordering pass. Infinity is worst rank, not
structural ineligibility or enqueue refusal. If it is the only candidate whose
real Product and writer authorities admit, Core still attempts that exact
commit. Thus advisory arithmetic cannot contradict work conservation.

The score and uncertainty use current carrier-scoped evidence. Before evidence
exists they use carrier startup priors. Let `M_0` be the canonical normalized
encoded work of one `STREAM_DATA` carrying `I_0` Product bytes; under this wire
profile `M_0 = I_0 + 30 = 14,630` bytes. When omitted, the timing priors are
`RTT_0 = 333 ms`, `T_0 = RTT_0 / 2 = 166.5 ms`, and `J_0 = 166.5 ms`, while
`C_0 = 8 * M_0 / RTT_0` (approximately `351 kbit/s`). A low or unknown `C_c` orders alternatives but
does not rate-limit, window-limit, or pace the sole admitting carrier. Missing
evidence is never measured zero.

All ownership-ledger arithmetic is checked fixed-point arithmetic. Time
projection rounds up; achieved-service bounds round down. An unrepresentable
frame/work amount is rejected before reservation and changes no Product or work
ownership; overflow of a live ownership counter takes the absorbing terminal
path below. Advisory score overflow follows the `+infinity` rule above and
never removes an otherwise admitting carrier. Every provisional addition, transition,
cancellation, receipt, rate boundary, and ordinary terminal clear advances a
checked non-reusing carrier-ledger generation. Candidate apply binds and
revalidates that shared generation, the complete scheduling-rate authority
stamp, evidence ordinal, priority, `M_c`, exact writer generation, and Product
authorities. Thus concurrent streams cannot all quote the same empty carrier
and then publish unaccounted work.

Carrier-ledger generation exhaustion is not an ordinary failed proposal,
because existing receipts and terminal cleanup must still make progress. The
last representable successor is reserved as an absorbing `Exhausted` state. An
attempt to allocate it atomically makes the carrier direction non-admitting,
invalidates every captured plan, and performs exact carrier-terminal cleanup
without allocating another numeric generation or acknowledging Product. The
physical carrier closes through ordinary exact-failure recovery; retained
Product survives. Thereafter only idempotent terminal cleanup and stale-receipt
no-ops may touch that exhausted ledger. It never wraps, saturates as a live
generation, or admits a new Product or work token.

Only a successful command commitment, after real writer reservation, creates
one provisional work token. The same infallible mutation records Product or
optional ownership when applicable and advances the shared ledger generation
before publication. Failed revalidation removes the provisional token and
refunds its reservation. Publication moves it to the queue; writer handoff
moves it to native ownership. Reinjection, observation, and requalification
create their own copy-specific carrier tokens even when they create no new
Product owner. Product Data ACK never retires native-owned `Q^n`, settles
`Z_c`, or claims carrier service. Its sole carrier-ledger removal is Section
8.5's atomic complete-range cancellation of an exact still-queued original;
partial queued work and native-owned work remain charged.

The score is a deterministic rank, not a claimed receiver-completion bound.
It makes no assertion that historical service is future service, paths have
independent or additive bottlenecks, flow shares sum to carrier capacity, or
one command completes by `S_c`. Loss, confidence, active-flow count, carrier
family, and a `Suspect` label add no independent numeric penalty. Their valid
effect enters through typed carrier rate/timing evidence or structural
eligibility. In particular, physical `C_c` MUST NOT be divided by the number
of active Product flows: the shared ledger already represents their work.

For the same selected structural tier, let `b` be the best candidate. An
eligible incumbent `o` is retained while:

```text
S_o <= S_b + U_o + U_b.
```

A challenger replaces it only under the strict reverse inequality. This
deadband is a deterministic anti-flap rule, not a bound on prediction error or
a guarantee of optimal completion. An evidence expiry is an exact wake;
Section 10.2's pessimistic fallback ensures expiry alone cannot improve rate or
timing. Exact identity is the final tie break.

Once an original token is native-owned, carrier service accounting cannot by
itself distinguish two Product-release worlds: that original on output `i` may
have served, or a reinjected copy on another output may have caused the same
Data ACK. Wall projection may already have reduced that original's `Z_c` to
zero even though its exact native token remains. Core therefore retains an
exact nonnumeric ambiguous-release guard on that native-owned token rather
than inventing an output rate penalty or an independent assignment ordinal.
A queued token is different: its exact stage proves non-service and follows
Section 8.5's cancellation or unguarded-retention rule.

Every published OriginalData owner records its exact output incarnation,
current checked output-admission epoch, exact carrier-work token, and whether
same-copy service has been proved. Product publication and movement of the
token from provisional reservation to the queue are one transaction, so an
externally visible Product owner always names a queued or native-owned token.
An advancing same-writer service frontier marks every covered linked original
as same-copy-served before retiring the carrier token. A partial or complete
Data ACK that releases any part of an original still lacking that proof first
applies Section 8.5's complete-range queued cancellation when available. If
the token is native-owned, the transaction sets one idempotent guard bit on
that exact still-live token before Product release. A partly unacknowledged
queued command remains unguarded because its stage is exact proof of
non-service; ACK fragmentation or overlap cannot create another guard.

Each output maintains the count of guarded tokens bound to its current
output-admission epoch. It is guarded exactly when that count is nonzero. A
later exact service frontier clears the token's bit and decrements that count
once. A Product-published queued token may otherwise be cancelled only in the
same transition that makes its bound output-admission epoch non-admitting or
terminal, that terminalizes the Product itself, or the complete-range Data ACK
case in Section 8.5 before any guard exists. Cancellation cannot silently drop
a current guard while preserving fresh admission. Native-owned tokens remain
until exact service receipt or writer terminal as specified below.

The output-admission epoch is the qualification epoch defined below; there is
only one output-local lifecycle fence. A newly admitted attachment owns its
initial checked epoch. The `AdmissionActive` to `Revoked` transition allocates
the checked non-reused successor as non-admitting. Successful exact
requalification activates that already-advanced epoch with zero current guards
and `q_i = 0`; it does not allocate another identity and inherits no carrier
rate, Product qualification, or byte authority. Tokens, guards, Product owners,
and carrier debt from the predecessor remain bound to that predecessor epoch
until their exact service, cancellation, Product, or writer terminal rules
apply. A delayed predecessor ACK can therefore guard only its predecessor and
cannot relatch the successor. Exhaustion makes the exact output permanently
ineligible for a successor epoch without changing existing Product or carrier
work; attachment replacement is required. Exact output incarnation or
direction terminal revokes its current epoch. One active output epoch binds one
ordered-writer epoch in its send direction; independent writer frontiers are
never merged to manufacture proof.

Usage tier remains the outer structural order. Core first tries unguarded
Regular outputs by `S/U`, then guarded Regular outputs as fallback. It may
consider Backup only after no Regular output can complete the exact commit, and
then tries unguarded Backup before guarded Backup. A guarded output therefore
cannot beat an unguarded peer in the same usage tier, a sole guarded Regular
remains work-conserving, and an administratively Backup output never jumps an
admitting Regular merely because of the guard. The guard grants no bytes, does
not change `S_c`, and does not bypass `W/L_i`, receive credit, reorder
authority, writer admission, or peer `PATH_STATUS`.

Native handoff gives a token no MPP wall deadline. Receipt latency includes
ordered native debt, native flow and congestion control, peer scheduling, and
reverse receipt service; Core has no valid finite upper bound for that sum and
MUST NOT close a healthy slow writer by inventing one. Every native-owned token
and guard remains charged within the aggregate all-stage `N^B_c/N^I_c`
byte/item authority until its
exact cumulative peer-processing receipt or the exact writer/carrier terminal
rules in Section 8.5. At the cap, the physical carrier direction admits no new
positive carrier-work token; sibling QUIC writers share that result. Other
carrier directions and physical carriers remain independent, while a sole
carrier direction exposes honest bounded backpressure. Zero-coordinate
receipts retain their separate Control-queue authority.

Conditional progress requires native service, peer processing, same-fate
reverse service, and fair actor/writer arbitration. At zero service, under
persistent higher-priority overload, or while an authenticated peer retains a
live but nonprocessing writer, `Q^n` and an ambiguous-release guard may persist
without a finite reuse guarantee. This is bounded state, not permission to
refund, recycle, or pretend service. Provisional and queued work remains
separately bounded and follows exact handoff, cancellation, and terminal
ownership. The token guard remains the separate ambiguous-Product-release
mechanism.

Every ordinary positive quantum freezes a finite regular-before-backup
candidate order under the `S/U` rank and incumbent hysteresis above, tries exact outputs
until one real writer reservation and all Product authorities succeed, and
ends after that one commitment. It does not give every output an equal byte
allotment. Carrier evidence orders candidates and estimates relative service
pressure; it is not Product or native byte authority. Backup
is considered only after every frozen Regular candidate has failed the current
exact commit. A Backup uses the same exact `L_i` as any other output: `P_i` for
the first owner, live-frontier owner, or qualified additional output, and `E_i`
only for an unqualified additional output. Mere structural Regular membership cannot deadlock
the stream after every Regular writer or authority has failed, and one Backup
commit does not promote it ahead of a Regular on a successor quantum.

Each exact writer-admission resource owns a checked monotone local capacity
generation. Enqueue, dequeue, reservation acquisition or refund, close, policy
or class-limit change, and an applied native-ready event MUST advance it when
that transition can change the result of an exact positive reservation. A
capacity event not yet serialized is ordered after the current mutation; once
applied it cannot change reservation outcome without advancing the generation.
Generation exhaustion fails later advisory certificates closed and arms no
reused value. It atomically makes that exact writer-admission resource
non-admitting for new reservations and invalidates every captured certificate;
existing dequeue, refund, receipt, and terminal cleanup remain permitted but
cannot reopen admission. The resource then reaches exact writer terminal or is
replaced under its ordinary lifecycle with a fresh resource identity. This
generation is a race detector, not byte credit, path evidence, or a polling
counter.

A zero-commit Regular pass records the exact Regular membership, eligibility,
Product-authority, and writer-capacity generations that it exhausted. A Backup
certificate is valid only while all those generations remain unchanged. Backup
apply validates them immediately before and atomically with acquiring the exact
Backup writer reservation. An external advance before that linearization
restarts the Regular pass. The identified capacity-generation advance caused by
the Backup reservation itself is part of the same successful mutation and does
not invalidate its own certificate; an event serialized afterward belongs to
the successor quantum, which starts with Regular again. Unchanged structural
presence of a still-non-admitting Regular does not invalidate Backup.

A zero-commit attempt scan MUST drop every temporary reservation, arm the
relevant source, Product-authority, writer-capacity, topology, and terminal
wakes, recheck exact state, and then park. It cannot spin or treat an advisory
queue-ready sample as a reservation. Same-tier additions wait for a successor
attempt; removal or replacement is skipped by exact revalidation and cannot
retarget the pending quantum.

A positive ordinary commitment is itself durable successor work. While staged
source bytes remain and there exists one same exact candidate `i` for which
`W/L_i`, receive credit, reorder authority, and writer readiness are all
currently true, the direction MUST reconsider higher-priority work and then
attempt a successor quantum without waiting for an unrelated socket, timer,
ACK, or topology event. The existential predicate cannot combine authority
from one candidate with writer readiness from another. One actor turn may end
after a bounded number of commits for cooperative fairness, but it MUST publish
exactly one coalesced self-wake before yielding when that same predicate still
holds. A raced advisory predicate merely causes the successor finite scan to
arm exact wakes and park; it cannot self-wake again without positive work. Each
loop therefore
either commits positive bytes, eliminates one frozen candidate, or parks after
the wake/recheck protocol; it cannot become one-quantum-per-external-wake or
busy-poll an unchanged writer.

Queue reservation itself is the portable native-admission capability for that
exact writer. Together with the all-stage carrier-token reservation it
precharges the command before Product publication;
cancellation before native handoff refunds it and retires the same provisional
or queued work token. Dequeue transfers reservation ownership to the writer
and moves the token into native ownership; local writer completion releases
only the transient writer-command reservation, not the token's all-stage byte
or item reservation. The token remains in `Q^n_c` and charged to `Q^t_c/K^t_c`
until its exact
peer-processing receipt or exact carrier terminal. A QUIC logical-stream
writer reservation does not claim connection-wide native credit; QUIC
connection flow control and congestion control remain authoritative below it.
This separation is symmetric across request/response and TCP/QUIC.

Ordinary service-pressure ranking alone cannot promptly rediscover a carrier
that it does not feed: a still-slow world and a recovered world are
observationally identical. Sending unique Product bytes buys the information by
risking a Data Sequence hole. Core instead defines optional **carrier
observation**: encrypted synthetic payload carried and discarded on one exact
physical carrier direction. It creates no Product owner, source lease,
receive-map entry, Data ACK horizon, receive credit, qualification tag, or
Product delivery sample. Its loss, reordering, or terminal cleanup cannot open
a Product gap.

Observation scope is the tuple `(authenticated session generation,
original-sender direction, carrier incarnation, active native activation
fence, observation-channel epoch, generation_id)`. Carrier, activation, and
channel identities are equality fences, not locators, capacity evidence, or
rank. A locator-only QUIC migration that preserves the exact active
`PathData`/controller activation preserves scope; installation or restoration
of another activation does not. Each carrier direction has at most one live
semantic observation generation, while different carriers may run generations
concurrently. No Product stream owns a generation or contributes a separate
startup allowance merely by existing.

Carrier observation is eligible only while at least one ordinary pending
opportunity in that session direction has effective `Throughput` demand.
`Automatic` retains its existing latency-first classifier; an explicit
throughput hint may opt in immediately. Polling, Product flight, or observation
itself cannot create demand. Latency and realtime work never starts synthetic
observation, although their ordinary traffic may naturally refresh native
evidence. Before every synthetic head, the coordinator also requires no pending
effective `Realtime` or `Latency` work anywhere in that same session direction.
This direction-wide sensitive-pressure gate is required even when that work
uses another writer or carrier, because local writer priority cannot disprove a
shared Wi-Fi, ISP, relay, or egress bottleneck. It stops new optional heads but
cannot preempt already-MPP-queued or native-owned bounded observation debt and
therefore makes no zero-latency-interference claim. It also makes no assertion
about another session or external traffic; process-global coupling without
resource evidence would create unrelated-tenant starvation.

Observation starts only when rate is the causal blocker for an exact frozen
ordinary opportunity. Core reruns the ordinary candidate algorithm with every
fact unchanged except that the target carrier's `C_c` is replaced by the
largest representable positive rate. Usage tier, guard, Product authority,
credit, writer state, debt, propagation, uncertainty, identities, and incumbent
remain unchanged. A target is rate-causal only when this counterfactual reaches
its reservation step while the real evaluation does not. If the
counterfactual fails, additional rate cannot resolve the blocker; Core arms the
exact non-rate wake and sends no observation.

For the integer score in this profile, let `S_o` be the incumbent score in
timer ticks and define:

```text
H = S_o - U_o - U_c - T_c
N = 8 * (D_c(k) + M_c) * ticks_per_second
q(C) = ceil(N / C).
```

Strict replacement requires `q(C) < H`. In the unbounded positive-integer rate
domain a winning rate exists exactly when `H >= 2`, and the exact derived
threshold is `C_req = ceil(N / (H - 1))`. In a finite implementation it exists
only when the ordinary comparator also wins at the largest representable rate;
an unrepresentable `N`, threshold, or score is not silently saturated into a
win. Division by `H` is incorrect because completion rounds upward and
replacement is strict. Implementations MUST use the existing checked score
comparator, or monotone search over that comparator, as authority; the scalar is
explanatory and cannot become a second arithmetic path. The counterfactual
includes current `D_c`, not only the new command, and is revalidated after any
debt, incumbent, uncertainty, evidence, or identity change. If an ordinary
commit already feeds the target, synthetic observation is unnecessary even
when its prediction is low.

Observation arbitration has a Product-neutral finite cyclic cursor. One pass
freezes the exact carriers in the selected usage tier for which some current
ordinary opportunity is rate-causal, beginning after the last surviving
boundary. A target already receiving ordinary work, already owning a live
generation, lacking an observation channel, or failing any exact authority is
skipped and advances the cursor. Removal is skipped without rewind, additions
wait for a successor pass, and a tier or session-direction change invalidates
the pass. A successful first-head publication starts that target's generation
and advances the boundary; it does not end generations already active on other
carriers. One globally live generation would let a permanently slow carrier
monopolize rediscovery of every sibling and is forbidden.

A pass starts only after an exact demand, rate-evidence, budget/grant, writer-
capacity, topology, or terminal wake followed by immediate recheck. If its
finite vector produces no head, the coordinator arms the exact failed
prerequisite wakes, rechecks, and parks. An empty pass cannot self-schedule.
After one positive head, remaining eligible targets or continuation work own
exactly one coalesced successor wake. Each round freezes membership, attempts
every member at most once, and advances after success, block, or invalidation;
new members wait for the next round. Under positive authority and fair writer
opportunities, one continuously eligible carrier therefore receives a bounded
attempt opportunity independent of sibling order.

Observation DATA uses a separate lowest-priority MPP lane. Control, lifecycle,
carrier-observation grant/ACK, Data ACK, realtime, latency, due cause-bounded
recovery, ordinary throughput, and optional repair are reconsidered before
each head. At most one head is MPP-queued per exact carrier direction. No
observation wait owns a global scheduler, ACK-held, Product, or writer turn.
Heads already transferred to a native TCP socket or QUIC connection cannot be
preempted; the all-stage byte/item cap is therefore the honest bound on
same-carrier native debt attributable to observation. It is not a finite
wall-time claim at zero service.

An observation generation, carrier acquisition, or pending receipt grants no reservation
that excludes ordinary work. Immediately before each observation head's
admission linearization, the actor reruns ordinary selection for that exact
carrier; ordinary work that is then pending and can commit takes priority. A
positive ordinary commitment on the target stops synthetic admission because
it supplies the required native backlog. Ordinary commitment on another carrier
neither ends this target's generation nor consumes its opportunity. After an
observation head is published, however, its exact shared-cap reservation and
queued/native debt are real: later-arriving ordinary work may wait for that
bounded head's receipt or terminal, and Core claims no zero head-of-line delay
after native handoff. After a bounded cooperative turn, a pending independently
writable target is attempted or owns one exact self-wake before yield.
Persistent higher-priority work on the same native writer may starve
observation; unrelated-writer traffic may not erase its turn.

Observation mints no traffic allowance. The local sender owns one coordinator
keyed by `(local optional-policy epoch, authenticated remote principal,
original-sender direction d)` and shared by every session for that key. Let
`U_p,d` be cumulative uniquely Data-ACKed Product bytes across those sessions,
counted once, and let `X_p,d` be all sender-published optional reliable payload:
repair, stale requalification, carrier observation, and separately authorized
critical recovery debt. The policy epoch freezes one startup allowance
`F0_p,d` and optional fraction `b_p,d`:

```text
A_p,d = max(0, F0_p,d + floor(b_p,d * U_p,d) - X_p,d).
```

The check and charge are atomic across every carrier writer in that direction.
A stream, carrier, session, or reconnect does not contribute another startup
allowance. Published spend never refunds on ACK, loss, deadline, generation/
channel reset, carrier terminal, session close, or reconnect. Critical cause-
bounded recovery retains its separately defined temporary exception but is
still included in `X_p,d`, leaving ordinary optional authority zero until
unique Product progress repays it. The coordinator and startup-issued state
survive the last session close. A successor policy epoch may reset them only
after every old-epoch session/writer is fenced, and a different credential ID
mapping to the same authenticated principal cannot mint another allowance.
Native retransmission remains outside this MPP payload ledger. Session-local
subcounters MAY diagnose or escrow work but cannot independently fund it.

The receiver separately grants only explicitly identifiable carrier-
observation work. It cannot enforce a grant on repair alone because an original
`STREAM_DATA` and a reinjected copy that arrives first have the same wire form;
charging every such frame would create another Product-credit gate. For each
observation-channel direction `o`, the receiver publishes cumulative
`M_o = max_charged_work` and retains accepted cumulative `C_o`. The sender
separately retains its cumulative published charged work `W_o^sent`; it may
publish a DATA head with `N > 0` only if checked
`W_o^sent + N + 68 <= M_o`, and the infallible publication atomically advances
`W_o^sent` by `N + 68`. The receiver
independently applies the same checked addition to `C_o` before accepting that
head. Thus `C_o <= W_o^sent <= M_o` while the ordered channel is live, including
with several outstanding heads. `N + 32` is the exact normalized DATA frame and
36 bytes reserve its maximum one cumulative ACK. ACK coalescing does not refund
the conservative charge.

Channel maxima are allocated by one atomic session-direction and authenticated-
principal-direction coordinator. For principal authority `E_p`, irreversible
consumed work `C_p`, and live channel escrows, it preserves:

```text
C_p + sum_live_o(M_o - C_o) <= E_p.
```

The principal coordinator is keyed by one non-reused endpoint policy epoch,
stable authenticated principal identity, and original-sender direction. It
outlives every session and carrier in that epoch. Closing the final session
returns only unused channel/session escrow; it does not clear `C_p`, restore a
startup-issued flag, or create a new `E_p`. A new principal policy epoch may
reset that state only after serialized revocation/terminal has fenced every old
session and channel so no old frame can be accepted into the successor epoch.

The session coordinator preserves the analogous inequality inside one exact
authenticated session generation and direction. `E_p` may grow only from one
configured principal startup allowance per principal policy epoch, receiver-
observed unique Product progress counted once, or an explicit checked principal
policy allowance. Observation work cannot fund it. Session or channel creation
does not grow principal authority. Accepting a head atomically moves its charged
work from channel escrow to irreversible session/principal consumption;
terminal returns only unused escrow. Thus reconnecting or opening parallel
carriers cannot multiply startup work.

Starting one semantic generation freezes positive finite payload cap `G`, head
cap `J_max`, and absolute admission deadline `D_O`, each bounded by configured
observation resources and currently available local principal-direction authority. These are hard
resource limits, not targets, BDP estimates, pacing rates, or promises of
completion. Before every head, Core rechecks its positive payload against
remaining `G` and live `A_p,d`, its `N + 68` work against channel grant, and its
one token against shared all-stage carrier byte/item authority. It charges all
ledgers before infallible publication. A provisional failure changes none;
published work and payload never refund.

A semantic successor may start after predecessor terminal while old
observation tokens still await the same channel's cumulative processed-work
ACK, provided the shared all-stage cap admits the successor. Requiring zero old
tokens would turn cumulative service into stop-and-wait. Channel terminal, by
contrast, retires all its unresolved tokens without claiming service before a
fresh channel epoch can publish. Checked counter exhaustion fails new work
closed and never wraps or reuses an identifier.

After every head, receipt, authority-mode event, evidence expiry, ledger or
grant mutation, incumbent change, writer-capacity event, and ordinary attempt,
the coordinator recomputes the exact counterfactual from fresh state.

- `Run` requires continuing throughput demand, no direction-wide pending
  effective Realtime/Latency work, a live exact scope, positive finite
  authorities, no ordinary target feed, counterfactual target selection, and
  failure of real selection solely because of rate. In `NativeMode`, an early
  or low `B_op` re-enters this predicate; it cannot stop, pace, shrink, or
  refund excitation.
- `Pause` retains the generation, spend, grants, and queued/native tokens and
  does not clear carrier-scoped `Acq_c`, but admits no new synthetic head. It applies when evidence is
  sufficient but ordinary commit loses a Product/writer race, or when a
  temporary non-rate prerequisite or direction-wide sensitive-pressure gate
  prevents commitment. Exact typed wakes re-evaluate from the beginning.
  Evidence expiry can therefore resume a paused generation.
- `Successful` terminal requires an actual positive ordinary commitment to the
  exact carrier. Observation evidence alone neither qualifies Product nor
  proves that commitment; later unambiguous unique Data ACK retains Product
  authority.
- Other semantic terminal causes are disappearance of throughput demand or
  pending work, class/tier or policy ineligibility, exact session/carrier/path/
  channel replacement or terminal, explicit cancellation, completed payload
  cap, local or peer authority exhaustion, `J_max`, `D_O`, or checked counter
  exhaustion. Incumbent or score change alone is recomputation, not terminality.

Terminal stops new semantic admission. It neither clears carrier-scoped `Acq_c`
nor refunds published spend or erases queued/native channel tokens. A later
cumulative channel ACK may retire those tokens before or during a successor
generation while applying predecessor generation semantics as a no-op. Exact
channel terminal retires all remaining channel tokens without claiming service
and returns only unused receiver escrow; their acquisition tags contribute
nothing, but already folded carrier evidence remains.

Every valid authenticated observation ACK transaction first applies any
advancing channel processed-work frontier, then applies generation semantics if
the exact generation is still live. A nonadvancing channel frontier may still
publish a later semantic payload frontier only up to already service-certified
`K`. `NativeMode` receipts retire service and wake native evidence but publish
no receipt rate. In `ReceiptMode`, exact service retirement folds a matching
live acquisition tag once into every covered authority-retained suffix anchor's
`W_a` before observation generation semantics; `V` does not count the same
DATA again. A published active term
remains authoritative only for its exact session generation, direction,
carrier, native-path epoch, authority mode, and fixed freshness. Observation
channel or semantic-generation success, deadline, cap, cancellation, or
replacement does not retroactively erase already folded carrier evidence. No
event publishes a positive term without distinct receipted work.

One observation-channel direction owns a checked non-reused generation
sequence, one cumulative assigned normalized DATA-work coordinate, and a
bounded ordered token deque across semantic generations. A generation has
positive `generation_id`, contiguous payload offset, published payload `B`,
cumulatively service-certified payload `K`, cumulatively receipted payload `V`,
and semantically unreceipted payload `M`:

```text
0 <= V <= K <= B <= G
M = B - V.
```

The first published generation uses the channel's next identifier and offset
zero. A head with `N > 0` carries the current `B`; checked publication advances
`B` by `N`, the channel assigned-work frontier by `N + 32`, and the head count
by one. It creates one exact token interval in that work coordinate and charges
`X_d` plus receiver work before publication. Every counter is checked; gaps,
overlap, reuse, wrap, saturation, or a nonzero first offset fail closed.
Synthetic bytes need no retained Product source or payload identity.

For each direction, the receiver retains the current semantic generation and
retired generation high-water, cumulative charged work, cumulative processed
DATA work, and one dirty cumulative ACK owner. It validates the complete DATA
envelope, authenticated path/channel, length, checked offset/work arithmetic,
channel grant, and generation classification before mutation. It then
atomically charges `N + 68`, advances `processed_observation_work` by `N + 32`,
discards the payload without Product delivery, applies live generation state,
and marks the cumulative ACK dirty. The first exact successor at offset zero
retires predecessor semantics; delayed predecessor service remains represented
by the channel coordinate. A gap, overlap, reused/older generation, impossible
future generation, grant excess, or arithmetic failure is a protocol violation
and changes no state.

`CARRIER_OBSERVE_ACK` carries both the named generation's cumulative payload
frontier and the channel's cumulative processed DATA-work frontier. The sender
fully validates exact channel scope, allocated generation classification, and
`processed_observation_work <= assigned_observation_work` before mutation. The
service scope is `(session generation, original-sender direction, carrier
incarnation, observation-channel epoch)`; it deliberately excludes native-path
epoch and semantic generation, so a cumulative successor ACK can retire old
tokens after either changes while publishing no stale evidence.

Each token maps its channel work-interval end to its semantic scope and
generation payload end. The sender transaction first derives, without
mutation, every complete token interval covered by the proposed processed-work
frontier and a prospective `K_candidate`: the current live generation's `K`
raised to the greatest contiguous covered payload end in its exact semantic
scope. If the ACK names that live generation, complete validation additionally
requires `cumulative_payload_bytes <= K_candidate <= B`; the prospective
`V_candidate` is `max(V, cumulative_payload_bytes)`. If it names an
allocated retired generation, the payload field is a semantic no-op and no
historical `B` is retained merely to validate it. A future unallocated
generation is a protocol violation. Failure of any validation changes neither
service nor semantic state.

After complete validation, one indivisible mutation first advances the channel
processed-work frontier and retires exactly the covered tokens, then installs
`K_candidate` for the still-live matching semantic scope and installs
`V_candidate`. No scheduler can observe an
intermediate phase. Thus service can advance across semantic generations, but
no payload receipt can outrun the work that the same channel frontier proves
processed, and a malformed semantic frontier cannot partially retire service
or raise `K`. Generic carrier-work service coordinates MUST NOT account this
DATA again.

The ACK returns only through the opposite half of the same reliable observation
channel. Dirty receiver authority clears only when that exact reverse queue
accepts the cumulative ACK or the channel terminalizes. Constructing a frame,
a full-queue attempt, or a cross-carrier copy cannot clear it. ACKs occupy no
processed-work coordinate and elicit no ACK, so recursion is impossible. A
later frontier subsumes a lost or coalesced predecessor. Carrier observation
and stale requalification retain distinct frames, identifiers, state, and
lifecycle effects.

In `NativeMode`, observation is pipelined through ordinary native admission and
waits on native congestion control, pacing, flow control, and socket
backpressure; `C_c` ranks ordinary work but MUST NOT meter the observation
producer. Observation attempts to keep funded native backlog available. The
adapter's finite-recovery contract applies only during an interval in which its
declared sustained-backlog, positive-progress, and bounded-loss/blackhole
premises actually hold through `K_up`; one queued head, a finite grant, or mean
loss alone does not assert that premise. The contracted native controller, not
an MPP receipt formula, then updates `B_op`, and the adapter publishes that
exact state to every live consumer within `D_pub`. If achieved native service
remains below `C_req`, no observation model can prove unsent link headroom;
controller recovery is a separate obligation.

In `ReceiptMode`, observation semantic generations and carrier-rate acquisition
are deliberately independent. Opening, succeeding, succeeding-with-old-tokens,
or successfully stopping a synthetic generation neither creates nor clears
`Acq_c`. An otherwise-admitted positive Product token creates an absent
acquisition and its first suffix anchor when evidence capacity is available; an
observation token may do so only after its synthetic rate-causal admission
succeeds. Every later eligible token on any writer of the same exact carrier
direction receives that tag; after expired-anchor removal it creates a commit-
boundary anchor only when the set is empty or the frozen `q_acq` busy-time
spacing is reached. Only after the token's exact service coordinate proves full
peer processing does the sender add its normalized work once to every retained
anchor whose `f_anchor` strictly precedes that token's post-commit carrier-
ledger ordinal. Observation ACK
payload `V` remains semantic evidence for the synthetic generation but does not
add a second copy of the DATA work.

Service apply precedes the acquisition fold in the same transaction. A
partially covered token, ambiguous Product Data ACK, queued cancellation, or
terminal-without-service contributes nothing. A delayed tag from a closed or
wrong-scope acquisition is discarded after ordinary service retirement; it
cannot enter a successor. The existing all-stage carrier token cap bounds tags,
and `J_acq` bounds suffix state. Checked acquisition identifier, anchor work,
busy-duration, or item exhaustion fences acquisition without wrap,
publication, or refund. A transient tag-cap failure leaves ordinary Product
untagged and leaves an existing acquisition intact; an optional observation
head fails before commit.

At each exact receipt, expiry, ordinary-decision, and typed prerequisite wake,
Core recomputes every authority-live exact suffix and their current maximum
`r_acq(t)` from Section 10.2. It stores neither a latest detached rate nor an
unbounded historical maximum: latest-per-source reintroduces source eviction,
while `H_acq` prevents an old fast suffix from regaining authority forever.
Checked cross multiplication and round-down are mandatory; no live anchor,
zero work, overflow, or an unrepresentable result publishes nothing. Only the fresh
real-versus-`r_acq` ordinary comparator can activate the candidate. The chosen
opportunity may differ from the one present when `Acq_c` began; no Product flow
owns carrier acquisition.

The suffix set is a bounded liveness certificate, not a claim of arbitrary
change-detection speed. For one authority-live anchor `a`, let its closed and
current suffix busy intervals be disjoint `I_a,j`. Every token counted by `a`
has its entire commit-to-peer-process-to-local-receipt interval inside one
`I_a,j`. Let `F_a = 8 * W_a` be its counted bits and `D_a^+` its conservative
elapsed upper bound. Then

```text
F_a <= Y(union I_a,j)
duration(union I_a,j) <= D_a^+
r_a = floor(F_a / D_a^+).
```

Thus `r_a` is a conservative achieved busy-service lower bound. Taking the
maximum selects one actually achieved suffix; it does not add their work or
rates. This permits summing distinct token work inside one suffix before one
division and forbids summing detached rates.

For this one anchor, suppose uncounted or interleaved work is bounded by `A_a`
bits, all pre-service, actor, peer-processing, receipt, and conservative clock
overhead by `Delta_a` seconds, and tagged work receives common achieved service
`C>0`, so that at the receipt being evaluated:

```text
D_a^+ <= (F_a + A_a) / C + Delta_a.
```

If that anchor remains authority-live through the receipt, one current exact
rate-causal opportunity and all of its non-rate prerequisites remain stable,
and its comparator requires a positive integer rate `R` bit/s with `R<C`, then
`r_acq >= r_a >= R` whenever:

```text
(C - R) * F_a >= R * (A_a + C * Delta_a).
```

The inequality first proves the unfloored ratio `F_a / D_a^+ >= R`.
Because `R` is an integer in the same bit/s domain as the checked candidate,
rounding that ratio down still leaves `r_a >= R`. This typing is essential:
the implication is false for a non-integer equality target.

Here `C` is achieved service offered to tagged work after the stated bounded
interference, not latent link capacity or aggregate service under
fair-but-unbounded competing work. The claim also requires stable acquisition
scope, retention of this specific anchor, enough Product or observation
authority, bounded writer/peer-processing/receipt service, a receipt and fair
evaluation while `D_a^+ <= H_acq`, and an exact required rate that does not grow
to `C` as carrier debt changes. A slow pre-anchor prefix is excluded; actual
post-anchor interference increases `A_a`, and delay or clock uncertainty
increases `Delta_a`. Intermediate receipts and active expiry preserve this
anchor and its work. A path, mode, authority-horizon, or immutable quiescent-
freshness expiry instead removes its authority honestly.

The Core horizon has a precise conditional meaning. If
`A_a / C + Delta_a <= P_acq`, continuous tagged service persists, and the
receipt/evaluation event occurs no later than the boundary, the threshold work
for a positive integer required rate `R<C` is:

```text
F_req = ceil(R * (A_a + C * Delta_a) / (C - R))
D_at_req^+ <= (F_req + A_a + C * Delta_a) / C.
```

The ceiling is not silently erased: the exact receipt must still satisfy
`D_a^+ <= H_acq`. In the divisible-work fluid model, choosing
`F_a = 9 * (A_a + C * Delta_a)` gives:

```text
F_a / D_a^+ >= alpha * C = 0.9 * C
D_a^+ <= (A_a / C + Delta_a) / (1 - alpha)
       <= 10 * P_acq = H_acq.
```

Consequently, an implementation receipt within the inclusive horizon
qualifies every positive integer comparator requirement
`R <= floor(0.9 * C)` for which the exact checked cross-product inequality
holds. No claim is made that an arbitrarily coarse token or receipt must land
on the fluid boundary; its actual work and elapsed bounds decide authority.

With `J_acq >= 11`, `q_acq <= P_acq`; under continuous eligible commits, a
rate change therefore gets a post-change anchor within at most one additional
`P_acq` plus the next commit, and that anchor then owns its full `H_acq` proof
horizon. Under continuous target-local backlog, the atomic publication handoff
instead creates the successor anchor at the publication boundary itself; all
subsequent actor/writer delay is already included in `Delta_a`. Exact receipt
apply, suffix recomputation, and comparator publication are one transaction,
so there is no unaccounted post-receipt evaluator delay. In this renewal case
the successor has `P_acq=P_pub` and `H_acq=H_pub`, exactly matching the active
lifetime. That lifetime is therefore sufficient to renew an exact integer
requirement at or below `floor(90% * C)` without an intentional fallback when
its checked work inequality holds and receipt apply and evaluation occur by the
inclusive boundary. If
no successor is opened, a
receipt is applied later, or a coarse final token crosses the boundary, the
claim does not apply. Equality with the fluid `90%` boundary is covered only
when it is representable and the exact integer inequality and receipt fit that
inclusive boundary. This is the declared
operating envelope, not a universal confidence claim.

An acquisition-wide average does not have this property. After `60 s` at
`10 Mbit/s`, a change to `500 Mbit/s` still needs `87 s` before the cumulative
average crosses `300 Mbit/s`; a post-change suffix reaches the same threshold
after its own bounded work and receipt interval. Conversely, retaining a fast
suffix forever would mis-rank a path forever after a downshift. The finite
`H_acq` makes that old suffix ineligible after at most its authority horizon of
continued busy time, while the immutable quiescent deadline bounds wall-clock
reuse during idle. No receipt-only estimator can both integrate over an
unbounded BDP and forget every old service history within a finite bound. A
configured initial-rate prior or a qualified native-controller contract is
required outside the declared envelope. `[flow].initial_rate_mbps` supplies an
optional endpoint-local prior to every MPP path; omission means unknown, and
any explicit path `initial-rate-*` form overrides it, including
`initial-rate=unknown`. For TCP it remains an MPP scheduling prior and does not
alter native TCP congestion control. For a finite resolved QUIC rate `R` and
initial RTT `T` (configured `initial-srtt-s`, otherwise `333 ms`), the native
initial window target is `max(IW10, ceil(R*T/8))` bytes and the native initial
pacing target is `ceil(R/8)` bytes/s. Neither seeds BBR `bw`, `max_bw`, or MPP
operational-rate authority; unknown and unlimited retain the exact native BBR3
defaults. An overestimate therefore authorizes a larger initial burst and may
cause queueing or loss, while native congestion control and recovery remain
authoritative. Configuration MUST reject a resolved finite QUIC pair unless
`ceil(R/8) <= 2^53` bytes/s and `ceil(R*T/8) <= u64::MAX` bytes; it MUST NOT
silently round the native pacing target or saturate the window. This
QUIC-native exactness bound does not restrict a TCP-only prior. Before the
finite-target controller has satisfied the authenticated two-round
qualification above, its internally learned bandwidth remains native BBR
state but is projected as `Absent`, so central scheduling retains `C_0`.
Qualification changes only the MPP authority basis; it never restores the
configured window/pacer, seeds BBR bandwidth, or weakens native downshift and
recovery. Omitted and Unlimited configurations do not enter this classifier.

Repeated isolated flights also do not amortize feedback delay: for `m` equal
`f`-bit bursts with delay `tau`, both counted work and delay grow by `m`, leaving
`r_acq = C*f/(f+C*tau)`. High-BDP capacity from only small stop/start objects is
therefore unidentifiable without native evidence, continuous observation, or a
configured rate prior.

At `C = 500 Mbit/s`, `Delta = 100 ms`, and `A = 0`, approximately `125 KiB` can
prove more than `10 Mbit/s`, while proving `90%` of `C` needs about `53.6 MiB`.
This is an information bound, not a chosen probe size. A small object cannot
both finish immediately and reveal unsent high-BDP capacity without a
configured rate prior such as Hysteria2's target-rate model.

Every published Core Profile ReceiptMode active term owns an absolute expiry
`t + H_pub`, frozen from the one publication snapshot shared with its atomic
successor. The source acquisition's `H_src` only licenses the old candidate;
`H_pub` cannot revive an expired source. Later polling or transport shape
cannot extend either term. The exact deadline is one
serialized, externally indivisible transaction: settle `Z_c` through the
deadline under the old `R_A`; logically remove that term and restore `H_R`;
apply any same-boundary exact receipt whose conservative elapsed bound remains
within its inclusive anchor horizon; recompute `r_acq` and the exact ordinary
comparator against `H_R`; and optionally install a qualifying successor before
exposing state or waking scheduling. An equal achieved rate can therefore
renew after the old term logically expires, while expiry alone cannot improve
the carrier. The
sample is ReceiptMode-only, cannot survive scope/mode replacement, cannot be
summed across carriers or multiplied by flow count, and creates no discrete
promotion flag. Active expiry applies the exact `H_R` transition in Section
10.2 without closing a live `Acq_c`; a lower receipt candidate never causes an
earlier downshift.

The snapshot-derived `3 * P_acq` horizon freezes `E_acq` when `Acq_c`
first becomes quiescent. Later polling, a short new burst, another quiescent
transition, or transport-shape change cannot move or cancel that absolute
deadline. This bounds wall-clock reuse of a completed busy prefix; a sliding
deadline would let one tiny keepalive retain old fast evidence indefinitely.
Three feedback bounds are a Core bounded-freshness policy, not a consequence of
the `90%` recovery theorem. It retains the existing short-burst reuse policy
while bounding stale idle evidence and remains an empirical acceptance item,
not a symbolically optimal constant. No recovery across an idle gap is promised unless a
reopened suffix satisfies its inequality before this deadline. Under continued
busy time an old suffix plus a term it publishes can influence scheduling for
at most `H_src + H_pub`. If first quiescence intervenes, the conservative mixed
wall-time bound is time to that first quiescence plus
`3 * P_src + H_pub`, never a sliding renewal.

On TCP, observation DATA uses the existing authenticated physical writer and
MAX/ACK uses the opposite writer of the same socket. TCP terminal ends the
observation channel. On QUIC, the client opens exactly one long-lived
bidirectional HTTP/3 observation request stream per physical Quinn connection;
the first MPP frame in each half is `CARRIER_OBSERVE_MAX_WORK`, including zero
grant. DATA and its reverse ACK remain on that exact stream. The carrier-control
stream is forbidden because stream-wide priority cannot make bulk observation
low priority while retaining control priority.

The client sends the ordinary request field section before request DATA. The
server validates the first kind-44 frame and exact carrier binding and
atomically acquires the singleton observation-channel slot before accepting the
operation. It then sends a successful response field section before response
DATA and places its own first kind-44 frame there. A duplicate channel, policy
refusal, or bounded-resource refusal receives a non-success response or request-
stream reset and creates no channel state. That refusal is operation-local and
MUST NOT publish physical-carrier failure.

The QUIC adapter MUST expose observation below ordinary throughput at its
stream arbiter; a diagnostic-only priority setter is insufficient. ACK or grant
starvation is conservative and halts further optional work under the finite
token/grant cap. Request-stream-local FIN, reset, refusal, truncation, or error
on either observation half terminalizes the whole observation channel, clears
semantic state and dirty ACK, retires its tokens without service, and returns
unused escrow. It does not retire the Quinn connection, physical carrier,
control stream, or Product. A QUIC-connection or HTTP/3 connection error retains
its ordinary physical-carrier terminal semantics. The client may open a
replacement only after exact old-channel terminal, with a fresh non-reused
channel epoch and zeroed wire coordinates.

Carrier observation proves only achieved service on one direction. It cannot
prove unused headroom, future capacity, path independence, or that TCP and QUIC
avoid a shared bottleneck. Several carrier terms are never summed into aggregate
capacity. Ordinary placement remains subject to exact usage, hysteresis,
Product, guard, credit, debt, and writer revalidation.

Connection-wide source staging precedes DSN assignment and may contain bounded
work for several independently admitted outputs. It is therefore governed by
the shared stream, repair, reorder, and configured resource envelopes, not by
one selected output's native congestion window. For reliable work its active
allowance is the bounded sum of the configured per-output Product envelopes
`P_i` for the exact live outputs eligible for original data, capped by shared
`W`; with one output it is exactly that output's envelope. Traffic class may
keep source reads and each atomic service turn small, but it does not replace
this byte authority with a smaller Product window. Selection and allowance use
one coherent
view: withdrawn or unschedulable outputs contribute nothing, non-stale outputs
precede stale recovery fallbacks, and a backup contributes only when no regular
output is eligible. Staging grants no output ownership or carrier reservation;
every assignment still revalidates `L_i` and reserves the exact native writer
as described above.

An authenticated, admission-active attachment can precede its first fresh
qualified carrier-service epoch. Its carrier uses the configured startup
prediction until Section 10.2 accepts stronger evidence; Product assignment nevertheless
uses exact `L_i`: `P_i` for a first/live-frontier or volume-qualified output,
and `E_i` only for an unqualified additional output. A
`PATH_CAPACITY_*` diagnostic result does not change either authority. Absence
of achieved-service evidence is not absence of an output. Source
admission is zero when no live eligible output contributes a positive Product
envelope; an exact zero `P_i` fails closed. Evidence from another carrier
incarnation with the same path key MUST NOT be substituted for the unmeasured
attachment.

Fresh Product acquisition is durable exact-volume qualification, not a rate or
path-capacity measurement. Each exact attachment direction has an
always-present, checked, non-reused output-admission epoch, which is also the
qualification and ambiguous-release-guard fence, and one of three local
authorities: `AdmissionActive`, `Revoked`, or permanently `Exhausted`. Counter
exhaustion MUST clear evidence and fail permanently closed; it MUST NOT wrap.
Each admission-active, non-stale exact direction also has a qualification bit
`q_i`. `q_i = 0` is unqualified and `q_i = 1` is qualified; `Stale` and
`Requalifying` remain separate lifecycle states governed by Section 15.2. The
first owner while `O = 0` and the live contiguous-frontier owner retain
`L_i = P_i` for either value of `q_i`. A qualified additional output also uses
`P_i`; only an unqualified additional output uses `E_i`.

The admission-active to revoked edge clears the generation and advances the
output-admission epoch even when no generation has started. Repeated revocation
inside the same inactive interval is idempotent. Exact requalification receipt
changes `Revoked` back to `AdmissionActive` under that already-advanced epoch,
without evidence and without starting a generation. Thus an initially active
attachment with no generation and the same attachment after a stale and
requalification cycle never have the same qualification identity. A new exact
attachment incarnation may begin its local epoch sequence again because the
attachment identity itself has changed.

For each unqualified exact direction, one Product qualification generation
begins atomically under the current output-admission epoch when the serialized
stream owner successfully commits its first current-generation OriginalData
quantum to that attachment. The same rule starts the successor generation only
after exact requalification has restored `AdmissionActive`. The commit first
revalidates all ordinary assignment and writer authorities, reserves the exact
writer, and then freezes both a strictly positive generation volume floor
`F_i > 0` and the strictly positive maximum atomic Product quantum
`N_i^max > 0`.
It records exact Product ownership and qualification metadata before publishing
the command. No fallible operation may remain between that metadata mutation
and the already-reserved command's publication. The generation creates no
rate, credit, recovery, pacing, or native-transport authority.

Let `V_i` be exact uniquely Data-ACKed qualification-tag volume. Let `T_i` be
the normalized set of nonempty disjoint outstanding qualification-tag ranges,
and let `M_i = measure(T_i)`. `M_i` is metadata on a subset of `O_i`, not
another byte owner. Define qualification-only accounting:

```text
B_i^Q = V_i + M_i
d_i^Q = F_i - B_i^Q
x_i^Q = min(N, d_i^Q)
```

where `N` is the complete OriginalData quantum already admitted by `W/L_i`.
Before mutating the generation, the implementation MUST prove
`0 < N <= N_i^max`,
that the range is fresh non-reinjected OriginalData for this exact owner, and
that it does not overlap another current tag. The commit tags only the
deterministic `x_i^Q`-byte prefix of that range and retains an opaque receipt
containing the output-admission epoch and exact tagged prefix with the OriginalData
Product flight. A
first/live-frontier commit may carry `N > d_i^Q` under its ordinary `P_i`
authority; its surplus is useful Product but not qualification evidence. An
unqualified-additional acquisition commit requires `d_i^Q > 0`, must still fit
`E_i` exactly, and may carry one final atomic quantum with `N > d_i^Q`; only
the remaining `d_i^Q` bytes are tagged. This is a bounded evidence-floor rounding of
less than one maximum quantum, not a `W`, `P_i`, or `E_i` overshoot.

Only unambiguous MPP Data ACK coverage of uniquely attributable current-
generation OriginalData may move exact tag weight from `M_i` into `V_i`.
Predecessor-generation, reinjected, duplicated, native-ACKed,
capacity-receipted, terminally discarded, or otherwise ambiguous portions do
not advance `V_i`. Accepted recovery, ambiguity, or terminal cleanup removes
only the overlapping tag from `M_i` without increasing `V_i`; the underlying
Product debt follows its independent ACK and recovery rules.

Every release is clipped to both the receipt's tagged range and the exact event
range, checks that the receipt epoch is the currently active epoch, and removes
only coverage still present in `T_i`. Consequently copied receipts, split
flight records, duplicate Data ACKs, repeated ambiguity reports, and delayed
predecessor releases are idempotent. Normalized nonempty integer ranges also
give the explicit state bound:

```text
items(T_i) <= measure(T_i) = M_i <= F_i.
```

`F_i` MUST be representable by the implementation's collection index type.
This is a bound derived from the existing qualification envelope, not another
traffic or tuning parameter.

When `V_i = F_i`, the sender sets `q_i = 1` even if the same Data ACK
transaction has invalid, zero-duration, source-limited, or otherwise unusable
timing. One Data ACK transaction may produce at most one numeric rate sample
per exact output, but numeric sample validity cannot undo exact volume progress
or split one transaction into several qualification events. Short or sparse
work MAY finish while an output remains unqualified.

Qualification is durable for the exact active attachment incarnation and
output-admission epoch. Time and rate expiry cannot make the historical
exact-volume fact false, and `q_i`
grants only configured `P_i` behind all ordinary Product and native authorities.
Role, usage, carrier rank, application-limited state, and native-window
changes therefore preserve `q_i`, `V_i`, and `M_i`. Entering `Stale` or
`Requalifying`, exact detach, or incarnation replacement atomically revokes the
active authority, advances its epoch exactly once for that inactive interval,
and sets `q_i=0`, `V_i=0`, and `M_i=0` without deleting unresolved `O/O_i`.
Exact terminal Product cleanup also revokes that state, but releases
Product debt only through its independent terminal ownership rule. OriginalData
sent through a `Stale` sole-survivor fallback MUST NOT begin acquisition or
clear stale state. `Stale` to `Requalifying` and duplicate cleanup do not
advance the output-admission epoch again. After exact `STREAM_REQUALIFY_ACK`, the
authority is active but empty; only a later current-incarnation OriginalData
commit may start the successor generation. No predecessor evidence is
inherited.

The qualification invariant is:

```text
0 <= V_i + M_i <= F_i.
```

It is inductive across every placement role. A commit adds at most `d_i^Q` tag
weight; an exact ACK moves the same weight from `M_i` to `V_i`; ambiguity,
recovery, and terminal cleanup only reduce `M_i`; qualification and unrelated
role or rate transitions add nothing. Capping the tag rather than every Product
commit is necessary: otherwise a first/frontier owner may tag `P_i >> F_i` and
a later role change falsifies the claimed bound. This evidence invariant grants
no Product or native authority; every complete quantum still passes the
independent `O/W/P_i/E_i`, credit, reorder, reservation, and writer checks.

Accepted reinjection and Data ACK application for one stream direction MUST
share a serialized order. Qualification and exact flight metadata are recorded
before a duplicate or original command is published. Both transitions consume
only opaque receipts found on the exact overlapping OriginalData flight; they
MUST NOT broadcast a raw range under the current epoch or synthesize a receipt
from current ledger state. The receipt binds exact output, output-admission epoch,
and tagged range. If reinjection is recorded first, it removes overlapping
`T_i` through those receipts before a later ACK can verify it.
If exact ACK application occurs first, it may move the still-unique tag into
`V_i`; a duplicate published later cannot have caused that earlier ACK and does
not retroactively revoke verified history. An ACK received earlier but applied
after reinjection is conservatively ambiguous. Native carrier concurrency does
not relax this Product-level serialization.

Qualification owns no separate arbitration cursor, ACK-held turn, or
mandatory preemption. When ordinary `S/U` order reaches an unqualified additional
output, the same useful Product commit is an acquisition commit: its exact
assignment authority is `L_i = E_i`, and it may start or advance the
qualification generation above. Short or sparse work may end before any
additional output qualifies.

For an absent generation, the first successful ordinary commit atomically
freezes `F_i` and `N_i^max` and initially has `d_i^Q = F_i`. Later ordinary
commits may advance its remaining tag deficit while `d_i^Q > 0`. A currently
blocked, ineligible, failed, or Product-exhausted advisory candidate is skipped
for the current frozen candidate order without blocking an independently usable
sibling. Regular membership remains structural, but only a Regular that passes
the current exact Product and writer checks is usable for this quantum. After a
finite zero-commit Regular pass, Backup retains the same role-sensitive `L_i`
(`P_i` for first/live-frontier or qualified additional, `E_i` only for
unqualified additional); every Backup remains below every Regular again on the
successor quantum. Stale sole-survivor behavior remains the separate Section
15.2 rule.

Planning remains advisory. Each ordinary attempt binds the pending exact range
and positive `N`, selected tier, exact attachment and path epochs, qualification
epoch and deficit, role, and whole-quantum legality. Exact candidate identities
MUST be unique. Apply obtains the real writer reservation and independently
revalidates that certificate plus `W/L_i`, receive credit, reorder, and current
writer authority. For a Backup certificate, an external change in any recorded
Regular membership, eligibility, Product-authority, or writer-capacity
generation before the atomic target-reservation linearization invalidates the
attempt and restarts Regular. The target reservation's own identified
generation advance does not; unchanged structural Regular presence does not.
A non-target sibling otherwise cannot grant or revoke target authority.
A failed reservation or revalidation changes no Product state. A successful
apply records `F_i/N_i^max/T_i` when applicable, `O/O_i`, and exact Product-flight state
before publishing the infallible reserved command. No qualification or planning
token grants bytes, ACK, rate, recovery, or native authority, and no await or
second reservation may remain after Product mutation.

During any stable interval in which output `i` remains an unqualified additional
output and no Data ACK or terminal transition releases its debt, newly assigned
OriginalData is bounded by:

```text
new_i <= max(0, E_i - O_i_start)
```

and independently by remaining shared `W`, receive credit, reorder authority,
and successful writer reservations. There is no quantum overshoot. Qualification
tag rounding remains bounded by the already-proved `V_i + M_i <= F_i`
invariant and does not enlarge Product exposure.

An unqualified output that ordinary `S/U` order never selects may remain unqualified
indefinitely. This is intentional: forcing unique low-sequence work onto it
would exchange discovery for Product head-of-line risk. The observation plane
may instead improve its carrier evidence without Product ownership; if that
evidence becomes competitive, a later ordinary commitment can begin or
advance qualification. Observation itself can never set `q_i`.

After `q_i` becomes one, the output immediately has `P_i` assignment authority
but retains whichever typed rate state Section 10.2 actually proved. Its raw
qualifying point is not mature rate authority, and Core creates no discrete
post-qualification promotion. A low NativeMode `B_op`, or low, expired, or
absent ReceiptMode rate evidence, may move a carrier later in ordinary `S/U`
order but does not remove it from the structural tier. Conditional carrier
observation, rather than mandatory unique traffic, is the mechanism that can
replace suppressed rate evidence.

More precisely, suppose an exact carrier, direction, authority mode, selected
tier, and NativeMode activation fence (or ReceiptMode native-path scope) remain
stable; effective throughput demand, positive local and peer
authority, bounded actor/writer/peer/ACK service, and native progress persist;
the exact required rate remains `R<C`; and Product and writer admission
eventually permit the ordinary quantum. In `NativeMode`, the required rate must
be positive and representable inside the declared adapter envelope, funding
must cover its documented observation work `W_up`, and the adapter's sustained-
backlog, positive-progress, and bounded-loss/blackhole premises must hold. The
controller then reaches `B_op >= R` within `K_up`, the adapter publishes it to
every live scheduler within `D_pub`, and the next fairly serviced exact
ordinary decision observes and successfully revalidates that authority. In
`ReceiptMode`, one specific
post-change suffix anchor must remain authority-live while its exact service
curve and sufficient `F_a` satisfy
`(C-R)F_a >= R(A_a+C*Delta_a)` and `D_a^+ <= H_acq`; the frozen spacing and
horizon give the Section 15.1 operating-envelope bound rather than an
acquisition-wide average. Either event wakes
the exact ordinary decision; only a later positive Product commit and its
unambiguous Data ACK can qualify the output.

Those premises are necessary, not hidden tuning. For every finite observation
budget there are two paths with the same transcript through that budget that
diverge on the next byte. If the native controller never delivers above the
required rate, funded volume is insufficient, reverse receipts fail, service is
zero, scope changes indefinitely, or other optional work consumes all local
authority, finite rediscovery is impossible. A small receipt can prove
reachability without proving high-BDP service. Forcing unique Product instead
would risk head-of-line delay. Core states this information-performance
boundary rather than hiding it behind a scan or threshold tweak.

The invariant debt bounds are:

```text
sum(O_i over all exact original owners) = O
O <= min(W, configured reorder authority)
O_i <= P_i
```

`E_i` is a prospective commit ceiling, not a retroactive debt invariant. Every
new commit made while the output is currently unqualified and additional MUST
leave `O_i <= E_i` under the then-current exact observation. A previously
qualified or first/frontier output may legally retain `O_i > E_i` after
qualification revocation at a lifecycle boundary or after a first/frontier
role transition, and a later configured Product-envelope change may shrink `E_i` below
already committed debt. Those transitions MUST preserve exact Product
ownership; they admit no new OriginalData on an unqualified additional output
until Data ACK or terminal cleanup restores current `E_i` headroom. Rate expiry
alone does not revoke qualification or change this assignment authority. All
retained debt remains bounded by `P_i` and shared `W`/reorder authority.

These debt bounds are also inductive. Only an OriginalData commit increases
`O` or one `O_i`, and it first proves the corresponding `W` and `L_i <= P_i`
inequalities. Data ACK and terminal Product cleanup only decrease them;
evidence, role, configured-envelope, and lifecycle transitions do not reassign or
duplicate retained original ownership. Reinjection adds no member to `O`.

These are the configured aggregate useful-Product/reordering bounds, not a new
congestion or pacing window. A stricter hidden aggregate Product-owner cap has
no unique derivation from these Product authorities and would serialize
healthy cold-path discovery. This does not remove the independent finite
all-stage carrier-token resource authority in Section 15.1. A blackholed unknown output can still create application head-of-line
delay; that cost is unavoidable when ordered useful Product explores an
unknown path. Each unqualified-additional commit is bounded by its then-current
`E_i`, while any debt inherited from earlier `P_i` authority remains bounded by
`P_i`; the existing Product recovery deadlines bound both cases without
logically blocking another output that still has shared headroom. No capacity
receipt or observation-generation deadline owns Product liveness.

Ordinary acquisition adds no synthetic traffic. Optional observation does, but
charges every payload byte and never owns Product order. Its direct Product
head-of-line cost is therefore zero. Its resource cost is not zero: one atomic
head already accepted by a TCP FIFO cannot be preempted, a completed generation
may have transferred as many as `G` bytes, and traffic on nominally separate
carriers may share an unknown bottleneck. The distinct lowest-priority queue and
per-head arbitration prevent an unadmitted train from blocking later ordinary
or control work; they cannot promise zero network delay or a finite drain time
when service is zero. This is the unavoidable information-versus-interference
tradeoff, bounded in payload bytes by the cumulative ledger and generation cap.

TCP pool establishment is owned by Section 7.2 and is independent of
instantaneous Product demand. A ready pool member enters the regular or backup
placement set defined by Sections 7.2 and 7.3. It receives no fixed share and
no special startup rate: acquisition is bounded by the existing unproven-path
flight, shared credit, queue, repair, and reorder rules above.

The scheduler evaluates every exact carrier direction from the shared carrier
ledger and current carrier service evidence. Qualified Product volume changes
an additional output from bounded `E_i` acquisition to configured `P_i`
assignment; it does not itself change `S_c`, the output guard, or `P_i`. Only
separately valid typed carrier rate/timing evidence or exact ledger work may
change `S/U`; usage selects the structural tier. Other sampled queue, flight,
loss, ECN, confidence, health, active-flow count, or native state cannot add an
independent score term. Exhausted Product/resource headroom, failed exact writer
reservation or native backpressure, structural ineligibility,
or lack of residual backlog may leave an output without new Product work.
Inferior or expired carrier evidence may suppress ordinary placement but
cannot remove structural membership; only eligible throughput observation may
then add bounded duplicate load. Carrier presence alone is therefore not
payload allocation or active flow load.

While the ordinary incumbent remains admitted, both request and response
placement apply the same `S/U` inequality above. The pending exact command
appears once as `M_c`; committed predecessor work appears once in `D_c`.
Counting either again can pin an incumbent by its own serialization delay.
Shared carrier-ledger generation revalidation exposes a concurrently accepted
quantum before another commit. This transport-neutral hysteresis reduces
ownership flapping without turning ownership into a fixed path preference.

The Core does not infer a common bottleneck or condition carrier membership or
directional authority on transient comparative throughput samples. Such a
comparison cannot reliably mature a new kernel TCP flow across the full
supported bandwidth and RTT range, and it makes the physical pool depend on
one transient traffic direction. The configured maximum is the explicit,
bounded connection policy; native TCP congestion control and ordinary carrier
service-pressure ordering remain the traffic policy.

No Mbps value, utilization percentage, source address, locator, interface
identity, application flow count, laboratory condition, or fixed observation
window may create, promote, or revoke a TCP pool member. Exact native failure
changes liveness immediately; planned configuration and maintenance changes
use the gradual lifecycle in Section 7.2.
### 15.2 Reinjection budget and timing

Ordinary optional reliable payload is limited by cumulative extra-traffic
credit funded by a bounded startup allowance and unique bytes acknowledged by
MPP Data ACK.
The Product default is 10 percent. `[flow].optional_reinjection_budget_percent`
sets the local sender default and an MPP inbound/outbound performance value may
override it for that node. The value is directional and peers do not negotiate
it. It meters optional repair reinjection, active observation, and stale-path
requalification payload. It does not meter native transport retransmission,
MPP control headers or receipts, or the cause-bounded critical recovery
authority defined below.

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
authorized no earlier than `loss_at` only when, at serialized evaluation time
`now`, `now + S_c(k,M_c) < fallback_at` for the exact frontier command and
captured shared carrier-ledger generation. If the alternate cannot win that
absolute comparison, the
retained gap waits until `fallback_at`. At or after `fallback_at`, an eligible
distinct alternate may perform bounded repair without a completion
gain; expiration is liveness authority, not evidence that the alternate is
faster or that native recovery failed.

The repair uses exact target `t`'s current published Product envelope `P_t`,
already bounded by shared `W` and the configured repair and path-flight
envelopes. Recovery MUST NOT reconstruct `P_t` from an unscoped carrier rate or
from a native-window observation of an older epoch. Let `O_t` be exact
un-DataACKed OriginalData on that target and let `A_t^r` be one
repair-admission quantum. This symbol is distinct from the unqualified
OriginalData Product envelope `E_i` in Section 15.1.
The target's Product repair capacity is:

```text
repair_cap_t = max(saturating_sub(P_t, O_t), A_t^r)
R_t = B_t + U_s + J_t
K_t = repair_cap_t - R_t                 (saturating at zero)
```

`B_t` is queued ReinjectedData bound to exact target `t`; `U_s` is
target-unbound queued ReinjectedData in the current stream and direction; and
`J_t` is every un-DataACKed ReinjectedData byte already accepted by exact
target `t`. Repair bound to another target is excluded. Raw OriginalData
staging, control work, other streams, aggregate path-health Product flight,
and sampled native queue or packet flight are excluded from `R_t`. Global
retained repair debt and configured resource ceilings then cap `K_t`.

When ordinary target headroom is full, `A_t^r` is one single outstanding
emergency reserve for the exact directional recovery target. It is not renewed
per range, timer expiry, evaluation, actor wake, or native queue drain.
Target-unbound repair conservatively consumes every eligible target's reserve
until assignment; after assignment it consumes only its exact target's
reserve. The actual bounded writer-command reservation is the native admission
boundary. After obtaining that reservation, the serialized Product actor MUST
revalidate `K_t` while excluding the current front intent from `B_t`/`U_s`,
record the accepted exact copy in `J_t`, and only then commit the writer
reservation. Failed revalidation drops the reservation without recording the
copy.
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
the exact unique original flight's assignment epoch. For the same exact gap
and assignment, later owner observations may move `loss_at` or `fallback_at`
earlier but MUST NOT restart either clock later. Alternate eligibility and
service-pressure projection are evaluated from the current target; a departed
target's early projection MUST NOT be inherited by its replacement. Without exact
ownership or a live measured distinct alternate no target-bound repair is
sent; when an eligible alternate cannot win early, ACK silence waits until the
one-interval `fallback_at` rather than erasing that fallback.

Recovery target ranking and commitment MUST refer to the same lowest-missing
frontier quantum. The first committed repair frame on the selected target has
the exact offset, payload extent, normalized `M_c`, and shared carrier-ledger
generation whose `S_c` rank authorized that target. If that frame cannot be
committed because it overlaps queued or recent
repair work, the evaluation MUST stop without publishing later omitted ranges.
After the frontier quantum is committed, the sender may fill the remainder of
the same bounded target service window behind it. A larger coalesced batch or
whole-window throughput estimate MUST NOT replace the exact frontier-quantum
carrier rank as the primary target objective.

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
current carrier service evidence and the ordinary `S/U` rank favors the
alternate outside its uncertainty deadband. Partial Data ACK progress shrinks
the retained range but does not rewrite any remaining original Product flight's
assignment time.
This finite-tail rule does not mark the original attachment stale, withdraw it
from ordinary placement, or replace native recovery. Exact range identity,
repeat-delay suppression, shared credit, queue, flight, repair, reorder, and
extra-traffic bounds continue to apply.

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
write completion, carrier-service receipt, or another timer MUST NOT release
`J_t`; TCP and QUIC already
recover the accepted bytes on that live reliable carrier. A replacement
incarnation does not inherit the old target's `J_t`. The structural alternative
predicate gates a new Product recovery commitment; it is not the lifetime of a
copy already committed. If no other exact target is eligible when `D` expires,
MPP waits for Data ACK, a target-set change, or an exact terminal event instead
of duplicating native recovery on the same survivor. The successful commitment
directly publishes `D` into the actor's
durable one-shot wake, even if `D` becomes due before the next ledger
observation. That next exact-ledger observation reconciles or cancels a future
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

The reinjected command owns a separate carrier-work token. Its cumulative
service receipt may retire that token without releasing `J_t`; the MPP Data ACK
may release `J_t` and Product flight without directly retiring carrier `Q/Z`.

Lifecycle and Product qualification are independent state axes. The
directional attachment lifecycle is `Active`, `Stale`, `Requalifying`, or
`Detached`; an active exact incarnation separately owns the `q_i` and
qualification generation defined in Section 15.1. Stale entry revokes that
Product qualification generation and makes `q_i = 0`. Entry into
`Requalifying` or `Detached` has the same output-local clearing rule. It does
not terminalize the Product-neutral carrier-observation generation, erase a
still-fresh carrier-scoped ReceiptMode active term or acquisition, or retire
native-owned carrier work; those retain their own carrier/path/mode and
service-frontier lifecycle. Neither a later requalification nor an otherwise equal path
identifier inherits output-local Product evidence.
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
copy identity, budget or critical authority, all-stage token headroom, and the
exact writer reservation all revalidate and that reservation succeeds.
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
retained bytes, permanently unavailable writer or all-stage token authority,
exhausted identifiers, or continuously changing membership.

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
probe ID, offset, payload length, origin writer epoch, and cumulative processed
frontier. For one session and stream direction, define exact probe identity
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
only the exact pending `T(P)`, applies the cumulative carrier-service frontier,
and then applies the dedicated requalification effect. A different session,
unattached return carrier, mismatched field, reused ID, stale or absent target,
`PATH_PROOF`, generic `STREAM_ACK`, or `CARRIER_OBSERVE_ACK` does not change
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

Loss of the probe leaves Product ownership unchanged. Its native-owned
carrier-work token remains until a later cumulative frontier or exact writer
terminal. At successful publication the transaction freezes the absolute
deadline `D = published_at + stale-attachment recovery interval` from that
exact attachment snapshot. The sender MUST compute `D` by checked addition
before publication; failure refunds every provisional reservation, publishes
no probe, advances the finite pass, and never saturates or wraps. Exhaustion of
the monotonic deadline domain for every candidate disables new
requalification deadlines for that exact session direction. Later metric, role, or policy
changes do not move `D`. At `D`, expiry cancels and refunds a still-removable provisional or queued
probe token, but retains a native-owned token until cumulative service or exact
writer terminal. It retires the pending proof identity, returns the target to
`Stale`, publishes the next cursor wake, and cannot leak or double-retire work.
A late ACK still applies any valid cumulative service frontier first, while its
expired requalification effect is a stale no-op. The next selected attempt
uses a fresh probe ID. Probe bytes consume the existing optional extra-traffic
budget and remain charged. Budget exhaustion MUST NOT permanently prevent
re-entry: one minimum useful recovery quantum may be sent per exact stale
interval as critical recovery debt, still subject to the single-pending,
retained-range, queue, pacing, and flight bounds. Thus one stream direction
can add at most one recovery quantum instantaneously and, under persistent
probe loss, at most one quantum per stale interval over time, excluding frame
headers. Later optional reinjection authority remains reduced by that debt.

The placement-persistence clock is independent for every exact attachment
incarnation that owns current-epoch, evidence-eligible OriginalData omitted
below a complete authoritative Data ACK horizon. Positive ACK ranges are
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

Only a Data ACK transaction that newly acknowledges unique current-epoch,
evidence-eligible OriginalData bytes unambiguously attributable to that exact
owner may replace its deadline. The attachment carrying the `STREAM_ACK` is
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
19. Each TCP carrier group reconciles toward its configured healthy maximum
    without exceeding it; the reserved first range value changes no behavior.
20. Every authenticated ready TCP pool member has bidirectional Product
    authority subject to directional `AVAILABLE`/`BACKUP` usage. Usage follows
    configured endpoint topology and never a throughput comparison.
21. Carrier presence never forces payload placement or duplication. The
    ordinary scheduler revalidates exact carrier health, usage, Product
    authority, shared carrier-ledger generation, writer reservation, credit,
    output guard/epoch, and carrier service prediction before every commit.
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
34. The `PATH_CAPACITY_*` measurement result is diagnostic in Core Profile 7.
    It creates no Product, carrier-lifecycle, usage, health,
    congestion-control, or capacity authority and cannot gate unrelated
    carrier work. Its positive data command still owns and retires the same
    generic carrier-work token as every service-bearing command.
35. Each positive reliable service-bearing command owns exactly one token in
    exactly one provisional, queued, or native-owned stage. Transfers are
    atomic, with no gap or duplication, and preserve
    `0 <= Z_c <= Q^n_c <= Q^t_c <= N^B_c` and
    `0 <= K^n_c <= K^t_c <= N^I_c`.
36. A carrier-service frontier is cumulative, exact-writer scoped, and no
    greater than the assigned frontier. It retires only complete covered
    carrier-work tokens. Product ACK never retires `Qn`, settles `Z`, or claims
    service; its sole `Qq/Qt` edit is the complete-range queued cancellation in
    Section 8.5. A service receipt never releases Product, credit, `q_i`, or
    native authority.
37. Applying causal service entries, publishing exact token guards, and
    releasing Product ownership have one linearization order before any new
    scheduling decision.
38. Output/stream terminal, ordered-writer terminal, native activation change,
    and physical-carrier terminal clear only their exact scopes and imply
    neither Product delivery nor peer service. Every actual `PathData`/
    controller installation or restoration is fenced at switch time, including
    same-identity cloning and complete install-and-rollback between polls. It
    replaces only activation-owned state from one coherent new-source snapshot
    in NativeMode, or clears path-scoped rate/timing evidence under the existing
    ReceiptMode rules, while preserving same-connection writer tokens and
    `Q/Z`; a writer terminal cancels its removable `Qp/Qq`, retires its `Qn`,
    and makes its bound output directions non-admitting; a physical-carrier
    terminal clears all writer scopes on that carrier.
39. All QUIC ordered-writer epochs on one physical connection direction share
    one aggregate carrier `Q/Z/C` clock. No logical stream, flow, or writer may
    copy, multiply, or divide that capacity, and every candidate commit
    atomically revalidates and advances the shared carrier-ledger generation.
40. A complete-range Product ACK atomically cancels a still-queued original
    before ownership release; partial queued work remains exact unguarded debt.
    An ambiguous Product release sets at most one guard only on an exact
    native-owned token, so `guarded(token)` implies `stage(token) = Qn`. Exact
    service clears it once; a successor output-
    admission epoch cannot be relatched by predecessor work. A guarded current
    epoch demotes fresh originals only while an unguarded candidate admits and
    never rewrites peer usage.
41. An origin allocates ordered-writer epochs strictly increasingly without
    reuse or wrap in one authenticated session and original-sender direction.
    The receiver enforces bounded live native-writer bindings without a scalar
    retired-order assumption. The origin ignores absent allocated epochs as
    stale receipts and rejects future or wrong-direction epochs.
42. Processed service-frontier state belongs to the ordered writer, not the
    Product stream. A Product-neutral `SERVICE_ACK` can drain shared-writer work
    after stream terminal and cannot grant Product authority.
43. Every positive token reserves exact all-stage byte/item authority before
    publication and has no synthetic MPP service deadline. Stage transfer
    cannot lose a second capacity race; native-owned tokens and guards remain
    charged until cumulative service receipt or exact writer/carrier terminal.
    Zero native service may therefore retain bounded backpressure without a
    finite progress claim, while zero-coordinate receipts remain token-free.
44. Receiver dirty service authority clears only after same-fate reverse-queue
    acceptance or exact origin terminal. For TCP that queue is on the same
    connection; for QUIC it is the carrier-control reverse writer whose loss
    terminalizes the carrier. Cross-carrier and operation-local copies do not
    discharge it.
45. A `STREAM_ACK` is completely validated against an immutable Product-state
    classification and its session-scoped service state before mutation. The
    transaction then applies service before Product. An absent or terminal
    non-reused `StreamId` makes only the Product subrecord stale and cannot
    discard valid service or resurrect Product state.
46. Carrier-ledger generation exhaustion enters one absorbing non-admitting
    state, invalidates captured plans, and performs exact carrier-terminal
    cleanup without a successor ordinal. It cannot strand existing ownership,
    wrap, saturate as a live generation, or admit new work.
47. Stale requalification uses one finite cyclic exact-incarnation cursor and
    at most one pending proof and one stream-owned receipt per direction. The
    ACK carrier is authenticated return service only; the exact non-reused
    pending tuple selects the forward target. One bounded pass copies a receipt
    at most once to each queue-admitting current attachment, retains it only
    after zero publications, and suppresses replay with the accepted exact
    probe high-water. A stable eligible set has bounded attempt fairness under
    the stated retained-byte, authority, local-admission, deadline, and
    actor-fairness assumptions; no rule claims delivery or wall-clock recovery
    at zero service.
48. Carrier observation is scoped to exact session generation, direction,
    carrier, native activation fence, observation-channel, and generation
    identities. It can create carrier work and rate evidence only; it cannot
    create or mutate Product ownership, ranges, Data ACK, receive credit,
    qualification, usage, health, or native congestion authority.
49. Each exact carrier-incarnation and original-sender direction has one
    persistent, exclusive scheduling-rate authority reducer and one checked
    non-resetting revision. `NativeMode` uses only the named controller's
    current gain-free operational `B_op`, consumed from coherent live central
    authority; `ReceiptMode` uses only receipt-derived evidence. Every actual
    Native `PathData`/controller activation has a distinct equality-compared
    lifetime fence, including restoration and same-identity clone. The
    asynchronous adapter exposes transport-owned strictly increasing `E_N` and
    central non-resetting `G` separately; both are carried through every
    decision and precommit, and polling alone is insufficient. Every accepted
    activation, basis/mode, or rate semantic change advances `G`; every
    decision and precommit compare the complete applicable fences. A
    serialized explicit structural contract revocation may switch
    Native→Receipt once while preserving work and spend and cannot raise its
    fallback; low controller state, missing polls, idleness, and wall time
    cannot switch or repeatedly fence acquisition. A construction-time copy is
    never live authority.
50. Observation starts only when changing the target rate alone changes the
    exact checked ordinary decision. The counterfactual preserves debt and all
    non-rate facts. Low native evidence cannot stop rate-causal NativeMode
    excitation; ordinary same-carrier feed pauses or successfully ends it.
51. Sender-local principal-policy-direction authority charges every optional reliable
    payload once. Receiver observation grant is per exact channel and escrowed
    atomically under session and principal direction. Consumed work never
    refunds, terminal returns only unused escrow, and session/channel creation
    cannot mint principal startup authority. Principal consumption survives the
    last session close until an exact old-policy epoch is fully fenced.
52. ReceiptMode has exactly one fixed-expiry active term and one carrier-
    direction acquisition `Acq_c`. The acquisition freezes `P_acq`,
    `H_acq=10*P_acq`, `J_acq>=11`, `q_acq=ceil(H_acq/(J_acq-1))`, conservative
    lower/upper busy-clock bounds, and at most `J_acq` authority-live suffix
    anchors. The first/empty anchor is unconditional; later anchors are at
    least `q_acq` lower-bound busy time apart. Every exact post-fence Product or
    observation token is tagged once and contributes its forward normalized
    work once to every covering retained anchor only after exact peer-
    processing service; it cannot enter a successor. Each candidate divides
    by a conservative elapsed upper bound and is live through `H_acq`
    inclusive. Publication captures one `P_pub/H_pub` snapshot shared by the
    fixed-expiry active term and any atomic zero-work successor; source and
    successor tokens remain fenced. Low candidates and active expiry leave
    `Acq_c` intact; only the fresh exact comparator may publish and fence it. No detached-rate sum,
    unbounded historical maximum, arbitrary eviction, latest-source
    replacement, cross-carrier sum, flow-count multiplier, or per-sample
    native fence supplies rate.
53. Observation DATA and ACK use one exact same-fate channel with a cumulative
    processed-work frontier independent of native-path and semantic-generation
    epochs. ACK applies service before fully scoped semantics, and semantic
    payload cannot exceed its covered-token frontier. A semantic successor may
    coexist with unresolved predecessor tokens under the shared all-stage cap;
    channel terminal retires them without service before channel-epoch reuse.
54. Observation arbitration is scoped to its exact target writer. Higher-
    priority work on that writer may starve it, but ordinary backlog on another
    writer cannot consume its opportunity. No new head is admitted while any
    effective Realtime/Latency work is pending in the same session direction,
    including on another carrier; already-native bounded debt is not
    preemptible. A finite frozen round advances after every attempt and
    publishes one successor wake only after positive work. An ordinary command
    pending at head admission wins; later arrival may wait only behind already-
    published bounded observation debt.

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
usage-aware carrier service-pressure scheduler may leave redundant members idle.

This Core Profile declares no named TCP NativeMode operational-bandwidth
adapter. A TCP carrier therefore selects ReceiptMode. Kernel TCP delivery,
pacing, congestion-window, and queue telemetry remains diagnostic for
scheduling-rate authority; a future profile may select NativeMode only by
declaring the full Section 10.2 adapter contract when a new carrier-incarnation
and direction reducer is constructed.

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

The `QuinnBbr3NativeOperationalV1` NativeMode adapter exports the controller's
current gain-free **operational bandwidth component** `B_op`, specifically
`min(max_bw, bw_shortterm)`, because that is the rate component used by the
controller's live send model. It MUST NOT export only the stale-high long-term
`max_bw` filter, a gain-scaled pacing rate, or an independently smoothed ACK
window. `B_op` is not asserted to be fresh achieved goodput: it may retain or
restore a probe opportunity before a new high sample, and its declared
loss-compensation domain may exceed raw delivered rate. With `p0 = 10%`, that
compensation alone is bounded by `1/(1-p0) = 10/9`, or about `11.11%`, over the
aligned raw rate. This operational state is advisory to Core scheduling; the
native controller still enforces cwnd, pacing, and recovery.

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

The export changes whenever the active native controller changes this
component. A source change resets predecessor-owned initialization and rate
evidence and compare-applies only the coherent new active snapshot, as defined
in Section 10.2. MPP adds no independent smoothing, cap, maximum with another
rate source, expiry, or recovery timer. Every live scheduling consumer
receives the corresponding central-authority revision within `D_pub`;
publication MUST detect a `B_op` change directly and cannot depend on a
detached wrapper sample-count change. This asynchronous adapter exposes
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

On a path where the operator deliberately authorizes an exogenous-loss
allowance, the preferred BBR-family model separates service estimation from
its residual congestion-loss objective.  Let `p0` be the sender-local
authorized loss-compensation fraction and let `q` be the controller's ordinary
residual loss objective, with exact domains `0 <= p0 < 1` and `0 <= q < 1`.
Configuration represents both as checked rational fixed-point values; NaN,
infinity, negative, or unit-and-above values are invalid at configuration
time.

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
allowance.  Integer implementations round compensated volume and rate down,
round `A`, `B`, and every credit addition down, and round loss debits up.  They use checked
widened arithmetic; inability to represent an input or result takes the raw-
authority transition below rather than wrapping, saturating optimistically,
panicking, or dropping evidence.

The corresponding high-loss boundary is the exact dimensionless fraction:

```text
theta = 1 - (1 - p0) * (1 - q)
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
an explicit burst envelope. For exact mathematical reasoning let:

```text
theta = 1 - (1 - p0) * (1 - q)
H     = 3 packet-timed operating rounds
E     = positive resolved volume for the current envelope epoch
A     = H * p0 * E
B     = (1 - theta) * A
```

`A` is the product policy's tolerated displacement of authorized lost bytes.
`B` is the corresponding excess-loss credit: a lost byte also contributes
`theta` credit as part of resolved volume, and therefore consumes
`1 - theta` net credit.  Before the first complete non-application-limited
round, `E` is the larger of the positive initial window and the earliest-sent
aligned lost packet's nonnegative transmit flight.  Thereafter only the
preceding complete non-application-limited round's positive resolved volume
may replace `E`.  If no positive representable `E` exists, this epoch has no
compensated decision and takes raw authority.  Three rounds is an explicit
MPTUNNEL product risk policy: at `p0 = 10%`, it permits at most `0.3 * E` lost
bytes to move across neighboring evidence rounds. It is not a BBR draft
constant, an inferred path property, or a value selected by a benchmark.

The sender maintains excess-loss credit `C` in `[0, B]`.  Creating the first
valid envelope initializes `C := B`; this initial full bucket is the premise
of the response-delay formula below.  Replacing an envelope computes the new
bound only after applying the closing epoch to the old `(C, B)`, then sets
`C := min(C, B_new)`.  Thus a smaller bound clamps retained credit and a larger
bound never mints it. While compensation remains enabled, every loss
declaration admitted to its journal has one stable record carrying its packet
number space, packet number, byte count,
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

The carrier-observation channel is an ordinary client-initiated bidirectional
HTTP/3 request stream, not a QUIC control stream. It retains the profile's
canonical encrypted `mpp-datagram: ?1` request field but opens no datagram flow
and carries observation frames only in reliable HTTP/3 DATA. HTTP/3 request/
response field sections and stream errors retain RFC 9114 semantics.
Operation-local observation-stream FIN or reset cannot be promoted into a
QUIC-connection or MPP-carrier failure; a connection-level error is still
carrier-terminal.

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
