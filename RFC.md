# mptunnel Protocol Specification

Intended status: Standards Track

Protocol version: 1

Last updated: 2026-07-03

## Abstract

This document specifies mptunnel version 1. mptunnel is an encrypted
multipath proxy and tunnel protocol that exposes local SOCKS5, HTTP CONNECT,
and TUN L4 ingress, then carries TCP streams and UDP datagrams over one or more
authenticated TCP and UDP underlay paths. The internal protocol terminates
external proxy handshakes at the client edge, opens internal reliable streams or
datagram flows to the server, and lets the server connect to the requested
target using direct, bind-source-IP, SOCKS5, HTTP CONNECT, or HTTP CONNECT-UDP
outbound policy.

This specification follows the broad structure used by IETF RFCs: terminology,
protocol overview, packet and frame formats, state machines, transport behavior,
security considerations, IANA considerations, references, and appendices.
It is the normative protocol and design contract for conforming implementations.
Reviewers should be able to understand the intended system behavior from this
document alone.

## Status of This Memo

This memo defines the mptunnel protocol and product behavior. It is not an IETF
RFC and it does not allocate IANA registry values. The normative keywords
"MUST", "MUST NOT", "REQUIRED", "SHOULD", "SHOULD NOT", and "MAY" are to be
interpreted as described by RFC 2119 and RFC 8174.

Conforming implementations MUST follow this specification. If behavior and this
document differ, the discrepancy is a defect to resolve by changing behavior or
by explicitly revising this specification.

The mptunnel project intentionally does not preserve old internal wire formats.
An implementation of protocol version 1 MUST reject unsupported versions and
MUST NOT silently accept legacy frame layouts.

## Table of Contents

1. Introduction
2. Terminology
3. Requirements and Product Model
4. Protocol Architecture
5. Configuration Model
6. Path Specifications and Capabilities
7. Cryptographic Material and Authentication
8. Product Frame Encoding
9. Product Frame Registry
10. TCP Underlay Transport
11. UDP Carrier Transport
12. Session and Path State Machines
13. Reliable Stream Layer
14. Datagram Flow Layer
15. Ingress Behavior
16. Outbound Behavior
17. Adaptive Auto Scheduling
18. Multipath, Failover, and Roaming
19. Resource Management
20. Management API and Diagnostics
21. Error Handling
22. Security Considerations
23. IANA Considerations
24. Versioning and Compatibility
25. References
Appendix A. Numeric Registries
Appendix B. Abstract Algorithms

## 1. Introduction

mptunnel provides a local proxy or TUN interface and a remote server endpoint.
The client and server share a high-entropy secret. The client opens one or more
underlay paths to the server. Each path is independently authenticated, encrypted
unless explicit plaintext lab mode is selected, and assigned a path identifier.

The protocol is not SOCKS5-over-the-wire. SOCKS5, HTTP CONNECT, and TUN
handshakes are local ingress mechanisms. The client extracts target metadata and
opens an internal stream or datagram flow:

```
application -> SOCKS5/HTTP CONNECT/TUN -> mptunnel client
             -> encrypted multipath protocol -> mptunnel server
             -> configured outbound -> target
```

The product model is:

```
any ingress x any underlay x any outbound x TCP/UDP target
```

TCP targets use the reliable stream layer. UDP targets use datagram flows. A
datagram flow MAY run over any reliable-stream-capable underlay carrier,
including TCP or QUIC UDP, and the selected carrier is determined by live path
metrics, TTL/freshness, loss, queue/flight state, and demand. There is no
hardcoded TCP-vs-UDP underlay preference: fresh realtime traffic starts
latency-first, sustained demand may move toward higher measured bandwidth, and
the scheduler may shrink back to latency/realtime behavior when bulk demand
disappears.

### 1.1 Design Goals and Operating Model

mptunnel is designed around a practical observation: users want one local proxy
or TUN interface, but real Internet paths differ by latency, bandwidth, loss,
NAT behavior, protocol blocking, and QoS. The protocol therefore separates
external compatibility from internal transport. SOCKS5, HTTP CONNECT, and TUN
exist only at the edge so applications do not need to change. The internal
protocol carries compact target metadata plus stream/datagram payloads so the
client and server can schedule traffic using measurements instead of preserving
proxy handshakes end-to-end.

The design has three cooperating planes. The session plane authenticates the
logical relationship and allows multiple underlay paths to join it. The product
plane exposes reliable streams and datagram flows using path-independent
identifiers and offsets. The carrier plane optimizes TCP and UDP underlays
according to their different properties. Ingress, underlay, outbound, and
target protocol are therefore orthogonal dimensions. The scheduler binds those
dimensions at runtime by combining flow demand with live path models.

The resulting behavior should be simple for operators: endpoints and a secret
are sufficient for normal use. The complexity lives inside Auto scheduling,
loss repair, pacing, and path selection, because fixed user-selected modes do
not generalize across daily browsing, SSH-like interaction, video streams, and
large file transfers.

## 2. Terminology

Client:
  The local process that accepts SOCKS5, HTTP CONNECT, or TUN ingress.

Server:
  The remote process that accepts path connections and connects to targets.

Underlay path:
  One TCP or UDP transport association between client and server.

Session:
  The logical authenticated relationship shared by all underlay paths for one
  client/server instance.

Product frame:
  A versioned `MPTF` frame carrying session, path, stream, datagram, metrics, or
  control data.

TCP underlay:
  A TCP connection carrying `MPTE` encrypted product frames.

UDP carrier:
  A QUIC transport association over UDP. QUIC owns packet numbers, packet ACKs,
  packet loss recovery, PTO, congestion control, pacing, PMTU, and connection
  continuity below product frames.

Reliable stream:
  An ordered byte stream identified by `StreamId`, carried as `STREAM_DATA`
  frames with absolute offsets and acknowledged by offset ranges.

Datagram flow:
  A flow identified by `DatagramFlowId`, carrying unordered payloads identified
  by `DatagramId`.

Auto:
  The adaptive scheduling policy. There is no user-selectable fixed transmission
  mode for production traffic.

Flow lane:
  An internal demand class: Control, Latency, Throughput, RealtimeDatagram, or
  Background.

Path model:
  Per-path measured and hinted state: RTT, jitter, delivery rate, loss, queue
  bytes, bytes in flight, pacing rate, inflight limit, confidence, application
  limited state, health, and capabilities.

## 3. Requirements and Product Model

An implementation of this protocol MUST satisfy these requirements.

* It MUST run on Windows, Linux, and macOS on amd64 and aarch64.
* It MUST support local SOCKS5, HTTP CONNECT, and TUN L4 ingress.
* It MUST support TCP and UDP underlay paths as first-class underlays.
* It MUST support TCP targets and UDP targets.
* It MUST support direct outbound, direct outbound with source IP binding,
  upstream SOCKS5 outbound, upstream HTTP CONNECT outbound for TCP targets, and
  upstream HTTP CONNECT-UDP outbound for UDP targets.
* It MUST encrypt internal transport by default.
* It MUST require an explicit insecure acknowledgement for plaintext lab mode.
* It MUST authenticate session and path setup even in plaintext lab mode.
* It MUST be adaptive by default. Operators SHOULD provide only endpoints for
  normal use.
* It MUST NOT terminate production traffic merely because a configured resource
  target is exceeded; production behavior is adaptive and self-evolving.
* Diagnostic assertions, ablations, and benchmarks MUST NOT be compiled into
  release bundles unless an explicit diagnostics feature is enabled.

The implementation targets fluent web browsing, SSH-like interactive flows,
video/game-like UDP behavior, bulk downloads, bulk uploads, failover recovery,
and mixed links with substantially different latency, bandwidth, and loss.

### 3.1 Operating Assumptions

Cross-platform support is a product requirement because the local edge is
usually a user device or workstation, while the remote edge is often a VPS. TCP
and UDP underlays are both first-class because they solve different operational
problems: TCP is widely reachable and proxy-friendly, while UDP lets QUIC own
packet recovery, pacing, congestion control, and roaming without waiting for
kernel TCP behavior.

Encryption is default because the internal protocol carries target metadata and
payloads. Plaintext is reserved for explicit lab use so that performance
experiments can isolate encryption overhead without creating an unsafe product
default. Authentication remains mandatory even in plaintext mode because path
joins and session attachment must not be forgeable.

The "adaptive by default" requirement follows from heterogeneous links. A
single fixed choice, such as always striping or always using the lowest RTT
path, fails under common cases: a high-bandwidth but higher-RTT link may be
excellent for bulk transfer, while the same path may harm short interactive
requests. Auto therefore treats path choice as a continuous control problem,
not as a user-visible transmission mode.

## 4. Protocol Architecture

mptunnel is layered as follows:

```
Ingress layer
  SOCKS5, HTTP CONNECT, TUN TCP, TUN UDP

Session/path layer
  SESSION_AUTH, PATH_JOIN, PATH_STATUS, health, replay protection

Stream/datagram layer
  OPEN_STREAM, STREAM_DATA, STREAM_ACK, STREAM_MAX_DATA, STREAM_FIN
  OPEN_DGRAM_FLOW, DGRAM_DATA, DGRAM_FEEDBACK

Scheduler/path model
  Flow demand, ETA scoring, striping, repair, failover, probes

Underlay carrier
  TCP: MPTE encrypted framed stream
  UDP: QUIC carrier stream over UDP

Cryptographic layer
  AES-256-GCM by default, ChaCha20-Poly1305 optional
```

The same product frames are used over TCP and UDP underlays. TCP underlay gives
reachability and a reliable byte pipe. UDP underlay lets QUIC own packet
numbers, packet ACKs, pacing, loss recovery, congestion control, PMTU, and NAT
rebinding behavior while mptunnel owns product semantics above the QUIC stream.

Using one product frame grammar above both underlays prevents feature drift. A
stream opened over TCP can later be repaired or reattached over UDP because
stream IDs and offsets live above the carrier. Conversely, keeping carrier
behavior below product frames lets TCP and UDP be optimized independently. TCP
does not expose packet loss or useful packet numbers to the product; UDP does.
The architecture therefore shares semantic frames but not congestion-control
assumptions.

This mirrors the useful separation in mature transports: MPTCP separates the
logical byte stream from subflow sequence spaces, QUIC separates streams from
packet recovery, and carrier congestion controllers separate delivery-rate
models from application semantics. mptunnel applies the same separation while
preserving proxy and TUN compatibility; it does not define a second UDP
reliability protocol underneath QUIC.

### 4.1 Ownership Model

A conforming implementation MUST keep ownership boundaries explicit. The rule is
simple: configuration owns operator intent, sessions own live protocol identity,
the sender service owns product bytes, path models own evidence, schedulers own
decisions, and carrier engines own packet or connection mechanics. No layer may
silently substitute its state for another layer's state.

The runtime hierarchy is:

```
inbound
  -> routing rule
    -> outbound tag or balancer tag
      -> outbound instance
        -> MPP session when the selected outbound is mpp
          -> live carrier paths
            -> product reliable streams and datagram flows
              -> selected remote outbound target
```

An inbound owns only local ingress exposure: listen addresses, TUN device
binding, local proxy authentication, accepted ingress protocol, and inbound tag.
It does not own remote MPP paths, target dialing, stream offsets, datagram IDs,
or congestion decisions. After it produces target metadata and local user
intent, the routing layer decides which outbound or balancer receives the flow.

Routing owns tag selection only. It may select one outbound tag directly or one
balancer tag. It does not buffer product bytes and does not choose carrier paths
inside an MPP session. A route decision ends when the selected outbound accepts
the flow.

An outbound owns how traffic leaves the current process. A direct outbound owns
target dialing and optional source-address binding; binding is a property of
direct dialing, not a separate outbound kind. SOCKS5, HTTP CONNECT, and HTTP
CONNECT-UDP outbounds own upstream proxy negotiation. An MPP outbound owns the
remote MPP endpoint relationship: peer address, security material, path
specifications, and session creation policy. An outbound does not own bytes after
they have entered an active MPP sender service.

A balancer owns selection among outbound tags. Sequence and random balancers
choose one member for a flow. A combined MPP balancer may assemble one logical
MPP outbound from multiple MPP members when policy explicitly asks for combined
transport capacity, including members with distinct secrets. A balancer owns
selection policy and member health; it does not own stream offsets, carrier
packet numbers, or repair state.

An MPP session owns the authenticated peer relationship and the versioned
protocol identity within that relationship: session ID, key schedule, negotiated
resource envelope, stream ID space, datagram flow ID space, live path registry,
path-join replay cache, and management snapshot identity. A session does not own
target sockets and does not directly schedule product bytes; it provides the
identity and registries used by the sender service and carrier engines.

A path owns one concrete underlay association inside a session. For TCP this is
one encrypted framed stream session. For UDP this is one QUIC association. A
path owns path ID, underlay family, bind/peer address, liveness,
carrier-local RTT/loss/rate/queue samples, carrier credit, and path-specific
authentication. A path MUST NOT own product stream offsets or decide that a
reliable stream should stripe onto it merely because it has capacity.

A path group or carrier subflow set is not a product-offset owner. It is a bounded
scheduler epoch for one flow: one Service path plus admitted Subflow members
selected from session paths, path-model evidence, queue state, and
ECF/BLEST/no-worse admission. ACK progress updates delivery metrics, ordering
frontier, and later admission inputs, but it MUST NOT erase the subflow set's
spent startup owner credit or recreate validation credit. The epoch remains
valid while its Service, owner-credit envelope, overhead budget, read-gap budget,
and live carrier membership still match the admission envelope. It is recreated
on material envelope change, detach/failover, or carrier-membership change, not
on ordinary ACK progress. Product byte ownership still belongs to the per-range
flight ledger, not to the subflow set itself. The Service path is the current
lower-frontier owner or live ordered-owner anchor for that stream direction. It
is not simply the lowest-ETA candidate, and a measured
alternate MUST NOT be relabeled as Service merely because it wins the next
payload quantum. Detach, close, or carrier loss invalidates the old live output,
but it MUST NOT by itself transfer Service ownership to a survivor while
ordered-owner scheduling debt remains. In that state the sender waits, performs
bounded failover repair, or resumes only after the contiguous frontier catches
up. A new Service owner is chosen by explicit sender-service admission at a
clear frontier or by a dedicated failover policy after lower ownership has been
resolved. A survivor is not promoted to Service merely because it is the only
remaining attached output; it needs explicit frontier-clear Service failover
admission and path-scoped sender evidence. Otherwise, proof/liveness-only
survivors remain Probe/Standby, and lower-ETA measured contributors are
Subflows.

Path attachment roles are not scheduler ownership. `Active`, `Validation`, and
`Repair` describe why a carrier stream was opened and which control frames were
used to attach it; they do not by themselves decide ordinary reliable-stream
ownership. If a validation-attached path owns the oldest lower outstanding byte
range, that path is the lower-frontier owner until ACK progress or repair clears
the range. The sender service MUST allow that owner to lead the next ordinary
quantum when it is still attached and can accept work, because sending later
unique bytes on a different path would expand the ordered receive hole. Conversely
a previously active path MUST NOT receive owner bytes merely because it was the
first or latest active attachment; it remains only the Service anchor until the
ordinary admission model grants Service or Subflow `OwnerData`.

A live response output has a single command-channel owner for a given
`(stream_id, underlay, path_id)`. Reannouncing the same live channel as
`Active` is an attachment/liveness update of the existing output; it does not
change Service ownership. Service ownership changes only through explicit
sender-service `OwnerData` admission, measured failover, or a dedicated
frontier-clear Service migration decision. Throughput-lane Service migration
requires bulk-sized direction-correct owner-byte evidence for the target path:
at least one current Service quantum of product `OwnerData` ACK evidence or an
equivalent non-app-limited carrier ACK sample. A startup/probe-sized delivery
sample may admit Probe/Subflow discovery, but it MUST NOT move the Service
owner. Reannouncing the same path key with a different live command channel is a
duplicate live output and MUST be ignored or rejected; it MUST NOT replace the
existing output, split owner bytes across two command channels for the same path
key, or use output-list tail position as a hidden ownership signal.

Same-underlay carrier subflow sets SHOULD be opened together when the sender has
already admitted multiple paths from the same carrier family for bulk work. This
keeps TCP+TCP and QUIC+QUIC path groups from being serialized by path-opening
mechanics after the scheduler has found a safe subflow set. Mixed TCP/UDP candidates
remain stricter: a mixed candidate may be opened for validation proof, repair,
or explicit frontier-safe migration, but a path opener MUST NOT turn a mixed ETA
list into blind same-stream unique-byte striping.

Opening a same-underlay subflow set is not the same as immediately committing
unique ordered bytes to every member. MPTCP and MPQUIC can coordinate packet
or subflow recovery inside one transport-level connection; mptunnel's TCP and
QUIC carrier outputs sit below the product stream and above separate carrier
recovery engines. Therefore, for one ordered reliable stream, a proof-only
validation output MUST NOT carry later unique `STREAM_DATA` while another
output owns an unresolved lower outstanding range. A validation output may carry
control, ACK, explicit repair, path proof, or a new independent product stream.
At a clear frontier, liveness, path-proof, configured hints, and peer hints MAY
rank validation/probe order, but they MUST NOT make a non-active path the
Service owner for ordered product bytes while a live Service owner remains. A
path also MUST NOT become Service merely because the selector has no better
lead fallback; fallback lead selection without active Service anchor rights,
direction-correct bulk-rate evidence, or explicit frontier-clear failover
admission with path-scoped sender evidence is still Probe/Standby, not product
ownership. Temporary sender-service backpressure or capacity filtering on the
current ordered owner also MUST NOT erase that Service anchor; dispatchable
alternates remain Subflows unless an explicit Service migration/failover
decision changes the owner. A measured path may become an admitted Subflow
after direction-correct bulk-rate evidence exists and the ETA/no-worse selector
admits that owner range; it does not replace the Service anchor unless the
explicit Service migration policy performs a handoff.
When the current Service owner is alive but backpressured by unresolved
contiguous owner tail, a cross-underlay alternate MUST wait instead of owning
later byte ranges. Same-underlay Subflow startup or steady-state admission may
still proceed through its normal no-worse gates because it shares the same
carrier-family recovery assumptions; cross-underlay ownership requires the
Service owner to be feedable, failed, or explicitly migrated.
ACK-data-only evidence from a tiny or application-limited probe is still not
bulk-rate evidence and does not grant Service rights. A
Subflow on the current Service family may carry a bounded startup `OwnerData`
window at a clear ordered frontier after the current Service has
direction-correct bulk-rate evidence and the Subflow has path-scoped sender
evidence. Configured or peer
hints alone are not sender evidence and MUST NOT unlock this unique-data
window. That window is unique payload data, not duplicate/probe traffic; it does
not change the Service owner hint and it does not make the Subflow the
lower-frontier Service for additional bytes. It is capped by the path-adaptive
startup owner credit and is not a steady-state role. After that credit is
spent, further Subflow `OwnerData` requires direction-correct bulk-rate
evidence. Steady-state Subflow `OwnerData` requires bulk-rate evidence and
sender-service admission proving that doing so will not expand product
receive-hole debt or worsen the completion horizon. This rule is path-metric
driven; it is not a TCP-preferred or UDP-preferred policy. Mixed TCP+QUIC paths
are deliberately stricter in production v1 because they do not share one
carrier-family recovery model. A bulk-rate-proven mixed candidate that already
owns the lower outstanding range may continue that range through the normal
no-worse gates. A clear-frontier mixed candidate that does not already own the
Service role remains Probe/RepairOnly/Standby until an explicit Service
migration or failover decision changes the owner.

A product reliable stream owns only stream semantics: stream ID, target metadata,
ingress metadata, outbound policy metadata, send offset space, receive offset
space, STREAM_ACK ranges, STREAM_MAX_DATA, STREAM_FIN final offset, repair-cache
byte ranges, and receive reorder ranges. It supplies byte facts to the sender
service. It MUST NOT own congestion, pacing, path scoring, or carrier packet
state.

A datagram flow owns flow ID, target metadata, datagram IDs, TTL, feedback
ranges, and datagram queue bytes. It owns unreliable message identity and expiry.
It does not own reliable-stream repair or carrier congestion state.

The sender service owns product work that is ready to leave the process but has
not yet been admitted to a carrier. It owns lane queues, per-flow fairness,
product queue age, repair priority, validation work, flow-control gating,
preemptible quanta, and the dispatch ledger that binds a product byte range to a
selected carrier path. Server response bytes and client upload bytes MUST enter
a sender-service queue before they can become STREAM_DATA carrier commands.
Control, ACK, FIN, RESET, DETACH, repair, realtime, latency, throughput, and
background lanes are sender-service concerns, not path-queue concerns.
Once the sender service admits `OwnerData` to a carrier output, all `OwnerData`
for the same product stream and output MUST enter one stream-ordered carrier
emission queue. Flow lanes may influence when bytes are admitted and which path
is selected, but they MUST NOT split ordered product bytes across priority and
bulk carrier queues where later offsets can overtake earlier offsets. This is
the mptunnel analogue of keeping connection-level data sequence ownership
separate from subflow/path scheduling.
Similarly, latency-first startup state and live latency-sensitive flow counters
may affect sender-service fairness and preemption, but they MUST NOT reduce a
clear-frontier bulk Service owner to a tiny startup-rate or carrier-cwnd
product admission ceiling. While bulk demand exists and the ordered frontier is
clear, the Service owner is fed through the product Service envelope; lower
carrier congestion and pacing remain the carrier engine's responsibility.
Before product progress exists, that envelope uses bounded startup-feedback
credit: it remains well above one carrier quantum, but it is below the
geometric Service horizon so a slow initial Service cannot preload megabytes of
lower ordered bytes before ACK evidence arrives. The Service envelope MAY use
non-app-limited product progress samples as a capacity signal. An app-limited
progress sample is not bulk-rate proof and MUST NOT initialize a tiny
startup-rate or carrier-cwnd product ceiling. When that sample already shows
meaningful ACK feedback, however, it MAY cap the clear-frontier Service feed to
the ACK-feedback horizon until non-app-limited bulk evidence arrives. That
prevents the Service owner from preloading later ordered bytes far beyond the
receiver's observed progress. Tiny app-limited samples may still inform ETA and
diagnostics, but they MUST NOT unlock the full geometric Service horizon. They
cap the feed at the bounded startup-feedback horizon that avoids carrier-cwnd
starvation.
This Service rule is deliberately different from optional Subflow admission:
the Service is the current primary owner and must remain fed while its ordered
frontier is clear, whereas optional paths must prove positive contribution
before receiving owner bytes.
When latency-sensitive work is active, the clear-frontier Service feed envelope
uses the preemptible Service horizon for queued sender-service bytes. That
backlog cap is feed/backpressure accounting, not reorder accounting. Reorder
budgets are for additional paths, cross-path lower-byte debt, and explicit
owner-debt pressure; they MUST NOT count same-Service queued carrier work as
cross-path reorder debt or make the active Service owner inadmissible because an
app-limited BDP estimate is smaller than one queued carrier quantum.

The scheduler and algorithms own policy decisions only. They consume sender
queue snapshots, path-model snapshots, stream-ordering debt, flow demand,
validation state, and carrier credit, then return an admitted path or subflow set plus
an explanation. They MUST NOT mutate repair caches, write to carriers, own
application buffers, or treat hints as delivery proof.

For data-plane accounting, the sender service reduces carrier work to four
meanings. `OwnerData` is the only work kind that owns a unique product byte
range and can move the ordered-data owner. `RepairData` is a duplicate of an
already-owned byte range and never creates path delivery proof or ordinary
ownership. `Probe` carries path-scoped validation such as `PATH_PROOF_DATA` and
does not enter product offset space. `Control` carries ACK, flow-control,
metrics, reset, detach, FIN, and similar protocol state. Implementations MAY
encode these meanings with existing frame types, but the ledgers MUST preserve
the distinction; a `STREAM_DATA` frame used for repair does not become
`OwnerData` merely because its wire frame type is `STREAM_DATA`.

Connection-level ACK/control frames update product-stream state, so their
return path is part of the product ACK clock. When the current Service path is
live and has control capacity, receive-progress `Control` such as `STREAM_ACK`
and `STREAM_MAX_DATA` SHOULD be emitted on that Service return path before
lower-ETA Probe or Validation paths are considered. If the Service return path
is blocked or failed, the sender MAY fall back to any admissible control path.
Probe paths therefore cannot indirectly throttle or steer an ordered bytestream
merely by becoming the preferred ACK carrier. This mirrors MPTCP's separation
between connection-level DATA_ACK state and subflow/path validation state while
avoiding the known failure mode where a lossy return path delays data ACK
release for a healthy forward owner.

The path model owns evidence and provenance. Local sender-side evidence from
carrier ACKs, QUIC delivery samples, unpolluted ordered stream delivery, and
datagram feedback is authoritative for scheduling. Peer PATH_METRICS are bounded
validation hints unless their provenance, freshness, confidence, and direction
make them safe for the specific decision. A STREAM_ACK for a unique outstanding
`OwnerData` range is path-scoped product delivery evidence because the sender's
flight ledger knows the only owner. A STREAM_ACK for duplicated `RepairData` or
ambiguous copies releases product repair state and ordering debt but is not
carrier bandwidth proof and cannot promote any path.

When authoritative ACK ranges expose a lower product gap and repair bytes remain
outstanding, a bulk sender MUST stop dispatching later queued bytes as new
`OwnerData` if doing so would expand the unresolved lower frontier. Bounded local
product-source reads may continue while those bytes remain unassigned in the
sender-service queue; reading source bytes is memory staging, not ownership.
Repair and control traffic may continue, but expanding future owner ranges
behind a known hole violates the no-worse/HOL guard and turns repair into
throughput churn. New `OwnerData` may resume after the ordered frontier closes
or the gap no longer exposes repair debt. Fresh or speculative repair remains
charged to the extra-traffic budget so a lossy owner cannot convert the stream
into unbounded duplicate traffic. A persistent authoritative gap is correctness
repair: it may exceed the optional extra-traffic budget only after the
persistent-gap guard fires, and each repair event remains bounded by the current
repair quantum, outstanding repair debt, and configured repair/path-flight
resource caps.

When ACK ranges are contiguous but stop before the sender's highest owner
offset, the unacknowledged suffix is not repairable merely because it is
unacknowledged. A live TCP or QUIC carrier owns its own packet recovery, and
same-output product retransmission cannot overtake the missing bytes. A live
owner tail with no complete ACK frontier is normal in-flight data, not repair
debt. If a complete ACK frontier exists, the frontier remains blocked for one
PTO-derived product stall timeout, and a different live output can carry the
lowest blocked range, that suffix becomes tail correctness repair: it may be
retransmitted as `RepairData` only on an output that did not own the offset
range and under the same repair/path-flight caps. The first tail probe
deliberately uses one product stall timeout, not the persistent-congestion
multiplier used for authoritative ACK-gap repair, because the repair is a
bounded reinjection of the exact lowest HOL-blocking suffix on a different
output. If that probe does not produce ACK-frontier progress, repeated tail
repair backs off to the persistent-congestion multiplier so a live carrier is
not converted into continuous duplicate traffic. Detached, failed, or
no-longer-serviceable owners use the same bounded tail-repair mechanism because
the original owner can no longer make progress. This repair remains duplicate
product data and MUST NOT create path delivery proof, move the Service owner,
or reset Subflow admission state.

For scheduling and tail-repair timers, the product ACK frontier is the end of
the first ACK range only when that range starts at offset 0. It is not the
largest end offset carried by a sparse `STREAM_ACK`. Sparse higher ranges may
release per-range flight bookkeeping, but they do not prove contiguous
application delivery and MUST NOT erase ordered-owner scheduling debt, reset the
lower-frontier stall clock, or authorize later `OwnerData` on a fallback path.

The sender-service queue MUST NOT treat all `RepairData` as a priority lane.
Ordinary budgeted repair is queued behind `OwnerData`; it can use otherwise idle
carrier capacity but cannot starve the service path. Only repair explicitly
classified as critical because it closes an authoritative product ACK gap, known
terminal tail, failed-owner gap/tail, or persistent live-owner tail on an
alternate output may preempt later `OwnerData`. Critical priority is separate
from budget bypass: critical repair still consumes the extra-traffic budget
unless the implementation is repairing one of those bounded correctness cases
and the optional budget is exhausted. That correctness repair is still
`RepairData`: it does not prove the repair path, does not move the Service
owner, and each event is bounded by the current repair quantum,
outstanding repair debt, and configured repair/path-flight resource caps.

Carrier engines own underlay mechanics. TCP owns encrypted framed records,
writer backpressure, TCP path heartbeat, and TCP session shutdown. QUIC UDP owns
QUIC packets, QUIC TLS, connection IDs, packet recovery, congestion control,
pacing, ACKs, and roaming. Carrier engines report credit and metrics upward;
they do not decide product flow fairness or rewrite stream ordering rules. QUIC
stream I/O MUST be polled with cancellation-safe read state: product frame
framing bytes already consumed from a QUIC stream remain carrier-owned until a
whole mptunnel frame is decoded. Because mptunnel frames over QUIC are
length-prefixed records on an ordered QUIC stream, the QUIC carrier MAY split a
large product `STREAM_DATA` quantum into smaller length-prefixed carrier records
with consecutive product offsets. This split is a carrier serialization detail:
it MUST NOT lower the product sender-service quantum, read quantum, flow-control
window, or admission envelope. QUIC bytes accepted into the carrier writer but
not yet carrier-ACKed MUST be reported upward as carrier queue/flight debt, not
as drained product capacity.

Buffers are owned by the layer whose invariant they protect:

* inbound buffers protect local proxy/TUN reads before routing;
* sender-service queues protect product lane fairness before carrier admission;
* repair caches protect reliable stream byte-range recovery;
* receive reorder buffers protect contiguous stream delivery;
* datagram queues protect TTL-bounded datagram flow delivery;
* carrier command queues are dumb emission pipes for already-admitted work;
* carrier flight state protects packet or framed-record accounting;
* orphan/reorder carrier buffers protect valid packets whose product control
  context has not arrived yet;
* outbound target buffers protect the selected egress socket or upstream proxy.

Management and diagnostics own observation and explicit control-plane requests.
They may expose snapshots, trends, path state, queue state, scheduler decisions,
ledger reconciliation, and manual administrative actions. They MUST NOT become a
hidden data-plane owner, and enabling diagnostics MUST NOT select a different
carrier, scheduler, protocol, crypto, or queue implementation.

These ownership rules are normative because most bad multipath behavior comes
from misplaced ownership: path queues trying to be fair schedulers, carrier ACKs
being treated as product delivery, stream ACKs being treated as carrier-rate
proof, validation bytes owning the only copy of ordered data, or TCP-named relay
buffers governing UDP-backed product streams. The model deliberately follows the
same separation that makes mature transports understandable: MPTCP maps data
sequence numbers onto subflows without letting subflows own the application byte
identity, QUIC keeps stream offsets separate from packet numbers, and BBR builds
a path model from delivery evidence instead of from application labels.

## 5. Configuration Model

### 5.1 Global Security Parameters

The global CLI/environment configuration includes:

* `--secret` / `MPTUNNEL_SECRET`
* `--cipher` / `MPTUNNEL_CIPHER`
* `--security` / `MPTUNNEL_SECURITY`
* `--auth-freshness-window-seconds`
* `--i-understand-this-is-insecure`

`--cipher` defaults to `aes-256-gcm`. `chacha20-poly1305` is supported when it is
better for the deployment CPU or platform. Both endpoints MUST choose the same
cipher suite.

AES-256-GCM is the default because modern amd64 and aarch64 systems commonly
provide hardware acceleration for AES and carry-less multiplication, making
AES-GCM fast and power-efficient. ChaCha20-Poly1305 is kept as an equal-strength
operational option for platforms where AES acceleration is absent, slow,
disabled, or more fingerprintable in the local environment. The cipher is
explicit configuration rather than negotiation so both endpoints have a
deterministic security posture.

### 5.2 Runtime Configuration File

A conforming command-line implementation SHOULD support both direct CLI/env
configuration and a TOML runtime configuration file. When the executable is
started without arguments, it MUST attempt to load `config.toml` from the
current working directory. An explicit `--config PATH` or `-c PATH` selects a
different TOML file. The file format is an operator interface only; it does not
change the protocol wire format.

The exact product configuration schema is intentionally not normative in this
protocol specification. It belongs to product design documentation because it
describes operator ergonomics, routing policy, tags, deployment roles, and
management surface rather than MPP wire behavior. A configuration frontend MUST
nevertheless compile to the same protocol objects defined here: authenticated
MPP path endpoints, encryption/authentication parameters for each MPP peer
relationship, ingress target metadata, outbound target policy, flow-control
limits, and management API settings. Unknown or contradictory operator fields
SHOULD be rejected before runtime.

MPP inbounds and MPP outbounds MAY carry performance hints scoped to that path
group. A version 1 implementation defines `extra_traffic_hint_percent` as an
operator hint for how much optional repair overhead is
acceptable when the sender has evidence that additional traffic can reduce
latency, failover time, or ordering stalls. Path proof/probe traffic is bounded
by validation fan-out and the path-proof startup payload rather than by
per-dispatch response sender accounting. The value is numeric rather than a
named mode because performance policy is continuous: `5` means roughly five
percent extra optional response traffic is acceptable under evidence-backed
pressure, `100` permits full duplication in pathological moments, and values
above `100` bias the sender toward redundant repair under severe instability.
This value is a hard sender-side budget for optional repair work, with a small
startup floor to avoid repair deadlock. It is not a fixed rate, not a
product-data throttle, and not a condition that can terminate production
traffic or prevent correctness repair for an authoritative ACK gap, a
failed-owner gap/tail, a persistent live-owner tail with an alternate output, or
a known final tail. Such repair may exceed the optional hint only in those
bounded cases, and remains bounded by repair-cache, path-flight, and sender
resource limits. Auto scheduling remains the default and MUST adapt from
measurements even when the hint is left unset.

### 5.3 Resource Parameters

Default resource parameters are:

| Parameter | Default |
| --- | ---: |
| max frame bytes | 1,048,576 |
| max payload bytes | 1,048,512 |
| max ACK ranges | 256 |
| max paths | 64 |
| max streams | 65,536 |
| max QUIC concurrent bidirectional streams | 65,536 |
| max stream window bytes | 67,108,864 |
| max repair bytes | 67,108,864 |
| max reorder bytes | 67,108,864 |
| max datagram queue bytes | 16,777,216 |
| max path flight bytes | 67,108,864 |
| max reliable relay chunk bytes | 524,288 |
| TCP path heartbeat interval | 10,000 ms |
| TCP path heartbeat timeout | 30,000 ms |

An implementation MUST validate that limits are internally coherent. In
particular, ACK range count, path count, stream count, stream window, repair
capacity, reorder capacity, datagram queue capacity, relay chunk size, path
inflight size, and heartbeat timings MUST be nonzero where applicable. A relay
chunk MUST NOT exceed maximum payload bytes, path flight MUST be at least one
relay chunk, and path flight MUST NOT exceed repair capacity.

These values are runtime resource limits, not benchmark pass/fail hard guards.
The scheduler and carriers MUST adapt within the configured envelope.

The defaults are chosen to provide a usable operating envelope without
hard-coding benchmark pass/fail behavior:

* A 1 MiB frame limit is large enough for efficient bulk transfer and compact
  enough to reject accidental or malicious oversized messages before they create
  unbounded allocation pressure.
* The 1,048,512 byte payload limit reserves header space under the 1 MiB frame
  ceiling so a conforming encoder can validate capacity before serialization.
* 256 ACK ranges support sparse recovery under burst loss without allowing ACK
  frames to become a second data stream.
* 64 paths and 65,536 streams are protocol-scale limits. They are above normal
  deployment needs but low enough to keep registries, arrays, and diagnostics
  bounded. QUIC bidirectional stream concurrency is a separate QUIC transport
  advertisement and defaults to the product stream cap. It MUST NOT be derived
  from byte receive-window ratios; QUIC stream count and QUIC byte flow control
  are separate resource mechanisms.
* 64 MiB stream, repair, and reorder budgets cover roughly one second of data at
  about 500 Mbps, or a smaller time slice near 1 Gbps, which is sufficient for
  high-BDP paths without making web browsing or SSH reserve that memory up
  front.
* The 16 MiB datagram queue protects realtime UDP from bulk stream pressure
  while still allowing short bursts and NAT-rebinding recovery.
* The 64 MiB path flight budget matches the default repair capacity. It is a
  sender-service and carrier-resource envelope, not a preallocated buffer and
  not a TCP-only queue. Config-file parsing derives it from repair capacity
  when the field is omitted so operators can scale high-BDP deployments by
  raising the repair/flight envelope together.
* The 512 KiB reliable relay chunk is a read-buffer ceiling. It MUST NOT become an
  indivisible scheduler item, AEAD record, or shared-path write quantum. The
  scheduler uses smaller preemptible quanta so control, ACK, repair, latency,
  and later bulk flows can interleave with existing bulk transfer.
  Throughput quanta are nevertheless required to amortize user-space encryption,
  framing, and write wakeups. A sender MUST NOT let a transient low measured
  delivery rate create a self-reinforcing tiny-frame loop for sustained bulk
  streams. For reliable TCP and QUIC UDP carriers, the product sender feeds the
  carrier with a bounded BBR-style bulk service quantum: no larger than the
  BBR-style 64 KiB send quantum and the configured read/payload envelope, and no
  smaller than that feed quantum while the live condition cap permits it. QUIC
  or kernel TCP then owns packet pacing and congestion below that product
  record. Lossy, jittery, or queued paths can still shrink the upper condition
  cap; high-rate stable paths repeatedly dispatch bounded quanta until the
  carrier remains fed. Inflight limits and carrier pacing control network
  pressure; the frame quantum controls scheduling preemption and per-byte
  processing cost.
  Receiver-side stream input queues and path command queues follow the same
  rule. Their depth is sized from the relevant byte window divided by the
  actual maximum product-frame payload used by the attachment plus one
  priority-headroom slot per non-throughput lane, not from the TCP relay chunk
  ceiling or a fixed slot clamp. A QUIC carrier may internally packetize one
  product frame into many QUIC packets, but the mptunnel path command queue is
  still sized by product-frame bytes and sender-service preemption points, not
  by a custom UDP packet count. This preserves byte-bounded memory while
  preventing the carrier input loop or relay sender task from blocking behind an
  artificially TCP-sized frame count.
* The 10s heartbeat interval and 30s timeout avoid noisy idle traffic while
  still detecting silent TCP path death fast enough for Auto to shift new work
  before users experience long stalls. UDP paths use QUIC connection state,
  QUIC loss/PTO progress, and product ACK/repair evidence for finer-grained
  data-plane recovery.

### 5.4 Parameter Audit and Adaptation Status

This section is a standing audit of numeric parameters used by protocol version
1 and the current implementation. It exists because hidden constants are a
transport-design risk: they can silently turn an adaptive protocol into a
benchmark-tuned product. A new production parameter, default, clamp, timer,
queue size, or scheduler weight MUST be added to this section when introduced.
A value in this table is not automatically justified merely because it is
documented.

The "common design" column identifies whether mature multipath or UDP
transports use the same kind of mechanism. It does not mean they use the exact
same number. MPTCP commonly uses subflows, data-sequence mapping, receive
reordering, reinjection, and path managers. QUIC commonly uses streams, ACK
ranges, flow control, packet loss recovery, PMTU limits, PTO, pacing, congestion
control, and path validation. Multipath QUIC work commonly adds per-path IDs,
per-path ACK state, path validation, and scheduling, but leaves scheduling
policy to implementations. BBR-style controllers commonly use delivery-rate,
RTT, inflight, loss, pacing-rate, and application-limited evidence. mptunnel
uses those mechanism classes, but most exact thresholds and weights below are
mptunnel policy choices and MUST be treated as suspect until diagnostics and
production evidence justify them.

| Parameter or family | Current value or formula | Design source and exact origin | Performance risk | Final handling |
| --- | --- | --- | --- | --- |
| Product frame limit | `max_frame_bytes = 1,048,576` | Mechanism is common bounded-frame design; exact value is mptunnel policy | Can waste CPU if too small or memory if too large | Keep as configurable safety envelope; not a scheduler quantum |
| Product payload limit | `max_payload_bytes = 1,048,512`; config omission derives it from frame size | Mechanism is common; exact default is derived mptunnel envelope | Can force excessive records if set too low | Keep as configurable decode/allocation envelope only |
| Product ACK ranges | `max_ack_ranges = 256` | Sparse ACK/SACK/QUIC ACK ranges are common; exact cap is mptunnel policy | Too low can hide holes under severe reorder/loss; too high makes ACKs bulky | Keep as encoding cap; ACK emission cadence remains adaptive |
| Path count | `max_paths = 64` | Finite path/subflow state is common in MPTCP/MPQUIC; exact cap is mptunnel policy | Can cap unusual path groups, not normal performance | Keep as registry cap; not a target path count |
| Stream count | `max_streams = 65,536` | QUIC-style stream registries are common; exact cap is mptunnel policy | Can cap very large fan-out; otherwise not hot path | Keep as non-preallocated registry cap |
| QUIC bidirectional stream count | `max_quic_concurrent_bidi_streams = max_streams` by default | QUIC `MAX_STREAMS` is a stream-count resource separate from byte flow-control windows | A hidden low cap blocks proxy/TUN fan-out even when byte windows are large | Keep as QUIC-scoped configurable cap; never derive it from receive-window ratios |
| Stream window | `max_stream_window_bytes = 64 MiB` | Flow-control windows are common in QUIC and MPTCP; exact envelope is mptunnel policy | Can limit high-BDP paths if below real BDP | Keep as configurable receive/flow-control envelope; credit release is adaptive |
| Repair cache | `max_repair_bytes = 64 MiB` | MPTCP-style reinjection requires retained unacked data; exact envelope is mptunnel policy | Can limit repair on lossy/heterogeneous paths | Keep as repair envelope; repair choice and spending are adaptive |
| Reorder budget | `max_reorder_bytes = 64 MiB` | Multipath byte streams need receive-hole bounds; exact envelope is mptunnel policy | Too low rejects useful paths; too high masks harmful striping | Keep as reorder envelope; admission uses live debt |
| Datagram queue | `max_datagram_queue_bytes = 16 MiB` | Datagram transports commonly bound queues; exact envelope is mptunnel policy | Too low drops bursts; too high hurts latency/memory | Keep as burst envelope; path choice and draining are adaptive |
| Path/product flight ceiling | `max_path_flight_bytes = 64 MiB`; omitted config derives it from repair capacity | Sender inflight/window concepts are common; exact envelope is mptunnel/operator policy | Can limit high-BDP transfers if below required flight | Keep as upper resource envelope; actual flight is BDP/queue/loss/carrier adaptive |
| Reliable relay read ceiling | `max_reliable_relay_chunk_bytes = 512 KiB` | Large reads are common user-space amortization; exact ceiling is mptunnel policy | Bad only if treated as indivisible send/AEAD quantum | Keep as read-buffer ceiling; sender service splits into adaptive preemptible quanta |
| TCP idle heartbeat | 10s interval, 30s timeout | Idle keepalive/liveness is common; exact timers are mptunnel policy | Would violate failover target if used for active data | Keep for idle liveness only; active failover uses data-plane stall/PTO/repair evidence |
| Path probe timer | 10s interval, 2s timeout | Path validation is common in MPTCP/MPQUIC; exact timers are mptunnel policy | Can delay idle-path discovery; must not gate active recovery | Keep as idle path-manager default; active path recovery is adaptive |
| Extra traffic hint | `extra_traffic_hint_percent = 5` default; 100/200 allowed | Reinjection is common in MPTCP/MPQUIC; numeric hint is mptunnel/operator policy | Bad if treated as product-data throttle, per-event refresh, or blind duplication allowance | Keep as hard optional-work budget; response sender spends repair traffic only with evidence; path proof remains bounded by validation fan-out |
| Security freshness | `auth_freshness_window_seconds = 300` | Replay freshness windows are common security controls; exact window is mptunnel policy | Affects clock-skew/replay tolerance, not data-plane rate | Keep as security policy; not data-plane adaptive |
| Cipher default | AES-256-GCM default; ChaCha20-Poly1305 optional | AEAD is mandatory; AES-GCM default follows modern hardware acceleration practice | CPU can matter on CPUs without AES acceleration | Keep as operator choice; no plaintext unless explicit |
| QUIC transport envelope | stream receive window = stream window; receive window = stream + repair + reorder + datagram + flight; send window >= path-flight/read ceiling; bidirectional stream count = QUIC-scoped stream cap | QUIC flow-control/congestion/MAX_STREAMS split is common; mapping is mptunnel policy | Can cap QUIC if mapped envelope or stream count is too small | Keep resource mapping; QUIC BBR pacing/congestion remains carrier-owned; stream count is independent from byte windows |
| QUIC congestion controller | Quinn BBR by default | Model-based BBR fits endpoint-only proxy/tunnel operation where no accurate per-direction path rate is configured; fixed-rate Brutal-like sending is only safe with explicit accurate bandwidth configuration | BBR is not a substitute for product no-worse scheduling; fixed-rate modes can overload weak/shared paths if guessed | Keep as carrier-owned congestion control; product scheduling consumes metrics but does not replace it; any Brutal-like configured-rate mode must be explicit |
| QUIC datagram MTU model | Startup 1200 byte payload; lower 512 and upper 65,000 path-spec bounds | 1200-byte UDP support is a QUIC requirement; mptunnel sets lower/upper guardrails | Low MTU can increase fragmentation/overhead | Keep startup safety plus path MTU observation/probing |
| TUN defaults | IPv4 `10.88.0.1/24`, MTU 1500, DNS TTL 5s | Local-interface defaults are common deployment choices; exact values are mptunnel examples | MTU/TTL can affect TUN behavior but not sender scheduling | Keep as operator defaults, scoped to TUN |
| Outbound DNS timeout | 5s default | Resolver timeouts are common control-plane safety; exact value is mptunnel policy | Slow resolvers may fail resolution; not hot path after resolve | Keep per outbound, not global data-plane behavior |
| Outbound target/proxy connect timeout | 10s default, scoped to each egress outbound or routing member | Connect setup deadlines are common control-plane safety; exact value is mptunnel policy | Too low can fail slow upstreams; too high can delay connect-time fallback | Keep per egress outbound/member; MPP path setup uses path-probe timeout instead; neither value participates in data-plane scheduling |
| SOCKS5 UDP/TUN idle TTLs | SOCKS5 UDP TTL 30s, TUN UDP flow idle 60s | NAT-style UDP state expiry is common; exact TTLs are mptunnel policy | Too short/long affects idle UDP associations | Keep as flow expiry policy; not a throughput cap |
| Management API bounds | request 64 KiB, trend 300 samples, sample interval 1s | Control-plane bounding is common; exact values are mptunnel policy | Can limit observability resolution, not packet throughput | Keep as low-overhead management-plane bounds |

The implementation also contains standard transport constants and adaptive
policy formulas that are not primary operator knobs. They are allowed only when
their origin is explicit and they do not become hidden modes.

| Parameter or family | Current value or formula | Design source and exact origin | Performance risk | Final handling |
| --- | --- | --- | --- | --- |
| Standard packet floor | `TRANSPORT_MSS_BYTES = 1460` | Portable Ethernet TCP MSS floor used only as a lower-bound packet quantum | Jumbo/offload paths may support more, but this does not cap high-rate quantum | Keep as floor below adaptive BDP/BBR sizing |
| QUIC initial window seed | `PATH_OPEN_SCORE_BYTES = 10 * MSS` | QUIC RFC 9002 initial congestion-window packet-count shape | Too small if reused as bulk cap | Keep only as startup/evidence seed and minimum useful path-open score, never as sustained bulk cap |
| QUIC timer granularity | 1 ms | QUIC RFC 9002 timer granularity | Too coarse would delay feedback; too fine would waste wakeups | Keep as standard timing floor |
| QUIC initial RTT seed | 333 ms | QUIC RFC 9002 initial RTT before a sample exists | Wrong if treated as measured RTT after live data | Keep only before live RTT evidence |
| QUIC max ACK delay input | 25 ms | QUIC default max ACK delay shape | Wrong if used as arbitrary retry sleep | Keep only inside PTO/RTT-derived formulas |
| QUIC persistent congestion threshold | 3 PTOs | QUIC RFC 9002 persistent congestion shape | Too low causes false failure; too high delays recovery | Keep for PTO/failure backoff decisions |
| BBR send quantum interval | 1 ms | BBR send-quantum reasoning | Too large bursts; too small burns CPU | Keep as standard quantum interval feeding adaptive bytes |
| BBR maximum send quantum | 64 KiB | BBR send-quantum guidance | Too small can cap throughput only if carrier cannot drain repeated quanta | Keep as preemptible service quantum cap; writer drains repeated admitted quanta |
| BBR minimum pipe cwnd | 4 * MSS | BBR MinPipeCwnd shape | Too small can stall startup; too large queues idle traffic | Keep as startup/inflight floor |
| BBR cwnd gain | 2.0 * BDP | BBR inflight/cwnd gain shape | Too low underfeeds; too high queues | Keep as named gain for fallback inflight, path scoring, and bulk admission |
| EWMA smoothing | RTT/loss `7/8 old + 1/8 new`; delivery/demand `3/4 old + 1/4 new`; upward QUIC-derived product delivery sample favors fresh sample | TCP/QUIC-style smoothed RTT and rate estimation; exact demand smoothing is mptunnel policy | Smoothing can lag sudden changes | Keep because it is measurement-based; diagnostics must expose raw and smoothed values |
| Path scoring | ETA = RTT/2 + queued/product/inflight drain time + payload time + jitter + adaptive loss/confidence/state penalties | ECF/BLEST-style completion estimate with BBR/QUIC evidence | Wrong inputs admit harmful paths or reject useful capacity | Keep adaptive formula; diagnostics must expose all inputs and rejection reasons |
| Scheduler state penalties | suspect/backup/expensive/TCP reorder/loss/confidence/shared-bottleneck penalties derive from PTO, payload transmit time, path BDP, RTT variance, jitter, loss, and queue drain time | QUIC PTO and BBR drain-time model | Hidden millisecond weights previously risked benchmark tuning | Fixed ms weights are removed; suspect bulk pays persistent-congestion PTO debt while latency/control/realtime may still validate a suspect low-latency path |
| Tail and duplication admission | Tail threshold derives from latency-path BDP; duplicate slack derives from jitter or one initial-window fraction of PTO; duplication is only control/realtime and must fit transmit cost | MPTCP/MPQUIC reinjection with QUIC/BBR timing evidence | Blind duplication wastes capacity; no duplication hurts recovery | Keep adaptive and gated by evidence/extra-traffic hint; tail avoidance MUST NOT bypass path capability flags such as no-bulk |
| Lane priority order | control, realtime datagram, latency, throughput, background | ACK/control protection is common in QUIC-style schedulers; taxonomy is mptunnel product policy | Wrong implementation can starve bulk or control | Keep as fixed priority invariant with dynamic queues |
| DRR lane/flow quanta | Deficit charge equals actual sender-service packet quantum | DRR/fair queuing is common | Fixed byte quanta previously underfed high-rate carriers | Keep adaptive charge based on actual queued frame size |
| Service frame quantum | Latency/control use small BBR-style quanta; reliable bulk feeds TCP/QUIC with the bounded 64 KiB BBR send quantum under the configured read/payload envelope and live condition cap | BBR send-quantum model applied at the product-record boundary, with TCP/QUIC packet pacing below | Tiny quanta cap throughput; giant quanta harm latency | Keep adaptive; high-rate stable paths repeatedly dispatch bounded quanta while control/repair/latency remain preemptive |
| Inflight target | BDP * BBR cwnd gain, send quantum, and MinPipeCwnd under configured flight envelope; latency/realtime lanes use the smaller preemptive target | BBR inflight model and product lane priority | Too low underfeeds; too high queues | Keep adaptive from live BDP/queue/loss/carrier evidence |
| Stability/backlog factors | Shrink by loss/jitter/queue/backlog relative to BDP with floor derived from MinPipeCwnd or send quantum divided by BDP | Congestion-sensitive adaptation; floor is no longer a fixed fraction | Over-shrinking can create low-rate loops | Keep adaptive; diagnostics must show shrink reason |
| Auto bulk classification | EWMA/rate/byte/idle-gap evidence promotes/demotes demand using service quantum, BDP, and PTO | Product-specific but measurement-based | Late/early promotion affects latency/throughput | Keep adaptive; no user-visible mode tag or port rule |
| ACK progress cadence | Product `STREAM_ACK` uses BDP/2 when measured, otherwise the bounded bulk service quantum, under the repair/flow-control resource ceiling; `STREAM_MAX_DATA` uses larger flow-control hysteresis | SACK/QUIC ACK-range practice with MPTCP-style product repair ownership release | Sparse ACKs fill repair cache and stall senders; chatty ACKs waste reverse bandwidth | Keep dynamic from receive progress and separate from MAX_DATA cadence |
| MAX_DATA cadence | Credit update after a window/chunk-derived threshold | QUIC flow-control update logic | Coarse credit can stall high-BDP streams | Keep adaptive from window/chunk |
| Active stall and retry timing | Derived from QUIC PTO, observed RTT/rttvar, lane state, TTL, and persistent congestion threshold | QUIC PTO/recovery model | Fixed sleeps underfeed high-rate carriers or delay failover | Fixed retry/stall constants are removed from data-plane policy |
| Path failure cooldown | Derived from PTO and consecutive failures, capped by QUIC persistent congestion threshold | QUIC persistent-congestion backoff applied to path reuse | Fixed cooldown can hide recovered paths | Fixed 5s cooldown is removed |
| UDP target/datagram path model | UDP/QUIC response deadline derives from PTO, RTT variance, TTL, loss, and persistent congestion threshold; pacing floor is one observed datagram payload per PTO | QUIC PTO plus UDP application congestion-control guidance | TCP-underlay datagrams can still HOL-block | Removed fixed 50ms/1s/250ms/8*SRTT/64Kbps clamps. A product datagram ID is emitted once on the selected carrier and expires on absent target response instead of being replayed on another carrier |
| QUIC metric sampler | Active polling uses SRTT/2 with timer granularity; app-limited/idle polling uses PTO; confidence derives from ACK-derived sample count | Carrier app-limited filtering and QUIC RTT/PTO evidence | Stale samples mislead scheduler | Removed fixed 10..250ms sampler clamp; keep evidence provenance |
| Path/stream queue depth | Byte envelope divided by actual service/frame payload plus priority-headroom slots, where headroom is one slot per non-throughput lane | Resource envelope plus lane model | Fixed slot caps underfeed high-rate carriers | Removed 1024/4096-style caps from data-plane queues |
| Bulk admission | Product flight/queue <= BDP/resource envelope for active owners; before product-progress evidence exists, active Service flight on any carrier uses bounded startup-feedback credit, not the full resource envelope, not the geometric Service horizon, and not a tiny carrier-cwnd or one-quantum gate; after product-progress evidence exists, active Service flight is capped by meaningful app-limited ACK feedback or non-app-limited product-progress BDP using queue-resistant RTT, not queue-inflated SRTT or carrier pacing; latency pressure may shrink or preempt Service bursts but MUST NOT switch active Service ownership back to a carrier-pacing-derived product backlog limit; QUIC active owners additionally count carrier-accepted-but-unacked product data as queue debt; carrier inflight/queue/RTT/loss/pacing shape ETA and extra-path admission; cross-underlay and debt-bearing same-stream sends use completion horizon while clear-frontier same-underlay sends use writer credit and reorder budget | MPTCP/MPQUIC simultaneous-path scheduling plus ECF/BLEST HOL avoidance, with QUIC packet congestion owned below the product stream | Highest-risk throughput governor | Keep dynamic invariant; diagnostics must explain each rejection; do not treat QUIC write-buffer acceptance as delivered capacity |
| Validation traffic | Probe/control traffic only; repair data only after explicit gap/failover evidence; no unique future bytes when admitted ordinary path exists | MPTCP reinjection and MPQUIC path validation | Violations create HOL debt | Keep invariant, not heuristic |
| Replay/security cache sizes | closed-stream cache and PATH_JOIN replay cache derive from stream/path scale with bounded caps | Security/control-plane state bounding | Not a throughput cap unless accidentally used for data-plane queues | Keep as security/resource envelope, not scheduler input |
| Header/parser safety | HTTP CONNECT request/response 64 KiB; CONNECT-UDP payload 65,527; SOCKS5 UDP packet 65,535; target host 255 | Parser/protocol bounds are common | These bound protocol parsing and packet buffers, not scheduling | Keep as scoped parser/packet envelopes, not scheduler input |

The following items were found during the parameter audit and are resolved in
protocol version 1. They are listed so stale implementations do not reintroduce
them.

| Item | Resolution |
| --- | --- |
| `max_udp_replay_window_packets` and `udp_replay_window_packets_for_inflight` | Removed. Production UDP is QUIC-only, and mptunnel does not expose or compute a custom UDP replay-window parameter. |
| `max_tcp_path_inflight_bytes` | Renamed to `max_path_flight_bytes` because the envelope applies to product path flight and QUIC send-window resource mapping, not only TCP. |
| `UDP_BBR_PACING_GAIN` / `UDP_DATAGRAM_MODEL_PACING_GAIN` | Removed. UDP target/datagram pacing now uses measured delivery rate, loss backoff, one-payload-per-PTO floor, and TTL/PTO-derived freshness state. QUIC packet pacing remains owned by the QUIC library. |
| Fixed scheduler millisecond weights | Removed. Expensive, suspect, backup, TCP-reorder, loss, confidence, shared-bottleneck, tail, and duplicate-admission decisions now derive from PTO, BDP, jitter, loss, transmit time, queue debt, and confidence. |
| Fixed tail/duplication byte thresholds | Removed. Tail avoidance derives from latency-path BDP, and duplication slack derives from jitter or a QUIC initial-window fraction of PTO. |
| Fixed lane BDP fractions | Removed from production data-plane policy. Service quanta now use BBR send quantum, MinPipeCwnd, BBR cwnd gain, and condition factors derived from queue/loss/jitter/backlog. |
| Fixed stability/backlog floors such as 0.125/0.25 | Removed. Floors now come from MinPipeCwnd or send quantum divided by current BDP, so low-BDP/idle paths stay efficient while high-BDP paths are not arbitrarily capped. |
| Fixed path-open 4 KiB score sample | Removed. Startup scoring uses QUIC initial-window packet-count shape, `10 * MSS`, not an unrelated small byte count. |
| Fixed 5s path failure cooldown | Removed. Cooldown is derived from PTO and consecutive failure count under the QUIC persistent-congestion threshold. |
| UDP target/datagram clamps `50ms..1s`, `250ms`, `8*SRTT`, and `64Kbps` | Removed. Datagram response deadlines, suppression, and path-open timing derive from PTO, RTT variance, TTL, loss, and persistent congestion threshold. |
| QUIC sampler clamp `10..250ms` | Removed. Active sampling uses SRTT/2 with timer granularity; idle/app-limited sampling uses PTO. |
| TCP/session/TUN queue slot caps such as `+4`, `1024`, and `4096` where byte envelopes already exist | Removed from data-plane queues. Queue depth is byte envelope divided by actual payload quantum plus lane-derived priority headroom. Security/control-plane cache caps remain separate. |
| Hard-coded egress and MPP path connect timeout call sites | Removed. Egress target/proxy connect timeout is owned by the selected outbound or routing member. MPP TCP path connect timeout is owned by the MPP path group probe/open timeout. Transport-layer defaults remain only as library fallbacks and tests. |
| Fixed closed-stream and `PATH_JOIN` replay cache clamps | Removed. Closed-stream retention scales from configured stream count without preallocation; `PATH_JOIN` nonce replay retention scales from configured stream count and the QUIC persistent-congestion threshold. |
| Fixed datagram retry exponent cap | Removed. Product datagrams are not retransmitted by mptunnel after emission; PTO/TTL-derived budgets bound only response waiting, carrier setup, and path suppression. |
| TCP/QUIC-underlay datagram product retransmit/reopen loop | Removed. The carrier owns packet/stream retransmission below mptunnel; mptunnel MUST NOT duplicate a datagram ID or open a replacement carrier only because a UDP target response timed out. Real setup, encryption, authentication, and session errors before useful product expiry remain retryable carrier failures. |
| Config example numbers | Annotated as examples and recommended ranges. Operators and tests MUST read defaults from the config model and this RFC, not from commented examples alone. |
| Diagnostic tooling constants | Remain tooling-only. Sample intervals, benchmark durations, and failure thresholds are not production protocol parameters and MUST NOT ship as release-bundle behavior unless explicitly part of the management API contract. |

The current highest-risk parameters for throughput are the path/product flight
envelope, QUIC send-window mapping, adaptive service-frame quantum formula,
bulk admission formula, and QUIC metric sampling/provenance. The current
highest-risk parameters for latency and failover are ACK cadence, lane priority,
active stall bounds, path cooldown, validation proof shape, and repair quantum.
Any performance investigation MUST report whether a bottleneck is caused by a
static envelope, a fixed policy weight, missing measurement evidence, or an
implementation bug that bypasses the adaptive sender-service model.

## 6. Path Specifications and Capabilities

Client paths and server bind paths use URI-like values:

```
tcp://host:port
udp://host:port
tcp://host:port?srtt-ms=50&rate-mbps=500&low-latency
udp://[2001:db8::1]:443?bulk&mtu=1200
```

The scheme selects the underlay protocol. Host parsing MUST support IPv4, IPv6
with brackets, and domain names. Port zero MUST be rejected.

Supported path metadata query parameters are:

* RTT hints: `srtt-ms`, `rtt-ms`
* Jitter hint: `jitter-ms`
* Rate hints: `rate-bps`, `rate-kbps`, `rate-mbps`, `rate=unknown`,
  `rate=unlimited`
* MTU hints: `mtu`, `mtu-bytes`, `payload-mtu`
* Capabilities: `backup`, `expensive`, `low-latency`, `bulk-allowed`, `bulk`,
  `no-bulk`, `probe-only`, `no-udp`

Boolean values MAY be explicit (`true`, `false`, `1`, `0`, `yes`, `no`, `on`,
`off`) or bare; a bare boolean means true.

`udp://` paths always use the QUIC carrier. Protocol version 1 has no UDP
engine selector; `engine=` is an unknown path query parameter and MUST be
rejected. The QUIC client MUST NOT emit a fixed product-identifying SNI value.
The QUIC server certificate and client trust anchor MUST be derived from the
configured mptunnel shared secret, and the client MUST reject any server
certificate that does not match that derived identity. This binds QUIC
confidentiality to the same operator secret used by the product `SESSION_AUTH`
and `PATH_JOIN` transcripts, so an active relay cannot terminate QUIC and
inspect product frames without knowing the shared secret. Product
authentication remains mandatory after the QUIC handshake; it provides
per-session and per-path freshness, replay resistance, and authorization.

The QUIC production engine MUST configure its QUIC transport envelope from the
same resource model used by the product stream layer. The QUIC per-stream
receive window is the configured mptunnel stream window. The QUIC connection
receive window is derived from the configured stream, repair, reorder, datagram
queue, and path-inflight byte budgets so high-BDP UDP receive-side correctness
is not silently constrained by generic library defaults. The local QUIC send
window is not the same aggregate receive envelope. It is derived from the
configured per-path inflight budget and reliable relay quantum, then QUIC's own
congestion controller and pacing decide packet emission inside that sender
envelope. This preserves the ownership split used by MPTCP and MPQUIC:
product scheduling keeps the stream fed, while the carrier owns packet flight
and pacing. The admitted concurrent QUIC bidirectional stream count is derived
from the QUIC-scoped stream cap and bounded by the product stream registry cap;
it MUST NOT be derived from byte receive-window ratios. QUIC unidirectional
streams are not used by protocol version 1. The production QUIC engine SHOULD
use a model-based congestion controller when no explicit per-direction rate is
configured; this implementation uses Quinn BBR. A fixed-rate Brutal-like mode is
valid only when the operator supplies an accurate target rate for that direction.
Product scheduling consumes carrier metrics but must not guess a fixed packet
send rate.

Hints seed the path model before measurements exist. They MUST NOT permanently
override live observations. Auto scheduling MUST correct stale hints from health
and delivery feedback.

URI-like path specifications let operators describe bind addresses, underlay
protocol, and initial hints without writing a policy language. The format is
compact, familiar, shell-friendly, and extensible. Hints are advisory because
configured RTT/rate values often become stale after roaming, congestion, cloud
routing changes, Wi-Fi changes, or QoS. Live measurements are therefore
authoritative once enough confidence exists.

## 7. Cryptographic Material and Authentication

### 7.1 Shared Secret

The shared secret MUST be either:

* a UUID string, or
* at least 32 bytes of high-entropy text.

The master secret is:

```
SHA256("mptunnel shared secret master v1" || kind || value)
```

where `kind` is `uuid` for UUID input and `raw` for raw high-entropy input.
The result is 32 bytes.

UUID input is accepted for operational ergonomics, similar to common proxy
deployments that use UUID-shaped credentials. The protocol still derives a fixed
32-byte master secret and requires high-entropy raw text as the preferred form
for long-lived deployments. The domain-separated hash prevents the same user
secret from being reused directly as an AEAD key.

### 7.2 AEAD Suites

The following AEAD suites are defined:

| Name | Key bytes | Nonce bytes | Tag bytes |
| --- | ---: | ---: | ---: |
| aes-256-gcm | 32 | 12 | 16 |
| chacha20-poly1305 | 32 | 12 | 16 |

The `cipher_suite_context` used by key derivation is the selected suite name
encoded as ASCII: `aes-256-gcm` or `chacha20-poly1305`.

### 7.3 TCP Underlay Key Derivation

TCP encrypted framed streams derive:

```
SHA256("mptunnel encrypted framed v1" ||
       cipher_suite_context ||
       master_secret)
```

TCP underlay encryption intentionally avoids exposing TLS metadata such as SNI
in the internal transport. The framed AEAD envelope gives confidentiality,
integrity, replay detection by counter, and deterministic record boundaries
without relying on external TLS behavior.

### 7.4 QUIC Carrier Keying

UDP carrier packet protection is QUIC packet protection. QUIC TLS derives the
packet-protection keys and nonces according to the QUIC and TLS specifications.
mptunnel binds that QUIC identity to the operator secret by deriving the server
certificate and client trust anchor from the shared secret, then requiring the
normal product `SESSION_AUTH` and `PATH_JOIN` transcript after the QUIC
handshake. mptunnel MUST NOT define or negotiate a separate UDP packet AEAD key
below QUIC.

### 7.5 TCP Nonce Construction

TCP framed encryption uses a 12-byte nonce:

```
byte 0      direction
bytes 1-3   zero
bytes 4-11  counter_or_packet_number_be64
```

Direction values are:

* 1: client to server
* 2: server to client

Counters MUST NOT repeat for the same key and direction.

The direction byte and monotonic counter make nonce uniqueness easy to audit.
Direction separation prevents a record emitted by one peer from being valid as
a replay in the opposite direction under the same session material. QUIC packet
nonces are owned by QUIC and are not part of the `MPTE` TCP framed envelope.

### 7.6 Session Authentication

`SESSION_AUTH` carries `session_id`, 16-byte nonce, issue time, and HMAC-SHA256
tag. The tag is:

```
exporter_secret =
  SHA256("mptunnel auth exporter v1" ||
         cipher_suite_context ||
         master_secret)

HMAC-SHA256(exporter_secret,
  "mptunnel session auth v1" ||
  session_id_be64 ||
  nonce_16 ||
  issued_at_unix_secs_be64)
```

Receivers MUST reject tags whose issue time differs from local time by more than
the configured freshness window. A zero freshness window MUST reject all
authentication frames.

Time-bounded authentication limits replay of captured setup traffic while
keeping startup single-round-trip and usable immediately after process start.
The freshness window is configurable because containers, embedded systems, and
VPS images can have different clock quality.

### 7.7 Path Join Authentication

`PATH_JOIN` carries session ID, path ID, underlay, nonce, issue time,
capabilities, and HMAC tag:

```
HMAC-SHA256(exporter_secret,
  "mptunnel path join v1" ||
  session_id_be64 ||
  path_id_be16 ||
  underlay_u8 ||
  nonce_16 ||
  issued_at_unix_secs_be64 ||
  path_capabilities_be16)
```

Servers MUST maintain a bounded replay cache for recent path-join nonces and
MUST reject replayed setup traffic within the freshness window.

## 8. Product Frame Encoding

All product frame integers are network byte order. Strings are UTF-8.

Each product frame has:

```
0..4   magic = "MPTF"
4      version = 1
5      frame kind
6..10  payload length u32
10..   payload
```

The frame header length is 10 bytes. The receiver MUST validate magic, version,
known kind, length, maximum frame bytes, and absence of trailing bytes.

Product frames use a short magic string and explicit version so misrouted data,
stale builds, and incompatible experiments fail early. The payload length is
fixed-width to make validation independent of frame kind. Product frames do not
contain carrier-specific sequence numbers; path packet numbers and stream
offsets live in their own layers.

### 8.1 Primitive Encodings

* `u8`, `u16`, `u32`, `u64`: unsigned big-endian integers.
* `bytes32`: 32 bytes.
* `nonce16`: 16 bytes.
* `payload`: `u32 length` followed by bytes.
* `domain target`: `u8 kind=1`, `u16 host_length`, UTF-8 host, `u16 port`.
* `IPv4 socket`: `u8 kind=2`, 4-byte IPv4 address, `u16 port`.
* `IPv6 socket`: `u8 kind=3`, 16-byte IPv6 address, `u16 port`.
* `IP address only`: `u8 kind=2/3`, address bytes without port.
* `offset range`: `u64 start`, `u64 end`, where `start < end`.
* `offset range vector`: `u16 count` followed by ranges.

Ports MUST be nonzero.

### 8.2 Enum Encodings

Underlay:

* 1: TCP
* 2: UDP

Ingress:

* 1: SOCKS5
* 2: HTTP CONNECT
* 3: TUN TCP
* 4: TUN UDP

Path status:

* 1: Active
* 2: Suspect
* 3: Draining
* 4: Failed

Stream open role:

* 1: Active
* 2: Repair
* 3: Validation

Close reason:

* 0: Normal
* 1: ProtocolError
* 2: AuthenticationFailed
* 3: PolicyRejected

Reset reason:

* 1: Refused
* 2: TimedOut
* 3: RemoteClosed
* 4: PolicyRejected

Rate hint:

* 0: Unknown
* 1: Unlimited
* 2: BitsPerSecond followed by `u64 bps`

Stream flags:

* bit 0: FIN
* bit 1: early data
* bits 2..7: reserved and MUST be zero

Outbound policy:

* 0: Direct
* 1: BindSourceIp followed by IP address only
* 2: Socks5 followed by socket address
* 3: HttpConnect followed by socket address

Path capabilities are a `u16` bitset:

* bit 0x0001: backup
* bit 0x0002: expensive
* bit 0x0004: low_latency
* bit 0x0008: bulk_allowed
* bit 0x0010: probe_only
* bit 0x0020: no_udp

Unknown capability bits MUST be rejected.

Big-endian integer fields match network byte order and keep wire dumps readable.
Explicit target variants avoid ambiguous string parsing for IPv4, IPv6, and
domain targets. Unknown enum and capability values are rejected because the
project intentionally does not preserve silent compatibility with experimental
wire formats.

## 9. Product Frame Registry

The frame kind registry is:

| Kind | Name | Payload |
| ---: | --- | --- |
| 1 | SESSION_HELLO | `session_id:u64` |
| 2 | SESSION_READY | empty |
| 3 | SESSION_CLOSE | `reason:u8` |
| 4 | PATH_JOIN | session ID, path ID, underlay, nonce, issue time, capabilities, auth tag |
| 5 | PATH_CHALLENGE | `path_id:u16`, `nonce:u64` |
| 6 | PATH_RESPONSE | `path_id:u16`, `nonce:u64` |
| 7 | OPEN_STREAM | stream ID, target, ingress, outbound, stream demand hint, role |
| 8 | STREAM_DATA | stream ID, offset, flags, payload |
| 9 | STREAM_ACK | stream ID, complete flag, offset ranges |
| 10 | STREAM_MAX_DATA | stream ID, max offset |
| 11 | STREAM_RESET | stream ID, reset reason |
| 12 | OPEN_DGRAM_FLOW | flow ID, target, ingress, outbound |
| 13 | DGRAM_DATA | flow ID, datagram ID, TTL milliseconds, payload |
| 14 | DGRAM_CLOSE | flow ID |
| 15 | MAX_CONNECTION_DATA | max bytes |
| 16 | PING | nonce |
| 17 | PONG | nonce |
| 18 | SESSION_AUTH | session ID, nonce, issue time, auth tag |
| 19 | PATH_JOIN_OK | path ID, nonce, auth tag |
| 20 | PATH_STATUS | path ID, status, capabilities |
| 21 | PATH_DRAIN | path ID |
| 22 | PATH_CLOSE | path ID, close reason |
| 23 | DGRAM_FEEDBACK | flow ID, received datagram ID ranges |
| 24 | PATH_METRICS | path metrics structure |
| 25 | RX_RATE_HINT | path ID, rate hint |
| 27 | STREAM_FIN | stream ID, final offset |
| 28 | PATH_MTU_PROBE | path ID, probe ID, payload |
| 29 | PATH_MTU_ACK | path ID, probe ID, payload byte count |
| 30 | STREAM_DETACH | stream ID |

Kind 26 is unassigned in version 1 and MUST be rejected.

Session, path, stream, datagram, metrics, and control frames share one registry
so carriers can remain generic. The registry keeps control frames small because
they must bypass bulk queues. Kind 26 remains unassigned because version 1 does
not need compatibility padding; rejection of gaps is a simple way to catch
corrupt or stale traffic.

### 9.1 Stream Demand Hint

`OPEN_STREAM` carries:

```
observed_bytes:u64
repair_bytes:u64
latency_weight_ppm:u32
throughput_weight_ppm:u32
realtime_weight_ppm:u32
```

Weights are parts per million. The maximum logical value is 1,000,000. The peer
uses the greatest applicable weight to infer a flow lane, but MUST still adapt
from local observations.

The demand hint uses ppm weights instead of user-visible class names because the
product needs continuous adaptation, not a small enum that hardcodes policy. A
receiver can combine peer demand with local measurements: for example, a
download may be throughput-heavy at the server sender while the client still
protects local interactive ingress.

### 9.2 Path Metrics

`PATH_METRICS` carries:

```
path_id:u16
underlay:u8
direction:u8
metric_epoch:u64
metric_age_us:u32
min_rtt_us:u32
srtt_us:u32
rttvar_us:u32
jitter_us:u32
delivery_rate_bps:u64
pacing_rate_bps:u64
loss_ppm:u32
ecn_ppm:u32
loss_observed:u8
ecn_observed:u8
bytes_in_flight:u64
queue_bytes:u64
inflight_limit_bytes:u64
inflight_hi_bytes:u64
confidence_ppm:u32
app_limited:u8
has_ack_derived_data_sample:u8
data_sample_count:u32
data_sample_bytes:u64
```

`loss_observed` and `ecn_observed` distinguish a measured zero from an
unknown value. A sender MUST NOT publish unknown QUIC carrier loss, ECN, flight,
or queue state as a measured zero. Metrics are advisory and MUST NOT bypass
local safety checks.

The fields are the minimum shared model needed for BBR-like and MPTCP-like
decisions. RTT and jitter describe latency risk. Delivery rate estimates useful
bandwidth. Pacing rate, inflight limit, and inflight high watermark describe the
sender-side control envelope. Loss and ECN describe congestion and repair cost.
Bytes in flight and queue bytes prevent a scheduler from choosing a path that
looks fast but is already full. Direction, age, confidence, application-limited
state, and ACK-derived sample fields tell the receiver whether the metrics are
fresh sender evidence or only a hint. Peer metrics are advisory because each
endpoint has different local observations and must remain robust against stale
or malicious peer reports. A response sender MUST NOT promote ordinary bulk
service from peer metrics alone; promotion requires local sender evidence or
stream delivery samples that are not polluted by ordering holes.

When `has_ack_derived_data_sample` is set by the local sender for the current
direction, `confidence_ppm`, `data_sample_count`, and `data_sample_bytes` are
sender-side evidence and SHOULD materially raise the path model confidence. The
count records independent ACK-derived samples; the byte total records how much
application DATA was newly acknowledged by the carrier samples used for the
current delivery model. Bulk-rate promotion MUST require both sample count and
adequate acknowledged byte volume for the path's current modeled flight
envelope. A small probe can prove ACK-data visibility, but it MUST NOT seed a
long-lived bulk-rate model or make the path an ordinary unique-data owner by
itself. A mature sample set with high confidence is not merely a liveness hint.
Peer-provided metrics, successful opens, and control-only traffic remain
low-confidence validation hints unless local delivery or carrier ACK-derived
data samples confirm them. The sender path model MUST also add locally queued
carrier command bytes to `queue_bytes` for all underlays, including TCP, so
hidden path queues cannot be ignored by ECF/BLEST admission.

Implementations MUST keep peer-hint metrics and local-sender metrics in
separate slots. A local path-proof or control-plane observation may improve
local liveness, RTT, and queue evidence, but it MUST NOT erase the peer's
advisory rate/RTT prior before direction-correct bulk-rate evidence exists.
Conversely, a peer hint MUST NOT overwrite local carrier queue, flight,
ACK-derived data, or delivery samples. The sender-service ETA model may combine
local liveness with peer advisory rate for validation/probe ranking, but Service
ownership and Subflow admission require local bulk-rate evidence except for the
current active/lower-frontier Service itself. Configured startup rate hints are
advisory priors and MUST be published as non-app-limited metrics.
An app-limited peer metric that came from proof/control or a tiny sample MUST NOT
seed the response rate prior. Once local ACK-derived DATA has been seen without
enough non-application-limited volume to become bulk-rate evidence, the peer hint
remains only an advisory rate prior; it does not authorize ordered owner data
for that path.

Each endpoint also keeps local lane occupancy for every session path. This
state is not trusted from the peer because it reflects local product work
already admitted to that endpoint's sender service. A path snapshot used for
bulk admission MUST include `active_flows` and
`active_latency_sensitive_flows` from this local ledger. When a bulk or
background stream evaluates a path with active control, latency, or realtime
datagram work while bulk/background work is also present on that path, the
sender MUST reserve adaptive latency headroom as additional queue debt before
reading more source bytes or choosing the next bulk quantum.
The headroom is derived from the same path model used for latency inflight
(`srtt`, delivery or pacing rate, loss, jitter, and queue pressure); it is not
an operator traffic mode and not a fixed product cap. This makes lane
protection part of admission rather than a late path-writer preference.
An all-startup state where every stream is still classified as latency MUST NOT
reserve those startup streams against each other as protected latency work; once
one flow is classified as throughput/background, separately active latency or
realtime flows become protected. This prevents validation/probe admission from
deadlocking while still protecting browsing, SSH-like,
ACK/control, repair, and datagram traffic from already-proven bulk.

## 10. TCP Underlay Transport

TCP underlay carries product frames in an encrypted `MPTE` envelope:

```
0..4    magic = "MPTE"
4       version = 1
5       direction
6..14   counter u64
14..18  ciphertext length u32
18..    ciphertext || tag16
```

The AEAD plaintext is exactly one encoded product frame. The TCP envelope
header is AEAD additional authenticated data. The receiver MUST validate:

* magic is `MPTE`;
* version is 1;
* direction is the expected peer direction;
* counter equals the next expected counter for that direction;
* ciphertext length is at least 16 and does not exceed `max_frame_bytes + 16`;
* AEAD tag verifies;
* decrypted product frame validates.

The sender MUST increment the counter after a successful write. The receiver
MUST increment the expected counter after a successful read. Counter gaps or
replays are fatal to that underlay path.

TCP path sessions maintain independent control, priority, and data queues.
Control and latency-sensitive frames MUST bypass saturated bulk data queues.
Heartbeat `PING`/`PONG` frames are sent on established TCP path sessions using
the configured heartbeat interval and timeout.

TCP underlay is optimized for reachability and compatibility, not for
packet-level recovery. It can cross restrictive networks and upstream TCP
proxies, but it hides packet loss and may amplify head-of-line blocking.
Therefore TCP paths are allowed to carry all product frames, including
best-effort UDP datagrams, while Auto avoids blind bulk striping over multiple
TCP paths unless measurements prove that doing so improves completion time.

## 11. UDP Carrier Transport

The UDP carrier is QUIC. A `udp://` path establishes a QUIC connection and
carries mptunnel product frames inside QUIC bidirectional streams. Protocol
version 1 does not define an alternate UDP engine selector. Configuration or
path strings containing `engine=` MUST be rejected rather than silently mapped
to another carrier. This keeps the product surface honest: every production UDP
optimization targets the QUIC path, and no stale custom UDP carrier can remain
as a hidden runtime branch.

The QUIC wire packet exposes no product magic string, target name, proxy
protocol metadata, or fixed product SNI. The client MUST disable SNI or use an
implementation behavior with the same privacy property. The server certificate
and the client trust anchor are deterministically derived from the configured
mptunnel shared secret. The client MUST reject a server certificate that does
not match this derived identity. Product `SESSION_AUTH` and `PATH_JOIN` remain
mandatory after the QUIC handshake, so QUIC authentication and product session
authentication are separate but bound to the same operator secret.

UDP-backed reliable paths are QUIC carriers: QUIC owns packet numbers, ACKs,
loss recovery, PTO, pacing, congestion control, PMTU, connection IDs, and NAT
rebinding above UDP. They are not preferred merely because their underlay is
UDP; TCP and QUIC carriers compete by live path metrics, lane demand, ordering
debt, and no-worse admission. mptunnel MUST NOT implement a second packet-level
reliable transport above QUIC. Above QUIC, mptunnel owns only product semantics:
reliable stream offsets, stream ACK ranges, FIN/final offsets, repair byte
ranges, datagram IDs, flow control, sender-service lanes, path admission, and
path-model interpretation.

### 11.1 QUIC Connection Profile

Each configured `udp://host:port` path creates one QUIC carrier association
inside an MPP session. The QUIC transport profile MUST be derived from the
mptunnel resource envelope:

* QUIC stream receive window is based on `max_stream_window_bytes`.
* QUIC connection receive window covers stream window, repair budget, reorder
  budget, datagram queue budget, and path-flight budget.
* QUIC send window is at least the configured path-flight budget and the
  reliable relay chunk budget.
* QUIC bidirectional stream concurrency is bounded by the resource envelope and
  configured stream count.
* QUIC datagram buffers, when enabled by the implementation, are bounded by the
  configured datagram queue budget.
* QUIC congestion control SHOULD use the implementation library's mature
  production controller by default; experimental controllers are lab-only until
  repeated shaped and unconstrained rows prove no-worse behavior.

The reason for this profile is the same as Hysteria2 and MPQUIC-style designs:
packet recovery and congestion control belong in the UDP transport, while proxy
and tunnel semantics remain above it. The QUIC controller provides mature ACK,
loss, pacing, and congestion behavior. mptunnel consumes that behavior through
carrier credit and telemetry; it does not duplicate it with a custom packet
number space.

### 11.2 Product Frames over QUIC Streams

A QUIC bidirectional stream carries length-prefixed mptunnel product frames. The
length prefix is four bytes in network byte order followed by one encoded
product frame. The product frame codec and product frame limits are specified in
Sections 8 and 9.

The path handshake is carried on a QUIC bidirectional stream using
`SESSION_HELLO`, `SESSION_AUTH`, and `PATH_JOIN`; the peer replies with
`SESSION_READY` and `PATH_STATUS`. Reliable product streams are opened with
`OPEN_STREAM` on their QUIC carrier stream and then exchange `STREAM_DATA`,
`STREAM_ACK`, `STREAM_MAX_DATA`, `STREAM_FIN`, `STREAM_RESET`, and related
control frames. Datagram flows are opened with `OPEN_DGRAM_FLOW` and exchange
`DATAGRAM_DATA`, `DATAGRAM_FEEDBACK`, and `DATAGRAM_CLOSE` product frames over
the QUIC carrier stream.

A valid decrypted QUIC stream frame whose product stream is unknown, already
closed, or not yet opened by ordered product control MUST NOT be treated as a
carrier-path packet loss. The product layer either associates it with a bounded
orphan/reorder context, drops it as stale product work, or returns a product
reset when the carrier writer is available. Unknown product data MUST NOT create
target sockets or ordinary stream ownership. This preserves transport layering:
QUIC packet delivery is not the same thing as product stream admissibility.

### 11.3 Product Quantum and Writer Feed

mptunnel product `STREAM_DATA` payloads over QUIC are application records, not
UDP datagrams. QUIC decides how those bytes become packets. The sender service
therefore sizes product quanta from product flow-control credit, lane priority,
repair priority, adaptive relay chunking, and the configured product envelope.
The QUIC writer may receive more than one product quantum in a writer drain when
there is already admitted work, but admission and fairness occur above the path
queue. A path command queue is only an emission pipe for already-admitted work.

For QUIC UDP carriers, the sender-service emission gate MUST NOT become a
second congestion controller built from stale product delivery rate, fixed-rate
guesses, or app-limited startup RTT samples. QUIC owns packet congestion control, pacing,
packet flight, loss recovery, and PTO. The product sender instead maintains a
bounded QUIC writer-feed envelope: it may keep enough already-admitted product
quanta queued to avoid app-limiting the QUIC controller, and the envelope is
bounded by the configured product path-flight/repair/reorder/window resources
and by live QUIC carrier debt such as carrier bytes in flight and writer
backlog. The sender still emits preemptible product quanta; this writer-feed
envelope controls how much admitted work may wait at the carrier writer, not the
network packet rate.

TCP framed-stream carriers use a stricter product flight model because kernel
TCP does not expose the same per-product-stream carrier controller to
mptunnel. For TCP, product-side BDP, queue, repair, and active-lead ownership
remain part of the emission gate. For QUIC, a low app-limited product sample
MUST NOT be allowed to starve a writable QUIC carrier whose own congestion
controller has live flight/backlog evidence.

Control, stream ACKs, FIN, RESET, DETACH, tail repair, latency data, and realtime
datagram work MUST be able to interleave ahead of ordinary throughput work at
the sender-service boundary. Bulk product quanta MUST remain preemptible by the
sender service; implementations MUST NOT rely on QUIC stream FIFO order alone to
protect small HTTP, SSH-like, datagram, control, or repair traffic during bulk
transfer.

This design intentionally avoids TCP-over-TCP-style behavior in the UDP path.
The product stream may be reliable, but QUIC owns packet loss recovery below it.
Product repair and stream ACKs exist for multipath stream-offset correctness and
cross-path reinjection; they do not replace QUIC packet ACKs, congestion control,
or PTO.

### 11.4 QUIC Metrics and Scheduling Evidence

The local path model consumes QUIC sender telemetry as carrier evidence. At
minimum, scheduling-visible UDP path snapshots use QUIC RTT, RTT variance,
minimum RTT, congestion window or inflight-high equivalent, pacing rate when
available, real carrier bytes in flight when exposed by the QUIC engine, writer
backlog, byte-counted ACK progress, loss provenance, and application data frame
progress.

Carrier ACK-only progress may update liveness and RTT. It MUST NOT by itself be
used as bulk throughput proof. A UDP path becomes ordinary bulk evidence only
after the sender has observed newly acknowledged carrier bytes from the QUIC
congestion controller or an unpolluted product delivery sample that the
scheduler can attribute to that path and direction. A QUIC ACK-frame count, UDP
datagram transmit byte count, PATH_RESPONSE, or ACK-only/control-only packet
MUST NOT be converted into a delivery-rate sample. Delivery-rate samples are
byte-counted: `sample_rate = newly_acked_bytes * 8 / elapsed`. Application-
limited and pure-control samples MUST NOT reduce the bulk delivery-rate
estimate. Peer `PATH_METRICS` remain validation hints unless freshness,
direction, confidence, and provenance make them safe for a specific admission
decision. A response sender MUST NOT use peer metrics, app-limited metrics,
control-only metrics, unknown loss reported as zero, or unknown carrier flight
reported as zero as authority for ordinary bulk delivery rate, pacing rate,
bytes in flight, inflight limit, or product-flight cap. Those values come from
local sender evidence for the same direction, or from unpolluted product
delivery samples where no packet-level carrier metric exists.

STREAM_ACK and QUIC ACKs release different ledgers. QUIC ACKs release carrier
packet flight and feed the QUIC congestion controller. STREAM_ACK releases
product repair and product path-flight ownership for stream byte ranges.
Contiguous delivery advances the receiver/application frontier. One ledger MUST
NOT be substituted for another.

Carrier feed credit is also ledger-specific. Product `STREAM_ACK` progress may
make a stream byte range safe for repair-cache release or lead migration, but it
does not prove that QUIC packet flight is empty. QUIC carrier telemetry may make
the writer-feed envelope larger so the QUIC controller stays fed, but it does
not release product repair ownership. A scheduler implementation that reports
carrier loss, flight, or queue as unknown MUST preserve that unknown state
instead of publishing zero and thereby admitting harmful or starving work.

### 11.5 QUIC Roaming and NAT Rebinding

UDP roaming is provided by QUIC connection IDs and QUIC path validation. Client
IP changes caused by NAT rebinding, CGNAT, Wi-Fi changes, mobile networks, or
VPS load balancers MUST NOT immediately terminate logical streams when the QUIC
connection remains authenticated and live. Product sessions and product stream
IDs remain stable across QUIC path changes.

mptunnel path health observes QUIC connection state and product progress. Idle
path liveness may use ordinary path probes. Active data-path failover MUST use
data-plane stall, PTO, carrier close, and product ACK/repair evidence rather
than waiting for a coarse TCP-style heartbeat.

## 12. Session and Path State Machines

### 12.1 Session Setup

A client starts a logical session by creating a random session ID and opening an
initial underlay path. The path carries session authentication and path join
authentication before application frames.

For reliable-stream traffic, every TCP and UDP underlay path created by one
client context for the same remote server MUST use the same logical session ID.
The server's reliable-stream registry is keyed by this session ID and stream
ID; using separate session IDs for TCP and UDP underlays would create
independent streams and independent outbound target connections, which is not
path attachment, repair, or aggregation. Implementations MUST NOT use separate
TCP and UDP reliable-stream session IDs for paths that are intended to stripe,
repair, migrate, or fail over one logical stream.

Conceptual flow:

```
client -> server: SESSION_HELLO
client -> server: SESSION_AUTH
client -> server: PATH_JOIN
server -> client: PATH_JOIN_OK
server -> client: SESSION_READY
```

The semantic ordering shown above is normative. Underlays MAY place these frames
in different records or packets, but a peer MUST validate `SESSION_AUTH` and
`PATH_JOIN` and observe successful session/path acceptance before processing
application stream or datagram frames.

Session and path authentication are explicit frames instead of being implicit
carrier state. This lets the same logical session add TCP, UDP, and mixed paths
independently, and lets new paths recover existing streams after failure or
roaming.

### 12.2 Joining Additional Paths

Each additional path sends `PATH_JOIN` with the same session ID and a path ID.
Path IDs are interpreted with the underlay protocol for path-specific state, so
TCP path 0 and UDP path 0 may both exist inside the same logical session. A
server that accepts the path responds with `PATH_JOIN_OK`. A server MUST reject:

* failed authentication;
* stale issue time;
* replayed path join nonce;
* unsupported underlay/capability combination;
* path count above the configured maximum.

Additional paths are treated as attachable resources, not new sessions. This
preserves stream IDs, repair caches, fairness state, and diagnostics across
failover and aggregation.

### 12.3 Path Health

Path health states are Active, Suspect, Draining, and Failed. Active paths are
eligible for ordinary scheduling. Suspect paths MAY be used with penalty or
repair confidence. Draining paths SHOULD avoid new traffic. Failed paths MUST
not receive new ordinary traffic until probing recovers them.

`PATH_STATUS`, `PATH_DRAIN`, `PATH_CLOSE`, `PING`, `PONG`, `PATH_MTU_PROBE`, and
`PATH_MTU_ACK` maintain path state.

State transitions are intentionally coarse. Fine-grained policy belongs in the
path model and scheduler; path health only answers whether a path is ordinarily
usable, risky, being drained, or failed. This keeps path lifecycle separate from
per-frame scheduling, as in mature multipath designs.

## 13. Reliable Stream Layer

### 13.1 Stream Open

`OPEN_STREAM` creates or reattaches a reliable stream. It carries:

* stream ID;
* target address;
* ingress kind;
* outbound policy;
* demand hint;
* role: Active, Repair, or Validation.

An Active open creates or promotes a normal data path. A Repair open attaches an
additional path for gap repair, failover repair, or retransmission and MUST NOT
receive ordinary bulk data merely because it is attached. A Validation open
attaches an additional path for bounded proof traffic. Validation is distinct
from Repair because the scheduler needs to learn whether an unknown path can
carry bulk without weakening the invariant that repair traffic is gap-targeted.
Validation traffic remains subject to ECF/BLEST-style admission, flow control,
and a finite validation budget. For ordered reliable streams, Validation credit
is not throughput evidence. A validation path without sender-side delivery
evidence MUST NOT own the only copy of new ordered bytes while any ordinary
ordered-data owner exists. This is true for both same-underlay and
cross-underlay validation. It validates by duplicate stream data that is also
sent on an admitted ordinary path, repair data for an already-missing range, or
carrier/control probe traffic until sender-side delivery evidence exists. Once
any path has sender-side evidence, an unproven validation path MUST NOT carry
new later stream offsets as unique data. Liveness from the open itself is not
delivery evidence. A receiver MUST NOT promote a Validation or
Repair attachment to the Active data slot merely because one frame arrived in
order. For bulk streams, receiver-side Active promotion is allowed only after
delivered application bytes have been accounted into the path model and the path
has local delivery samples or ACK-derived carrier data samples. Configured
hints, successful opens, control
probes, RTT-only liveness, and single duplicated stream ranges do not satisfy
this requirement.

Repair and Validation opens are attach-only. If their stream ID is unknown to
the receiver, or if the receiver has recently closed that product stream, the
open MUST be rejected or ignored as stale product control. It MUST NOT create a
new outbound target connection. Active opens create a product stream only when
the stream ID is not in the receiver's recent closed-stream cache; an Active
open for a recently closed stream is also stale reattachment control and is
rejected or ignored without opening the target again. This rule keeps path
validation and reannouncement from replaying user connections during races
around stream teardown.

The server maps a repeated stream ID to the same outbound TCP connection when
reattaching after path migration or repair. For a given product stream and
carrier path key, there MUST be at most one live response-output attachment.
A repeated `OPEN_STREAM` for the same stream ID on the same live carrier path
is an idempotent reannouncement: it may refresh lane metadata, demand hints,
credit, and path status, but it MUST NOT replace the live writer/output channel
or create another ordinary output for unique later offsets. It also MUST NOT
reorder the existing output entry or change ordered-data ownership merely
because the duplicate open used the Active role. Senders MUST NOT use repeated
same-key Active opens as normal validation or throughput discovery; they are
bounded failover/survivor recovery control after an explicit carrier failure or
promotion decision. Same-key replacement is allowed only after the previous
output has been closed, detached, or otherwise made unusable by carrier
teardown. If an `OPEN_STREAM` arrives on a different live carrier channel for a
stream/path key that already has a live output, the
receiver MUST NOT replace the live response-output channel. An Active duplicate
carrier channel MAY be accepted as an overlapping input/control channel when the
sender has already abandoned or is replacing the previous carrier instance; in
that case valid product frames are still routed by stream offset, but unique
response bytes continue to use the already-owned response output until carrier
teardown or explicit detach clears the live Service owner and the sender
performs frontier-safe repair or re-admission. Non-active duplicate proof
channels SHOULD be ignored or closed when an equivalent live output already
exists. Any
duplicate-carrier close MUST NOT be encoded as a product `STREAM_RESET`, because
the original product stream remains alive. This rule preserves carrier ordering
scope: QUIC and TCP guarantee order inside one carrier stream, not across
accidental parallel carrier streams carrying the same product offset space; when
active replacement overlap is unavoidable, the sender service must repair or
avoid the resulting product ordering debt.

Product-level stall evidence alone MUST NOT create a replacement carrier stream
for the same product stream on the same sole reliable carrier or inside a stable
same-underlay carrier subflow set. On a live QUIC subflow set, QUIC owns packet loss
recovery, stream ordering inside each carrier stream, PTO, congestion control,
and NAT rebinding. On a live TCP subflow set, TCP owns byte retransmission, stream
ordering inside each carrier stream, write pressure, and connection teardown. A
product sender may send recv-progress, ACK, control, and bounded repair work on
the existing attached outputs, but it MUST keep queued bytes, path-flight
ownership, and repair ownership on that carrier or subflow set until a carrier
reports close/error or a distinct survivor path has positively delivered
replacement data. Opening a fresh reliable carrier stream for later unique
product offsets while the previous carrier stream may still deliver lower
offsets defeats the carrier's ordering guarantee and recreates above-carrier
head-of-line debt. Reshuffling an already-attached same-underlay subflow set on
product stall is the same class of error: it consumes control capacity and can
move ordered-data ownership without fixing the missing byte range. Real carrier
teardown remains a carrier event and may attach a replacement path; a product
repair-cache stall is not by itself proof of carrier teardown.

Stream IDs are stable logical identifiers, not carrier connection identifiers.
Reattaching the same stream ID is what lets mptunnel repair over a survivor path
without forcing the application to reconnect.

Product frames that arrive for an unknown stream ID are not carrier-liveness
evidence and are not terminal product-stream evidence. A receiver MAY drop such
frames when no bounded orphan/reorder buffer is available, but it MUST NOT add
that stream ID to the recent closed-stream cache merely because an unknown
`STREAM_DATA`, `STREAM_ACK`, `STREAM_FIN`, or `STREAM_RESET` was observed.
Only a real product terminal transition, such as accepted FIN/RESET handling or
local registry teardown after the stream has completed, may create recent-closed
state. This preserves the layering rule from QUIC and MPTCP: packet/path arrival
order and product stream creation order are independent, and a reordered data
frame must not make a later valid `OPEN_STREAM` impossible.

### 13.2 Stream Data

`STREAM_DATA` carries:

```
stream_id:u64
offset:u64
flags:u8
payload:u32 bytes
```

Offsets are absolute within the stream. Receivers MUST buffer out-of-order data
up to the configured reorder limit and deliver contiguous bytes in order.
Invalid ranges MUST be rejected. Duplicate or partially overlapping valid ranges
MUST NOT be fatal: the receiver trims the incoming payload to byte subranges not
already received, buffers only those novel bytes, and treats fully duplicate data
as an idempotent no-op while still allowing ACK feedback to describe the received
range set.

Absolute offsets give mptunnel the same essential tool that MPTCP data sequence
mapping provides: data correctness is independent of the underlay that carried a
chunk. This enables striping, retransmission, validation probes, and path-aware
reinjection without changing the application byte stream. Because reinjection
can race the original path, overlap acceptance is a correctness requirement, not
a compatibility fallback.

### 13.3 Stream ACKs

`STREAM_ACK` carries:

```
stream_id:u64
complete:u8
range_count:u16
ranges[range_count]
```

`complete` is 1 when the frame is repair-authoritative through the largest end
offset carried in that frame. It does not require the frame to contain every
later received range. A receiver that has more ranges than fit in one frame MUST
send the lowest-offset ranges first and MAY set `complete` to 1 for that bounded
horizon. `complete` is 0 only when the frame is an arbitrary snapshot whose
omissions below its largest carried offset are not authoritative. A sender MUST
release explicitly acknowledged ranges in both cases. A sender MUST infer
missing stream holes from omitted ranges only when `complete == 1`, and only
below the largest end offset carried by that frame.

This rule is critical. A bounded partial ACK MUST NOT be interpreted as proof
that every omitted offset was lost. That behavior would create false repair
bursts and head-of-line amplification.

Horizon-authoritative ACKs are used because high-throughput UDP paths can
generate sparse receive ranges faster than one bounded control frame can report
the entire stream state. Waiting until all ranges fit disables exactly the gap
repair needed to make progress. The repair horizon keeps the inference safe:
later omitted ranges are above the largest carried end offset and therefore
cannot be mistaken for holes in the current repair decision.

### 13.4 Stream Flow Control

`STREAM_MAX_DATA` advertises the maximum accepted offset. Senders MUST NOT send
new data beyond this offset. Receivers update the maximum offset from delivered
progress and configured window size.

Stream flow control limits memory exposure while still letting a receiver
advertise enough window for high-BDP bulk transfer. The configured stream window
is a capacity envelope, not proof that the sender should fill it blindly. The
sender-service scheduler, repair ledger, path admission, and carrier-credit
gates decide how aggressively to use the advertised credit from live path
state.

A sender-side product stream starts with exactly the peer-advertised credit. An
implementation MUST NOT manufacture the configured stream window as local send
credit before receiving the peer's open/MAX_DATA credit. For QUIC reliable
carriers, the advertised product window SHOULD be path-adaptive: the configured
window is a hard ceiling, while the active advertised window is derived from
receive progress, measured BDP/carrier credit, and a bounded startup floor. This
keeps product flow control aligned with QUIC's own congestion/stream backpressure
and prevents hidden multi-second QUIC stream backlogs from becoming application
zero-throughput gaps. TCP reliable carriers MAY advertise the full configured
window because kernel TCP backpressure is the observable carrier queue at this
layer.

### 13.5 Stream Close

`STREAM_FIN` carries the final offset. A receiver MUST deliver FIN only after all
bytes below final offset have been delivered. `STREAM_RESET` aborts a stream.
`STREAM_DETACH` detaches one path instance without closing the logical stream.

FIN, RESET, and DETACH represent different failure domains. FIN completes the
byte stream, RESET aborts the logical stream, and DETACH removes only one
carrier attachment. Separating them prevents a path failure from unnecessarily
killing the application connection.

An endpoint that locally removes an accepted carrier attachment while the
logical stream remains known MUST enqueue `STREAM_DETACH` on that carrier before
it deletes local receive state or closes the carrier's product-stream command
pipe. This includes attach/open timeout cancellation after `OPEN_STREAM` may
have reached the peer, carrier-failure removal, validation-path removal, and
ordinary rebalancing cleanup. The detach is a control-lane product frame and
MUST be ordered before local close/removal on that carrier. If the carrier has
already failed and the detach cannot be delivered, the endpoint MUST still cool
the path down and use repair/failover on survivor paths; however, it MUST NOT
silently remove a live carrier attachment without notifying the peer.
For stream carriers that batch product frames before an explicit flush, a
`STREAM_DETACH` is also a writer-drain boundary: the writer MUST flush the
detach before processing a queued local close/removal for the same stream.
Processing `STREAM_DETACH` and `CloseStream` in one unflushed command batch is
not conformant, because the peer can continue sending valid data-level ranges
while the local endpoint has already deleted the attachment state.

Local carrier close/removal is a drain transition, not immediate receive-route
deletion. After the local endpoint has emitted `STREAM_DETACH`, it MUST stop
admitting new ordinary bytes onto that carrier attachment, but it MUST continue
to route valid in-flight `STREAM_DATA`, `STREAM_ACK`, credit, FIN, and RESET
frames for that stream until a remote `STREAM_DETACH`, terminal FIN/RESET
delivery, receiver shutdown, or carrier teardown removes the route. This mirrors
MPTCP subflow semantics: removing one subflow does not invalidate connection
data sequence numbers that were already in flight on that subflow. Timed-out
validation opens that never produced an accepted product stream MAY drain and
discard late carrier frames as stale validation traffic, but they MUST NOT be
reported as unknown logical stream ownership.

If the sender has observed EOF from its local product source, it MUST attempt to
emit `STREAM_FIN(final_offset)` before detaching the carrier path. A
`STREAM_DETACH` is not a substitute for FIN and MUST NOT be used to signal
logical stream completion. If a carrier path is lost after the local side is
closed, no product bytes are queued, no repair bytes remain, no receive-hole
debt exists, and the receiver has already delivered contiguous response bytes,
the stream MAY complete without waiting for a late remote FIN that can no
longer be recovered. This is a product-level close race rule: it releases stale
teardown bookkeeping, not unread data or repair debt.

Because `STREAM_FIN` carries the final offset, it is not generic urgent control
inside a sender-service queue. A sender MAY encode and dispatch FIN through the
carrier's control or priority queue after it is selected, but the sender service
MUST stage that final-close work behind all already queued data and repair work
for the same stream direction. Otherwise a receiver can observe EOF before the
bytes below the advertised final offset have arrived, which violates the
product stream contract even when the carrier delivered the FIN correctly. When
an admitted ordered-data owner exists for that stream direction, FIN/final-offset
control is owned by that same output. If that output is alive but temporarily
backpressured, the sender service MUST wait for its carrier credit instead of
sending FIN on a validation, standby, repair-only, or failover output. A
different output may carry FIN only after explicit ordered-owner migration,
ordered-owner loss, or reset/close semantics that make the old owner no longer
responsible for lower outstanding bytes.
The sender does not need to wait until every byte below the final offset has
already been acknowledged before emitting FIN; QUIC and MPTCP both separate a
stream's final offset from packet/subflow recovery. However, emitting FIN MUST
NOT release repair ownership. Unacknowledged byte ranges below `final_offset`
remain in the repair cache and path-flight ledger until `STREAM_ACK` releases
them or the stream is reset.

### 13.6 Repair Cache

Senders retain unacknowledged `STREAM_DATA` chunks in a repair cache bounded by
`max_repair_bytes`. ACKs release cache entries. Path failure, ACK gaps, receive
holes, or stalls may trigger retransmission of missing chunks on the same path
or another eligible path, but the trigger is carrier-aware.

The repair model follows the same high-level principle as MPTCP data sequence
mapping: the logical stream offset is independent from the underlay path packet
or byte sequence, and the same stream offset MAY be reinjected over another
path.

Repair cache bytes are the product-level substitute for TCP retransmission when
data moves across multiple carriers. Repair MUST be gap-targeted and path-aware:
retransmit missing offsets on the path with the best expected completion time.
Whole-cache replay is prohibited. Cached `STREAM_DATA` frame boundaries are not
repair quanta; a sender MUST be able to retransmit any missing byte subrange
below the repair horizon using a smaller `STREAM_DATA` frame whose offset and
payload exactly describe that subrange.

On `STREAM_ACK`, a sender MUST release all explicitly ACKed byte ranges from
repair state, including ACKed subranges inside a previously cached
`STREAM_DATA` frame. This release is not the same as contiguous application
progress: ACKed bytes above a lower missing range remain part of the sender's
ordering-debt ledger until the contiguous ACK frontier reaches them. If the ACK
is not repair-authoritative
(`complete == false`), omitted ranges MUST NOT be interpreted as holes. If the
ACK is repair-authoritative (`complete == true`),
the sender may compute holes below the largest end offset carried in that frame
and schedule only those unacknowledged ranges for repair only after persistent
gap evidence shows that the carrier recovery that owns the original flight is
not sufficient. For ordinary reliable streams over TCP or QUIC carriers, an ACK
gap by itself MUST NOT trigger product-level `STREAM_DATA` reinjection: TCP and
QUIC already own packet/stream reliability below mptunnel. Product-level
reinjection is reserved for explicit path failure, active stall, migration, or
multipath repair where the first missing product offset persists beyond the
PTO-derived persistent-congestion window for the active reliable carrier. A
fresh ACK gap below the largest carried end offset is evidence of a possible
receive hole, not by itself proof that product-level repair should race the
carrier. When a reliable stream has more than one attached path and a
repair-authoritative `STREAM_ACK` exposes the same first missing offset beyond
the persistent repair delay derived from path RTT, jitter, lane, and stall
state, that persistent hole is a multipath repair signal even if the hole's
upper bound grows as later bytes arrive. The
sender SHOULD reinject only the missing cached ranges on an eligible alternative
path, avoiding the path that last carried the missing range when an alternative
exists, and MUST rate-limit repeated reinjection of the same first missing
offset by the same persistent repair delay. This is the product-layer
equivalent of MPTCP reinjection. It does not weaken carrier recovery; it
prevents one slow or lossy carrier from holding the only copy of an ordered
stream byte while other survivor paths are usable. On path failure, the sender repairs only
unacknowledged bytes last sent on the failed or suspect path. A sender MUST NOT
retransmit acknowledged ranges and MUST NOT replay the entire repair cache after
reattach.

When a tail-stall repair timer fires on a live stream, a sender MUST inspect the
most recent repair-authoritative `STREAM_ACK`. If that ACK proves an
unacknowledged gap below its largest end offset, the repair extent is that gap,
not bytes after the ACK frontier. If no authoritative lower gap is known, the
live contiguous owner tail after the ACK frontier is not immediately a
path-scoped missing range; TCP and QUIC still own recovery for the carrier
stream that holds those bytes until the PTO-derived product tail timer fires.
After that first timer, the sender MAY reinject only the lowest blocked suffix
as bounded `RepairData` on a different eligible output, and MUST NOT treat that
repair ACK as path delivery proof. If no ACK-frontier progress follows that
probe, repeated live-tail repair uses the persistent-congestion delay. Terminal
tail recovery is separate: once a final offset is known, a sender may repair
unacknowledged bytes below that final offset on an eligible survivor path so the
DATA_FIN/STREAM_FIN can be acknowledged, and that final-tail repair may use the
bounded critical repair path. Repair candidate selection is
prefix-preserving: if the lowest unresolved repair frame cannot be sent on an
alternate eligible output, the sender MUST NOT skip it and send a later ordered
range instead. This is targeted duplicate repair, not whole-cache replay.

## 14. Datagram Flow Layer

`OPEN_DGRAM_FLOW` creates a datagram flow for a target and ingress kind. The
server validates that the configured outbound supports UDP targets.

`DGRAM_DATA` carries:

```
flow_id:u64
datagram_id:u64
ttl_ms:u32
payload:u32 bytes
```

Datagrams are unordered. TTL controls freshness, carrier selection, and the
response-wait budget; it is not permission to replay an emitted product
datagram. A path whose ETA cannot fit the TTL SHOULD be avoided.
`DGRAM_FEEDBACK` acknowledges received datagram ID ranges and feeds
RTT/loss/delivery-rate observations into path models.

Datagram workers MUST treat target responses, `DGRAM_FEEDBACK`, and
`DGRAM_CLOSE` as realtime feedback. If target responses and additional outbound
requests are both ready, the response/feedback side is processed first so that
fresh datagrams and close signals do not wait behind more request sends.
`DGRAM_DATA` response and `DGRAM_CLOSE` emission use the realtime
carrier-credit gate. If the carrier command queue cannot accept a realtime
datagram response or close signal immediately, the worker MUST NOT block behind
bulk or ordinary request sending. It may drop the response as
expired/backpressured work, and close emission is best-effort when the target
side has already failed. The worker then continues or exits according to the
flow state. This preserves datagram freshness and prevents a full path queue
from becoming a hidden realtime head-of-line blocker.

`DGRAM_CLOSE` closes a flow. A closed flow MUST release scheduler load and
delivery statistics.

Datagram flows are freshness-aware product objects, not a request to prefer a
UDP underlay carrier. When TCP and QUIC UDP carriers are both available, the
client chooses the carrier from live RTT, jitter, loss, delivery-rate, queue,
flight, TTL, and demand evidence. Fresh realtime datagrams start latency-first;
sustained volume may move toward a higher-bandwidth carrier; when that demand
falls away, the flow can shrink back to latency/realtime behavior. If the
selected carrier fails in a retryable path-level way, the client MAY try the
next schedulable TCP or QUIC UDP carrier by the same evidence order.

After a `DGRAM_DATA` frame for a product datagram has been emitted on any
carrier, the sender MUST NOT resend that same product datagram ID on another
carrier, reopen a carrier for it, or retransmit it after receiving
`DGRAM_FEEDBACK`. TCP and QUIC already own packet/stream recovery below this
layer, and QUIC DATAGRAM-style applications are freshness-bound rather than
reliable. The client sends one product datagram ID once on the selected
carrier, waits for response or expiry using the PTO/TTL-derived useful budget,
and reopens or migrates the carrier only for actual setup, carrier,
encryption, authentication, or session errors. This prevents product-level
duplicates from consuming target UDP state and prevents response absence from
poisoning path health.

UDP targets need unordered, freshness-aware product delivery. Old datagrams
should expire rather than block later datagrams, but the underlay carrier used
for each product datagram is selected by path evidence and demand, not by the
target protocol family.

## 15. Ingress Behavior

### 15.1 SOCKS5

SOCKS5 ingress supports CONNECT and UDP ASSOCIATE. SOCKS5 is terminated locally.
The client MUST NOT forward the SOCKS5 handshake end-to-end. For CONNECT, the
client sends `OPEN_STREAM` and then relays payload bytes as reliable stream data.

SOCKS5 username/password authentication is optional and disabled by default.
When configured, username and password MUST match in constant time.

UDP ASSOCIATE creates local UDP relay state. The client validates the UDP peer
against the association and relays datagrams through internal datagram flows.

Terminating SOCKS locally removes a legacy proxy protocol from the internal wire
format and lets mptunnel use the same stream/datagram machinery for SOCKS,
HTTP, and TUN traffic.

### 15.2 HTTP CONNECT

HTTP CONNECT ingress parses the CONNECT authority locally. If proxy auth is
configured, the client requires Basic proxy authentication. On successful
internal stream open, the client returns a success response and relays bytes as a
reliable stream.

HTTP CONNECT is a compatibility surface for tools and enterprise environments.
Only the authority and authentication result are semantically needed by
mptunnel; the HTTP exchange itself is not carried across the tunnel.

### 15.3 TUN L4

TUN ingress creates a cross-platform TUN device and uses a user-space network
stack to accept TCP and UDP flows. TUN TCP flows become reliable streams with
ingress kind `TunTcp`. TUN UDP flows become datagram flows with ingress kind
`TunUdp`.

TUN supports IPv4, IPv6, or dual-stack addresses. DNS UDP traffic MAY be
remapped to configured TUN DNS resolvers. Responses MUST be translated back so
the local TUN client observes the original DNS destination.

TUN mode lets applications that cannot configure a proxy still use mptunnel. DNS
handling is explicit because name resolution decides whether traffic enters the
tunnel, which address family is used, and which outbound resolver policy is
applied.

## 16. Outbound Behavior

Server outbound policies are:

* Direct TCP/UDP.
* Direct TCP/UDP with bound source IP.
* SOCKS5 CONNECT and SOCKS5 UDP ASSOCIATE.
* HTTP CONNECT for TCP.
* HTTP CONNECT-UDP for UDP.

Domain targets are resolved through configured outbound DNS resolvers when
present, otherwise through the system resolver. DNS strategy controls IPv4/IPv6
lookup order and filtering.

Plain HTTP CONNECT outbound MUST reject UDP targets. HTTP CONNECT-UDP outbound
MUST use a UDP-capable HTTP proxy profile. In RFC 9298-compatible mode, this is
Extended CONNECT with HTTP Datagrams.

Direct outbound is the baseline path to the target. Source IP binding supports
multi-homed servers and policy routing. Upstream SOCKS5, HTTP CONNECT, and
CONNECT-UDP allow mptunnel to compose with existing proxy infrastructure without
changing ingress semantics. DNS is located at outbound because that is where
operator policy, source binding, and upstream proxy behavior can differ.

## 17. Adaptive Auto Scheduling

Production mptunnel has no fixed transmission mode. Auto is mandatory.

Fixed transmission modes are stale by construction. The same path can be ideal
for bulk at one moment and harmful to latency-sensitive work moments later
after QoS, queue growth, packet loss, or roaming. Auto continuously chooses
between latency-first, balanced, and throughput-first behavior using flow demand
and path evidence.

### 17.1 Flow Demand

Reliable streams start latency-first. Observed bytes, send rate, repair bytes,
idle gaps, and path BDP promote sustained large flows toward throughput-first
behavior. Idle gaps, stalls, repair pressure, and tails move behavior back
toward latency-sensitive handling.

Latency-first startup is finite owner credit, not an implicit bulk mode. Before
a reliable stream has been promoted to throughput, the initial Service owner may
carry only enough unique product bytes to establish the bulk evidence floor.
After that credit is consumed, the sender MUST stop reading additional product
bytes into the latency-owner queue until either throughput promotion/admission
runs or the stream becomes idle again. Prevalidation may start at the smaller
path-open score, but prevalidation alone MUST NOT allow the latency owner to
accumulate megabytes of lower ordered bytes that later block Subflow admission.
This preserves the negotiated model: open latency-first, then move toward the
measured bandwidth carrier on demand without creating stale lower-owner debt.

Flow demand is represented by lane and ppm weights rather than by user-visible
traffic class names. The defined lanes are Control, Latency, Throughput,
RealtimeDatagram, and Background.

Streams are equal by default. If two bulk downloads coexist, the first MUST
gradually share capacity with the second instead of retaining permanent
priority. Interactive bursts, ACKs, and control frames get protected latency,
but bulk flows compete fairly over time.

Fair sharing is evaluated at two levels. ECF/BLEST-style admission applies
inside one ordered stream and prevents chunks from being striped onto paths that
would create head-of-line blocking. Independent bulk streams, however, do not
share an ordering dependency. When multiple healthy paths exist, Auto scores
each candidate as if the stream would join that path's active bulk set. A busy
peer path MUST NOT be scored as if its full delivery rate were free for another
independent bulk stream. This prevents later or parallel downloads and uploads
from collapsing onto the same low-latency path merely because per-chunk ETA
favors it before sharing is modeled, while still allowing a stream to move away
from a stale active path when ECF admission proves that another path is better.

### 17.2 Path Model

The path model combines configured hints and live measurements:

* smoothed RTT;
* jitter;
* delivery rate;
* loss rate;
* queue bytes;
* bytes in flight;
* pacing rate;
* inflight limit;
* active flow count;
* active latency-sensitive flow count;
* confidence;
* app-limited state;
* path flags.

Confidence prevents unknown paths from being trusted as fully measured bulk
paths too early. Hints seed the model but measured delivery samples override
hints.

Each metric prevents a known failure mode. RTT alone chooses low-latency paths
that may be low-bandwidth. Bandwidth alone chooses paths that may be deeply
queued. Active flow counts let startup and short interactive work spread away
from already busy paths without inventing fake queue bytes. Loss alone cannot
distinguish congested loss from lossy but usable wireless links. Confidence
prevents early wrong decisions before the model has enough samples.

Successful stream opens and association opens are liveness evidence. They MAY
clear failure state and update active-flow counts, but they MUST NOT by
themselves create RTT, delivery-rate, or freshness confidence samples. Stream
ACKs release inflight ownership and repair-cache entries, but delayed, compressed,
or tiny ACK-release timing MUST NOT raise or lower the bulk delivery-rate
estimate. Probe responses, ACK-derived carrier data samples, datagram feedback,
and other data-plane observations are the inputs that raise path-model
confidence. Datagram feedback is path-scoped evidence for realtime/datagram
scheduling, but it is not by itself proof that the same path may own new ordered
reliable-stream bytes; reliable bulk ownership requires ordered-stream product
delivery evidence or carrier ACK-derived data evidence from the reliable
carrier.

Long-lived streams update path delivery evidence while they are active. Once a
receiver has delivered enough ordered stream bytes to form a meaningful rate
sample, the endpoint updates that path's delivery model immediately instead of
waiting for stream close. The sampling cadence is derived from the relay buffer
envelope so it is frequent enough for scheduling decisions but not a per-packet
counter update. This prevents an active bulk stream from being scheduled for
many seconds using only liveness evidence, and it prevents an unproven path from
replacing a path that is already delivering application bytes.

If no candidate path has delivery evidence yet, a sustained bulk stream remains
on its already-active throughput or background path while the endpoint
validates idle unknown paths with controlled attachment or repair probes. The
scheduler MUST NOT abandon an active but unmeasured throughput path for another
equally unmeasured path just because the other path has a better default ETA.
Latency-sensitive activity is not throughput evidence. Within a same-underlay
endpoint-only subflow set, the implementation may keep an unmeasured latency-started
stream on its current path until controlled validation succeeds, avoiding
needless spraying across equally unknown subflows. In a mixed TCP/UDP subflow set,
latency-sensitive work on one underlay is pressure that can make another
suitable underlay preferable for throughput validation. This rule preserves
startup progress without confusing liveness with throughput confidence.
When no path in a same-stream bulk subflow set has delivery evidence and no path is
already carrying bulk work for that stream, the ordinary striping subflow set is
limited to the single best candidate. Additional unknown candidates belong to
validation, not to ordinary data striping. This prevents an all-unknown
endpoint-only startup from becoming fake aggregation before the sender has
observed actual delivery.

Once a stream has an ordered-data owner and the scheduler has opened
same-underlay candidate outputs, app-limited startup samples MUST NOT be treated
as long-term bandwidth proof for ECF/BLEST completion-horizon rejection. QUIC's
initial congestion window and early MPTCP subflow growth are probe mechanisms, not
accurate bulk-rate priors. A same-underlay candidate may receive a bounded
frontier-clear startup Subflow owner window after the current Service has
bulk-rate evidence and the candidate has path-scoped sender evidence, but it may
be admitted for steady-state Subflow ownership only after bulk-rate evidence
exists and it still fits product inflight, carrier credit, completion, and
reorder budgets. This is true even if a candidate's current ETA is worse than
the lead's ETA, because same-underlay ETA can be an artifact of
underfeeding and validation; the correct proof is whether the additional path
creates ordered receive-hole debt. If same-underlay Service later creates real
stream-ordering debt, the lower-frontier owner rule and completion-horizon gate
become authoritative for further unique bytes. Cross-underlay candidates remain
strict even during startup, because TCP and QUIC expose different queueing and
HOL behavior.

### 17.3 ETA Scoring

Scheduler scoring estimates completion time from RTT, queue bytes, bytes in
flight, pacing/delivery rate, loss, jitter, confidence, and capability
penalties. Latency, realtime, control, and repair lanes score the next
preemptible quantum. Throughput and background lanes score a service horizon
derived from both the next quantum and the configured product resource
envelope. The envelope is the minimum of the stream window, path inflight
envelope, and receiver reorder envelope. The service horizon is the geometric
mean of the next quantum and that envelope, bounded below by the actual next
quantum and above by the envelope. This makes scoring more forward-looking than
latency-probe scheduling without letting a fresh bulk stream behave as though
an entire product envelope were already safe to put in flight. This horizon is
used only for path scoring; it MUST NOT become an indivisible frame, AEAD
record, or path write. The sender still emits bounded quanta so control, ACK,
repair, realtime, and latency work can interleave.

The service horizon applies to initial Service selection and to the current
Service owner feed. It MUST NOT be reused as the per-candidate payload for an
already measured Subflow owner decision. A same-family Subflow candidate is
scored against the next owner range or explicit Subflow credit being assigned to
that path, then checked against ordering-debt and completion-horizon guards. If
the implementation scores every 64 KiB Subflow decision as though the Subflow had
to drain the entire product service horizon, the scheduler serializes the stream
onto the Service path and prevents useful aggregation.

For throughput and background lanes, delivery rate is first adjusted by the
number of active bulk flows sharing the path; when a stream considers moving or
adding work to a non-active path, that stream is counted as joining the path
for scoring. Backup, expensive, suspect, high-loss, high-jitter, and
low-confidence paths receive penalties. For reliable streams over UDP, the
bulk score also includes the estimated repair cost of the next emitted quantum
from loss, MTU fragmentation, RTT, and jitter; this keeps poor-loss UDP from
being treated as free capacity while still allowing normal low-loss UDP to win
when its measured delivery rate is better. Realtime datagrams are latency
sensitive. Bulk reliable streams may use multiple paths only after ECF/BLEST-
style admission proves that the additional path should not increase completion
time versus the best safe available path.

Earliest-completion scoring approximates the practical goal of MPTCP ECF-style
scheduling without exposing subflow details to applications. The service
horizon prevents a sustained file transfer from being mis-modeled as an
infinite sequence of tiny latency probes, so a high-RTT/high-bandwidth path can
lead or join a bulk subflow set when its bandwidth and queue state offset its
latency. Short flows remain sticky to the path that completes the immediate
quantum soonest.

### 17.4 Fairness

The class scheduler uses lane priority with deficit-round-robin style flow
fairness. A later bulk download MUST be able to share bandwidth with an earlier
bulk download rather than starving forever. Control, ACK, and latency-sensitive
work MUST remain able to bypass saturated bulk queues.

Throughput is not the only product metric. Browsing and SSH feel broken when a
bulk flow consumes all scheduler attention, even if aggregate Mbps is high.
Deficit-style fairness gives bulk flows long-term sharing while priority queues
keep small control and interactive work fluent.

Frame lane classification is derived from frame semantics at the sender-service
boundary. A caller's local flow label can raise or lower the priority of
ordinary `STREAM_DATA`, but it cannot convert control-shaped frames into bulk
data. `STREAM_RESET`, `STREAM_DETACH`, stream credit, product ACKs, and product
control frames therefore bypass saturated throughput queues even when the
stream had previously been promoted to throughput demand. `STREAM_FIN` is
control-shaped on the wire but tail-ordered in the product stream: it may bypass
unrelated bulk work, but it MUST NOT overtake already-owned data or repair work
for the same stream direction. This rule prevents a bulk data queue from
delaying flow-control release or repair state transitions while preserving the
final-offset ordering guarantee.

### 17.5 Mixed TCP and UDP Underlay

TCP and UDP underlays are optimized separately and may also be used together.
Mixed-carrier reliable streams MUST avoid blind TCP+UDP striping without
evidence. Auto MAY move a stream between carriers or attach repair paths when
live measurements show benefit. Datagram flows use the same evidence-driven
carrier rule: TCP or QUIC UDP may carry a product datagram when its measured
latency, loss, queue/flight state, TTL fit, and demand model is best.

TCP and UDP can coexist, but they report very different signals. UDP exposes
packet-level recovery, ACK-derived delivery samples, pacing, and congestion
state through QUIC. TCP exposes encrypted writer pressure and product delivery
evidence while kernel TCP owns packet recovery below the process. Neither
carrier family is preferred for reliable streams by default. TCP carries when
its measured completion, loss, queue, and ordering-risk model is better; UDP
carries when its measured completion, loss, queue, and ordering-risk model is
better. Mixed scheduling is therefore evidence-driven rather than
protocol-prejudiced.

Reliable stream scheduling is symmetric. The endpoint that sends bytes for a
direction MUST have path metrics for that direction's candidate paths before it
admits validation or ordinary bulk data onto them. A client that opens or
reattaches a reliable stream over a path sends a `PATH_METRICS` frame for that
path using its current path model, direction, metric age, confidence,
application-limited state, and ACK-derived sample count. The server stores
those metrics per session, underlay, and path ID and may use them for bounded
validation admission and ETA scoring. Peer metrics are not response-direction
proof. If no local sender metrics or stream delivery samples exist yet, the
server treats the path as low-confidence and MUST NOT prefer it over the
active/measured path except when the adaptive validation admission rule says the
proof traffic should not increase completion time. This keeps client and server
policy aligned without requiring a large control-plane exchange or pretending
that one endpoint's outbound samples prove the reverse direction. Configured
endpoint hints such as initial RTT, jitter, or rate are also advisory priors:
they may rank the current Service candidate or validation order, but they are
not delivery evidence and MUST NOT by themselves make an optional path a
Subflow owner for same-stream reliable bytes.

### 17.6 Unified Sender Service

Each product flow is governed by exactly one sender-service ownership boundary
between the stream/datagram layer and the carrier writers. The sender service
is a concrete queued ownership boundary, not a permit wrapper around an
immediate path-write loop. An implementation MAY run the sender service inside
the same asynchronous task as a relay only if that code still owns product
queues, classifies lanes before path selection, dispatches bounded quanta, and
keeps carrier writer pipes as emission sinks only. Fixed single-path flows are
the degenerate case of this model. The sender service consumes stream bytes,
datagram payloads, ACK/control frames, repair work, path model snapshots,
flow-control credit, and carrier availability, then emits carrier writes that
respect lane priority, per-flow fairness, path admission, and carrier pacing.
Product frames MUST NOT bypass this boundary merely because they originate from
a read loop, ACK handler, repair trigger, or path reattach handler.

A server response sender is subject to the same rule as the client sender. A
server-to-client target-read loop MUST enqueue raw response bytes into the
sender service and MUST NOT construct and send ordinary `STREAM_DATA` directly.
The sender service creates the `STREAM_DATA` frame only when dispatching an
admitted quantum, after lane priority, flow-control credit, path admission,
path-flight ownership, and carrier-credit checks have run. Diagnostics-enabled
implementations MUST emit a sender decision event for every server response
`STREAM_DATA` write so diagnostics can assert that response bytes did not
bypass the measured scheduling path.

Creating a candidate `STREAM_DATA` frame for an admitted dispatch is a staged
operation. The sender may calculate the next offset and build the carrier frame
after all gates pass, but it MUST NOT advance the stream offset, place the range
in the repair cache, remove the raw bytes from the sender queue, or record the
range as product flight before lane priority and path admission have selected a
carrier. Once a carrier command is about to become visible to a writer pipe, the
sender MUST atomically move the byte range from prepared candidate state into
repair-cache/product-flight ownership before the peer can ACK it. If carrier
queue acceptance then fails before the frame is visible, the sender rolls back
that tail commit and keeps the raw byte range queued. This prepare/commit split
prevents the same byte from being treated as free while also preventing a fast
ACK from arriving before the repair cache owns the acknowledged range.

When ordinary queued data is blocked by flow control, ordering debt, or path
admission, inbound path frames remain part of the active sender-service loop.
An implementation MUST poll and process product ACKs, flow-control frames,
stream resets, detach/close frames, and received stream data before treating a
local path-output update as sufficient progress. Output-update wakeups are
scheduler housekeeping; they MUST NOT outrank feedback that can release
product ownership, repair state, or flow-control credit.
Sender-service retry state is independent from raw carrier queue capacity. A
blocked dispatch may mean no admitted owner path, unresolved lower-byte debt,
flow-control exhaustion, optional-traffic budget exhaustion, or a full carrier
pipe. Therefore a pending retry MUST block additional source reads and ordinary
queued-data dispatch until one of the real release events occurs: ACK/control
feedback is processed, flow-control credit changes, path admission state
changes, a carrier-capacity notification fires, or the retry deadline expires.
It MUST NOT be cleared merely because the front carrier queue currently reports
spare capacity.

Carrier command queues are emission pipes, not permits around the sender
service. For ordinary and repair `STREAM_DATA`, carrier queue capacity is a
nonblocking credit gate. If the chosen carrier command queue cannot accept the
next data quantum immediately, the sender MUST keep the product work queued,
MUST NOT advance stream offset or product-flight ownership permanently, MUST NOT
mark the carrier failed, and MUST continue polling ACK, credit, control,
repair, and path-update feedback. A full carrier queue is backpressure, not a
liveness failure. Control and ACK lanes may use their higher-priority emission
path, but they MUST NOT sit behind a bulk data queue.

After a sender service emits one bounded data quantum, it MUST give carrier
feedback tasks an opportunity to run before continuing an unlimited bulk drain.
An implementation may do this by polling buffered inbound frames first or by a
cooperative scheduler yield at the sender-service quantum boundary. This is not
a throughput throttle: it is the ownership boundary that lets `STREAM_ACK`,
`STREAM_MAX_DATA`, detach, reset, and carrier credit feedback release product
flight and repair state before the next bulk continuation. A diagnostic build
MUST NOT be faster merely because logging accidentally creates this yield.

The same nonterminal rule applies when a switchable stream has no currently
usable carrier output. A closed TCP writer queue or closed QUIC stream output is
a path-output event. It MUST detach that carrier attachment and wake the sender
service, but it MUST NOT by itself close the product stream, advance the product
offset, release repair ownership, drop the queued byte range, or insert the
stream ID into the recent closed-stream cache. If no remaining output can accept
the next quantum, the sender service remains blocked and waits for carrier
capacity, path-output updates, stream ACK/credit feedback, or a new path
attachment. This rule is necessary because a path failure and a product stream
FIN/RESET are different ledgers; treating the former as the latter creates false
remote resets, unknown-frame drops, and lost high-rate transfers during QUIC/TCP
reattachment races.

The size of a carrier command queue is derived from the sender-service quantum
that will actually be emitted on that carrier, bounded by the carrier's legal
frame size and the path inflight envelope. An implementation MUST NOT size a
QUIC UDP writer pipe from the maximum product frame that QUIC can carry if the
sender-service bulk quantum is smaller; doing so undercounts the number of
commands needed to keep the carrier fed and turns a healthy reliable carrier
into a bursty sender. In the single-path/same-underlay case, TCP and QUIC UDP
therefore use the same product quantum sizing rule, while their carrier engines
remain responsible for their own packet congestion control and pacing.

A sender-service ordinary throughput frame is one preemptible service quantum.
A dispatch run MAY emit multiple ordinary throughput quanta for a flow, but only
up to a bounded feed window, and then MUST yield back to the feedback loop before
starting another ordinary bulk run. The dispatch-run byte budget is not the path
inflight envelope, the carrier congestion window, or the full sender queue
limit. Those larger values describe how much product or carrier flight may exist
over time; they do not authorize one scheduling pass to move tens of MiB into a
writer pipe. This keeps product ACKs, carrier ACKs, FIN/RESET/DETACH, repair,
and latency work observable between bounded bulk runs while still allowing a
high-rate carrier to stay fed through repeated nonblocking dispatches.

Carrier command-queue credit is event-driven. When a carrier writer consumes or
discards a queued command and releases queue capacity, that release is a sender
wakeup for streams whose next queued work could use that carrier lane. The
sender service MUST check current carrier capacity before remaining in a
blocked state. For ordinary throughput data, carrier capacity is a byte-credit
predicate: the selected writer pipe must have both queue-slot capacity and
pending writer bytes below the path-model emission budget for that flow. That
budget is byte-based and evidence-based; it is not a count of mpsc slots. A
carrier with free mpsc slots but a large pending data backlog is not considered
ready for unlimited ordinary throughput data. Implementations MAY incorporate
carrier inflight-high or congestion-window evidence into this gate, but MUST
avoid treating a transiently small carrier window as a second product receive
window that starves a healthy QUIC/BBR sender. Control, product ACK,
FIN/RESET/DETACH, and bounded repair lanes remain priority work and MUST NOT be
delayed behind bulk writer-pipe debt. If capacity is unavailable, the sender
SHOULD wait on carrier capacity release or path-output feedback rather than
sleeping for the receive-progress, repair, or path-stall timer. A fixed retry
timer MAY exist only as a lost-notification race fallback, and it MUST be
derived from transport feedback cadence rather than a fixed tight poll. The
fallback MUST be no finer than the carrier timer granularity and SHOULD be
capped by the QUIC max-ack-delay-scale feedback window so missed notifications
do not stall the sender. It MUST NOT be the primary pacing mechanism for a
high-rate reliable stream. This rule follows the same ownership split as QUIC
and MPTCP: product bytes remain in the sender queue while the carrier is full,
but the byte-producing side is credit-clocked by actual carrier progress instead
of by an unrelated timer.
The blocked state is derived from the current front queued item and current
carrier credit, not from whether an earlier dispatch attempt already failed.
When the front item has no eligible carrier credit, the sender service is
blocked immediately, stops reading more product bytes for that flow, subscribes
to carrier-capacity wakeups, and arms only a short fallback retry. Treating that
state as runnable until a failed dispatch installs a retry timer is
non-conformant because it makes throughput depend on event-loop timing or
diagnostic logging delay rather than on carrier progress.
Validation, repair, standby, and failover attachments MUST NOT suppress the
active service path's carrier-credit visibility. A readiness/wakeup predicate
answers only whether some eligible output could accept the queued lane now; it
does not choose the final path, promote validation, or bypass ECF/BLEST
admission. Ordinary unique data readiness therefore considers active or
admission-eligible non-repair outputs, while the dispatch planner remains the
single owner of lead selection, validation policy, repair policy, and
stream-ordering-debt checks.

Once a carrier writer is selected by its event loop, it SHOULD drain a bounded
run of already-admitted commands before yielding back to the event selector.
This writer-feed run is not a sender-service admission pass: every command in
the queue has already passed lane priority, flow control, path admission, and
carrier-credit checks. The writer-feed budget MAY be larger than one
sender-service quantum so that TCP and QUIC/BBR remain fed, but it MUST remain
bounded, MUST NOT create product-flow fairness or lead-path ownership inside the
writer pipe, and MUST continue to prefer control and priority commands over data
commands between drained items. It MUST yield when the run budget is exhausted,
the command queues are empty, or the carrier reports backpressure or closure.
TCP writers MAY flush once at the end of such a run instead of after every
product frame. This is the degenerate single-path form of the sender-service
model: feedback remains timely, but the byte-producing side is not limited to
one frame per select wakeup when a healthy carrier has queued work. A narrower
writer-feed quantum is valid only when diagnostics show it reduces delay without
underfeeding the carrier; fixed one-frame writer-feed behavior is not a
protocol requirement and can underfeed high-rate QUIC UDP.

The sender-service executor follows the same bounded-run rule at the boundary
above path command queues. After it dispatches a non-empty ordinary bulk run to
carrier command queues, it MUST yield cooperatively before taking another
ordinary bulk run. This gives carrier writers, product ACK processing,
flow-control updates, and path metrics a scheduling opportunity without relying
on diagnostic logging, stdout/stderr backpressure, or other accidental delays.
For ordinary `STREAM_DATA`, each emitted frame is one adaptive product quantum.
A sender-service bulk run MAY contain multiple such quanta up to a bounded feed
window, then MUST yield before another ordinary bulk run. The 512 KiB
read-buffer ceiling and path-flight envelope are resource ceilings, not
per-frame payload sizes.

A carrier event loop MUST NOT let ordinary data commands outrank already
available inbound feedback. The loop first services ready control and priority
commands, because local ACK/control/latency work must bypass bulk. If no such
command is ready, inbound carrier frames that may contain product ACKs, stream
credit, resets, path metrics, datagram feedback, or close signals are selected
before ordinary data commands. Only after those feedback opportunities are not
ready may the loop drain throughput data commands. This ordering prevents a
continuously ready bulk queue from delaying ACK/credit processing and inflating
repair or product-flight debt while preserving the higher-priority local
control path.

Feedback that can release sender work is also a send wakeup. After a carrier
loop processes an inbound product ACK, stream credit update, path metric,
datagram feedback, path status update, or other frame that can release product
flight, repair state, flow-control credit, validation state, or path admission,
it MUST attempt one bounded drain of already-admitted carrier commands before
returning to an idle selector wait. The drain uses the same priority ordering
and service envelope as the ordinary writer run. This rule prevents an
ACK-heavy receiver loop from repeatedly consuming feedback while newly released
response bytes remain queued until a separate command event wins the selector.
It does not authorize unbounded sending, does not let data outrank feedback, and
does not convert ACK timing into a delivery-rate estimate; it only connects
feedback progress to the sender-service emission gate.

For server response streams, `STREAM_ACK`, `STREAM_MAX_DATA`, and `STREAM_FIN`
are queued sender-service work. A target-read or path-receive handler MUST NOT
write them directly to a carrier queue. `STREAM_ACK` and `STREAM_MAX_DATA` use
the product control lane. `STREAM_FIN` uses final-close staging: it can use the
carrier priority queue once dispatched, but it remains behind already-owned
same-direction data and repair until the final offset is safe to expose.
Queue-full is sender-service backpressure.

Session and path handshake traffic is a separate ownership domain until a
product stream or datagram flow has been admitted. `SESSION_HELLO`,
`SESSION_AUTH`, `SESSION_READY`, `PATH_JOIN`, `PATH_STATUS`, `PATH_METRICS`,
`PATH_DRAIN`, `PATH_CLOSE`, `PING`, `PONG`, and the immediate accept/reject
responses to `OPEN_STREAM` or `OPEN_DATAGRAM_FLOW` are owned by the
session/path manager. These frames MAY be emitted by the handshake writer or by
the sender-service control gate, but they MUST NOT carry target payload bytes,
ordinary repair data, validation bytes, or throughput work. Once a stream or
datagram flow is admitted, its product data, product feedback, repair,
flow-control credit, FIN, RESET, and DETACH work is owned by the sender-service
lane model described here.

The service maintains separate logical lanes in this priority order:

1. carrier ACK-only feedback;
2. product control, stream ACKs, connection credit, RESET, DETACH, and
   final-close FIN when same-stream data and repair ahead of that final offset
   have drained;
3. latency or tail-critical gap repair;
4. latency-sensitive stream data and realtime datagrams;
5. throughput stream data;
6. throughput repair;
7. background work.

Carrier ACK-only feedback is a carrier responsibility and bypasses the product
scheduler as described in Section 11.3. All other product work enters the
sender service. A saturated throughput lane MUST NOT prevent control, ACK,
latency, or repair lanes from making progress. Throughput lanes use
deficit-round-robin style service across flows so a later bulk transfer
gradually shares capacity with an earlier transfer.

Initial reliable-stream carrier selection is part of the same sender-service
contract. When both TCP and UDP underlay paths are configured, a new stream
MUST NOT be opened on TCP merely because TCP paths are stored or attempted
first. The sender chooses the initial lead carrier from the path model using
the stream lane, health state, configured path capabilities, RTT, delivery
rate, queue/inflight debt, and lane-protection pressure. The selected path is
then opened through the corresponding TCP or UDP carrier engine. UDP-only and
TCP-only deployments are degenerate candidate sets of this rule.

Endpoint-only startup uses cautious evidence handling before cross-carrier
sorting. Probe-only RTT or rate samples MUST NOT by themselves make a path
steal the first reliable stream when no product delivery evidence exists, and
tiny carrier/accounting differences such as path-proof bytes, ACK/control
flights, or command-queue noise MUST NOT reorder an otherwise unknown
endpoint-only latency-started candidate set. In that exact no-load/no-delivery-evidence
state, configured path order is only a deterministic startup fallback; it is not
a throughput preference. If fresh latency opens are already active but no
sender delivery evidence exists, active startup load MAY spread new opens away
from busy paths, but the remaining tie-breaker is still configured fallback
order; probe bytes, ACK/control frames, command-queue debt, or zero-byte
differences MUST NOT make a later unknown endpoint outrank an earlier equally
eligible endpoint. Delivery-backed evidence discards this fallback. A path
already serving realtime or latency-sensitive work is then scored by the same
active-flow, queue, RTT, loss, and delivery-rate model regardless of whether the
carrier is TCP or QUIC UDP. This is not a manual mode or fixed traffic class:
fresh opens are latency-first, sustained demand may move toward larger measured
bandwidth, and the sender can shrink back to latency/realtime behavior when
that demand disappears. The intent is to preserve lane isolation without
hardcoded TCP-vs-UDP preference.

When a path has path-scoped product delivery evidence or carrier delivery
evidence, those observations remain valid startup inputs even after the path has
gone idle. The endpoint-only startup filter only suppresses probe-only liveness
noise; it MUST NOT discard delivery-backed RTT, rate, loss, or queue evidence
and fall back to configured order.

The DRR service quantum for throughput data is the actual preemptible
sender-service packet quantum selected from live BDP, stability, queue pressure,
and the configured read/payload envelope. It is independent from the 512 KiB
default TCP read-buffer ceiling. Larger local reads may be split into multiple
service quanta; batching or vectored writes may reduce syscall overhead, but
they MUST NOT remove lane preemption points.

Gap repair is often encoded as `STREAM_DATA` because it carries the same stream
offset bytes as original transmission. Its scheduling lane is nevertheless
repair priority, not the original stream's throughput lane. Implementations MUST
NOT leave repair `STREAM_DATA` behind already-enqueued ordinary bulk data on the
same path when a receiver has an active ordering hole. Repair generation itself
is also preemptible: one ACK gap, path failure, or stall event MUST NOT emit an
unbounded set of cached chunks. It emits at most the adaptive repair quantum,
normally an MSS-to-latency-quantum-sized byte range, and later progress or stall
events may emit subsequent ranges. ACK handlers and stall timers create repair
work items; receive-hole detectors send timely receive-progress ACK/credit so
the peer can create gap repair from authoritative stream ACK ranges. They do
not send repair frames through the ordinary stream-data branch and they do not
call carrier/path send APIs directly. Path-failure handlers follow the same
rule: a failed path may identify unacknowledged ranges that require reinjection,
but those ranges become queued repair work, not immediate writes. This applies
symmetrically to request and response directions. The sender service dispatches
those repair items through the repair lane and records their path-flight
ownership separately from ordinary throughput data.

The sender service separates send quantum from send rate. This distinction is
essential for user-space encrypted proxying: very small bulk frames can consume
CPU, syscalls, wakeups, and AEAD setup before the path reaches capacity. The
service therefore uses BBR-style send-quantum reasoning. A throughput quantum is
large enough to amortize processing cost on a healthy path, bounded by the
configured relay envelope, and reduced only when the path model shows actual
instability or queue pressure. Pacing, inflight, and flow-control gates still
bound how much data may be outstanding.

Throughput quanta are preemption points. A carrier writer MUST NOT turn the
carrier record size into the product scheduling quantum. For QUIC reliable
streams, the writer MAY serialize one product `STREAM_DATA` quantum as several
length-prefixed carrier records with consecutive offsets so the receiver can
release earlier product ranges before the tail record is recovered. That split
MUST preserve product ownership, offset order, FIN placement, and stream fairness;
it MUST NOT reduce local reads, sender-service dispatch, or flow-control credit
to the carrier record size. This keeps QUIC packet recovery per-packet while
application fairness remains at bounded product-quantum boundaries.

At a packet-run boundary, backlog ordering follows the sender lane order rather
than raw arrival order. Single-packet stream frames, stream ACKs, datagrams,
control frames, and close/reset work from other streams are serviced before a
throughput continuation only up to one bounded urgent slice. The urgent slice is
derived from the safe carrier packet payload budget; it is large enough for at
least one queued urgent command, but it MUST NOT drain an unbounded urgent
backlog ahead of the continuation. Ordinary throughput from other streams
remains behind the current fragmented product-frame continuation, and later
frames from the current stream remain behind that continuation. This rule is
deliberate: urgent work can avoid user-visible queueing behind bulk, while
incomplete bulk product frames are closed promptly so the receiver does not
accumulate long fragment-assembly holes. Fair sharing between bulk flows is
enforced by bounded product-frame quanta and DRR/ECF admission at frame
boundaries, not by letting unrelated bulk streams or a sustained urgent backlog
indefinitely overtake the missing tail of one partially sent product frame.

QUIC batching, generic segmentation offload, and platform-specific send
coalescing are allowed only as carrier implementation optimizations. They do
not create a mptunnel UDP packet format. The sender service may hand a bounded
run of already-admitted product frames to the QUIC implementation, and QUIC may
packetize, pace, segment, or coalesce that work according to its own transport
state. mptunnel MUST NOT rely on a platform segmentation primitive for protocol
correctness, MUST preserve sender-service preemption before admitting the next
product-frame run, and MUST treat QUIC packet ACK/loss/PTO as carrier telemetry
rather than product stream delivery.

The sender service owns queued-but-not-sent product bytes. The stream repair
cache owns unacknowledged stream ranges. The path flight ledger owns the mapping
from stream ranges to the last path that carried them. The receiver flow-control
state owns advertised stream and connection credit. A QUIC carrier owns UDP
packet bytes in flight, congestion-window or inflight-high state, pacing state,
PTO state, and ACK-derived delivery samples below the product stream. TCP path
state owns encrypted frame write pressure and path-level inflight accounting. An
implementation MUST NOT count the same byte as free in more than one owner, and
MUST release ownership only from the corresponding ACK, loss, failure, expiry,
or local-delivery event.
For reliable streams, ownership moves in this order: raw source byte in the
sender queue, prepared dispatch candidate, repair-cache and product-flight entry
immediately before carrier visibility, carrier-command acceptance, stream-ACK
release, and finally contiguous-delivery evidence. A prepared candidate is not
durable ownership; a tail commit may be rolled back only when the selected
carrier rejects the command before the frame becomes visible to the peer.

Before a product data frame is emitted, all applicable gates must pass:

* stream or datagram freshness and target policy allow the frame;
* stream and connection flow-control credit allow the byte range;
* sender queue budget and repair-cache budget allow retained state;
* the selected path is healthy enough for the lane;
* the selected path passes ETA/admission checks for the frame;
* the carrier writer has bounded queue capacity for the packet or frame, except
  for the explicit ACK-only and PTO exceptions in this specification.

For an active reliable QUIC UDP data owner, QUIC carrier congestion state is
sender evidence and carrier-owned pacing state, not a second hard product
flight gate. The product scheduler MUST keep product flight, repair cache,
sender queues, and ordering debt within the mptunnel resource envelope, and it
MUST use QUIC bytes-in-flight, queue, RTT, pacing, and loss as ETA/admission
inputs. It MUST NOT stop feeding the active ordered-stream owner solely because
the QUIC carrier reports bytes in flight at its inflight limit while the carrier
writer still accepts bounded work. QUIC itself remains responsible for packet
pacing, congestion-window enforcement, ACK/loss/PTO, and stream-level
backpressure below the product frame.

For reliable bulk streams, the sender service also performs admission before it
pulls another source byte range into a `STREAM_DATA` frame when the next offset
and candidate service quantum are known. Read loops may stage raw bytes only up
to the sender-service queue, stream-credit, and repair-cache budgets. They MUST
NOT pre-create later-offset frames because doing so would assign product
ownership before path admission and repair priority are known. If the
path-flight ledger shows that
lower offsets are outstanding on other paths and no attached path can safely
advance the ordered frontier, the sender pauses the source read and continues
servicing control, ACK, repair, latency, and carrier events. It MUST NOT create
new later-offset `STREAM_DATA` merely to keep an active path busy, because doing
so moves the fairness boundary behind hidden path queues and expands receiver
ordering debt before ECF/BLEST admission can reject it.

If any gate fails, the service either chooses another eligible path, keeps the
work queued, reduces send pace, marks a path suspect, or drops expired
best-effort datagrams. It MUST NOT bypass flow control or carrier inflight
accounting to preserve short-term throughput.

The service is deliberately narrow. It does not replace UDP loss recovery,
stream ACK handling, or path health. Instead, it is the point where their
outputs meet. This mirrors mature designs: MPTCP separates data-sequence
mapping from subflow scheduling, QUIC lets one congestion controller arbitrate
packet sending for streams and datagrams, and BBR-style control needs a single
sender-side view of delivered bytes, inflight bytes, and pacing. Without this
service contract, independent correct components can still create a wrong
system by double-queueing, delaying ACKs behind bulk, overfilling a slow path,
or replaying repair outside the measured send loop.

## 18. Multipath, Failover, and Roaming

### 18.1 Bulk Assignment and Striping

For bulk reliable streams, the scheduler maintains a small subflow set epoch for the
current flow: one Service owner plus Subflow members admitted from live ETA,
flow sharing, health, and capability state. Startup Subflow samples are bounded
and require path-scoped sender evidence; steady-state Subflow owner bytes require
bulk-rate evidence. Individual dispatches consume credit from that set; they do
not recreate validation credit from scratch, and ordinary ACK progress does not
reset spent startup Subflow credit. ACKs update the per-range flight ledger,
delivery samples, and the next admission calculation; detach, carrier close,
failover, or a changed Service/envelope resets the subflow set. Additional paths
attached to the same stream are not automatically ordinary data paths. Their
role decides what the scheduler may do: Repair paths carry gap-targeted repair
or failover repair, Validation paths may receive bounded proof traffic, and the
Service path may carry ordinary data. A path with any role may carry a specific
repair frame when it is the best survivor and
avoids the path that likely lost the original bytes.

Same-stream bulk striping is allowed for TCP, UDP, and mixed TCP+UDP reliable
streams only when the candidate passes the same admission rule used for the
lead path. This is intentionally stricter than "all attached paths may send."
TCP hides packet-level loss and delivery timing, while UDP exposes packet
numbers, ACK ranges, pacing, and loss state; the path model therefore uses the
best available sender evidence for each underlay and refuses candidates whose
modeled arrival would increase completion time or receiver reorder debt. This
follows the MPTCP ECF/BLEST lesson: connection-level sequence numbers make
striping possible, but the scheduler must still avoid subflows that create
head-of-line blocking.

Validation is the bridge between conservative startup and useful aggregation.
An unknown path does not join ordinary same-stream bulk merely because it is
open, but a Validation attachment can receive path-scoped proof traffic.
Path proof creates liveness/sender evidence only; it is not product delivery
proof and does not itself make the path a bulk subflow. Same-family proof paths
MUST NOT receive steady-state product `OwnerData` until they have bulk-rate
evidence. They MAY receive only the bounded clear-frontier startup Subflow
`OwnerData` window described above, and only after the current Service has
direction-correct bulk-rate evidence. A path that lacks bulk-rate evidence and
has no remaining startup Subflow credit stays `Probe`, `Standby`, or
`RepairOnly`.
The bulk-rate evidence floor is byte-counted, but it MUST tolerate one
packet-scale accounting slack around the startup graduation threshold. QUIC ACK
accounting, stream segmentation, and product-frame boundaries can differ by a
small number of bytes; such slack must not decide whether a path is permanently
excluded. This slack does not make tiny ACK-data samples bulk-rate evidence.
Validation attachment is triggered by bulk
demand and path admission; it MUST NOT depend on the sender having outbound
repair bytes.
This matters for ordinary downloads, where the client may have little or no
outbound data after the request while the server-to-client stream is clearly
bulk. If QUIC carrier-ACK metrics, configured path hints, or other path-scoped
sender evidence yield direction-correct bulk-rate evidence, the path can compete
in the ordinary ECF/BLEST subflow set. If it does not, the path remains excluded
except for failover, explicit repair, or another bounded validation event after
the admission envelope is refreshed.

Reliable path membership uses explicit roles. An attached output starts as
`Standby` or `Probe`, not as a data owner. `Service` is the current ordered-owner
anchor and MUST remain the scheduler baseline while it is live and healthy. If
the current Service output is detached or closed, the binding clears the live
Service owner key instead of promoting a survivor from output membership. While
ordered-owner scheduling debt remains, that absent-owner state is a
wait/repair/failover state, not permission to send later `OwnerData` on another
path. Once the contiguous frontier is clear, sender-service admission may choose
the best measured live output as the next Service. Output-list tail position,
validation attach order, and recent repair selection are not ownership signals.
A measured Subflow may compete for owner credit, but its existence MUST NOT
hide or invalidate the current Service as a lead candidate; a blocked optional
Subflow creates backpressure or
Standby/RepairOnly state, not a stream with no admissible Service. `Subflow` is
an additional owner path admitted by the same no-worse completion and
ordering-debt model used for the Service path. There is one Subflow owner
admission mode: direction-correct bulk-rate evidence plus no-worse completion,
ordering-debt, queue, and overhead guards.
`RepairOnly`, `Standby`, and `Failed` outputs cannot receive speculative owner
bytes. Role transitions are monotonic with evidence and carrier state for the
current decision; they are not implied by attachment order, carrier family,
configured path order, or temporary queue availability. In particular, `Probe`,
path-proof-only, and sender-evidence-only paths are not permission to carry an
unbounded stream of future offsets. The only exception is explicit
frontier-clear Service failover after the previous Service is gone; that
exception elects one new Service path and remains subject to ordinary Service
feed/admission limits.

Validation admission is evaluated with the bounded proof payload, not with the
full product path inflight envelope. This keeps validation aggressive enough to
learn new paths while preventing validation churn from consuming the same budget
as established bulk data.

Validation attachment adds a path-manager output; it does not remove or hide the
active service output. If the active output still has carrier credit for the
queued lane, the sender service MUST remain wakeable even while validation proof
traffic is pending on other outputs or replacement control is in progress.
Product-source backpressure and sender target snapshots MUST use the same live
Service owner identity as ordinary OwnerData scheduling; using output-list tail
position as a second active-owner model is stale and invalid. This prevents
optional validation from turning a usable active path into an apparent
sender-service starvation state.

Validation lifecycle is path-manager state, not per-quantum data-scheduler
state. Once a reliable product stream has an attached carrier output for a path,
the sender MUST NOT repeatedly open new carrier streams for that same
stream/path merely because a rebalance timer fired, an ordinary receive hole is
present, or a previous validation copy has not yet become ordinary bulk
evidence. The scheduler may re-evaluate whether the attached output is admitted
for ordinary, repair, or validation work, but it MUST reuse the existing output
until the carrier closes, an explicit detach is sent, or carrier-level failure
requires failover. Product-level receive holes, stream ACK gaps, or delivery-rate
oscillation are not path-membership failures by themselves. This mirrors MPTCP
subflow management and MPQUIC path management: path discovery and validation
create stable path membership, while packet or stream scheduling decides how
much work to place on each member.

Bulk validation is incremental. A reliable product stream may have at most one
new validation open pending at a time, and an immediate bulk attach pass stops
after the first successful validation attachment. Later passes may attach the
next metric-ordered candidate after the previous result is known. This preserves
carrier-diverse validation without turning one bulk stream into a cross-product
of simultaneous stream opens, proofs, duplicate bytes, and repair obligations.
For a given product stream and carrier path, validation/probe attachment is a
one-shot path-manager attempt. A path that has already been attempted for that
stream MUST NOT be reopened by prevalidation or rebalance simply because the
previous validation handle closed, failed to graduate, or stopped being attached.
The next action is a scheduler decision over the current path set: keep the
path as `Probe`/`Standby`/`RepairOnly`, wait for new evidence, or use another
candidate. A later product stream may probe the path again, and explicit
failover recovery may open a survivor when required for correctness, but normal
bulk validation MUST NOT create repeated same-stream `OPEN_STREAM` churn.

Mixed TCP+UDP validation MUST be carrier-diverse without carrier-family
prejudice. When an admitted validation subflow set contains both TCP and UDP
underlays, the sender SHOULD attempt the best currently admissible candidate
from each carrier family before spending later validation attempts on additional
same-family candidates. "Best" is determined by path metrics, capability flags,
queue/flight debt, validation credit, and lane demand; it is not determined by
whether the carrier is TCP or UDP. Both TCP and QUIC UDP carriers support the
explicit path proof frames defined below, and QUIC UDP carrier ACK metrics are
useful only when they are local, direction-correct, and ACK-derived. This
carrier-diverse validation rule prevents a slow or blocked proof track in one
family from hiding a useful path in another family while preserving the rule
that validation proof is not ordinary bulk capacity.

Validation credit is separate from validation admission. Admission decides one
preemptible proof quantum at a time. Credit bounds the total amount of proof
traffic that may be sent before sender-side delivery evidence exists. Initial
validation credit is deliberately small and multi-frame rather than a full BDP
grant. A path MUST NOT receive a large speculative validation flight merely
because a hint suggests high bandwidth, since that can create the same
ordered-stream head-of-line debt the ECF/BLEST admission rule is designed to
avoid.

The validation proof quantum is bounded by the latency/preemptible startup
quantum, not by the bulk read-buffer ceiling and not by the full bulk striping
decision quantum. This keeps validation visible to the scheduler and lets ACKs
arrive quickly enough to prove or reject the path without making an unproven
path responsible for a large unique ordered-stream range.

Validation for ordered reliable streams is non-blocking and path-scoped. A
validation byte range that is sent only on an unproven path can itself create
the ordered-stream hole being measured, so the sender MUST NOT treat validation
credit as ordinary bulk capacity. When an ordered-data owner exists, validation
does not compete to become the primary owner of the next unique ordered byte
range. Instead, the sender duplicates the same `STREAM_DATA` on an admitted
ordinary path and the validation path, sends repair for an already-missing
range, or sends carrier/control probes that do not create a new
application-data dependency. This follows QUIC path validation and
MPTCP/MPQUIC reinjection practice while adapting it to a product-layer stream
that must avoid creating irreversible receive-hole debt.

`PATH_PROOF_DATA` and `PATH_PROOF_ACK` are the carrier/control proof mechanism
for this purpose. `PATH_PROOF_DATA(path_id, proof_id, payload)` carries bounded
opaque proof bytes on the attached carrier output. It is not product
`STREAM_DATA`, has no stream offset, does not enter the repair cache, and MUST
NOT become an ordering-frontier owner. A peer that receives fresh proof data on
the matching path replies immediately with `PATH_PROOF_ACK(path_id, proof_id,
payload_bytes)` on the same carrier. The sender records the proof send time and
payload length; a matching proof ACK creates path-local liveness, RTT, and
proof-byte-rate evidence for that carrier direction. It is still
control/proof-plane evidence, not ordinary bulk-rate evidence: it MUST NOT by
itself make the path a unique ordered `STREAM_DATA` owner when another admitted
ordinary path exists. Unknown, duplicate, or stale proof ACKs are ignored. Proof
payloads are bounded by the latency/preemptible startup quantum and by the frame
payload envelope.

Same-underlay validation is still subject to this rule. A path that uses the
same underlay family as the lead path may be cheaper and safer to validate than a
cross-underlay path, but it MUST NOT receive the only copy of a new future
ordered byte range before path-local sender evidence exists. If the sender wants
to spend traffic to prove that path, it sends duplicate `STREAM_DATA`, repair for
an already-missing range, or carrier/control proof traffic. Duplicate validation
copies MUST be recorded as non-owners of the ordered frontier so they can release
carrier/product flight on ACK without making the validation path the lower
frontier owner for later unique bytes.

Validation path opens are also non-blocking with respect to the byte-producing
sender path. A target-read loop, local-read loop, sender-service drain, ACK
handler, or carrier writer MUST NOT await a long-running validation open before
continuing ordinary work on already admitted paths. Validation opens run as
bounded path-management tasks; when one completes, the sender may attach the
path as Validation state and include it in later admission decisions. If it
fails or times out, that result is path evidence, not a failure of the product
stream. This mirrors MPTCP path managers and MPQUIC path validation: subflow or
path discovery proceeds beside the connection-level byte stream rather than
inside the hot data loop.

A stream ACK for duplicated data proves end-to-end byte delivery but does not
identify which underlay path delivered the bytes. It therefore releases product
flight for every duplicate copy of that range, but it MUST NOT by itself promote
the validation path into ordinary same-stream bulk service. Path proof ACKs are
validation/liveness evidence. QUIC UDP carrier ACK metrics and unpolluted
admitted stream-delivery samples are the sender-side bulk evidence that may make
a path eligible for unique ordered bulk ownership. Ordered-stream validation
payload MUST NOT be the only copy of a new future offset while any admitted
ordinary path exists; if the sender spends product bytes for validation, those
bytes are duplicate `STREAM_DATA` or repair for a known missing range.

Response-side validation uses the same principle. The server MUST NOT schedule
download bytes onto a validation path merely from generic TCP or UDP defaults,
but it MUST send bounded `PATH_PROOF_DATA` on validation attachments so TCP and
QUIC UDP outputs can gather local sender evidence without consuming unique
ordered response bytes. Before proof succeeds, a validation output remains
excluded from ordinary unique response `STREAM_DATA` except for duplicate proof
or gap-targeted repair. After path-scoped sender evidence exists, it can become
the single Service failover only when the prior Service owner is gone and the
ordered frontier is clear; it still cannot become an optional Subflow owner
while another Service/lower owner has unresolved bytes.
Client-supplied `PATH_METRICS` are hints, not final proof of response-direction
throughput. They are useful to distinguish a plausible
high-bandwidth path from a poor or high-loss path before bounded proof is sent,
but sender-side evidence decides sustained ordinary promotion. The receiver
applies the same rule when it observes incoming stream data: ordered progress on
a validation or repair path may refresh liveness and may feed delivery sampling,
but the path becomes a unique ordered-data candidate only after that sampling
has created real delivery evidence and ETA scoring says it should displace the
current lead path. This prevents a high-RTT, high-loss, or reordered path from
winning ordinary bulk service because it delivered a small probe before its
long-term behavior was known.

For UDP underlays, the response sender also maintains local carrier TX metrics
from its own UDP packet controller. Once the server has ACK-derived carrier
delivery samples for a UDP path, those sender-side metrics take precedence for
response scheduling over peer hints and over ordered stream-ACK timing alone.
Stream ACKs still release product flight and prove end-to-end stream progress,
but they MUST NOT initialize, raise, or replace the UDP/QUIC carrier delivery
rate or RTT model. Ordered stream ACK timing can be delayed by receiver reorder
holes, product queueing, and application flow-control, so using it as UDP carrier
rate evidence can inflate product queues or collapse pacing independently of the
actual QUIC packet controller. It is product evidence only: it may release repair
state, update contiguous-progress diagnostics, validate that some copy of a byte
range reached the peer, and maintain a product-progress rate used only to bound
source-read and product-backlog horizons. That product-progress rate MUST NOT be
exported as UDP/QUIC carrier delivery rate, MUST NOT replace packet ACK-derived
congestion evidence, and MUST NOT drive QUIC pacing. This mirrors QUIC and BBR
practice: congestion and pacing decisions are sender-side and packet/path
scoped, while stream ordering and product backlog are separate correctness
layers.

When the UDP production engine is QUIC, the response sender MUST preserve both
ACK-derived delivery rate and QUIC pacing/cwnd-derived pacing rate in its path
snapshot. Application-limited ACK samples MUST NOT initialize or reduce the
bulk delivery-rate model to a tiny value. The sender keeps separate facts:
carrier ACK-derived data seen, carrier non-application-limited bulk-rate
evidence, and product-ledger owner progress. Carrier ACK-derived data seen proves
that the path carried carrier data and keeps it visible to admission policy, but
it does not by itself make the path eligible for ordered `STREAM_DATA`
ownership. Local QUIC pacing remains carrier-owned scheduling evidence even when
the latest ACK-derived data sample is application-limited; app-limited status
only prevents that sample from becoming a delivery-rate proof. The carrier
ACK-derived rate becomes bulk-rate evidence only after the acknowledged DATA byte
volume is large enough for the path's modeled flight envelope, with two bounds.
The floor MUST be at least a small multi-packet DATA sample so a tiny ACK burst
cannot create a bulk-rate Subflow, and it MUST be capped by a bounded startup
graduation window so a large transient QUIC cwnd/inflight estimate cannot make
proof self-defeating by requiring more bytes than the product scheduler will feed
before graduation. Otherwise the sample remains ACK-data evidence for validation
visibility only.

Product-ledger owner progress is also path-scoped bulk-rate evidence when the
ACKed range had exactly one outstanding `OwnerData` copy and the release handler
records a product progress rate for that owner. This does not conflict with the
RepairData rule: duplicated repair ACKs never increment owner delivery samples
and never create product-owner progress for the repair path. ACK-data seen does
not set bulk-rate evidence, does not overwrite the delivery rate, does not
rewrite the ordinary lead, and does not permit the path to own later offsets
while another path owns unresolved lower bytes.

Validation outputs do not receive product `STREAM_DATA` for discovery. Attached
but unproven paths use `PATH_PROOF_DATA`/`PATH_PROOF_ACK` and control traffic
for bootstrap. A same-family Subflow may receive only the bounded clear-frontier
startup `OwnerData` window described in the Service/Subflow rules before
bulk-rate evidence exists. After that window is spent, a Subflow may receive
unique owner bytes only after path-scoped bulk-rate evidence exists and the
no-worse admission model accepts it.

The production SafeBestPath guard separates two debt ledgers. Ordered-owner
scheduling debt is any bulk `OwnerData` suffix below the sender's highest owner
offset that the peer has not yet acknowledged contiguously. Authoritative repair
debt is narrower: an explicit ACK-range gap, a failed/detached owner tail, a
persistent live-owner tail with alternate-output repair evidence, or a known
final tail with persistent stall evidence. Ordered-owner scheduling debt MUST
be passed into Service/Subflow admission so an alternate cannot own later bytes
behind an unresolved lower owner. Authoritative repair debt alone may create
`RepairData`.

Normal unacknowledged `OwnerData` retained in the repair cache is carrier
recovery state for repair purposes, but it is still owner scheduling pressure
while the ACK frontier is behind the sender's highest owner offset. Treating
every retained repair-cache byte as immediate repair would create duplicate
storms; treating the same contiguous suffix as zero scheduling debt lets
cross-underlay Service migration create large receive holes. The sender MUST
wait, keep feeding the current Service owner when safe, or admit only a
candidate whose ordering-debt input passes the normal no-worse checks. A
remembered Service owner that is absent, or an unknown Service owner ledger while
ordered-owner scheduling debt remains, is not a hint to elect another Service
path; it is a wait/repair/failover state until lower ownership is resolved or
the debt is converted into explicit `RepairData`. A
contiguous ACK frontier that stops advancing becomes `RepairData` only after
carrier failure, detach, or known-final-tail evidence makes repair a correctness
action. Repair overlap avoidance may inspect the full product-flight ledger
separately, but that ledger is not itself repair debt.

Proof-only and unmeasured candidates remain `Probe`, `Standby`, or `RepairOnly`
until ordered-owner scheduling debt falls below pressure or explicit
loss/failure/final-tail evidence converts the affected range into `RepairData`.
A mixed-family path is owner-eligible under debt pressure only when it is
bulk-rate-proven and already owns the lower outstanding range. The surviving
OwnerData candidates still pass the normal
ECF/BLEST-style no-worse checks for ETA, inflight, ordering debt, read-gap,
queue, overhead, and completion horizon. This prevents per-quantum ETA changes
from turning an unhealthy flow into cross-family owner migration, receive-hole
growth, and repeated duplicate traffic without treating a stalled Service
owner's carrier queue credit as proof that the product frontier is safe. Optional
paths may still carry `Probe`, `Control`, and gap-targeted `RepairData` that is
justified by explicit evidence.

For mixed TCP+QUIC reliable streams, production v1 uses a stricter same-family
`OwnerData` rule under lower bytes owned by another family. A product stream
MUST NOT stripe later ordinary `OwnerData` onto the other carrier family merely
because both paths have proof or short-term rate samples. MPTCP subflows share
TCP recovery semantics and MPQUIC paths share QUIC recovery semantics;
mptunnel's TCP and QUIC reliable-stream carriers have independent ACK clocks,
pacing, flow control, and loss recovery. Cross-family paths therefore remain
`Probe`, `RepairOnly`, or `Standby` while they would expand unresolved
cross-family lower-byte debt. If the mixed candidate already owns the lower
outstanding range, continuing that candidate does not expand the ordered hole
and remains eligible when the path is bulk-rate-proven or is the live active
path currently responsible for that lower frontier. Mixed-family health filters
MUST NOT remove the effective lower-frontier owner before lead selection; they
may only block optional paths that would expand the hole. At a clear ordered
frontier, mixed-family Service change is still an explicit migration/failover
decision, not a side effect of per-quantum ETA selection. This rule is
carrier-neutral: it blocks TCP-to-QUIC and QUIC-to-TCP speculative same-stream
ownership equally, while still allowing latency-first streams to move to the
measured best bulk carrier through the dedicated Service migration policy. That
policy requires bulk-sized direction-correct owner-byte evidence for the target
family: at least one current Service quantum of product `OwnerData` ACK evidence
or an equivalent non-app-limited carrier ACK sample. A tiny startup/probe-sized
sample can keep the path eligible for Probe/Subflow discovery, but it MUST NOT
move Service ownership across TCP/QUIC families.

ACK-data seen is a durable path-local fact derived from local QUIC ACKed bytes
after product `STREAM_DATA` or `DATAGRAM_DATA` was written on that carrier. It
MUST NOT require product TX and QUIC ACK to happen in the same sampling interval,
and it MUST NOT be inferred from path proof, stream ACK, MAX_DATA, or other
control-only frames. The QUIC path metrics publisher MUST report ACK-data-seen
to the product scheduler even when the sample is application-limited and has
zero non-app-limited delivery samples; otherwise a validation path can prove
real product-byte delivery but remain invisible to the graduation state machine.
That publication is only path-scoped data evidence: the path remains not
bulk-rate-proven until non-application-limited ACK-derived data samples exist.
Once such samples exist, bulk-rate proof is durable path evidence: a later
idle/application-limited metrics poll MAY mark the current snapshot as
application-limited for scheduling caution, but it MUST NOT erase the existing
bulk-rate proof or collapse the carrier feed envelope back to startup-only
credit. The first accepted non-application-limited QUIC data sample MAY raise
the sender's path-rate model when it exceeds the current startup/cwnd/pacing
fallback, but it MUST NOT initialize the model below that fallback; otherwise
one underfed validation quantum can permanently classify a useful path as slow.
Until a non-application-limited data sample exists, and until the local QUIC
stack exposes usable pacing or congestion-window capacity, ACK progress MUST
NOT become bulk delivery-rate evidence. MTU is packet sizing evidence, not bulk
capacity evidence, and MUST NOT by itself initialize the QUIC/UDP delivery-rate
model. The scheduler MAY use the QUIC pacing/cwnd rate or the normal UDP
startup model for bounded admission, but it MUST keep the app-limited or
capacity-unknown provenance visible to diagnostics and admission.
Before a non-application-limited ACK-derived data sample exists, a QUIC
pacing/cwnd value that is below the normal UDP startup model is carrier-local
startup state, not product-scheduler bulk capacity proof. The product scheduler
MUST NOT export such a tiny value as the path delivery or pacing rate; it MUST
use the startup model as the bounded admission floor while the QUIC carrier
itself remains responsible for actual packet pacing and congestion control.

For any same-stream bulk striping, the scheduler chooses eligible paths from
live ETA. Eligibility requires active or sufficiently confident suspect state,
no probe-only/backup restriction unless necessary, acceptable inflight/queue
pressure, and explicit admission against the best next path. A path MUST NOT
join a bulk striping subflow set merely because it has available capacity.

A path is admitted for the next bulk chunk only if the implementation estimates:

```
lead_path = min_eta_candidate_that_is_eligible_and_admissible_for_ordinary_bulk()
if path is the lead path and stream_ordering_debt(path, chunk) == 0:
    product_queue_debt(path) + stream_ordering_debt(path, chunk) + chunk
        <= lead_product_queue_envelope(path, chunk)
else if path is the lead path:
    stream_ordering_debt(path, chunk) + chunk
        <= same_underlay_reorder_budget(path, chunk)
else if path uses the same underlay family as the lead path:
    product_reorder_debt(path) + stream_ordering_debt(path, chunk) + chunk
        <= same_underlay_reorder_budget(path, chunk)
else:
    carrier_queue_debt(path) + chunk <= carrier_validation_queue_limit(path, chunk)
    product_reorder_debt(path) + stream_ordering_debt(path, chunk) + chunk
        <= effective_reorder_budget(path)
if path is an additional data path:
    eta_p(chunk) <= completion_horizon(lead_path, path, chunk)
```

The additional-data completion rule is a measured-subflow gate, not a startup
validation gate. Sharing a carrier family, such as QUIC+QUIC or TCP+TCP, is not
proof that later offsets will arrive before the lead can send the next quantum;
therefore a bulk-rate-proven same-underlay path that wants ordinary unique
`OwnerData` MUST show positive incremental completion gain before joining the
ordinary bulk subflow set. A same-underlay path that has only proof,
low-confidence sender samples, or app-limited evidence is not rejected by this
measured completion-gain rule; it remains governed by probe admission,
owner-debt safety, and no-ordering-debt-expansion rules. Reorder budget
is a safety envelope for already-admitted work; it MUST NOT be used as extra
time slack to put unique ordered bytes onto a high-latency path that loses the
ECF/BLEST next-quantum comparison.

The lead path is a safe baseline, not merely the lowest raw ETA. A candidate
whose carrier or product debt already violates the active data-path admission
gate MUST NOT be used as the baseline that rejects other paths. Otherwise a
saturated path can prevent a proven alternate from carrying traffic while also
being unable to accept the next quantum itself. This rule is the sender-service
equivalent of ECF/BLEST comparing against the best usable subflow rather than
against an unavailable one.

Implementations MUST compute the lead from candidates that pass active
ordinary-data admission against their own current product and carrier debt
before evaluating additional paths. A raw lowest-ETA path, a previously active
attachment, or a round-robin cursor position is not a valid lead unless it can
accept the next ordinary quantum. If the oldest lower outstanding range has a
path owner, that owner remains responsible for the lower frontier until it
becomes admissible, is repaired, or ACK progress removes the lower-frontier
debt.

For each ordered reliable stream, lead choice is flow-level state, but it is not
a sticky override. When a lower-frontier owner exists, that owner is the only
ordered-data owner until it becomes admissible, is repaired, or ACK
progress removes the ordering debt. When the ordered frontier is clear, the
sender computes the eligible and admissible ECF/BLEST lead for the next quantum.
The previous ordered-data owner is only a hysteresis hint: it may remain selected
when it is still admitted and within measured jitter/queue hysteresis of the
best admissible candidate. It MUST NOT keep ownership over a substantially
lower-ETA, admissible, sender-evidenced path merely because it was the previous
path. Temporary carrier-credit or queue backpressure on the old owner is not a
reason to move unique later offsets to a harmful path, but neither is old-owner
attachment a reason to ignore a safer faster candidate when no lower-frontier
debt would expand. This preserves same-stream ordering while allowing
same-protocol path groups to aggregate instead of being pinned to stale state.
Successful emission of ordinary data on a non-lead carrier MUST NOT by itself
migrate the stream lead. Lead migration is a sender-service decision caused by
admissibility, frontier state, detach, failure, or explicit frontier-safe
reattachment; it is not a side effect of a validation, repair, or opportunistic
write succeeding. Independent bulk streams SHOULD keep independent leads when
the chosen leads remain admissible, so same-protocol path groups can share load
without creating same-stream ordering debt.
For request/upload bulk, reading from the local product source is also
preemptible. One relay task MUST NOT drain multiple bulk source-read quanta in a
single cooperative turn while other product flows are runnable. Bulk batching
MAY happen at the carrier writer through vectored writes or QUIC packetization,
but product-source reads and sender-service admission SHOULD yield after one
bounded service quantum so independent uploads converge toward fair sharing.
Additional paths still carry control, ACK, realtime, latency, duplicate
validation, and explicit gap-targeted repair; they become ordinary data paths
only through the same flow-level lead admission rule.

Path-scoped `STREAM_DETACH` is explicit product-control work. An implementation
MUST NOT hide `STREAM_DETACH` creation inside a generic local carrier close
helper. Normal ordered stream teardown MAY emit `STREAM_DETACH` on each
attached carrier path before closing that local carrier handle. Failure removal
of a carrier path SHOULD close the local handle and release local ownership
without trying to send new product-control frames over a path already marked
failed.

`carrier_debt` is the sender-visible network backlog: carrier bytes in flight,
carrier queue bytes, and locally queued carrier commands that are ahead of the
candidate chunk. `product_reorder_debt` is the stream-level byte ownership that
has not yet been released by `STREAM_ACK`. These are deliberately different
ledgers. QUIC and BBR gate packet emission and pacing on carrier debt, while
MPTCP-style sequence repair and receive-window protection reason about product
byte ownership. `product_queue_debt` is the lead path's bounded,
preemptible product work already admitted to the transport.

The active service output with a clear ordered frontier MUST NOT be gated by a
second product-layer copy of the carrier congestion window. QUIC already owns
packet pacing, congestion response, stream flow control, and sender backpressure
below the active owner; TCP already owns kernel write pressure, congestion
response, and packet pacing below its writer. The product scheduler gates this
case with the product queue and stream-ordering envelope so the service owner
remains fed without creating unbounded response backlog. This applies even when
other validation or subflow set outputs are attached: optional outputs do not reduce
the service owner's product feed budget. Additional paths, validation paths, and
cross-underlay candidates still use carrier debt as an admission gate because
they can create new reordering debt or probe traffic outside the active service
owner. An implementation MUST NOT use slow product-ACK release timing as a
carrier congestion window, MUST NOT use carrier ACK progress as proof that a
stream byte is no longer needed for repair, and MUST NOT treat the configured
product envelope as a floor above carrier credit for optional paths.
The product source-read horizon MUST NOT be capped by the carrier congestion
window, inflight-high, or send-window equivalent. Those values belong to the
carrier emission gate and to multipath admission, where they describe whether a
specific carrier path can accept another admitted quantum. They are not a
second product-layer receive window. On a single reliable carrier, applying the
same carrier horizon at the product source-read layer makes the byte-producing
side application-limited before QUIC or TCP can exercise its own pacing and
congestion control. The source-read horizon is therefore computed from the
path's sender-side delivery or pacing evidence, path quality, stream flow
control, repair-cache/resource envelopes, and configured product ceiling.
Ordered `STREAM_ACK` product progress MAY raise confidence or expose lag, but a
low product-progress sample MUST NOT downshift the source-read horizon below
credible carrier evidence. The product-progress rate remains a backlog and
diagnostic signal only; it MUST NOT be treated as UDP/QUIC packet delivery or
congestion evidence.

The sender-service admission model also applies session-level lane pressure.
When a session has active latency-sensitive or realtime flows, an active bulk
lead MUST NOT use the large throughput BDP envelope to accumulate hidden command
backlog behind path queues. Its product admission envelope is reduced to the
preemptible service horizon for the next quantum, while carrier pacing and
stream flow control continue to govern final emission. This rule is independent
of whether the latency-sensitive flow is attached to the same underlay path as
the bulk flow; otherwise dedicated latency paths can hide user-visible pressure
from the bulk scheduler and the path command queue becomes an unintended product
queue. When the session has only throughput/background flows, the model-based
BDP envelope remains available so file-download aggregation is not penalized.

Bulk admission also includes lane-protection debt. When another flow on the
same session path is currently using a control, latency, or realtime lane and
at least one flow on that path has already become throughput/background, the
sender charges that path with an adaptive latency headroom before it compares
ETAs, computes reorder budgets, or reads additional source bytes. This local
headroom is the amount of product work that must remain available for small
HTTP responses, SSH-like echo, carrier/product ACKs, FIN/RESET/DETACH, repair,
and realtime datagrams. It is derived from the latency lane's current modeled
inflight target for that path. Therefore a bulk stream may still use the path
when it is clearly best, but it must compete against proven alternate paths
after the protected latency work is accounted for. An all-startup condition
where all streams are still classified as latency does not create
lane-protection debt by itself; otherwise parallel downloads would reserve
against each other before demand classification has a chance to promote them.
This is the product-layer equivalent of QUIC/BBR keeping ACK/control feedback
out of bulk queues and of MPTCP schedulers avoiding subflows that increase
application-visible blocking.

`stream_ordering_debt(path, chunk)` is the sender's estimate of lower-offset
bytes in the same ordered stream whose latest outstanding copy is owned by
other paths. It is zero when the candidate path owns all lower outstanding
bytes relevant to the next chunk. It is positive when sending a later offset on
the candidate would move the receiver further ahead of bytes still expected
from another path. This value is part of admission, not a late repair-only
signal. MPTCP's data sequence mapping makes this distinction explicit: a
subflow can be locally healthy while the connection-level byte stream is still
blocked behind data mapped to another subflow. ECF/BLEST-style scheduling must
therefore include the existing connection-level ordering debt before it admits
a faster path for later bytes. In the current implementation, cross-underlay
ordinary striping is allowed only before it would extend an existing
connection-level ordering debt. Once later offsets would queue behind lower
bytes owned by the other carrier family, the sender either continues on the
path that owns the lower bytes, performs bounded gap-targeted reinjection, or
waits for ACK/path-state progress; it MUST NOT keep feeding later offsets to a
path that will expand the ordered receive hole.

`STREAM_ACK` processing maintains two product-side ledgers. Explicitly ACKed
ranges release repair-cache and product-flight state even when they arrive
above a lower missing range. They do not, however, prove ordered application
progress until the sender's contiguous ACK frontier reaches those bytes. ACKed
ranges above that frontier remain visible to `stream_ordering_debt` as
receive-hole debt, and a path that carried those bytes gains ordinary response
delivery evidence only when the contiguous frontier advances through the
range. This mirrors QUIC's separation between packet ACK state and stream
delivery state, and MPTCP's distinction between subflow progress and
connection-level data-sequence progress.

Version 1 applies this as a contiguous-frontier ownership rule for ordinary
same-stream bulk: while any lower byte range is still outstanding on an
attached path, the next ordinary `STREAM_DATA` quantum for that stream is sent
only on the path that owns the oldest lower outstanding range. Other paths may
still carry carrier ACKs, product ACKs, control frames, FIN/RESET/DETACH,
latency traffic, realtime datagrams, and explicit gap-targeted repair. They may
also become the ordinary owner once ACK progress reaches the frontier and
ECF/BLEST admission selects them for the next quantum. This rule intentionally
favours "do no worse than the best safe path" over blind same-stream striping:
diagnostics have shown that path hopping inside one ordered stream can create
tens of MiB of receive-hole debt and collapse goodput even when every carrier
is locally healthy. Aggregation in this state comes from independent flows,
safe frontier switches, and repair/failover; broader same-stream striping
requires stronger path-scoped proof that it will not increase completion time
or ordered receive debt.

Lower-frontier ownership is a correctness guard, not an unconditional
throughput entitlement. If the path that owns the oldest lower outstanding
range is still attached but no longer passes active-data serviceability against
a proven alternate path in the same sender direction, the sender MUST NOT keep
admitting later ordinary unique bytes to that stale owner merely because it
owns the lower offset. It also MUST NOT move those later unique bytes to the
alternate path while the lower frontier is still unresolved. Instead, it pauses
ordinary source reads for that stream and continues servicing carrier ACKs,
product ACKs, control frames, flow-control updates, explicit gap repair,
path proof, and path events. Ordinary data resumes when ACK progress,
repair delivery, detach/failover, or updated path evidence produces a
serviceable lower-frontier owner or advances the contiguous frontier. This
rule closes the MPTCP-style failure mode where a slow or failed subflow owns
early data and all later high-rate data either blocks behind it or deepens the
receive hole.

The lower-frontier owner is also bounded by the preemptible bulk service
horizon while `stream_ordering_debt` is nonzero. Owning the oldest unresolved
byte does not grant the full path reorder envelope for additional unique data.
The sender may keep the owner fed enough for ACK-clocked progress, but once the
owner's unresolved ordering debt exceeds the service horizon it pauses ordinary
unique bytes until ACK progress, repair, detach/failover, or updated path
evidence changes the frontier. This prevents a TCP or QUIC UDP writer from
turning one lost or delayed lower byte into tens of MiB of undeliverable later
offsets while still avoiding carrier starvation.

Lead-path admission and lead-path repair are intentionally separate decisions.
The lead path may keep a larger product queue than additional paths so that a
QUIC carrier stream or TCP writer is not starved by slow product ACKs. A sender
MUST NOT treat ordinary queued response bytes blocked by
unresolved lower-frontier debt as loss evidence by itself. In that condition it
continues servicing carrier ACKs, product ACKs, control frames, flow-control
updates, explicit gap repair, path proof, and path events while the
ordinary byte range waits for ACK progress or a serviceable lower-frontier
owner. If the blocked frontier later produces data-plane PTO/stall evidence,
the sender may act only on the explicit gap or known-final-offset repair
conditions described below.
`extra_traffic_hint_percent` feeds a cumulative extra-traffic ledger owned by
the sender service. ACK-released ordinary `OwnerData` progress earns additional
repair budget; emitting bytes into unresolved ordered flight does not. Optional
repair debits that ledger. Path proof traffic is bounded by validation attach
fan-out and the path-proof startup payload, not by the sender's data queue. A
small startup floor prevents repair deadlock before enough ordinary bytes have
made ACK progress, but the floor is spent once and does not refresh on every
ACK-gap event or tail-failover event. After that floor is spent, newly earned repair
budget accumulates until it can fund at least one useful repair burst; sub-MSS
or crumb-sized repairs are not emitted merely because a small fractional budget
was earned. The value is a continuous hint for how aggressively the sender may
trade duplicate traffic for recovery speed; it is not a fixed rate, not a
per-event multiplier, not a product-data throttle, and not permission to send
speculative unique bytes that deepen ordered receive debt. Correctness repair
may exceed the optional hint only for an authoritative ACK gap, failed-owner
gap/tail, persistent live-owner tail on an alternate output, or known final
tail, and only as bounded `RepairData`.

Repair is triggered by explicit evidence: a complete `STREAM_ACK` that exposes
a gap, a path failure or detach event, persistent live-owner tail stall with an
alternate output, or known-final-offset tail recovery. Data-plane PTO/stall
evidence is a timer input for deciding when to act on those facts; it is not by
itself permission to duplicate arbitrary live tail bytes on the same/only
carrier. The same sender-owned extra-traffic ledger applies to persistent
ACK-gap repair, alternate-output tail repair, and later final-tail repair,
because all cases spend duplicate traffic from the same stream-level budget.
The repair extent is the missing or suspect unacknowledged byte range indicated
by that event, not every cached chunk below the frontier.
Repair target choice is evidence-ordered, not carrier-family ordered. The
sender first prefers an eligible path that did not carry the last outstanding
copy of the repaired range and that is active or bulk-rate proven for this
direction. If no such path exists, it may spend bounded repair on another
non-owner validation/proof path. Only if no useful non-owner path is available
may it resend the same lowest unresolved repair range on the current survivor
output. This is the MPTCP reinjection rule applied only after loss, failure, or
explicit repair evidence exists, with the QUIC-style recovery constraint that a
repair action is small, ACK-clocked, and never a replay of unrelated cached bytes. A
sender MUST NOT substitute a later range merely because the frontier range is
already in flight. A sender MUST NOT duplicate every lower outstanding byte
merely because a faster active path is available. Speculative reinjection outside
the sender-service queue is prohibited because it can occupy path queues before
the receiver has proven useful repair. A queued sender may spend additional
repair traffic only after explicit ACK-gap, path failure/detach, persistent
alternate-output tail stall, or known-final-offset tail recovery, and the
numeric traffic hint only scales the sender-service repair budget.

Repair `STREAM_DATA` is still stream data for correctness and flow accounting,
but ordinary budgeted repair is not a priority bypass. It remains `RepairData`,
is charged to the stream extra-traffic ledger, and is queued behind already
admitted `OwnerData` unless it closes an authoritative ACK gap, a failed-owner
gap/tail, a persistent live-owner tail on an alternate output, or a known final
tail. This service rule changes queue priority only for bounded correctness
repair; it does not change stream semantics. The receiver still accepts the
data by stream offset and discards duplicates after the corresponding range is
ACKed or delivered.

For the lead path, `lead_product_queue_envelope` is the preemptible product
repair and flow-control envelope, not the UDP carrier cwnd. This larger
envelope applies only while the lead path owns the lower outstanding stream
frontier. If the lead candidate would send after lower offsets already owned by
another path, it is no longer simply feeding its own contiguous frontier; it
must fit within the same-underlay reorder budget before ordinary bulk can
continue there. This prevents the lead role from becoming a loophole that
admits tens of MiB of ordered receive hole. The configured path inflight value
is the resource ceiling for the product queue; it is not a congestion-control
claim and it does not permit non-preemptible giant frames. Additional
cross-underlay paths use the stricter reorder budget because they can create
head-of-line debt behind data already committed on another path.

Here, `chunk` is the next preemptible scheduler quantum or bounded validation
proof quantum for the stream. It is not the read-buffer ceiling and it is not
the full product inflight envelope. A large product inflight envelope controls
how much already-admitted work may be outstanding after repeated ACK-clocked
decisions; it MUST NOT be reused as a single admission payload, because doing
so turns a resource ceiling into a scheduling quantum and can suppress useful
path validation or create artificial head-of-line debt.

If those conditions are not met, the scheduler MUST NOT stripe onto that path
and MUST either wait for the best path or keep the stream single-path. A test
case where all-path bulk performs below the best single path is a scheduler
admission failure unless the result is explained by external non-repeatable
environment noise and confirmed by rerun.

The lead data path and additional striping paths have different risk. The lead
path is a scheduling role selected per bulk quantum from the current ETA model;
it is not simply the path that was attached most recently or used for the
previous frame. The lead path is still gated by product flight and carrier
backpressure, but it is not rejected by a completion-horizon comparison against
another path that may itself fail admission. It does not consume a cross-path
reorder budget merely by continuing a stream on the path that currently defines
the receiver's contiguous frontier. Additional paths, including validation
duplicates and same-stream striping candidates, are admitted against the smaller
confidence-scaled reorder budget and the completion-horizon gate. They MUST NOT
borrow the full product inflight envelope. This distinction preserves
single-path throughput while preventing a speculative or heterogeneous extra
path from creating tens of MiB of ordered-stream head-of-line debt.

The older attached active path remains a lifecycle and failover concept, but it
does not grant ordinary bulk scheduling privilege. If diagnostics show that a
path with lower ETA and delivery evidence exists, that path becomes the lead for
the next quantum and the stale attached path is evaluated as an additional
candidate. This prevents active-path stickiness after a path switch, which
diagnostics showed can otherwise alternate between a fast UDP path and a
high-RTT or low-rate TCP path while growing tens of MiB of receive hole.

Data-plane repair progress releases product repair state, but it is not
path-scoped delivery proof when the repaired byte range was duplicated. When a
tail-stall or path-failure repair frame is sent on an alternate path and the
next `STREAM_ACK` advances the contiguous ACK frontier or releases bytes that
were still in the repair cache, the sender MUST release product flight and may
clear stall diagnostics, but it MUST NOT increment the repair path's ordinary
delivery sample count, move that path to the active output slot, or change the
ordinary lead from this ambiguous product ACK. Promotion requires path-scoped
sender evidence: for QUIC, local ACK-derived carrier data samples; for TCP, an
explicit path proof followed by evidence that is not merely an ACK of duplicated
repair data. This preserves the MPTCP reinjection lesson without pretending a
data-level ACK identifies which duplicate carrier delivered the byte.

Conversely, a `STREAM_ACK` for a byte range that had exactly one outstanding
`OwnerData` copy is path-scoped sender evidence for that owner. This evidence
is a ledger property, not a packet type: the release handler first examines all
outstanding product-offset copies for the acknowledged range, then marks the
release as path-proving only when the owner copy was unique. If any `RepairData`
copy was outstanding for that range, the ACK releases
inflight/product state but creates no delivery sample for any carrier path.

ACK completeness is part of that ledger contract. If the receiver has more ACK
ranges than fit in one `STREAM_ACK`, every emitted ACK chunk for that snapshot
MUST be marked `complete=false` unless all ranges are present. A sender may use
incomplete chunks to release owner/repair flight for the included ranges, but it
MUST NOT infer missing stream holes from ranges omitted by ACK chunking. Treating
a truncated ACK as complete converts normal multipath reordering into false gap
repair and is forbidden.

Product repair over reliable TCP/QUIC carriers is reinjection onto an independent
subflow or failover path. It is not a second retransmission layer queued behind
the same in-order carrier stream. Same-output tail retransmission cannot overtake
missing carrier bytes, but it can consume duplicate tunnel traffic and create
ACK/repair feedback loops. A sender therefore MAY arm tail repair only when an
independent repair subflow is available, and the tail timer may emit repair only
for failed-owner tail failover or a known final-offset tail. Authoritative ACK
gaps are handled by the ACK-gap repair controller, not by the tail timer. Both
paths SHOULD wait for persistent congestion-scale stall evidence rather than one
ordinary PTO before sending repair.

For TCP, the configured path inflight limit is a product-queue resource ceiling
because kernel TCP still owns congestion control inside that stream. For UDP,
QUIC owns carrier congestion control and packet pacing; mptunnel owns only the
product work admitted to the QUIC stream. In both cases, lead and same-underlay
product admission is derived from live BDP, path inflight evidence when it is
smaller, the next quantum size, and the configured resource ceiling. The
configured ceiling MUST NOT become the lead path's scheduling target merely
because the path is attached or active. Actual network emission remains gated by
the QUIC sender or kernel TCP. This matches QUIC and BBR practice: the stream
scheduler may have ready data, while the packet sender paces and gates network
flight.
Cross-underlay ordinary striping is stricter: it also accounts for confidence and
receiver reorder budget because TCP and UDP expose different loss, pacing, and
head-of-line behavior.

Additional paths do not all receive the same treatment. An additional path using
the same underlay family as the lead path may use the unscaled ACK-clocked BDP
reorder budget, because the sender has comparable carrier semantics and the
same-carrier path model can safely aggregate only when sender-side evidence
shows positive contribution without harmful ordered-stream debt. An additional
path crossing underlay families, such as TCP lead to UDP additional or the
reverse, uses the stricter confidence-scaled budget until sender-side evidence
proves that it will not create harmful ordered-stream debt.

Same-underlay additional admission is quantum-granular. If the candidate already
has less committed product/carrier debt than its current admission budget, the
sender may admit one bounded service quantum even when `committed + quantum`
slightly exceeds the instantaneous budget. The next quantum is blocked until ACK
or carrier progress lowers the committed debt again. This avoids suppressing an
otherwise useful same-family contributor because of tiny outstanding proof,
repair, or already-owned bytes while still bounding overshoot to one sender
service quantum. Cross-underlay additional admission MUST remain strict:
`committed + quantum` has to fit the confidence-scaled reorder and inflight
budgets because mixed TCP/QUIC owner bytes can create much larger HOL debt.

Before same-underlay sender evidence exists, the scheduler may spend bounded
control-plane probe traffic, but it MUST NOT borrow the lead path's rate as
proof for unique ordinary data on the candidate. The candidate's stored path
model changes only after local sender evidence or unpolluted product delivery
evidence exists.

When no active, evidenced, or validation candidate passes admission, the sender
keeps the frame queued and wakes the scheduler when stream ACKs release product
flight bytes, when path metrics change, or when attachment state changes. This
is backpressure, not a liveness failure: control, carrier ACK, stream ACK, and
repair lanes remain separately prioritized.

Receive-progress feedback is advisory control work, not owner data. If a
`STREAM_ACK` or `STREAM_MAX_DATA` resend cannot enter a carrier command queue,
the product stream MUST NOT be failed, and the ACK/MAX_DATA emission watermark
MUST NOT advance as though the feedback was sent. The sender may retry after
carrier queue capacity returns or after the normal feedback cadence expires.
When multiple attached carriers can carry control feedback, a full low-ETA
carrier queue MUST NOT reject the feedback while another eligible carrier queue
has immediate capacity. This preserves the control/owner split: carrier queues
remain backpressure surfaces, but transient control backpressure does not create
false liveness failure or suppress required retransmission of product feedback.

The same rule applies before reading another local or target-side bulk segment
into the product stream. A blocked admission result is final for that service
turn: the implementation MUST NOT fall through to the current active path or a
round-robin path after declaring that no safe candidate exists. The next attempt
is made only after a normal wake event such as stream ACK release, path metric
refresh, attachment change, repair progress, or a short scheduler retry.
That retry is a scheduler timer, not an awaited sleep inside the ordinary-data
send branch. While the timer is pending, the relay remains active for inbound
carrier frames, product ACKs, stream credit, resets, detach/close, repair
evidence, and path updates. Blocking the whole relay task behind a rejected
ordinary-data quantum is prohibited because it delays exactly the feedback that
can make the queued byte range admissible again.
When feedback or control frames are already buffered at the product stream
boundary, the sender MUST process them before reading more source bytes or
dispatching another ordinary unique `STREAM_DATA` quantum. This does not give
carrier queues scheduling authority; it preserves the sender-service invariant
that ACK, credit, reset, detach, close, and path-update feedback can update
flow control, repair state, ordering debt, and path admission before additional
bulk data is committed.
This remains true when only one carrier path is currently attached. If the
oldest lower outstanding range is owned by a detached, failed, or otherwise
non-serviceable path, the remaining carrier path is not automatically safe for
later ordinary unique bytes. The remaining path may carry explicit gap repair,
control, ACK traffic, and path proof, but ordinary later `STREAM_DATA` waits
until repair or ACK progress resolves the lower frontier.

If additional same-stream paths are not admitted but the lead data path is
within its product-flight budget, ordinary bulk remains on the lead path. A
sender MUST NOT fall through to a repair or validation attachment merely because
the round-robin cursor points there after a previous send. Repair and validation
placements carry repair or proof traffic only unless ECF/BLEST admission has
explicitly selected them for the current bulk quantum.

Each `STREAM_DATA` chunk has an offset, so data can be sent over different
underlay paths without changing stream correctness.

Bulk striping is useful only when it improves completion time after accounting
for path queue, inflight, pacing rate, RTT, jitter, loss, and reorder cost. The
scheduler MUST NOT chase aggregate bandwidth by sending chunks onto a path that
will arrive too late and create head-of-line stalls.

The version 1 same-stream bulk subflow set uses a completion horizon rather than a
fixed millisecond slack:

```
eta_rate = max(path.pacing_rate, path.delivery_rate)
eta_bdp = eta_rate * path.srtt
product_budget_rate = path.product_progress_rate if known else none
product_budget_bdp = product_budget_rate * path.srtt
lead_path = min_eta_candidate_that_is_eligible_and_admissible_for_ordinary_bulk()
if path is the lead path:
    if product_budget_rate is known and path is app-limited:
        # The sample proves product ACK progress but not path capacity.  It
        # must not seed a tiny BDP cap or move ownership, and it must not
        # unlock the full configured product envelope before non-app-limited
        # bulk evidence exists.
        product_inflight_limit = min(service_horizon_window,
                                     configured_path_inflight)
    else if product_budget_rate is known:
        product_inflight_limit = min(2 * product_budget_bdp,
                                     configured_path_inflight)
    else:
        product_inflight_limit = min(service_startup_product_window,
                                     configured_path_inflight)
    product_inflight_limit = max(product_inflight_limit, chunk.len)
else if path uses the same underlay family as the lead path:
    product_inflight_limit = min(path.carrier_inflight_limit if known else infinity,
                                 2 * eta_bdp,
                                 configured_path_inflight)
    product_inflight_limit = max(product_inflight_limit, chunk.len)
else:
    modeled_inflight = max(min(path.carrier_inflight_limit if known else infinity,
                               2 * eta_bdp),
                           chunk.len)
    product_inflight_limit = min(modeled_inflight,
                                 max(configured_path_inflight, chunk.len))
base_reorder_budget = min(max(2 * eta_bdp, chunk.len),
                          configured_receiver_reorder)
effective_reorder_budget = base_reorder_budget * path.confidence
if path does not own the oldest lower outstanding range
   and stream_ordering_debt(path, chunk) > 0:
    suppress additional OwnerData for this bulk quantum
if path is the lead path and stream_ordering_debt(path, chunk) == 0:
    admission_reorder_budget = product_inflight_limit
else if path is the lead path:
    admission_reorder_budget = base_reorder_budget
else if path uses the same underlay family as the lead path:
    admission_reorder_budget = base_reorder_budget
else:
    admission_reorder_budget = effective_reorder_budget

best_rate = max(best_path.pacing_rate, best_path.delivery_rate)
best_chunk_tx = chunk.len / best_rate
candidate_debt = path.queue_bytes + path.bytes_in_flight + chunk.len
candidate_debt += stream_ordering_debt(path, chunk)
reorder_absorption = max(0, effective_reorder_budget - candidate_debt)
                     / best_rate
completion_horizon = eta_best + best_chunk_tx + reorder_absorption
if path is the previously attached active path but not the lead path
   and stream_ordering_debt(path, chunk) > 0
   and eta_path > eta_best
   and eta_path > completion_horizon:
    suppress stale active path for this bulk quantum
```

Admission gains are internal model-control coefficients, not operator-visible
traffic modes. In production v1 they apply to additional same-family ordinary
striping and explicit Service migration/failover decisions. Cross-underlay TCP+QUIC
`OwnerData` is not admitted as concurrent later-offset striping or implicit
clear-frontier Service reselection. Lead and same-underlay product queues use a
BDP/inflight-derived envelope capped by the configured resource ceiling, while
carrier controllers still enforce network flight. This follows BBR's separation
between ready application data and paced network inflight, while preserving the
ECF/BLEST rule that heterogeneous paths must not create avoidable head-of-line
blocking.

The candidate may pass the ETA gate only when it can arrive before this
completion horizon. This is deliberately different from both a narrow
near-best-ETA subflow set and an unbounded all-path rule. A narrow ETA subflow set blocks
useful high-bandwidth heterogeneous paths because it ignores how long the best
path would need to carry the same next chunk. An unbounded all-path rule can
inflate receiver reorder debt and create long ordered-stream gaps. The
completion horizon follows the MPTCP ECF/BLEST principle: a second path is useful
when it can finish useful work before the best path and the receiver's measured
reorder budget would be exhausted by waiting for that work.

The completion horizon is an evidence-based ECF/BLEST gate for heterogeneous
or debt-bearing scheduling, not a blanket same-underlay stickiness rule. It
MUST NOT reject a same-underlay candidate with a clear ordered frontier solely
because an app-limited initial-window sample or underfed validation history
implies a tiny rate. Such a sample proves that the sender was not feeding the
path enough to measure capacity; it does not prove that the path is slow.
However, an app-limited sample also does not prove that the path can safely hold
the path's bulk capacity. It therefore MUST NOT initialize a tiny BDP-derived
bulk model or make another path Service, but it also MUST NOT unlock the full
configured product envelope. It may raise the clear-frontier Service owner from
the pre-progress startup window to a bounded startup-feedback horizon; a
meaningful app-limited ACK-feedback sample MAY raise or cap that Service feed
below the geometric horizon until stable non-app-limited evidence exists. Only
non-app-limited bulk evidence can replace that Service/feed horizon with a
BDP-derived product envelope.
While there is no lower-frontier owner on another path, same-underlay admission
is governed by explicit product inflight, live carrier credit, and reorder
budgets.
Once stream-ordering debt exists for a non-owner candidate, additional
same-stream `OwnerData` is suppressed until the lower frontier clears. The
completion horizon remains the positive-contribution gate for clear-frontier
same-family admission and for explicit cross-underlay Service migration once
the migration policy decides the carrier family may change.

For same-underlay startup, the reorder/feed budget uses live carrier credit when
available. If the carrier reports an inflight or congestion-window limit, that
carrier limit shapes ETA and emission credit, but the pre-progress Service owner
credit is derived from bounded startup-feedback credit. It is a preemptible
product horizon above one carrier quantum, not a carrier congestion window and
not permission to preload the geometric Service horizon or the full configured
resource envelope before the first product ACK. Startup
Subflow owner credit remains capped by the existing ACK/update BDP window and
the product reorder/resource envelope.
The sender MUST NOT replace these credits with a tiny product-rate BDP derived
from the default path-open score or from one app-limited sample. This follows
MPQUIC's per-path congestion-state model: QUIC decides packet pacing and packet
flight, while mptunnel bounds the amount of ordered product data it is willing
to expose to reordering risk.

The same completion-horizon logic applies to the previously attached active path
when it is no longer the Service path and continuing it would expand an existing
cross-path hole. This is necessary for long-running Auto traffic: after a path
switch, the sender must not keep sending ordinary bulk on a stale path merely
because that path was active earlier. The rule is still not a human traffic mode
or static failover threshold; it is an epoch-scoped ECF/BLEST admission decision
revalidated by ETA, stream ordering debt, ACK progress, and the receiver's
current reorder budget.

The configured path inflight value is a product-queue resource ceiling, not a
carrier congestion window and not an active-path scheduling target. A conforming
sender derives lead, same-underlay, and cross-underlay product inflight from the
live BDP model, path inflight evidence when present, and the next chunk size,
then caps that result by the configured path inflight ceiling. Control, ACKs,
repair, and latency frames must still interleave with any admitted bulk work.
The configured ceiling MUST be applied as an upper bound over the adaptive
product-flight model. It MUST NOT be implemented as a floor that expands a
smaller ACK-clocked or carrier-derived sender queue to the configured maximum.
Ordered-owner scheduling debt MUST NOT bypass the active Service product-flight
admission check. If older owner debt is above pressure, debt pressure acts as a
family/evidence filter and an ordering-debt input. The current Service anchor
does not get a special later-OwnerData exemption merely because it owns lower
bytes or has carrier queue credit. Proof-only candidates and debt-expanding
cross-family candidates remain Probe, Standby, or RepairOnly until the debt
clears. The surviving OwnerData candidates are then ranked by the
normal no-worse admission checks: ETA, inflight, ordering debt, read-gap/reorder
budget, queue, and completion horizon. A sender MUST NOT hard-pin new OwnerData
to the current owner merely because that owner owns older bytes; doing so turns
ordering debt into receive-hole growth and sender starvation instead of an
ECF/BLEST-style completion decision. A measured Subflow admitted at a clear frontier still
MUST NOT change the Service owner hint merely because it carried the next range.
The current Service may remain the subflow-set anchor even when it is temporarily
over its local product-feed envelope. That anchor status is not send admission:
it only supplies the measured baseline for evaluating Subflow candidates. A
clear-frontier Subflow on the current Service family with path-scoped sender
evidence may spend its bounded startup OwnerData credit while the Service waits
for ACK or queue progress, provided the Subflow's own no-worse gates pass. If no
candidate passes those gates, the sender waits; it MUST NOT bypass the Service
admission check by relabeling another path as Service.
An app-limited Subflow is not a positive-contribution proof strong enough to
replace a feedable Service quantum. A non-app-limited bulk-rate-proven Subflow
may still win the normal no-worse ETA/completion admission. The sender MUST NOT
use periodic Subflow OwnerData "retention" merely to keep an app-limited rate
sample warm; that is a hidden proof/discovery semantic and can turn a lower-ETA
but weak path into the dominant owner. Subflow health is maintained through
ACKed OwnerData it already owns, carrier metrics, probes, repair-only work, and
future frontier-clear admission, not by replacing a feedable Service quantum
with app-limited keepalive OwnerData.
When no live path snapshot exists yet, startup product flight is derived from
the normal startup path model and lane gain, then capped by the same ceiling.
Unknown-path startup MUST NOT jump directly to the configured maximum merely
because the operator allowed that maximum for proven high-BDP paths.

The reorder budget is confidence scaled for additional paths. A path with fresh
ACK-derived delivery samples can use more of the modeled BDP/reorder envelope.
A path known only by startup hints or peer-supplied `PATH_METRICS` receives only
bounded validation traffic until real delivery evidence arrives. This prevents
unknown paths from being trusted as production bulk lanes while still allowing
aggressive proof traffic when the model predicts it will not increase
completion time.

Confidence scaling does not shrink the lead path's basic product Service
horizon below `product_inflight_limit`; otherwise a single path would bootstrap
too slowly and bulk throughput would regress. It also does not
shrink same-underlay aggregation below the unscaled BDP reorder budget, because
that turns a healthy pure-UDP or pure-TCP multipath transfer into a permanent
probe. The unscaled lead/same-underlay rule MUST NOT be applied to
cross-underlay additional paths, because that would convert a resource ceiling
into mixed-carrier reorder permission and reintroduce the all-path
below-best-single-path failure mode.

Product admission and carrier congestion control are separate gates, but they
must be consistent. The active Service path is admitted by the carrier-neutral
product envelope when the ordered frontier is contiguous; the TCP or QUIC
carrier then drains that preemptible stream work only when its own send, pacing,
and congestion gates permit. QUIC carrier inflight or congestion-window state
MUST NOT be reinterpreted as a tiny product-admission ceiling for the active
UDP Service owner. Additional Subflows remain stricter and may be rejected when
carrier queue debt plus the next chunk exceeds the validation queue limit,
because speculative paths can create head-of-line debt without
improving completion time.

For TCP reliable streams, the lower-frontier active lead is different from an
additional path. Kernel TCP owns packet congestion and loss recovery, while the
mptunnel product sender owns preemptible stream-frame admission and stream ACK
repair state. Therefore a contiguous TCP lead MUST NOT be throttled to a tiny
initial-window/RTT product BDP estimate when there is no stream-ordering debt
and no latency-pressure lane on that path. It may use the configured product
flight envelope, subject to flow control and path command backpressure, so
TCP+TCP multipath cannot perform worse than a single TCP carrier merely because
the bulk admission layer is present. Once that path would send after lower bytes
owned elsewhere, or once latency-sensitive traffic needs protection, the normal
BDP/reorder admission gate applies again.

Per-stream striping admission MUST NOT be confused with independent-flow
fairness. A path excluded from a stream's striping subflow set does not become an ordinary data
path simply because it is attached. If the previously attached active path is
no longer the best admitted path, the sender MAY explicitly move ordinary bulk
data to the better lead candidate and let delivery evidence decide whether it
should remain in the subflow set. It MUST NOT silently convert a Repair path into an
ordinary data path. This rule follows the MPTCP lesson that subflow scheduling
and connection-level sequence correctness are separate decisions: a scheduler
may use multiple subflows, but it must not move every independent flow to the
same subflow just because that subflow has the best immediate ETA before flow
sharing is charged.

### 18.2 Failover

When an underlay path fails, the endpoint marks it failed, releases its active
load, and schedules subsequent work on surviving paths. For reliable streams,
the endpoint can reopen the same stream ID on a survivor path and repair
unacknowledged gaps. The peer reattaches that stream ID to the existing outbound
connection.

Idle TCP paths may use the configured 10 second heartbeat interval and 30 second
timeout. Active data paths MUST NOT depend on that idle heartbeat for failover.
On data-plane PTO or stall, the sender marks the path suspect for new bulk,
sends one or two ACK-eliciting probes, and schedules missing stream ranges on a
survivor path. After repeated PTOs or an absolute stall budget below the
5-second fluency target, active work MUST detach from that path when a usable
survivor exists.

Repair, validation, and reattach opens that are launched on behalf of an active
stream are part of data-plane recovery. Such opens MUST be bounded by the same
active stall/PTO-derived budget used for that stream and path. They MUST NOT
wait for a generic operating-system TCP connect timeout, idle heartbeat timeout,
or long path-probe timeout before the scheduler can try another survivor. If a
recovery open exceeds that budget, the endpoint marks the attempted path as a
data-plane failure for active scheduling, cancels the pending logical stream
open, releases its reserved load, and continues with other candidates or the
best currently attached survivor.

Validation opens are evidence-gathering probes, not proof of throughput
eligibility. A successful open proves liveness. It does not by itself make a
path eligible for unbounded ordinary bulk on a stream that already has an
attached path. For that use, the endpoint needs delivery evidence such as a
stream delivery-rate sample, ACK-derived carrier rate sample, or configured rate
hint. If the active path has failed and no measured survivor exists, the endpoint
MAY still use an attached survivor to preserve liveness, but it MUST treat that
as failover recovery and keep measuring before adding the path to normal bulk
striping subflow sets.

An ordered receive hole is product-layer ordering debt, not by itself carrier
failure. A receiver that observes out-of-order reliable stream data MUST send
timely `STREAM_ACK`/`STREAM_MAX_DATA` progress so the peer can schedule
gap-targeted repair. It MUST NOT detach, close, or cool down an otherwise live
carrier output solely because the ordered frontier is blocked. The scheduler
MAY stop admitting later unique `STREAM_DATA` on paths that would expand the
hole, and the sender MAY schedule bounded repair on a different survivor, but
carrier failure still requires carrier close/error evidence, persistent
data-plane stall evidence, or an explicit failover decision. This prevents the
MPTCP/MPQUIC anti-pattern where normal multipath reordering is mistaken for a
dead subflow and creates repeated detach/reopen churn.

Owner backpressure is a sender wait state, but it is not automatically a
product-source starvation signal. The backpressure condition for scheduling is
ordered-owner scheduling debt above the path's pressure threshold; the
backpressure condition for repair is the narrower authoritative repair debt
described above. While owner backpressure exists, the sender MUST NOT dispatch
queued bulk bytes as later `OwnerData` if doing so would expand the unresolved
lower frontier. However, reading from the local product source into the bounded
sender queue does not assign product offsets and does not create ordering debt
by itself. A conforming sender MAY continue bounded product-source reads while
dispatch is owner-debt blocked, subject to stream flow control, queue limits,
and memory limits, so the service path is not starved by target-read shutdown.
The repair cache is retained unacked
`OwnerData` memory and MUST NOT be counted as already-queued source bytes for
product-source read admission. Repair-cache capacity is enforced when bytes are
assigned product offsets and committed as `OwnerData`; the sender queue remains
a separate bounded staging resource above path admission. If the next queued
response work is
`OwnerData` and a carrier has queue credit, the sender service may attempt the
normal target-selection/admission decision with the current owner-debt value.
If that decision emits no work, the sender MUST NOT immediately re-poll the same
non-emitting admission machinery just because a carrier queue still has byte
capacity. It must wait for ACK progress, repair/progress timer expiry, local
source progress that can close the stream, or carrier capacity notification that
can make a different work item admissible. This keeps product-ordering debt
separate from carrier pipe availability and prevents CPU spin or repeated
non-emitting scheduling decisions.

Persistent owner-tail stalls are repair only when they can make progress on a
different output. If the ACK frontier has stopped below the sender's next
product offset, the stream has retained unacked `OwnerData`, and no
authoritative ACK gap exists, the sender waits for ACK progress until the
PTO-derived tail-stall timer fires. After that stall evidence, it MAY reinject
the lowest blocked suffix as bounded critical `RepairData` on an alternate
output that did not own the range. It MUST NOT retransmit the live contiguous
owner tail on the same/only carrier merely because the product ACK frontier is
stalled, and if the first alternate repair does not advance the ACK frontier it
MUST wait for the persistent repair delay before repeating. A known final offset
is not sufficient by itself: terminal owner-tail repair may spend bounded
critical repair only after tail-stall, carrier failure/detach, or equivalent
final-debt evidence shows the retained tail is no longer making progress. That
repair remains bounded by repair-cache,
path-flight, and sender resource limits; those bytes are still counted as
repair overhead and MUST NOT move Service ownership.

A product-level stall on the only reliable carrier output is also not a reason
to reannounce an active `OPEN_STREAM` on that same carrier before the carrier has
failed. TCP and QUIC already own below-product reliability for the live carrier.
The receiver may send forced progress, and the sender may wait for ACK progress
or carrier-level PTO/failure evidence, but it MUST NOT create repeated active
stream-open control traffic on an already attached sole carrier merely because
product STREAM_ACK or repair progress is temporarily stalled.

When an active data path is detached because it closed or stalled while a usable
survivor exists, the sender MUST cool that path down as failed for active data
scheduling. It MAY continue bounded probes or future liveness checks, but
immediate active reopen attempts MUST prefer survivor paths. Open/probe failures
MAY use a softer suspect state because they are not proof that in-flight
application data stalled.

A path that moves from Failed to Suspect by cooldown expiry alone is not
considered recovered for bulk auto-admission or active repair admission.
Recovered bulk eligibility requires a liveness, open, or delivery success that
returns the path to Active. This separates passive time from positive evidence:
cooldown expiry permits probes, while successful feedback permits ordinary
scheduling.

For active repair or reattach, if at least one Active survivor is schedulable,
the sender MUST choose Active survivors before Suspect paths. Repair-only
candidate ordering is carrier-neutral: TCP and QUIC reliable-stream carriers are
ordered by live status, ETA, queue/flight state, and path policy, not by a
TCP/UDP family preference and not by a QUIC-only product-delivery prerequisite.
Repair traffic remains duplicate `RepairData`, consumes the sender-service
repair budget, and never creates ordinary path delivery evidence. Repair that is
not closing an active product hole is ordinary repair and MUST NOT be queued
ahead of `OwnerData`. A Suspect path MAY be used only when no Active survivor can
carry the work, or as a bounded probe that does not block the active flow.

Failover recovery for browsing, downloads, and SSH-like sessions SHOULD be
below 5 seconds in real-Internet-like conditions when at least one usable path
survives.

The 5 second target is a user-experience goal, not a production kill switch. Web
pages, downloads, and SSH sessions can often survive a short stall, but longer
stalls feel broken and may trigger application-level timeouts. The target pushes
the scheduler toward quick suspect marking, survivor-path repair, and low-cost
probing.

### 18.3 Active Probes

Idle path probes MAY be sent when they are small, authenticated, and bounded.
Probes MUST NOT impose material overhead on active traffic. Startup and recovery
MUST make connections usable immediately; probes improve path knowledge but are
not a prerequisite for first use.

Small active probes are allowed because waiting passively for traffic can leave
idle backup paths unknown until failure time. Probe traffic must remain bounded
so it does not distort throughput measurements or waste metered links.

### 18.4 Roaming

UDP carrier paths MUST tolerate authenticated peer address changes. TCP path
roaming is achieved by opening new TCP paths and reattaching streams using the
logical stream ID and repair cache.

UDP can preserve a carrier association across a new peer address after
cryptographic validation. TCP cannot move an existing connection across
addresses, so it uses the higher stream layer for recovery. The common stream ID
and repair cache make both mechanisms appear as path change rather than
application reconnect.

## 19. Resource Management

All queues and caches are bounded by configured resource limits. Production
resource exhaustion MUST apply backpressure, reduce send pace, suppress
expensive choices, or mark paths unhealthy. It MUST NOT terminate the process
solely because a benchmark target such as 256 MB RAM or 1 Gbps was exceeded.

Backpressure points include:

* stream flow-control offset;
* repair cache bytes;
* reorder buffer bytes;
* datagram queue bytes;
* path flight bytes;
* QUIC carrier send pressure exposed by the QUIC library;
* stream input queues sized by actual frame payload and reorder byte budget;
* path command queues sized by actual frame payload and path inflight budget.

Backpressure is applied through the unified sender service. A byte range waiting
in a sender lane is queued application work, a byte range retained in the repair
cache is unacknowledged stream state, and a UDP packet counted by a controller
is carrier flight. These states are related but not interchangeable. Moving work
between them MUST be caused by an explicit event such as scheduling, ACK,
confirmed loss, path failure, datagram expiry, or stream close. Implementations
MUST avoid hidden side queues that can keep sending after the sender service has
blocked a lane or marked a path unsuitable.

Stream input queues and path command queues are backpressure surfaces, not
throughput modes. Their capacity MUST represent bytes of reorder tolerance or
path inflight tolerance using the actual frame payload size emitted by the
selected carrier. Implementations MUST NOT size an MTU-fragmented UDP reliable
stream queue or UDP path command queue as though every item were a 512 KiB TCP
relay chunk, because doing so can delay carrier receive, ACK processing, loss
repair, sender-state release, and path model feedback even when CPU and memory
are idle.

Path command queues are bounded writer pipes only. They MUST NOT implement
product-flow fairness, lead-path selection, validation policy, repair
generation, ECF/BLEST admission, or stream-ordering-debt policy. They MAY
preserve separate control/priority/data pipes so already-admitted control and
latency work is not delayed by throughput writes, but the decision that a frame
belongs to a lane and is admissible for a path is made by the sender service
before the command enters the path queue. Any per-flow fair queue inside a path
writer is a stale policy layer because it moves fairness behind hidden
backpressure and prevents the sender service from explaining why a byte range
was dispatched.

Carrier-path binding objects are registries and ledgers, not send schedulers.
They may record which carrier paths are attached to a reliable product stream,
which byte ranges are in product/path flight, and which local sender evidence is
available for each path. They MUST NOT expose a method whose effect is "send
this product frame on whichever attached path looks best" and MUST NOT expose a
readiness predicate that asks the binding whether a future ordered byte range
may be sent. Those APIs recreate the rejected immediate relay shape. Ordinary
data, repair data, validation probes, stream ACKs, FIN/RESET/DETACH, and
datagram frames enter sender-service queues first; the sender service is the
only component that can transform a queued product item into an admitted carrier
command.

Implementations SHOULD expose path command-queue pending bytes to diagnostics
and sender-service accounting. Those pending bytes explain local scheduling
backpressure and stalled path writers, but they MUST NOT be treated as a peer
congestion signal and MUST NOT replace stream ACK ranges, carrier ACK ranges, or
the carrier controller's own bytes-in-flight model.

A path command queue owns frame bytes until the writer has either emitted the
frame to its transport endpoint or explicitly dropped it because the path or
stream closed. Dequeueing a command inside a writer task is not a release event:
the frame can still be waiting on encryption, transport write readiness, QUIC
stream credit, TCP write pressure, or local error handling. Sender-service
admission and diagnostics therefore MUST release path command pending bytes only
after transport emission or local discard, never merely because a writer loop
received the command from an in-process channel. This keeps local writer backlog
visible to the scheduler and prevents an endpoint from admitting more ordered
bulk bytes on a path whose hidden writer queue has not actually drained.

Configured limits are operating envelopes, not assumptions that all traffic
reserves memory. The implementation should allocate according to demand and
measured BDP. This lets browsing and SSH remain lightweight while file downloads
can grow windows and queues when paths prove they can use them. In production,
exceeding a target means adapting pressure and pace, not terminating the
process.

## 20. Management API and Diagnostics

### 20.1 Release Management API

Implementations SHOULD ship a lightweight JSON management API in the normal
release bundle. The API is disabled unless one or more management listen
addresses are configured. The API MUST be separate from the data-plane protocol:
management requests do not create streams, do not enter sender lanes, and do not
participate in congestion control.

The release API exposes bounded runtime state that operators need for inspection
and control:

* current node services, uptime, and schema version;
* per-path underlay, index, endpoint, state, configured flags, RTT, jitter,
  delivery rate, pacing rate, loss, queue bytes, bytes in flight, inflight
  limit, confidence, sample counts, and application-limited state where
  available;
* aggregate traffic summaries;
* short traffic trends sampled at a bounded interval;
* server-side response path metrics with evidence/provenance fields where
  available;
* management controls that are explicitly supported by the current services.

The API MUST NOT expose shared secrets, derived keys, authentication tags,
private certificate material, proxy passwords, or packet payloads. If a token is
configured, requests other than `/healthz` MUST authenticate with either
`Authorization: Bearer <token>` or `X-Mptunnel-Token: <token>`. Token comparison
SHOULD be constant-time.

The following endpoints are defined for version 1:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/healthz` | cheap process health check |
| GET | `/status` | full status snapshot |
| GET | `/paths` | link/path status only |
| GET | `/traffic` | aggregate summary and recent trend samples |
| GET | `/diagnostics` | release-safe diagnostic snapshot |
| POST | `/control/path` | client-side path state control |

`GET /status`, `/traffic`, and `/diagnostics` MUST be self-contained for a
role-free node. If the process contains both non-MPP inbounds that use MPP
outbounds and MPP inbounds that use egress outbounds, the response MUST include
aggregate node summaries plus separate service sections for each MPP outbound
group and MPP inbound group. This prevents operators from needing to know
whether an implementation internally split the node into client and server
objects.

`POST /control/path` accepts a JSON object containing `underlay`, `index`, and
`state`. `underlay` is `tcp` or `udp`; `state` is `active`, `suspect`,
`failed`, or `disabled`. On any node with non-MPP inbounds and MPP outbounds,
this control mutates the same path health record consumed by the scheduler. A
disabled path MUST be reported as failed to scheduling until explicitly
re-enabled. On a node with only MPP inbounds, version 1 does not provide
listener mutation through this endpoint; the node reports support status instead
of pretending to control listener-level paths.

Management sampling MUST be bounded and low overhead. A typical implementation
keeps a short ring buffer sampled once per second. The API reads counters and
snapshots already maintained by the scheduler, sender service, path health, and
server path registry. It MUST NOT add per-packet work to the transport hot path
merely to satisfy management requests.

The release API complements, but does not replace, implementation diagnostics.
It is safe for normal operations because it exposes coarse current counters and
bounded trends. Fine-grained component timing, packet event logs, allocation
tracing, and process statistics are implementation tooling and MUST NOT change
data-plane behavior.

### 20.2 Diagnostic Instrumentation

Detailed diagnostics are optional. Release bundles MUST NOT include extra
diagnostic hot-path work unless explicitly enabled by build or runtime policy.
When enabled, diagnostics MUST observe existing state rather than becoming a
hidden scheduler input.

Diagnostic tooling MAY expose:

* timestamped scheduler decisions;
* path model snapshots;
* sender lane occupancy, deficit, flow ID, selected path, and rejection reason;
* QUIC carrier ACK/loss/PTO/congestion events, including connection identity,
  RTT, RTT variance, congestion window or inflight-high equivalent, pacing rate
  when available, bytes sent, bytes acknowledged, application data progress,
  close reason, and path validation or migration events exposed by the QUIC
  library;
* stream ACK and repair events, including `complete`, ACK range count,
  largest repair horizon, released bytes, repair-cache bytes before and after
  ACK application, generated repair frame count, active path, whether a
  multipath repair alternative existed, and whether the UDP persistent-hole gate
  admitted product-level repair;
* receive-hole events, including next deliverable offset, buffered reorder
  bytes, ACK range count, largest received offset, and the path that delivered
  the out-of-order data;
* flow-control blocked time and credit updates, including sender repair-cache
  bytes, available stream credit, inflight budget, sent offset, and received
  offset;
* path flight ledger entries and releases;
* queue-to-carrier timing for control, repair, latency, datagram, and bulk work;
* per-component timing and byte counters.

Experiment design and measurement methodology are outside this protocol
specification. They belong in operator or test documentation and MUST NOT define
production protocol behavior.

## 21. Error Handling

Receivers MUST reject invalid magic, unsupported version, unknown frame kind,
invalid enum value, invalid port, empty range, over-limit payload, over-limit
frame, over-limit ACK ranges, trailing bytes, and unexpected EOF.

Authentication failure MUST close the path or session. A path-level IO failure
SHOULD fail that path and allow other paths to continue. A stream-level reset
MUST abort only that stream unless policy requires session closure.

Server listener failure is fatal to the runtime. In supervised service mode, the
process MAY restart the runtime with exponential backoff.

## 22. Security Considerations

Encryption is required by default. Plaintext lab mode removes confidentiality and
MUST require explicit operator acknowledgement. Session and path integrity remain
authenticated even in plaintext lab mode.

Implementations MUST:

* require a shared secret;
* reject short non-UUID secrets;
* redact secrets and passwords in debug output;
* use fresh AEAD nonces;
* reject TCP envelope counter replay and rely on QUIC packet protection to
  reject QUIC packet replay;
* validate authentication freshness;
* maintain replay protection for path joins;
* validate target ports and outbound policy support;
* avoid exposing product metadata in UDP carrier plaintext or QUIC SNI;
* treat upstream proxy authentication and local proxy authentication separately.

UUID-derived secrets are accepted for operator usability, but deployments SHOULD
use high-entropy secrets. Traffic captured today should remain impractical to
decrypt with foreseeable computation when strong secrets and modern AEAD suites
are used.

mptunnel does not attempt to hide packet sizes, timing, endpoint IPs, or all
traffic analysis signals. The QUIC carrier disables fixed product-identifying
SNI and carries product metadata only inside encrypted product frames, but it is
not a complete anonymity system.

## 23. IANA Considerations

This document makes no IANA requests. All registries in this document are
private to mptunnel protocol version 1.

## 24. Versioning and Compatibility

Product frames use version 1 in the `MPTF` header. TCP envelopes use version 1
in the `MPTE` header. UDP carrier packets are QUIC packets and are versioned by
QUIC; mptunnel does not define a separate UDP packet version byte.

Receivers MUST reject unsupported versions. The project does not preserve
backward compatibility for internal experimental versions. A later version that
changes wire encoding MUST update this RFC and increment the relevant version
number.

## 25. References

### 25.1 Normative References

* RFC 2119, "Key words for use in RFCs to Indicate Requirement Levels",
  https://www.rfc-editor.org/rfc/rfc2119
* RFC 8174, "Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words",
  https://www.rfc-editor.org/rfc/rfc8174

### 25.2 Informative References

* RFC 7322 / RFC Editor style guidance, used for document structure,
  https://www.rfc-editor.org/rfc/rfc7322
* RFC 8684, "TCP Extensions for Multipath Operation with Multiple Addresses",
  especially data sequence mapping and reinjection concepts,
  https://www.rfc-editor.org/rfc/rfc8684
* RFC 9000, "QUIC: A UDP-Based Multiplexed and Secure Transport", especially
  stream multiplexing, path validation, and transport state separation,
  https://www.rfc-editor.org/rfc/rfc9000
* RFC 9002, "QUIC Loss Detection and Congestion Control", especially ACK ranges,
  PTO, and packet-number-based loss recovery,
  https://www.rfc-editor.org/rfc/rfc9002
* draft-ietf-quic-multipath, "Multipath Extension for QUIC", especially
  per-path identifiers, path management, and the deliberate separation between
  multipath protocol mechanisms and implementation-specific scheduling policy,
  https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/
* RFC 9298, "Proxying UDP in HTTP", for HTTP CONNECT-UDP outbound behavior,
  https://www.rfc-editor.org/rfc/rfc9298
* Hysteria2 protocol documentation, for QUIC-based proxy transport and
  BBR-style performance motivation,
  https://v2.hysteria.network/docs/developers/Protocol/

## Appendix A. Numeric Registries

### A.1 Product Frame Kinds

See Section 9.

### A.2 Direction Values

* 1: client to server
* 2: server to client

### A.4 Path Capability Bits

* 0x0001: backup
* 0x0002: expensive
* 0x0004: low_latency
* 0x0008: bulk_allowed
* 0x0010: probe_only
* 0x0020: no_udp

## Appendix B. Abstract Algorithms

### B.1 Reliable Stream ACK Handling

```
on_stream_ack(stream_id, complete, ranges):
    release_repair_cache_entries_covered_by(ranges)
    release_path_inflight_entries_covered_by(ranges)
    do_not_lower_delivery_rate_from_feedback_only_release_timing()
    if ack_frontier_advanced_after_tail_repair:
        record_repair_ack_progress_diagnostic_only()
        do_not_mark_repair_path_as_sender_evidence()
        do_not_promote_repair_path_to_active_lifecycle_slot()
    if active_path_uses_reliable_udp_carrier:
        if not complete:
            clear_product_gap_repair_tracker()
            do_not_schedule_product_repair_from_ack_gap()
        else if no_multipath_repair_alternative:
            clear_product_gap_repair_tracker()
            do_not_schedule_product_repair_from_ack_gap()
        else:
            hole = first_unacked_gap_below_largest_acked(ranges)
            if hole is none:
                clear_product_gap_repair_tracker()
            else if hole.start != tracked_first_missing_offset:
                remember_possible_receive_hole(hole.start)
            else if hole_start_has_persisted_for_progress_interval(hole.start):
                holes = unacked_chunks_covered_by_persistent_hole(hole)
                schedule_repair(holes)
                rate_limit_repeated_repair_for(hole.start)
            else:
                remember_possible_receive_hole(hole.start)
    else if complete:
        repair_budget = min(base_repair_budget, sender_extra_traffic_remaining())
        holes = unacked_chunks_below_largest_acked_not_covered_by(ranges)
        schedule_repair(holes, repair_budget)
    else:
        do_not_infer_holes_from_omitted_ranges()

on_tail_stall_repair(stream_id, last_complete_ack_ranges):
    repair_budget = critical_repair_budget(base_repair_budget)
    holes = unacked_chunks_below_largest_acked_not_covered_by(last_complete_ack_ranges)
    if holes is not empty:
        schedule_prefix_repair(holes, repair_budget)
    else if no_complete_ack_frontier_exists and original_owner_is_live:
        do_not_repair_live_tail_without_ack_frontier()
    else if lowest_unacked_owner_tail_can_use_alternate_output():
        schedule_lowest_tail_repair_on_alternate_output(repair_budget)
    else:
        do_not_repair_live_tail_on_same_or_only_output()
    if lowest_repair_range_is_already_in_flight_on_every_usable_survivor
       and stall_or_PTO_evidence_exists:
        retransmit_same_lowest_range_once()
    never_skip_lowest_range_to_send_later_ordered_bytes()
    never_replay_whole_repair_cache()

on_path_failure(path):
    holes = unacked_ranges_last_sent_on(path)
    schedule_repair(holes)
    never_replay_whole_repair_cache()
```

### B.2 Auto Stream Demand

```
on_local_stream_bytes(observed_bytes, repair_bytes, path_model):
    threshold = adaptive_bulk_threshold(path_model)
    throughput_weight = clamp(observed_bytes / threshold, 0, 1_000_000)
    latency_weight = 1_000_000 - throughput_weight
    if idle_gap_or_tail_or_repair_pressure:
        increase_latency_weight()
    lane = Throughput if throughput_weight > latency_weight else Latency
```

### B.3 Path ETA

```
score(path, lane, payload_bytes):
    if path.failed or path.draining:
        reject_or_penalize()
    transmit_ms = 8 * (path.queue_bytes + path.bytes_in_flight + payload_bytes)
                  / max(path.pacing_rate_bps, 1)
    eta = path.srtt_ms / 2 + transmit_ms
    eta += capability_penalties(path.flags)
    eta += loss_jitter_confidence_penalties(path, lane)
    return eta
```

### B.4 Bulk Assignment and Striping Admission

```
select_bulk_data_path(stream, frame, paths):
    if frame is repair:
        return best_survivor_avoiding_original_path()
    candidates = paths excluding Repair role
    candidates += Validation paths only while bounded validation budget remains
    admitted = admitted_bulk_candidates(stream, frame, candidates)
    if admitted is empty:
        queue_until_ack_release_or_path_update()
    return best_admitted_path(admitted)

assign_independent_bulk_flow(flow, paths):
    candidates = live_paths_with_delivery_or_probe_evidence(paths)
    for candidate in candidates:
        score candidate with active_bulk_flows incremented
    return best_candidate_with_fair_sharing()

admit_bulk_path(path, best_path, chunk):
    eta = score(path, Throughput, chunk.len)
    best_eta = score(best_path, Throughput, chunk.len)
    if path.bytes_in_flight + chunk.len > product_inflight_limit(path, chunk, role_of(path)):
        reject()
    ordering_debt = lower_offset_debt_owned_by_other_paths(stream, path, chunk)
    if receiver_reorder_bytes_after_send(path, chunk, ordering_debt) >
       admission_reorder_budget(path, chunk, role_of(path), ordering_debt):
        reject()
    if role_of(path) != lead_data_path and
       eta > completion_horizon(stream, best_path, path, chunk, best_eta):
        reject()
    admit()

score_for_join(path, chunk, current_stream_active_on_path):
    snapshot = path.snapshot
    if not current_stream_active_on_path:
        snapshot.active_bulk_flows += 1
    payload = throughput_service_horizon(chunk.len)
    return score(snapshot, Throughput, payload)

throughput_service_horizon(chunk_len):
    envelope = min(configured_stream_window,
                   configured_path_inflight_envelope,
                   configured_receiver_reorder)
    return clamp(sqrt(chunk_len * envelope), chunk_len, envelope)

safe_lead_candidate(path, stream, chunk):
    debt = lower_offset_debt_owned_by_other_paths(stream, path, chunk)
    return admission_allows(path, chunk, lead_data_path, debt)

completion_horizon(stream, best_path, path, chunk, best_eta):
    best_rate = max(best_path.pacing_rate, best_path.delivery_rate)
    chunk_tx = chunk.len / best_rate
    ordering_debt = lower_offset_debt_owned_by_other_paths(stream, path, chunk)
    debt = path.queue_bytes + path.bytes_in_flight + ordering_debt + chunk.len
    absorption = max(0, effective_reorder_budget(path) - debt) / best_rate
    return best_eta + chunk_tx + absorption

base_reorder_budget(path, chunk):
    path_rate = max(path.pacing_rate, path.delivery_rate)
    path_bdp = path_rate * path.srtt
    return min(max(2 * path_bdp, chunk.len),
               configured_receiver_reorder)

effective_reorder_budget(path, chunk):
    return base_reorder_budget(path, chunk) * path.confidence

lane_protection_debt(path, lane):
    if lane is not bulk_or_background:
        return 0
    latency_flows = local_active_latency_sensitive_flows(path)
    if latency_flows == 0:
        return 0
    return latency_flows * adaptive_latency_inflight_target(path)

admission_reorder_budget(path, chunk, role, ordering_debt):
    if role == lead_data_path and ordering_debt == 0:
        return product_queue_envelope(path, chunk, role)
    if role == lead_data_path:
        return base_reorder_budget(path, chunk)
    if role == additional_same_underlay:
        return base_reorder_budget(path, chunk)
    return effective_reorder_budget(path, chunk)

product_queue_envelope(path, chunk, role):
    bdp_limit = max(2 * path_bdp(path), chunk.len)
    if path.carrier_inflight_limit is known:
        modeled = min(path.carrier_inflight_limit, bdp_limit)
    else:
        modeled = bdp_limit
    return min(max(modeled, chunk.len),
               max(configured_path_inflight, chunk.len))

scheduler_inflight_debt(path, role):
    if role == lead_data_path:
        return path.product_bytes_in_flight + path.queue_bytes
    if path.underlay == UDP and role == additional_cross_underlay:
        return path.carrier_queue_bytes + path.carrier_bytes_in_flight
    return path.product_bytes_in_flight

carrier_validation_queue_limit(path, chunk):
    if path.carrier_inflight_limit is known:
        modeled = min(path.carrier_inflight_limit, 2 * path_bdp(path))
    else:
        modeled = 2 * path_bdp(path)
    return max(modeled, chunk.len)

bulk_admit(path, chunk, role):
    if role == additional_cross_underlay:
        if scheduler_inflight_debt(path, role) + chunk.len >
           carrier_validation_queue_limit(path, chunk):
            return false
    else:
        if scheduler_inflight_debt(path, role) + chunk.len >
           product_queue_envelope(path, chunk, role):
            return false
    if product_reorder_debt(path) + chunk.len >
       admission_reorder_budget(path, chunk, role):
        return false
    return completion_horizon_allows(path, chunk, role)

attach_validation_paths(stream, demand, paths):
    if demand is not bulk:
        return
    chunk = bounded_validation_proof_quantum(stream)
    candidates = paths without active stream attachment
    for path in candidates ordered by score_for_join:
        if path can be admitted for chunk bytes of bounded validation traffic:
            OPEN_STREAM(role=Validation)
            enqueue PATH_PROOF_DATA on the validation output

on_PATH_PROOF_ACK(path, proof):
    if proof matches a pending proof on that path:
        sample = byte_counted_delivery_sample(proof.payload_bytes, proof.rtt)
        record_sender_evidence(path, sample)

on_ordered_delivery(stream, path, delivered_bytes):
    account_delivered_bytes(path, delivered_bytes)
    if stream.demand is bulk:
        if not path.has_delivery_sample:
            return
        if score(path, Throughput, next_quantum) >=
           score(stream.active_path, Throughput, next_quantum):
            return
    promote_path_to_active(path)
```

### B.5 QUIC Carrier Send

```
send_product_frame_over_quic(frame, lane):
    assert frame was admitted by sender_service
    assert path_command_queue has emission credit for lane
    encoded = length_prefix(encode_product_frame(frame))
    quic_send_stream.write(encoded)
    observe_quic_sender_metrics()

on_quic_sender_metrics(path, metrics):
    path_model.update_carrier_evidence(metrics)
    notify_sender_service_capacity_if_credit_released()

on_quic_connection_closed(path, reason):
    mark_path_closed_or_suspect(reason)
    queue_product_repair_for_unacked_stream_ranges_owned_by(path)
```

### B.6 QUIC Carrier Receive

```
receive_product_frame_from_quic():
    encoded = quic_recv_stream.read_length_prefixed_frame()
    frame = decode_product_frame(encoded)
    if frame targets unknown_or_closed_product_object:
        handle_as_product_orphan_or_reset()
        return
    deliver_frame_to_product_layer(frame)
    if frame releases product_credit_or_path_flight:
        notify_sender_service_capacity()
```

### B.7 QUIC Stall and Product Repair

```
on_quic_or_product_stall(path, stream):
    if quic_reports_connection_closed_or_path_failed(path):
        mark_path_suspect_for_new_bulk()
    if repair_authoritative_ack_gap_stalls_while_survivor_exists(stream):
        queue_gap_targeted_product_repair_for_unacked_ranges(path, stream)
    if final_offset_known_and_terminal_tail_stalls(stream):
        queue_budget_capped_terminal_tail_repair(path, stream, optional_budget)
    if active_stall_budget_exceeded(path, stream):
        detach_active_work_to_survivor_path()
        cool_failed_active_path_for_data_scheduling()

recovery_open(path, stream):
    deadline = active_stall_or_pto_budget(path, stream.lane)
    if open_stream_on_path(path, stream.id) does not complete before deadline:
        cancel_pending_stream_open(path, stream.id)
        release_reserved_path_load(path, stream.lane)
        mark_data_plane_failure(path)
        try_next_survivor_without_waiting_for_idle_heartbeat()
```

### B.8 Unified Sender Loop

```
sender_tick():
    refresh_path_models_from_carriers_and_peer_metrics()
    release_completed_ownership_from_ack_loss_failure_and_expiry_events()

    while carrier_ack_only_feedback_ready():
        send_carrier_ack_immediately_or_coalesce_without_bulk_delay()

    for lane in [
        ProductControl,
        LatencyRepair,
        LatencyDataOrRealtimeDatagram,
        ThroughputData,
        ThroughputRepair,
        Background,
    ]:
        for flow in deficit_round_robin(lane):
            work = flow.peek_next_quantum()
            if work.is_expired_datagram():
                drop_and_record_expiry(work)
                continue
            if not product_policy_and_flow_control_allow(work):
                record_blocked(flow, "flow-control-or-policy")
                continue
            path = select_path_by_eta_and_lane(work)
            if no_path(path):
                record_blocked(flow, "no-eligible-path")
                continue
            if work.is_throughput_data() and not admit_bulk_path(path, best_path, work):
                record_blocked(flow, "bulk-admission")
                continue
            if not carrier_or_tcp_budget_allows(path, work):
                record_blocked(flow, "carrier-budget")
                continue

            frame = flow.pop_next_quantum()
            retain_repair_state_if_reliable(frame)
            record_path_flight_if_stream_data(path, frame)
            emit_to_carrier(path, frame)
            charge_sender_queue_and_carrier_state(path, frame)
            record_scheduler_decision(flow, lane, path, frame)

            if lane_latency_budget_exhausted():
                break
```

This loop is conceptual, not an implementation requirement for a single thread
or task. A conforming implementation may shard lanes, paths, or flows across
tasks, but the externally visible behavior must match the same ownership,
admission, priority, fairness, and diagnostics rules.

For same-stream reliable OwnerData, carrier-family boundaries are part of the
production scheduler model. TCP+TCP and QUIC+QUIC candidates share a carrier
family and can compete as same-family subflows once admitted by live metrics,
ordering-debt, and no-worse checks. TCP+QUIC candidates are different carrier
families with independent ACK clocks, recovery, pacing, and flow-control
semantics, so they MUST NOT steal same-stream OwnerData while another family
owns unresolved lower bytes that the candidate would extend into a receive
hole. They remain valid Probe, Standby, RepairOnly, migration, and failover
paths. A bulk-rate-proven cross-family path that already owns the lower
outstanding range is not stealing ownership; it is continuing an existing safe
owner sequence. At a clear frontier, cross-family Service selection is allowed
only through explicit migration/failover policy, followed by the same no-worse
admission model as any other Service switch. If the ordered-owner hint is absent
or was cleared, the live Active Service output remains the carrier-family anchor
for mixed-family admission. A missing hint is a wait/repair/failover condition,
not permission to elect a lower-ETA TCP/QUIC-family alternate as implicit
Service.
