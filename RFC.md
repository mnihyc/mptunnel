# mptunnel Protocol Specification

Intended status: Standards Track

Protocol version: 1

Last updated: 2026-07-13

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
An implementation of product protocol version 1 MUST reject unsupported
versions and MUST NOT silently accept undocumented legacy frame layouts. TCP
envelopes use `MPTE` version 2 and MUST reject version 1 because its static
cross-connection traffic key did not provide a unique AEAD nonce domain.

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

### 1.2 Current Implementation Evidence Scope

The normative design below is supported by separate, reproducible lab cohorts;
one result is not universal transport proof. The current same-condition TCP
request control is Iteration 109: exact diagnostics-disabled 18-second uploads
with two logical flows and five shaped 500 Mbps, 180 ms, 1 ms jitter, zero-loss
paths reach 691.368 Mbps multipath and 314.999 Mbps single-path. That is 2.195x
overall, 2.935x over `[9,18)`, and 2.409x over `[15,18)`, with client transmit
shares of 37.15%, 33.44%, 4.78%, 10.47%, and 14.17%. Against Iteration 69 under
the same profile, multipath changes +10.38%, +11.77%, and -6.36% over those
windows. The `[9,15)` window improves 22.02%, so the lower late window reflects
earlier delivery rather than a hidden aggregate loss. The single control changes
-0.32%, +0.56%, and +3.53%.

This evidence proves multi-flow TCP aggregation for that upload cohort and
guards against silently accepting a same-condition regression. It does not
prove one-flow request-side optional-path aggregation, QUIC or mixed-carrier
aggregation, real-Internet performance, failover, or superiority over a current
matched MPTCP or Hysteria2 baseline. Those remain independent evidence tracks.

The current response-direction control is Iteration 128: one logical flow under
the same shaped five-path profile reaches 236.774 Mbps multipath versus
112.274 Mbps single, or 2.109x overall and 2.368x in `[15,18)`, with 75.5/24.5%
material server path shares. Iterations 126/127 causally show that endpoint-only
startup now moves directly to ordinary bounded ownership under a temporary
Service opportunity prior instead of paying a 5.3-8.8 second exclusive
calibration. This proves shaped equal-fat one-flow TCP response aggregation,
not QUIC, mixed carriers, faults, real Internet, TUN, or external baseline
superiority. CPU, memory, and maximum-gap cost remain non-ideal. The separate
default heterogeneous Iteration 129 guard reaches only 104.531 Mbps multipath
versus 110.489 Mbps best single. It improves materially over preserved one-flow
history but leaves the fat path unused. Later cold-path ordered sampling was
rejected after creating 0.525-1.269 second read gaps; native TCP carrier evidence
remains required before claiming heterogeneous aggregation.
Iteration 135 separately verifies the negative admission boundary: with one
200 Mbps, 20 ms Service and four 50 Mbps, 420 ms, 10%-loss candidates,
multipath/single is 182.247/182.777 Mbps with 0.251/0.247 second maximum gaps,
and the slow candidates remain control-only.

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

A path owns one configured underlay route inside a session. For UDP this is one
QUIC association. For TCP this is a bounded pair of lazy persistent encrypted
carrier instances: one selected by attachments opened as control, latency, or
realtime, and one selected by attachments opened as throughput or background.
The pair is independent of path count and product-stream count; a single
configured path MUST NOT create an unbounded carrier instance per latency-first
product stream. Carrier class is fixed for the lifetime of one attachment.
Later Auto promotion changes sender-service lane priority and admission, not its
TCP association; frontier-safe detach/reattach remains the only way to move that
stream. Each concrete TCP instance owns its connection lifecycle and kernel
congestion state, while the configured path owns path ID, underlay family,
bind/peer address, health, carrier-local RTT/loss/rate/queue evidence, carrier
credit, and path-specific authentication. A path MUST NOT own product stream
offsets or decide that a reliable stream should stripe onto it merely because it
has capacity.

A path group or carrier subflow set is not a product-offset owner. It is a bounded
scheduler epoch for one flow: one Service path plus admitted Subflow members
selected from session paths, path-model evidence, queue state, and
ECF/BLEST/no-worse admission. ACK progress updates delivery metrics, ordering
frontier, and later admission inputs, but it MUST NOT recreate validation
credit or reinterpret probe evidence as owner evidence. The epoch remains
valid while its Service, owner-credit envelope, overhead budget, and read-gap
budget still match the admission envelope. It is recreated on material envelope
change, detach/failover, active attachment, output replacement, or role change,
not on ordinary ACK progress or passive Validation/Repair membership growth.
Passive growth invalidates the planner revision without refilling the preserved
epoch. Product byte ownership still belongs to the per-range
flight ledger, not to the subflow set itself. The Service path is the live
ordered-owner anchor for that stream direction. The per-range lower-frontier
owner may be that Service or a distinct admitted Subflow; owning the lower range
does not change the latter's role. Service is not simply the lowest-ETA
candidate, and a measured
alternate MUST NOT be relabeled as Service merely because it wins the next
payload quantum. Detach, close, or carrier loss invalidates the old live output,
but it MUST NOT by itself transfer Service ownership to a survivor while
ordered-owner scheduling debt remains. In that state the sender waits, performs
bounded failover repair, or resumes only after the contiguous frontier catches
up. A new Service owner is chosen by explicit sender-service admission at a
clear frontier or by a dedicated failover policy after lower ownership has been
resolved. If no live or lower-flight owner remains but the stream still has
pre-ACK tail debt from the disappeared owner, the only non-clear-frontier
Service failover allowed is a sender-evidenced survivor from the same carrier
family. This is resilience failover, not aggregation: it resumes bounded
`OwnerData` service so the product stream does not deadlock behind a missing
output, but it does not credit any outstanding `RepairData` as delivery proof
and it does not admit cross-family migration.
A survivor is not promoted to Service merely because it is the only remaining
attached output; it needs explicit frontier-clear Service failover admission,
or the same-family sender-evidenced failover rule above. Path-scoped sender evidence
is preferred, but if no live Service owner remains and the ordered frontier is
clear, a live liveness-only candidate MAY become the bounded startup Service
failover path so the stream does not stall without an owner. When the frontier
is not clear, ownerless proof/liveness-only survivors and cross-family
survivors may carry bounded `RepairData` for the explicit blocking range, but
they MUST NOT receive later `OwnerData` merely because repair was queued or
pending. Lower-ETA
measured contributors are Subflows only when ordered-debt and no-worse
admission allow it.

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
owner. If the live Service owner has not yet produced bulk-rate evidence, a
clear-frontier migration may choose a materially better path only after that
target has bulk-rate evidence; otherwise sender evidence keeps the target in
Probe, Standby, RepairOnly, or no-live-Service failover eligibility. Reannouncing
the same path key with a different live command channel is a
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
For sustained bulk prevalidation, configured candidates from the current
Service family SHOULD begin their Validation opens together. A retryable open
failure MAY receive one later retry after path health is independently
revalidated; one unlucky handshake MUST NOT permanently remove a path from that
product stream or bypass global failure fencing. Open concurrency and retry are
path management only: neither grants product ownership or capacity evidence.

Opening a same-underlay subflow set is not the same as immediately committing
unique ordered bytes to every member. MPTCP and MPQUIC can coordinate packet
or subflow recovery inside one transport-level connection; mptunnel's TCP and
QUIC carrier outputs sit below the product stream and above separate carrier
recovery engines. Therefore, for one ordered reliable stream, a proof-only
validation output without local sender evidence MUST NOT carry later unique
`STREAM_DATA` while another output owns an unresolved lower outstanding range.
A validation output may carry control, ACK, explicit repair, path proof, or a
new independent product stream.
At a clear frontier, liveness, path-proof, configured hints, and peer hints MAY
rank validation/probe order, but they MUST NOT make a non-active path the
Service owner for ordered product bytes while a live Service owner remains. A
path also MUST NOT become Service merely because the selector has no better
lead fallback; fallback lead selection without active Service anchor rights,
direction-correct bulk-rate evidence, or explicit frontier-clear failover
admission is still Probe/Standby, not product ownership. Temporary
sender-service backpressure or capacity filtering on the
current ordered owner also MUST NOT erase that Service anchor; dispatchable
alternates remain Subflows unless an explicit Service migration/failover
decision changes the owner. A measured path may become an admitted Subflow
after direction-correct bulk-rate evidence exists and the ETA/no-worse selector
admits that owner range. For a sustained bulk-only response stream in a session
with active direction-relevant response demand and no active latency-sensitive
or realtime pressure, one same-underlay Validation candidate
with local sender evidence may instead enter the bounded startup Subflow
sampling epoch defined in Section 18.1. This applies equally to TCP beside TCP
and QUIC beside QUIC. The selected candidate may receive repeated preemptible
whole `OwnerData` frames only until the epoch's fixed cumulative sample budget
is spent. This sampling does not replace the Service anchor; the Service
remains the owner and resumes normal ordinary feed at the epoch cap unless the
candidate has graduated to ordinary measured Subflow admission. Flow count is
not capacity proof: only the one exact bounded candidate may carry unproven
unique data, while every other unmeasured alternate remains Probe, Standby, or
RepairOnly. Under latency-sensitive or realtime pressure, all unmeasured
candidates likewise remain Probe or Standby and the Service remains
preemptible.
Once the persistent Service has direction-correct bulk evidence, offset-free
carrier discovery is independent from product Subflow graduation. It MUST NOT
wait for an optional path to own ordered product bytes: that serial dependency
delays high-BDP discovery and makes the active set depend on attachment timing.
Each exact carrier still has bounded exploration, and receipt-bound capacity
only permits ordinary admission; it does not itself assign product offsets.
The shared session MUST retain a live discovery reservation across binding
retirement, release it before waking another planner, and apply one absolute
deadline to train emission plus receipt. An already measured cross-family
handoff that clears product placement MUST outrank optional same-family
discovery.
Response startup-slot graduation requires the sampled candidate's exact
`OwnerData` flight to drain. For TCP, the completely ACKed sealed startup sample
proves exact-path reachability. An eligible endpoint-only candidate installs a
temporary typed Service capacity prior and begins a fresh ordinary exact-ACK
epoch; a configured or independently measured candidate opens bounded fallback
ACK-clock calibration. The first assignment-to-ACK interval MUST NOT publish
candidate capacity. For QUIC UDP, product ACK
progress alone is insufficient and the local carrier ACK controller MUST publish
bulk-rate evidence. Graduation preserves the Service anchor and sampled
membership, invalidates stale planner snapshots, and releases only the exclusive
unproven-startup slot for a different candidate.

When at least two bulk response flows are active, the current directional
bootstrap MAY run when a measured active TCP Service family leads UDP by at
least two flows and neither side has latency pressure. One reachable,
unmeasured UDP Validation output MAY
receive a bounded `PATH_CAPACITY_DATA` train. These frames have no product stream
extent, cannot advance `STREAM_ACK` state, and cannot become `OwnerData` or
`RepairData`. The sender MUST gate ordinary application writers, encode the
train as bounded records, append `PATH_CAPACITY_FINISH` on the same ordered QUIC
stream, and require the peer to return `PATH_CAPACITY_RECEIPT` with the exact
declared byte count. Receipt of the complete token train is authoritative.
Connection-aggregate Quinn ACK timing is provisional transport diagnostics and
MUST NOT, by itself, complete a token or create placement authority. Existing
cross-stream queue or flight can only consume service during the receipt window
and therefore lower the measured available rate. The session MUST reserve one exact path instance, expire an
incomplete attempt on a PTO-derived deadline, clear it on detach, and cap the
number and cumulative bytes of attempts. Each exact
`(session, path, path_instance)` key permits at most two attempts; eligible
never-attempted fitting keys MUST rank ahead of retries. Every attempt uses the
larger of the startup sample floor or the current QUIC inflight window plus one
fresh strict-proof window and MUST fit the session envelope without clamping.
Ordinary product
rate MUST use timed non-app-limited native QUIC ACK bytes. Capacity rate uses
the full declared train over the complete nonzero sender-to-receipt interval,
bounded by timer granularity. Native timing and the receipt RTT remain
diagnostics; subtracting them could create an unstable near-zero denominator. Scheduler
poll time MUST NOT be a denominator. Bulk-placement rights use one lifetime frozen
from the planning snapshot's three-PTO persistent-congestion horizon without erasing
connection reachability; later RTT changes MUST NOT shorten it, and a later full
ordinary timed window MAY lower the retained rate. Receipt releases the ordinary
writer gate immediately. ACK attribution remains separate: callbacks for
packets sent from probe start through the frozen proof deadline MUST be excluded
from generic product evidence until that deadline, even after public probe
metrics retire. A replacement probe on that connection MUST wait for the prior
attribution quarantine to expire. The whole frozen proof contract and train MUST
be admitted as one typed data-lane command, and the
complete train MUST fit the remaining
cumulative, non-refilling session capacity counter. That counter is separate
from product ledgers; its limit is derived from the minimum configured
flight/repair/reorder/stream-window envelope. A session may hold only one probe
reservation or one handoff drain at a time.
Only failure of the exact
provisional atomic enqueue may roll back its attempt and bytes. Successful
enqueue commits a PTO-derived lease. Proof acceptance MUST exact-match the token,
path instance, and frozen byte geometry, and MUST retain serialization until the
registry has published the marker; proof, completion, expiry, detach, or close
releases serialization without refunding exploration spend. Capacity proof
does not move Service ownership. Because carrier evidence is connection-wide,
an indeterminate partial, cancelled, or expired carrier write MUST fail-close that exact
QUIC connection before later streams can reuse its prepublished evidence.

After that target has direction-correct bulk proof, the scheduler MAY move one
whole response flow at an exact clear product frontier. Diversification requires
the source family to lead the target family by at least two Service flows and the
projected target share to be no worse. A balanced-family performance override
instead requires at least a two-fold projected share. Ordinary TCP product
ACK-clock evidence is per-flow goodput and MUST NOT be divided again. A drained
TCP response-calibration median is typed path capacity and, like carrier-scoped
TCP or QUIC evidence, is divided by projected bulk-flow count until mature
ordinary evidence replaces it.
Ordinary TCP response goodput MUST count only unambiguous binding-local
`OwnerData` releases. The first exact product ACK establishes the time boundary
without publishing a rate. Later exact bytes MUST be divided by continuous
ACK-to-ACK wall time over a bounded observation epoch (currently at least 100 ms
and retained for at most 2 s), including elapsed time from mixed-assignment ACK
callbacks. Implementations MUST NOT average per-callback point rates or discard
the long interval before an ACK-compressed callback tail. Repair copies, global
frontier movement, assignment residence before the first ACK, and other
bindings' bytes remain excluded.

The frontier requires no lower binding-owned `OwnerData` flight, ACK hole, or
active sampling/TCP calibration. Shared carrier command queue, native bytes in
flight, and other streams' work are pressure inputs, not this binding's ownership
debt, so they MUST NOT be required to equal zero. If sustained Service feed is
the only obstacle, the session MAY hold one bounded one-shot drain reservation
for one exact response binding. Only fresh `OwnerData` assignment on that binding
pauses; other bindings, control, ACK/credit, and correctness-critical repair
remain eligible. Offset-free raw staging stays inside the existing bounded
source-feed/sender-queue reservoir.

At the resulting frontier, the first ordinary payload commits the preselected
whole-flow handoff atomically. The commit MUST revalidate exact path instances,
planner/model/session generations, current Service identity, handoff mode,
projected share, proof authority, and the target's ranked pending-byte credit
bound. Pending pressure may fall but growth beyond that bound cancels the move.
Expiry, detach, identity change, capacity regression, or credit regression MUST
resume normal Service feed without changing ownership. A successful move is
sticky for the stream and changes response Service load accounting without
rewriting control-plane Active/Validation attachment roles.

For a sustained bulk-only TCP request/upload stream, the request sender MAY use
the same bounded startup Subflow sampling mechanism only while at least two
active logical bulk request flows with exact committed TCP Service ownership
and present queued or outstanding request data exist when the exact startup
owner is assigned. Reverse-direction bytes, idle completed uploads, and
QUIC-Service flows never count. That owner may
drain after a two-to-one transition. The exact live
ordered Service attachment MUST remain Active, already have direction-correct
bulk-rate evidence, and stay stable for the current output incarnation. Only one
same-underlay Validation output
incarnation with fresh local path proof may receive startup `OwnerData` at a
time. Section 18.1 defines its per-candidate credit, graduation, sequencing, and
debt guards. This request-side exception does not elect a new Service or weaken
the response-side active-demand gate.
When the current Service owner is alive but backpressured by unresolved
contiguous owner tail, a cross-underlay alternate MUST wait instead of owning
later byte ranges. An ordinary measured same-underlay Subflow may proceed only
after the candidate has direction-correct bulk-rate evidence and passes its
no-worse gates. The one bounded startup-sampling candidate may cross only a live
Service owner's ordinary contiguous suffix, and only while projected Service
tail plus candidate debt fits the same-underlay reorder budget. It MUST NOT cross
authoritative lower-flight debt, an ACK-range hole, missing- or failed-owner
debt, or any range already requiring repair. Sharing a carrier family by itself
is not enough to create ordered ownership.
Cross-underlay ownership with unresolved prior Service bytes is allowed only
when the candidate continues the lower-frontier owner. A live Service owner that
is merely out of queue/emission credit is backpressure, not failure: it MUST NOT
cause Service migration, but it MAY leave a measured same-underlay Subflow
eligible for `OwnerData` through the Subflow admission ledger. If no such
Subflow is admissible, later owner bytes MUST wait for capacity, ACK progress,
repair progress, or an explicit frontier-clear migration after the prior owner
is gone.
Before any product ACK establishes a contiguous frontier, all already-sent
bulk bytes below the next send offset are unresolved Service owner-tail debt for
alternate owners. This pre-ACK debt is a scheduler guard, not repair evidence.
ACK-data-only evidence from a tiny or application-limited probe is still not
bulk-rate evidence and does not grant Service owner rights. Configured or peer
hints alone are not sender evidence and MUST NOT unlock unique-data ownership.
`Subflow` `OwnerData` requires bulk-rate evidence or, for a response path or TCP
request path, an explicit bounded same-underlay startup-sampling epoch, plus
sender-service admission proving that
the candidate fits the active product, carrier, and ordering envelopes. A
response-side epoch additionally requires at least one active
direction-relevant reliable response flow; the session response-flow count and
its generation MUST be revalidated before response sample enqueue. The first
candidate is the bounded discovery bootstrap; after a measured same-family
Subflow exists, a later candidate MUST also satisfy the whole-sample Service
reservoir completion projection in Section 18.1. A
TCP request-side epoch instead requires at least two active logical bulk request
flows with exact committed TCP Service ownership, present queued or outstanding
request data, and sustained bulk-only local upload demand. Reverse-direction
bytes, idle completed uploads, and QUIC-Service flows do not contribute. It also
requires the stable request-side Service and fresh Validation-instance evidence
defined in Section 18.1. Only a bounded epoch may
bypass a completion-rate comparison polluted by underfeeding; ordinary measured
Subflows remain subject to the completion horizon. QUIC request paths do not use
this exception: optional ownership requires exact fresh post-attachment,
non-app-limited native packet-ACK evidence. Ordinary measured admission remains
path-metric and capability driven rather than TCP- or UDP-preferred. Mixed TCP+QUIC paths are
deliberately stricter in production v1 because they do not share one
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
clear, the Service owner is fed through the product Service feed window; lower
carrier congestion and pacing remain the carrier engine's responsibility.
Before exact Service feed evidence exists, and while the ordered frontier is
still contiguous, Service source and emission use a bounded bootstrap rather
than the configured product Service envelope. Switchable same-family source
staging uses the derived feed reservoir; carrier-specific emission further
limits TCP to the preemptible Service horizon, while QUIC uses that feed
reservoir so its native controller can obtain a meaningful sample. With default
limits and a 64 KiB bulk quantum, the reservoir is 4 MiB. It is derived from the
product quantum, Service horizon, and configured resource envelope; it is not a
carrier cwnd, rate estimate, receive window, or capacity claim. Product receive
credit is a separate receiver-memory authority and MUST NOT depend on path
proof.

For the current clear-frontier QUIC Service only, feed evidence may come from
either substantial uniquely owned product `STREAM_ACK` progress or enough local
carrier ACK-derived DATA to retain a durable estimate, even when that carrier
sample is app-limited. Without same-path latency pressure, either feed-only
predicate may unlock the configured source/emission envelope because QUIC still
enforces native congestion control, pacing, and flow control below it. Neither
predicate publishes carrier capacity, admits a Subflow, or authorizes migration.
Those decisions still require fresh non-app-limited carrier bulk proof. TCP
Service graduation continues to use its strict product/carrier evidence.
The safety boundary for a contiguous Service owner is product flow control,
sender-service queue pressure, repair-cache ownership, product progress, and
carrier-command backpressure. BDP-derived completion and reorder gates are for
optional Subflows, cross-path debt, and explicit migration/failover decisions.
This Service rule is deliberately different from optional Subflow admission:
the Service is the current primary owner and must remain fed while its ordered
frontier is clear, whereas optional paths must prove positive contribution
before receiving owner bytes.
For a mature single-family response with no latency pressure,
"remain fed" means soft priority through the derived Service horizon, not
exclusive use of the entire hard Service envelope. Once the exact live
Service's unacknowledged unique `OwnerData` reaches that horizon, an already-admitted,
bulk-rate-proven same-underlay Subflow MAY own the next quantum while
`max(global_ordered_tail, Service_assigned) + quantum` remains within the
configured product/reorder/stream envelope. The whole lower Service tail is the
completion backlog; only later candidate/other-candidate bytes consume receiver
reorder occupancy. The selected Subflow MUST carry a commit for
the exact current Service epoch and MUST still pass the ordinary completion,
BDP, emission-credit, lower-frontier, reorder, and resource checks. Its
`Subflow` role MUST NOT move the Service identity. If no such candidate passes,
the sender MUST fall back to the live Service. This partitions the existing
product envelope; it does not enlarge source credit or the hard Service envelope.
TCP proof comes from exact product ACK evidence; UDP/QUIC proof and per-path
emission credit remain owned by the local QUIC carrier ACK/congestion/pacing
controller. The common reservoir is only product ordering admission and MUST
NOT replace either carrier controller.
When latency-sensitive work is active on the same Service path, the
clear-frontier Service feed envelope uses a bounded preemptible window with
BBR-style headroom over the Service horizon for total owner credit already
admitted to that Service path, including owner bytes in flight and queued
carrier work. Raw bytes in the sender-service staging queue are not admitted
owner credit because they have no offset or path owner.
Because product flight retains accepted OwnerData from carrier enqueue through
`STREAM_ACK`, its carrier-queue view overlaps rather than adds to that flight.
Hard Service debt is the maximum of assigned product flight and carrier queue;
the queue remains an authoritative fallback when the product-flight view is
absent.
That cap is feed/backpressure accounting, not reorder accounting. Without
same-path latency pressure, a clear-frontier Service with exact feed evidence
MAY use the configured product Service envelope. The product/
repair envelope remains a hard correctness and memory ceiling, not a carrier
congestion-window claim. This envelope does not apply to optional Subflows or
lower-owner debt.
Reorder budgets are for additional paths, cross-path lower-byte debt, and
explicit owner-tail guards; they MUST NOT count same-Service queued carrier work
as cross-path reorder debt. The point of the latency-pressure cap is different:
it bounds how much ordered debt one Service path may preload while realtime or
latency work needs fast recovery and preemption. The resulting Service envelope
is ordinary `OwnerData`, not optional traffic, and it MUST NOT admit Subflows,
repairs, probes, or duplicates.
The ordered-data owner hint is not a carrier-family preference. If the ordered
frontier is clear and the hinted Service output cannot enqueue owner bytes at
all, the sender MAY elect a new Service from validated live outputs by metrics.
If unresolved owner debt exists, the hint remains authoritative and later
owner bytes MUST wait for repair or frontier progress instead of migrating
behind the hole.
Before a Service output has exact feed evidence, bulk owner feed follows the
carrier-specific bounded bootstrap above. Once the Service has feed evidence, a
bulk-only Service may use the configured envelope while the ordered frontier
remains clear. Same-path latency pressure narrows it to the BBR-headroom feed
reservoir. Strict bulk-rate evidence is still required before optional Subflows
are treated as mature contributors.

Switchable response outputs enforce these owner envelopes when raw bytes are
converted into offset-bearing `STREAM_DATA` at sender-service dispatch. Before
conversion, a stream with only one live owner-capable underlay family couples
the staging limit across assigned owner tail plus raw queued bytes. Before feed
evidence, a switchable response uses the derived feed reservoir; same-path
latency pressure narrows it to the Service horizon. After exact Service feed
evidence, the response MAY use one configured product envelope across that exact
global owner tail and raw queue. Per-path admission and the TCP or QUIC carrier
still own congestion, pacing, and backpressure. Both feed evidence and same-path
latency pressure MUST come from the exact live Service output. Evidence on an
alternate, Validation, Subflow, Repair, or closed output MUST NOT unlock or
resize the Service source-staging reservoir.
This exact-Service proof rule applies to switchable server response source
staging. Fixed request-side outputs instead use the carrier-neutral product
admission window anchored to their exact ordered Service. Exact product ACK
turnover may grow that source window but never supplies carrier capacity proof.

Bulk `STREAM_MAX_DATA` advertises the configured product receive window for both
TCP and QUIC. The receiver selected that memory envelope, so path evidence MUST
NOT shrink it or make credit growth circular with the data needed to measure a
path. Switchable source staging and carrier-specific emission retain the bounded
bootstrap above, and native TCP/QUIC congestion control still limits network
flight. Latency QUIC streams MAY use the smaller startup product window because
their memory isolation is a lane policy rather than a capacity claim.

When live owner-capable outputs span both TCP and UDP, the unassigned raw queue
MAY instead use a separate bounded reservoir with the same horizon/feed limits.
Its headroom subtracts raw queued bytes only; it neither spends assigned owner-
tail credit nor borrows the full product or repair envelope. This mixed-family
exception exists so the sender-service planner can convert staged bytes only
after choosing the carrier family. Repair-only or closed outputs do not enable
it, and loss of live family diversity immediately returns source staging to the
coupled policy. Neither staging policy is path evidence or optional-owner
admission. Raw bytes staged while family diversity was live MUST retain
remaining-repair-capacity prefix limiting after topology contraction; losing a
family MUST NOT turn repair-cache backpressure into a fatal stream error.

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
Critical completion repair remains `RepairData`; it does not become a
repeatable owner stream. While a repair frame for a product byte range is still
queued in the sender service or already in live carrier flight, later
tail/FIN/output-update events for the same range MUST be treated as already
pending and MUST NOT enqueue another copy, debit extra-traffic accounting
again, or increase sender-service queued bytes. ACK progress, detaching the
output that owns the only repair copy, a repair deadline that declares the
previous attempt stale, or a materially different missing range may create a
new bounded repair attempt.

Terminal FIN reliability is `Control`, not repair or path proof. `STREAM_FIN`
is idempotent for a stream ID and final offset, and a sender MAY replay it once
after it has already sent FIN, has no queued owner or repair work for the
stream, and the peer's contiguous ACK frontier covers every owner byte up to
the final offset. This replay MUST NOT create product offsets, change Service
ownership, credit path delivery evidence, or consume duplicate/repair proof
budget. Its purpose is only to close the terminal-control gap that can occur
when final-tail `RepairData` completes on a survivor after the original
ordered-control FIN was lost with an older carrier.

Connection-level ACK/control frames update product-stream state, so their
return path is part of the product ACK clock. When the current Service path is
live and has control capacity, receive-progress `Control` such as `STREAM_ACK`
and `STREAM_MAX_DATA` SHOULD be emitted on that Service return path before
lower-ETA Probe or Validation paths are considered. If the Service return path
is blocked or failed, the sender MAY fall back to any admissible control path.
When a client relay's existing product-stall or receive-hole timer supplies
explicit evidence that the Service ACK clock is not making progress, one forced
receive-progress retry SHOULD instead prefer an already accepted Repair
attachment with control capacity. That retry MUST NOT use a Validation
attachment, promote Repair to Service, or credit the Repair carrier with
delivery evidence. If no Repair attachment exists, a receive-hole or timer-only
product-stall recovery attempt MAY open one new path, but that attachment MUST
use the Repair role and MUST preserve the current Active control path. A finished
request or replayed request `STREAM_FIN` does not turn response-side repair into
Active migration. Opening or reannouncing a new Active path requires carrier
failure evidence or a separate explicit failover decision; the recovery timer
alone is insufficient. Normal feedback ticks continue to prefer Service, bounding
the alternate control traffic while allowing an authoritative ACK frontier to
escape a silently blackholed return path.
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
For QUIC, even an unambiguous product ACK remains product progress only; enough
uniquely owned progress may release the current Service feed boundary, but it
does not publish carrier capacity or authorize an optional path. The local QUIC
packet ACK controller remains the authority for carrier bulk-rate proof.

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
persistent-gap guard fires. If the gap's exact single-copy `OwnerData` owner was
TCP and the flow is bulk, one event selects a bulk-model-proven output distinct
from every owner of the repaired range and may repair one modeled service
flight for that output, normally approximately `2 * BDP`. The event remains
capped by that selected output's remaining modeled service-flight headroom, the
outstanding gap debt, repair cache, configured repair/path-flight resources,
and actual output capacity. Its queued frames remain bound to the selected
attachment instance or output incarnation; dispatch MUST pause rather than
silently move the remainder to another output whose envelope was not used to
size the event. If that identity detaches, is replaced, or cannot drain by the
persistent-gap retry deadline, the sender cancels the whole remaining queued
batch. A later authoritative gap replay MUST select and size a fresh output
instead of inheriting the stale envelope. An unproven output,
UDP/QUIC owner, ambiguous owner, or
latency/realtime flow retains the normal single adaptive repair quantum. Thus
the product-gap
controller is unified, but it does not overwrite the carrier-specific recovery
and pacing models.

When ACK ranges are contiguous but stop before the sender's highest owner
offset, the unacknowledged suffix is not repairable merely because it is
unacknowledged. A live TCP or QUIC carrier owns its own packet recovery, and
same-output product retransmission cannot overtake the missing bytes. A live
owner tail with no complete ACK frontier is normal in-flight data before stall
evidence, not immediate repair debt. If no complete ACK frontier exists while
the recorded owner is still live, the sender MUST wait for ACK progress, carrier
failure evidence, or a terminal-tail condition instead of converting live-owner
bytes into product `RepairData`. Unknown-owner no-frontier startup
tails likewise MUST wait instead of duplicating the whole repair cache. If the
latest repair-authoritative ACK is complete, contains exactly one contiguous
`[0, frontier)` range, the frontier remains blocked for one PTO-derived product
stall timeout, and a different live output can carry the lowest blocked range,
that suffix becomes tail correctness repair: it may be retransmitted as
`RepairData` only on an output that did not own that offset range and under the
same repair/path-flight caps. A sparse ACK set MUST repair its lowest explicit
gap first; the sender MUST NOT use its largest acknowledged end as a tail
frontier and skip a lower hole. Ownership is evaluated for the range being
repaired, not from the path that happened to carry the latest later
`OwnerData`. This correctness rule applies to every reliable flow lane; the
lane changes its PTO-derived delay and bounded quantum, not whether a proven
blocked suffix can use a distinct repair output. The first tail probe
deliberately uses one product stall timeout, not the persistent-congestion
multiplier used for authoritative ACK-gap repair, because the repair is a
bounded reinjection of the exact lowest HOL-blocking suffix on a different
output. If that probe does not produce ACK-frontier progress, repeated tail
repair for a live owner backs off to the persistent-congestion multiplier so a
live carrier is not converted into continuous duplicate traffic. Detached,
failed, or no-longer-serviceable owners retry the same bounded repair mechanism
on the PTO-derived product stall timeout because the original owner can no
longer make progress and failover repair is now the correctness path. This
repair remains duplicate
product data and MUST NOT create path delivery proof, move the Service owner,
or reset Subflow admission state. Because the failed owner can no longer make
progress, failed-owner repair target admission MUST NOT be blocked by stale
owner emission-credit debt from the failed path. It still requires a live
survivor with actual carrier queue capacity, remains capped by the current
repair quantum and repair resource limits, and is still `RepairData` rather
than new `OwnerData`. When a same-family sender-evidenced survivor exists for
the failed owner, failed-owner repair SHOULD use that survivor before
cross-family live repair fallback. This target-ordering rule follows the
Service failover envelope; it does not path-prove the repair carrier, does not
move Service ownership, and does not prevent cross-family repair when no
same-family survivor can carry the blocking range.

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
owner, and each event is bounded by outstanding repair debt and configured
repair/path-flight resources. The exact-TCP-owner bulk persistent-gap exception
may use one selected distinct output's modeled service flight; every other
critical repair event retains the normal adaptive repair quantum.
Critical repair priority is a sender-service/product-queue ordering rule, not a
carrier control-priority rule. A repair frame that is encoded as `STREAM_DATA`
MUST use the carrier stream-data queue after target selection so it cannot
starve `STREAM_ACK`, flow-control, proof, detach, or reset control traffic.

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
proof, unbounded validation bytes owning the only copy of ordered data, or
TCP-named relay buffers governing UDP-backed product streams. A bounded
direction-gated startup Subflow epoch is explicit sender-service sampling, not
validation credit or implicit ownership transfer. The model deliberately follows the
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
  record. The only calibration-specific exception is a positive exact active
  TCP response residual smaller than the normal chunk: it may become a smaller
  frame only when a residual-sized first planning pass returns that exact
  calibration commit. Service fallback and UDP/QUIC retain the normal quantum.
  Lossy, jittery, or queued paths can still shrink the upper condition cap;
  high-rate stable paths repeatedly dispatch bounded quanta until the carrier
  remains fed. Inflight limits and carrier pacing control network pressure; the
  frame quantum controls scheduling preemption and per-byte processing cost.
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
| Path probe timer | 10s interval, 2s timeout | Idle authenticated liveness probing is common in MPTCP/MPQUIC; exact timers are mptunnel policy | Can delay idle-path discovery; must not gate demand-bearing setup or active recovery | Keep only as the configured bound for one idle authenticated health-probe transaction. It MUST NOT cap or extend Active, Repair, Validation, reattach, TCP-carrier, or datagram deadlines |
| Extra traffic hint | `extra_traffic_hint_percent = 5` default; 100/200 allowed | Reinjection is common in MPTCP/MPQUIC; numeric hint is mptunnel/operator policy | Bad if treated as product-data throttle, per-event refresh, or blind duplication allowance | Keep as hard optional-work budget; response sender spends repair traffic only with evidence; path proof remains bounded by validation fan-out |
| Security freshness | `auth_freshness_window_seconds = 300` | Replay freshness windows are common security controls; exact window is mptunnel policy | Affects clock-skew/replay tolerance, not data-plane rate | Keep as security policy; not data-plane adaptive |
| Cipher default | AES-256-GCM through the `ring` provider already used by the QUIC/TLS stack; ChaCha20-Poly1305 optional | AEAD is mandatory; AES-GCM default follows modern hardware acceleration practice | CPU can matter on CPUs without AES acceleration or with a provider that has high per-record cost | Keep as operator choice; no plaintext unless explicit; validate provider changes against standard vectors and wire-compatible peers |
| QUIC transport envelope | stream receive window = stream window; receive window = stream + repair + reorder + datagram + flight; send window >= path-flight/read ceiling; bidirectional stream count = QUIC-scoped stream cap | QUIC flow-control/congestion/MAX_STREAMS split is common; mapping is mptunnel policy | Can cap QUIC if mapped envelope or stream count is too small | Keep resource mapping; QUIC BBR pacing/congestion remains carrier-owned; stream count is independent from byte windows |
| QUIC congestion controller | Quinn BBR by default | Model-based BBR fits endpoint-only proxy/tunnel operation where no accurate per-direction path rate is configured; fixed-rate Brutal-like sending is only safe with explicit accurate bandwidth configuration | BBR is not a substitute for product no-worse scheduling; fixed-rate modes can overload weak/shared paths if guessed | Keep as carrier-owned congestion control; product scheduling consumes metrics but does not replace it; any Brutal-like configured-rate mode must be explicit |
| QUIC datagram MTU model | Startup 1200 byte payload; lower 512 and upper 65,000 path-spec bounds | 1200-byte UDP support is a QUIC requirement; mptunnel sets lower/upper guardrails | Low MTU can increase fragmentation/overhead | Keep startup safety plus path MTU observation/probing |
| TUN defaults | IPv4 `10.88.0.1/24`, MTU 1500, DNS TTL 5s | Local-interface defaults are common deployment choices; exact values are mptunnel examples | MTU/TTL can affect TUN behavior but not sender scheduling | Keep as operator defaults, scoped to TUN |
| Outbound DNS timeout | 5s default | Resolver timeouts are common control-plane safety; exact value is mptunnel policy | Slow resolvers may fail resolution; not hot path after resolve | Keep per outbound, not global data-plane behavior |
| Outbound target/proxy connect timeout | 10s default, scoped to each egress outbound or routing member | Connect setup deadlines are common control-plane safety; exact value is mptunnel policy | Too low can fail slow upstreams; too high can delay connect-time fallback | Keep per egress outbound/member; this owns only server-side target or upstream-proxy setup. Client MPP carrier and product-open attempts use role-specific PTO deadlines. The configured idle path-probe timeout owns neither |
| SOCKS5 UDP/TUN idle TTLs | SOCKS5 UDP TTL 30s, TUN UDP flow idle 60s | NAT-style UDP state expiry is common; exact TTLs are mptunnel policy | Too short/long affects idle UDP associations | Keep as flow expiry policy; not a throughput cap |
| Management API bounds | request 64 KiB, trend 300 samples, sample interval 1s | Control-plane bounding is common; exact values are mptunnel policy | Can limit observability resolution, not packet throughput | Keep as low-overhead management-plane bounds |

The implementation also contains standard transport constants and adaptive
policy formulas that are not primary operator knobs. They are allowed only when
their origin is explicit and they do not become hidden modes.

| Parameter or family | Current value or formula | Design source and exact origin | Performance risk | Final handling |
| --- | --- | --- | --- | --- |
| Standard packet floor | `TRANSPORT_MSS_BYTES = 1460` | Portable Ethernet TCP MSS floor used only as a lower-bound packet quantum | Jumbo/offload paths may support more, but this does not cap high-rate quantum | Keep as floor below adaptive BDP/BBR sizing |
| QUIC initial window seed | `PATH_OPEN_SCORE_BYTES = 10 * MSS` | QUIC RFC 9002 initial congestion-window packet-count shape | Too small if reused as bulk cap or one-window per-stream bulk-validation trigger | Keep only as startup/evidence seed and minimum useful path-open score; bulk prevalidation uses an amortized multi-window floor capped by the service-quantum/rate-evidence floor |
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
| TCP attachment-open carrier classes | At most one lazy persistent control/latency/realtime instance and one lazy persistent throughput/background instance per configured path | Bounded connection pooling plus attachment-open lane isolation; exact two-class split is mptunnel policy | One instance for every product stream repeats TCP/authentication setup and hides unbounded congestion domains behind one path ID; migrating a live attachment on lane promotion creates path-instance and ordering ambiguity | Keep the bounded two-class pool for every topology; multiplex attachments within their class, retain independent carrier lifecycles between classes, and keep a promoted attachment on its existing carrier until explicit frontier-safe reattachment. Product priority and MPTE record boundaries limit application queue blocking, but kernel TCP HOL remains |
| DRR lane/flow quanta | Deficit charge equals actual sender-service packet quantum | DRR/fair queuing is common | Fixed byte quanta previously underfed high-rate carriers | Keep adaptive charge based on actual queued frame size |
| Service frame quantum | Latency/control use small BBR-style quanta; reliable bulk feeds TCP/QUIC with the bounded 64 KiB BBR send quantum under the configured read/payload envelope and live condition cap | BBR send-quantum model applied at the product-record boundary, with TCP/QUIC packet pacing below | Tiny quanta cap throughput; giant quanta harm latency | Keep adaptive; high-rate stable paths repeatedly dispatch bounded quanta while control/repair/latency remain preemptive |
| TCP AEAD record granularity | Send exactly one encoded product frame per independently counted/authenticated `MPTE` version 2 envelope; a writer run may batch consecutive envelopes into one socket write | TLS 1.3's bounded independently authenticated record layer is the mature design precedent; mptunnel retains its own adaptive product-frame quantum rather than copying the TLS record-size limit | Coalescing a 512 KiB writer run into one record makes one lost TCP segment block decryption and product feedback for the whole run; one syscall per small record can waste CPU | Keep strict frame/record identity on emission and bounded multi-envelope socket-write batching; reject non-canonical multi-frame plaintexts |
| Startup Subflow sample epoch | Per candidate, cumulative `OwnerData` budget = `min(RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2, path_flight_envelope, receiver_reorder_envelope, repair_envelope, stream_window_envelope)`; the current fixed startup window makes the unclamped budget 256 KiB. One active sustained bulk response may sample same-family TCP or QUIC after local sender evidence. Fresh TCP request sampling instead requires at least two active logical bulk request flows with exact committed TCP Service ownership and present request work; reverse bytes, idle completed uploads, and QUIC-Service flows never count. It also requires sustained bulk demand, the exact stable bulk-rate-proven ordered Service, and fresh proof for the exact Validation attachment. A useful near-cap TCP request sample seals without splitting, stays on Service for the oversized frame, and queues one ordered proof marker. Either the exact ACK completing all sealed startup `OwnerData` or the exact ordered marker ACK establishes the follow-on causal boundary. QUIC requests have no product startup epoch | Multi-quantum startup sampling follows MPTCP subflow probing, QUIC path validation and initial-window growth, and BBR send-quantum/app-limited sampling practice; the directional logical-flow gates, dual TCP request boundary, and 256 KiB rules are mptunnel policy | One quantum underfeeds useful paths; ACK-refilled or concurrent candidates create unbounded HOL debt; attachment proof mistaken for rate admits an unmeasured path; treating the receipt as the only boundary can stall a fully ACKed sealed sample; using finite ordered request bytes to discover QUIC capacity creates HOL debt without non-app-limited native evidence | Keep one frozen cumulative, non-refilling budget per exact candidate instance and at most one unproven startup owner per stream. Preserve the response active-flow generation fence and require two active TCP-Service logical bulk request flows only to open a fresh request owner; a begun exact owner may drain after a two-to-one transition. Bind TCP request proof, either valid causal boundary, sample membership, ACK attribution, and graduation to exact instances; normal Service-flight residence age alone is not startup stale-tail authority. QUIC request optional ownership requires exact fresh post-attachment non-app-limited native packet-ACK evidence. Suppress either eligible startup branch under latency/realtime pressure or authoritative lower debt |
| TCP request ACK-clock calibration | After TCP request startup graduation and exact startup-flight drain, one explicit exact Validation instance owns a cumulative, non-refilling target frozen at claim time; the default resource-clamped target is 2 MiB. Only exact candidate `OwnerData` sent after the sealed-data ACK or ordered-receipt ACK boundary can complete the causal proof. Failed enqueue restores owner, target, and spend; an exhausted exact owner serializes later candidates until proof or exact-instance lifecycle termination. Exact ownership may let an endpoint-only candidate provisionally borrow the Service per-flow rate and derived pipe for scheduling, but the candidate's own continuous exact product-ACK model replaces that prior after ten exact candidate samples. A configured candidate retains its own hint. QUIC is excluded | TCP lacks a path-local packet ACK controller, so a finite exact-owner product ACK epoch establishes attribution without pretending to control kernel TCP. Service seeding avoids asking a 2 MiB proof to fill a high-BDP pipe before TCP leaves slow start | A moving BDP-sized target may never finish; inferred ownership may interleave candidates; receipt-only boundaries can miss valid ACK order; one early underfilled sample can permanently collapse the candidate model; borrowed Service rate mistaken for proof can fabricate capacity | Freeze the exact instance and target atomically, accept either exact causal startup boundary, retain the 2 MiB default proof and all resource/debt/pressure gates, and keep endpoint-only Service seeding explicitly provisional until ten exact candidate samples. Never feed TCP product ACK timing into QUIC's native controller |
| QUIC response capacity train | With two or more bulk response flows and a measured TCP Service-family lead of at least two, one exact reachable, unmeasured UDP Validation path may receive a typed `PATH_CAPACITY_DATA` train followed by same-stream `PATH_CAPACITY_FINISH`; the peer returns exact `PATH_CAPACITY_RECEIPT`. Train bytes are `max(startup sample floor, live QUIC inflight window + one fresh strict-proof window)` and must fit a separate cumulative session capacity envelope. Each exact session/path/path-instance gets at most two attempts; eligible fitting fresh keys rank before retries. Ordinary connection writers wait behind the token gate, but a globally empty Quinn queue is not assumed. Full-train receipt owns proof, freezes its full receipt-interval rate and lifetime, and releases the carrier gate; native ACK/BIF timing remains diagnostic. Bulk rights receive one proof lifetime frozen from the planning snapshot | QUIC owns packet delivery, congestion, loss, pacing, and flow control below carrier-neutral response ownership; an ordered token receipt attributes the finite train without borrowing TCP proof semantics or pretending aggregate packet ACKs carry frame identity | Treating buffer acceptance, product ACK progress, aggregate ACK bytes, stale proof, or a moving live floor as capacity can fabricate or erase proof; refunded attempts can exceed the declared budget; partial writes can leak late ACKs into ordinary evidence | Prefer unattempted fitting paths, reserve one typed command provisionally, exact-match token/path-instance/frozen geometry/validity, require exact written and received train bytes, and retain session serialization through registry publication. Publication resolves the command ticket without cancelling the carrier; failures remain cancellation. Fail-close indeterminate writes and never create product flight or ownership |
| Cross-family response Service placement | A measured target in an underloaded family may receive one sticky whole response flow at an exact clear frontier. When sustained feed prevents that frontier, one session-serialized, per-binding one-shot bounded drain pauses fresh `OwnerData` only on the selected binding; other bindings, control, ACK/credit, and critical repair continue, while offset-free staging remains within the existing bounded source reservoir | MPTCP/MPQUIC connection-level placement plus ECF/BLEST ordering safety, applied above separate TCP and QUIC recovery engines | Count-only balancing can permanently move a 500 Mbps flow to a lower-RTT 100 Mbps carrier; per-frame migration creates cross-family HOL debt; waiting passively for a frontier under continuous feed makes safe placement unreachable | Keep family count as the need signal and measured fair share as the gain signal. Commit one atomic binding/session ownership transaction after exact identity/model revalidation; cancel the drain on expiry or projected fair-share regression. Attachment role and response Service load remain separate |
| TCP response capacity prior and fallback ACK-clock calibration | After exact response-startup drain, endpoint-only TCP with no independent carrier ACK/hint may inherit the proven same-family Service rate as a typed path-capacity prior. It enters ordinary bounded Subflow ownership immediately; ten completed ordinary exact-ACK windows plus a usable continuous sample atomically replace the prior as per-flow goodput. If that Service prior is ineligible, one exact TCP Validation instance may instead use cumulative staged calibration with initial credit `I = min(resource ceiling, Service horizon, max(one send quantum, 2 * candidate BDP))`. For one fallback stage let `B` be spend at authorization, `L` the cumulative ceiling, `A=L-B`, `W` noncausal fresh ACKed bytes, `E` strict causal fresh evidence, and `F=min(resource-clamped Service horizon, max(path-proof floor, Service horizon/2))`. Three accepted aggregates publish their median as the same typed prior after exact drain. Service ownership never moves, exact commits remain binding-local, and UDP/QUIC is excluded | TCP lacks QUIC's local packet ACK controller, but the exact startup sample already proves endpoint-only reachability and bounded ownership. Reusing the bounded Service opportunity avoids a redundant exclusive measurement transport while ordinary exact ACKs supply path-local correction. Staged calibration remains fallback policy where independent evidence must be preserved | Treating the Service prior as permanent capacity can fabricate a path model; mandatory staged calibration can serialize several MiB, underfill a high-BDP path, and still publish a low rate; mixed or compressed windows can fabricate fallback rates; applying product ACK timing to QUIC corrupts carrier evidence | Type the borrowed value only as temporary path capacity, reset the candidate's ordinary ACK epoch at installation, retain the ten-window takeover, and keep all ordinary completion/reorder/inflight gates. For fallback calibration, retain cumulative spend, strict current-stage aggregation, one timer floor, median publication, exact residual commits, and the binding-local fence through flight drain. Leave QUIC packet ACK congestion/pacing authoritative |
| Fresh TCP calibration opportunity | Before the first response calibration byte, response policy projects the whole seed against one bounded Service feed reservoir behind that lower prefix and clamps their sum to the product resource envelope; an unsafe response identity may retire only after coherent revalidation. Fresh request calibration permits no path-wide completion-estimate veto until request-direction, provenance-bound authority exists. One active bulk response flow may start same-family response calibration, while request calibration requires two active logical bulk request flows with exact committed TCP Service ownership and present request work; reverse bytes, idle completed uploads, and QUIC-Service flows never count. Request calibration additionally requires the exact sealed-data or ordered-receipt causal boundary and atomically claims one exact instance with its frozen target. Exact begun response calibration may finish after response-demand churn; an unstarted identity becomes dormant and blocks only itself | MPTCP ECF/BLEST ordered-completion reasoning above kernel TCP; mptunnel keeps QUIC carrier ACK control separate | Unconditional probing creates HOL stalls; canceling begun work strands offsets; dormant binding-wide serialization wastes proven capacity; recomputing a request target or owner after claim weakens rollback and ACK attribution | Keep response completion projection and its prefix-plus-Service reservoir enforced until ACK progress, apply no path-wide request completion veto before request-direction provenance authority, retain every independent safety gate and begun exact ownership serial until drain, freeze request owner and target through rollback-safe enqueue, and leave dormant Service/other measured work open; retirement remains response-policy-specific |
| Inflight target | BDP * BBR cwnd gain, send quantum, and MinPipeCwnd under configured flight envelope; latency/realtime lanes use the smaller preemptive target | BBR inflight model and product lane priority | Too low underfeeds; too high queues | Keep adaptive from live BDP/queue/loss/carrier evidence |
| Stability/backlog factors | Shrink by loss/jitter/queue/backlog relative to BDP with floor derived from MinPipeCwnd or send quantum divided by BDP | Congestion-sensitive adaptation; floor is no longer a fixed fraction | Over-shrinking can create low-rate loops | Keep adaptive; diagnostics must show shrink reason |
| Auto bulk classification | EWMA/rate/byte/idle-gap evidence promotes/demotes demand using service quantum, BDP, and PTO; per-stream bulk prevalidation requires an amortized multi-window floor, not merely one initial window and not a full throughput-promotion delay | Product-specific but measurement-based | Late/early promotion affects latency/throughput; too-early prevalidation creates short-flow open/close churn | Keep adaptive; no user-visible mode tag or port rule |
| ACK progress cadence | Product `STREAM_ACK` uses BDP/2 when measured, otherwise the bounded bulk service quantum, under the repair/flow-control resource ceiling; `STREAM_MAX_DATA` uses larger flow-control hysteresis | SACK/QUIC ACK-range practice with MPTCP-style product repair ownership release | Sparse ACKs fill repair cache and stall senders; chatty ACKs waste reverse bandwidth | Keep dynamic from receive progress and separate from MAX_DATA cadence |
| MAX_DATA cadence | Credit updates use a window/chunk-derived threshold. Bulk TCP and QUIC advertise the configured receiver-memory window independently of path evidence; latency QUIC retains a smaller startup window | QUIC flow-control update logic, kept distinct from carrier congestion control | Credit below BDP stalls high-bandwidth streams; treating product credit as carrier cwnd or proof confuses two ledgers | Keep cadence adaptive from the configured window/chunk while source staging and carrier congestion control independently bound admitted/network flight |
| Active stall and retry timing | Derived from QUIC PTO, observed RTT/rttvar, lane state, TTL, and persistent congestion threshold | QUIC PTO/recovery model | Fixed sleeps underfeed high-rate carriers or delay failover | Fixed retry/stall constants are removed from data-plane policy |
| Path failure cooldown | Derived from PTO and consecutive failures, capped by QUIC persistent congestion threshold | QUIC persistent-congestion backoff applied to path reuse | Fixed cooldown can hide recovered paths | Fixed 5s cooldown is removed |
| Datagram target/path model | TCP- and QUIC-carried datagram response deadlines derive from PTO, RTT variance, TTL, loss, and persistent congestion threshold; pacing floor is one observed datagram payload per PTO | QUIC PTO plus UDP application congestion-control guidance; TCP RTO/PTO-style path evidence for TCP-carried datagrams | TCP-underlay datagrams can still HOL-block | Removed fixed 50ms/1s/250ms/8*SRTT/64Kbps clamps. A datagram attempt that has received product feedback expires on absent target response; a pre-feedback path timeout may try one remaining schedulable carrier within TTL as path failover, not reliable replay |
| QUIC metric sampler | Active polling uses SRTT/2 with timer granularity; app-limited/idle polling uses PTO; confidence derives from ACK-derived sample count | Carrier app-limited filtering and QUIC RTT/PTO evidence | Stale samples mislead scheduler | Removed fixed 10..250ms sampler clamp; keep evidence provenance |
| Path/stream queue depth | Byte envelope divided by actual service/frame payload plus priority-headroom slots, where headroom is one slot per non-throughput lane | Resource envelope plus lane model | Fixed slot caps underfeed high-rate carriers | Removed 1024/4096-style caps from data-plane queues |
| Bulk admission | A clear-frontier Service owner is admitted by product ownership, not by a second carrier-cwnd ceiling. Bulk receive credit is the configured receiver-memory envelope and is independent of proof. Before exact Service feed evidence, a switchable same-family response still couples source staging and owner tail inside the derived feed reservoir; QUIC Service emission uses that reservoir while TCP emission retains the narrower horizon. With exact feed evidence and no same-path latency pressure, the Service may use the configured product envelope. A current QUIC Service may establish feed evidence from either substantial uniquely owned product `STREAM_ACK` progress or a durable local carrier ACK-derived DATA estimate, even when the latter is app-limited; TCP uses strict product/carrier evidence. Neither QUIC authority is carrier capacity proof, and this exception is feed-only: optional Subflows, capacity claims, and migration still require strict non-app-limited carrier proof plus BDP/ETA/reorder/no-worse admission. Same-path latency pressure narrows Service credit. Raw staging remains bounded and is not owner debt; one-family staging is coupled to the exact owner tail, while live TCP+UDP owner-capable outputs may use a separate bounded raw reservoir. Repair-only or closed outputs do not enable that mixed-family exception. Carrier inflight, queue, RTT, loss, pacing, and flow control remain authoritative below product windows | MPTCP/MPQUIC simultaneous-path scheduling plus ECF/BLEST HOL avoidance, with separate TCP and QUIC recovery/control loops below a unified product policy | Highest-risk throughput governor: underfeeding prevents carrier measurement, while over-admitting optional paths creates ordered debt | Keep receive credit independent from proof and keep the evidence split explicit in diagnostics; do not treat the derived 4 MiB source/emission bootstrap, QUIC write-buffer acceptance, product-progress feed evidence, or app-limited carrier feed evidence as optional-path capacity |
| Validation traffic | Probe/control traffic by default; repair data only after explicit gap/failover evidence. Unique future bytes are permitted through a bounded same-underlay startup Subflow epoch for one active sustained bulk response after local sender evidence, and for fresh TCP request paths after at least two active logical bulk request flows with exact committed TCP Service ownership and present request work; reverse bytes, idle completed uploads, and QUIC-Service flows never count. TCP request startup also requires sustained bulk demand, stable ordered-Service evidence, and fresh proof produced after the exact Validation instance attached. One stream-ordered proof marker follows only a sealed TCP request sample; either its exact ACK or the exact ACK completing the sealed candidate-owned startup data establishes the calibration boundary. QUIC request Validation remains proof-only until exact fresh post-attachment non-app-limited native packet-ACK evidence supports ordinary measured ownership | MPTCP reinjection and subflow startup plus MPQUIC path validation and local scheduling guidance | Unbounded or cross-family Validation `OwnerData` creates HOL debt; proof-only validation can permanently underfeed useful capacity, while treating attachment-proof or finite ordered-product rate as QUIC capacity can over-admit it | Keep the response and TCP request exceptions explicit, cumulative, non-refilling, bulk-only, and limited to one unproven candidate at a time. Preserve response active-flow generation fencing; count exact TCP-Service logical request flows rather than path attachments, allow begun exact owners to drain after a two-to-one transition, and bind TCP request proof, either causal startup boundary, sample membership, ACK attribution, and graduation to the attachment instance; suppress startup under latency/realtime pressure, authoritative lower-flight/repair debt, or resource-envelope exhaustion |
| Replay/security cache sizes | closed-stream cache and PATH_JOIN replay cache derive from stream/path scale with bounded caps | Security/control-plane state bounding | Not a throughput cap unless accidentally used for data-plane queues | Keep as security/resource envelope, not scheduler input |
| Header/parser safety | HTTP CONNECT request/response 64 KiB; CONNECT-UDP payload 65,527; SOCKS5 UDP packet 65,535; target host 255 | Parser/protocol bounds are common | These bound protocol parsing and packet buffers, not scheduling | Keep as scoped parser/packet envelopes, not scheduler input |

The request-side startup and Validation exceptions in this table are TCP-only.
QUIC request paths require strict non-app-limited native carrier evidence before
optional `OwnerData`; finite ordered product samples are product turnover, not
QUIC capacity discovery.

For any high-confidence additional same-underlay QUIC path without a durable
product-progress sample, ordered overlap uses a BBR-style inflight target of
`2 * delivery-rate BDP`, bounded by the reorder envelope. Native pacing and
congestion-window growth remain carrier-local and MUST NOT enlarge product
reorder authority. Low-confidence startup instead uses
its native inflight window, or the delivery-rate inflight target when the native
window is unavailable, inside the separately bounded, non-refilling startup
epoch; carrier pacing is not product authority. After durable product progress
exists, the native inflight window may participate in the bounded product
reorder budget.

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
| UDP target/datagram clamps `50ms..1s`, `250ms`, `8*SRTT`, and `64Kbps` | Removed from response and suppression policy. Datagram response deadlines and suppression derive from PTO, RTT variance, TTL, loss, and persistent congestion threshold. QUIC carrier setup is additionally bounded by the remaining product TTL and the implementation's handshake safety envelope. |
| QUIC sampler clamp `10..250ms` | Removed. Active sampling uses SRTT/2 with timer granularity; idle/app-limited sampling uses PTO. |
| TCP/session/TUN queue slot caps such as `+4`, `1024`, and `4096` where byte envelopes already exist | Removed from data-plane queues. Queue depth is byte envelope divided by actual payload quantum plus lane-derived priority headroom. Security/control-plane cache caps remain separate. |
| Hard-coded egress and MPP path connect timeout call sites | Removed. Egress target/proxy setup remains owned by its outbound/member. Initial Active setup budgets each serialized exchange: three PTOs for TCP and two for QUIC UDP while another candidate remains. A sole candidate gets the remaining phase prefix plus the persistent-congestion PTO backoff series, nine/eight PTOs for TCP/QUIC UDP. Every initial Active TCP attempt retains the conservative initial PTO floor because its actor may establish or re-establish the carrier. Attach/recovery uses one live candidate PTO. One absolute deadline covers queue wait, carrier setup, authenticated session and `PATH_JOIN`, `OPEN_STREAM`, current path-metric publication, and the role-required peer accept/reset. Idle-probe policy neither preempts nor extends it. |
| Fixed closed-stream and `PATH_JOIN` replay cache clamps | Removed. Closed-stream retention scales from configured stream count without preallocation; `PATH_JOIN` nonce replay retention scales from configured stream count and the QUIC persistent-congestion threshold. |
| Fixed datagram retry exponent cap | Removed. Product datagrams are not retransmitted by mptunnel after feedback or response expiry; PTO/TTL-derived budgets bound only response waiting, carrier setup, path suppression, and a single pre-feedback alternate-carrier failover attempt when one remains schedulable. |
| TCP/QUIC-underlay datagram product retransmit/reopen loop | Removed. The carrier owns packet/stream retransmission below mptunnel; mptunnel MUST NOT duplicate an acknowledged datagram ID or open a replacement carrier only because a UDP target response timed out. Real setup, encryption, authentication, session errors, and pre-feedback path timeout before useful product expiry remain retryable carrier failures. |
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

TCP encrypted framed streams use HKDF-SHA256 as defined by RFC 5869. First
derive suite-bound TCP key material:

```
tcp_key_material =
    HKDF-Extract(salt = "mptunnel encrypted framed v2",
                 IKM = cipher_suite_context || master_secret)
```

Each TCP connection has a 16-byte CSPRNG `client_salt`; the server also creates
an independent 16-byte CSPRNG `server_salt` after authenticating the first
client record. Derive:

```
base_prk = HKDF-Extract(salt = client_salt, IKM = tcp_key_material)

client_write_key =
    HKDF-Expand(base_prk,
                "mptunnel encrypted framed v2 traffic key" ||
                cipher_suite_context || 0x01,
                32)

server_write_key =
    HKDF-Expand(base_prk,
                "mptunnel encrypted framed v2 traffic key" ||
                cipher_suite_context || 0x02 || server_salt,
                32)
```

The direction octets above are the values in Section 7.5. Client-to-server
records carry `client_salt`; server-to-client records carry `server_salt`.
Both values are part of the authenticated envelope header. The server MUST NOT
commit a received client salt or generate server traffic before the first
client record authenticates. The client MUST retain its client salt and MUST
not commit the received server salt until the first server record authenticates.
All later records in one direction MUST carry the committed salt. This creates
a new directional traffic-key domain for every TCP connection without another
round trip and prevents a captured server response from authenticating on a
fresh client connection.

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

Counters start at zero for each freshly salted directional traffic key and MUST
NOT repeat for the same key and direction. A sender MUST reject counter
overflow before invoking AEAD or emitting bytes.

The per-connection key domain, direction byte, and monotonic counter make nonce
uniqueness easy to audit. Directional key separation prevents a record emitted
by one peer from being valid in the opposite direction. QUIC packet nonces are
owned by QUIC and are not part of the `MPTE` TCP framed envelope.

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
| 31 | PATH_PROOF_DATA | path ID, proof ID, opaque payload |
| 32 | PATH_PROOF_ACK | path ID, proof ID, payload byte count |
| 33 | PATH_CAPACITY_DATA | path ID, calibration ID, opaque QUIC carrier payload |
| 34 | PATH_CAPACITY_FINISH | path ID, calibration ID, declared train payload byte count |
| 35 | PATH_CAPACITY_RECEIPT | path ID, calibration ID, received train payload byte count |

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
ownership and ordinary measured Subflow admission require local bulk-rate
evidence except for the current active/lower-frontier Service itself. With
active direction-relevant response demand, the bounded
same-underlay startup Subflow epoch may use local sender evidence before
bulk-rate graduation; peer or configured
hints alone cannot start that epoch.
Independently, a sustained bulk-only TCP request/upload sender may use a bounded
same-underlay startup epoch when its exact Active attachment is the stable,
direction-correct bulk-rate-proven Service and the candidate is an exact
Validation attachment instance with successful local path proof produced after
that instance attached. Neither configured nor peer hints satisfy the request
proof requirement, and proof-byte rate is not bulk-rate evidence. A QUIC request
candidate instead needs exact fresh post-attachment non-app-limited native
packet-ACK evidence before ordinary optional ownership.
Configured startup rate hints are advisory priors and MUST retain that
provenance; they MUST NOT be relabeled as local non-application-limited delivery
evidence.
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

Persistent flow-share occupancy is a Service-placement reservation ledger, not
carrier membership and not a count of every stream that has ever placed one
optional owner quantum on a path. A reliable stream reserves one persistent flow
share on each of its Active attachments and MUST NOT reserve a share merely
because a Validation or Repair attachment exists. An implementation MAY reserve
a candidate provisionally while an attachment open is in flight to serialize
path choice, but it MUST release that reservation after a successful passive
attachment. A bulk-proven Validation attachment may remain protocol-role
Validation while it contributes measured Subflow `OwnerData`; each such quantum
MUST pass Subflow/no-worse admission, and its actual queue, inflight, ordering,
and optional overhead debt MUST immediately enter later path snapshots. That
optional work does not become a continuously active flow-share reservation
unless the attachment is explicitly promoted to Active or the implementation
separately creates a durable Subflow reservation. Validation proof bytes,
bounded startup samples, and RepairData follow the same live-debt rule and MUST
NOT divide modeled capacity merely because the passive attachment exists. An
accepted Active promotion adds the persistent share exactly once in the
stream's current lane; lane changes, detach, replacement, failure, and teardown
update or release only shares that the stream actually reserved.

The response-side startup-flow ledger is separate from that per-path occupancy
ledger. For the startup gate, one active direction-relevant reliable response
flow means one logical response stream in the sender-to-receiver direction that
currently has at least one Active attachment. That logical stream contributes
exactly one to the session count regardless of how many Active carrier
attachments it has, and it contributes zero when it has only Validation,
Repair, or no attachments. A zero-to-one or one-to-zero transition MUST update
the same session load generation used to fence Subflow admission. Opposite-
direction reliable streams do not enter this sender's count. Realtime datagram
flows likewise do not enter the reliable-response-flow count, but they remain
categorical pressure through the separate latency/realtime ledger. The sender
MUST snapshot the response-flow count and generation together and MUST reject a
startup-sample commit if that generation changed before enqueue.
The TCP request-side discovery ledger is also separate from both per-path
occupancy and the response-flow ledger. One active direction-relevant reliable
bulk request flow means one logical stream whose local request direction is
open, has crossed the bulk-demand threshold, has present queued or outstanding
request data, and whose exact committed ordered Service is TCP. It contributes
exactly one regardless of how many carrier attachments it has. Reverse-direction
bytes, idle completed uploads, and QUIC-Service flows never count, and per-path
attachment load does not add logical contention. Exact Service-family handoff
updates the registration before another item in the same sender batch.
Fresh request startup and fresh zero-spend ACK-clock calibration require at
least two such flows. No path-wide completion estimate may veto fresh request
calibration until request-direction, provenance-bound authority exists.
Once an exact startup owner is assigned or a calibration has spent its first
byte, that exact epoch may finish and drain after a two-to-one transition so it
does not strand lower offsets. The exact ACK completing a sealed startup sample
and the exact ordered receipt ACK are equivalent candidate-local causal
boundaries; the first valid event wins. Request calibration then freezes one
explicit exact-instance owner and its bounded target. QUIC requests have
neither product startup nor product-ACK calibration and instead require
attributable native packet-ACK evidence.
This stability gate does not establish one-flow optional-path aggregation for
either carrier family; that use case remains unproven until it has independent,
direction-correct attributable capacity evidence.

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
4       version = 2
5       direction
6..22   connection salt, 16 bytes
22..30  counter u64
30..34  ciphertext length u32
34..    ciphertext || tag16
```

For client-to-server records the connection-salt field is `client_salt`; for
server-to-client records it is `server_salt`. Section 7.3 defines their key
schedule and binding rules. The entire fixed 34-byte header is AEAD additional
authenticated data.

The AEAD plaintext is exactly one encoded product frame. A bounded
writer run MAY serialize multiple consecutive envelopes in one socket write,
but it MUST assign each product frame its own counter, header, authentication
tag, and receiver-visible decrypt boundary. A sender MUST NOT coalesce several
product frames into one AEAD plaintext, and a receiver MUST reject trailing
product frames or bytes. The receiver MUST validate:

* magic is `MPTE`;
* version is 2;
* direction is the expected peer direction;
* the connection salt authenticates and matches the salt already committed for
  that direction, if any;
* counter equals the next expected counter for that direction;
* ciphertext length is at least 16 and does not exceed `max_frame_bytes + 16`;
* AEAD tag verifies;
* decrypted product frame validates.

The sender MUST preflight counter availability, invoke AEAD once, emit the
complete envelope, and then increment the counter after a successful write. It
MUST NOT retry an uncertain partial write on the same connection. The receiver
MUST increment the expected counter after a successful read. Counter gaps or
replays are fatal to that underlay path.

An interrupted, cancelled, timed-out, or failed encrypted write can leave both
the byte-stream boundary and nonce counter uncertain. The writer MUST
permanently poison that TCP connection before control returns and MUST reject
all later records without invoking AEAD. Runtime deadline owners MUST retire the
poisoned carrier even when product feedback had already acknowledged the
request; an ACK does not make a partially emitted later control record safe.

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
decision. A response sender MUST NOT use peer metrics, control-only metrics,
unknown loss reported as zero, or unknown carrier flight reported as zero as
authority for ordinary bulk delivery rate, pacing rate, bytes in flight,
inflight limit, or optional-path product-flight capacity. App-limited metrics
have the same prohibition except for the current QUIC Service's narrowly scoped
feed predicate: a sufficiently large local carrier ACK-derived DATA sample may
unlock bounded source/emission staging. Independently, substantial uniquely
owned product `STREAM_ACK` progress may unlock the same feed boundary. Neither
authority publishes a bulk rate or admits an optional path. Authoritative
carrier values otherwise come from local sender evidence for the same direction,
or from unpolluted product delivery samples where no packet-level carrier metric
exists.

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

TCP carrier reuse is attachment-open lane-class scoped, not topology scoped. The
first control/latency/realtime open on a configured TCP path lazily creates that
class's carrier actor; later opens in the class reuse it until carrier failure.
Attachments already opened as throughput/background use the path's separate
reusable service carrier. An attachment that starts latency-first and later
promotes to throughput stays on its existing carrier; promotion changes
sender-service lane priority and budgets, not carrier identity. This bounds
handshakes, authentication work, connection state, and congestion domains
independently of concurrent stream count while preserving stream/path ownership.
Priority queues, bounded writer runs, and independent MPTE records protect
application control and interactive work at product-frame boundaries, but they
do not remove kernel TCP head-of-line blocking within a shared carrier. A
single-path deployment and one member of a multipath deployment follow the same
rule.

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
Only an Active attachment reserves that product stream's persistent ordinary
flow share on the carrier. Repair and Validation attachments remain passive in
that ledger even while measured Validation Subflow `OwnerData` or their real
proof, sample, and repair bytes remain visible as queue, inflight, ordering, and
overhead debt under Section 9.
Validation traffic remains subject to ECF/BLEST-style admission, flow control,
and a finite validation budget. For ordered reliable streams, Validation credit
is not throughput evidence. A validation path without local sender evidence
MUST NOT own the only copy of new ordered bytes while any ordinary
ordered-data owner exists. This is true for both same-underlay and
cross-underlay validation. It validates by duplicate stream data that is also
sent on an admitted ordinary path, repair data for an already-missing range, or
carrier/control probe traffic until local sender evidence exists. Once local
sender evidence and active direction-relevant response demand exist, one
same-underlay response candidate may carry new later offsets as
unique data only through the
bounded startup Subflow epoch in Section 18.1. Other unproven Validation paths
remain excluded. Liveness from the
open itself is not sender evidence, the epoch is disabled under
latency-sensitive/realtime pressure, and neither sampling nor one in-order frame
promotes the candidate to Active. A receiver MUST NOT promote a Validation or
Repair attachment to the Active data slot merely because one frame arrived in
order. For bulk streams, receiver-side Active promotion is allowed only after
delivered application bytes have been accounted into the path model and the path
has local delivery samples or ACK-derived carrier data samples. Configured
hints, successful opens, control
probes, RTT-only liveness, and single duplicated stream ranges do not satisfy
this requirement.
For sustained bulk-only TCP request/upload, the request sender may use the same
bounded exception without consulting the response-flow count, but a fresh
exception additionally requires at least two active direction-relevant logical
bulk request flows. The exact Active attachment MUST remain its stable,
direction-correct bulk-rate-proven Service. The selected same-underlay
Validation instance MUST have a matching path proof produced after that
attachment instance opened.
Other request candidates remain proof-only, and path proof itself neither
supplies bulk-rate evidence nor promotes the Validation attachment. A QUIC
request Validation path remains proof-only until exact fresh post-attachment,
non-app-limited native packet-ACK evidence admits it as an ordinary Subflow.

Those samples make a bulk Validation path eligible for measured Subflow
admission; they do not by themselves authorize an Active Service migration.
In particular, carrier-level bulk evidence shared by independent product
streams MUST NOT cause every stream that observes delivery on that carrier to
reannounce it as Active. Ordinary bulk delivery, including delivery on a
bulk-rate-proven Validation or Subflow attachment, MUST preserve the current
Service placement. An Active reannouncement for bulk requires a separate,
explicit frontier-safe migration or failover decision based on carrier failure,
product stall, or missing-owner recovery. This separates simultaneous Subflow
use from Service migration and prevents independent bulk streams from
converging on the same low-latency carrier merely because it produced the first
alternate delivery sample.

The same separation applies inside the response sender. If a measured
same-family Subflow owns the oldest authoritative ACK hole, it remains a
Subflow while it is eligible to continue that lower frontier. Lower-range
ownership is an ordering constraint, not Service authority. The sender MUST NOT
relabel that Subflow as Service, commit it as the stream's Service key, or grant
it the Service feed envelope merely because it owns the hole. Other paths remain
blocked from later unique data until the authoritative lower debt clears. If
the lower owner fails Subflow/no-worse admission, the sender waits or emits
justified bounded RepairData; it does not turn the debt into implicit migration.

Repair and Validation opens are attach-only. If their stream ID is unknown to
the receiver, or if the receiver has recently closed that product stream, the
open MUST be rejected or ignored as stale product control. It MUST NOT create a
new outbound target connection. Active opens create a product stream only when
the stream ID is not in the receiver's recent closed-stream cache; an Active
open for a recently closed stream is also stale reattachment control and is
rejected or ignored without opening the target again. This rule keeps path
validation and reannouncement from replaying user connections during races
around stream teardown. Registry removal and recent-closed insertion MUST be one
serialized `streams -> closed-streams` transaction. A concurrent open cannot
observe the stream absent before its tombstone is visible, and late frames
cannot be routed into a resurrected product connection with the same ID.

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
teardown. Every accepted response-output channel has a binding-local, nonzero
output incarnation. An idempotent reannouncement that leaves the role unchanged
keeps that incarnation; a role transition, new attachment, or closed-output
replacement receives a fresh incarnation. Sender targets, product-flight records, and deferred ACK-hole
records carry that incarnation in addition to the carrier path key. If an
`OPEN_STREAM` arrives on a different live carrier channel for a
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

Every authenticated server-side carrier connection also receives a distinct
carrier-instance ID. This identity is separate from the binding-local output
incarnation: it scopes TCP/QUIC sender metrics, sampler publications, and the
server metric cache to the connection that produced them. An idempotent open on
the same live carrier retains its carrier-instance ID; a replacement connection
receives a new one even when session, underlay, and path ID are unchanged. A
metric update or response attachment MUST carry a live registration lease for
that exact instance. The connection accept loop, every spawned carrier-stream
task, and any background metric sampler retain cloned leases while they may use
the instance. Releasing the final lease makes the instance inactive, removes
its cached metrics, and detaches any orphaned response outputs for that
instance. Thus a delayed sampler cannot publish after retirement, a parent
accept-loop exit cannot retire an instance still used by a child, and cached
metrics from an old connection cannot prove its replacement.

Stream close enqueues carrier-local close commands; it does not separately
debit lane-load state. The carrier command handler owns ordinary output
detachment, while final carrier-lease retirement and binding destruction are
idempotent fallback cleanup. All three converge on removal of the matching live
output before decrementing lane load, so a close followed by carrier teardown
cannot double-debit another stream sharing the same session/path load counter.
Each accepted QUIC reliable-output task additionally owns an idempotent detach
guard from successful attach through every normal or abnormal return. A remote
session close, malformed frame, read error, or local writer failure therefore
cannot leave that task's closed command output, owner hint, or lane load alive
merely because sibling tasks still retain the shared carrier lease.

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

For the local-to-remote request direction, a product stream MUST NOT treat the
full advertised receive window as immediate source-read permission. One
stream-local product-admission window counts undispatched unique source bytes
and ACK-retained repair bytes together. It retains one source-queue reservoir
while classifying the flow, starts bulk work at the resource-derived Service
feed reservoir, and may double only after exact unique `STREAM_ACK` release
consumes at least `max(ceil(window/2), durable_product_floor)` within the exact
ordered Service instance's PTO. The durable floor is the resource-clamped
startup-sample limit. Growth is attributed from the flight ledger to the exact
attachment instance that was the single `OwnerData` owner. That owner MUST
still be the current ordered Service instance or an exact live same-family graduated
Subflow; a graduated TCP Subflow additionally requires usable ACK-clock proof.
The ordered Service instance supplies the window epoch and PTO clock; the carrier on
which the ACK arrived is irrelevant. Repair-only, duplicated, ambiguously
attributed, cross-family, or stale-instance ACKs may release product retention
but MUST NOT grow the window. Every exact TCP/QUIC ordered-Service handoff resets
the epoch; Active-list churn alone does not. If Service placement is temporarily
absent, the prior bound remains in force. Product turnover never initializes or changes TCP congestion state or
QUIC delivery rate, pacing, congestion window, or optional-path authority.

`STREAM_ACK` proves that the receiving mptunnel endpoint accepted contiguous
product bytes into its local target socket; it does not prove that an arbitrary
target application consumed them. This admission rule therefore bounds source
queue and repair retention without claiming end-application delivery telemetry.

A sender-side product stream starts with exactly the peer-advertised credit. An
implementation MUST NOT manufacture the configured stream window as local send
credit before receiving the peer's open/MAX_DATA credit. Bulk TCP and QUIC
receivers advertise the configured receiver-memory window independently of path
proof; latency QUIC MAY retain its smaller lane-isolation startup window. The
separate request product-admission window, sender queues, and native TCP/QUIC
backpressure bound source and network flight below that receive-credit ceiling.
Advertised product credit is neither carrier capacity proof nor a congestion
window.

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
bounded critical repair path. Final-tail repair is connection-completion
`RepairData`, not generic ACK-gap repair; if every distinct survivor lacks
immediate stream-data queue credit, or no distinct survivor is currently
attached, it may use the current Service survivor without changing Service
ownership or creating path delivery evidence. Repair candidate selection is
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

Datagrams are unordered. TTL establishes one absolute product-expiry time
before carrier selection. Carrier setup, flow setup, pacing, request emission,
feedback/response waiting, and pre-feedback fallback all consume its remaining
time; none may reset it. TTL is not permission to replay an acknowledged or
expired product datagram. A path whose ETA cannot fit the remaining TTL SHOULD
be avoided.
`DGRAM_FEEDBACK` acknowledges received datagram ID ranges. QUIC UDP feedback
feeds scheduler RTT/loss/delivery-rate observations; TCP-carried feedback feeds
association-local response timing and later useful-payload delivery accounting.

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
The previous successful datagram carrier is only a hysteresis hint: it MAY break
near-ties to avoid avoidable realtime path churn, but it MUST NOT override a
substantially lower-ETA, lower-loss, or lower-queue candidate once live
path-scoped evidence says the alternate is safer. This keeps datagram scheduling
metric-driven without replaying a product datagram ID.

When multiple fresh datagrams are outstanding at the local UDP edge before
feedback arrives, edge lanes SHOULD reserve different schedulable carriers from
the same metric-ordered candidate list when alternatives exist. This is
route-diversity for independent product datagrams, not duplication: each
`DGRAM_DATA` still has exactly one selected carrier, and a route hint MUST be
ignored if that carrier is no longer schedulable within the TTL/freshness
budget.

After a `DGRAM_DATA` frame for a product datagram has been acknowledged by
`DGRAM_FEEDBACK`, the sender MUST NOT resend that same product datagram ID on
another carrier, reopen a carrier for it, or retransmit it after absent target
response. TCP and QUIC already own packet/stream recovery below this layer, and
QUIC DATAGRAM-style applications are freshness-bound rather than reliable.

If the selected carrier/path times out before any product feedback acknowledges
the request and another schedulable carrier can still complete inside the TTL,
the client MAY treat that as pre-feedback path failover and send one fresh
datagram attempt on the next evidence-ordered carrier. This two-attempt limit is
global across TCP and QUIC UDP underlays, including same-family replacement
paths. The failed carrier is
suppressed by PTO-derived path failure backoff. This is not a repair or
duplicate-discovery mechanism: it creates no ordered-stream debt, no path
delivery proof, and no right to replay after feedback. Once feedback has
acknowledged the datagram request, response absence is terminal product expiry.
This boundary keeps UDP target delivery unreliable and freshness-aware while
avoiding a single dead carrier from consuming the whole realtime TTL budget
before the server has even acknowledged the request.

For a TCP-carried datagram that needs a carrier, TCP dial, authenticated MPP and
`PATH_JOIN` setup, datagram-flow setup, request emission, and feedback/response
waiting consume the original absolute TTL. QUIC UDP carrier handshake, MTU
probing, pacing, flow setup, request emission, and feedback/response waiting use
the same rule. When an unattempted carrier remains, a pre-feedback attempt
reserves part of the remaining TTL for that alternative; after
`DGRAM_FEEDBACK`, response waiting may consume the rest of the original product
TTL but carrier replay is forbidden.

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
runs or the stream becomes idle again. Bulk prevalidation is not a one-initial-
window event and it is not an eager start-of-stream event: it uses an amortized
multi-window floor capped by the service-quantum/rate-evidence floor, so short
latency streams do not spawn per-stream Probe opens that cannot be amortized,
while sustained bulk can start Subflow validation before full throughput
promotion. Prevalidation alone MUST NOT allow the latency owner to accumulate
megabytes of lower ordered bytes that later block Subflow admission.
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
from a stale active path through explicit frontier-safe migration or failover.
Per-chunk ECF admission may use another measured path as a Subflow, but ordinary
bulk delivery on that path MUST NOT implicitly rewrite the stream's Service
placement.

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
initial congestion window and early MPTCP subflow growth are probe mechanisms,
not accurate bulk-rate priors. On a sustained bulk-only response stream in a
session with active direction-relevant response demand and no active
latency-sensitive or realtime pressure, one same-underlay TCP or QUIC
Validation candidate with
local sender evidence may enter the bounded startup Subflow epoch. The candidate
receives repeated preemptible unique `OwnerData`
quanta up to the cumulative epoch budget, even when a low underfed rate would
lose the ordinary completion comparison. The projected live Service suffix plus
candidate debt must still fit the same-underlay reorder budget. Direction-correct
bulk-rate evidence and the ordinary product inflight, carrier credit,
completion, and reorder gates are required after the epoch. Same-underlay ETA
can be an artifact of underfeeding and validation; the authoritative proof is
whether the additional path has delivered enough unambiguous path-scoped owner
data without creating ordered receive-hole debt. An ACK hole, missing owner,
failed owner, Repair role, or other authoritative lower debt blocks startup
sampling. Cross-underlay candidates remain strict during startup because TCP
and QUIC expose different queueing and HOL behavior.

TCP request/upload bootstrap uses the same bounded sampling mechanism but an
intentionally different eligibility signal. Once Auto has classified one
TCP request stream as Throughput or Background and its local source continues to
offer ordinary data, that stream has sustained request-side bulk demand; it
does not need a second request stream. The exact live ordered Service attachment
MUST remain Active and direction-correct bulk-rate-proven. One freshly proven
same-underlay Validation attachment instance at a time may then receive the
bounded startup `OwnerData` defined in Section 18.1. Path proof ranks and
qualifies the current attachment instance for sampling, but its proof-byte rate
is not capacity evidence. The stable Service, latency-pressure, resource,
ordering-debt, and graduation rules remain mandatory. QUIC request candidates
do not use this product sample; they require exact fresh post-attachment,
non-app-limited native packet-ACK evidence before ordinary optional ownership.

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
The evidence mechanism is symmetric, but bounded startup eligibility is
direction-specific: response sampling requires one active direction-relevant
reliable response flow and retains its generation fence for TCP or QUIC, while
TCP request sampling uses sustained
demand, its exact ordered Service attachment, and current Validation attachment
evidence. QUIC request paths instead enter ordinary optional ownership only from
exact fresh post-attachment non-app-limited native packet-ACK evidence.

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
FIN/RESET/DETACH, and bounded correctness repair remain priority work in the
sender-service queue and MUST NOT be delayed behind not-yet-admitted bulk source
bytes. Repair `STREAM_DATA` still uses the carrier stream-data queue after
admission; carrier control priority is reserved for control/probe/latency
frames. If capacity is unavailable, the sender
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

For a TCP carrier, an outbound writer run whose socket write can block MUST
either continue routing established inbound product feedback while that write
is pending or return to inbound polling after at most one maximum 64 KiB product
service quantum. The interlocked form MAY serialize the larger bounded run as
multiple independently authenticated envelopes in one socket write; it stops
reading at the first pending-open, session-control, or delivery-backpressure
barrier so later frames cannot overtake it. This is separate from sender-service
admission. It prevents a full TCP socket buffer from serializing already-read
product ACK routing behind the 512 KiB read/pass ceiling without imposing one
syscall per product quantum on a healthy high-rate carrier. QUIC UDP retains its
carrier-owned packet scheduling and may use the larger bounded writer-feed run
above.

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
Initial Active opens are bounded path attempts, not unbounded waits on the
first configured candidate. Let `open_phases(TCP) = 3` for TCP dial,
authenticated MPP/`PATH_JOIN`, and product open/accept, and let
`open_phases(QUIC_UDP) = 2` for QUIC/path setup and product open/accept. A
candidate with another schedulable alternative gets
`open_phases(path) * open_pto(path)`. A sole candidate gets
`(open_phases(path) - 1 + sum(2^i, i=0..persistent_congestion_threshold-1)) * open_pto(path)`,
which is nine PTOs for TCP and eight for QUIC UDP. Every initial Active TCP
attempt keeps the conservative initial PTO floor even after a low-latency idle
probe because the shared session actor may establish or re-establish its carrier. Explicit
Active reattach, Repair, and Validation/recovery opens instead get one candidate
PTO. These deadlines are intentionally different. On expiry the path actor
rejects or detaches the pending open, the sender releases reserved load, marks
the path failed/suspect for data scheduling, and tries the next schedulable
candidate with a fresh product stream ID. A timed-out initial open can still
arrive late at the peer; using a fresh ID prevents that stale candidate from
creating and closing the product stream ID that a later candidate needs.
Attach, Repair, and Validation opens attach to an already accepted product
stream and therefore MUST reuse that accepted stream ID.
When an initial Active open fails or times out and another schedulable path
exists, the failed candidate enters the same data-plane cooldown used for active
reopen failures. A sole remaining path may remain probeable, but a user-visible
new connection MUST NOT repeatedly spend its startup budget on a candidate that
just failed while a survivor exists.
This is the reliable-stream analogue of Happy-Eyeballs connection
establishment and MPTCP path-manager failover: it improves startup resilience
without making failed opens delivery evidence or granting owner rights to
unmeasured paths.

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
offset bytes as original transmission. Its sender-service scheduling lane is
nevertheless repair priority, not the original stream's source-data lane.
Implementations MUST NOT leave critical repair behind not-yet-admitted ordinary
bulk source data when a receiver has an active ordering hole. Once admitted to a
carrier, repair `STREAM_DATA` uses the carrier stream-data queue and therefore
does not overtake already-enqueued same-carrier stream data or control frames.
Repair generation itself is also preemptible: one ACK gap, path failure, or
stall event MUST NOT emit an
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
to the sender-service queue, stream-credit, and memory budgets. Repair-cache
capacity is enforced when dispatch assigns offsets and ownership. Read loops MUST
NOT pre-create later-offset frames because doing so would assign product
ownership before path admission and repair priority are known. If the
path-flight ledger shows that lower offsets are outstanding on other paths and
no attached path can safely advance the ordered frontier, the sender pauses
conversion and dispatch of later raw bytes and continues servicing control,
ACK, repair, latency, and carrier events. Bounded raw source staging MAY
continue within the Service horizon/feed reservoir and the harder sender-queue,
stream-credit, and memory limits because it does not assign an offset or owner.
After exact Service feed evidence, a switchable response Service MAY stage
against the configured product envelope; for the current QUIC Service, this may
be substantial uniquely owned product progress or a durable local carrier ACK
estimate. Every existing ordered owner tail and raw queue byte is charged to
that envelope, and TCP or QUIC still enforces carrier congestion and
backpressure below it.
That raw reservoir is independent from assigned owner tail only for a response
with live owner-capable TCP and UDP outputs; single-family responses retain the
coupled staging reservation defined above. Optional same-family outputs do not
reduce source feed merely because they participate: the global owner-tail ledger
already charges their assigned bytes, and their own path admission remains
mandatory. The sender
MUST NOT create new later-offset `STREAM_DATA` merely to keep an active path
busy, because doing so moves the fairness boundary behind hidden path queues and
expands receiver ordering debt before ECF/BLEST admission can reject it.

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
flow sharing, health, and capability state. Ordinary Subflow owner bytes require
direction-correct bulk-rate evidence. When the session has at least two active
direction-relevant reliable response flows, one response-side candidate may
instead use the bounded same-underlay startup sampling epoch described below. A
sole active direction-relevant reliable response flow keeps unmeasured
alternates outside unique-data ownership. A sustained bulk-only TCP
request/upload stream may open a separate request-side startup epoch when at
least two active logical bulk request flows have exact committed TCP Service
ownership and present request work, and its exact Active attachment remains the
stable, bulk-rate-proven Service. Individual dispatches consume the
selected candidate's cumulative
credit; they do not recreate that candidate's validation or startup-sample
credit from scratch, and ordinary ACK progress does not reset either credit.
After explicit graduation and release of that candidate's ordering-owner
flights, a different never-sampled attachment instance may receive its own
finite credit. ACKs update the per-range flight ledger,
delivery samples, and the next admission calculation; detach, carrier close,
failover, or a changed Service/envelope resets the subflow set. Additional paths
attached to the same stream are not automatically ordinary data paths. Their
role decides what the scheduler may do: Repair paths carry gap-targeted repair
or failover repair, Validation paths may receive bounded proof traffic, and the
Service path may carry ordinary data. A path with any role may carry a specific
repair frame when it is the best survivor and
avoids the path that likely lost the original bytes.

On the response-side binding, attaching a previously absent passive Validation
or Repair output MUST advance the planner generation so a pre-attach
optional-owner or Subflow plan cannot
commit afterward, but it MUST preserve the existing subflow-set epoch identity,
Service, startup candidate, cumulative startup spend, and measured members.
Passive membership growth MUST NOT reset, refill, or transfer startup-sample
credit. A pre-attach Service plan carries no optional-owner spend and MAY still
linearize one bounded preemptible quantum when enqueue atomically revalidates the
exact live output key, channel, incarnation, and non-Repair role; the next
quantum uses a fresh plan. Active attachment, output replacement, role change,
detach/failure, explicit Service change, and envelope change remain semantic
response reset conditions. TCP request-side passive attachment likewise preserves
the existing `request_subflow_set`; its send path instead revalidates exact
`RelayPathInstance` values and restores the previous set if carrier enqueue
fails.

Same-stream bulk striping is allowed for TCP, UDP, and mixed TCP+UDP reliable
streams only when the candidate passes the same role-aware admission framework
and no-worse model used for Service and Subflow decisions. This is intentionally
stricter than "all attached paths may send."
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
proof and does not itself make the path an ordinary measured bulk subflow.
Before local sender evidence exists, same-family proof paths MUST NOT receive
product `OwnerData`. Once that evidence and at least two active
direction-relevant reliable response flows exist, a single same-family
Validation candidate on a bulk-only response stream may receive its cumulative
startup-sample budget. When at least two active logical bulk request flows have
exact committed TCP Service ownership and present request work, a sustained TCP
request/upload stream may instead select one same-family
Validation attachment instance after that exact instance has fresh local path
proof and while its exact Active Service remains stable and bulk-rate-proven.
Each bounded exception exists to produce the
direction-correct path evidence that an underfed candidate cannot otherwise
earn. QUIC request paths do not use the product startup epoch; their optional
ownership requires exact fresh post-attachment non-app-limited native packet-ACK
evidence. A path with ACK-data evidence but no bulk-rate evidence otherwise remains
`Probe`, `Standby`, or `RepairOnly`; ACK-data visibility keeps the path in the
validation/ranking set but does not authorize ordinary measured ownership.
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
except for failover, explicit repair, control proof, or, in a session whose
active direction-relevant reliable response-flow count is at least one, its one
bounded startup Subflow sample epoch. That epoch is cumulative
and MUST NOT refresh on ordinary ACK progress, a scheduler retry, or an
app-limited metrics poll.

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
ordering-debt model used for the Service path. There are two explicit Subflow
owner gates: ordinary measured admission requires direction-correct bulk-rate
evidence plus no-worse completion, ordering-debt, queue, and overhead guards;
startup sampling requires the one-candidate, same-underlay, bulk-only bounded
epoch and all of its debt and pressure guards. The response branch additionally
requires active direction-relevant response demand. The
fresh TCP request branch instead requires at least two active logical bulk
request flows with exact committed TCP Service ownership and present request
work, sustained local bulk demand, the stable bulk-rate-proven Active Service
instance, and fresh proof for the exact Validation instance. The
startup gate is not a second Service-election path.
`RepairOnly`, `Standby`, and `Failed` outputs cannot receive speculative owner
bytes. Role transitions are monotonic with evidence and carrier state for the
current decision; they are not implied by attachment order, carrier family,
configured path order, or temporary queue availability. In particular, `Probe`,
path-proof-only, and sender-evidence-only paths are not permission to carry an
unbounded stream of future offsets. With active direction-relevant response
demand, one sender-evidenced Validation path may use the finite
startup-sampling epoch, but remains unmeasured at its cap. The TCP request-side
counterpart additionally requires two active logical bulk request flows with
exact committed TCP Service ownership and present request work when its
sustained-demand, stable-Service, fresh-Validation-instance epoch begins;
it is equally finite and remains unmeasured at its cap. The only unmeasured path
that may continue as the Service
is explicit frontier-clear
Service failover after the previous Service is gone; that
exception elects one new Service path and remains subject to ordinary Service
feed/admission limits.

An attachment whose protocol role is `Repair` remains excluded from unique
`OwnerData` even if it has historical bulk-rate evidence. Evidence does not
silently relabel Repair as a measured Subflow. Only an explicit accepted Active
reannouncement may change that attachment role, and that role change resets the
subflow-set generation before later owner admission.

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
When a Service path is already active, the first validation candidate SHOULD be
a same-family survivor if one is available. This is not a TCP or UDP preference:
it keeps a repair/failover-capable sibling ready for the current Service family
before spending the one-shot validation open on cross-family probes. The
cross-family candidates remain eligible as later probes and as live repair
fallback when no same-family survivor can carry the blocking range.
For a given product stream and carrier path, validation/probe attachment is a
one-shot path-manager attempt. A path that has already been attempted for that
stream MUST NOT be reopened by prevalidation or rebalance simply because the
previous validation handle closed, failed to graduate, or stopped being attached.
The next action is a scheduler decision over the current path set: keep the
path as `Probe`/`Standby`/`RepairOnly`, wait for new evidence, or use another
candidate. A later product stream may probe the path again, and explicit
failover recovery may open a survivor when required for correctness, but normal
bulk validation MUST NOT create repeated same-stream `OPEN_STREAM` churn.

TCP request-side startup sampling is sequential even when several Validation
attachments are already present. The per-flow subflow set records at most one
unproven `startup_owner` attachment instance. That instance retains exclusive
use of the startup slot until it has direction-correct bulk-rate evidence and no
remaining ordering-owner flight, or until detach/failure invalidates the
current set. Explicit graduation clears the startup slot but retains the
sampled instance as a subflow-set member, so the same instance cannot consume a
fresh startup budget. A different never-sampled same-underlay Validation
instance may then become `startup_owner` and receives its own cumulative,
non-refilling budget. At no point may two unproven request candidates own unique
startup ranges concurrently.

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
the ordered-stream hole being measured, so proof credit MUST NOT be treated as
ordinary bulk capacity. Before local sender evidence exists, the sender
duplicates the same `STREAM_DATA` on an admitted ordinary path and the
validation path, sends repair for an already-missing range, or sends
carrier/control probes that do not create a new application-data dependency.
After local sender evidence exists, the bounded response-side startup Subflow
epoch is one exception: while at least one active direction-relevant reliable
response flow has sustained bulk demand, one same-underlay Validation candidate may own a
limited sequence of unique future ranges while the live Service owns only an
ordinary contiguous suffix and the combined projected debt fits the reorder
budget. A session with no active direction-relevant reliable response flow
continues using carrier/control proof instead of unique future ranges.
The other exception is the separately gated sustained TCP request/upload epoch
defined below; it uses the same debt boundary without weakening this response
active-demand rule. This
follows QUIC path validation and MPTCP/MPQUIC subflow probing while adapting them
to a product-layer stream that must avoid unbounded receive-hole debt.

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
itself make the path an ordinary measured `STREAM_DATA` owner when another
admitted ordinary path exists. It MAY satisfy the local-sender-evidence
prerequisite for the separately bounded same-underlay startup Subflow epoch.
Unknown, duplicate, or stale proof ACKs are ignored. Proof payloads are bounded
by the latency/preemptible startup quantum and by the frame payload envelope.
For TCP request-side startup eligibility, the matching proof exchange MUST have
completed after the exact Validation attachment instance was created. A proof
record from an earlier attachment, a successful open without proof, or a proof
record whose current path state is no longer Active for scheduling is stale.
Even a fresh proof-byte-rate sample remains proof provenance only and MUST NOT
initialize `delivery_rate`, `pacing_rate`, bulk-rate confidence, or graduation.

`PATH_CAPACITY_DATA(path_id, calibration_id, payload)` is a different,
QUIC-only response-capacity mechanism. It is sent only after ordinary path
proof has established reachability, on one exact UDP Validation attachment
reserved by a multi-flow imbalanced response session. Like `PATH_PROOF_DATA`,
it has no product offset and never enters product ACK, flight, repair, or
ordering ledgers. The sender gates ordinary connection writers, emits bounded
Data records, then emits
`PATH_CAPACITY_FINISH(path_id, calibration_id, payload_bytes)` on the same
ordered QUIC stream. After consuming exactly that declared train, the client
returns
`PATH_CAPACITY_RECEIPT(path_id, calibration_id, received_payload_bytes)`.
The full matching receipt is the local ownership proof. Quinn's
connection-aggregate packet ACK bytes, pacing, loss, and timing remain
provisional diagnostics and cannot identify the token. TCP rejects all three
capacity records, and ordinary QUIC `SendFrame` admission rejects them; only
the typed server command and the explicit client/server peer roles may emit
them.

One typed command freezes the calibration ID, path instance, sample floor,
accounting slack, warmup, required proof bytes, live carrier window, exact
train, attempt deadline, proof-validity duration, and invalidatable ownership
ticket.
The train is the larger of the startup proof floor or the live carrier window
plus one fresh strict-proof window. It MUST fit the remaining cumulative,
non-refilling session resource envelope without clamping and be admitted
atomically. Each exact
session/path/path-instance key permits at most two attempts, and an eligible
never-attempted key precedes a retry. Reservation is provisional until the one
typed command is admitted; only failure of that exact provisional admission
refunds its count and bytes. A committed attempt has a bounded
feed-horizon-plus-PTO lease, while proof, completion, expiry, or detach does not
refund session spend.

TCP MUST NOT use these records to replace its product-ACK calibration, and QUIC
MUST NOT treat write-buffer acceptance or aggregate native ACK bytes as
capacity proof. Ordinary optional-path capacity evidence retains
non-app-limited filtering; the weaker current-Service feed predicates do not
satisfy this capacity contract.
Capacity rate uses the exact full-train byte count and the complete
sender-to-receipt interval bounded by timer granularity; later native carrier
timing never supplies or mutates its numerator, denominator, or completion. The exact token, path instance,
frozen geometry, full written/received byte count, and proof lifetime MUST match
the live reservation, which remains held until the registry publishes the
marker. Exact committed whole-train receipt releases the carrier writer gate;
native BIF and send-watermark snapshots remain cleanup diagnostics because a
receipt-triggered ACK-only send need not receive another ACK callback. The
conservative interval is the maximum of one millisecond and the full
sender-to-receipt elapsed time. The full train is the rate numerator. The attempt deadline
bounds command, write, and receipt completion. Candidate acceptance time is the
carrier receipt time, and candidate expiry is that time plus the proof-validity
interval frozen from the planning RTT snapshot; later polls and RTT changes
cannot extend or shrink it. The accepted marker MUST NOT rewrite generic QUIC
delivery-rate, ACK-derived-data, product ownership, or Service state. Ticket
cancellation before start drops the command and aborts an admitted carrier
epoch. Successful registry publication instead resolves the ticket as
published and MUST NOT cancel the receipt-completed carrier. An indeterminate
partial, cancelled, or expired write MUST fail-close the connection so its
late native ACKs cannot become ordinary evidence.

Same-underlay validation is still subject to this rule. A path that uses the
same underlay family as the lead path may be cheaper and safer to validate than a
cross-underlay path, but it MUST NOT receive the only copy of a new future
ordered byte range before path-local sender evidence exists. Before then, the
sender uses duplicate `STREAM_DATA`, repair for an already-missing range, or
carrier/control proof traffic. After then, only a selected startup-sampling
candidate may receive unique owner ranges under the epoch cap and safety gates:
the response candidate requires at least one active direction-relevant reliable
response flow, while a TCP request candidate requires the sustained-demand,
stable-Service, and fresh-instance proof predicate. A QUIC request candidate has
no startup-sampling exception and needs exact fresh post-attachment,
non-app-limited native packet-ACK evidence for ordinary optional ownership.
Duplicate validation copies MUST be recorded as non-owners of the ordered
frontier so they can release carrier/product flight on ACK without making the
validation path the lower-frontier owner for later unique bytes.

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

For QUIC UDP carrier streams, Validation attachment is non-blocking with respect
to the byte-producing path. The client bounds carrier connection setup by the
one-candidate-PTO attach deadline, sends `OPEN_STREAM`, starts the carrier reader and
writer, and may return a Validation output with a zero initial product send
window. The later `STREAM_MAX_DATA` accept frame is processed through the normal
stream frame path and updates flow control when it arrives. Completing that
optimistic Validation attach proves only that the local endpoint queued
`OPEN_STREAM`; it is not peer acceptance or path liveness proof.

An initial Active attachment MUST receive the peer's `STREAM_MAX_DATA` accept or
terminal reset within its phase-counted Active deadline. An Active reattach or Repair
attachment MUST do so within its one-candidate-PTO attach/recovery deadline
before entering the attached path set. This includes a survivor expected to
carry unique Service bytes and a Repair output expected to carry correctness
`RepairData`. A timed-out candidate remains path-failure evidence and the path
manager continues to the next candidate; it MUST NOT leave a local-only
attachment that suppresses further recovery. Validation retains the optimistic
behavior above where applicable, but still MUST NOT clear a recent data-plane
failure or promote a path for new Active Service opens until peer evidence
arrives: `STREAM_MAX_DATA`, `PATH_PROOF_ACK`, carrier ACK-derived data evidence,
or ordinary accepted owner-data delivery according to the role rules below.

A stream ACK for duplicated data proves end-to-end byte delivery but does not
identify which underlay path delivered the bytes. It therefore releases product
flight for every duplicate copy of that range, but it MUST NOT by itself promote
the validation path into ordinary same-stream bulk service. Path proof ACKs are
validation/liveness evidence. QUIC UDP carrier ACK metrics and unpolluted
admitted stream-delivery samples are the sender-side bulk evidence that may make
a path eligible for ordinary unique ordered bulk ownership. Outside the explicit
same-underlay startup-sampling epoch, ordered-stream validation payload MUST NOT
be the only copy of a new future offset while any admitted ordinary path exists;
such bytes are duplicate `STREAM_DATA` or repair for a known missing range.
Ambiguous ACKs for duplicated ranges MUST NOT credit the startup epoch or
graduate its candidate.

Response-side validation uses the same principle. The server MUST NOT schedule
download bytes onto a validation path merely from generic TCP or UDP defaults,
but it MUST send bounded `PATH_PROOF_DATA` on validation attachments so TCP and
QUIC UDP outputs can gather local sender evidence without consuming unique
ordered response bytes. Before proof succeeds, a validation output remains
excluded from unique response `STREAM_DATA` except for duplicate proof or
gap-targeted repair. After local sender evidence exists and at least two active
direction-relevant reliable response flows exist, one same-underlay candidate
may use the bounded startup
Subflow epoch while the live Service has only an ordinary contiguous suffix. If
the prior Service owner is gone and the
ordered frontier is clear, an attached live output can instead become the
bounded startup Service failover path. These are distinct states: Subflow
sampling never changes Service ownership, while failover explicitly elects a
new Service. Path-scoped bulk-rate evidence still controls ordinary measured
Subflow admission and Service migration.

TCP request-side validation uses the corresponding client-to-server evidence. A
TCP request stream becomes eligible only after Auto classifies its continuing local
source as Throughput or Background. Its exact ordered Service attachment MUST be
live and already have direction-correct bulk-rate evidence. The
candidate MUST be an attached protocol-role Validation instance on the same
underlay family, MUST have a successful `PATH_PROOF_ACK` observation newer than
that instance's attachment time, and MUST not yet have bulk-rate evidence. One
candidate may then receive bounded unique request `OwnerData` as Subflow work;
this does not change the Active placement, ordered-data owner hint, or Service
identity. Fresh assignment additionally requires the active TCP-Service
request-flow count defined below; a QUIC-Service flow never satisfies it. QUIC
request Validation never uses this product startup path; exact fresh
post-attachment, non-app-limited native packet-ACK evidence is required before
ordinary optional ownership.

Before every TCP request sample commit, the sender MUST revalidate the exact
ordered Service instance, Validation role and instance, current proof observation,
Throughput/Background lane, carrier enqueue credit, and absence of path- or
session-scoped latency-sensitive or realtime pressure. It MUST also reject the
sample if a missing or failed lower owner, an ACK-range hole, queued/active
repair, or a foreign lower ordering owner is authoritative for the next offset.
A normal exact live Service product flight does not become startup stale-tail
authority merely from residence age; it may be crossed only while
the projected suffix, candidate queue/inflight, and next quantum fit the
path-flight, receiver-reorder, repair-cache, and stream-window envelope. The
later TCP ACK-clock calibration retains a separate mature-tail age gate.
Client-supplied `PATH_METRICS` are hints, not final proof of response-direction
throughput. They are useful to distinguish a plausible
high-bandwidth path from a poor or high-loss path before bounded proof is sent,
but sender-side evidence decides sustained ordinary promotion. The receiver
applies the same rule when it observes incoming stream data: ordered progress on
a validation or repair path may refresh liveness and may feed delivery sampling,
but the path becomes a unique ordered-data candidate only after that sampling
has created real delivery evidence and the no-worse ETA gates admit measured
Subflow overflow or an explicit frontier-safe Service migration. Evidence and a
lower next-quantum ETA alone do not displace a feedable Service. For sustained
bulk backlog, a measured same-family Subflow may instead use the bounded
concurrent reservoir only when it completes before Service drains the lower
ordered tail. This prevents a high-RTT, high-loss, or reordered path from winning
ordinary work because it delivered a small probe before its long-term behavior
was known.

For UDP underlays, the response sender also maintains local carrier TX metrics
from its own UDP packet controller. Once the server has ACK-derived carrier
delivery samples for a UDP path, those sender-side metrics take precedence for
response scheduling over peer hints and over ordered stream-ACK timing alone.
Stream ACKs still release product flight and prove end-to-end stream progress,
but they MUST NOT initialize, raise, or replace the UDP/QUIC carrier delivery
rate or RTT model. Ordered stream ACK timing can be delayed by receiver reorder
holes, product queueing, and application flow-control, so using it as UDP carrier
rate evidence can inflate product queues or collapse pacing independently of the
actual QUIC packet controller. It is product evidence only: it may release
repair state, update contiguous-progress diagnostics, validate that some copy of
a byte range reached the peer, and maintain a product-progress rate. Substantial
progress attributed to uniquely owned `OwnerData` may release the current QUIC
Service feed boundary, but it MUST NOT be exported as UDP/QUIC carrier delivery
rate, replace packet ACK-derived congestion evidence, drive QUIC pacing, or
authorize an optional path. This mirrors QUIC and BBR
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
it does not by itself make an optional path eligible for ordered `STREAM_DATA`
ownership. A sufficiently durable sample may release only the current Service's
feed boundary. Local QUIC pacing remains carrier-owned scheduling evidence even
when the latest ACK-derived data sample is application-limited; app-limited
status prevents that sample from becoming delivery-rate or optional-capacity
proof. The carrier
ACK-derived rate becomes bulk-rate evidence only after the acknowledged DATA byte
volume is large enough for the path's modeled flight envelope, with two bounds.
The floor MUST be at least a small multi-packet DATA sample so a tiny ACK burst
cannot create a bulk-rate Subflow, and it MUST be capped by a bounded startup
graduation window so a large transient QUIC cwnd/inflight estimate cannot make
proof self-defeating by requiring more bytes than the product scheduler will feed
before graduation. Otherwise the sample remains ACK-data evidence for validation
visibility only.

Once a path has bulk-rate evidence, a later application-limited metrics poll MUST
NOT erase that evidence or let the path bypass ordinary measured completion-gain
admission. The scheduler retains the last valid path-scoped rate and evaluates
the proven Subflow through the same ECF/BLEST completion model as other measured
Subflows. The current application-limited flag remains available for Service
feed/backlog balancing and diagnostics; only the bounded unproven startup epoch
is exempt from measured completion-gain comparison.

For TCP outputs, exact product-ledger owner progress is path-scoped evidence when
the ACKed range had one outstanding `OwnerData` copy. TCP needs an ACK-clock
fallback because the kernel carrier does not expose QUIC's packet ACK controller.
The first exact-owner window establishes only a per-output ACK boundary;
scheduler assignment time is neither kernel dispatch nor RTT and MUST NOT
publish rate. For a sealed TCP request startup sample, either the exact ACK that
completes its uniquely owned data or the exact ordered receipt ACK may install
that boundary. Later windows use ACK-to-ACK time only when every counted byte
was sent after the boundary that begins the interval. Bytes first assigned
before that boundary make the window noncausal and produce no rate sample. A
full bounded TCP request calibration window may replace the exact instance's
startup/receipt sample, but an endpoint-only candidate may retain a higher
Service-derived scheduling prior explicitly provisionally until ten continuous
exact candidate samples exist. A configured candidate retains its own hint.
Response-side TCP calibration instead uses the
stage-authorizing robust publication rule below; an ordinary strict window that
did not authorize a fully spent stage MUST NOT replace the startup rate. A
replacement attachment starts fresh and MUST NOT inherit a replacement rate or
its clock. For QUIC outputs, the same product ACK is attribution and backlog
progress only: substantial uniquely owned progress may release the current
Service feed, but it MUST NOT set carrier bulk-rate evidence, replace the QUIC
delivery-rate model, or authorize an optional path. A range with an OwnerData
copy plus any repair/duplicate
copy is ambiguous, so its ACK never increments owner delivery samples, creates
product-owner progress for a path, grows the product request window, or advances
ACK-clock calibration. ACK-data seen alone does not set bulk-rate evidence,
rewrite the ordinary lead, or authorize ordinary measured ownership.

Attached but unproven Validation paths use
`PATH_PROOF_DATA`/`PATH_PROOF_ACK` and control traffic for bootstrap. A response
sender may start a unique-OwnerData sampling epoch only when all of the following
are true:

* the response stream has sustained bulk-only work, and the stream, session,
  Service path, and candidate path have no active latency-sensitive or realtime
  pressure;
* the session has at least one active direction-relevant reliable response flow
  in the current session load generation;
* the candidate is attached in Validation role, uses the same underlay family
  as the live bulk-rate-proven Service, has local direction-correct sender
  evidence, and does not yet have bulk-rate evidence;
* no other candidate has been assigned startup-sampling credit in this response
  stream's current subflow-set epoch; and
* no authoritative lower-flight debt owned by another path,
  repair-authoritative ACK hole, missing- or failed-owner debt, or queued/active
  repair range lies below the next offset. A begun startup owner may continue
  only its own exact lower frontier while the same epoch remains eligible and
  has cumulative non-refilling credit.

The active-flow and latency/realtime pressure guards are categorical, not
smaller sampling gains. Without active direction-relevant response demand,
unmeasured paths remain Probe, Standby, or RepairOnly for unique data and the
Service keeps ordinary ownership. The sender MUST capture the session response-flow count and
its load generation together; a response-flow-count change before commit rejects the planned
sample. Exact begun work may drain after demand-count churn, but fresh sampling
requires the current generation to retain active demand. While
latency/realtime pressure exists, unmeasured
paths receive no startup `OwnerData`, and the Service stays on its preemptible
feed horizon.
The server MUST register both reliable latency attachments and realtime
datagram flows in a pressure ledger scoped to their logical session. A datagram
flow becomes pressure after duplicate/capability/target validation and before
the outbound UDP connect can block; connect failure, cancellation, flow close,
or drop releases that registration. A realtime flow carried outside the
reliable-stream registry is still pressure for its own session and cannot be
omitted from this guard. Churn in another session MUST NOT change this session's
pressure generation or reject its Subflow commit.
This keeps sampling debt out of mixed and flapping workloads whose recovery and
latency bounds take precedence over aggregation discovery.

Separately, a TCP request sender may start a unique-`OwnerData` sampling epoch only
when all of the following are true:

* the Service and candidate underlay family is TCP; QUIC optional capacity
  requires strict non-app-limited native carrier evidence and never uses
  ordered product bytes as a capacity probe;
* at least two active direction-relevant logical bulk request flows have exact
  committed TCP Service ownership and present queued or outstanding request
  data; each logical stream counts once regardless of its path attachments,
  while reverse bytes, idle completed uploads, and QUIC-Service flows never count;
* Auto has classified the request stream as Throughput or Background, the local
  source continues to offer ordinary request data, and the stream, path set, and
  session have no active latency-sensitive or realtime pressure;
* the exact request-side Active attachment instance is live, remains the Service
  and ordered-data anchor, and has direction-correct bulk-rate evidence;
* the candidate is an exact attached Validation instance, uses the same
  underlay family as Service, has a successful local path-proof observation
  produced after that attachment time, and does not yet have bulk-rate evidence;
* the per-flow subflow set has no other unproven `startup_owner`, and this exact
  candidate instance is not already retained as a sampled member; and
* no authoritative foreign lower-flight debt, repair-authoritative ACK hole,
  missing- or failed-owner debt, or queued/active repair range lies below the
  next offset. Normal exact Service product-flight residence age is not a
  startup stale-tail signal.

The logical-flow count is a fresh-owner gate, not path occupancy: per-path
attachment reservations MUST NOT satisfy it. Once the exact startup owner is
assigned, the count is not reapplied, so its bounded epoch may drain after a
two-to-one transition. The remaining conditions are revalidated for every
request quantum; a Service instance change, candidate role/incarnation change,
stale proof, demand demotion, or newly active pressure rejects the planned
sample without changing Service.

The selected candidate is stable while it owns the startup slot; per-frame
ETA changes MUST NOT switch the sample among candidates or create a second
startup sampler for the stream. This request product ACK-clock bootstrap is TCP-only.
Repair-role and cross-underlay outputs are never startup-sampling
candidates. The only lower debt the candidate may cross is an ordinary
contiguous unacknowledged suffix uniquely owned by the live Service. The sender
counts that suffix plus the candidate's queued, in-flight, and next-quantum
owner bytes as projected ordering debt, and the result MUST fit the startup
product envelope: the configured path-flight, receiver-reorder, repair-cache,
and stream-window envelopes clamped by the actual next quantum. An unmeasured
path's tiny prior-rate BDP MUST NOT replace this finite startup envelope. Once
the candidate owns the lower frontier, later sample quanta MUST remain on that
exact startup owner until its epoch suspends, seals, or exhausts; another
unmeasured candidate and Service MUST NOT interleave higher unique offsets
behind that bounded train.

Each selected candidate's cumulative unique-owner budget is:

```
startup_sample_budget =
    min(RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2,
        path_flight_envelope,
        receiver_reorder_envelope,
        repair_envelope,
        stream_window_envelope)
```

The current 512 KiB startup product window therefore yields a 256 KiB
unclamped sample budget. Each dispatch is still one whole preemptible product
frame and MUST separately pass carrier emission credit, queue, path-flight,
flow control, repair-cache, and projected reorder checks. A frame MUST NOT be
split merely to consume the remaining startup credit. ACK release does not
replenish `startup_sample_budget`: cumulative admitted unique `OwnerData`, not
current inflight, spends that candidate's budget. The sampling candidate may
temporarily win these bounded dispatches even when an application-limited or underfed rate would
lose the ordinary completion-horizon comparison. That bypass applies only to
the active startup epoch; it does not waive any debt, resource, pressure,
capability, or carrier-family guard.

Service ownership remains unchanged throughout sampling. Control, repair,
latency, and realtime work can preempt every sample frame. Exact budget
exhaustion seals the sample. If at least the useful minimum rate sample has been
admitted and the next whole frame exceeds the remaining credit, the sender MUST
instead seal the sample irrevocably at the actual admitted byte count and route
that next frame through ordinary Service admission. The oversized frame is not
part of the sample, and a smaller later frame MUST NOT reopen or extend it. Once
sealed, the candidate receives no more startup `OwnerData`, the Service resumes
normal ordinary feed, and a sealed evidence record remains only to attribute
ACKs for already-admitted sample ranges. The epoch MUST NOT restart or refill on
an ACK, timer, queue wakeup, smaller later frame, or app-limited metric refresh. Bulk-demand demotion,
authoritative debt owned by another path, or newly active latency/realtime
pressure suspends further sampling without transferring ownership; if those
conditions fully clear, only
the same candidate's remaining non-refilling credit may resume. A detach,
failure, role change, output replacement, or Service change invalidates the
active candidate rather than preserving stale credit. On the response side this
resets planner generation and epoch identity; on the request side it discards
the incompatible `request_subflow_set`.
For TCP request-side sequential sampling, explicit graduation after all
ordering-owner flights for the candidate have released clears the
`startup_owner` slot while retaining that instance's sampled membership. Only a
different never-sampled Validation attachment instance may then receive its own
budget; the graduated instance cannot regain startup credit in the same set.

For response-side sampling, planner generation and epoch identity are distinct tokens: planner-only
invalidation rejects an obsolete path choice without changing the ledger that
owns already reserved credit. Every planned Subflow send carries the planner
generation and lane-pressure generation captured before target, owner, debt,
and pressure snapshots are read. The commit MUST fail if either changed, and
the per-session pressure
generation check is serialized with the Subflow-set mutation so a concurrent
realtime registration cannot enter between validation and admission.
Changing any admission-envelope field (Service, startup-credit cap,
optional-overhead cap, or maximum read-gap budget) MUST atomically create a new
epoch identity and advance planner generation before admitting the replacement
epoch. This makes same-generation competing commits stale and prevents an old
refund from mutating the replacement ledger.
Lane changes, output attach/detach membership, and their per-session tracker
updates are serialized in `lane -> outputs -> tracker` order. No observer may
publish a new binding lane while the tracker still accounts the old lane, and a
detach always debits the lane that is atomically current for that output.
Pressure-generation storage is live session state, not a permanent tombstone.
It is retained only while the session has a response binding, attached path
load, or realtime flow. Binding destruction removes any remaining attached
loads before releasing its session reference; the last reference/load/flow
release removes the generation entry.
Response startup credit represents emitted `OwnerData`, not a planning attempt. If a
shared carrier queue loses its last slot after planning and the nonblocking
enqueue fails, the sender MUST roll back the committed owner and
optional-overhead bytes using the epoch identity returned by reservation.
That refund MUST remain valid across planner-only passive-attachment
invalidation, while a semantic reset or envelope replacement MUST reject a
refund carrying the former epoch identity. A queue-full race MUST NOT consume
the finite startup epoch without transmitting its sample.
The request sender provides the same externally visible rollback by restoring
the previous `request_subflow_set` when carrier enqueue fails. It MUST NOT retain
a candidate reservation or spend per-instance credit for a request frame that
did not enter the carrier queue.

An output replacement also invalidates cached sender metrics and outstanding
old-output flights as evidence. Flight recording and ACK release are serialized
with role, detach, and replacement changes under one output-to-flight lock order. A record
whose planned role no longer matches is retained for product ordering and
recovery but is not evidence; a record whose output incarnation is stale cannot
increase the replacement's flight accounting. ACK release debits bytes and
creates product-rate or delivery samples only on an entry whose path key and
output incarnation both match the flight. The same incarnation check applies
when an earlier ACKed hole later becomes contiguous. Thus a delayed ACK from a
replaced carrier may still advance product-level ACK state, but it cannot debit
or prove the new attachment. Durable evidence on an unchanged live output
remains path-scoped evidence; it is not startup credit and does not refill the
epoch.

A planned Service dispatch also carries the target output incarnation and
command-channel identity. After carrier enqueue, committing the ordered-data
owner MUST revalidate all three under the output lock before storing the path
key. Detach and owner commit are thereby ordered: either the live incarnation
commits first and detach clears it, or detach wins and the stale plan cannot
restore it. Recording a stale flight may preserve product repair/order state,
but it cannot make a later same-key Validation replacement appear Active.
A role change or closed-output replacement that installs a fresh non-Active
incarnation clears any same-key Service owner before that output is exposed;
only an explicitly Active replacement may preserve same-key Service
continuity.

A TCP request candidate queues exactly one `PATH_PROOF_DATA` receipt marker
behind its sealed startup sample on the same reliable carrier stream. The
marker MUST use the stream-ordered carrier queue, not the priority/control
queue, so its validated ACK proves that the peer parsed all earlier sample
bytes on that exact attachment. The receipt rate sample uses the sealed
`OwnerData` byte count divided by the interval from first sample enqueue to
marker-ACK completion; a later scheduler poll MUST NOT extend that interval.
This is TCP ordered carrier-stream receipt evidence. It is not the only causal
boundary: an exact product ACK that completes the sealed, unambiguous,
single-copy, candidate-owned startup `OwnerData` installs the same follow-on
ACK-clock boundary when it arrives first. Either event is bound to the exact
candidate instance. A candidate may also accumulate qualifying startup ACK
evidence before the sample seals, but that does not create a different
calibration owner or clock.
QUIC request candidates never queue this marker or graduate from finite product
samples: optional ownership requires exact fresh post-attachment,
non-app-limited native packet-ACK evidence. An ACK
covering duplicated or ambiguously attributed data and any `RepairData` ACK
MUST NOT spend the unambiguous TCP evidence floor, but the TCP ordered receipt
marker prevents such ambiguity from permanently stranding an exhausted epoch.

TCP request graduation additionally waits until no exact
ordering-owner flight remains for the candidate. Attachment-liveness proof
payload rate, arbitrary proof/control traffic, peer metrics, and unrelated
connection-wide carrier ACKs MUST NOT graduate it. A graduated TCP request
candidate then receives a bounded chance to replace its conservative startup
rate with an ACK-clocked rate before ordinary ETA ranking can starve it. At most
one explicit exact live graduated Validation instance on the Service underlay
family may own calibration `OwnerData` at a time. Claiming it atomically freezes
that exact path instance and its cumulative, non-refilling calibration target;
the default resource-clamped target is 2 MiB. It MUST NOT grow with a later BDP
or Service-rate estimate and remains capped by path-flight, repair, reorder,
stream-window, and frame-reachability resources. Calibration `OwnerData` MUST
be sent after either valid startup causal boundary. Before its first byte, a
fresh calibration requires at least two active logical bulk request flows with
exact committed TCP Service ownership and present request work. No
path-wide completion estimate may veto it until request-direction,
provenance-bound authority exists; such estimates MUST NOT be negative authority
for the calibration needed to establish that evidence. Exact-owner, debt,
resource, pressure, and cumulative 2 MiB limit guards remain mandatory. After
its first spend, the exact calibration owner may finish after a two-to-one
transition and neither
fresh-start gate is reapplied.
Sustained bulk demand, fresh proof, stable exact Active Service, usable carrier
bulk evidence, absence of latency/realtime pressure, repair debt, foreign lower
owners, and mature stale tails are revalidated for every whole frame. Failed
carrier enqueue rolls back the exact owner, target, and spend reservation. ACK
of the frozen target through a strictly causal candidate-local interval ends
the bounded proof and updates the candidate's exact per-flow model. To avoid
requiring this finite proof to fill a high-BDP kernel pipe from slow start, the
scheduler MAY retain a higher Service-derived provisional rate and pipe only
for an endpoint-only candidate. A configured candidate retains its own hint.
That prior is not candidate evidence and MUST yield after ten exact continuous
candidate samples; it MUST NOT be exported as QUIC carrier capacity.
Exhaustion, ACK release,
timer wakeup, and smaller later frames never refill its credit; an exhausted
candidate blocks the next calibration candidate until exact proof completes or
its exact path-instance lifecycle ends. Flight drain alone MUST NOT create a
new owner. Subsequent Subflow owner
bytes use ordinary measured ECF/BLEST admission with the retained valid rate
even if the latest carrier poll is application-limited. If graduation does not
occur by the startup seal, the candidate remains Probe or Standby for new sends
while later qualifying evidence for already-admitted sample ranges may still
graduate it. It receives no additional startup credit.

A graduated TCP response candidate uses a separate staged exact-instance
ACK-clock calibration. At most one exact live graduated response Validation
instance may own calibration `OwnerData` at a time, and the response Service
owner MUST remain unchanged. One active sustained bulk response is sufficient
fresh demand; requiring an unrelated second response makes optional capacity
unreachable for a large one-flow transfer. The initial authorized cumulative credit is the
minimum of the resource-clamped Service horizon and two candidate BDPs, with a
one-send-quantum floor. This keeps an underestimated candidate measurable
without turning the full product horizon into its first HOL obligation. Before
the first byte, an endpoint-only TCP candidate may use the exact Service rate
as a provisional rate and pipe solely for the completion-opportunity
projection. This prior is neither candidate proof nor TCP congestion authority;
a configured candidate keeps its own capacity hint, and actual candidate ACK
evidence owns every later calibration decision. A fresh zero-spend stage may
start only while a generation-stable active response count exists. That count
is a start gate, not a lifecycle cancellation: exact active or partly spent
work may finish after demand-count churn. An unstarted identity then becomes
dormant without refill or identity change; it blocks only itself from generic
`OwnerData`, not Service or other measured reservoir work. Spent
bytes never decrease or refill. Once cumulative spend reaches the current authorized ceiling, that
ceiling may double only after a strictly causal later ACK-to-ACK sample whose
latest sampled send precedes the prior ACK and whose earliest sampled send is
not earlier than the current stage's authorization time. Bytes sent after the
prior ACK make the interval application-limited; bytes retained from before
stage authorization make it the wrong stage. Neither case authorizes growth.
Each doubling is capped by the configured path-flight, repair,
receiver-reorder, and stream-window resource ceiling; peer hints and
wrong-direction metrics MUST NOT raise it. Failed enqueue rolls back only the
unemitted reservation. Commit revalidates planner generation, the combined
session lane/response-flow generation, the response path/ordering model
generation, exact Service and target key/incarnation, captured Service and
target command-pending byte values, and the exact calibration ceiling. Pending
byte equality is a pressure-value fingerprint, not a queue event generation.

An exact active TCP response calibration may have positive credit smaller than
the normal response source chunk. The sender handles that residual with
two-pass planning. The first pass plans only the exact remaining calibration
bytes. It may emit that smaller product frame only when the returned dispatch
plan contains the ACK-clock calibration commit for that same exact path and
output incarnation. If the first pass selects Service, selects another target,
or cannot produce that exact commit, its result is discarded and the second
pass replans the normal chunk; Service MUST NOT inherit the calibration-sized
fragment. The residual is not rounded up to a minimum quantum. An exhausted,
proven, retired, stale, or UDP/QUIC calibration produces no TCP residual pass.
UDP/QUIC response work retains normal product-frame sizing and its carrier-local
controller.

Stage-credit growth and response-rate publication are separate decisions. Let
`B` be cumulative spend at measurement-stage authorization, `L` the cumulative
credit ceiling, `A = L - B` fresh authorized capacity, `W` fresh current-stage
bytes consumed by first, noncausal, or mixed windows, and `E` strict causal
fresh bytes. A mixed window charges only its exact fresh suffix to `W`. Thus
`A - W`, not cumulative `L` or raw `A`, is the maximum strict evidence still
reachable in this stage.

Every strict ACK-to-ACK window whose sends all follow current authorization and
precede the prior ACK accumulates bytes and raw elapsed time into `E`. Its
aggregate is eligible for the exact instance's bounded stage-rate buffer only
when `E` reaches `F = min(service_horizon, max(MIN_RATE_SAMPLE_BYTES,
ceil(service_horizon / 2)))`, 1 MiB under the default horizon. `F` is
independent from the candidate-BDP seed. Product ACK delivery may justify
bounded credit turnover, but only strict causality may justify rate publication.

When a stage is fully spent and `A - W < F`, the sender MUST top up the same
measurement stage to `min(resource_ceiling, max(2 * L, B + W + F))`. This
reachability growth preserves `B`, authorization time, `W`, and `E`; resetting
them would repeat the clock-establishment loss. When `A - W >= F` but `E < F`,
the sender waits for later strict windows. When exact OwnerData flights drain,
all authorized current-stage bytes not already in `E` become `W`; the state
then either restores reachable credit or terminates at the hard envelope. It
MUST NOT wait with an exhausted, flightless, unreachable stage. Prior-stage,
retired, or stale-incarnation evidence MUST NOT carry into the next accepted
aggregate. Before three qualifying stage aggregates exist,
the candidate retains its provisional startup/receipt rate. At three aggregates,
their median MUST overwrite product-progress and delivery rate as exclusive
path-capacity evidence, including when the median is lower than the prior value,
and the calibration MUST become proven without authorizing another exclusive
doubling stage. It MUST NOT enter the ordinary per-flow TCP ACK-clock field. A
max filter or upward-only EWMA is forbidden here:
ACK-compressed sub-millisecond bursts can be orders of magnitude above sustained
path capacity. Ordinary strict ACK windows contribute only to their current
stage aggregate and never publish independently. ACK release never restores
spent credit; credit can grow only under the bounded reachability or accepted
stage transitions above. A calibration that reaches the hard resource ceiling with fewer than
three representative aggregates publishes no fabricated rate. In either
terminal case, the serial slot advances only after that candidate's exact
ordering-owner flights drain. The terminal ACK itself remains calibration
evidence. At drain the sender MUST reset the ordinary TCP product ACK clock and
MUST NOT count calibration traffic in the replacement epoch. A robust median
becomes a typed path-capacity prior. Only completed ordinary exact OwnerData ACK
windows count toward replacement; fragmented callbacks below one window do not.
After ten such windows and a usable continuous goodput sample spanning at least
the ordinary ACK-clock time floor, the sender atomically publishes that sample
as per-flow product-progress, delivery, and TCP ACK-clock evidence and removes
the prior. General delivery confidence, a calibration tombstone, or a completed
window without a usable goodput sample MUST NOT retire or reinstall the prior.
A terminal state with no robust calibrated rate installs no prior but still
resets the ordinary ACK clock, so later unambiguous TCP OwnerData may establish
fresh rate evidence. Lab diagnostic selection and sample events MUST
include a binding-local `binding_instance_id` in addition to session, path, and
incarnation so concurrent response streams cannot be conflated. UDP/QUIC
response candidates never enter this product-ACK calibration; their bulk
delivery-rate evidence remains owned by the local QUIC carrier ACK controller.

Active or partly spent calibration serialization is binding-wide because one
stage needs isolated product ACK coverage and owns the response's ordered tail.
The same-family reservoir remains closed during TCP calibration until that
identity's exact flights drain, while Service may continue under the unchanged
product gates.
A fresh unspent identity serializes only while the active-response-demand gate is open.
If that gate closes, the state is dormant and its exact target remains excluded
from ordinary `OwnerData`, but the rest of the binding reservoir remains open.
Other response bindings retain independent product ledgers; session-scoped
physical-path coordination may prevent duplicate calibration but MUST NOT merge
those ledgers.

The accumulator MUST apply timer granularity once to the completed stage
aggregate. Repeated per-window clamping makes the result depend on ACK callback
partitioning. The global ordered tail already includes the candidate's product
flight, and product flight already includes frames pending in the carrier
command pipe. Planner and authoritative commit headroom MUST combine these as
overlapping views rather than adding them. Exact
`spent_bytes + payload` remains the calibration-credit authority. A calibration
identity that has become proven MUST remain excluded from generic owner
selection while it is still the exact active calibration; its existing flights
drain before the fence clears or another candidate advances. `RepairData` does
not spend or preserve this unique-owner fence and does not prevent zero-spend
retirement. It remains real carrier pressure: aggregate `OwnerData + RepairData`
and command-pending bytes MUST still fit Admit headroom.

The production SafeBestPath guard separates live in-flight tail state from
authoritative repair debt. A contiguous unacknowledged `OwnerData` suffix below
the sender's highest owner offset is normal carrier recovery state while the
Service owner is live. It is a tail guard for alternate-owner admission: it MAY
block non-Service `OwnerData` and missing-owner failover from assigning later
offsets, except that the selected same-underlay startup-sampling candidate may
cross it within its cumulative epoch and projected reorder budgets. The tail
MUST NOT make the live Service owner inadmissible and MUST NOT by itself create
duplicate repair. Authoritative debt is narrower: an explicit
ACK-range hole tracked by the product flight ledger, a failed or detached owner
tail, a persistent live-owner tail after the product tail timer, or a known
final tail with persistent stall evidence. That authoritative debt MUST be
passed into Service/Subflow admission so an alternate cannot own later bytes
behind an unresolved lower owner. Only those debt states may create
`RepairData`.

Normal unacknowledged `OwnerData` retained in the repair cache is carrier
recovery state. Treating every retained repair-cache byte as immediate repair
creates duplicate storms; treating a live contiguous suffix as generic
Service-stopping debt starves the Service path and makes flapping links appear
as sender starvation even when no receive hole exists. The sender MUST keep
feeding the current Service owner when the normal product envelope permits,
wait for ACK or carrier failure evidence, or convert the blocked range into
bounded `RepairData` only after the explicit gap/failure/tail conditions above.
A remembered Service owner that is absent is a wait/repair/failover state; it
is not permission to elect another Service path for later bytes until lower
ownership is resolved or converted into `RepairData`. Repair overlap avoidance
may inspect the full product-flight ledger separately, but that ledger is not
itself repair debt.

Proof-only and unmeasured candidates remain `Probe`, `Standby`, or `RepairOnly`
until authoritative ordered-owner debt clears or explicit loss/failure/final-tail
evidence converts the affected range into `RepairData`. A response-side
sender-evidenced Validation candidate is still unmeasured, but may use the
bounded startup epoch with at least one active direction-relevant reliable
response flow. A TCP request-side freshly proven Validation instance is likewise
unmeasured, but may use its bounded per-candidate epoch only under sustained
bulk demand with the exact stable bulk-rate-proven Service. QUIC request
Validation remains proof-only until exact fresh post-attachment non-app-limited
native packet-ACK evidence permits ordinary measured ownership. Either eligible
startup direction may
cross only the live Service's non-authoritative contiguous suffix and only in
bulk-only, no-latency-pressure state.
A mixed-family path is owner-eligible under a tail guard only when it is
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
ownership equally, while still allowing measured carriers to contribute as
Subflows and allowing a separate frontier-safe policy to move Service when
migration or failover is justified. Direction-correct bulk evidence may be an
input to that policy, but it MUST NOT trigger Service migration by itself. A
tiny startup/probe-sized sample can keep the path eligible for Probe/Subflow
discovery, but it MUST NOT
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
credit. One underfed non-application-limited QUIC data sample below the existing
confidence floor MUST NOT initialize the model below the startup/cwnd/pacing
fallback; otherwise one validation quantum can permanently classify a useful
path as slow. Once cumulative non-application-limited carrier samples reach the
initial-window confidence floor and acknowledged DATA volume satisfies the
delivery-evidence floor, however, the current measured rate MUST replace the
unmeasured fallback even when it is lower. An optimistic pacing or cwnd prior is
not durable delivery evidence. Max-rate retention and smoothing apply only
after a measured rate has crossed that confidence boundary.
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
service_path = stream.live_service_owner
if stream has authoritative lower-frontier debt:
    comparison_lead = live service_path, even if temporarily backpressured
else if service_path is eligible and admissible for ordinary bulk:
    comparison_lead = service_path
else:
    comparison_lead = min_eta_candidate_that_is_eligible_and_admissible_for_ordinary_bulk()
if startup_sample_epoch_selects(stream, path):
    assert stream/session/path are bulk-only
    if stream.sender_direction == response:
        assert session.active_direction_relevant_reliable_response_flows >= 1
        assert session.session_load_generation is unchanged at commit
    else:
        assert stream.sender_direction == request
        assert stream.lane in {Throughput, Background}
        assert stream.local_source_has_continuing_data
        assert service_path is the exact live Active attachment instance
        assert service_path.has_bulk_rate_evidence
        assert path.proof_observation_at >= path.attachment_time
    assert no latency-sensitive or realtime pressure
    assert path.role == Validation
    assert path.underlay_family == service_path.underlay_family
    assert path.has_local_sender_evidence and not path.has_bulk_rate_evidence
    assert no authoritative_lower_flight_or_repair_debt(stream)
    assert lower_debt_is_only_live_service_contiguous_suffix(stream)
    epoch.candidate.cumulative_owner_bytes + chunk.len <= epoch.candidate.sample_budget
    live_service_suffix_debt(stream)
        + candidate_product_debt(path) + chunk.len
        <= same_underlay_reorder_budget(path, chunk)
    # Do not compare an underfed startup rate with the measured anchor ETA.
else if path is service_path and stream_ordering_debt(path, chunk) == 0:
    assigned_service_debt(path) + stream_ordering_debt(path, chunk) + chunk
        <= service_owner_envelope(path, chunk)
else if path is service_path:
    if latency_sensitive_work_is_active_on_this_path:
        stream_ordering_debt(path, chunk) + chunk
            <= min(same_underlay_reorder_budget(path, chunk),
                   throughput_service_horizon(chunk.len))
    else:
        stream_ordering_debt(path, chunk) + chunk
            <= same_underlay_reorder_budget(path, chunk)
else if path uses the same underlay family as service_path:
    product_reorder_debt(path) + stream_ordering_debt(path, chunk) + chunk
        <= same_underlay_reorder_budget(path, chunk)
else:
    carrier_queue_debt(path) + chunk <= carrier_validation_queue_limit(path, chunk)
    product_reorder_debt(path) + stream_ordering_debt(path, chunk) + chunk
        <= effective_reorder_budget(path)
if path is an additional data path and not startup_sample_epoch_selects(stream, path):
    eta_p(chunk) <= completion_horizon(comparison_lead, path, chunk)
```

The additional-data completion rule is a measured-subflow gate, not a startup
validation gate. Sharing a carrier family, such as QUIC+QUIC or TCP+TCP, is not
proof that later offsets will arrive before the lead can send the next quantum;
therefore a bulk-rate-proven same-underlay path that wants ordinary unique
`OwnerData` MUST show positive incremental completion gain before joining the
ordinary bulk subflow set. A same-underlay path that has only proof,
low-confidence sender samples, or app-limited evidence is not rejected by this
measured completion-gain rule; it remains governed by probe admission or, when
all eligibility guards hold, the cumulative same-underlay startup Subflow
epoch. The response branch additionally requires at least one active
direction-relevant reliable response flow and applies the first-bootstrap/later-
candidate completion rule in Section 18.1; a fresh request branch requires at
least two active logical bulk request flows with exact committed TCP Service
ownership and present request work, plus its sustained-demand, stable-Service,
and fresh-instance predicate. An epoch may
span repeated TCP or QUIC owner quanta, but only for its one stable candidate,
and it ends at that candidate's 256 KiB resource-clamped cap. Reorder budget
is a safety envelope for already-admitted work; it MUST NOT be used as extra
time slack to put unique ordered bytes onto a high-latency path that loses the
ECF/BLEST next-quantum comparison.

At a clear frontier, the live Service path is the safe dispatch baseline, not
merely the lowest raw ETA. A Service whose carrier or product debt violates the
active data-path admission gate is not feedable for that quantum; an admitted
Subflow may then carry overflow work without becoming Service. This is the
sender-service equivalent of ECF/BLEST refusing to pin work behind an
unavailable subflow while also avoiding frame-by-frame path changes whenever a
feedable Service exists.

Authoritative lower-flight or ordered-owner tail debt requires a separate
comparison anchor. While that debt remains, the current live Service snapshot
MAY remain the no-worse baseline even when its output is temporarily
backpressured. This does not admit a Service send; it prevents the lower owner
from comparing against itself and borrowing the Service envelope. A distinct
lower owner must still pass its ordinary Subflow gates against that baseline. If
it fails, the sender waits or repairs instead of electing another Service.

Implementations MUST compute clear-frontier Service leads from candidates that
pass active ordinary-data admission against their current product and carrier
debt. Under authoritative debt, they MUST derive the persistent Service
comparison anchor before filtering candidates by current enqueue capacity. A
raw lowest-ETA path or round-robin cursor is not a valid lead. If the oldest
lower outstanding range has a path owner, that owner remains responsible for
the lower frontier until it becomes admissible, is repaired, or ACK progress
removes the lower-frontier debt.

For each ordered reliable stream, lead choice is flow-level state. When a
lower-frontier owner exists, that owner is the only path eligible to continue
ordinary unique data until it becomes admissible, is repaired, or ACK progress
removes the ordering debt. It retains its existing Service or Subflow role while
doing so. At a clear frontier, an admitted startup-sampling candidate may spend
its bounded epoch first only while its directional predicate remains true: the
captured response-flow count and generation for response, or at least two
active logical bulk request flows with exact committed TCP Service ownership
and present request work, together with sustained request demand and the exact
stable Service and freshly proven Validation instances for a fresh request
epoch. An already-begun exact request owner may finish after that count
falls from two to one. Otherwise a feedable Service receives the next ordinary
quantum. A measured same-family Subflow may precede it only inside the bounded
bulk-backlog reservoir when its completion beats Service draining the lower
tail; this is concurrent overflow, not a Service move. Other measured Subflows
receive clear-frontier overflow only when Service is absent from the admitted
set because of capacity, detach, failure, or another explicit admission guard.
This Service-first rule outside the backlog reservoir is not permanent pinning:
explicit frontier-safe migration or failover changes Service, and unavailable
Service capacity still permits admitted overflow. It prevents 64 KiB-scale path
ping-pong from repeatedly turning heterogeneous RTT into product receive holes,
while independent flows remain spread across Service paths by the flow-level
placement policy.
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
The TCP request-side startup exception may select one freshly proven
same-underlay Validation attachment instance before ordinary measured
admission only when at least two active logical bulk request flows have exact
committed TCP Service ownership and present request work, but
records that work as bounded Subflow `OwnerData`; it does not change the Active
attachment or Service owner. Once selected, that exact owner may drain its
non-refilling epoch after a two-to-one flow transition.

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
byte ownership. `assigned_service_debt` is the overlap-safe maximum of the lead
path's unreleased offset-bearing product flight and carrier work already
assigned to that path; the two views of the same bytes MUST NOT be summed.
Raw sender-service staging is shared, unassigned backlog: it may affect ETA and
memory pressure, but it is not path debt. A single-family response couples
source staging to the exact global assigned owner tail. After exact Service feed
evidence, that combined debt may fill one configured product envelope; path
admission and native carrier pressure remain harder gates. A response with
live owner-capable TCP and UDP outputs may use an independent bounded raw
reservoir because carrier family is not selected until dispatch. This
mixed-family case does not expand raw staging to the full sender/repair envelope.

The active service output with a clear ordered frontier MUST NOT be gated by a
second product-layer copy of the carrier congestion window. QUIC already owns
packet pacing, congestion response, stream flow control, and sender backpressure
below the active owner; TCP already owns kernel write pressure, congestion
response, and packet pacing below its writer. The product scheduler gates this
case with assigned product flight, carrier queue, and the stream-ordering
envelope so the service owner remains fed without creating unbounded response
backlog. This applies even when other validation or subflow set outputs are
attached: passive optional outputs do not reduce the Service carrier's emission
or source-feed credit. Same-family owner participation also does not reduce
coupled source-feed credit because every assigned range remains charged to the
exact global owner tail.
Additional paths, validation paths, and cross-underlay candidates still use
carrier debt as an admission gate because they can create new reordering debt or
probe traffic outside the active service owner. An implementation MUST NOT use
slow product-ACK release timing as a
carrier congestion window, MUST NOT use carrier ACK progress as proof that a
stream byte is no longer needed for repair, and MUST NOT treat the configured
product envelope as a floor above carrier credit for optional paths.
The product source-read horizon MUST NOT be capped by the carrier congestion
window, inflight-high, or send-window equivalent. Those values belong to the
carrier emission gate and to multipath admission, where they describe whether a
specific carrier path can accept another admitted quantum. They are not a
second product-layer receive window. On a reliable carrier, applying a smaller
fixed reservoir at the product source-read layer makes the byte-producing
side application-limited before QUIC or TCP can exercise its own pacing and
congestion control. After exact Service feed evidence, a switchable response
source boundary may therefore use the configured product envelope across its
coupled owner tail and raw queue; stream flow control, repair resources, path
admission, and the carrier writer remain harder gates. For the current QUIC
Service, substantial uniquely owned product `STREAM_ACK` progress may satisfy
that feed predicate independently of a durable local carrier ACK estimate.
Product progress remains a backlog and end-to-end delivery signal, not
UDP/QUIC packet delivery, congestion, or optional-path capacity evidence; a low
sample MUST NOT downshift source read-ahead below credible carrier evidence.

The sender-service admission model also applies path-local lane pressure. When a
Service path has active latency-sensitive or realtime flows, an active bulk lead
MUST NOT use the large throughput envelope to accumulate hidden command backlog
behind that path's queue. Its product admission envelope is reduced to the
bounded latency-pressure Service window, while carrier pacing and stream flow
control continue to govern final emission. Latency work on a different dedicated
path retains its own priority and flow-control protection without shrinking an
unrelated bulk Service path.

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
ordinary owner assignment and dispatch for that stream and continues servicing
carrier ACKs, product ACKs, control frames, flow-control updates, explicit gap
repair, path proof, and path events. Bounded unassigned source staging may
continue. Ordinary data resumes when ACK progress, repair delivery,
detach/failover, or updated path evidence produces a serviceable lower-frontier
owner or advances the contiguous frontier. This
rule closes the MPTCP-style failure mode where a slow or failed subflow owns
early data and all later high-rate data either blocks behind it or deepens the
receive hole.

Owning the oldest unresolved byte does not grant an unbounded path envelope.
For a bulk lower-frontier owner without local latency pressure, the additional
ordinary unique-data budget is adaptive: at least the next whole frame, normally
approximately `2 * path_BDP` from the live delivery/pacing rate and SRTT, and
always capped by the configured receiver-reorder envelope and the normal path,
repair, queue, and stream-window resources. This budget keeps an ACK-clocked
TCP or QUIC UDP owner fed across a high-BDP path instead of collapsing it to a
fixed service horizon whenever any lower debt exists. If that exact path has
latency-sensitive pressure, the lower-frontier budget remains the smaller
preemptible bulk service horizon, capped by the same reorder envelope. Once the
applicable budget is exhausted, the sender pauses ordinary unique bytes until
ACK progress, repair, detach/failover, or updated path evidence changes the
frontier. TCP and QUIC share this product-ordering rule while retaining their
separate carrier congestion, pacing, and ACK clocks.

Lead-path admission and lead-path repair are intentionally separate decisions.
The lead path may keep a larger assigned product flight and carrier queue than
additional paths so that a QUIC carrier stream or TCP writer is not starved by
slow product ACKs. A sender
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
ACK-gap event or tail-repair event. After that floor is spent, newly earned
repair budget accumulates until it can fund one useful repair attempt; sub-MSS
or crumb-sized repairs are not emitted merely because a small fractional budget
was earned. The value is a continuous hint for how aggressively the sender may
trade duplicate traffic for recovery speed; it is not a fixed rate, not a
per-event multiplier, not a product-data throttle, and not permission to send
speculative unique bytes that deepen ordered receive debt. Correctness repair
may exceed the optional hint only for an authoritative ACK gap, failed-owner
gap/tail, persistent live-owner tail on an alternate output, or known final
tail, and only as bounded `RepairData`. Each such event is capped to one
service repair quantum, outstanding repair debt, and configured resource caps;
later repair requires later ACK progress, capacity, detach/failover, or repair
deadline evidence.

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
sender MUST NOT enqueue another `RepairData` copy for a byte range that is
already pending in the sender-service queue or in recent live carrier flight;
that copy is treated as the current repair attempt and the repair timer backs
off until ACK, capacity, detach/failover, the repair retry timeout, or the next
materially different repair range changes the decision. The retry timeout is
derived from the same path/lane stall model used to decide tail repair, so it is
a bounded retransmission clock rather than a new packet semantic. Repair already
in carrier flight on a detached or failed output does not block failover repair
on a live survivor; stale unacknowledged repair in carrier flight on a live
output does not block correctness repair forever. Fresh repair in flight on a
live output still blocks duplicate spending for the same range.
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

For the lead path, `service_owner_envelope` is the preemptible product repair
and flow-control envelope, not the UDP carrier cwnd. Before exact Service feed
evidence exists, it follows the carrier-specific bounded bootstrap: switchable
source staging uses the feed reservoir, QUIC emission uses that reservoir, and
TCP emission retains the narrower horizon. After feed evidence exists, it is the
configured product envelope. Same-path latency pressure narrows it back to the
feed reservoir. The configured product/repair
envelope remains a hard resource ceiling, not a carrier congestion-window
claim. This envelope applies only while the lead path owns the lower
outstanding stream frontier, including the first quantum of a newly elected
Service failover. If the lead candidate would send after lower offsets already
owned by another path, it is no longer simply feeding its own contiguous
frontier; it must fit within the same-underlay reorder budget before ordinary
bulk can continue there. This prevents the lead role from becoming a loophole
that admits tens of MiB of ordered receive hole. The configured path inflight
value is the resource ceiling for assigned product work; it is not a
congestion-control claim and it does not permit non-preemptible giant frames.
Additional cross-underlay paths use the stricter reorder budget because they
can create head-of-line debt behind data already committed on another path.

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

The Service owner and optional striping paths have different risk. The current
ETA model selects a comparison anchor for no-worse and completion-horizon tests;
that anchor is not by itself a dispatch role and does not migrate Service. After
any bounded startup-sampling spend, a feedable Service receives the next ordinary
bulk quantum unless a measured same-family path passes the bounded bulk-backlog
completion reservoir. Service is gated by assigned product flight and carrier
backpressure, but it is not rejected merely because an optional path has a lower
raw ETA. A
Service that continues its own contiguous frontier does not consume cross-path
reorder budget. Additional paths, including Validation candidates and measured
Subflows, are admitted against their BDP/reorder budget and the completion-
horizon gate. They MUST NOT borrow the full Service product envelope. This
distinction preserves single-path throughput while preventing a speculative or
heterogeneous extra path from creating tens of MiB of ordered-stream head-of-
line debt.

An attached Active path is a lifecycle concept unless it is the current Service
or authoritative lower-frontier owner; attachment alone grants no ordinary bulk
privilege. A lower-ETA measured Subflow may carry admitted overflow when Service
cannot accept the next quantum, but that send does not migrate Service. Service
moves only through explicit frontier-safe migration, reattachment, or failover.
This avoids both stale Active stickiness and frame-by-frame alternation between
a fast path and a high-RTT or low-rate path.

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
product work admitted to the QUIC stream. In both cases, a Service without feed
evidence follows its carrier-specific bounded bootstrap; a clear-frontier
Service with feed evidence may use the configured product envelope, narrowed to
its feed reservoir by same-path latency pressure. Optional same-underlay
admission is instead derived from live BDP,
path inflight evidence when it is smaller, the next quantum size, and the
configured resource ceiling. The configured envelope is permission for bounded
ready Service data, not a queue-fill target and not a carrier congestion-window
claim. Actual network emission remains gated by the QUIC sender or kernel TCP.
This matches QUIC and BBR practice: the stream scheduler may have ready data,
while the packet sender paces and gates network flight.
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

When no active, live failover, evidenced, or validation candidate passes admission, the sender
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
placements carry repair or proof traffic only unless ordinary measured
ECF/BLEST admission or the explicit bounded startup epoch has selected them for
the current bulk quantum.

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
service_path = stream.live_service_owner or explicit_frontier_safe_failover_service()
comparison_anchor = persistent_service_anchor_under_authoritative_debt()
                    or min_eta_candidate_eligible_for_no_worse_comparison()
startup_sampling = startup_sample_epoch_selects(stream, path, chunk)
                   and ((stream.sender_direction == response
                         and session.active_direction_relevant_reliable_response_flows >= 1)
                        or (stream.sender_direction == request
                            and stream.lane in {Throughput, Background}
                            and service_path is exact live Active attachment instance
                            and service_path.has_bulk_rate_evidence
                            and path is exact same-underlay Validation attachment instance
                            and path.proof_observation_at >= path.attachment_time))
if path is the service_path and stream_ordering_debt(path, chunk) == 0:
    # The current Service owner is the primary ordered-byte owner.  App-limited
    # or low-rate ACK feedback is visibility, not a product-flight ceiling.
    # TCP/QUIC carrier congestion still drains below this envelope.
    if not path.has_bulk_rate_evidence:
        product_inflight_limit = service_horizon(chunk,
                                                 configured_path_inflight,
                                                 configured_stream_window,
                                                 configured_receiver_reorder)
    else:
        feed_reservoir = min(
            bounded_bbr_headroom_window(service_horizon(chunk,
                                                        configured_path_inflight,
                                                        configured_stream_window,
                                                        configured_receiver_reorder)),
            configured_path_inflight,
            configured_stream_window,
            configured_receiver_reorder)
        if latency_sensitive_work_is_active_on_this_path:
            product_inflight_limit = feed_reservoir
        else:
            product_inflight_limit = min(configured_path_inflight,
                                         configured_stream_window,
                                         configured_receiver_reorder)
    product_inflight_limit = max(product_inflight_limit, chunk.len)
else if path uses the same underlay family as the service_path:
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
ordering_debt = classify_stream_ordering_debt(stream, path, chunk)
if path != service_path and ordering_debt.bytes > 0:
    if ordering_debt is authoritative_lower_flight_or_repair_debt:
        suppress additional OwnerData for this bulk quantum
    else if startup_sampling:
        assert ordering_debt is live_service_contiguous_suffix
        assert no latency_sensitive_or_realtime_pressure
        assert path.underlay_family == service_path.underlay_family
        assert epoch.candidate.cumulative_owner_bytes + chunk.len <= epoch.candidate.sample_budget
        ordering_debt.bytes + candidate_product_debt(path) + chunk.len
            <= base_reorder_budget
    else if not (path has bulk_rate_evidence
                 and path.underlay_family == service_path.underlay_family):
        suppress additional OwnerData for this bulk quantum
if path is the service_path and stream_ordering_debt(path, chunk) == 0:
    admission_reorder_budget = product_inflight_limit
else if path is the service_path:
    admission_reorder_budget = min(base_reorder_budget,
                                   throughput_service_horizon(chunk.len))
                               if latency_sensitive_work_is_active_on_this_path
                               else base_reorder_budget
else if path uses the same underlay family as the service_path:
    admission_reorder_budget = base_reorder_budget
else:
    admission_reorder_budget = effective_reorder_budget

best_rate = max(comparison_anchor.pacing_rate, comparison_anchor.delivery_rate)
best_chunk_tx = chunk.len / best_rate
candidate_debt = path.queue_bytes + path.bytes_in_flight + chunk.len
candidate_debt += stream_ordering_debt(path, chunk)
reorder_absorption = max(0, effective_reorder_budget - candidate_debt)
                     / best_rate
completion_horizon = eta_comparison_anchor + best_chunk_tx + reorder_absorption
if path is an optional data path and not startup_sampling
   and eta_path > completion_horizon:
    suppress additional OwnerData for this bulk quantum
if path is attached Active but is neither Service nor the lower-frontier owner
   and not startup_sampling
   and stream_ordering_debt(path, chunk) > 0
   and eta_path > eta_comparison_anchor
   and eta_path > completion_horizon:
    suppress stale active path for this bulk quantum

selected_path = startup_sampling_candidate if one was admitted
                else feedable service_path if admitted
                else best_admitted_optional_path_by_eta()
successful optional emission does not change service_path
```

Admission gains are internal model-control coefficients, not operator-visible
traffic modes. In production v1 they apply to additional same-family ordinary
striping and explicit Service migration/failover decisions. Cross-underlay TCP+QUIC
`OwnerData` is not admitted as concurrent later-offset striping or implicit
clear-frontier Service reselection. Mature clear-frontier Service product
flight/queues use the configured product envelope, with the startup horizon and
same-path latency-pressure narrowing defined above. Optional same-underlay paths
use BDP/inflight-derived admission capped by the configured resource ceiling,
while carrier controllers still enforce network flight. This follows BBR's separation
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
MUST NOT permanently reject a same-underlay Validation candidate solely because
an app-limited initial-window sample or underfed validation history implies a
tiny rate. Such a sample proves that the sender was not feeding the path enough
to measure capacity; it does not prove that the path is slow. The bounded
startup Subflow epoch therefore bypasses only this measured completion-rate
comparison while it supplies one response candidate, or one TCP request
candidate, with enough unique owner data to produce a useful sample. QUIC
request candidates are excluded from this product sampling path.
Fresh TCP request ACK-clock calibration follows the same evidence rule: no
path-wide completion estimate has veto authority until request-direction,
provenance-bound authority exists. Exact-owner, debt, resource, pressure,
two-flow, and cumulative 2 MiB limit guards remain independent and mandatory.
However, an app-limited sample also does not prove that the path can safely hold
the path's bulk capacity. It therefore MUST NOT initialize a tiny BDP-derived
bulk model or make another path Service. For the current clear-frontier QUIC
Service, either substantial uniquely owned product `STREAM_ACK` progress or a
durable local carrier ACK-derived DATA estimate may become feed evidence. The
carrier estimate may be app-limited. Absent same-path latency pressure, either
may unlock the carrier-neutral product source/emission envelope; native QUIC
still bounds network flight, and neither authority is carrier capacity proof.
Before that evidence, the derived feed reservoir remains the bounded bootstrap.
TCP uses its strict product/carrier
feed evidence. Fresh non-app-limited carrier proof remains required for ordinary
QUIC Subflow capacity claims and migration ranking; neither weak feed authority
can satisfy it. That proof is not required just to keep the current Service path
fed or to spend a response-side or TCP-request bounded startup-sample epoch.
While there is no lower-frontier owner on another path and the Service-owner
frontier is clear, same-underlay admission is governed by explicit product
inflight, live carrier credit, and reorder budgets. Admission makes a measured
Subflow eligible for overflow; at a clear or small frontier it does not displace
a feedable Service for the next ordinary quantum. With sustained bulk backlog, a
measured same-family Subflow may precede Service only through the bounded
completion reservoir below. With no active direction-relevant reliable response flow,
an unmeasured response candidate cannot use unique `OwnerData` to create that
evidence. One sustained bulk response is sufficient for bounded same-family
discovery. In contrast, one logical bulk request flow cannot open a fresh TCP
request startup or zero-spend calibration epoch. An exact request epoch begun
while its two-flow gate held may still drain after the count falls to one.
Once an authoritative lower-flight owner exists, additional same-stream
`OwnerData` by other candidates is suppressed until that lower frontier clears.
Contiguous Service-tail debt without an authoritative lower-flight owner is a
weaker scheduler guard. Cross-underlay, Repair-role, and sender-unevidenced
candidates remain blocked. A bulk-rate-proven same-underlay Subflow may receive
`OwnerData` when the ordinary Subflow/no-worse ledger admits the range with the
tail counted as ordering risk. When active direction-relevant reliable response
demand exists, the one selected sender-evidenced Validation
candidate may instead spend its
startup epoch while the projected tail and candidate debt fit the startup
product envelope. This exception is disabled whenever active response demand
ends or the stream, session, or path has active latency-sensitive or realtime
pressure. The Service owner may
continue if its own product-feed admission
passes; otherwise the sender waits, uses an admissible measured same-underlay
Subflow, spends an eligible bounded startup sample, or emits justified bounded
`RepairData`. The completion horizon remains the positive-contribution gate for
ordinary debt-bearing same-family admission and for explicit cross-underlay
Service migration once the migration policy decides the carrier family may
change.

The measured same-family response reservoir is a narrower conjunctive
exception, not a
larger reorder window. Let `T` be the global unacknowledged unique-product tail,
`S` the Service's unacknowledged unique `OwnerData`, `H` the protected Service
quota, `C` the candidate's unacknowledged unique `OwnerData`, `P` all product
copies assigned to that candidate (`OwnerData + RepairData`), `q` the next
quantum, and `E` the configured product/reorder/stream resource envelope. Global
admission MUST first satisfy `max(T, S) + q <= E`. Only after the exact live Service is generically
admitted, `S >= H`, the authoritative lower frontier is clear, both paths have
no latency pressure, and any TCP calibration identity in the binding is neither
active, partly spent, nor currently eligible to start, may a bulk-rate-proven
same-underlay Subflow retry admission. Completion uses the whole lower backlog
`T`, less Service command queue and native carrier flight already represented in
the Service ETA; receiver reorder exposure is separately `max(T - S - C, 0)`. Generic bulk
admission separately charges `P`, so duplicate
repair copies remain fully charged; when no repairs exist, the candidate-local
total reduces to candidate and other-candidate later bytes rather than `C + T`.
Earlier Service bytes extend the completion deadline but do not themselves
occupy receiver reorder memory. `S` and `C` MUST come from
the exact response flight ledger and MUST NOT be inferred from carrier-wide
queue pressure or aggregate product copies. This is an ownership-aware union:
the Service quota consumes global envelope credit once, and the candidate
consumes its own path BDP/emission allowance plus other-candidate overflow.
Authoritative lower-frontier debt remains fully charged. The
exception MUST NOT apply cross-family or to proof-only, Repair, stale-Service,
any active or partly spent TCP calibration, or a fresh TCP calibration
while its start gate is open. A fresh dormant identity blocks only its exact
target from generic ownership; it MUST NOT close the reservoir for Service and
other measured candidates. The exception MUST NOT move Service ownership.

For TCP request/upload, the one selected freshly proven same-underlay Validation
instance may begin spending its own startup budget under the same projected-tail
and candidate-debt envelope only while at least two active logical bulk request
flows have exact committed TCP Service ownership and present request work. This
request exception is disabled on bulk-demand demotion, an
Active Service key or instance change, loss of Service bulk-rate evidence, stale
or pre-attachment proof, Validation role or instance change, latency/realtime
pressure, or authoritative lower debt. Once assigned, the exact owner may drain
after a two-to-one request-flow transition; the flow-count gate is not reapplied
to that owner. It never depends on the response-flow count and never changes
Service.

For response-side same-underlay startup, a fresh candidate requires a
generation-stable session count of at least one active direction-relevant
reliable response flow. Once a measured same-family Subflow exists, a later
fresh candidate must use its own completion model and finish the whole bounded
sample within the current Service reservoir; the first bootstrap is exempt to
avoid circular proof. Begun exact startup work may finish after generation churn.
Fresh TCP request-side startup requires at least two
active logical bulk request flows with exact committed TCP Service ownership and
present request work. After assignment, every whole request frame
revalidates sustained Throughput/Background demand and the exact stable Service,
Validation instance, and post-attachment proof, but does not repeat the
request-flow count gate. Both use live carrier credit
when available. If the carrier reports an inflight or congestion-window limit, that carrier
limit shapes ETA and emission pacing, but it does not replace the cumulative
sample budget. Each candidate's Startup Subflow owner credit is capped at
`min(256 KiB, path flight, receiver reorder, repair, stream window)` for the
entire epoch, and the projected ordering debt must fit the
same startup product envelope on every dispatch. ACKs release flight but do not
refill the cumulative cap. Product frames are not split to fit the cap. After a
useful TCP request sample exists, a later whole frame larger than the remaining
credit seals the sample permanently at the actual admitted count, stays on
Service, and causes its one ordered receipt marker to follow the sealed sample.
Smaller later frames cannot reopen it. The sender MUST NOT replace this cap with a tiny
product-rate BDP derived from the default path-open score or from one
app-limited sample. This follows MPQUIC's per-path congestion-state model: QUIC
decides packet pacing and packet flight, while mptunnel bounds the amount of
ordered product data it is willing to expose to reordering risk.

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
sender keeps a Service without feed evidence inside its carrier-specific bounded
bootstrap and permits a clear-frontier Service with feed evidence to use the
configured product envelope, narrowed under same-path latency pressure. It
derives optional same-underlay and cross-underlay
product admission from the live BDP model, path inflight evidence when present,
and the next chunk size, then caps that result by the configured path inflight
ceiling. Control, ACKs, repair, and latency frames must still interleave with any
admitted bulk work. The configured ceiling MUST remain an upper bound for
optional or unknown-path admission; it MUST NOT be used as a floor that expands
a smaller ACK-clocked or carrier-derived optional queue. The mature
clear-frontier Service envelope is the deliberate product-readiness exception,
not an expansion of carrier network flight.
Tail guards and authoritative lower-flight debt MUST NOT bypass the active
Service product-flight admission check. A live contiguous Service tail acts as
a family/evidence filter for alternate owners but MUST NOT make the current
Service owner inadmissible. Authoritative lower-flight debt remains an
ordering-debt input for candidates that would expand a receive hole. It blocks
the startup exception as well as ordinary candidates. Proof-only candidates and
debt-expanding cross-family candidates remain Probe, Standby, or RepairOnly
until the guard/debt clears. The surviving measured OwnerData candidates are
ranked by the normal no-worse admission checks: ETA, inflight, ordering debt,
read-gap/reorder budget, queue, and completion horizon. The selected startup
candidate uses its narrower epoch checks instead. A sender MUST NOT pin new
OwnerData to a stale lifecycle Active output or raw lowest-ETA candidate merely
because it carried earlier bytes. An authoritative lower-frontier owner is the
exception: it alone continues that frontier until admission, repair, or ACK
progress resolves the debt. A measured Subflow admitted at a clear frontier still
MUST NOT change the Service owner hint merely because it carried the next range.
If that Subflow later owns an authoritative lower ACK hole, it still uses its
Subflow role and admission envelope while continuing the lower frontier; the
hole does not grant Service role or permission to commit a new Service owner.
The current Service may remain the subflow-set anchor even when it is temporarily
over its local product-feed envelope. That anchor status is not send admission:
it only supplies the measured baseline for evaluating Subflow candidates. An
ordinary Subflow may receive OwnerData only when it already has path-scoped
bulk-rate evidence and its no-worse gates pass. The bounded startup epoch is not
an implicit Service relabel: its candidate stays Validation and Service ownership
does not move. A response startup candidate is absent from unique-data admission
when no direction-relevant reliable response flow is active. A
fresh TCP request startup candidate is absent when fewer than two logical bulk
request flows are active or its sustained-demand, stable-Service,
fresh-instance, pressure, or debt predicate fails. A begun exact owner may
continue after a two-to-one transition. If neither
an ordinary candidate nor
the one startup candidate passes its respective gates, the sender waits; it MUST NOT bypass Service
admission by relabeling another path as Service.
An app-limited sample alone is not positive-contribution proof. Once cumulative
path-scoped bulk-rate evidence has graduated a Subflow, however, a later
app-limited carrier poll MUST NOT erase or lower that retained model. A measured
Subflow still passes the ordinary no-worse gates. A feedable Service MUST be
selected ahead of measured Subflows at a clear or small frontier regardless of
whether its bulk-rate proof is complete. Under sustained bulk backlog, one
measured same-family Subflow may precede it only when the ownership-aware product
reservoir, completion-backlog, inflight, and reorder gates all pass. This
prevents frame-level ping-pong while allowing proven capacity to contribute
concurrently.
Measured Subflows retain their admitted overflow role when Service cannot accept
the next quantum; this is not periodic OwnerData retention or keepalive
sampling.
At exact startup-cap exhaustion or an irrevocable near-cap seal without
graduation, Service feed resumes and the candidate stays Probe or Standby for
new sends while ACK attribution remains live. A sealed TCP request sample also
keeps its one ordered receipt marker live.
For TCP request-side sampling, only explicit graduation after its ordering-owner
flights release frees the startup slot for a different never-sampled instance;
cap exhaustion or sealing alone does not rotate through candidates or refill
credit. After graduation, one explicit exact same-underlay TCP candidate at a
time may own its separate ACK-clock calibration. The owner and cumulative,
non-refilling target are frozen together; the target is 2 MiB with default
resource envelopes and never expands into a pipe-sized transfer. Only exact
candidate bytes sent after its sealed-data ACK or ordered-receipt ACK boundary
may complete the causal proof.
A fresh zero-spend calibration requires at least two active logical bulk request
flows with exact committed TCP Service ownership and present request work. No
path-wide completion estimate may veto it until request-direction,
provenance-bound authority exists; path-wide estimates MUST NOT block the
calibration needed to establish that evidence. Exact-owner, debt, resource,
pressure, and cumulative 2 MiB limit guards remain in force.
Once any calibration byte is spent, the exact owner may finish after a
two-to-one transition and the opportunity gate is not reapplied. An exhausted
calibration candidate blocks the next until exact proof completes or the exact
instance is failed, detached, replaced, or role-changed. A
failed, detached, replaced, or role-changed candidate invalidates the epoch; an
attached Validation instance that still owns ledger ranges remains visible to
live-tail repair even after that policy epoch resets. Exact ownership may grant
only an endpoint-only candidate a provisional Service-derived scheduling rate
and pipe until its own continuous exact product-ACK model reaches ten samples.
A configured candidate retains its own capacity hint.
For response-side sampling, the current startup key likewise remains exclusive
until its OwnerData flight drains and canonical bulk proof exists. TCP may use
unambiguous OwnerData ACK rate; QUIC requires local carrier ACK-derived bulk
metrics. Graduation retains the measured member and Service key, increments the
planner generation, and preserves the Service owner. When the TCP candidate is
endpoint-only, has no local carrier ACK sample, and retains only an app-limited
peer hint, graduation MUST install the current proven same-family Service rate
as a temporary typed path-capacity prior. It MUST reset the candidate's ordinary
ACK epoch, MUST NOT create an exclusive calibration identity, and MUST let the
existing measured-Subflow completion, inflight, and reorder gates bound ordinary
ownership. Ten completed ordinary exact-ACK windows plus a usable continuous
sample replace that prior as per-flow goodput. A configured or independently
measured candidate MUST preserve its own evidence and uses the fallback below.
Before another unproven response candidate may consume a separate startup
sample, one such graduated TCP fallback candidate at a time may use staged
exact-instance ACK-clock calibration. Its
initial cumulative credit is the smaller of the resource-clamped Service
horizon and a two-BDP candidate window, with a one-send-quantum floor; it never
refills. Starting a fresh stage requires at least one active direction-relevant
response flow. Once the stage is active or partly spent, exact authorized work
may finish after response-demand generation churn. A fresh identity instead
becomes dormant and excludes only itself from ordinary ownership until active
demand returns or it is safely retired. After full stage spend, a
strictly causal later ACK window whose earliest send follows stage authorization
may double the authorized cumulative ceiling up to the resource envelope.
Before the first calibration byte, the scheduler MUST project completion of the
whole initial credit. Let `C` be that credit, `Q` the next payload, `F` the
bounded Service feed reservoir, `E` the product resource envelope, and
`R=min(E,C+F)`. Candidate completion is its next-payload ETA plus transmission
time for `C-Q`; the ordering deadline is current Service ETA plus Service
transmission time for `F`. The lower calibration prefix does not itself occupy
receiver reorder memory; only the later Service bytes do. If candidate completion exceeds that
deadline, the binding retires only this unspent calibration identity after
revalidating the planner, combined lane/flow, and path/ordering-model
generations; exact Service and target incarnations; captured command-pending
values; the calibration ceiling; the active-response-demand gate; and absence of exact
OwnerData flight, then resnapshots. Repair-only flight does not preserve this
policy identity, although aggregate repair pressure still gates Admit. This
opportunity gate is not reapplied after any calibration spend because an active
exact stage must finish rather than strand lower offsets.
While that exact calibration prefix remains serialized, new Service assignment
MUST stop when the global ordered tail reaches `R`; ACK progress may reopen the
remaining projected credit. Raw offset-free staging may remain bounded by its
separate source-memory policy, but it MUST NOT assign later product offsets past
the completion reservoir assumed by admission.
Fallback growth does not wait for rate-publication readiness. Strict windows form one
byte/raw-time aggregate per stage; only aggregates covering at least half the
resource-clamped Service horizon, with a path-proof floor, enter the bounded
stage-rate buffer. Startup rate remains before three aggregates, then
the median overwrites the old rate without max-filtering and becomes the same typed
path-capacity prior after exact calibration flight drains. A fresh ordinary
exact-ACK epoch replaces it as per-flow goodput only after ten completed windows
and one usable continuous sample. UDP/QUIC candidates
skip this product-ACK calibration and continue to use local carrier ACK-derived
evidence. The serial slot and ordinary proven ownership advance only after the
TCP calibration candidate's exact flights drain.
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

Confidence scaling does not shrink the Service path's basic product
horizon below `product_inflight_limit`; otherwise a single path would bootstrap
too slowly and bulk throughput would regress. It also does not
shrink same-underlay aggregation below the unscaled BDP reorder budget, because
that turns a healthy pure-UDP or pure-TCP multipath transfer into a permanent
probe. The Service/same-underlay rule MUST NOT be applied to
cross-underlay additional paths, because that would convert a resource ceiling
into mixed-carrier reorder permission and reintroduce the all-path
below-best-single-path failure mode.

Product admission and carrier congestion control are separate gates, but they
must be consistent. Once exact feed evidence exists, the active Service path is
admitted by the carrier-neutral product envelope when the ordered frontier is
contiguous; before that, it uses the carrier-specific bounded bootstrap. The TCP
or QUIC carrier then drains that preemptible stream work only when its own send,
pacing, and congestion gates permit. QUIC carrier inflight or congestion-window state
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
path simply because it is attached. A previously attached Active output has no
ordinary-data privilege unless it is the current Service or authoritative lower-
frontier owner. A measured Subflow may carry admitted overflow without becoming
Service; moving ordinary bulk ownership requires an explicit frontier-safe
Service migration or failover decision. The sender MUST NOT silently convert a
Repair path into an ordinary data path. This rule follows the MPTCP lesson that subflow scheduling
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

Define `open_pto(path)` as the PTO computed from live RTT/rttvar once applicable
live evidence exists. Every initial Active TCP attempt MUST use at least
`max(candidate_derived_pto, conservative_initial_pto)` because its session actor
may need to establish or re-establish the carrier. Define
`open_phases(TCP) = 3` and `open_phases(QUIC_UDP) = 2`. An initial
demand-bearing Active attempt with another schedulable candidate MUST establish
`deadline = attempt_start + open_phases(path) * open_pto(path)`. A sole
candidate adds persistent-congestion backoff after its phase prefix with
multiplier `open_phases(path) - 1 +
sum(2^i, i=0..persistent_congestion_threshold-1)`. An Active reattach,
Repair, or Validation/recovery attempt MUST establish
`deadline = attempt_start + candidate_pto(path)`.

Each is one absolute per-candidate deadline. Command-queue wait and every
carrier or role-required phase, including DNS, address attempts, TCP dial,
encrypted MPP authentication/session setup, `PATH_JOIN`, product-open emission,
and peer acceptance, consume only the remaining time. No nested phase may
restart the budget. The configured idle path-probe timeout MUST neither shorten
nor extend it. On expiry the path actor rejects or detaches the pending open,
the endpoint marks the attempted path as a data-plane failure, releases its
reserved load, and continues with another candidate or the best attached
survivor without blocking past the deadline on queued cleanup.

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

Receive-progress replay is transport-specific above the shared product ACK
format. Once a UDP/QUIC reliable stream has established any receive progress,
its PTO-derived progress timer MAY resend the current bounded
`STREAM_ACK`/`STREAM_MAX_DATA` snapshot while the peer remains open; QUIC packet
recovery does not guarantee that the peer observed this product-ordering state.
A TCP reliable stream MUST enable periodic progress replay only when a live
multipath repair alternative exists, and MUST send it only while receiver
reorder debt remains. Contiguous TCP progress without reorder debt relies on the
ordinary ACK cadence and idle heartbeat/liveness system. This split preserves
one product-level recovery protocol without pretending TCP and QUIC have the
same carrier ACK or congestion behavior.

A complete product ACK whose same first missing offset persists for the
PTO-derived persistent-gap interval may trigger one gap-repair event only when a
distinct live repair output exists. The flight ledger determines the exact
single-copy `OwnerData` owner of the lowest repaired range; the carrier that
delivered the ACK does not. For a bulk range exactly owned by TCP, the sender
selects one output distinct from all owners of that range and may repair one
modeled service flight using the selected output's live rate and RTT, normally
approximately `2 * BDP`. Outstanding gap debt, repair-cache capacity,
configured path-flight resources, queue credit, and the selected
output's capacity cap the event. If the owner is UDP/QUIC, ownership is
ambiguous, or the lane is latency/realtime, the event remains one ordinary
adaptive repair quantum. Repeated repair of the same gap is rate-limited by the
same persistent interval and never creates Service, Subflow, or rate evidence
for the repair output.

Owner backpressure is a sender wait state, but it is not automatically a
product-source starvation signal. The backpressure condition for scheduling is
an explicit queue/flight/resource limit on the current Service owner or
authoritative lower-flight debt that would make a later owner expand a receive
hole; the backpressure condition for repair is the narrower authoritative
repair debt described above. While owner backpressure exists, the sender MUST
NOT dispatch queued bulk bytes as later `OwnerData` if doing so would expand
the unresolved lower frontier. However, reading from the local product source
into the bounded sender queue does not assign product offsets and does not
create ordering debt
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
stalled. If live-owner alternate repair does not advance the ACK frontier it
MUST wait for the persistent repair delay before repeating; if the prior owner
has detached or failed, repeated failover repair uses the PTO-derived
tail-stall timeout instead. A known final offset is not sufficient by itself:
terminal owner-tail repair may spend bounded
critical repair only after tail-stall, carrier failure/detach, or equivalent
final-debt evidence shows the retained tail is no longer making progress. That
repair remains bounded by repair-cache,
path-flight, and sender resource limits; those bytes are still counted as
repair overhead and MUST NOT move Service ownership.
If the previous owner has detached, the sender has an ACK frontier, and the
sender retains unacked product bytes but no longer has a path-flight owner
record for that suffix, persistent tail-stall repair MAY use any live survivor
as unknown-owner correctness repair. Without an ACK frontier, unknown-owner
repair MUST wait; otherwise a sender can duplicate the entire startup tail and
inflate overhead. This rule is intentionally narrower than Service failover: it
sends `RepairData` only, creates no path delivery proof, and does not promote
the survivor to Service or Subflow ownership. For target admission, this
unknown-owner correctness repair is classified with failed-owner/path-failure
repair rather than ordinary ACK-gap repair: it may use a live survivor because
the missing owner record is itself failover evidence, but the resulting
`RepairData` still cannot create delivery samples, bulk-rate evidence, Service,
or Subflow ownership. The same target-admission rule applies: stale owner
emission-credit debt cannot block this bounded correctness repair, but real
survivor queue capacity and repair resource limits still apply.

Persistent live-Service-tail repair does not suppress the live Service owner for
later `OwnerData`, and it does not elect a survivor while the lower suffix is
still unresolved. Failed-owner and unknown-owner tail recovery also remain
`RepairData`-only until ACK progress or owner-state cleanup makes ordinary
Service election safe; otherwise a survivor can own later bytes while the
entire prefix is still unresolved. The queued repair continues to resolve the
lower suffix as `RepairData`; it does not grant delivery samples, bulk-rate
evidence, Subflow admission, or Service ownership to the repair path.

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
* create fresh CSPRNG TCP connection salts, derive connection-scoped directional
  traffic keys, and never reuse an AEAD nonce under one key;
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

Product frames use version 1 in the `MPTF` header. TCP envelopes use version 2
in the `MPTE` header. UDP carrier packets are QUIC packets and are versioned by
QUIC; mptunnel does not define a separate UDP packet version byte.

Receivers MUST reject unsupported versions. `MPTE` version 1 used a static
traffic key with a counter that restarted on each TCP connection, so concurrent
paths or reconnects could repeat AEAD key/nonce pairs. Version 2 is a coordinated
endpoint upgrade: implementations MUST reject version 1 and MUST NOT retry it as
an automatic fallback after a version 2 failure. The project does not preserve
backward compatibility for internal experimental versions. A later version that
changes wire encoding MUST update this RFC and increment the relevant version
number.

## 25. References

### 25.1 Normative References

* RFC 2119, "Key words for use in RFCs to Indicate Requirement Levels",
  https://www.rfc-editor.org/rfc/rfc2119
* RFC 8174, "Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words",
  https://www.rfc-editor.org/rfc/rfc8174
* RFC 5869, "HMAC-based Extract-and-Expand Key Derivation Function (HKDF)",
  https://www.rfc-editor.org/rfc/rfc5869

### 25.2 Informative References

* RFC 7322 / RFC Editor style guidance, used for document structure,
  https://www.rfc-editor.org/rfc/rfc7322
* RFC 8684, "TCP Extensions for Multipath Operation with Multiple Addresses",
  especially data sequence mapping and reinjection concepts,
  https://www.rfc-editor.org/rfc/rfc8684
* RFC 6356, "Coupled Congestion Control for Multipath Transport Protocols",
  especially independent subflow congestion state and aggregate resource
  pooling under sufficient offered load,
  https://www.rfc-editor.org/rfc/rfc6356
* RFC 9000, "QUIC: A UDP-Based Multiplexed and Secure Transport", especially
  stream multiplexing, path validation, and transport state separation,
  https://www.rfc-editor.org/rfc/rfc9000
* RFC 9002, "QUIC Loss Detection and Congestion Control", especially ACK ranges,
  PTO, and packet-number-based loss recovery,
  https://www.rfc-editor.org/rfc/rfc9002
* RFC 8446, "The Transport Layer Security (TLS) Protocol Version 1.3",
  especially bounded, independently authenticated TLSCiphertext records,
  https://www.rfc-editor.org/rfc/rfc8446
* RFC 5116, "An Interface and Algorithms for Authenticated Encryption",
  especially the uniqueness requirement for a nonce under one key,
  https://www.rfc-editor.org/rfc/rfc5116
* RFC 8985, "The RACK-TLP Loss Detection Algorithm for TCP", especially
  time-based loss detection and bounded tail probes below application framing,
  https://www.rfc-editor.org/rfc/rfc8985
* draft-ietf-ccwg-bbr-06, "BBR Congestion Control", especially delivery-rate
  sampling, application-limited sample handling, and bounded send quanta,
  https://datatracker.ietf.org/doc/html/draft-ietf-ccwg-bbr-06
* draft-ietf-quic-multipath-21, "Multipath Extension for QUIC", especially
  per-path identifiers, path management, and the deliberate separation between
  multipath protocol mechanisms and implementation-specific scheduling policy,
  including the warning that distributing one stream over paths with different
  delays can impose the maximum delay of all paths used,
  https://datatracker.ietf.org/doc/html/draft-ietf-quic-multipath-21
* RFC 9298, "Proxying UDP in HTTP", for HTTP CONNECT-UDP outbound behavior,
  https://www.rfc-editor.org/rfc/rfc9298
* Hysteria2 protocol and congestion-controller documentation, for QUIC-based
  proxy transport, direction-local BBR/Brutal selection, and the warning that
  an overstated fixed-rate target wastes traffic and becomes unstable,
  https://v2.hysteria.network/docs/developers/Protocol/
  and https://v2.hysteria.network/docs/advanced/Full-Server-Config/#congestion-control-details
* Lim et al., "MPTCP is not Pareto-Optimal: Performance Issues and a Possible
  Solution", for earliest-completion-first subflow scheduling,
  https://api.repository.cam.ac.uk/server/api/core/bitstreams/3ec47f93-4360-4630-bd4a-9e1ed23605fa/content
* Ferlin et al., "BLEST: Blocking Estimation-based MPTCP Scheduler for
  Heterogeneous Networks", for completion-aware head-of-line avoidance,
  https://olivier.mehani.name/publications/2016ferlin_blest_blocking_estimation_mptcp_scheduler.pdf

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
    stream = lookup_stream(stream_id)
    uniquely_proven_owner_ranges =
        newly_acked_ranges_with_exactly_one_flight_copy_of_kind_OwnerData(ranges)
    release_repair_cache_entries_covered_by(ranges)
    release_path_inflight_entries_covered_by(ranges)
    do_not_lower_delivery_rate_from_feedback_only_release_timing()
    for range in uniquely_proven_owner_ranges:
        if stream.is_request
           and exact_live_owner_may_grow_product_request_window(range.owner)
           and range.owner.underlay == stream.exact_ordered_Service_instance.underlay:
            record_product_request_window_ack_progress(
                owner=range.owner,
                bytes=range.byte_count,
                epoch=stream.exact_ordered_Service_instance,
                interval=stream.exact_ordered_Service_instance.PTO)
    # ACK-carrier identity is irrelevant. Repair, ambiguous, duplicated, and
    # stale-owner releases never enter uniquely_proven_owner_ranges above.
    epoch = stream.startup_sample_epoch
    if epoch exists:
        if epoch.request_side:
            assert epoch.candidate.underlay == TCP
        if epoch.candidate.underlay == TCP:
            for range in uniquely_proven_owner_ranges:
                if range.owner == epoch.candidate:
                    epoch.unambiguous_acked_owner_bytes += range.byte_count
            if epoch.unambiguous_acked_owner_bytes >= tcp_product_evidence_floor(epoch):
                mark_candidate_bulk_rate_proven_without_changing_service(epoch)
            if epoch.request_side
               and epoch.sample_is_irrevocably_sealed
               and epoch.unambiguous_acked_owner_bytes >= epoch.sealed_owner_bytes:
                seed_request_calibration_ACK_boundary(
                    exact_instance=epoch.candidate,
                    acked_at=this_ACK.completed_at,
                    provenance=ExactSealedOwnerDataACK)
    # Product STREAM_ACK timing is never QUIC packet delivery evidence.
    do_not_credit_quic_bulk_rate_evidence_from_product_ack(ranges)
    # Exact cap exhaustion seals at the cap. After a useful sample, a whole
    # next frame that exceeds remaining credit seals at actual admitted bytes;
    # that whole next frame stays on Service and smaller frames cannot reopen it.
    if epoch exists and epoch.request_side
       and epoch.sample_is_irrevocably_sealed
       and not epoch.receipt_marker_enqueued:
        epoch.receipt_marker =
            enqueue_stream_ordered_path_proof(epoch.exact_candidate_instance)
    if epoch exists and epoch.request_side
       and epoch.receipt_marker has matching validated ACK:
        sample_elapsed = epoch.receipt_marker.ack_completed_at
                         - epoch.first_owner_enqueue_at
        mark_candidate_bulk_rate_proven_without_changing_service(
            bytes=epoch.sealed_owner_bytes,
            elapsed=sample_elapsed,
            provenance=ExactOrderedCarrierStreamReceipt)
        seed_request_calibration_ACK_boundary(
            exact_instance=epoch.candidate,
            acked_at=epoch.receipt_marker.ack_completed_at,
            provenance=ExactOrderedCarrierStreamReceipt)
    # RepairData and ambiguous/duplicated ACKs are absent from
    # uniquely_proven_owner_ranges and never credit the startup epoch.
    if epoch exists and epoch.candidate.has_exact_bulk_rate_evidence
       and no_ordering_owner_flight_remains(epoch.candidate):
        clear_startup_owner_but_retain_sampled_membership(epoch)
        # A different never-sampled candidate may now receive its own budget.
    if ack_frontier_advanced_after_tail_repair:
        record_repair_ack_progress_diagnostic_only()
        do_not_mark_repair_path_as_sender_evidence()
        do_not_promote_repair_path_to_active_lifecycle_slot()
    if not complete:
        clear_product_gap_repair_tracker()
        do_not_infer_holes_from_omitted_ranges()
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
            owner = exact_single_copy_OwnerData_owner(holes.lowest_range)
            target = select_one_live_output_distinct_from_all_range_owners(holes)
            if target does not exist:
                do_not_repair_on_same_or_already_owning_output()
            else if owner exists and owner.underlay == TCP
                    and stream.lane is bulk
                    and target.has_bulk_model_evidence:
                # modeled_service_flight is approximately 2 * target BDP,
                # bounded by the selected output's remaining live service
                # headroom and resource envelope. The scheduled batch remains
                # bound to this target incarnation during dispatch; detach,
                # replacement, or repair-timeout cancels the queued remainder
                # before a later gap replay replans it.
                repair_limit = min(modeled_service_flight(target),
                                   holes.byte_count,
                                   repair_cache_remaining,
                                   configured_path_flight_remaining,
                                   target.capacity_remaining)
            else:
                repair_limit = ordinary_adaptive_repair_quantum(target)
            if target exists:
                schedule_repair(holes, target, repair_limit)
                rate_limit_repeated_repair_for(hole.start)
        else:
            remember_possible_receive_hole(hole.start)

on_tail_stall_repair(stream_id, last_complete_ack_ranges):
    repair_budget = critical_repair_budget(base_repair_budget)
    holes = unacked_chunks_below_largest_acked_not_covered_by(last_complete_ack_ranges)
    if holes is not empty:
        schedule_prefix_repair(holes, repair_budget)
    else if no_complete_ack_frontier_exists
            and lowest_unacked_owner_tail_can_use_alternate_output()
            and owner_tail_has_stalled_for_one_PTO():
        schedule_lowest_tail_repair_on_alternate_output(repair_budget)
    else if no_complete_ack_frontier_exists:
        do_not_repair_unknown_or_unstalled_live_tail_without_ack_frontier()
    else if lowest_unacked_owner_tail_can_use_alternate_output():
        schedule_lowest_tail_repair_on_alternate_output(repair_budget)
    else:
        do_not_repair_live_tail_on_same_or_only_output()
    if repair_is_final_tail
       and no_distinct_survivor_is_currently_attached:
        if stall_or_PTO_evidence_exists:
            retransmit_same_lowest_range_once_as_RepairData()
            route_as_connection_completion_repair_not_generic_ack_gap()
    else if lowest_repair_range_is_already_in_flight_on_every_usable_survivor
       or every distinct survivor lacks immediate stream-data queue credit:
        if stall_or_PTO_evidence_exists:
            retransmit_same_lowest_range_once_as_RepairData()
            if repair_is_final_tail:
                route_as_connection_completion_repair_not_generic_ack_gap()
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
startup_sample_allowed(stream, candidate):
    if stream.sender_direction == response:
        direction_allowed =
            stream.session.active_direction_relevant_reliable_response_flows >= 1
            and stream.session_load_generation_is_stable
    else if stream.sender_direction == request:
        direction_allowed =
            stream.service_path.underlay == TCP
            and candidate.underlay == TCP
            and stream.lane in {Throughput, Background}
            and stream.local_source_has_continuing_data
            and stream.service_path is exact live Active attachment instance
            and stream.service_path.has_bulk_rate_evidence
            and candidate is exact attached Validation instance
            and candidate.proof_observation_at >= candidate.attachment_time
    else:
        return false
    return direction_allowed
       and stream.is_bulk_only
       and stream.session_has_no_latency_sensitive_or_realtime_work
       and candidate.role == Validation
       and candidate.underlay_family == stream.service_path.underlay_family
       and not candidate.has_bulk_rate_evidence
       and no_authoritative_lower_debt(stream)

select_bulk_data_path(stream, frame, paths):
    if frame is repair:
        return best_survivor_avoiding_original_path()
    prospective_Service = none
    if stream.current_Service is absent
       and stream has no live lower_frontier_owner:
        if stream.ordered_owner_debt == 0:
            prospective_Service = best_live_attached_output(excluding Repair)
        else if stream.ordered_owner is missing:
            prospective_Service = best_sender_evidenced_output_on_same_underlay(
                                      stream.ordered_owner)
    candidates = current Service or prospective_Service
                 plus bulk-rate-proven measured Subflows,
                 excluding Repair attachments
    if stream has an active startup-sampling candidate
       and startup_sample_allowed(stream, that candidate):
        candidates += that one Validation candidate
    admitted = []
    for path in candidates:
        role = startup_sample_subflow
               if path is stream.startup_sample_epoch.candidate
               else lead_data_path
               if path is stream.current_Service or prospective_Service
               else ordinary_data_role(path)
        if bulk_admit(stream, path, frame, role):
            admitted += path
    if admitted is empty:
        queue_until_ack_release_or_path_update()
    if admitted contains startup_sample_subflow:
        return best_startup_sample_candidate(admitted)
    if stream has a lower_frontier_owner in admitted:
        return stream.lower_frontier_owner
    if admitted contains measured same-family Subflow
       and stream has sustained bulk backlog
       and that Subflow completes before Service drains the lower ordered tail
       and product/reorder/inflight reservoirs admit its next quantum:
        return best_completion_safe_same_family_Subflow(admitted)
    if stream.current_Service is in admitted:
        return stream.current_Service
    if prospective_Service is in admitted:
        return prospective_Service as Service
        # Commit Service ownership only after carrier enqueue succeeds.
    return best_admitted_overflow_Subflow(admitted)

assign_independent_bulk_flow(flow, paths):
    candidates = live_paths_with_delivery_or_probe_evidence(paths)
    for candidate in candidates:
        score candidate with active_bulk_flows incremented
    return best_candidate_with_fair_sharing()

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

admission_reorder_budget(stream, path, chunk, role, ordering_debt):
    if role == lead_data_path and ordering_debt == 0:
        return service_owner_envelope(path, chunk)
    if role == lead_data_path:
        if latency_sensitive_work_is_active_on_this_path:
            return min(base_reorder_budget(path, chunk),
                       throughput_service_horizon(chunk.len))
        return base_reorder_budget(path, chunk)
    if role == startup_sample_subflow:
        return startup_product_envelope(stream, chunk)
    if role == additional_same_underlay:
        return base_reorder_budget(path, chunk)
    return effective_reorder_budget(path, chunk)

service_owner_envelope(path, chunk):
    horizon = throughput_service_horizon(chunk.len)
    envelope = min(configured_stream_window,
                   configured_path_inflight,
                   configured_receiver_reorder)
    if not path.has_bulk_rate_evidence:
        return horizon
    reservoir = min(bounded_bbr_headroom_window(horizon), envelope)
    if latency_sensitive_work_is_active_on_this_path:
        return reservoir
    return envelope

optional_product_queue_envelope(path, chunk, role):
    bdp_limit = max(2 * path_bdp(path), chunk.len)
    if path.carrier_inflight_limit is known:
        modeled = min(path.carrier_inflight_limit, bdp_limit)
    else:
        modeled = bdp_limit
    return min(max(modeled, chunk.len),
               max(configured_path_inflight, chunk.len))

scheduler_inflight_debt(path, role):
    if role == lead_data_path:
        return max(path.product_bytes_in_flight, path.queue_bytes)
    if path.underlay == UDP and role == additional_cross_underlay:
        return path.carrier_queue_bytes + path.carrier_bytes_in_flight
    return path.product_bytes_in_flight

carrier_validation_queue_limit(path, chunk):
    if path.carrier_inflight_limit is known:
        modeled = min(path.carrier_inflight_limit, 2 * path_bdp(path))
    else:
        modeled = 2 * path_bdp(path)
    return max(modeled, chunk.len)

bulk_admit(stream, path, chunk, role):
    if role == startup_sample_subflow:
        epoch = stream.startup_sample_epoch
        if no epoch or not epoch.sampling_is_open or epoch.candidate != path:
            return false
        if not active_startup_epoch_selects(path, chunk):
            return false
        if not startup_sample_allowed(stream, path)
           or not stream.session_has_no_latency_sensitive_or_realtime_work
           or not stream.service_path.is_bulk_only
           or not path.is_bulk_only_eligible:
            return false
        if path.role != Validation
           or path.underlay_family != stream.service_path.underlay_family
           or not stream.service_path.has_bulk_rate_evidence
           or not path.has_local_sender_evidence
           or path.has_bulk_rate_evidence:
            return false
        if has_latency_sensitive_or_realtime_pressure(stream, path):
            return false
        if has_authoritative_lower_flight_or_repair_debt(stream):
            return false
        if epoch.candidate.cumulative_owner_bytes + chunk.len > epoch.candidate.sample_budget:
            return false
        if projected_reorder_debt(stream, path, chunk) >
           startup_product_envelope(stream, chunk):
            return false
        return carrier_and_resource_envelopes_allow(path, chunk)
    if role == lead_data_path:
        if scheduler_inflight_debt(path, role) + chunk.len >
           service_owner_envelope(path, chunk):
            return false
    else if role == additional_cross_underlay:
        if scheduler_inflight_debt(path, role) + chunk.len >
           carrier_validation_queue_limit(path, chunk):
            return false
    else:
        if scheduler_inflight_debt(path, role) + chunk.len >
           optional_product_queue_envelope(path, chunk, role):
            return false
    ordering_debt = stream_ordering_debt(stream, path, chunk)
    if product_reorder_debt(path) + ordering_debt + chunk.len >
       admission_reorder_budget(stream, path, chunk, role, ordering_debt):
        return false
    return completion_horizon_allows(path, chunk, role)

attach_validation_paths(stream, demand, paths):
    if demand is not bulk:
        return
    if stream has a validation open pending:
        return
    chunk = bounded_validation_proof_quantum(stream)
    candidates = paths without an existing stream output and not already attempted
    for path in candidates ordered by score_for_join:
        if path can be admitted for chunk bytes of bounded validation traffic:
            start_nonblocking_OPEN_STREAM(role=Validation)
            return  # at most one pending open; this pass stops here

on_validation_open_result(stream, path, result):
    clear_pending_validation_open(stream, path)
    if result is success and stream has no existing output for path:
        attach_validation_output(result)
        enqueue PATH_PROOF_DATA on the validation output
    # A later scheduler pass may try the next not-yet-attempted candidate.

on_PATH_PROOF_ACK(path, proof):
    if proof matches a pending proof on that path:
        record_path_proof_observation(path,
                                      proof.payload_bytes,
                                      proof.rtt,
                                      current_attachment_instance,
                                      now())
        mark_liveness_and_local_sender_evidence(path)
        do_not_record_bulk_delivery_or_pacing_rate_from_path_proof()

on_ordered_delivery(stream, path, delivered_bytes):
    account_delivered_bytes(path, delivered_bytes)
    if stream.demand is bulk:
        if path.has_bulk_rate_evidence:
            mark_path_eligible_for_measured_subflow_admission(path)
            if path is stream.subflow_set.startup_owner
               and no_ordering_owner_flight_remains(path):
                clear_startup_owner_but_retain_sampled_membership(stream, path)
        return  # Delivery or graduation never moves the Service owner implicitly.
    if explicit_service_migration_policy_allows(stream, path):
        promote_path_to_active_service(path)
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
    if metrics are exact fresh post_attachment
       direction_correct_non_app_limited_bulk_rate_evidence:
        mark_matching_response_startup_candidate_bulk_rate_proven_without_changing_service(path)
        mark_matching_request_Validation_eligible_for_ordinary_measured_ownership(path)
        if no_ordering_owner_flight_remains(path):
            clear_startup_owner_but_retain_sampled_membership(path)
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

### B.7 Carrier Stall and Product Repair

```
on_carrier_or_product_stall(path, stream):
    if quic_reports_connection_closed_or_path_failed(path):
        mark_path_suspect_for_new_bulk()
    if repair_authoritative_ack_gap_stalls_while_survivor_exists(stream):
        queue_gap_targeted_product_repair_for_unacked_ranges(path, stream)
    if final_offset_known_and_terminal_tail_stalls(stream):
        queue_budget_capped_terminal_tail_repair(path, stream, optional_budget)
    if active_stall_budget_exceeded(path, stream):
        detach_active_work_to_survivor_path()
        cool_failed_active_path_for_data_scheduling()

on_receive_progress_timer(path, stream):
    if path.underlay == UDP
       and stream.peer_is_open
       and (stream.next_receive_offset > 0 or stream.reorder_debt > 0):
        replay_bounded_STREAM_ACK_and_MAX_DATA_snapshot()
    else if path.underlay == TCP
            and stream.peer_is_open
            and stream.has_live_multipath_repair_alternative
            and stream.reorder_debt > 0:
        replay_bounded_STREAM_ACK_and_MAX_DATA_snapshot()
    else:
        do_not_use_idle_TCP_heartbeat_as_product_progress()

initial_active_open(path, stream, alternative_exists):
    pto = candidate_pto(path)
    if path.underlay == TCP:
        pto = max(pto, conservative_initial_pto())
    phases = 3 if path.underlay == TCP else 2
    multiplier = phases if alternative_exists
                 else phases - 1 + sum(2^i,
                                       i=0..persistent_congestion_threshold-1)
    deadline = now() + multiplier * pto
    open_stream_on_path_until(path, stream.id, deadline)

recovery_open(path, stream):
    deadline = now() + candidate_pto(path)
    if open_stream_on_path_until(path, stream.id, deadline) fails:
        release_reserved_path_load(path, stream.lane)
        mark_data_plane_failure(path)
        try_next_survivor_without_waiting_for_idle_heartbeat()
```

### B.8 Unified Sender Loop

```
startup_sample_budget(stream):
    envelope_budget = min(RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2,
                          stream.path_flight_envelope,
                          stream.receiver_reorder_envelope,
                          stream.repair_envelope,
                          stream.flow_control_window)
    return envelope_budget

startup_product_envelope(stream, next_quantum):
    configured = min(stream.path_flight_envelope,
                     stream.receiver_reorder_envelope,
                     stream.repair_envelope,
                     stream.flow_control_window)
    return max(next_quantum.len, configured)

may_start_response_subflow_sample(stream, candidate, next_quantum):
    return candidate exists
       and stream.is_response and stream.has_sustained_bulk_backlog
       and stream.is_bulk_only
       and stream.session.active_direction_relevant_reliable_response_flows >= 1
       and stream.session_has_no_latency_sensitive_or_realtime_work
       and stream.service_path.is_bulk_only
       and candidate.is_bulk_only_eligible
       and not has_latency_sensitive_or_realtime_pressure(stream, candidate)
       and not stream.subflow_set_epoch.has_startup_sample_candidate
       and candidate.role == Validation
       and candidate.underlay_family == stream.service_path.underlay_family
       and stream.service_path.has_bulk_rate_evidence
       and candidate.has_local_sender_evidence
       and not candidate.has_bulk_rate_evidence
       and no_authoritative_lower_debt(stream)
       and lower_debt_is_empty_or_live_service_contiguous_suffix(stream)
       and projected_reorder_debt(stream, candidate, next_quantum)
           <= startup_product_envelope(stream, next_quantum)
       and (stream has no measured same-family Subflow
            or whole_startup_sample_completion(candidate)
               <= current_Service_reservoir_completion(stream))

may_start_request_subflow_sample(stream, candidate, next_quantum):
    # Normal exact Service product-flight residence age is not stale-tail
    # authority for request startup. The flow-count gate opens a fresh owner;
    # an already-assigned exact owner may drain after a 2 -> 1 transition.
    return candidate exists
       and stream.is_request
       and stream.service_path.underlay == TCP
       and candidate.underlay == TCP
       and (stream.subflow_set.startup_owner is candidate
            or stream.session
                .active_tcp_service_bulk_request_flows_with_present_work >= 2)
       and stream.lane in {Throughput, Background}
       and stream.local_source_has_continuing_data
       and stream.is_bulk_only
       and stream.session_has_no_latency_sensitive_or_realtime_work
       and stream.service_path is exact live Active attachment instance
       and stream.service_path.has_bulk_rate_evidence
       and candidate is exact attached Validation instance
       and candidate.underlay_family == stream.service_path.underlay_family
       and candidate.proof_observation_at >= candidate.attachment_time
       and not candidate.has_bulk_rate_evidence
       and not stream.subflow_set.has_different_startup_owner
       and not stream.subflow_set.contains_sampled_instance(candidate)
       and not has_latency_sensitive_or_realtime_pressure(stream, candidate)
       and no_authoritative_lower_debt(stream)
       and no_foreign_lower_ordering_owner_flight(stream)
       and lower_debt_is_empty_or_live_service_contiguous_suffix(stream)
       and projected_reorder_debt(stream, candidate, next_quantum)
           <= startup_product_envelope(stream, next_quantum)

plan_response_source_chunk(stream, normal_chunk):
    residual = exact_active_TCP_response_calibration_remaining_credit(stream)
    if residual exists and 0 < residual < normal_chunk.len:
        short_plan = plan_response_owner_path(stream, normal_chunk.prefix(residual))
        if short_plan carries exact_active_TCP_calibration_commit(stream):
            return normal_chunk.prefix(residual), short_plan
        # A non-calibration first pass is not permission to fragment Service.
        discard(short_plan)
    return normal_chunk, plan_response_owner_path(stream, normal_chunk)

select_response_owner_path(stream, work):
    epoch = stream.startup_sample_epoch
    if epoch exists:
        assert stream.service_path did not change
        if not epoch.sampling_is_open:
            return select_path_by_eta_and_lane(work)
        if not stream.has_sustained_bulk_backlog or not stream.is_bulk_only
           or stream.session.active_direction_relevant_reliable_response_flows < 1
           or not stream.service_path.is_bulk_only
           or not epoch.candidate.is_bulk_only_eligible
           or has_latency_sensitive_or_realtime_pressure(stream, epoch.candidate)
           or has_authoritative_lower_debt(stream)
           or epoch.candidate is detached/failed/not Validation:
            suspend_epoch_sampling_without_refilling_or_owner_transfer(epoch)
            return select_path_by_eta_and_lane(work)
        remaining = epoch.candidate.sample_budget
                    - epoch.candidate.cumulative_owner_bytes
        if work.payload.len <= remaining
           and projected_reorder_debt(stream, epoch.candidate, work)
               <= startup_product_envelope(stream, work)
           and carrier_and_resource_envelopes_allow(epoch.candidate, work):
            return epoch.candidate
        if epoch.candidate.cumulative_owner_bytes >= epoch.candidate.sample_budget:
            close_epoch_sampling_and_resume_service_without_owner_transfer(epoch)
        return stream.service_path if service_admission_allows(work) else no_path
    # Eligible endpoint-only TCP installs a temporary Service capacity prior
    # at exact startup drain and enters ordinary bounded Subflow admission.
    calibration = exact_live_graduated_TCP_response_fallback_calibration(stream)
    if calibration exists:
        assert stream.service_path did not change
        if calibration.spent_bytes == 0 and not calibration.is_active:
            if stream.session.active_direction_relevant_reliable_response_flows < 1:
                keep_exact_identity_dormant_and_block_only_it_from_generic_owner(calibration)
                return select_path_excluding_exact_calibration_identity(stream, work)
            C = calibration.authorized_cumulative_ceiling
            F = service_feed_reservoir(stream, work)
            R = min(product_resource_envelope(stream), C + F)
            candidate_completion = calibration.next_payload_eta
                + transmit_time(calibration, C - work.payload.len)
            ordering_deadline = stream.service_path.current_eta
                + transmit_time(stream.service_path, F)
            if candidate_completion > ordering_deadline:
                atomically_retire_unspent_calibration_if(
                    planner_and_combined_lane_flow_generations_match
                    and response_path_ordering_model_generation_matches
                    and exact_service_and_target_incarnations_match
                    and captured_service_and_target_pending_values_match
                    and exact_ceiling_and_active_response_start_gate_match
                    and calibration_has_no_exact_owner_flight)
                resnapshot_response_plan_once()
                return select_response_owner_path(stream, work)
        # Exact begun work may finish after response-demand generation churn;
        # the fresh active-demand predicate is deliberately not repeated.
        if calibration.spent_bytes + work.payload.len
               <= calibration.authorized_cumulative_ceiling
           and aggregate_owner_repair_pressure_pending_and_resource_guards_allow(
                   stream, calibration, work):
            reserve_exact_nonrefilling_calibration_credit_with_enqueue_rollback(
                calibration, work)
            return calibration as ack_clock_calibration_subflow
        # Exhaustion does not refill credit or move Service. A strictly causal
        # later ACK window may double the cumulative ceiling, bounded by the
        # configured resource envelope; otherwise this stage is terminal.
        return stream.service_path if service_admission_allows(work) else no_path
    candidate = best_same_underlay_validation_candidate_with_sender_evidence(stream)
    if may_start_response_subflow_sample(stream, candidate, work):
        begin_epoch(candidate, startup_sample_budget(stream, work))
        return select_response_owner_path(stream, work)
    return select_path_by_eta_and_lane(work)

on_exact_TCP_response_owner_ACK(calibration, window):
    fresh = exact_bytes_sent_at_or_after(
        window.released_owner_flights, calibration.stage_authorized_at)
    if fresh == 0:
        return
    strict = window.is_later_ACK_window
        and window.latest_sampled_send_at <= window.previous_ACK_at
        and window.earliest_sampled_send_at >= calibration.stage_authorized_at
        and fresh == window.total_bytes
    if strict:
        calibration.E += fresh
        calibration.stage_rate_elapsed += window.raw_ACK_to_ACK_elapsed
    else:
        calibration.W += fresh
    if calibration.spent_bytes < calibration.L:
        return

    F = min(calibration.resource_clamped_service_horizon,
        max(MIN_RATE_SAMPLE_BYTES,
            ceil(calibration.resource_clamped_service_horizon / 2)))
    A = calibration.L - calibration.B
    if A - calibration.W < F:
        required = calibration.B + calibration.W + F
        if required > calibration.resource_ceiling:
            calibration.terminal_without_rate = true
            return
        calibration.L = min(calibration.resource_ceiling,
            max(2 * calibration.L, required))
        return  # Preserve B, authorization time, E, and W.
    if calibration.E < F:
        return  # Later strict windows can still complete this stage.

    aggregate_rate = 8 * calibration.E
        / max(calibration.stage_rate_elapsed, TIMER_GRANULARITY)
    append_to_bounded_stage_rates(calibration, aggregate_rate)
    reset_current_stage_rate_evidence(calibration)  # E = W = 0.
    if calibration.stage_rate_count >= 3:
        calibrated = median(calibration.stage_rates)
        overwrite_product_progress_and_delivery_rate(calibration, calibrated)
        # Exclusive calibration is path-capacity evidence, not the ordinary
        # per-flow TCP ACK clock.
        calibration.proven = true
        return
    retain_provisional_startup_rate(calibration)
    if calibration.L < calibration.resource_ceiling:
        calibration.B = calibration.spent_bytes
        calibration.L = min(2 * calibration.L, calibration.resource_ceiling)
        calibration.stage_authorized_at = window.acked_at
    else:
        calibration.terminal_without_rate = true

on_TCP_calibration_owner_flights_drained(calibration):
    A = calibration.L - calibration.B
    calibration.W = max(calibration.W, A - calibration.E)
    apply_the_same_reachability_topup_or_hard_terminal(calibration)
    reset_ordinary_TCP_product_ACK_clock(calibration.path)
    if calibration.calibrated_rate exists:
        calibration.path.capacity_prior = {
            rate: calibration.calibrated_rate,
            ordinary_windows: 0,
        }
    else:
        calibration.path.capacity_prior = none
    # ACK release never decreases cumulative spent credit. UDP/QUIC product
    # ACKs never enter this calibration; their local carrier ACK controller
    # remains authoritative.

on_ordinary_exact_TCP_response_ACK(path, window):
    update_continuous_product_goodput(path, window)
    if path.capacity_prior exists and window.completed_exact_ACK_window:
        path.capacity_prior.ordinary_windows += 1
    if path.capacity_prior exists
       and path.capacity_prior.ordinary_windows >= 10
       and path.has_usable_continuous_goodput_sample:
        publish_per_flow_product_delivery_and_TCP_ACK_rate(path)
        path.capacity_prior = none

select_request_owner_path(stream, work):
    if stream.exact_ordered_Service_instance.underlay == QUIC_UDP:
        return select_ordinary_request_owner_with_native_evidence(
            stream, work, exact_instance=true, post_attachment=true,
            non_app_limited_packet_ACK=true)
    assert stream.exact_ordered_Service_instance.underlay == TCP
    set = stream.request_subflow_set
    candidate = set.startup_owner
    if candidate exists and candidate.has_bulk_rate_evidence
       and no_ordering_owner_flight_remains(candidate):
        clear_startup_owner_but_retain_sampled_membership(set, candidate)
        candidate = none
    if candidate exists:
        if may_start_request_subflow_sample(stream, candidate, work):
            remaining = candidate.sample_budget - candidate.cumulative_owner_bytes
            if work.payload.len <= remaining
               and carrier_and_resource_envelopes_allow(candidate, work):
                return candidate as startup_sample_subflow
            if candidate.cumulative_owner_bytes >= useful_rate_sample_bytes
               and work.payload.len > remaining:
                irrevocably_seal_sample_at_actual_admitted_bytes(candidate)
                enqueue_one_ordered_receipt_marker_behind_sealed_sample(candidate)
                # The whole frame remains ordinary Service work; do not split it.
                return stream.service_path if service_admission_allows(work) else no_path
        # Cap, pressure, debt, or stale state never rotates or refills credit.
        return stream.service_path if service_admission_allows(work) else no_path
    candidate = best_never_sampled_same_underlay_validation_instance_with_fresh_proof(stream)
    if may_start_request_subflow_sample(stream, candidate, work):
        assign_startup_owner(candidate,
                             startup_sample_budget(stream, work),
                             retain_membership_after_graduation=true)
        return select_request_owner_path(stream, work)
    calibration = stream.explicit_request_calibration_owner
    if calibration is none:
        candidate = best_exact_live_same_underlay_graduated_TCP_candidate(stream)
        if candidate exists
           and candidate.has_exact_request_calibration_ACK_boundary
           and stream.session
                  .active_tcp_service_bulk_request_flows_with_present_work >= 2:
            calibration = atomically_claim_request_calibration_owner(
                exact_instance=candidate,
                frozen_target=min(2_MiB_default_target,
                                  configured_path_flight,
                                  configured_repair,
                                  configured_receiver_reorder,
                                  configured_stream_window,
                                  frame_reachability_ceiling))
    if calibration exists:
        assert calibration.exact_instance
               == stream.explicit_request_calibration_owner.exact_instance
        limit = calibration.frozen_target
        if calibration.spent_bytes == 0:
            if stream.session
                   .active_tcp_service_bulk_request_flows_with_present_work < 2:
                return stream.service_path if service_admission_allows(work) else no_path
            # A path-wide estimate has no request-side completion-veto authority
            # until request-direction, provenance-bound evidence exists. The
            # exact-owner, debt, resource, pressure, and 2 MiB cap remain.
        # Once spent, the exact calibration owner may finish after a 2 -> 1
        # transition; neither fresh-start gate is deliberately reapplied.
        if calibration.spent_bytes + work.payload.len <= limit
           and stable_Active_Service_and_fresh_Validation_proof(stream, calibration)
           and work.was_sent_after(calibration.exact_ACK_boundary)
           and TCP_calibration_guards_allow_including_stale_tail_age(stream)
           and carrier_and_resource_envelopes_allow(calibration, work):
            reserve_exact_owner_target_and_nonrefilling_credit_with_enqueue_rollback(
                calibration, work)
            return calibration as ack_clock_calibration_subflow
        if calibration.spent_bytes + work.payload.len > limit
           and exact_ordering_owner_flight_remains(calibration):
            return stream.service_path if service_admission_allows(work) else no_path
        # Flight drain alone does not clear this exact owner. Exact proof or an
        # exact lifecycle transition must resolve it before another owner.
    return select_path_by_eta_and_lane(work)

on_exact_TCP_request_candidate_ACK(candidate, sample):
    update_candidate_continuous_exact_product_ACK_model(candidate, sample)
    if candidate.is_endpoint_only and candidate.exact_sample_count < 10:
        retain_only_provisional_Service_seed_for_candidate_rate_and_pipe(candidate)
    else:
        replace_provisional_Service_seed_with_candidate_model(candidate)
    # This product model is never native QUIC packet-ACK authority.

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
            path = select_response_owner_path(flow, work)
                   if flow is a response stream
                   else select_request_owner_path(flow, work)
                   if flow is a reliable request stream
                   else select_path_by_eta_and_lane(work)
            work = flow.peek_next_quantum()  # Product frame identity is stable.
            if no_path(path):
                record_blocked(flow, "no-eligible-path")
                continue
            startup = active_startup_owner(flow)
            bulk_role = startup_sample_subflow
                        if startup exists and path == startup.path
                           and startup.sampling_is_open
                        else ordinary_data_role(path, best_path)
            if work.is_throughput_data()
               and not bulk_admit(flow, path, work, bulk_role):
                record_blocked(flow, "bulk-admission")
                continue
            if not carrier_or_tcp_budget_allows(path, work):
                record_blocked(flow, "carrier-budget")
                continue

            frame = flow.pop_next_quantum()
            retain_repair_state_if_reliable(frame)
            record_path_flight_if_stream_data(path, frame)
            emit_to_carrier(path, frame)
            if startup exists and frame is unique OwnerData
               and path == startup.path and startup.sampling_is_open:
                startup.cumulative_owner_bytes += frame.payload.len
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
