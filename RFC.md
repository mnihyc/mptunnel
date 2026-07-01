# mptunnel Protocol Specification

Intended status: Standards Track

Protocol version: 1

Last updated: 2026-07-01

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
20. Management API, Diagnostics, and Lab Instrumentation
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

TCP targets use the reliable stream layer. UDP targets use datagram flows and
prefer UDP underlay, but MAY use TCP underlay as best-effort UDP-in-TCP relay
when that is the only available or surviving option.

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
  A UDP packet transport with encrypted packet headers/payloads, packet numbers,
  ACK ranges, frame fragmentation/reassembly, retransmission, pacing, and
  connection continuity below product frames.

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
* It MUST NOT terminate production traffic merely because a lab resource goal is
  exceeded; production behavior is adaptive and self-evolving.
* Lab assertions, diagnostics, ablations, and benchmarks MUST NOT be compiled
  into release bundles unless an explicit diagnostics feature is enabled.

The implementation targets fluent web browsing, SSH-like interactive flows,
video/game-like UDP behavior, bulk downloads, bulk uploads, failover recovery,
and mixed links with substantially different latency, bandwidth, and loss.

### 3.1 Operating Assumptions

Cross-platform support is a product requirement because the local edge is
usually a user device or workstation, while the remote edge is often a VPS. TCP
and UDP underlays are both first-class because they solve different operational
problems: TCP is widely reachable and proxy-friendly, while UDP allows the
protocol to observe packet numbers, ACK ranges, loss, pacing, and roaming
directly.

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
  UDP: encrypted packet carrier with ACK ranges and retransmission

Cryptographic layer
  AES-256-GCM by default, ChaCha20-Poly1305 optional
```

The same product frames are used over TCP and UDP underlays. TCP underlay gives
reachability and a reliable byte pipe. UDP underlay lets mptunnel own packet
numbers, ACKs, pacing, loss recovery, and NAT rebinding behavior.

Using one product frame grammar above both underlays prevents feature drift. A
stream opened over TCP can later be repaired or reattached over UDP because
stream IDs and offsets live above the carrier. Conversely, keeping carrier
behavior below product frames lets TCP and UDP be optimized independently. TCP
does not expose packet loss or useful packet numbers to the product; UDP does.
The architecture therefore shares semantic frames but not congestion-control
assumptions.

This mirrors the useful separation in mature transports: MPTCP separates the
logical byte stream from subflow sequence spaces, QUIC separates streams from
packet recovery, and BBR-style controllers separate delivery-rate models from
application semantics. mptunnel applies the same separation while preserving
proxy and TUN compatibility.

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

### 5.3 Resource Parameters

Default resource parameters are:

| Parameter | Default |
| --- | ---: |
| max frame bytes | 1,048,576 |
| max payload bytes | 1,048,512 |
| max ACK ranges | 256 |
| max paths | 64 |
| max streams | 65,536 |
| max stream window bytes | 67,108,864 |
| max repair bytes | 67,108,864 |
| max reorder bytes | 67,108,864 |
| max datagram queue bytes | 16,777,216 |
| max TCP path inflight bytes | 33,554,432 |
| max TCP relay chunk bytes | 524,288 |
| TCP path heartbeat interval | 10,000 ms |
| TCP path heartbeat timeout | 30,000 ms |

An implementation MUST validate that limits are internally coherent. In
particular, ACK range count, path count, stream count, stream window, repair
capacity, reorder capacity, datagram queue capacity, relay chunk size, path
inflight size, and heartbeat timings MUST be nonzero where applicable. A relay
chunk MUST NOT exceed maximum payload bytes, TCP inflight MUST be at least one
relay chunk, and TCP inflight MUST NOT exceed repair capacity.

These values are runtime resource limits, not lab pass/fail hard guards. The
scheduler and carriers MUST adapt within the configured envelope.

The defaults are chosen to provide a usable operating envelope without
hard-coding lab pass/fail behavior:

* A 1 MiB frame limit is large enough for efficient bulk transfer and compact
  enough to reject accidental or malicious oversized messages before they create
  unbounded allocation pressure.
* The 1,048,512 byte payload limit reserves header space under the 1 MiB frame
  ceiling so a conforming encoder can validate capacity before serialization.
* 256 ACK ranges support sparse recovery under burst loss without allowing ACK
  frames to become a second data stream.
* 64 paths and 65,536 streams are protocol-scale limits. They are above normal
  deployment needs but low enough to keep registries, arrays, and diagnostics
  bounded.
* 64 MiB stream, repair, and reorder budgets cover roughly one second of data at
  about 500 Mbps, or a smaller time slice near 1 Gbps, which is sufficient for
  high-BDP lab and VPS paths without making web browsing or SSH reserve that
  memory up front.
* The 16 MiB datagram queue protects realtime UDP from bulk stream pressure
  while still allowing short bursts and NAT-rebinding recovery.
* The 32 MiB TCP path inflight budget is intentionally below repair capacity so
  TCP path queues cannot consume all retransmission memory.
* The 512 KiB TCP relay chunk is a read-buffer ceiling. It MUST NOT become an
  indivisible scheduler item, AEAD record, or shared-path write quantum. The
  scheduler uses smaller preemptible quanta so control, ACK, repair, latency,
  and later bulk flows can interleave with existing bulk transfer.
  Throughput quanta are nevertheless required to amortize user-space encryption,
  framing, and write wakeups. A sender MUST NOT let a transient low measured
  delivery rate create a self-reinforcing tiny-frame loop for sustained bulk
  streams. For throughput lanes, the send quantum is chosen from path BDP,
  configured relay capacity, and observed stability/queue pressure: healthy
  ordinary paths use CPU-amortizing quanta up to the 64 KiB bulk quantum ceiling,
  while lossy, jittery, or queued paths shrink toward smaller preemptible
  quanta. Inflight limits and carrier pacing control network pressure; the
  frame quantum controls scheduling preemption and per-byte processing cost.
  Receiver-side stream input queues and path command queues follow the same
  rule. Their depth is sized from the relevant byte window divided by the
  actual maximum product-frame payload used by the attachment, not from the TCP
  relay chunk ceiling. A UDP carrier delivering roughly-MTU reliable fragments
  therefore receives many more queue slots than a TCP path delivering large
  relay frames for the same byte budget. This preserves byte-bounded memory
  while preventing the carrier input loop or relay sender task from blocking
  behind an artificially TCP-sized frame count.
* The 10s heartbeat interval and 30s timeout avoid noisy idle traffic while
  still detecting silent TCP path death fast enough for Auto to shift new work
  before users experience long stalls. UDP paths use packet ACK/PTO state for
  finer-grained data-plane recovery.

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
* UDP engine: `engine=quic`, `engine=custom-lab`
* Capabilities: `backup`, `expensive`, `low-latency`, `bulk-allowed`, `bulk`,
  `no-bulk`, `probe-only`, `no-udp`

Boolean values MAY be explicit (`true`, `false`, `1`, `0`, `yes`, `no`, `on`,
`off`) or bare; a bare boolean means true.

The `engine` parameter is valid only on `udp://` paths. It selects the internal
UDP control engine while preserving the product-level UDP underlay contract. If
`engine` is omitted on a UDP path, the implementation MUST use `engine=quic`.
`engine=quic` denotes the QUIC/Hysteria-style production carrier track:
reliable streams and datagrams over UDP with mature packet ACK, loss recovery,
pacing, congestion control, connection ID, and roaming behavior. `engine=custom-lab`
selects the current custom UDP carrier and is experimental/lab-only until it
reaches at least 80% of Hysteria2 or direct throughput on the same
high-bandwidth single-path lab while preserving browsing, upload, datagram,
failover, and resource behavior. Implementations MUST NOT silently treat a
requested engine as another engine. A production build MUST support the QUIC
runtime; custom-lab is opt-in and MUST NOT be the default UDP engine. The QUIC
client MUST NOT emit a fixed product-identifying SNI value. The QUIC server
certificate and client trust anchor MUST be derived from the configured
mptunnel shared secret, and the client MUST reject any server certificate that
does not match that derived identity. This binds QUIC confidentiality to the
same operator secret used by the product `SESSION_AUTH` and `PATH_JOIN`
transcripts, so an active relay cannot terminate QUIC and inspect product
frames without knowing the shared secret. Product authentication remains
mandatory after the QUIC handshake; it provides per-session and per-path
freshness, replay resistance, and authorization.

The QUIC production engine MUST configure its QUIC transport envelope from the
same resource model used by the product stream layer. The QUIC per-stream
receive window is the configured mptunnel stream window. The QUIC connection
receive window and local send window are derived from the configured stream,
repair, reorder, datagram queue, and path-inflight byte budgets so a high-BDP
UDP path is not silently constrained by generic library defaults. The admitted
concurrent QUIC bidirectional stream count is derived from that byte envelope
and the stream window, then bounded by the configured stream limit. QUIC
unidirectional streams are not used by protocol version 1. The production QUIC
engine SHOULD use a BBR-style congestion controller when the implementation
library provides one, because mptunnel's UDP goal is Hysteria-like high-BDP
delivery with model-based pacing rather than loss-only growth.

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

### 7.4 UDP Carrier Key Derivation

UDP carrier packets derive a per-connection key:

```
SHA256("mptunnel udp carrier packet key v1" ||
       cipher_suite_context ||
       connection_id_be64 ||
       master_secret)
```

### 7.5 Nonce Construction

TCP framed encryption and UDP carrier packet encryption use a 12-byte nonce:

```
byte 0      direction
bytes 1-3   zero
bytes 4-11  counter_or_packet_number_be64
```

Direction values are:

* 1: client to server
* 2: server to client

Counters and packet numbers MUST NOT repeat for the same key and direction.

The direction byte and monotonic counter or packet number make nonce uniqueness
easy to audit. Direction separation prevents a packet emitted by one peer from
being valid as a replay in the opposite direction under the same session
material.

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
The freshness window is configurable because lab containers, embedded systems,
and VPS images can have different clock quality.

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
bytes_in_flight:u64
queue_bytes:u64
inflight_limit_bytes:u64
inflight_hi_bytes:u64
confidence_ppm:u32
app_limited:u8
has_ack_derived_data_sample:u8
data_sample_count:u32
```

Metrics are advisory and MUST NOT bypass local safety checks.

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
direction, `confidence_ppm` and `data_sample_count` are sender-side evidence and
SHOULD materially raise the path model confidence. A mature sample set with high
confidence is not merely a liveness hint. Peer-provided metrics, successful
opens, and control-only traffic remain low-confidence validation hints unless
local delivery or carrier ACK-derived data samples confirm them. The sender path
model MUST also add locally queued carrier command bytes to `queue_bytes` for
all underlays, including TCP, so hidden path queues cannot be ignored by
ECF/BLEST admission.

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
realtime flows become protected. This prevents bulk discovery from deadlocking
while still protecting browsing, SSH-like, ACK/control, repair, and datagram
traffic from already-proven bulk.

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

The UDP carrier is a product-level UDP abstraction below product frames. Its
wire packet exposes no product magic string, target name, fixed product SNI, or
proxy protocol metadata.

UDP is the performance carrier. It gives the protocol direct control of packet
numbers, ACK ranges, pacing, PTO, PMTU, and NAT rebinding. This is the same
broad reason QUIC and Hysteria2 build above UDP: the transport can recover loss
and pace data without waiting for kernel TCP behavior.

Protocol version 1 defines two UDP engine contracts:

* `engine=quic`, the production UDP carrier. It uses standard QUIC wire packets
  and QUIC TLS 1.3, but product frames remain mptunnel frames carried inside
  QUIC bidirectional streams. QUIC certificates and trust anchors are derived
  from the shared secret as specified in Section 6, and product `SESSION_AUTH`
  and `PATH_JOIN` are still required after the QUIC connection is established.
  The QUIC transport profile MUST use the product resource envelope and
  BBR-style congestion control as specified in Section 6.
* `engine=custom-lab`, the experimental custom UDP carrier. It uses the packet
  and ACK formats in Sections 11.1 through 11.8. It is not the production
  default until it reaches the release gate stated in Section 6.

mptunnel keeps the UDP carrier boundary explicit: future UDP engines MAY be
added only when the product-frame contract, authentication, replay protection,
scheduling semantics, and security properties remain conformant with this
specification.

### 11.1 Custom-Lab UDP Packet Format

This section applies to `engine=custom-lab`.

Each UDP packet has:

```
0       version = 1
1       direction
2..10   connection_id u64
10..18  packet_number u64
18..    encrypted_payload || tag16
```

The first 18 bytes are AEAD additional authenticated data. The encrypted payload
is one UDP carrier payload. The receiver uses `connection_id` for demultiplexing
before decryption.

### 11.2 UDP Carrier Payload Registry

Payload kind values are encrypted:

| Kind | Name | Fields |
| ---: | --- | --- |
| 1 | ACK | `largest_acked:u64`, `ack_delay_us:u32`, `u16 count`, packet ACK ranges |
| 2 | ordered frame fragment | stream ID, frame ID, fragment offset, total frame length, bytes |
| 3 | close stream | stream ID |
| 4 | unreliable unordered frame fragment | same fragment fields |
| 5 | reliable unordered frame fragment | same fragment fields |

Packet ACK ranges use `u64 start`, `u64 end`, with `start < end`.

Frame fragment fields are:

```
stream_id:u64
frame_id:u64
offset:u32
total_len:u32
payload:bytes
```

`ordered` fragments are ACK-eliciting. Reliable unordered fragments are
ACK-eliciting. Unreliable unordered fragments are not ACK-eliciting.

When product frames are carried over UDP, `STREAM_DATA` uses reliable unordered
carrier fragments because product stream offsets provide the ordering and repair
identity. `STREAM_ACK` uses unreliable unordered carrier fragments because it is
feedback, not payload: later ACKs cover earlier state, and retransmitting stale
ACK snapshots can consume reverse-path capacity and corrupt the sender's
congestion model. Receivers compensate by sending ACKs promptly on gap changes,
delivery progress, and repair-horizon advancement. Carrier ACK-only feedback
remains a separate UDP carrier payload and is not itself reliable or congestion
controlled.

A product-bearing UDP carrier stream is established by an ordered control frame,
normally `OPEN_STREAM`, `OPEN_DGRAM_FLOW`, or `PING`. Reliable unordered
fragments, such as `STREAM_DATA`, MUST NOT create a new product carrier stream
by themselves. If an endpoint receives a fresh, successfully decrypted,
ACK-eliciting packet that contains a reliable unordered fragment for an unknown
carrier stream, it MUST queue the carrier ACK for that packet. The product
fragment is then stored in a bounded orphan/reorder buffer until the ordered
control frame arrives, or it is dropped when the buffer or age bound is
exceeded. Dropping an orphaned product fragment is a product-layer event and
MUST NOT be represented as carrier packet loss. This preserves the product
invariant that target metadata and stream role are known before data is routed
without corrupting the carrier RTT, delivery-rate, PTO, or congestion model.

### 11.3 Packet Numbers and ACKs

Each direction has a monotonically increasing packet number. Receivers collect
packet numbers of ACK-eliciting packets and send ACK ranges. ACKs are encrypted
carrier payloads and are not themselves reliable product frames.

An ACK-only carrier packet is pure feedback. It MUST NOT be counted as bytes in
flight, MUST NOT be stored in the retransmission pending set, MUST NOT wait for
congestion-window or pending-byte capacity, and MUST NOT wait for a bulk pacer
token. The implementation MAY coalesce or rate-limit pathological ACK floods,
but bulk data MUST NOT delay ACK feedback. Timely ACK delivery is part of loss
recovery, RTT measurement, delivery-rate sampling, and failover.

`largest_acked` is the largest packet number covered by the ACK ranges.
`ack_delay_us` is the receiver-side time between receiving `largest_acked` and
emitting the ACK packet. The timestamp used for `ack_delay_us` MUST belong to
the largest packet in the emitted ACK batch, not merely the largest packet ever
seen on the connection. The sender computes a raw RTT sample from
`now - sent_time[largest_acked]`, then subtracts at most the configured maximum
ACK delay when doing so would not reduce the sample below the path minimum RTT:

```
latest_rtt = now - sent_time[largest_acked]
ack_delay = min(ack_delay_us, max_ack_delay_us)
if latest_rtt >= min_rtt + ack_delay:
    adjusted_rtt = latest_rtt - ack_delay
else:
    adjusted_rtt = latest_rtt
```

Receivers SHOULD batch ACKs during in-order delivery to reduce overhead, but
MUST send an ACK promptly when a newly received packet reveals reordering or a
gap. This mirrors the practical reason QUIC ACK ranges exist: gap feedback must
reach the sender quickly enough for packet-threshold loss detection and
survivor-path repair.

ACK frames MUST describe recent received packet ranges, not only packets
received since the previous ACK flush. Because UDP ACK packets can themselves be
lost, a receiver MUST retain a bounded recent receive history and repeat those
ranges in later ACKs until they age out of the history. If an ACK frame cannot
carry every recent range, it MUST prefer the newest ranges, including the range
that contains `largest_acked`. This keeps RTT sampling and loss detection tied
to current delivery and prevents a lost ACK from turning already delivered
packets into artificial loss.

On ACK, the sender releases pending packets, updates RTT, derives delivery-rate
samples from ACKed data packets, and performs packet-threshold and
time-threshold loss detection.
Treating every packet below an ACK frontier as lost is prohibited. A pending
packet is declared lost only when all of the following hold:

```
packet.unacked
packet.in_flight
packet.pn < largest_acked
largest_acked - packet.pn >= packet_loss_threshold
    OR now - packet.sent_at >= max(9/8 * max(srtt, latest_rtt), granularity)
```

The initial packet threshold is 3. If a packet declared lost is later ACKed, the
path has demonstrated reordering; the sender MUST record the spurious loss and
increase the path's reordering tolerance within bounded limits. A packet number
that has already produced a confirmed loss event MUST NOT be declared lost or
charged to congestion control again while it remains in the outstanding packet
window. Repeated loss accounting for the same packet number is prohibited
because it converts ordinary reordering into artificial congestion collapse. On
timeout, the sender uses PTO-style probes rather than treating every timeout as
confirmed loss. PTO expiration sends one or two ACK-eliciting probe packets and
MUST NOT mark old packets lost solely because the PTO fired.

PTO is a per-direction path timer, not one independent repeating timer per
outstanding packet. The sender MAY use per-packet deadlines to select useful
probe payloads, but it MUST gate timeout probes through a single path-level PTO
deadline. After a PTO probe batch is sent, additional timeout probes for that
direction MUST wait until the next PTO deadline. PTO backoff is exponential and
ACK progress resets the backoff. This prevents a large outstanding window from
turning into a probe storm where every expired packet emits fresh recovery
traffic while the path is already not producing feedback.

Recovery retransmits payload, not old packets. A sender MUST NOT replay a stale
encrypted UDP datagram with the old packet number as its normal loss-recovery
mechanism. When a packet is declared lost, the sender releases that packet's
pending-byte and bytes-in-flight ownership, records the packet number for
spurious-loss detection, and schedules the recoverable payload in a fresh UDP
packet with a fresh packet number and AEAD nonce. If the old packet is later
ACKed, the ACK is reordering evidence and MUST NOT release ownership a second
time. PTO probes also use fresh packet numbers; they do not mark the original
packet lost and they do not mutate the original packet's packet number.

This rule follows from the reason packet numbers exist. Packet-number spaces
measure delivery of packets, while stream offsets and carrier frame identifiers
identify recoverable content. Replaying old ciphertext keeps stale packet
numbers alive in the loss detector, hides whether the probe itself arrived, and
can repeatedly charge the same bytes to congestion control. Fresh-number
recovery lets the peer ACK the recovery attempt independently while product
stream offsets continue to deduplicate payload correctness.

ACK ranges are used because burst loss and reordering are common on real
Internet paths. A single cumulative ACK would hide sparse delivery and delay
repair. PTO-style timeout probes are separated from confirmed loss because
timeouts can also mean ACK delay, path jitter, CPU scheduling delay, or
application-limited behavior; collapsing the model on every timeout is too
conservative for fluent browsing.

### 11.4 Fragmentation and Stream Packetization

The safe startup target datagram size is 1200 bytes. The maximum probed datagram
size is 1400 bytes. The fragment payload limit is:

```
target_datagram_bytes - UDP_HEADER_LEN(18) - AEAD_TAG_LEN(16) - fragment_prefix(25)
```

For latency-sensitive reliable stream data over UDP, the implementation MUST
choose product `STREAM_DATA` payload sizes that fit one safe UDP carrier packet
after product frame overhead. This keeps short requests, SSH-like interaction,
tail repair, and control-adjacent stream data visible at fine stream-offset
granularity.

For sustained throughput lanes on a healthy UDP carrier, the sender MAY use a
larger product stream quantum and fragment that product frame across multiple
UDP carrier packets. The carrier packets remain independently numbered,
ACKed, paced, and recovered; the larger product frame is only a CPU and
scheduler amortization unit. The throughput quantum MUST be adaptive: it is
bounded by flow-control credit, sender queue budget, the configured relay
envelope, receiver reorder tolerance, live loss/jitter/queue pressure, and the
current carrier inflight model. If loss, reordering, queue pressure, or tail
latency rises, the sender MUST shrink back toward the safe single-packet stream
quantum. Control frames, ACKs, repair for latency/tail holes, and realtime
datagrams MUST remain able to interleave before ordinary bulk fragments.

PMTU probes MAY raise the packet target when ACKed; loss MAY reduce it. A PMTU
change MUST remain path-local.

The 1200-byte startup size follows modern UDP transport practice and is safe
across ordinary Internet paths. The 1400-byte probe ceiling targets common
Ethernet/VPS paths while leaving IP/UDP and tunnel overhead margin. The
single-packet rule is kept for latency-sensitive data because hiding a short
request behind multiple UDP fragments would make one lost fragment stall the
whole user-visible operation. Bulk data has the opposite pressure: forcing every
stream quantum to be one packet can burn CPU, syscalls, queue operations, and
AEAD setup before the path reaches capacity. The adaptive bulk quantum follows
the same rationale as BBR send quanta and QUIC stream packetization: packet
recovery remains at the transport layer, while stream writes become large
enough to amortize userspace overhead only when the path has evidence that this
does not harm latency or loss recovery.

### 11.5 UDP Controller

Each UDP path maintains an independent sender-side controller per direction.
The client-to-server controller and server-to-client controller MUST NOT share
bytes-in-flight, delivery-rate, loss, or PTO state because forward and reverse
congestion can differ. Each controller maintains:

* direction;
* smoothed RTT, minimum RTT, and RTT variance;
* delivery rate;
* pacing rate;
* send quantum;
* bytes in flight;
* inflight high watermark;
* next send time;
* target datagram bytes;
* PMTU acknowledged bytes;
* loss-event count;
* PTO count;
* application-limited state.

The controller is mandatory for reliable and bulk UDP data. Reliable or bulk
UDP data MUST NOT be sent when `bytes_in_flight + packet_size` exceeds the
current inflight limit, except for PTO probes or an explicit recovery allowance.
The controller MUST pace sends and enforce pending-byte capacity for data
packets. It MUST derive delivery-rate samples from ACKed data packets using
valid delivered-data timing. In protocol version 1, valid
non-application-limited data samples MAY raise the delivery-rate model when
they exceed the current model, subject to the startup growth bound below.
Lower samples MUST be recorded for diagnostics, admission confidence, and
future controller revisions, but they MUST NOT directly lower the bulk pacing
rate. A conforming v1 implementation instead responds to poor delivery by
reducing inflight allowance, PMTU probe state, repair admission, path
confidence, or striping admission. This rule is intentional: experiments showed
that scalar delivery-rate decay from isolated low epochs creates a
self-reinforcing tiny-rate loop under ordinary single-path UDP traffic. A future
revision MAY specify a lower-rate estimator only if it is stable under sustained
full-pipe traffic, ACK compression, repair traffic, and heterogeneous path
reordering. Application-limited samples MAY raise the estimate when they exceed
the current model, but they MUST NOT reduce it. Pure-control samples and delayed
feedback-only releases MUST NOT change the bulk bandwidth estimate. Control and
ACK-only packets may update liveness, but they are not evidence of bulk data
throughput.

The local Auto path model MUST consume sender-controller telemetry from each
active UDP carrier connection. At minimum, the scheduling-visible path snapshot
uses the controller's ACK-corrected smoothed RTT, RTT variance, delivery-rate
model, bytes in flight, pending queue bytes, inflight high watermark, and
application-limited state. The carrier inflight high watermark is authoritative
when present; a higher default BDP guess MUST NOT override it. A carrier
delivery-rate value becomes bulk throughput evidence only after the controller
has accepted at least one ACK-derived data delivery sample. RTT-only carrier
telemetry may improve latency scoring, but it MUST NOT promote an unknown path
into ordinary bulk striping or repair.

Delivery-rate sampling MUST be resistant to ACK compression. A controller does
not calculate bandwidth from `acked_bytes / time_since_previous_ack` alone.
For a sample epoch, it accumulates ACKed data bytes and computes the sampling
interval as the larger of ACK elapsed time and send elapsed time for the packets
covered by that epoch. The epoch MUST span a meaningful fraction of the current
minimum-RTT model before it can raise the delivery-rate estimate. If many ACK
ranges arrive back-to-back for packets that were sent over a longer interval,
the send elapsed time dominates; if packets were sent in a tight burst and ACKed
in a tight burst, the sample is held until enough ACK time has elapsed. This is
the same reason BBR uses delivery-rate samples rather than raw ACK arrival
spacing.

Startup growth is aggressive but bounded. A valid non-application-limited sample
MAY raise the delivery-rate model, but a single sample epoch MUST NOT raise it
by more than the controller's startup pacing gain. The inflight high watermark
MUST grow from ACKed data delivery and MUST be capped by the delivery-rate
model, observed minimum RTT, active bytes in flight, and the configured pending
budget. Pacing rate MUST be derived from the delivery-rate model; it MUST NOT be
raised merely because an already-inflated inflight watermark implies a larger
BDP. Otherwise ACK compression can falsely inflate `inflight_hi`, which then
inflates pacing, which creates loss and repair churn without increasing useful
goodput. Conversely, pacing rate MUST NOT be lowered by isolated low-rate
samples in v1; the controller should prefer temporary inflight and admission
pressure over reducing the sender to a rate that cannot refill the path.

Confirmed packet loss is congestion and repair evidence, but it is not by
itself a lower bottleneck-bandwidth sample. Loss response MAY reduce inflight
allowance, PMTU probe state, and repair admission according to the measured lost
byte fraction; it MUST NOT reduce the delivery-rate estimate unless ACK-derived
data delivery produces a valid lower model through a specified controller
revision. This follows the BBR-style separation between bandwidth estimation
and inflight control and prevents random wireless/VPS loss from collapsing
pacing rate under otherwise continuing delivery.

The controller algorithm is replaceable only behind the conformance boundary
defined by this section. A change that alters UDP packet encoding, product-frame
semantics, authentication, replay protection, or scheduling-visible behavior
requires a revision of this specification.

The controller follows the same principles as QUIC loss recovery and BBR-style
control: pace by a path model, update the model from delivery samples, bound
inflight data, and distinguish confirmed loss from probe timeout. The goal is
aggressive fluency, not passive safety. When links are unstable, the controller
MAY spend a small amount of extra probe or duplicate traffic if the expected
latency/failover benefit justifies it.

Startup is part of the controller, not a lab constant. A UDP sender MUST NOT
use the full pending-byte, stream-window, repair-cache, or production memory
budget as the initial congestion flight. The initial reliable-data flight is
derived from the safe datagram payload size and a bounded fraction of available
sender budget, then grows from ACKed data delivery. This is the same reason QUIC
starts with a bounded initial congestion window and BBR enters Startup by
probing from measured delivery rather than by dumping the whole application
buffer into the network. Aggressive probing is allowed, but startup must remain
paced and ACK-grown so ordinary 1% loss paths do not begin with self-inflicted
queue loss and long ordered-stream repair tails.

### 11.6 NAT Rebinding and Roaming

The UDP receiver updates the peer socket address from the authenticated packet
source after successful packet processing. Therefore client IP changes caused by
NAT rebinding, CGNAT, Wi-Fi changes, or mobile roaming MUST NOT immediately
terminate logical streams. A server MUST continue accepting authenticated packets
with the same connection ID and valid direction/packet number from a new source
address, subject to replay and freshness checks.

Client address changes are routine with CGNAT, Wi-Fi handoff, mobile networks,
and VPS load balancers. Binding a logical session to one UDP five-tuple would
unnecessarily terminate streams that can otherwise be validated
cryptographically.

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
evidence MAY carry a bounded unique next `STREAM_DATA` proof only while no other
candidate has sender-side evidence and the proof would not jump over lower
offsets already owned by another path. Once any path has sender-side evidence,
an unproven validation path MUST NOT carry new later stream offsets; it is used
only for duplicate stream data, repair data for an already-missing range, or
carrier/control probe traffic. Liveness from the open itself is not delivery
evidence. A receiver MUST NOT promote a Validation or Repair attachment to the
Active data slot merely because one frame arrived in order. For bulk streams,
receiver-side Active promotion is allowed only after delivered application bytes
have been accounted into the path model and the path has local delivery samples
or ACK-derived carrier data samples. Configured hints, successful opens, control
probes, RTT-only liveness, and single duplicated stream ranges do not satisfy
this requirement.

The server maps a repeated stream ID to the same outbound TCP connection when
reattaching after path migration or repair.

Stream IDs are stable logical identifiers, not carrier connection identifiers.
Reattaching the same stream ID is what lets mptunnel repair over a survivor path
without forcing the application to reconnect.

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
advertise enough window for high-BDP bulk transfer. The window is a capacity
envelope; the scheduler decides how aggressively to fill it from live path
state.

### 13.5 Stream Close

`STREAM_FIN` carries the final offset. A receiver MUST deliver FIN only after all
bytes below final offset have been delivered. `STREAM_RESET` aborts a stream.
`STREAM_DETACH` detaches one path instance without closing the logical stream.

FIN, RESET, and DETACH represent different failure domains. FIN completes the
byte stream, RESET aborts the logical stream, and DETACH removes only one
carrier attachment. Separating them prevents a path failure from unnecessarily
killing the application connection.

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
and schedule only those unacknowledged ranges for repair when the active carrier
does not already own reliable packet recovery for that data. For ordinary
reliable streams over the UDP carrier, an ACK gap by itself MUST NOT trigger
product-level `STREAM_DATA` reinjection, because the UDP carrier is already
recovering lost packet payloads with packet numbers, ACK ranges, threshold loss,
and PTO. Product-level reinjection over UDP is reserved for explicit path
failure, active stall, migration, or multipath repair where carrier ownership of
the original flight is no longer sufficient. A fresh ACK gap below the largest
carried end offset is evidence of a possible receive hole, not by itself proof
that product-level repair should race the UDP carrier. When a reliable stream
has more than one attached path and a repair-authoritative `STREAM_ACK` exposes
the same first missing offset beyond the stream progress interval derived from
path RTT, jitter, lane, and stall state, that persistent hole is a multipath
repair signal even if the hole's upper bound grows as later bytes arrive. The
sender SHOULD reinject only the missing cached ranges on an eligible alternative
path, avoiding the path that last carried the missing range when an alternative
exists, and MUST rate-limit repeated reinjection of the same first missing
offset by the same progress interval. This is the product-layer equivalent of
MPTCP reinjection. It does not weaken UDP carrier recovery; it prevents one slow
or lossy UDP carrier from holding the only copy of an ordered stream byte while
other survivor paths are usable. On path failure, the sender repairs only
unacknowledged bytes last sent on the failed or suspect path. A sender MUST NOT
retransmit acknowledged ranges and MUST NOT replay the entire repair cache after
reattach.

When a tail-stall repair timer fires, a sender MUST first inspect the most
recent repair-authoritative `STREAM_ACK`. If that ACK proves an unacknowledged
gap below its largest end offset, the repair extent is that gap, not bytes after
the ACK frontier. Bytes after the largest ACKed end are eligible for tail repair
only when no authoritative lower gap is known. This keeps receive-hole repair
ahead of continuation replay and matches the same ordering principle used by
QUIC ACK ranges and MPTCP reinjection: recover the earliest missing logical
offset before spending repair budget on later stream bytes.

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

Datagrams are unordered. TTL controls retry and scheduling freshness. A path
whose ETA cannot fit the TTL SHOULD be avoided. `DGRAM_FEEDBACK` acknowledges
received datagram ID ranges and feeds RTT/loss/delivery-rate observations into
path models.

`DGRAM_CLOSE` closes a flow. A closed flow MUST release scheduler load and
delivery statistics.

UDP targets prefer UDP underlay. When no UDP path exists or a UDP carrier error
is retryable and TCP paths exist, a client MAY relay datagram flow frames over
TCP underlay. This is best-effort and may suffer TCP head-of-line blocking.

UDP targets need unordered, freshness-aware delivery. Reliable streams are the
wrong abstraction because old datagrams should expire rather than block later
datagrams. UDP-over-TCP remains supported because the product model requires any
ingress by any underlay, but the scheduler prefers UDP underlay whenever it is
healthy.

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
estimate. Probe
responses, ACK-derived carrier data samples, datagram feedback, and other
data-plane observations are the inputs that raise path-model confidence.

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
endpoint-only cohort, the implementation may keep an unmeasured latency-started
stream on its current path until controlled validation succeeds, avoiding
needless spraying across equally unknown subflows. In a mixed TCP/UDP cohort,
latency-sensitive work on one underlay is pressure that can make another
suitable underlay preferable for throughput validation. This rule preserves
startup progress without confusing liveness with throughput confidence.
When no path in a same-stream bulk cohort has delivery evidence and no path is
already carrying bulk work for that stream, the ordinary striping cohort is
limited to the single best candidate. Additional unknown candidates belong to
validation, not to ordinary data striping. This prevents an all-unknown
endpoint-only startup from becoming fake aggregation before the sender has
observed actual delivery.

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
lead or join a bulk cohort when its bandwidth and queue state offset its
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
data. `STREAM_FIN`, `STREAM_RESET`, `STREAM_DETACH`, stream credit, and product
control frames therefore bypass saturated throughput queues even when the
stream had previously been promoted to throughput demand. This rule prevents a
bulk data queue from delaying stream teardown, flow-control release, or repair
state transitions.

### 17.5 Mixed TCP and UDP Underlay

TCP and UDP underlays are optimized separately and may also be used together.
Mixed-carrier reliable streams MUST avoid blind TCP+UDP striping without
evidence. Auto MAY move a stream between carriers or attach repair paths when
live measurements show benefit. UDP datagrams prefer UDP underlay, with TCP
underlay fallback only as best-effort datagram relay.

TCP and UDP can coexist, but they report very different signals. UDP exposes
packet-level recovery and should normally carry datagrams and high-performance
reliable data when healthy. TCP carries traffic when UDP is unavailable,
blocked, or worse by measurement. Mixed scheduling is therefore evidence-driven
rather than protocol-prejudiced.

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
that one endpoint's outbound samples prove the reverse direction.

### 17.6 Unified Sender Service

Each product flow is governed by exactly one sender-service ownership boundary
between the stream/datagram layer and the carrier writers. The sender service
is an abstract protocol role, not necessarily a dedicated asynchronous task:
an implementation may realize it as an immediate-mode relay service, a
switchable output binding, or a packet-sender loop, provided that one owner
decides when product work is ready to leave on a path. Fixed single-path flows
are the degenerate case of this model. The sender service consumes stream
bytes, datagram payloads, ACK/control frames, repair work, path model
snapshots, flow-control credit, and carrier availability, then emits carrier
writes that respect lane priority, per-flow fairness, path admission, and
carrier pacing. Product frames MUST NOT bypass this boundary merely because
they originate from a read loop, ACK handler, repair trigger, or path reattach
handler.

A server response sender is subject to the same rule as the client sender. A
server-to-client `STREAM_DATA` frame MUST enter the sender-service boundary
before reaching a TCP writer or UDP controller. Diagnostics-enabled
implementations MUST emit a sender decision event for every server response
`STREAM_DATA` write so lab runs can assert that response bytes did not bypass
the measured scheduling path.

The service maintains separate logical lanes in this priority order:

1. carrier ACK-only feedback;
2. product control, stream ACKs, connection credit, FIN, RESET, and DETACH;
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
a UDP path already serving realtime or latency-sensitive work receives an
additional lane-protection cost before it is chosen for a reliable latency
stream. This is not a manual mode or fixed traffic class: it is a path-model
penalty that can be overcome by a materially better UDP path. The intent is to
preserve QUIC/BBR-style lane isolation while still allowing a low-RTT/high-rate
QUIC UDP carrier to become the initial lead when it is genuinely the best
completion candidate.

The DRR service quantum for throughput data is capped at the preemptible bulk
quantum ceiling, currently 64 KiB, and is independent from the 512 KiB TCP
read-buffer ceiling. Larger local reads may be split into multiple service
quanta; batching or vectored writes may reduce syscall overhead, but they MUST
NOT remove lane preemption points.

Gap repair is often encoded as `STREAM_DATA` because it carries the same stream
offset bytes as original transmission. Its scheduling lane is nevertheless
repair priority, not the original stream's throughput lane. Implementations MUST
NOT leave repair `STREAM_DATA` behind already-enqueued ordinary bulk data on the
same path when a receiver has an active ordering hole. Repair generation itself
is also preemptible: one ACK gap, path failure, or stall event MUST NOT emit an
unbounded set of cached chunks. It emits at most the adaptive repair quantum,
normally an MSS-to-latency-quantum-sized byte range, and later progress or stall
events may emit subsequent ranges.

The sender service separates send quantum from send rate. This distinction is
essential for user-space encrypted proxying: very small bulk frames can consume
CPU, syscalls, wakeups, and AEAD setup before the path reaches capacity. The
service therefore uses BBR-style send-quantum reasoning. A throughput quantum is
large enough to amortize processing cost on a healthy path, bounded by the
configured relay envelope, and reduced only when the path model shows actual
instability or queue pressure. Pacing, inflight, and flow-control gates still
bound how much data may be outstanding.

Throughput quanta are preemption points. A carrier writer MUST NOT treat one
large product frame as a non-preemptible unit when that frame expands into many
carrier packets. After sending one controller-derived packet run, the writer
MUST give newly queued control, ACK, repair, latency, realtime, and other-stream
work a chance to run before continuing the remaining fragments of the same bulk
frame. Requeued fragments for the same stream retain their original frame ID and
stream order, so later frames from that same stream do not overtake them. This
keeps the QUIC-style stream scheduler invariant: packet recovery remains
per-packet, while application stream fairness is decided at packet-run
boundaries rather than only at whole-frame boundaries.

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

UDP segmentation offload is allowed only as a carrier submission optimization.
When the platform supports a safe primitive such as Linux `UDP_SEGMENT`, a
sender MAY hand a run of already-due UDP carrier packets to the kernel as one
segmented write. Each segment is still exactly one encoded carrier packet with
its own packet number, AEAD nonce, ACK-eliciting property, pending record, loss
state, and PTO state. The receiver observes ordinary UDP carrier packets; the
protocol does not define a larger wire datagram or a different reliability mode.
Segmentation MUST NOT be used for carrier ACK-only feedback, MUST stop at
controller pacing or inflight gates, and MUST fall back to ordinary datagram
writes with identical wire semantics when the operating system or path does not
support it.

The sender service owns queued-but-not-sent product bytes. The stream repair
cache owns unacknowledged stream ranges. The path flight ledger owns the mapping
from stream ranges to the last path that carried them. The receiver flow-control
state owns advertised stream and connection credit. A UDP controller owns
carrier packet bytes in flight, cwnd or inflight-high state, pacing state, PTO
state, and ACK-derived delivery samples. TCP path state owns encrypted frame
write pressure and path-level inflight accounting. An implementation MUST NOT
count the same byte as free in more than one owner, and MUST release ownership
only from the corresponding ACK, loss, failure, expiry, or local-delivery event.

Before a product data frame is emitted, all applicable gates must pass:

* stream or datagram freshness and target policy allow the frame;
* stream and connection flow-control credit allow the byte range;
* sender queue budget and repair-cache budget allow retained state;
* the selected path is healthy enough for the lane;
* the selected path passes ETA/admission checks for the frame;
* the carrier controller or TCP path write budget allows the packet or frame,
  except for the explicit ACK-only and PTO exceptions in this specification.

For reliable bulk streams, the sender service also performs admission before it
pulls another source byte range into a `STREAM_DATA` frame when the next offset
and candidate service quantum are known. If the path-flight ledger shows that
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

For bulk reliable streams, the scheduler first chooses a lead data path for the
next preemptible quantum from live ETA, flow sharing, health, and capability
state. Additional paths attached to the same stream are not automatically
ordinary data paths. Their role decides what the scheduler may do: Repair paths
carry gap-targeted repair or failover repair, Validation paths may receive
bounded proof traffic, and the lead path may carry ordinary data. A path with
any role may carry a specific repair frame when it is the best survivor and
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
open, but a Validation attachment can receive a bounded amount of admitted proof
traffic. Validation attachment is triggered by bulk demand and path admission;
it MUST NOT depend on the sender having outbound repair bytes. This matters for
ordinary downloads, where the client may have little or no outbound data after
the request while the server-to-client stream is clearly bulk. If validation
traffic yields delivery evidence, the path can compete in the ordinary
ECF/BLEST cohort. If it does not, the path remains excluded except for failover
or explicit repair.

Validation admission is evaluated with the bounded proof payload, not with the
full product path inflight envelope. This keeps discovery aggressive enough to
learn new paths while preventing validation churn from consuming the same budget
as established bulk data.

Mixed TCP+UDP validation MUST be carrier-diverse. When an admitted validation
cohort contains both TCP and UDP underlays, the sender MUST attempt at least the
best UDP validation candidate and the best TCP validation candidate before
spending later validation attempts on additional same-carrier candidates. In
practice, the best UDP validation candidate is attempted first because UDP
carrier ACKs provide path-scoped sender proof, while TCP validation only proves
attach/control liveness in version 1. This ordering does not make UDP a
fallback or a fixed traffic mode; it prevents a serial TCP open timeout from
blocking the independent UDP proof track during a bulk rebalance cycle.

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
credit as ordinary bulk capacity. However, when no sender-evidence candidate
exists and a UDP validation path wins the same ECF/BLEST lead admission check
for the next preemptible quantum without jumping over lower offsets owned
elsewhere, the UDP validation path MAY carry the unique next `STREAM_DATA`
quantum as a bounded primary probe. This exception is deliberately limited to
the validation credit envelope and to UDP underlays, where the carrier can
provide packet/path-scoped ACK-derived proof. It prevents a stale startup path
from owning the lower frontier before a better carrier has a chance to prove
itself. If the validation path does not win lead admission, the sender either
duplicates the same `STREAM_DATA` on an admitted ordinary path and the
validation path, sends repair for an already-missing range, or sends
carrier/control probes that do not create a new application-data dependency.
This follows QUIC path validation and MPTCP reinjection practice while adapting
it to a product-layer stream that must avoid creating irreversible receive-hole
debt.

A stream ACK for duplicated data proves end-to-end byte delivery but does not
identify which underlay path delivered the bytes. It therefore releases product
flight for every duplicate copy of that range, but it MUST NOT by itself promote
the validation path into ordinary same-stream bulk service. By contrast, a
stream ACK for non-duplicated data that was sent as an admitted UDP validation
lead is path-scoped for that quantum and MAY become sender-side stream delivery
evidence if the receiver had no lower receive hole that could have polluted the
sample. In protocol version 1, ordered-stream validation payload is used only
for UDP underlays before path-scoped sender evidence exists, because the
sender's local UDP carrier ACK metrics can provide the stronger proof needed
for sustained promotion. TCP validation uses attach/control liveness and peer
hints only; it MUST NOT spend ordered-stream payload for promotion unless a
future path-scoped TCP proof signal is specified.

Response-side validation uses the same principle. The server MUST NOT schedule
download bytes onto a validation path merely from generic TCP or UDP defaults,
but it MAY let a UDP validation path lead a bounded proof quantum only before
any sender-evidence candidate exists and when ECF/BLEST admission says that
waiting on or continuing the current ordinary path would be worse.
Client-supplied `PATH_METRICS` are hints, not final proof of response-direction
throughput. They are useful to distinguish a plausible
high-bandwidth path from a poor or high-loss path before bounded proof is sent,
but sender-side evidence decides sustained ordinary promotion. The receiver
applies the same rule when it observes incoming stream data: ordered progress on
a validation or repair path may refresh liveness and may feed delivery sampling,
but the path becomes an ordinary lead candidate only after that sampling has
created real delivery evidence and ETA scoring says it should displace the
current lead path. This prevents a high-RTT, high-loss, or reordered path from
winning ordinary bulk service because it delivered a small probe before its
long-term behavior was known.

For UDP underlays, the response sender also maintains local carrier TX metrics
from its own UDP packet controller. Once the server has ACK-derived carrier
delivery samples for a UDP path, those sender-side metrics take precedence for
response scheduling over peer hints and over ordered stream-ACK timing alone.
Stream ACKs still release product flight and prove end-to-end stream progress,
but they MUST NOT be the only source of per-path delivery rate in a same-stream
multipath transfer because ordered-stream holes from another path can pollute
that signal. This mirrors QUIC and BBR practice: congestion and pacing decisions
are sender-side and packet/path scoped, while stream ordering is a separate
correctness layer.

When the UDP production engine is QUIC, the response sender MUST preserve both
ACK-derived delivery rate and QUIC pacing/cwnd-derived pacing rate in its path
snapshot. Application-limited ACK samples MUST NOT initialize or reduce the
bulk delivery-rate model to a tiny value. Until a non-application-limited data
sample exists, the scheduler MAY use the QUIC pacing/cwnd rate or the normal
UDP startup model for bounded admission, but it MUST keep the app-limited
provenance visible to diagnostics and admission.

For any same-stream bulk striping, the scheduler chooses eligible paths from
live ETA. Eligibility requires active or sufficiently confident suspect state,
no probe-only/backup restriction unless necessary, acceptable inflight/queue
pressure, and explicit admission against the best next path. A path MUST NOT
join a bulk striping cohort merely because it has available capacity.

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

The lead path is a safe baseline, not merely the lowest raw ETA. A candidate
whose carrier or product debt already violates the active data-path admission
gate MUST NOT be used as the baseline that rejects other paths. Otherwise a
saturated path can prevent a proven alternate from carrying traffic while also
being unable to accept the next quantum itself. This rule is the sender-service
equivalent of ECF/BLEST comparing against the best usable subflow rather than
against an unavailable one.

`carrier_debt` is the sender-visible network backlog: carrier bytes in flight,
carrier queue bytes, and locally queued carrier commands that are ahead of the
candidate chunk. `product_reorder_debt` is the stream-level byte ownership that
has not yet been released by `STREAM_ACK`. These are deliberately different
ledgers. QUIC and BBR gate packet emission and pacing on carrier debt, while
MPTCP-style sequence repair and receive-window protection reason about product
byte ownership. `product_queue_debt` is the lead path's bounded,
preemptible product work already admitted to the transport. An implementation
MUST NOT use slow product-ACK release timing as the UDP carrier congestion
window, MUST NOT use carrier ACK progress as proof that a stream byte is no
longer needed for repair, and MUST NOT treat the configured product envelope as
a floor above UDP carrier credit. The UDP controller limits packet emission and
provides an upper gate for active UDP product admission; the lead product
scheduler keeps only enough bounded, preemptible work queued for that controller
to stay ACK-clocked.

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
lab diagnostics showed that path hopping inside one ordered stream can create
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
duplicate validation, and path events. Ordinary data resumes when ACK progress,
repair delivery, detach/failover, or updated path evidence produces a
serviceable lower-frontier owner or advances the contiguous frontier. This
rule closes the MPTCP-style failure mode where a slow or failed subflow owns
early data and all later high-rate data either blocks behind it or deepens the
receive hole.

Lead-path admission and lead-path repair are intentionally separate
decisions. The lead path may keep a larger product queue than additional
paths so that a UDP controller or TCP writer is not starved by slow product
ACKs. Version 1 does not perform proactive active ordering-debt repair merely
because lower-offset bytes are outstanding on another path. That condition is
handled first by admission: the sender must avoid creating or expanding an
ordered receive hole when the modeled completion cost is worse than waiting for
the path that owns the lower bytes.

Repair is triggered by explicit evidence: a complete `STREAM_ACK` that exposes
a gap, a path failure or detach event, or data-plane PTO/stall evidence. The
repair extent is the missing or suspect unacknowledged byte range indicated by
that event, not every cached chunk below the frontier. When another eligible
path exists, repair prefers a path that did not carry the last outstanding copy
of the repaired range. This is the MPTCP reinjection rule applied only after
loss, failure, or stall evidence exists, with the QUIC-style recovery
constraint that a repair action is small, ACK-clocked, and never a replay of
unrelated cached bytes. A sender MUST NOT duplicate every lower outstanding byte
merely because a faster active path is available. Lab evidence showed that
immediate or stale-gated speculative response-side reinjection can reduce
download goodput by occupying send queues before the receiver has proven a gap.

A future protocol revision may add proactive lead ordering-debt repair only
if it is implemented inside the unified nonblocking sender service and
diagnostics prove that it cannot delay ordinary response transmission, carrier
ACK feedback, or explicit gap repair. Until then, active ordering debt is an
admission input and explicit repair signal, not a standalone repair trigger.

Repair `STREAM_DATA` is still stream data for correctness and flow accounting,
but its service lane is repair/latency, not ordinary bulk. It therefore uses
the priority product queue and may interleave ahead of already queued bulk
frames. This lane override changes queue service, not stream semantics: the
receiver still accepts the data by stream offset and discards duplicates after
the corresponding range is ACKed or delivered. An implementation MUST NOT allow
a repair frame to wait behind the bulk queue for the same path merely because
the parent stream has been classified as throughput.

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
admission failure unless the result is explained by external non-repeatable lab
noise and confirmed by rerun.

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

Data-plane repair progress is also delivery evidence, but only for failover
admission. When a tail-stall or path-failure repair frame is sent on an
alternate path and the next `STREAM_ACK` advances the contiguous ACK frontier or
releases bytes that were still in the repair cache, the sender MAY mark that
repair path as locally delivered and promote it to the active lifecycle slot for
future admission. This does not derive a bandwidth estimate from ACK-release
timing and does not convert duplicated validation ACKs into high-rate evidence.
It is the product-layer equivalent of MPTCP reinjection over a surviving subflow
and QUIC PTO recovery: progress on a survivor path should detach active work
from the stalled path before heartbeat liveness timers expire. If the repair
does not advance the stream frontier, no promotion occurs.

For TCP, the configured path inflight limit is a product-queue resource ceiling
because kernel TCP still owns congestion control inside that stream. For UDP,
mptunnel owns congestion control at the carrier packet sender, not at a hidden
product queue. In both cases, lead and same-underlay product admission is derived
from live BDP, path inflight evidence when it is smaller, the next quantum size,
and the configured resource ceiling. The configured ceiling MUST NOT become the
lead path's scheduling target merely because the path is attached or active.
Actual encrypted-packet emission remains gated by the sender-side UDP controller
or kernel TCP. This matches QUIC and BBR practice: the stream scheduler may have
ready data, while the packet sender paces and gates network flight.
Cross-underlay ordinary striping is stricter: it also accounts for confidence and
receiver reorder budget because TCP and UDP expose different loss, pacing, and
head-of-line behavior.

Additional paths do not all receive the same treatment. An additional path using
the same underlay family as the lead path may use the unscaled ACK-clocked BDP
reorder budget, because the sender has comparable carrier semantics and the
earlier pure-UDP experiments prove that same-carrier aggregation can be both
fast and stable. An additional path crossing underlay families, such as TCP
lead to UDP additional or the reverse, uses the stricter confidence-scaled
budget until sender-side evidence proves that it will not create harmful
ordered-stream debt.

When no active, evidenced, or validation candidate passes admission, the sender
keeps the frame queued and wakes the scheduler when stream ACKs release product
flight bytes, when path metrics change, or when attachment state changes. This
is backpressure, not a liveness failure: control, carrier ACK, stream ACK, and
repair lanes remain separately prioritized.

The same rule applies before reading another local or target-side bulk segment
into the product stream. A blocked admission result is final for that service
turn: the implementation MUST NOT fall through to the current active path or a
round-robin path after declaring that no safe candidate exists. The next attempt
is made only after a normal wake event such as stream ACK release, path metric
refresh, attachment change, repair progress, or a short scheduler retry.

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

The version 1 same-stream bulk cohort uses a completion horizon rather than a
fixed millisecond slack:

```
path_rate = max(path.pacing_rate, path.delivery_rate)
path_bdp = path_rate * path.srtt
lead_path = min_eta_candidate_that_is_eligible_for_ordinary_bulk()
if path is the lead path:
    product_inflight_limit = min(path.carrier_inflight_limit if known else infinity,
                                 2 * path_bdp,
                                 configured_path_inflight)
    product_inflight_limit = max(product_inflight_limit, chunk.len)
else if path uses the same underlay family as the lead path:
    product_inflight_limit = min(path.carrier_inflight_limit if known else infinity,
                                 2 * path_bdp,
                                 configured_path_inflight)
    product_inflight_limit = max(product_inflight_limit, chunk.len)
else:
    modeled_inflight = max(min(path.carrier_inflight_limit if known else infinity,
                               2 * path_bdp),
                           chunk.len)
    product_inflight_limit = min(modeled_inflight,
                                 max(configured_path_inflight, chunk.len))
base_reorder_budget = min(max(2 * path_bdp, chunk.len),
                          configured_receiver_reorder)
effective_reorder_budget = base_reorder_budget * path.confidence
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
traffic modes. They apply only to additional cross-underlay ordinary striping,
where the scheduler must prove that a path with different transport semantics
will not increase completion time. Lead and same-underlay product queues use a
BDP/inflight-derived envelope capped by the configured resource ceiling, while
carrier controllers still enforce network flight. This follows BBR's separation
between ready application data and paced network inflight, while preserving the
ECF/BLEST rule that heterogeneous paths must not create avoidable head-of-line
blocking.

The candidate may pass the ETA gate only when it can arrive before this
completion horizon. This is deliberately different from both a narrow
near-best-ETA cohort and an unbounded all-path rule. A narrow ETA cohort blocks
useful high-bandwidth heterogeneous paths because it ignores how long the best
path would need to carry the same next chunk. An unbounded all-path rule can
inflate receiver reorder debt and create long ordered-stream gaps. The
completion horizon follows the MPTCP ECF/BLEST principle: a second path is useful
when it can finish useful work before the best path and the receiver's measured
reorder budget would be exhausted by waiting for that work.

The same completion-horizon logic applies to the previously attached active path
when it is no longer the lead path and continuing it would expand an existing
cross-path hole. This is necessary for long-running Auto traffic: after a path
switch, the sender must not keep sending ordinary bulk on a stale path merely
because that path was active earlier. The rule is still not a human traffic mode
or static failover threshold; it is a per-quantum ECF/BLEST admission decision
derived from ETA, stream ordering debt, and the receiver's current reorder
budget.

The configured path inflight value is a product-queue resource ceiling, not a
carrier congestion window and not an active-path scheduling target. A conforming
sender derives lead, same-underlay, and cross-underlay product inflight from the
live BDP model, path inflight evidence when present, and the next chunk size,
then caps that result by the configured path inflight ceiling. Control, ACKs,
repair, and latency frames must still interleave with any admitted bulk work.

The reorder budget is confidence scaled for additional paths. A path with fresh
ACK-derived delivery samples can use more of the modeled BDP/reorder envelope.
A path known only by startup hints or peer-supplied `PATH_METRICS` receives only
bounded validation traffic until real delivery evidence arrives. This prevents
unknown paths from being trusted as production bulk lanes while still allowing
aggressive proof traffic when the model predicts it will not increase
completion time.

Confidence scaling does not shrink the lead path's basic ACK-clocked
product-flight window below `product_inflight_limit`; otherwise a single path
would bootstrap too slowly and bulk throughput would regress. It also does not
shrink same-underlay aggregation below the unscaled BDP reorder budget, because
that turns a healthy pure-UDP or pure-TCP multipath transfer into a permanent
probe. The unscaled lead/same-underlay rule MUST NOT be applied to
cross-underlay additional paths, because that would convert a resource ceiling
into mixed-carrier reorder permission and reintroduce the all-path
below-best-single-path failure mode.

Product admission and carrier congestion control are separate gates, but they
must be consistent. UDP carrier `inflight_hi` limits encrypted UDP packet
emission inside the carrier controller and also gates how much new product work
may be admitted onto an active UDP path. The configured product limit MUST NOT be
treated as a floor over carrier credit or BDP-derived credit. The product
scheduler admits bounded, preemptible stream work; the carrier packetizer drains
that work only when cwnd, pacing, and pending-byte gates permit; the stream
repair layer separately retains product byte ranges until `STREAM_ACK` releases
them. Cross-underlay additional paths remain stricter and may be rejected when
carrier queue debt plus the next chunk exceeds the validation queue limit,
because a heterogeneous speculative path can create head-of-line debt without
improving completion time.

Per-stream striping admission MUST NOT be confused with independent-flow
fairness. A path excluded from a stream's striping cohort does not become an ordinary data
path simply because it is attached. If the previously attached active path is
no longer the best admitted path, the sender MAY explicitly move ordinary bulk
data to the better lead candidate and let delivery evidence decide whether it
should remain in the cohort. It MUST NOT silently convert a Repair path into an
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
striping cohorts.

When an active data path is detached because it closed, stalled, or was selected
as the victim of a receive-hole repair decision while a usable survivor exists,
the sender MUST cool that path down as failed for active data scheduling. It MAY
continue bounded probes or future liveness checks, but immediate active reopen
attempts MUST prefer survivor paths. Open/probe failures MAY use a softer suspect
state because they are not proof that in-flight application data stalled.

A path that moves from Failed to Suspect by cooldown expiry alone is not
considered recovered for bulk auto-discovery or active repair admission.
Recovered bulk eligibility requires a liveness, open, or delivery success that
returns the path to Active. This separates passive time from positive evidence:
cooldown expiry permits probes, while successful feedback permits ordinary
scheduling.

For active repair or reattach, if at least one Active survivor is schedulable,
the sender MUST choose Active survivors before Suspect paths. A Suspect path MAY
be used only when no Active survivor can carry the work, or as a bounded probe
that does not block the active flow.

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
solely because a lab target such as 256 MB RAM or 1 Gbps was exceeded.

Backpressure points include:

* stream flow-control offset;
* repair cache bytes;
* reorder buffer bytes;
* UDP carrier orphan fragment bytes;
* datagram queue bytes;
* TCP path inflight bytes;
* UDP carrier pending bytes;
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

## 20. Management API, Diagnostics, and Lab Instrumentation

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

The release API complements, but does not replace, lab instrumentation. It is
safe for normal operations because it exposes coarse current counters and
bounded trends. Fine-grained component timing, packet event logs, allocation
tracing, and container process statistics remain lab tooling.

### 20.2 Lab Instrumentation

Lab diagnostics are optional and intended for lab builds. Release bundles MUST
NOT include lab-only diagnosis paths unless explicitly compiled with
diagnostics.

Lab diagnostics MAY expose:

* timestamped scheduler decisions;
* path model snapshots;
* sender lane occupancy, deficit, flow ID, selected path, and rejection reason;
* UDP carrier and controller ACK/loss/PTO events, including connection identity,
  ACK ranges, ACK delay, released bytes, declared loss, spurious loss, pending
  bytes, and probe batch size;
* stream ACK and repair events, including `complete`, ACK range count,
  largest repair horizon, released bytes, repair-cache bytes before and after
  ACK application, generated repair frame count, active carrier, whether a
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
* per-component timing and byte counters;
* container CPU/RAM/network samples from external Docker tooling.

Lab experiments SHOULD follow this methodology:

1. synthesize the experiment;
2. run and record full metrics;
3. reflect on what worked and what did not;
4. make one essential protocol or implementation improvement;
5. prove effectiveness across relevant scenarios;
6. record the result and rejected attempts.

Lab conditions SHOULD include normal daily scenarios, single path, pure TCP,
pure UDP, mixed TCP/UDP, multipath, bulk download, bulk upload, web browsing,
SSH-like interaction, UDP datagrams, 2^3 bandwidth/latency/loss matrix, flapping
links, latency spikes, blackholes, QoS, and baselines such as direct, VMess,
Hysteria2, and MPTCP where available.

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
* reject counter or packet-number replay;
* validate authentication freshness;
* maintain replay protection for path joins;
* validate target ports and outbound policy support;
* avoid exposing product metadata in UDP carrier plaintext;
* treat upstream proxy authentication and local proxy authentication separately.

UUID-derived secrets are accepted for operator usability, but deployments SHOULD
use high-entropy secrets. Traffic captured today should remain impractical to
decrypt with foreseeable computation when strong secrets and modern AEAD suites
are used.

mptunnel does not attempt to hide packet sizes, timing, endpoint IPs, or all
traffic analysis signals. The UDP carrier removes protocol strings such as SNI
from internal packets, but it is not a complete anonymity system.

## 23. IANA Considerations

This document makes no IANA requests. All registries in this document are
private to mptunnel protocol version 1.

## 24. Versioning and Compatibility

Product frames use version 1 in the `MPTF` header. TCP envelopes use version 1
in the `MPTE` header. UDP carrier packets use version 1 in byte 0.

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
* RFC 9298, "Proxying UDP in HTTP", for HTTP CONNECT-UDP outbound behavior,
  https://www.rfc-editor.org/rfc/rfc9298
* Hysteria2 protocol documentation, for QUIC-based proxy transport and
  BBR-style performance motivation,
  https://v2.hysteria.network/docs/developers/Protocol/

## Appendix A. Numeric Registries

### A.1 Product Frame Kinds

See Section 9.

### A.2 UDP Carrier Payload Kinds

* 1: ACK
* 2: ordered frame fragment
* 3: close stream
* 4: unreliable unordered frame fragment
* 5: reliable unordered frame fragment

### A.3 Direction Values

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
        mark_repair_path_as_sender_evidence_for_failover()
        promote_repair_path_to_active_lifecycle_slot()
    if active_carrier_is_reliable_udp:
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
        holes = unacked_chunks_below_largest_acked_not_covered_by(ranges)
        schedule_repair(holes)
    else:
        do_not_infer_holes_from_omitted_ranges()

on_tail_stall_repair(stream_id, last_complete_ack_ranges):
    holes = unacked_chunks_below_largest_acked_not_covered_by(last_complete_ack_ranges)
    if holes is not empty:
        schedule_repair(holes)
    else:
        tail = unacked_chunks_after_largest_acked(last_complete_ack_ranges)
        schedule_repair(tail)
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

### B.5 UDP Carrier Send

```
send_udp_payload(payload, reliable, ack_only):
    packet_number = next_packet_number++
    encrypted = AEAD(packet_header, payload)
    if ack_only:
        socket.send_to(encrypted, peer)
        return
    if reliable:
        wait_until_controller_allows(packet_len)
        pending[packet_number] = recoverable_payload, encoded_len, sent_at, deadline
        controller.on_packet_sent(packet_len)
    socket.send_to(encrypted, peer)

on_confirmed_packet_loss(packet_number):
    lost = pending.remove(packet_number)
    controller.release_and_charge_loss(lost.encoded_len)
    remember_recent_declared_loss(packet_number)
    send_udp_recovery_payload(lost.recoverable_payload, fresh_packet_number)
```

### B.6 UDP Carrier Receive

```
receive_udp_packet(packet, source):
    header = parse_clear_header(packet)
    plaintext = AEAD_decrypt(header, packet.payload)
    peer = source
    if payload.ack:
        release_pending_ack_ranges(payload.ranges)
        latest_rtt = now - sent_time[payload.largest_acked]
        ack_delay = min(payload.ack_delay_us, max_ack_delay_us)
        adjusted_rtt = adjust_rtt(latest_rtt, ack_delay, min_rtt)
        update_rtt_and_delivery_rate(adjusted_rtt, acked_data_packets_only)
        for lost in detect_packet_threshold_and_time_threshold_loss():
            release_old_packet_ownership(lost.packet_number)
            requeue_payload_with_fresh_packet_number(lost.payload)
        if ack_covers_recent_declared_loss:
            record_spurious_loss_and_raise_reordering_tolerance()
    if payload.frame_fragment:
        if payload.ack_eliciting:
            queue_packet_ack(header.packet_number)
            if packet_reveals_gap_or_reordering(header.packet_number):
                flush_ack_immediately()
        reassemble_frame_fragment()
        deliver_complete_product_frame()
```

### B.7 UDP PTO

```
on_pto(path):
    if now < path.next_pto_deadline:
        return
    controller.pto_count += 1
    mark_path_suspect_for_new_bulk()
    send_one_or_two_ack_eliciting_probe_packets_with_fresh_packet_numbers()
    path.next_pto_deadline = now + backed_off_pto(controller.pto_count)
    do_not_mark_old_packets_lost_only_because_pto_fired()
    if repeated_pto_or_absolute_active_stall_budget_exceeded:
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
