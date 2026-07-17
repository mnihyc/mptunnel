# mptunnel

`mptunnel` is an encrypted multipath proxy and tunnel written in Rust. It can
carry reliable streams and datagrams over TCP, QUIC over UDP, or a mixed path
set while keeping each carrier's native congestion control and recovery.

The current wire format is Multipath Proxy Protocol (MPP) v2. Its data level owns
independent per-direction data sequence numbers, range Data ACKs, flow control,
path-neutral attachments, and cross-path reinjection. TCP and QUIC remain
separate below that layer.

## Build

```bash
cargo build --bin mptunnel
cargo test
scripts/check-line-counts.sh
```

`scripts/check-line-counts.sh` is warning-only. Architecture and development
rules are documented in:

- [`RFC.md`](RFC.md): protocol behavior and wire format
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): current owners and data flow
- [`docs/CODE_STRUCTURE.md`](docs/CODE_STRUCTURE.md): module and test rules
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md): deployment and packaging
- [`docs/LAB.md`](docs/LAB.md): performance proof methodology
- [`docs/MILESTONES.md`](docs/MILESTONES.md): accepted release milestones

## Configuration

Running `mptunnel` without arguments reads `./config.toml`. Validate a config
without opening listeners:

```bash
mptunnel --config ./config.toml --check-config
mptunnel platform
```

The TOML graph is V2Ray-style: `[[inbounds]]` accept traffic and select an
`[[outbounds]]` entry or routing balancer by tag. MPP endpoints and security
belong to `protocol = "mpp"` entries.

Minimal local proxy node:

```toml
[management]
listen = ["127.0.0.1:7600"]
token = "replace-with-management-token"
dashboard = true
allow_peer_diagnostics = true

[[inbounds]]
tag = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]
outbound = "edge-mpp"

[[outbounds]]
tag = "edge-mpp"
protocol = "mpp"
endpoints = ["tcp://203.0.113.10:443", "udp://203.0.113.10:443"]

[outbounds.security]
secret = "8f14e45f-ea9b-4e5b-9f2d-8a1c4d7e6b30"
```

Minimal edge node:

```toml
[management]
listen = ["127.0.0.1:7601"]
token = "replace-with-management-token"
dashboard = true
allow_peer_diagnostics = true

[[inbounds]]
tag = "edge-mpp"
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:443", "udp://0.0.0.0:443"]
outbound = "direct-egress"

[inbounds.security]
secret = "8f14e45f-ea9b-4e5b-9f2d-8a1c4d7e6b30"

[[outbounds]]
tag = "direct-egress"
protocol = "direct"
```

Use `mptunnel --help`, `mptunnel client --help`, and
`mptunnel server --help` for the complete CLI and environment-variable surface.

## CLI examples

Local SOCKS5 and HTTP CONNECT ingress:

```bash
mptunnel --check-config \
  --secret 8f14e45f-ea9b-4e5b-9f2d-8a1c4d7e6b30 \
  client \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://203.0.113.10:443 \
  --path udp://203.0.113.10:443
```

Server listeners with direct egress:

```bash
mptunnel --check-config \
  --secret 8f14e45f-ea9b-4e5b-9f2d-8a1c4d7e6b30 \
  server \
  --bind-path tcp://0.0.0.0:443 \
  --bind-path udp://0.0.0.0:443 \
  --outbound direct
```

Listener, path, bind, and resolver arguments accept repeated IPv4 and IPv6
addresses. Do not depend on platform-specific dual-stack socket defaults.

## Runtime model

MPP stream opens are neutral:

```text
OPEN_STREAM(stream_id, target, demand)
```

They do not assign a persistent path role. The receiver advertises a
directional, sequenced `PATH_STATUS` of `Available` or `Backup`. Scheduling
considers eligible available paths first, then backup paths, and ranks within a
set from live RTT, delivery rate, queue, flight, loss, jitter, confidence, and
current work demand.

The local `TrafficClass` is mutable queued-work demand, not a link
tag. Interactive work can remain latency-oriented while a sustained transfer
uses additional measured paths; tails, stalls, or competing realtime work can
shift the same stream back toward latency-sensitive scheduling.

Each reliable-stream direction uses one data sequence space across transport
paths; the opposite direction has an independent offset space, Data ACK state,
and receive window. `STREAM_ACK` releases acknowledged MPP ranges and
flight but grants no new offsets. `STREAM_MAX_DATA` grants offsets in that
direction but acknowledges no bytes. A native TCP ACK or QUIC packet ACK
informs its own transport controller but cannot release MPP flight.

Request attachments are fenced by the exact physical path instance plus
`attachment_id`. Response output dispatch uses the analogous physical instance
plus output incarnation and revalidates the observed response-model generation
when it reserves the carrier queue and commits the exact original flight. The
output carrying the contiguous Data Sequence frontier remains governed by the
shared MPP receive window and native carrier credit. An additional output
without durable, unambiguous Data ACK progress is limited to one bounded
startup flight; native transport ACK evidence alone does not unlock mature
additional-path placement.

When causal evidence identifies an unacknowledged range, MPP may reinject that
exact range on another eligible path within retention, flight, queue, reorder,
and cumulative extra-traffic limits. Path-failure, persistent authoritative
Data ACK gap, and bounded live-tail recovery have a cause-specific critical
exception so an exhausted cumulative budget cannot deadlock recovery. The
exception remains event-bounded, avoids overlapping queued copies, prefers or
requires a distinct live output as appropriate, and charges its bytes against
later optional reinjection.

TCP and QUIC are intentionally distinct below this boundary:

- kernel TCP owns TCP congestion control and retransmission;
- Quinn owns QUIC packet ACKs, loss detection, PTO, pacing, and congestion
  control; and
- MPP compares typed observations without sharing mutable controller state.

TCP capacity evidence uses receiver-confirmed receipts and optional telemetry
from the exact socket. QUIC capacity evidence uses fresh native packet-ACK
observations with an independent proof lifetime. MPP Data ACK is the shared
delivery authority above both and is never interchangeable with either native
proof.

MPP datagrams use flow IDs, datagram IDs, TTL, payload, and feedback ranges.
They do not enter the reliable-stream offset ledger. SOCKS5 UDP ASSOCIATE and
TUN UDP can use either transport family according to live completion cost.
Feedback confirms admission to the target worker, not end-to-end delivery.
Alternative-path retries use a new flow-local datagram ID, so datagrams retain
ordinary duplicate-delivery risk under failover.

## Path hints

Endpoint URI query parameters express either configured operator restrictions
or startup measurement priors:

- measurement priors: `srtt-ms`, `rtt-ms`, `jitter-ms`, `rate-bps`,
  `rate-kbps`, `rate-mbps`, `rate=unknown`, `rate=unlimited`
- operator restrictions: `backup`, `expensive`, `bulk-allowed`, `bulk`,
  `no-bulk`, `probe-only`, `no-udp`
- `datagram-payload-limit` for an MPP datagram allocation ceiling

The legacy `mtu`, `mtu-bytes`, and `payload-mtu` aliases mean the same MPP
allocation ceiling; they do not override Quinn's transport PMTU. Measurement
priors are not proof and live observations replace them. Operator restrictions
remain hard gates until policy changes them; they do not classify a stream or
create a permanent transport role.

## TUN and platforms

Desktop TUN mode uses `tun-rs` for packet-device I/O and `netstack-smoltcp` for
user-space TCP/UDP flow translation. Creating and configuring a TUN device
requires host privileges.

Android is a library embedding target. The host application establishes the
descriptor with `VpnService`, implements `runtime::PacketDeviceProvider` and
`transport::CarrierNetworkProvider`, and protects or binds every carrier socket
to the selected Android network so it does not re-enter the catch-all VPN.

Protocol and scheduling are portable. Exact-socket TCP telemetry adapters use
`TCP_INFO` on Linux/Android, `TCP_CONNECTION_INFO` on macOS, and
`SIO_TCP_INFO` on supported Windows versions. Each adapter exposes only native
fields that the host actually supplies; missing fields remain unknown. Native
telemetry is never required for correctness or eligibility, and an unavailable
adapter keeps startup work bounded, then uses durable Data ACK progress, shared
MPP resource windows, and socket backpressure without inventing a TCP
congestion window. It also prints an explicit performance warning. Windows
client with Linux server is a primary target, not yet a native
network-integration claim.

## Security

All transport paths are encrypted. TCP paths use the MPP record layer;
AES-256-GCM is its default cipher and ChaCha20-Poly1305 is selectable when both
peers use it. QUIC paths use TLS 1.3 and QUIC packet protection through rustls;
the `cipher` setting does not select a QUIC TLS cipher suite. The shared secret must
be a random UUID or at least 32 bytes of high-entropy text. MPP derives
domain-separated transport and authentication material from it.

Session and path authentication includes issue times and a bounded freshness
window. QUIC uses packet protection with a secret-derived certificate identity.

## Management

When `[management].listen` or `--management-listen` is configured, the release
binary exposes a versioned operational API:

- `GET /api/`
- `GET /api/health`
- `GET /api/status`
- `GET /api/paths`
- `GET /api/traffic`
- `GET /api/sessions`
- `GET /api/flows`
- `GET /api/diagnostics`
- `POST /api/control/path`
- `POST /api/diagnostics/peer`

Set `dashboard = true` or `--management-dashboard` to serve the embedded page
at `/`. Runtime API calls use `Authorization: Bearer <token>`; the page keeps a
token in session storage only. Tokens contain 16-256 visible ASCII characters.
Every management listener requires a token and
must use a loopback address. For remote access, put a same-host TLS reverse
proxy in front of it or use an SSH tunnel. Local proxy credentials are never
returned.

`allow_peer_diagnostics = true` (or
`--management-allow-peer-diagnostics`) lets an authenticated MPP peer request a
sanitized path snapshot. It is disabled by default, does not require a local
HTTP listener, never exposes endpoints or targets, and never changes scheduling.
Peer requests are manual, selectable by service/index/session, bounded to one
in-flight request per session, and rate-limited by the responder.

## Build targets

CI runs host checks on Linux, macOS, and Windows and target checks for Linux,
macOS, Windows, and an Android aarch64 library build. The Android check is not
an APK or device-runtime claim.

Local release packaging:

```bash
scripts/package-release.sh --target x86_64-unknown-linux-musl
pwsh scripts/package-release.ps1 -Target x86_64-pc-windows-msvc
```

Windows archives include the architecture-matched, checksum-verified Wintun
runtime. Packaging and Wine smoke do not replace native Wintun integration
testing. See [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

## Performance evidence

The deterministic benchmark crate and Docker lab are developer tools, not
release behavior:

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict
lab/run-heterogeneous-ablation.sh
```

Simulator gates can reject a model regression but cannot prove runtime queues,
carrier recovery, aggregation, or failover. Current release claims require
matched protocol-v2 end-to-end rows for upload/download, single/multipath,
latency/bulk, TCP/QUIC/mixed carriers, aggregation/failover, and traffic/resource
overhead. Direct, MPTCP, Hysteria2, and other applicable baselines must use the
same topology and time window.

Older iteration results in `lab/results/` are explicitly pre-v2 historical
references. They are useful regression targets but are not current performance
claims. See [`docs/LAB.md`](docs/LAB.md) for cohort and accounting rules.

The dated protocol-v2 assessment on 2026-07-16 proves basic operation,
same-family aggregation, and bounded reliable failover, but it is not a general
availability release verdict. High-RTT lossy TCP startup remains weaker than the
adjacent VMess control, one blackhole composite lost an unreliable datagram, and
no same-condition MPTCP row is present. Native macOS, Android, and Wintun
networking have not been exercised. The exact evidence and historical
executable A/B are recorded in `docs/LAB.md`.
