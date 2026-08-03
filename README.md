# MPTUNNEL

MPTUNNEL is an encrypted multipath proxy and tunnel for everyday Internet use.
It combines independent TCP and QUIC paths into one logical connection, adds
their available capacity according to live completion evidence, and keeps
established traffic alive when a carrier disappears.

It provides the daily-use surface expected from a modern proxy: SOCKS5, HTTP
CONNECT, TCP/UDP port forwarding, TUN, routing, DNS policy, outbound selection,
balancing, persistent configuration, live management, and connection
diagnostics.

## Contents

- [Why MPTUNNEL?](#why-mptunnel)
- [Performance](#performance)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Configuration and operation](#configuration-and-operation)
- [Platform support](#platform-support)
- [Security](#security)
- [Release assets](#release-assets)
- [Documentation](#documentation)

## Why MPTUNNEL?

Most proxies place a logical connection on one transport. MPTUNNEL maintains
one Multipath Proxy Protocol (MPP) sequence across independent TCP and QUIC
carriers. Native transports keep their congestion control and loss recovery;
MPP adds live path selection, exact Data ACKs, and bounded reinjection across
them. One flow can therefore aggregate links and remain attached when a
carrier disappears.

| System | Proxy | TUN | Route | DNS | TCP | QUIC | Multipath | Failover |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **MPTUNNEL** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Hysteria2 | ✓ | ✓ | ✓ | ✓ | — | ✓ | — | — |
| Xray/V2Ray | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| MPTCP | — | — | — | — | ✓ | — | ✓ | ✓ |

Live delivery—not a source IP—ranks eligible carriers. A TCP endpoint defaults
to three ordinary carrier connections; keeping a carrier ready never forces
traffic onto it.

Reference behavior is taken from the
[Hysteria client modes and TUN documentation](https://v2.hysteria.network/docs/advanced/Full-Client-Config/),
[Hysteria ACL/outbound documentation](https://v2.hysteria.network/docs/advanced/ACL/),
[Xray routing documentation](https://xtls.github.io/en/config/routing), and
[MPTCP RFC 8684](https://www.rfc-editor.org/rfc/rfc8684.html).

![Live MPTUNNEL Overview with real connections, paths, sessions, and transfer speed](docs/assets/dashboard.png)

## Performance

All rates below are receiver-delivered goodput from the same Linux/Docker
host. Product comparisons used two flows for 20 seconds.

### One 500 Mbps path

180 ms one-way delay, 20 ms jitter, 1% loss.

| System | Transport | Download (Mbps) | Upload (Mbps) | Directions |
| --- | --- | ---: | ---: | ---: |
| Direct | TCP | 203.719 | 104.872 | 2/2 |
| Xray 26.3.27 | VMess/TCP | 215.402 | ≥207.296 | 1/2 |
| **MPTUNNEL** | MPP/TCP | 139.771 | 122.328 | 2/2 |
| Hysteria2 2.10.0 | QUIC | 92.626 | ≥103.035 | 1/2 |
| **MPTUNNEL** | MPP/QUIC | **244.596** | **229.631** | 2/2 |

MPP/QUIC delivered 2.64× Hysteria2's download goodput. MPP/TCP did not beat
Xray on this lossy single-path sample. Multiple TCP carriers on one route do
not create independent network capacity or remove native TCP head-of-line
recovery. Incomplete uploads are excluded from ratios.

### Five 500 Mbps paths

180 ms one-way delay, 20 ms jitter, 0% loss per path.

| System | Transport | Paths | Download (Mbps) | Upload (Mbps) | Directions |
| --- | --- | ---: | ---: | ---: | ---: |
| Linux MPTCP | TCP | 5 | 257.275 | 194.296 | 2/2 |
| **MPTUNNEL** | MPP/TCP | 5 | **726.106** | **533.922** | 2/2 |
| **MPTUNNEL** | MPP/QUIC | 5 | **596.944** | **753.061** | 2/2 |

MPP/TCP delivered 2.82× MPTCP download goodput and 2.75× its upload goodput.

### 20 links

Ten TCP and ten QUIC links with varied bandwidth, latency, jitter, and loss.

| Rate/link (Mbps) | Download (Mbps) | Upload (Mbps) | Directions |
| ---: | ---: | ---: | ---: |
| 30–100 | 378.764 | 292.168 | 2/2 |
| 300–1,000 | 1,395.311 | 610.051 | 2/2 |
| 3,000–10,000 | 2,882.347 | 731.499 | 2/2 |

Complementary 200/20 and 20/200 Mbps links placed 90.0% of download traffic and
91.1% of upload traffic on the faster direction.

### Continuity

| Condition | Download (Mbps) | Upload (Mbps) | TCP echo | HTTP | Datagrams | DL gap (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| QUIC port hop | 2,605.402 | 2,662.391 | — | — | — | 8 |
| TCP+QUIC blackhole | 153.510 | — | 60/60 | 98/98 | 252/253 | 532 |
| TCP+QUIC latency | 187.348 | — | 60/60 | 115/115 | 314/315 | 1,995 |
| TCP+QUIC handover | 221.587 | — | 53/53 | 89/90 | 218/221 | 1,117 |

A five-second total carrier outage passed 1/1. Client/server restart recovery
passed 2/2.

| Concurrency | Object (KiB) | Duration (s) | Requests | Rejected | Failed | Deadline (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 32 | 30 | 90/90 | 0 | 0 | 3,000 |
| 20 | 1,024 | 60 | 777/777 | 0 | 0 | — |

Every batched request met its three-second deadline. The 60-second run kept 20
requests active and replaced each completed request immediately.

### Local host ceiling

No rate, delay, jitter, or loss was configured.

| System | Transport | Endpoints | Download (Gbps) | Upload (Gbps) | Directions |
| --- | --- | ---: | ---: | ---: | ---: |
| Direct | TCP | 1 | 23.334 | 23.048 | 2/2 |
| Xray 26.3.27 | VMess/TCP | 1 | 8.525 | ≥7.197 | 1/2 |
| **MPTUNNEL** | MPP/TCP | 1 | 6.915 | 6.933 | 2/2 |
| **MPTUNNEL** | MPP/TCP | 5 | 5.726 | 6.011 | 2/2 |

All proxy rows reached their host processing ceiling. MPP/TCP performs
encryption, framing, sequencing, scheduling, Data ACKs, flow control, and relay
work. Extra unshaped endpoints add ordering work but no network capacity;
independently shaped paths provide the aggregation gain above. The remaining
local gap is implementation processing cost, not an MPP bandwidth threshold,
congestion controller, or recovery timer.

The product tables are matched comparisons. Scale, continuity, and load results
show capability and are not direct product rankings.

Production contains no fixed Mbps target or fixed percentage threshold.

See [Performance evidence](docs/PERFORMANCE.md) for the full results and
limits.

## How it works

```text
SOCKS5 / HTTP CONNECT / TCP+UDP forward / TUN
                         |
             routing, DNS, ACL, balancer
                         |
          MPP stream or datagram sequence space
                         |
       selection, Data ACK, flow control, reinjection
                         |
         TCP/TLS carriers       QUIC/HTTP/3 carriers
                  \              /
                   independent links
```

MPP version 6 uses independent sequence and receive-window state in each
stream direction. Carrier state is fenced by its physical lifetime, so a
reconnect never inherits stale congestion, flight, or delivery evidence.
Configured backup/expensive flags are restrictions; live measurements and
current demand rank eligible paths.

## Quick start

Download the archive for your platform from
[GitHub Releases](../../releases/latest). Generate one shared MPP credential
and a separate TLS identity:

```bash
umask 077
openssl rand -hex 32 > mpp-credential.key
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=server.example.com" \
  -addext "subjectAltName=DNS:server.example.com" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth" \
  -keyout server-key.pem -out server-cert.pem
```

Start the server:

```bash
mptunnel --credential-secret-file ./mpp-credential.key \
  server \
  --tls-certificate-chain ./server-cert.pem \
  --tls-private-key ./server-key.pem \
  --bind-path tcp://0.0.0.0:7443 \
  --bind-path udp://0.0.0.0:7443 \
  --outbound-protocol direct
```

Start the client:

```bash
mptunnel --credential-secret-file ./mpp-credential.key \
  client \
  --tls-server-name server.example.com \
  --tls-pinned-certificate ./server-cert.pem \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://server.example.com:7443 \
  --path udp://server.example.com:7443
```

For persistent operation, copy `examples/client.toml` or
`examples/server.toml` to `config.toml`, replace the placeholders, and validate
before startup:

```bash
mptunnel --config ./config.toml --check-config
mptunnel --config ./config.toml
```

## Configuration and operation

The same graph is available through TOML, the simple CLI surface, and supported
authenticated runtime updates. Successful runtime updates are written
atomically to `config.toml`; invalid or interrupted updates leave the active
generation and last valid file unchanged.

Every configurable resource has a canonical `name`. References use the
resource noun (`outbound`, `balancer`, `dns_plan`); `_id` fields identify
protocol credentials, principals, or signed artifacts. `target` means an
application destination, while `endpoint` means a listener or connector.

Fixed-target listeners use `tcp-forward` or `udp-forward`. Domain-capable
SOCKS5/HTTP/MPP outbounds can receive a domain unchanged; DNS resolution is
performed only when routing or the selected outbound requires an IP. Ranged
carrier endpoints use syntax such as `udp://server.example:20000-40000`.

Logging supports `off`, `error`, `warn`, and `info`, text or JSON, console,
append-only file, or both. Optional flow events record sanitized connection
open/close summaries without credentials or payload.

The opt-in loopback management endpoint provides live health, paths, sessions,
connections, traffic, DNS, balancers, configuration state, and bounded
controls. Its embedded dashboard stores a successfully authenticated token in
same-origin `localStorage` until **Forget token** is selected or authentication
fails.

See [the reference configuration](examples/config.reference.toml) and
[operations guide](docs/OPERATIONS.md).

## Platform support

| Platform | Proxy | TUN | Backend |
| --- | ---: | ---: | --- |
| Linux amd64/arm64 | ✓ | ✓ | Native |
| Windows amd64/arm64 | ✓ | ✓ | Wintun |
| macOS amd64/arm64 | ✓ | ✓ | NE |
| Android arm64 | ✓ | ✓ | `VpnService` |

Linux is the primary performance platform. Windows builds and tests natively
in GitHub Actions. macOS product VPN requires a signed Network Extension host;
Android embedding requires a host application.

The protocol and scheduler are portable. Platform-specific code is used only
for a beneficial host facility, with a neutral fallback wherever the operation
can remain correct. Run `mptunnel platform` for the current host report.

## Security

All carriers use TLS 1.3 with an independently configured server identity. TCP
negotiates no fixed ALPN, sends an exporter-bound binary admission prelude, and
then carries MPP records; it does not become HTTP. QUIC uses standard HTTP/3
framing and RFC 9297 datagrams with an encrypted credential-derived admission
selector before MPP parsing. Carrier 0-RTT is disabled.

This removes simple plaintext protocol markers, but it is not an
indistinguishability or cover-service claim. A source-aware active observer may
still fingerprint certificate, TLS/QUIC/HTTP/3 parameters, packet shape, timing,
or response behavior. MPP is a new custom protocol without an independent
security audit. Use high-entropy credentials, protect key/token files, keep the
management listener on loopback, and read [SECURITY.md](SECURITY.md).

## Release assets

Each immutable release publishes:

- `mptunnel-<version>-linux-amd64.tar.gz`
- `mptunnel-<version>-linux-arm64.tar.gz`
- `mptunnel-<version>-windows-amd64.zip`
- `mptunnel-<version>-windows-arm64.zip`
- `mptunnel-<version>-macos-amd64.zip`
- `mptunnel-<version>-macos-arm64.zip`
- `mptunnel-<version>-android-arm64.tar.gz`
- `version.json`

`version.json` records the tag, source commit, asset names, and immutable
tag-specific download URLs. GitHub supplies each asset digest. Published tags
and assets are never replaced; corrections use a new release.

## Documentation

- [Operations](docs/OPERATIONS.md)
- [Reference configuration](examples/config.reference.toml)
- [Performance evidence](docs/PERFORMANCE.md)
- [MPP version 6 specification](RFC.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Contributing](CONTRIBUTING.md)

Licensed under the [Apache License 2.0](LICENSE).
