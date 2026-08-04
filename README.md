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

A lone TCP endpoint maintains up to three regular carriers by default. With
several endpoints, each endpoint contributes a regular primary while its
correlated siblings remain ready backups. Live directional delivery ranks
carriers inside the eligible tier; source addresses never establish capacity.

![Live MPTUNNEL Overview with real connections, paths, sessions, and transfer speed](docs/assets/dashboard.png)

## Performance

All rates are receiver-delivered goodput from the same Linux/Docker host.
Matched proxy comparisons used two flows for 20 seconds. Movement around five
percent can be ordinary run-to-run variance.

### One 500 Mbps path

180 ms one-way delay, 20 ms jitter, 1% loss.

| System | Transport | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: |
| Direct | TCP | 207.720 | 201.212 |
| Xray 26.3.27 | VMess/TCP | 218.716 | ≥151.299 |
| Hysteria2 2.10.0 | QUIC | 87.525 | ≥105.615 |
| **MPTUNNEL** | MPP/TCP | 225.564 | 253.758 |
| **MPTUNNEL** | MPP/QUIC | 252.859 | 209.684 |
| **MPTUNNEL** | **MPP/TCP+QUIC (default)** | **284.982** | **305.017** |

The default delivered 1.37× direct TCP download and 1.52× upload goodput;
MPP/QUIC delivered 2.89× Hysteria2's download. Incomplete baseline uploads
are shown only as receiver-confirmed lower bounds.

### Five 500 Mbps paths

180 ms one-way delay, 20 ms jitter, 0% loss per path.

| System | Transport | Shaped links | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: | ---: |
| Linux MPTCP | TCP | 5 | 357.424 | 382.493 |
| **MPTUNNEL** | MPP/TCP | 5 | 641.250 | 526.701 |
| **MPTUNNEL** | MPP/QUIC | 5 | 602.712 | 748.958 |
| **MPTUNNEL** | **MPP/TCP+QUIC (default)** | 5 | **677.370** | **748.829** |

The default delivered 1.90× MPTCP download and 1.96× upload goodput.

### TCP carrier pool

| Network condition | Direction | `1-1` | Default `1-3` | 3 × `1-1` |
| --- | --- | ---: | ---: | ---: |
| 500 Mbps per flow | Download | 355.923 | **886.246** | 794.061 |
| 500 Mbps per flow | Upload | 347.045 | 823.774 | **901.910** |
| Shared 200 Mbps | Download | 153.965 | 165.220 | 164.624 |
| Shared 200 Mbps | Upload | 157.547 | 151.999 | 167.363 |

The default pool aggregates independent per-flow capacity and remains at the
same aggregate ceiling when its carriers share one bottleneck.

### 20 links

Ten TCP and ten QUIC links with varied bandwidth, latency, jitter, and loss.

| Rate/link (Mbps) | Transport | Download (Mbps) | Upload (Mbps) |
| ---: | --- | ---: | ---: |
| 30–100 | MPP/TCP+QUIC (default) | 356.397 | 247.699 |
| 300–1,000 | MPP/TCP+QUIC (default) | 1,403.046 | 592.790 |
| 3,000–10,000 | MPP/TCP+QUIC (default) | 2,277.788 | 723.070 |

| Direction | Single fast link (Mbps) | Multipath (Mbps) | Fast-link share |
| --- | ---: | ---: | ---: |
| Download | 143.262 | 153.998 | 90.7% |
| Upload | 148.979 | 156.716 | 90.3% |

### Continuity

| Condition | Transport | Download (Mbps) | Upload (Mbps) | TCP echo | HTTP | Datagrams | Max DL gap (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Port hop | MPP/QUIC | 2,759.843 | 2,754.965 | — | — | — | 8 |
| Blackhole | MPP/TCP+QUIC (default) | 223.127 | — | 60/60 | 93/93 | 231/234 | 371 |
| Latency change | MPP/TCP+QUIC (default) | 132.196 | — | 60/60 | 102/102 | 303/304 | 547 |
| Repeated link changes | MPP/TCP+QUIC (default) | 159.741 | — | 40/40 | 34/37 | 86/91 | 2,609 |

A five-second total carrier outage passed 1/1. Client/server restart recovery
passed 2/2. Repeated-change misses began inside deliberate blackholes and met
their application deadlines before service returned; persistent TCP completed.

| Concurrency | Object (KiB) | Duration (s) | Requests | Rejected | Failed | Deadline (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 32 | 30 | 90/90 | 0 | 0 | 3,000 |
| 20 | 1,024 | 60 | 686/686 | 0 | 0 | — |

Every batched request met its three-second deadline. The 60-second run kept 20
requests active and replaced each completed request immediately.

### Local host ceiling

No rate, delay, jitter, or loss was configured.

| System | Transport | Carriers | Download (Gbps) | Upload (Gbps) |
| --- | --- | ---: | ---: | ---: |
| Direct | TCP | 1 | 21.393 | 22.113 |
| Xray 26.3.27 | VMess/TCP | 1 | 8.044 | ≥6.952 |
| Hysteria2 2.10.0 | QUIC | 1 | 2.714 | ≥2.816 |
| **MPTUNNEL** | MPP/TCP (`1-1`) | 1 | 6.580 | 6.715 |
| **MPTUNNEL** | MPP/TCP (default) | 3 | 5.550 | 6.393 |
| **MPTUNNEL** | MPP/QUIC | 1 | 2.825 | 2.721 |
| **MPTUNNEL** | **MPP/TCP+QUIC (default)** | 4 | 5.026 | 6.173 |

These rows measure local processing capacity, not a public Internet link.
Extra unshaped carriers add work without adding network capacity; independently
shaped links provide the aggregation opportunity shown above.

The proxy tables are matched comparisons. Scale, continuity, and load results
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
