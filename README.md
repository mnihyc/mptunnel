# MPTUNNEL

MPTUNNEL is an encrypted multipath proxy and tunnel for everyday Internet use.
It combines independent TCP and QUIC paths into one logical connection, adds
capacity when demand justifies it, and keeps established traffic alive when a
carrier disappears.

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
carriers, so one connection can use several links and survive losing one.

- Healthy paths can serve one flow concurrently instead of only balancing
  separate connections.
- Exact Data ACKs and bounded reinjection preserve traffic across carrier loss.
- Live delivery evidence selects paths; an IP address is not treated as link
  quality or peer identity.
- TCP and QUIC retain native congestion control, pacing, retransmission,
  migration, and loss recovery.
- One TCP endpoint defaults to a demand-driven `1-3` carrier range. Extra TCP
  sessions are kept only when completed delivery proves useful added service.

### Product and multipath model

| System | Proxy/VPN | One-flow aggregation | TCP | QUIC | Cross-carrier recovery |
| --- | ---: | ---: | ---: | ---: | ---: |
| **MPTUNNEL** | Yes | Yes | Yes | Yes | Yes |
| **Hysteria 2** | Yes | No | No | Yes | No |
| **Xray/V2Ray** | Yes | No | Yes | Yes | No |
| **MPTCP** | No | Yes | Yes | No | Yes |

Reference behavior is taken from the
[Hysteria client modes and TUN documentation](https://v2.hysteria.network/docs/advanced/Full-Client-Config/),
[Hysteria ACL/outbound documentation](https://v2.hysteria.network/docs/advanced/ACL/),
[Xray routing documentation](https://xtls.github.io/en/config/routing), and
[MPTCP RFC 8684](https://www.rfc-editor.org/rfc/rfc8684.html).

![Live MPTUNNEL Overview with real connections, paths, sessions, and transfer speed](docs/assets/dashboard.png)

## Performance

Values are retained delivered-goodput observations on the same Linux/Docker
host, not configured rates or an Internet-speed guarantee. Compare only rows
within the same explicitly described run.

### Single-path baseline

Each system used one 500 Mbps path with 180 ms one-way delay, 20 ms jitter,
and 1% configured loss. The object, two-flow workload, and run duration were
the same.

| System | Carrier | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: |
| Direct | TCP | 231.521 | ≥240.939 |
| Xray 26.3.27 | VMess/TCP | 219.529 | ≥240.849 |
| MPTUNNEL | MPP/TCP | 151.722 | ≥162.267 |
| Hysteria2 2.10.0 | QUIC | 114.506 | ≥117.541 |
| **MPTUNNEL** | **MPP/QUIC** | **212.704** | **≥207.649** |

MPP/QUIC delivered 85.8% more download goodput than the matched Hysteria2 row.
The upload lower bounds were ≥207.649 and ≥117.541 Mbps respectively; they do
not establish a final upload ratio. Xray was faster than MPP/TCP on this single
path; MPTUNNEL does not claim otherwise. Its performance purpose is aggregation
and continuity across independent carriers.

### Multipath aggregation

The measurement used five equal 500 Mbps paths with 180 ms one-way delay,
20 ms jitter, and no configured loss.

| System | Carrier | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: |
| **MPTUNNEL** | **MPP/TCP × 5** | **834.364** | **649.766** |
| **MPTUNNEL** | **MPP/QUIC × 5** | **648.493** | **≥738.113** |

An earlier matched MPP-v5/MPTCP run measured:

| System | Carrier | Download (Mbps) | Upload (Mbps) |
| --- | --- | ---: | ---: |
| MPTUNNEL | MPP/TCP × 5 | 875.187 | 617.392 |
| Linux MPTCP | TCP × 5 | 168.085 | 450.738 |

The current MPTUNNEL and earlier MPTCP results are not compared across runs.

### Operating envelope

Twenty-carrier runs used 10 TCP and 10 QUIC paths over five independently
seeded bandwidth, latency, jitter, and loss epochs.

| Mbps/path | Download (Mbps) | Upload (Mbps) | Complete |
| ---: | ---: | ---: | ---: |
| 30–100 | 344.534 | 210.378 | 2/2 |
| 300–1,000 | 1,178.811 | 609.004 | 2/2 |
| 3,000–10,000 | 2,261.932 | 670.693 | 2/2 |

Complementary 200/20 and 20/200 Mbps links placed 91.5% of download traffic on
the faster direction. Blackhole and latency transitions kept every reliable
flow alive, delivered 296/300 datagrams, and bounded the maximum bulk gap to
0.717–1.293 seconds. Five QUIC port hops passed 2/2 at
2,459.750/2,498.275 Mbps. A five-second total carrier outage passed 1/1, and
client/server restart recovery passed 2/2.

| Pattern | Concurrent | KiB | Window (s) | Complete | Reject/incomplete | Max (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 3 s batches | 10 | 32 | 30 | 90/90 | 0/0 | 1.263 |
| Closed loop | 20 | 1,024 | 60 | 570/570 | 0/0 | — |

The baseline tables compare only matched product runs. The larger scale and
browser rows show completion under load and are not blended into cross-product
speed claims. Measurements also cover datagrams, TUN, mixed load,
bandwidth/latency/loss combinations, shared bottlenecks, adaptive TCP carriers,
migration, failure, and recovery.

Production contains no fixed Mbps target or fixed percentage threshold.

See [Performance evidence](docs/PERFORMANCE.md) for exact conditions,
limitations, and interpretation.

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

MPP version 5 uses independent sequence and receive-window state in each
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
  -subj "/CN=server.example" \
  -addext "subjectAltName=DNS:server.example" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth" \
  -keyout server-private-key.pem -out server-certificate.pem
```

Start the server:

```bash
mptunnel --credential-secret-file ./mpp-credential.key \
  server \
  --tls-certificate-chain ./server-certificate.pem \
  --tls-private-key ./server-private-key.pem \
  --bind-path tcp://0.0.0.0:4433 \
  --bind-path udp://0.0.0.0:4433 \
  --outbound-protocol direct
```

Start the client:

```bash
mptunnel --credential-secret-file ./mpp-credential.key \
  client \
  --tls-server-name server.example \
  --tls-pinned-certificate ./server-certificate.pem \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://server.example:4433 \
  --path udp://server.example:4433
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

| Platform | Proxy | TUN | Integration |
| --- | ---: | ---: | --- |
| Linux amd64/arm64 | Yes | Managed | Native |
| Windows amd64/arm64 | Yes | Managed | Wintun |
| macOS amd64/arm64 | Yes | Host | Network Extension |
| Android arm64 | Yes | Host | `VpnService` |

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
- [MPP version 5 specification](RFC.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Contributing](CONTRIBUTING.md)

Licensed under the [Apache License 2.0](LICENSE).
