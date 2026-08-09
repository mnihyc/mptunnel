# MPTUNNEL

[![CI](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml)
[![Release Build](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

MPTUNNEL is an encrypted multipath proxy and tunnel for everyday Internet use.
It lets one application connection use several independent TCP and QUIC paths,
chooses paths from live latency and delivery evidence, and keeps the connection
alive when a path disappears.

It provides the daily-use surface expected from a modern proxy: SOCKS5, HTTP
CONNECT, TCP/UDP port forwarding, TUN, routing, DNS policy, outbound selection,
balancing, persistent configuration, live management, and connection
diagnostics.

The global forwarding mode defaults to L4 for SOCKS5, HTTP CONNECT, port
forwarding, and TUN-L4. An explicit experimental L3 mode instead carries
complete IP packets, with server-owned address pools and per-principal
allocations. L3 can use TCP and QUIC together while host routes, DNS, firewall
policy, forwarding, and NAT remain under operator control. Both TUN modes are
experimental and cannot be mixed in one runtime generation.

## Contents

- [Why MPTUNNEL?](#why-mptunnel)
- [Performance](#performance)
- [Quick start](#quick-start)
- [Configuration and operation](#configuration-and-operation)
- [Platform support](#platform-support)
- [Security](#security)
- [Release assets](#release-assets)
- [Documentation](#documentation)

## Why MPTUNNEL?

Xray/V2Ray routes or balances separate connections across outbounds. Hysteria2
carries proxy streams within one QUIC session and supports transparent UDP
port hopping. MPTUNNEL addresses the gap between them: one application flow
can use several independent TCP and QUIC carriers at the same time.

| Within one logical flow | Xray/V2Ray | Hysteria2 | **MPTUNNEL** |
| --- | ---: | ---: | ---: |
| Multiple independent paths | — | — | ✓ |
| TCP + QUIC together | — | — | ✓ |
| Upload/download path ranking | — | — | ✓ |
| Independent-carrier failover | — | — | ✓ |

Beyond its daily-use proxy, forwarding, TUN, routing, and DNS surface,
MPTUNNEL's advantage is inside the flow: it can add independent link capacity,
rank paths from live latency and delivery evidence, choose differently for
upload and download, and move undelivered ranges to a surviving carrier.

```text
forwarding_mode = l4 (default)
SOCKS5 / HTTP CONNECT / port forward / TUN-L4
                       |
             routing, DNS, outbounds
                       |
                one MPP flow
                       |
        live ranking + Data ACK + reinjection
              /             |             \
        TCP path A      QUIC path B     TCP path C

forwarding_mode = l3 (experimental)
TUN-L3 packet device -> authenticated IP packets -> the same carrier set
```

![Live MPTUNNEL Overview with real connections, paths, sessions, and transfer speed](docs/assets/dashboard.png)

## Performance

Results below are receiver-delivered goodput from controlled Linux tests,
rounded to the nearest Mbps. Xray-core 26.3.27 uses VMess/TCP; Hysteria2 2.10.0
uses Brutal at the shaped link rate; MPTUNNEL uses its default TCP+QUIC paths
unless a transport is named explicitly.

### One link

Each product used the same 500 Mbps link, two parallel downloads, and a
20-second load window.

| RTT | Jitter | Loss | Xray/VMess | Hysteria2 | MPTUNNEL |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 40 ms | 10 ms | 0.5% | 441 Mbps | 464 Mbps | 414 Mbps |
| 280 ms | 20 ms | 10% | 71 Mbps | 96 Mbps | 195 Mbps |

MPTUNNEL was 10.6% below the fastest baseline on the ordinary link. On the
adverse link it delivered 2.02× Hysteria2 and 2.75× Xray/VMess goodput.

### Add links

Every physical link repeats the 500 Mbps, 40 ms RTT, 10 ms jitter, and 0.5%
loss profile above.

| System | Links | Download | Upload |
| --- | ---: | ---: | ---: |
| MPTUNNEL | 1 | 414 Mbps | 425 Mbps |
| MPTUNNEL | 2 | 772 Mbps | 621 Mbps |
| Linux MPTCP | 5 | 885 Mbps | — |
| MPTUNNEL | 5 | 1,366 Mbps | 1,384 Mbps |

Two links provide 1.86× download and 1.46× upload goodput; five provide 3.30×
and 3.25×. Xray/VMess and Hysteria2 remain the one-link product controls above;
they do not aggregate one application flow across independent links. Linux
MPTCP is a kernel transport control, not an encrypted proxy.

### Use each link for what it does best

Link A is 200 Mbps down / 20 Mbps up; Link B is 20 Mbps down / 200 Mbps up.
The single-path products remain on Link A in both directions. MPTUNNEL receives
both links in one configuration; this compares fixed-link and same-flow
multipath capability, not equal path provisioning.

| System | Links | Download | Upload |
| --- | --- | ---: | ---: |
| Xray/VMess | A | 182 Mbps | ≥18 Mbps |
| Hysteria2/Brutal | A | 189 Mbps | ≥19 Mbps |
| MPTUNNEL (TCP) | A + B | 199 Mbps | 197 Mbps |

MPTUNNEL carried 90.7% of download traffic on Link A and 90.7% of upload
traffic on Link B without changing endpoints between directions.

The next control combines an 80 Mbps ordinary link with the 500 Mbps adverse
link from the first table. MPTUNNEL uses default TCP+QUIC paths while bulk,
short HTTP, persistent TCP, and datagrams run together for 30 seconds.

| Links | Bulk | TCP latency | TCP checks | HTTP latency | HTTP checks | UDP checks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Ordinary | 61 Mbps | 103/217 ms | 60/60 | 444/1,026 ms | 45/45 | 102/102 |
| Adverse | 99 Mbps | 452/1,868 ms | 35/35 | 1,835/2,686 ms | 9/11 | 14/18 |
| Both | 160 Mbps | 173/318 ms | 60/60 | 376/838 ms | 53/53 | 205/205 |

The two-link run measured 160 Mbps versus 61 and 99 Mbps separately, while
every TCP, HTTP, and UDP check completed. Latency cells are p50/p95;
check cells are completed/attempted.

### Short connections

| Load | Object | Window | Completed | Rejected | Failed |
| --- | ---: | ---: | ---: | ---: | ---: |
| 10 every 3 s | 32 KiB | 30 s | 90/90 | 0 | 0 |
| 20 continuous | 1 MiB | 60 s | 755/755 | 0 | 0 |

The slowest ten-request batch completed in 0.681 seconds against its
three-second bound. The continuous run held twenty requests in flight and
immediately replaced each completion.

### Stay online

Each row is one controlled disruption run. Counts are completed/attempted
application checks; pause is the longest receiver-side download gap.

| Event | TCP checks | HTTP checks | UDP checks | DL pause |
| --- | ---: | ---: | ---: | ---: |
| 2 s path blackhole | 60/60 | 72/72 | 228/229 | 636 ms |
| Latency/loss change | 60/60 | 93/94 | 241/243 | 1,489 ms |
| Repeated changes | 47/47 | 81/83 | 217/219 | 869 ms |

Additional controls observed same-flow recovery after a five-second total
carrier outage (1/1) and renewed connectivity after server and client process
restarts (2/2 checks). New inbound connections are rejected while every
outbound path is unavailable; established flows remain attached during carrier
recovery.

See [Performance evidence](docs/PERFORMANCE.md) for exact setup, upload
accounting, stress tests, recovery evidence, and limitations.

## Quick start

Download the archive for your platform from
[GitHub Releases](../../releases/latest). Generate one shared MPP credential,
one shared transport key, and a separate TLS identity:

```bash
umask 077
openssl rand -hex 32 > mpp-credential.key
openssl rand -out mpp-transport.key 32
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=mptunnel.example" \
  -addext "subjectAltName=DNS:mptunnel.example" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth" \
  -keyout server-key.pem -out server-cert.pem
```

Start the server:

```bash
mptunnel --credential-secret-file ./mpp-credential.key \
  server \
  --transport-secret-file ./mpp-transport.key \
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
  --transport-secret-file ./mpp-transport.key \
  --tls-pinned-certificate ./server-cert.pem \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://server.example.com:7443 \
  --path udp://server.example.com:7443
```

The shared transport key replaces the TCP TLS handshake with PSK-gated Noise
and prevents public QUIC Initial packets from eliciting a certificate flight.
It is not an MPP client credential. Shipped configurations enable it; the field
is optional so peers can instead use TLS 1.3 TCP and public QUIC Initials. The
QUIC and TLS-fallback certificate name defaults to `mptunnel.example`;
`--tls-server-name` remains available as an override.

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

Logging starts with the running version, configuration source, safe inbound and
outbound inventory, bound listeners, runtime readiness, and shutdown. The
default UTC text format is readable at a terminal; newline-delimited JSON,
append-only files, and sanitized opt-in flow summaries are also supported.
One bounded background HTTPS check reports the newest published GitHub release
without delaying startup or forwarding; an available update includes its
release-page URL.

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

The shipped profile uses PSK-gated Noise for TCP and private QUIC Initial keys
before QUIC's inner TLS/HTTP/3 handshake; carrier 0-RTT is disabled. Public and
wrong-secret probes receive no TCP handshake response and cannot elicit or
decrypt a QUIC certificate flight. Omitting the optional transport secret uses
TLS 1.3 TCP with no ALPN and public QUIC Initials instead. MPP client
credentials stay separate and still authorize individual peers after carrier
protection.

This removes simple plaintext protocol markers, but it is not an
indistinguishability or cover-service claim. A source-aware active observer may
still fingerprint QUIC packet shape and version, Noise ephemeral keys, timing,
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
- [Protocol specification](RFC.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Contributing](CONTRIBUTING.md)

Licensed under the [Apache License 2.0](LICENSE).
