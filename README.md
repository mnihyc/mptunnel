# MPTUNNEL

MPTUNNEL is an encrypted multipath proxy and tunnel for everyday Internet use.
It lets one application connection use several independent TCP and QUIC paths,
chooses paths from live latency and delivery evidence, and keeps the connection
alive when a path disappears.

It provides the daily-use surface expected from a modern proxy: SOCKS5, HTTP
CONNECT, TCP/UDP port forwarding, TUN, routing, DNS policy, outbound selection,
balancing, persistent configuration, live management, and connection
diagnostics.

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
SOCKS5 / HTTP CONNECT / port forward / TUN
                       |
             routing, DNS, outbounds
                       |
                one MPP flow
                       |
        live ranking + Data ACK + reinjection
              /             |             \
        TCP path A      QUIC path B     TCP path C
```

![Live MPTUNNEL Overview with real connections, paths, sessions, and transfer speed](docs/assets/dashboard.png)

## Performance

Rates below are receiver-delivered goodput from isolated Linux containers.
Matched product cells use a 500 Mbps path, two downloads, a 20-second window,
zero jitter, Xray-core 26.3.27 with VMess/TCP, Hysteria2 2.10.0, and the default
MPTUNNEL TCP+QUIC configuration.

### Across Internet conditions

| RTT (ms) | Loss | Xray/VMess (Mbps) | Hysteria2 (Mbps) | **MPTUNNEL (Mbps)** |
| ---: | ---: | ---: | ---: | ---: |
| 40 | 0% | 461.341 | **461.425** | 439.091 |
| 40 | 10% | 406.613 | **421.454** | 405.129 |
| 360 | 0% | **355.414** | 251.473 | 346.164 |
| 360 | 10% | 25.000 | 71.960 | **225.025** |

On a clean 40 ms route, MPTUNNEL is 4.8% below the fastest single-transport
baseline. At 360 ms RTT and 10% loss, it delivers 3.13× Hysteria2 and 9.00×
Xray/VMess goodput. The default can combine independent TCP and QUIC delivery
inside each logical flow.

### Aggregation

Each shaped link is 500 Mbps with 180 ms one-way delay, 20 ms jitter, and no
loss.

| System | Links | Download (Mbps) | Upload (Mbps) |
| --- | ---: | ---: | ---: |
| **MPTUNNEL (default)** | 1 | 370.207 | 398.793 |
| Linux MPTCP | 5 | 357.424 | 382.493 |
| **MPTUNNEL (default)** | 5 | **662.573** | **794.876** |

Five links raise MPTUNNEL goodput by 1.79× download and 1.99× upload. On the
same five-link topology, that is 1.85× and 2.08× Linux MPTCP respectively.

### Path choice

Link A provides 200 Mbps download and 20 Mbps upload. Link B provides the
reverse. MPTUNNEL ranks each direction independently.

| Direction | Link A (Mbps) | Link B (Mbps) | Faster-path share | Goodput (Mbps) |
| --- | ---: | ---: | ---: | ---: |
| Download | 200 | 20 | 90.1% | 147.748 |
| Upload | 20 | 200 | 89.4% | 149.680 |

### Failover and recovery

| Event | Existing-flow checks | Outcome |
| --- | ---: | ---: |
| Active path blackholed | TCP 60/60; HTTP 108/108 | Continued; 366 ms max gap |

Existing flows stay attached to their MPP sequence space while a surviving or
replacement carrier resumes delivery. New inbound connections are rejected
while every outbound path is unavailable. Total-outage and peer-restart
recovery results remain in the detailed evidence guide.

Movement around five percent can be ordinary run-to-run variance, not a pass
threshold. See [Performance evidence](docs/PERFORMANCE.md) for the detailed
topologies, upload accounting, load tests, resource limits, and reproducible
methodology.

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
