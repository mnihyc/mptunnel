# MPTUNNEL

[![CI](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml)
[![Release Build](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

MPTUNNEL is an encrypted multipath proxy and tunnel for everyday Internet use.
It lets one application connection use several independent TCP and QUIC paths,
chooses paths from live latency and delivery evidence, and keeps the connection
alive when a path disappears.

It provides the daily-use surface expected from a modern proxy: SOCKS5, HTTP
CONNECT, a single-port mixed SOCKS5/HTTP CONNECT listener, TCP/UDP port
forwarding, TUN, routing, DNS policy, outbound selection,
balancing, persistent configuration, live management, and connection
diagnostics.

SOCKS5, HTTP CONNECT, mixed proxy, port forwarding, TUN, and MPP listeners use
the ordinary L4 routing model. Experimental `tun-l3` and `mpp-l3` inbounds
instead carry complete IP packets, with server-owned address pools and
per-principal allocations. L3 can use TCP and QUIC together while host routes,
DNS, firewall policy, forwarding, and NAT remain under operator control. A
configuration cannot mix L4 and L3 inbound protocols.

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
SOCKS5 / HTTP CONNECT / mixed proxy / port forward / TUN-L4
                       |
             routing, DNS, outbounds
                       |
                one MPP flow
                       |
        live ranking + Data ACK + reinjection
              /             |             \
        TCP path A      QUIC path B     TCP path C

TUN-L3 packet device -> authenticated IP packets -> the same carrier set
```

![MPTUNNEL management dashboard with live charts, path health, peer paths, inbound connections, and outbound services](docs/assets/dashboard.png)

## Performance

The scalar tables below are accepted historical evidence for v0.2.1–v0.2.2;
they do not characterize v0.4.4 or the current source tree. No current
current-release time-series figure or ranking is published until at least two
matched repetitions pass the host, source, workload, isolation, and provenance
gates described in the [performance methodology](docs/PERFORMANCE.md).

These historical results are receiver-delivered goodput from controlled Linux
tests, rounded to the nearest Mbps. Xray-core 26.3.27 uses VMess/TCP; Hysteria2
2.10.0 uses Brutal at the shaped link rate; MPTUNNEL uses the measured
release's default TCP+QUIC paths unless a transport is named explicitly.

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
openssl rand -out mpp-credential.key 32
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
  --bind-path quic://0.0.0.0:7443 \
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
  --path quic://server.example.com:7443
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

The same configuration is available through TOML, the simple CLI surface, and supported
authenticated runtime updates. Successful runtime updates are written
atomically to `config.toml`; invalid or interrupted updates leave the active
generation and last valid file unchanged.

Every TOML secret, certificate, and key uses one byte-material table:
`{ from = "file", path = "..." }` reads exact file bytes;
`{ from = "env", var = "MPTUNNEL_NAME_FILE" }` reads the file whose path is in
that environment variable; `{ from = "hex", value = "..." }` and
`{ from = "base64", value = "..." }` decode strict inline encodings; and
`{ from = "raw", value = "..." }` supplies UTF-8 bytes. `{ value = "..." }`
is shorthand for the raw form.
Inline encodings are not encryption and remain stored in the configuration.
Material bytes are exact: file content, decoded inline content, and raw UTF-8
are never trimmed. Consumer-specific size, UTF-8, or PEM validation follows.
Relative material paths—including a relative path read from an environment
variable—resolve beside the selected TOML document.

Every configurable resource has a canonical `name`. References use the
resource noun (`outbound`, `balancer`, `dns_policy`); `_id` fields identify
protocol credentials, principals, or signed artifacts. `target` means an
application destination; a listen address accepts local traffic; an `endpoint`
is a proxy connector or MPP carrier URI. A DNS server defines how and where to
send DNS messages. A DNS policy selects servers, address families, security,
limits, cache behavior, named exact-name `override_records`, and at most one
named `synthetic_capture`. Policy selection is explicit: a route-selected DNS
policy wins, otherwise exact and longest-suffix DNS rules precede the default.
Within that policy an attached override record wins, captured DNS may then use
its attached synthetic capture, and only then are its servers queried. Ordinary
dial-time resolution never synthesizes an address. A recovered synthetic
address retains the policy and capture that issued it; a route may omit
`dns_policy`, but cannot silently replace that policy with another.

Fixed-target listeners use `tcp-forward`, `udp-forward`, or `mixed-forward`;
the mixed form binds both transports on the same addresses and sends them to
one target.
Domain-capable SOCKS5/HTTP/MPP outbounds can receive a domain unchanged.
`[routing].target_resolution` makes the ownership explicit: `as-is` never
resolves during routing, `route-only` resolves only for route/ACL evidence but
keeps the hostname for a domain-capable outbound, and `full-resolve` passes
authorized literal IPs. Omission retains the historical demand-driven
behavior. MPP carrier endpoint DNS is separate from application-target DNS.
Ranged carrier endpoints use syntax such as
`quic://server.example:20000-40000`.

All L4 inbounds, including local listeners and `protocol = "mpp"`, use one
ordered, first-match `[[routing.rules]]` table. A normal rule names one
`outbound` or `balancer`; no separate allow action is required. Explicit
`decision = "allow-restricted"` authorizes a narrowly matched private or
special-use destination, while `reject` and `drop` are terminal. Omitted
`inbounds` or `principal_ids` means any; scalar `"*"` is the equivalent explicit
spelling. If no rule matches, traffic is rejected—MPTUNNEL never silently uses
the first outbound.

`protocol = "mpp-l3"` is the distinct server-side packet service used with a
`tun-l3` client. It does not enter L4 routing, application-target DNS, L4 flow
admission, or target outbounds. Carrier endpoint DNS and `max_dns_work` remain
available. Every definition is validated, but runtime starts only DNS policies
reachable from `[dns].default`, DNS rules, or route `dns_policy`, and outbounds
reachable from routes, active DNS servers, balancers, or a `tun-l3` inbound.
Unused definitions make no network or system changes.

Logging starts with the running version, configuration source, safe inbound and
outbound inventory, bound listeners, runtime readiness, and shutdown. The
default UTC text format is readable at a terminal; newline-delimited JSON,
append-only files, and sanitized opt-in flow summaries are also supported.
Set `level = "debug"` to see one correlated trace whose inbound, routing,
optional balancer, and outbound records repeat the same accepted-request
context. It includes the principal, requested destination, a typed local or MPP
carrier peer, the exact route, and each configured outbound destination,
protocol, attempt, and result. Server-side MPP records also identify the
opening session and ingress carrier/path; reliable MPP outbound records include
their independently selected underlay and path. UDP is traced per logical
association; packets and per-packet MPP path choices are not logged.
One bounded background HTTPS check reports the newest published GitHub release
without delaying startup or forwarding; an available update includes its
release-page URL.

The opt-in loopback management endpoint exposes only the authenticated v4 API
under `/api/v4/` and provides live health, paths, sessions,
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
| Android arm64/x86_64 | ✓ | ✓ | `VpnService` |

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
- `mptunnel-<version>-android-x86_64.tar.gz`
- `version.json`

Each Android archive contains the command-line binary and the matching JNI
library under `arm64-v8a/libmptunnel.so` or `x86_64/libmptunnel.so`.

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
