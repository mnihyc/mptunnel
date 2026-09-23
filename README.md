# MPTUNNEL

[![CI](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml)
[![Release Build](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**One connection. Multiple paths. TCP and QUIC together.**

MPTUNNEL is an encrypted proxy and tunnel that carries a single application
connection over several network paths. Combine independent links for more
capacity, use TCP and QUIC together, and recover undelivered data over another
carrier when a path fails.

[**Get started**](#quick-start) · [**Download**](../../releases/latest) ·
[**Performance**](docs/PERFORMANCE.md) · [**Configuration**](examples/config.reference.toml)

## Why MPTUNNEL?

A download, upload or long-lived connection can use more than one path without
changes to the application. MPTUNNEL handles path selection and delivers the
bytes back in order at the other end.

- **Combine links within one connection.** Spread a transfer across independent
  paths instead of assigning the entire connection to just one.
- **Adapt in both directions.** Choose paths using live delivery and latency
  measurements; upload and download can use different combinations.
- **Recover across transports.** Move undelivered data to a surviving TCP or
  QUIC carrier when another carrier stops making progress.
- **Use it with your existing applications.** SOCKS5, HTTP CONNECT, a mixed
  proxy listener, TCP/UDP forwarding and TUN share routing and DNS controls.

```text
                         ┌── TCP path ──┐
Your app ── MPTUNNEL ────┤              ├──── MPTUNNEL ── Destination
                         └── QUIC path ─┘
                           one connection
```

Independent paths can add capacity. TCP and QUIC on the same physical link
share its bandwidth, while giving the tunnel different transport options.

<a name="performance"></a>

## See it in action

The [performance guide](docs/PERFORMANCE.md) follows a transfer as one path
slows down and goes offline, then compares download speed, responsiveness and
CPU cost with Hysteria2, Xray and direct TCP.

<a href="docs/PERFORMANCE.md#speed-and-responsiveness">
<picture>
  <source media="(max-width: 600px)" srcset="docs/assets/performance/shared-link-tradeoffs-narrow.svg">
  <img src="docs/assets/performance/shared-link-tradeoffs.svg" alt="Download speed and response latency on a shared 500 Mbps connection">
</picture>
</a>

In this 40-second Linux download, TCP+QUIC delivered **406 Mbps**;
QUIC alone delivered **343 Mbps**, with 95% of small echo requests answered
within **154 ms** during the download. The two modes offer different balances
of speed and responsiveness.
[See the setup, full timelines and results](docs/PERFORMANCE.md#speed-and-responsiveness).

## Know what your tunnel is doing

The built-in dashboard shows live traffic, path health, peer paths and active
connections. Inspect which carriers are working and manage routing, DNS and
configuration from the same interface.

![MPTUNNEL dashboard showing live traffic, path health and connections](docs/assets/dashboard.png)

Enable the authenticated local dashboard using the
[management setup](docs/OPERATIONS.md#management-api).

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

Copy the two shared key files and `server-cert.pem` securely to the client.
Keep `server-key.pem` on the server. Replace `server.example.com` with your
server's address and allow both TCP and UDP on port 7443.

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

Point an application at SOCKS5 `127.0.0.1:1080` or HTTP proxy `127.0.0.1:8080`:

```bash
curl --proxy socks5h://127.0.0.1:1080 https://example.com
```

**Upgrading to 0.6.0:** upgrade both client and server together. This release
uses MPP wire version 16 and requires matching peers; see the
[upgrade instructions](docs/OPERATIONS.md#mpp-wire-version-upgrade).

For persistent operation, copy `examples/client.toml` or
`examples/server.toml` to `config.toml`, replace the placeholders, and validate
before startup:

```bash
mptunnel --config ./config.toml --check-config
mptunnel --config ./config.toml
```

## Configuration and operation

Start from [client.toml](examples/client.toml) and
[server.toml](examples/server.toml), then use the
[annotated reference](examples/config.reference.toml) for the complete configuration.

| I want to… | Start here |
| --- | --- |
| Route applications or domains through different outbounds | [Routing and DNS configuration](docs/OPERATIONS.md#config-and-validation) |
| Run TUN or forward TCP/UDP ports | [Configuration reference](examples/config.reference.toml) |
| Configure multiple paths or port hopping | [Path policy](docs/OPERATIONS.md#path-policy-and-status) |
| Inspect traffic or update a running configuration | [Dashboard and management API](docs/OPERATIONS.md#management-api) |
| Size buffers and memory for a VPS | [Resource envelopes](docs/OPERATIONS.md#resource-envelopes) |
| Run as a service and collect logs | [Runtime supervision](docs/OPERATIONS.md#runtime-supervision) |

Experimental L3 packet tunneling is also available through `tun-l3` and
`mpp-l3`, with server-managed address pools. See the
[L3 setup and host networking requirements](docs/OPERATIONS.md#config-and-validation).

## Platform support

| Platform | Proxy | TUN integration |
| --- | --- | --- |
| Linux amd64 / arm64 | ✓ | Native |
| Windows amd64 / arm64 | ✓ | Wintun |
| macOS amd64 / arm64 | ✓ | Signed Network Extension host required |
| Android arm64 / x86_64 | ✓ | Host app with `VpnService` required |

Run `mptunnel platform` to check host capabilities. Android releases include
both the command-line binary and a JNI library for embedding.

<a name="release-assets"></a>

<details>
<summary>Release archive names and contents</summary>

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

</details>

## Security

Connections are encrypted and peers authenticate with shared credentials.
The shipped configuration also uses a separate transport key to gate TCP and
QUIC handshakes. Keep key files private and the management listener on loopback.
MPTUNNEL's custom protocol has not had an independent security audit; see
[the security model](SECURITY.md) for deployment guidance and reporting issues.

## Documentation

- [Operations and troubleshooting](docs/OPERATIONS.md)
- [Configuration reference](examples/config.reference.toml)
- [Performance and comparisons](docs/PERFORMANCE.md)
- [Protocol specification](RFC.md) and [architecture](docs/ARCHITECTURE.md)
- [Release packages](packaging/README.md) and [latest downloads](../../releases/latest)
- [Contributing](CONTRIBUTING.md)

Licensed under the [Apache License 2.0](LICENSE).
