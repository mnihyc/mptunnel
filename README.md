# MPTUNNEL

[![CI](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/ci.yml)
[![Release Build](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml/badge.svg)](https://github.com/mnihyc/mptunnel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Encrypted multipath tunneling over TCP and QUIC.**

MPTUNNEL is an encrypted proxy and tunnel that combines TCP and QUIC connections
to carry your traffic. Even a single download or upload can use several of these
connections at once, drawing on their available bandwidth and recovering through
another connection when one stops making progress.

[**Get started**](#quick-start) · [**Download**](../../releases/latest) ·
[**Performance**](docs/PERFORMANCE.md) · [**Configuration**](examples/config.reference.toml)

## Multipath transport

A fast Internet link does not always mean a fast application connection.
Individual connections can encounter rate limits, loss or congestion, and the
best route or transport can change during a transfer. Other connections may
still have useful capacity.

MPTUNNEL's Multipath Proxy Protocol (MPP) groups TCP and/or QUIC connections into one
tunnel between peers. Each transport connection is a **carrier**. MPP distributes
parts of an application's byte stream across eligible carriers, tracks delivery,
and puts the bytes back in order at the other end. Undelivered parts can be sent
again over another carrier, so a transfer can keep moving when one carrier stalls.

```text
            One MPP session
         ┌── TCP 1, 2, … ──┐
MPTUNNEL ┤                 ├ MPTUNNEL
         └── QUIC 1, 2, … ─┘
          TCP, QUIC, or both
```

Choose TCP-only, QUIC-only or mixed sets, from a few carriers to tens. One MPP
session carries many application flows, and each reliable flow can use several
carriers. Upload and download select carriers independently using live delivery
and latency measurements; the set carrying data changes with demand and conditions.

**Aggregation works on one network link or across several.** On one Internet
link, parallel carriers share its total bandwidth and can help use it more fully
when individual connections are limited. With separately routed links, a transfer
can also draw on their combined capacity. Carrier count and link count are
different: you can run many carriers through one network interface. To use
separate links, configure endpoints and source routing to reach them.

Applications connect through SOCKS5, HTTP CONNECT, a mixed proxy listener,
TCP/UDP forwarding or TUN, with routing and DNS controls. They need no multipath
support of their own. See the [path configuration guide](docs/OPERATIONS.md#path-policy-and-status)
to choose carriers and their usage policies.

<a name="performance"></a>

## Performance evaluation

### Aggregation across independent links

One download uses **3 TCP carriers on a 200 Mbps link** and **1 QUIC carrier on
another 200 Mbps link**. Before any impairment, it averages **312 Mbps over the
first 15 seconds**, including startup. The application uses more bandwidth than
either link's 200 Mbps limit, from a combined capacity of 400 Mbps.

### Delivery during a QUIC interruption

Later in the same transfer, UDP is blocked for **2.6 seconds** while the TCP link
remains available. Application delivery continues: the one full second inside
the recorded block delivers **184 Mbps**, and four small echo requests sent and
answered within the block complete in **223–234 ms**.

[![Download delivery and echo latency across the initial aggregation period, QUIC slowdown and UDP interruption](docs/assets/performance/independent-paths.svg)](docs/PERFORMANCE.md#one-connection-two-links)

The full timeline also includes a **10 Mbps restriction on QUIC** before the
interruption. [Per-phase results and concurrent HTTP outcomes](docs/PERFORMANCE.md#one-connection-two-links)
explain each condition separately.

### Throughput and latency on one Internet link

Here each system runs separately on the same **500 Mbps download / 100 Mbps
upload** bottleneck. All carriers within a run share that capacity. The link starts
with jitter, which is removed between seconds 8 and 10.

| System | 0–8 s<br>Mbps | 10–40 s<br>Mbps | Echo p95<br>10–40 s, ms |
| --- | ---: | ---: | ---: |
| MPTUNNEL<br>3 TCP + 1 QUIC | 241 | 453 | 823 |
| MPTUNNEL<br>1 QUIC | 269 | 367 | 118 |
| Hysteria2 | 49 | 467 | 117 |
| Xray VMess/TCP | 8 | 379 | 122 |
| Direct TCP | 139 | 354 | 117 |

After jitter removal, mixed MPTUNNEL delivers more data than its single QUIC
carrier, with longer response times under load. Hysteria2 has the highest
throughput in that interval.

These are single Linux runs using a MPTUNNEL 0.6.0 development build. The
[performance guide](docs/PERFORMANCE.md#speed-and-responsiveness) provides the full
timing curves, CPU costs, request counts and controller settings.

## Traffic monitoring and management

The built-in dashboard shows live traffic, path health, peer paths and active
connections. Inspect which carriers are working and manage routing, DNS and
configuration from the same interface.

![MPTUNNEL dashboard showing live traffic, path health and connections](docs/assets/dashboard.png)

Enable the authenticated local dashboard using the
[management setup](docs/OPERATIONS.md#management-api).

## Quick start

Download the archive for your platform from
[GitHub Releases](../../releases/latest) and use matching versions on both peers.
Generate one shared MPP credential, one shared transport key, and a separate
TLS identity:

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

A configured **path** names a remote endpoint and its carrier policy. A TCP path
opens a carrier group (three by default, adjustable with `max-tcp-carriers`);
each QUIC path creates one carrier. This example therefore has four carrier slots.
Add path entries to build larger sets, including multiple QUIC carriers; the
default session limit is 64 carrier slots. Paths use the operating system's
routing unless you configure source addresses and routes.

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
| Send path, probe, or address-change notifications | [Webhooks](docs/WEBHOOKS.md) |
| Run as a service and collect logs | [Runtime supervision](docs/OPERATIONS.md#runtime-supervision) |
| Upgrade an existing deployment | [Peer compatibility and upgrades](docs/OPERATIONS.md#mpp-wire-version-upgrade) |

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
