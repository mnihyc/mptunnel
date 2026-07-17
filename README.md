# mptunnel

`mptunnel` is an encrypted multipath proxy and tunnel written in Rust. It
carries reliable streams and datagrams over TCP, QUIC over UDP, or both while
keeping TCP and QUIC congestion control and recovery in their native transport
layers.

The shared Multipath Proxy Protocol (MPP) data layer provides directional data
sequence numbers, Data ACKs, flow control, measured path selection,
cross-path reinjection, and failover. The current wire format is MPP v2 and is
not compatible with protocol v1.

> **Release status:** v0.1.0 is an initial release. The protocol is custom and
> has not received an independent security audit. Review the
> [security and platform limitations](#security-and-limitations) before
> exposing a deployment.

## Why mptunnel

- Aggregate healthy paths for sustained reliable traffic without assigning a
  permanent role to a link.
- Prefer measured completion cost for latency-sensitive work, then use
  additional measured capacity when demand grows.
- Preserve one reliable data sequence space across TCP and QUIC carrier paths.
- Carry SOCKS5, HTTP CONNECT, TUN TCP/UDP, and MPP datagram flows.
- Continue a reliable flow across a failed carrier through bounded
  data-level reinjection.
- Inspect paths, sessions, flows, and forwarded traffic through a local
  management API and embedded dashboard.

## Measured performance

The table below is one controlled pre-release Linux Docker reference cohort,
not an Internet-wide claim or a rebinding of old baselines to the final binary.
Each shaped high-bandwidth path was 500 Mbps with 180 ms one-way delay, 20 ms
jitter, and no configured loss. Each row used one 10-second reliable bulk flow;
multipath rows used five equal paths.

| Reliable carrier | Paths | Download | Upload | Multipath gain |
| --- | ---: | ---: | ---: | ---: |
| MPP over TCP | 1 | 159.142 Mbps | 167.440 Mbps | - |
| MPP over TCP | 5 | 246.717 Mbps | 268.819 Mbps | +55.0% / +60.5% |
| MPP over QUIC | 1 | 208.399 Mbps | 235.062 Mbps | - |
| MPP over QUIC | 5 | 326.844 Mbps | 430.732 Mbps | +56.8% / +83.2% |

In the same cohort, five-path MPTCP measured 97.120 Mbps download and
148.929 Mbps upload; single-path direct TCP measured 171.006/168.156 Mbps and
VMess measured 172.267/168.776 Mbps. These results show aggregation in this
specific high-delay topology. They do not imply the same ranking on every
network.

See [Performance evidence](docs/PERFORMANCE.md) for the final-runtime guard,
full historical baseline table, failover results, Wine comparison, exact
identities, methodology, and limitations.

## Install

Download the archive for your platform and its adjacent checksum from
[Releases](https://github.com/mnihyc/mptunnel/releases). Release archives are
produced for:

- Linux x86_64 and aarch64 using musl
- Windows x86_64 and aarch64 using MSVC
- macOS x86_64 and aarch64
- Android aarch64 as a best-effort shell proxy binary

Verify an archive before extracting it:

```bash
sha256sum -c mptunnel-0.1.0-x86_64-unknown-linux-musl.tar.gz.sha256
```

On macOS, use `shasum -a 256 -c <checksum-file>`. On Windows, compare
`Get-FileHash <archive> -Algorithm SHA256` with the published checksum.
The release-wide `SHA256SUMS` file covers every other published asset.

## Quick start

Generate one secret and transfer it securely to both endpoints:

```bash
openssl rand -hex 32
```

PowerShell:

```powershell
(New-Guid).Guid
```

Start the server, replacing `<secret>`:

```bash
mptunnel --secret '<secret>' server \
  --bind-path tcp://0.0.0.0:4433 \
  --bind-path udp://0.0.0.0:4433 \
  --outbound direct
```

Start the client with the same secret and the server's reachable address:

```bash
mptunnel --secret '<secret>' client \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://server.example:4433 \
  --path udp://server.example:4433
```

The client now exposes SOCKS5 on `127.0.0.1:1080` and HTTP CONNECT on
`127.0.0.1:8080`. TCP and UDP listeners can share a numeric port because
they are separate transports. Established logical streams are retained for
five minutes when every carrier is unavailable; change that policy with
`--session-retention-timeout-ms` or `[session].retention_timeout_ms`.

For repeatable deployments, put the same graph in `config.toml`. Tagged
`[[inbounds]]` select tagged `[[outbounds]]` or routing balancers; MPP
endpoints and security are scoped to their MPP inbound or outbound. Validate
without opening listeners, then start it:

```bash
mptunnel --config ./config.toml --check-config
mptunnel --config ./config.toml
```

Use `mptunnel --help`, `mptunnel client --help`, and
`mptunnel server --help` for the complete CLI and environment-variable
surface. Listener, path, bind, and resolver options may be repeated for
explicit IPv4 and IPv6 addresses.

## Dashboard

The management server is opt-in, token-protected, and restricted to loopback
listeners. Add these options to either the client or server command:

```bash
--management-listen 127.0.0.1:7600 \
--management-token '<separate-management-token>' \
--management-dashboard
```

Open `http://127.0.0.1:7600/`. The dashboard shows current path state and
metrics, forwarded traffic rates and history, sessions, and active flows.
Runtime data and controls are under `/api/`; only the static page and
`GET /api/health` are public.

![mptunnel management dashboard](docs/assets/dashboard.png)

Peer path diagnostics are disabled by default. Enable
`--management-allow-peer-diagnostics` on the endpoint that may answer a
request. The same 1 s, 5 s, 30 s, or manual-only dashboard cadence refreshes
local status and the selected authenticated peer without overlapping cycles.
The response does not expose endpoints, targets, credentials, or other sessions. See
[Operations](docs/OPERATIONS.md#management-api) for the API contract and
remote-access guidance.

## Protocol model

```text
application ingress
    MPP reliable stream or datagram
        directional data sequence, Data ACK, receive window, reinjection
            available-first path scheduler
                TCP carrier controller | QUIC carrier controller
                    network
```

MPP borrows connection-level sequence, Data ACK, receive-window, reinjection,
and backup-path principles from MPTCP, plus directional path usage from
Multipath QUIC. It does not replace either transport's congestion controller or
loss recovery.

Configured `backup`, `expensive`, `no-bulk`, and related URI values are
operator restrictions. RTT, jitter, and rate URI values are startup priors.
Neither is a permanent traffic classification: live observations and current
demand rank eligible paths.

The normative wire contract is in [RFC.md](RFC.md). The implementation owners
and data flow are in [Architecture](docs/ARCHITECTURE.md).

## Platform support

| Platform | Release scope |
| --- | --- |
| Linux x86_64/aarch64 | Proxy and TUN runtime; Linux x86_64 is the primary end-to-end lab platform. TUN setup requires host network privileges. |
| Windows x86_64/aarch64 | Proxy runtime and Wintun packaging. GitHub Actions builds, tests, and packages MSVC x86_64/aarch64; a GNU-target PE has portable TCP and basic-UDP QUIC proxy evidence under Wine. Native Wintun and throughput/failover remain unmeasured. |
| macOS x86_64/aarch64 | Proxy and packet-device code is built for release; native packet-device integration is best effort and not represented by the Linux lab results. |
| Android aarch64 | Best-effort shell proxy binary only. The archive is not an APK, AAR, or `VpnService` integration. |

Android VPN applications must provide the packet-device and carrier-network
integration, obtain `VpnService` consent, and protect or bind every carrier
socket so it does not re-enter the VPN. Run `mptunnel platform` for the
current host report and read [Operations](docs/OPERATIONS.md#platform-check)
before enabling TUN.

Native TCP telemetry is adapted per platform: `TCP_INFO` on Linux/Android,
`TCP_CONNECTION_INFO` on macOS, and `SIO_TCP_INFO` where Windows provides
it. Correctness does not depend on telemetry. When it is unavailable,
`mptunnel` uses portable Data ACK and socket-backpressure evidence and prints
a performance warning; high-bandwidth, high-delay upload may be slower.

QUIC normally uses Quinn's native UDP adapter. On Windows compatibility layers
that lack optional ECN or segmentation socket features, `mptunnel` falls back
to basic datagram I/O and prints a separate performance warning. Quinn still
owns QUIC congestion control, packet recovery, and timeouts; native Windows
uses the optimized adapter when its socket capabilities are available.

## Security and limitations

- Use a random UUID or at least 32 bytes of high-entropy text as the shared
  secret. Do not reuse the management token as the MPP secret.
- TCP carriers use an authenticated MPP record layer. QUIC carriers use TLS 1.3
  and QUIC packet protection through rustls.
- Keep the built-in management listener on loopback. Use an SSH tunnel or a
  same-host TLS reverse proxy for remote access.
- This is a new custom protocol and implementation without an independent
  cryptographic or application-security audit.
- Controlled Docker and Wine results do not prove real-Internet, native
  Windows, native macOS, Android VPN, or Wintun performance.
- The measured Linux runtime used the GNU target, while release Linux archives
  use musl; the Wine run used a GNU-target PE, while Windows release archives
  use MSVC. Those packaged targets are not performance-equivalent claims.

## Development

```bash
cargo build --locked --release --bin mptunnel
cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
scripts/check-line-counts.sh
```

The Docker lab changes networking only inside its namespaces. See
[Lab methodology](docs/LAB.md), [Developer benchmarks](docs/BENCHMARKS.md),
[Code structure](docs/CODE_STRUCTURE.md), and
[Release operations](docs/OPERATIONS.md). Contributions follow
[CONTRIBUTING.md](CONTRIBUTING.md); report vulnerabilities through
[SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
