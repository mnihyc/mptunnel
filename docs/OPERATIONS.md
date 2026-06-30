# Operations

## Platform Check

Use the platform command before installing a service or enabling TUN mode:

```bash
mptunnel platform
```

It prints the current OS/architecture, the TUN backend, privilege expectations, current TUN device status when it can be detected safely, the native service manager, and the release target matrix.

## Maintainability Gate

Run the warning-only line-count check before expanding large modules:

```bash
scripts/check-line-counts.sh
```

The threshold is 2,000 lines for tracked source and public documentation files. Files above the threshold should be split by cohesive ownership, with narrow module visibility, instead of accumulating unrelated runtime, test, or documentation concerns in one place.

## Privileges

SOCKS5 and HTTP CONNECT ingress can run as an ordinary user when binding unprivileged local ports. TUN mode needs elevated network privileges because it creates/configures a virtual network device.

Linux:

- TUN backend: `/dev/net/tun` through `tun-rs`.
- TUN privilege: run with `CAP_NET_ADMIN` or an equivalent service capability.
- Binding ports below 1024 needs `CAP_NET_BIND_SERVICE`.

macOS:

- TUN backend: utun through `tun-rs`.
- TUN and route/DNS configuration require administrator-approved service or launchd setup.

Windows:

- TUN backend: Wintun through `tun-rs`.
- TUN mode requires Administrator rights and the Wintun driver.

## Service Mode

Service managers should run `mptunnel` with:

```bash
--service-mode --supervise
```

`--service-mode` makes service intent explicit. `--supervise` restarts the runtime after top-level listener/device failures using exponential backoff.

Supervisor knobs:

- `--restart-backoff-ms` / `MPTUNNEL_RESTART_BACKOFF_MS`
- `--restart-max-backoff-ms` / `MPTUNNEL_RESTART_MAX_BACKOFF_MS`
- `--max-restarts` / `MPTUNNEL_MAX_RESTARTS`

Use service-manager restart policies as the outer process guard and `--supervise` as the in-process guard for recoverable listener/device failures.

## Dual-Stack Networking

Configure IPv4 and IPv6 listeners explicitly instead of depending on operating-system dual-stack socket defaults:

```bash
mptunnel client \
  --listen 127.0.0.1:1080 \
  --listen '[::1]:1080'
```

`MPTUNNEL_LISTEN` accepts comma-separated socket addresses:

```text
MPTUNNEL_LISTEN=127.0.0.1:1080,[::1]:1080
```

When SOCKS5 and HTTP CONNECT ingress run in the same client process, use the HTTP-specific listener flag and keep `--listen`/`--socks5-listen` for SOCKS5:

```bash
mptunnel client \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080
```

The matching environment variables are `MPTUNNEL_SOCKS5_LISTEN` and `MPTUNNEL_HTTP_LISTEN`.

Local proxy authentication is off by default. To require browser/tool authentication, set both `--proxy-username` and `--proxy-password`; service deployments can use `MPTUNNEL_PROXY_USERNAME` and `MPTUNNEL_PROXY_PASSWORD`. SOCKS5 uses username/password negotiation, and HTTP CONNECT uses Basic proxy authentication.

Server path bindings use the same explicit model through repeated or comma-separated `--bind-path` values, for example `tcp://0.0.0.0:443`, `tcp://[::]:443`, `udp://0.0.0.0:443`, and `udp://[::]:443`.

UDP targets are not limited to UDP underlay. mptunnel prefers UDP-target relay over UDP carrier paths when schedulable UDP paths exist, but it can carry UDP-target datagram flow frames over encrypted TCP underlay as best-effort relay. Use UDP underlay for the lowest latency and fastest packet-level recovery; keep TCP underlay available when reachability is more important than datagram-native behavior.

## Config File And Management API

Running `mptunnel` with no arguments reads `./config.toml`. Use `--config PATH` or `-c PATH` to select a different TOML file, and use `--config PATH --check-config` to validate it without opening listeners. The file is role-free and V2Ray-style: `[[inbounds]]` accept SOCKS5, HTTP, TUN, or MPP traffic; `[[outbounds]]` forward to MPP, direct, source-IP bound direct, SOCKS5, HTTP CONNECT, or HTTP CONNECT UDP. Each entry can have a tag. An inbound selects either one outbound with `outbound = "tag"` or one routing balancer with `balancer = "tag"`.

MPP endpoints and security belong to `protocol = "mpp"` outbounds. Routing balancers reference outbound tags: `combined-mpp` combines MPP outbounds, while `sequence` and `random` select among egress outbounds. DNS resolver policy belongs to the egress outbound that resolves target names, usually as an inline `dns = { ... }` table on that outbound.

The release management API is enabled only when `--management-listen` or `[management].listen` is configured. Keep it on loopback unless an operator network explicitly protects it. Set `--management-token` or `[management].token` for bearer-token authentication. Release endpoints expose JSON status and bounded traffic trends without lab-only component timing. When one process has both local inbounds using MPP outbounds and MPP inbounds using egress outbounds, the API reports a self-contained node snapshot with both service groups:

```bash
curl -H 'Authorization: Bearer replace-with-token' http://127.0.0.1:7600/status
curl -H 'Authorization: Bearer replace-with-token' http://127.0.0.1:7600/paths
```

Client-side path control uses the scheduler-visible path health record:

```bash
curl -X POST \
  -H 'Authorization: Bearer replace-with-token' \
  -H 'Content-Type: application/json' \
  --data '{"underlay":"udp","index":0,"state":"disabled"}' \
  http://127.0.0.1:7600/control/path
```

For node configs with multiple MPP outbounds or balancers, use the configured
target tag instead of an array index:

```bash
curl -X POST \
  -H 'Authorization: Bearer replace-with-token' \
  -H 'Content-Type: application/json' \
  --data '{"client_tag":"edge-mpp","underlay":"udp","index":0,"state":"disabled"}' \
  http://127.0.0.1:7600/control/path
```

## Encryption

Encrypted transport is the default and uses `aes-256-gcm` unless `--cipher chacha20-poly1305` or `MPTUNNEL_CIPHER=chacha20-poly1305` is set on both peers. Cipher suites are not negotiated; client and server must be configured consistently. `--secret` / `MPTUNNEL_SECRET` must be a random UUID or at least 32 bytes of high-entropy secret text. Runtime transport and HMAC keys are derived from that secret with mptunnel-specific context separation. Authenticated session/path control frames carry issue times and are rejected outside `--auth-freshness-window-seconds` / `MPTUNNEL_AUTH_FRESHNESS_WINDOW_SECONDS`, default `300`.

## Packaging

Local release packaging:

```bash
scripts/package-release.sh --target x86_64-unknown-linux-musl
pwsh scripts/package-release.ps1 -Target x86_64-pc-windows-msvc
```

Each package contains:

- `mptunnel` or `mptunnel.exe`
- `README.md`
- `LICENSE`
- `docs/`
- a SHA-256 checksum next to the archive

Release archives intentionally do not include `mptunnel-bench`, Docker lab scripts, generated lab results, service templates, or other developer-only tooling. The product binary is built as `--bin mptunnel`.

Linux release artifacts use musl targets, not glibc targets, so they do not depend on a host glibc baseline:

```bash
scripts/package-release.sh --target x86_64-unknown-linux-musl
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
  scripts/package-release.sh --target aarch64-unknown-linux-musl
```

Release targets:

- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`
- `aarch64-pc-windows-msvc`

## Tag Releases

GitHub Actions publishes releases from tags that match `v*`, for example:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow runs:

- format, clippy, and tests for the product code
- Linux packages through musl Rust targets
- macOS and Windows packages through the native packaging scripts
- artifact upload for all target archives and `.sha256` files
- GitHub Release publication only when the workflow was triggered by a tag

Manual `workflow_dispatch` runs execute the same checks and package jobs, but the publish job is skipped unless the ref is a tag.

Benchmarks and Docker lab checks are manual-only processes. They are not part of CI, release checks, package jobs, or tag publication.

## Test Policy

Normal build, format, clippy, unit, and integration checks run on the host.

Lab tests that create TUN devices, change routes, alter DNS settings, bind privileged service state, or otherwise mutate host network/device state must run in Docker or an equivalent isolated environment.
