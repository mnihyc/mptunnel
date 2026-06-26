# Operations

## Platform Check

Use the platform command before installing a service or enabling TUN mode:

```bash
mptunnel platform
```

It prints the current OS/architecture, the TUN backend, privilege expectations, current TUN device status when it can be detected safely, the native service manager, and the release target matrix.

## Privileges

SOCKS5 and HTTP CONNECT ingress can run as an ordinary user when binding unprivileged local ports. TUN mode needs elevated network privileges because it creates/configures a virtual network device.

Linux:

- TUN backend: `/dev/net/tun` through `tun-rs`.
- TUN privilege: run with `CAP_NET_ADMIN` or an equivalent service capability.
- Binding ports below 1024 needs `CAP_NET_BIND_SERVICE`.
- The supplied systemd client unit grants `CAP_NET_ADMIN`; the server unit grants `CAP_NET_BIND_SERVICE`.

macOS:

- TUN backend: utun through `tun-rs`.
- TUN and route/DNS configuration require administrator-approved launchd setup.
- Use the supplied launchd plists as service templates.

Windows:

- TUN backend: Wintun through `tun-rs`.
- TUN mode requires Administrator rights and the Wintun driver.
- Use `packaging/windows/install-service.ps1` from an elevated PowerShell session.

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
  --ingress socks5 \
  --listen 127.0.0.1:1080 \
  --listen '[::1]:1080'
```

`MPTUNNEL_LISTEN` accepts comma-separated socket addresses:

```text
MPTUNNEL_LISTEN=127.0.0.1:1080,[::1]:1080
```

Server path bindings use the same explicit model through repeated or comma-separated `--bind-path` values, for example `tcp://0.0.0.0:443`, `tcp://[::]:443`, `udp://0.0.0.0:443`, and `udp://[::]:443`.

## Packaging

Local release packaging:

```bash
scripts/package-release.sh --target x86_64-unknown-linux-gnu
pwsh scripts/package-release.ps1 -Target x86_64-pc-windows-msvc
```

Each package contains:

- `mptunnel` or `mptunnel.exe`
- `README.md`
- `LICENSE`
- `packaging/`
- a SHA-256 checksum next to the archive

Release targets:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`
- `aarch64-pc-windows-msvc`

## Test Policy

Normal build, format, clippy, unit, and integration checks run on the host.

Lab tests that create TUN devices, change routes, alter DNS settings, bind privileged service state, or otherwise mutate host network/device state must run in Docker or an equivalent isolated environment.
