# MPTUNNEL release package

Each archive contains one MPTUNNEL command-line binary, two editable
client/server examples, and the project license. It is the same MPTUNNEL binary
for client and server use. Windows archives additionally contain the signed
architecture-matched Wintun runtime and its required license.

## First run

Check the binary:

```text
./mptunnel --version
./mptunnel platform
```

On Windows, use `.\mptunnel.exe` instead. Keep `wintun.dll` beside the
executable when using the Windows packet-device integration.

For a quick proxy-only trial, generate one credential file and a server TLS
identity. Securely copy `mpp-credential.key` and the public
`server-certificate.pem` to the client:

```text
umask 077
openssl rand -hex 32 > mpp-credential.key
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=server.example" \
  -addext "subjectAltName=DNS:server.example" \
  -keyout server-private-key.pem -out server-certificate.pem
```

Start the server:

```text
./mptunnel --credential-secret-file ./mpp-credential.key \
  server \
  --tls-certificate-chain ./server-certificate.pem \
  --tls-private-key ./server-private-key.pem \
  --bind-path tcp://0.0.0.0:4433 \
  --bind-path udp://0.0.0.0:4433 \
  --outbound-protocol direct
```

Start the client:

```text
./mptunnel --credential-secret-file ./mpp-credential.key \
  client \
  --tls-server-name server.example \
  --tls-pinned-certificate ./server-certificate.pem \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://server.example:4433 \
  --path udp://server.example:4433
```

For a persistent setup, copy `examples/client.toml` or
`examples/server.toml`, replace every placeholder, supply the referenced TLS
certificate files, and validate before starting:

```text
./mptunnel --config ./config.toml --check-config
./mptunnel --config ./config.toml
```

Configuration-relative certificate paths resolve beside the selected TOML
file. Run `./mptunnel --help` for the simple CLI surface.

## Service helpers

Linux archives include `service/systemd/mptunnel.service`. It expects:

- the executable at `/usr/local/bin/mptunnel`;
- configuration at `/etc/mptunnel/config.toml`; and
- a locked-down `mptunnel` system user and group.

The unit grants only the network capabilities needed for managed TUN and
privileged listener operation. Remove its capability lines for a proxy-only
deployment. The service account must be able to replace `config.toml` in
`/etc/mptunnel` so supported management-API changes can be persisted
atomically. Before enabling the unit, create the account and directory with
equivalent ownership and permissions:

```text
useradd --system --home-dir /var/lib/mptunnel --shell /usr/sbin/nologin mptunnel
install -d -o mptunnel -g mptunnel -m 0750 /etc/mptunnel
install -o mptunnel -g mptunnel -m 0600 ./config.toml /etc/mptunnel/config.toml
```

Use the platform's equivalent account-management command when `useradd` is
unavailable. Keep referenced credential and TLS private-key files readable
only by that account.

The macOS archive intentionally contains no launchd definition. MPTUNNEL runs
as a foreground command-line process; a persistent proxy deployment should use
a host-specific, non-root supervisor definition with explicit configuration,
working-directory, and bounded log handling. A launchd job is not a Network
Extension and cannot make managed macOS VPN mode available. Native packet flow
and DNS publication still require the host adapter described by `mptunnel
platform`.

MPTUNNEL is a foreground console process on Windows. A native Windows Service
wrapper and installer are not included. The Android archive is a command-line
binary, not an APK or one-click `VpnService` application. Android VPN hosts
must establish the TUN descriptor, bind carrier sockets to the selected
network, protect every carrier, target, proxy, and DNS socket before I/O, and
use external TUN host mode. Low-level host-provider APIs reject process-managed
TUN mode because they do not own OS route/DNS publication.

## Downloads and checksums

Release assets use stable, version-independent names:

- `mptunnel-linux-amd64.tar.gz`
- `mptunnel-linux-arm64.tar.gz`
- `mptunnel-windows-amd64.zip`
- `mptunnel-windows-arm64.zip`
- `mptunnel-macos-amd64.zip`
- `mptunnel-macos-arm64.zip`
- `mptunnel-android-arm64.tar.gz`

The release page supplies one `SHA256SUMS` manifest for those seven archives.
On Linux, verify one downloaded archive with:

```text
asset=mptunnel-linux-amd64.tar.gz
grep "  ${asset}$" SHA256SUMS | sha256sum -c -
```

On macOS, replace `sha256sum` with `shasum -a 256`. On Windows, compare
`Get-FileHash <archive> -Algorithm SHA256` with the matching line in
`SHA256SUMS`.

The release page identifies the version. The extracted binary also reports it
with `--version`; filenames deliberately remain stable so automated
`releases/latest/download/...` links do not change shape.

## Important limits

MPTUNNEL uses a new custom protocol and has not received an independent
security audit. Managed VPN operation changes host routes and DNS state and
requires platform privileges. Read the repository's security, operations, and
performance documentation before exposing a deployment.
