# MPTUNNEL release package

Each archive contains one MPTUNNEL command-line binary, two editable
client/server examples, and this package guide. It is the same MPTUNNEL binary
for client and server use. Windows archives additionally contain the signed,
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
`server-cert.pem` to the client:

```text
umask 077
openssl rand -hex 32 > mpp-credential.key
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=mptunnel.example" \
  -addext "subjectAltName=DNS:mptunnel.example" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth" \
  -keyout server-key.pem -out server-cert.pem
```

Start the server:

```text
./mptunnel --credential-secret-file ./mpp-credential.key \
  server \
  --tls-certificate-chain ./server-cert.pem \
  --tls-private-key ./server-key.pem \
  --bind-path tcp://0.0.0.0:7443 \
  --bind-path udp://0.0.0.0:7443 \
  --outbound-protocol direct
```

Start the client:

```text
./mptunnel --credential-secret-file ./mpp-credential.key \
  client \
  --tls-pinned-certificate ./server-cert.pem \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://server.example.com:7443 \
  --path udp://server.example.com:7443
```

The certificate name defaults to `mptunnel.example`. Optional transport
protection uses a separate raw 32-byte endpoint secret, passed to both commands
as `--transport-secret-file`:

```text
openssl rand -out mpp-transport.key 32
# add to the server command: --transport-secret-file ./mpp-transport.key
# add to the client command: --transport-secret-file ./mpp-transport.key
```

Do not reuse an MPP client credential as this endpoint-wide secret.

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
unavailable. Keep referenced credential, transport-secret, and TLS private-key
files readable only by that account.

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

## Downloads

Every release uses versioned bundle names:

- `mptunnel-<version>-linux-amd64.tar.gz`
- `mptunnel-<version>-linux-arm64.tar.gz`
- `mptunnel-<version>-windows-amd64.zip`
- `mptunnel-<version>-windows-arm64.zip`
- `mptunnel-<version>-macos-amd64.zip`
- `mptunnel-<version>-macos-arm64.zip`
- `mptunnel-<version>-android-arm64.tar.gz`

The release's `version.json` records its schema, product, version, tag, source
commit, and the exact name and tag-specific GitHub download URL of every
bundle. GitHub supplies the digest for each uploaded asset. The extracted
binary also reports its version with `--version`.

Once a draft is published, GitHub release immutability freezes its tag, title,
notes, and assets. Corrections therefore use a new version rather than
replacing an existing release.

## Important limits

MPTUNNEL uses a new custom protocol and has not received an independent
security audit. Managed VPN operation changes host routes and DNS state and
requires platform privileges. Read the repository's security, operations, and
performance documentation before exposing a deployment.
