# mptunnel

`mptunnel` is a Rust application for encrypted multipath proxy transport.

## Build

```bash
cargo build
cargo test
```

Cross-platform release archives are produced by the packaging scripts:

```bash
scripts/package-release.sh --target x86_64-unknown-linux-musl
pwsh scripts/package-release.ps1 -Target x86_64-pc-windows-msvc
```

CI checks Linux, macOS, and Windows, plus amd64/aarch64 Rust targets. Tag builds such as `v0.1.0` publish release archives with SHA-256 checksums. Linux tag artifacts use musl targets and do not depend on glibc. Release process details are in `docs/OPERATIONS.md`.

## Configuration Check

Platform, TUN, service, and release-target report:

```bash
mptunnel platform
```

Developer-only benchmark and ablation tool:

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict
cargo run --manifest-path lab/benchmarks/Cargo.toml -- ablation --format json
```

Benchmarks are manual lab tooling outside the root crate and are not part of CI, release, package, or normal build workflows.

Docker-only heterogeneous network lab:

```bash
lab/run-heterogeneous-ablation.sh
```

The lab emulates low-latency, cross-continent high-bandwidth, and poor-Internet paths at the same time, then records direct, single-path, multipath, UDP, and failover comparison results under `lab/results/`. It mutates Docker network namespaces only.

Client-side proxy ingress:

```bash
mptunnel --check-config client \
  --secret replace-with-shared-secret \
  --ingress socks5 \
  --listen 127.0.0.1:1080 \
  --listen '[::1]:1080' \
  --path-probe-interval-ms 10000 \
  --path-probe-timeout-ms 2000 \
  --default-tcp-class auto \
  --tcp-class-rule 22=control \
  --tcp-class-rule 8443=bulk \
  --path 'tcp://203.0.113.10:443?srtt-ms=20&rate-mbps=30&low-latency=true' \
  --path 'tcp://203.0.113.11:443?srtt-ms=180&rate-mbps=300' \
  --path 'udp://203.0.113.10:443?srtt-ms=20&rate-mbps=30'
```

Proxy ingress accepts repeated or comma-separated `--listen` addresses, so IPv4 and IPv6 listeners can be configured explicitly without relying on platform-specific dual-stack socket defaults.

Client-side TUN L4 ingress with dual-stack addressing and explicit DNS forwarding:

```bash
mptunnel --check-config client \
  --secret replace-with-shared-secret \
  --ingress tun-l4 \
  --tun-name mptun0 \
  --tun-ipv4 10.88.0.1 \
  --tun-ipv4-prefix 24 \
  --tun-ipv6 fd00:88::1 \
  --tun-ipv6-prefix 64 \
  --tun-mtu 1500 \
  --tun-dns-resolver 1.1.1.1:53 \
  --tun-dns-resolver '[2606:4700:4700::1111]:53' \
  --path 'tcp://203.0.113.10:443?srtt-ms=20&rate-mbps=30&low-latency=true' \
  --path 'udp://203.0.113.10:443?srtt-ms=20&rate-mbps=30'
```

For IPv6-only TUN ingress, add `--tun-disable-ipv4` and provide `--tun-ipv6`.

Server-side path listener and direct outbound:

```bash
mptunnel --check-config server \
  --secret replace-with-shared-secret \
  --bind-path tcp://0.0.0.0:443 \
  --bind-path 'tcp://[::]:443' \
  --bind-path udp://0.0.0.0:443 \
  --bind-path 'udp://[::]:443' \
  --outbound direct \
  --outbound-dns-resolver 1.1.1.1:53 \
  --outbound-dns-resolver '[2606:4700:4700::1111]:53' \
  --outbound-dns-strategy ipv4-then-ipv6
```

Server-side upstream HTTP proxy outbound:

```bash
mptunnel --check-config server \
  --secret replace-with-shared-secret \
  --bind-path tcp://0.0.0.0:443 \
  --bind-path 'tcp://[::]:443' \
  --bind-path udp://0.0.0.0:443 \
  --bind-path 'udp://[::]:443' \
  --outbound http-connect-udp \
  --upstream-http 127.0.0.1:8080
```

Internal transport is encrypted by default. Plaintext lab mode requires an explicit security mode and acknowledgement. It removes confidentiality for lab use only; session/path integrity remains authenticated:

```bash
mptunnel --check-config \
  --secret replace-with-shared-secret \
  --security plaintext-lab \
  --i-understand-this-is-insecure \
  client \
  --path tcp://127.0.0.1:7443
```

Path URI query parameters seed the runtime scheduler before live health observations exist. Supported path hints are:

- `srtt-ms`, `rtt-ms`, `jitter-ms`
- `rate-bps`, `rate-kbps`, `rate-mbps`, `rate=unknown`, `rate=unlimited`
- `mtu`, `mtu-bytes`, `payload-mtu`
- `backup`, `expensive`, `low-latency`, `bulk-allowed`, `bulk`, `no-bulk`, `probe-only`, `no-udp`

Boolean hints accept `true`, `false`, `1`, `0`, `yes`, `no`, `on`, and `off`; bare boolean hints mean `true`.

TCP ingress traffic class policy is port-based at stream open and adaptive during relay. `--default-tcp-class` accepts `auto`, `control`, `interactive`, `bulk`, or `background`; the default `auto` starts TCP streams latency-first and promotes sustained larger flows toward bulk scheduling and larger adaptive inflight/chunk budgets from live BDP, queue, jitter, and loss observations. `--tcp-class-rule` accepts repeated or comma-separated `port=class` and `port:class` rules. Explicit non-`auto` rules are fixed overrides. `realtime-datagram` is reserved for UDP datagram flows and is not valid for TCP class rules.

Global resource limits are configurable and validated before runtime starts:

```bash
mptunnel --check-config \
  --secret replace-with-shared-secret \
  --service-mode \
  --supervise \
  --restart-backoff-ms 1000 \
  --restart-max-backoff-ms 30000 \
  --max-frame-bytes 1048576 \
  --max-payload-bytes 1048512 \
  --max-ack-ranges 256 \
  --max-paths 64 \
  --max-streams 65536 \
  --max-stream-window-bytes 16777216 \
  --max-repair-bytes 16777216 \
  --max-reorder-bytes 16777216 \
  --max-datagram-queue-bytes 4194304 \
  --max-tcp-path-inflight-bytes 4194304 \
  --max-tcp-relay-chunk-bytes 262144 \
  --tcp-path-heartbeat-interval-ms 10000 \
  --tcp-path-heartbeat-timeout-ms 30000 \
  client \
  --path tcp://127.0.0.1:7443
```

Common environment variables:

- `MPTUNNEL_LOG`
- `MPTUNNEL_SECURITY`
- `MPTUNNEL_SECRET`
- `MPTUNNEL_CHECK_CONFIG`
- `MPTUNNEL_SERVICE_MODE`
- `MPTUNNEL_SUPERVISE`
- `MPTUNNEL_RESTART_BACKOFF_MS`
- `MPTUNNEL_RESTART_MAX_BACKOFF_MS`
- `MPTUNNEL_MAX_RESTARTS`
- `MPTUNNEL_PATHS`
- `MPTUNNEL_BIND_PATHS`
- `MPTUNNEL_INGRESS`
- `MPTUNNEL_LISTEN`
- `MPTUNNEL_TUN_NAME`
- `MPTUNNEL_TUN_IPV4`
- `MPTUNNEL_TUN_DISABLE_IPV4`
- `MPTUNNEL_TUN_IPV4_PREFIX`
- `MPTUNNEL_TUN_IPV4_GATEWAY`
- `MPTUNNEL_TUN_IPV6`
- `MPTUNNEL_TUN_IPV6_PREFIX`
- `MPTUNNEL_TUN_MTU`
- `MPTUNNEL_TUN_DISABLE_ICMP`
- `MPTUNNEL_TUN_DNS_RESOLVERS`
- `MPTUNNEL_TUN_DNS_TTL_MS`
- `MPTUNNEL_PATH_PROBE_INTERVAL_MS`
- `MPTUNNEL_PATH_PROBE_TIMEOUT_MS`
- `MPTUNNEL_DEFAULT_TCP_CLASS`
- `MPTUNNEL_TCP_CLASS_RULES`
- `MPTUNNEL_OUTBOUND`
- `MPTUNNEL_OUTBOUND_BIND_IP`
- `MPTUNNEL_UPSTREAM_SOCKS5`
- `MPTUNNEL_UPSTREAM_HTTP`
- `MPTUNNEL_OUTBOUND_DNS_RESOLVERS`
- `MPTUNNEL_OUTBOUND_DNS_STRATEGY`
- `MPTUNNEL_OUTBOUND_DNS_TIMEOUT_MS`
- `MPTUNNEL_MAX_FRAME_BYTES`
- `MPTUNNEL_MAX_PAYLOAD_BYTES`
- `MPTUNNEL_MAX_ACK_RANGES`
- `MPTUNNEL_MAX_PATHS`
- `MPTUNNEL_MAX_STREAMS`
- `MPTUNNEL_MAX_STREAM_WINDOW_BYTES`
- `MPTUNNEL_MAX_REPAIR_BYTES`
- `MPTUNNEL_MAX_REORDER_BYTES`
- `MPTUNNEL_MAX_DATAGRAM_QUEUE_BYTES`
- `MPTUNNEL_MAX_TCP_PATH_INFLIGHT_BYTES`
- `MPTUNNEL_MAX_TCP_RELAY_CHUNK_BYTES`
- `MPTUNNEL_TCP_PATH_HEARTBEAT_INTERVAL_MS`
- `MPTUNNEL_TCP_PATH_HEARTBEAT_TIMEOUT_MS`

## Current Runtime Scope

The current runtime exposes local SOCKS5, HTTP CONNECT, and TUN L4 ingress.

SOCKS5 and HTTP CONNECT ingress support explicit IPv4/IPv6 dual-stack operation through repeated `--listen` values such as `127.0.0.1:1080` and `[::1]:1080`. TUN, server bind paths, outbound DNS resolvers, upstream proxy endpoints, and direct target addresses all preserve IPv4 and IPv6 socket addresses; DNS resolution order is controlled by `--outbound-dns-strategy`.

TCP ingress uses encrypted TCP-underlay paths and can reach remote TCP targets through direct outbound, bind-source-IP direct outbound, upstream SOCKS5 CONNECT, upstream HTTP CONNECT, or `http-connect-udp` using ordinary CONNECT for TCP targets. Each configured TCP path is managed as a lazy persistent encrypted path session, and multiple local SOCKS5/HTTP CONNECT streams can multiplex over the same authenticated TCP path instead of creating a fresh internal TCP connection for every proxy stream. When several TCP paths are configured, stream setup classifies each target with the configured TCP traffic policy, then uses scheduler ETA scoring from path hints, current path health, observed delivery rate, active stream load, and the selected class. The default `auto` policy opens streams as interactive, promotes sustained flows to bulk from live BDP/resource observations, and can detach and reattach the same logical stream to a better bulk path without closing the remote TCP target. It retries the next schedulable path after path-level open failures. Active TCP streams use one logical stream ID across TCP path sessions; when a path fails, the client can re-open that logical stream on a survivor path and replay unacknowledged data from the reliable-stream repair cache, while the server reattaches the same outbound TCP connection through its shared stream registry. Successful opens feed measured latency and live load back into later path choices; completed relays feed measured payload delivery rate after enough useful payload has been observed and release that load, while failed opens put the path into a short cooldown before probing resumes. The relay caps unacknowledged TCP-underlay stream payload with `--max-tcp-path-inflight-bytes`, and uses adaptive effective inflight/chunk budgets under that cap from live BDP, queue, jitter, loss, and traffic class, so local reads pause until end-to-end tunnel ACKs free budget instead of burying unlimited data in a kernel TCP send buffer. TCP path sessions send encrypted internal `PING`/`PONG` heartbeats using `--tcp-path-heartbeat-interval-ms` and `--tcp-path-heartbeat-timeout-ms`; a heartbeat timeout fails the path, releases live stream load, and lets later scheduling avoid that path until probes recover it. The TCP path-session reader is isolated from writer/heartbeat scheduling so encrypted frame reads are not cancelled mid-frame. The client also runs bounded authenticated path probes on the configured interval, using `PING`/`PONG` after `PATH_JOIN` so TCP path health can recover without opening remote target connections.

SOCKS5 UDP ASSOCIATE ingress uses authenticated encrypted UDP path sessions. It opens compact internal datagram flows per target, then sends repeated datagrams with flow ID, datagram ID, TTL, and payload without repeating target metadata. Datagrams are acknowledged with internal `DGRAM_FEEDBACK` ranges; client-side ACK observations update UDP path RTT, jitter, loss, and delivery-rate inputs for later scheduling, while response datagrams are acknowledged back to the server. When several UDP paths are configured, one local UDP association can keep multiple encrypted UDP path sessions active under one logical session ID. It opens additional eligible paths before paced reuse, uses a BBR-style runtime model from observed delivery rate, RTT, jitter, and loss to pace sends and set response timeouts, probes encrypted UDP path MTU before sending larger datagrams, records measured MTU in path health, and retries a timed-out datagram on a survivor path. Request blackholes mark the path failed; ACKed request/response-loss timeouts record packet loss without declaring the whole path dead. UDP session setup still uses scheduler inputs, adaptive health records, datagram freshness TTL, observed delivery rate, measured MTU, and active association load, then retries after path-level handshake failures. Closed associations feed measured datagram delivery rate after enough useful payload has been observed and release their scheduler load. The same bounded authenticated probe loop exercises UDP path handshakes and `PING`/`PONG` without opening datagram flows. Server UDP listeners demux peers on one bound socket into bounded per-peer encrypted session tasks.

TUN L4 ingress uses `tun-rs` for cross-platform TUN device creation and `netstack-smoltcp` for user-space TCP/UDP flow translation. TCP packets accepted from the TUN stack become encrypted internal reliable streams with `IngressKind::TunTcp`; UDP packets are demuxed by local/remote socket pair into bounded per-flow tasks and sent as encrypted internal datagram flows with `IngressKind::TunUdp`. TUN supports IPv4, IPv6, or dual-stack addressing. UDP port 53 traffic can be explicitly remapped to configured `--tun-dns-resolver` addresses while responses are written back to the TUN client as if they came from the original DNS destination, so DNS traffic can pass through TUN without relying on host resolver defaults.

UDP targets can be reached through direct UDP, bind-source-IP direct UDP, upstream SOCKS5 UDP ASSOCIATE, or upstream HTTP CONNECT-UDP. The `http-connect-udp` outbound performs the RFC 9298 HTTP/1.1 Upgrade handshake, requires a `101 Switching Protocols` response with capsule support, and carries UDP payloads in HTTP Datagram capsules. Plain `http-connect` outbound is TCP-only.

Server direct and bind-source outbounds resolve domain targets through `--outbound-dns-resolver` when configured, or the system resolver when no explicit resolver is supplied. `--outbound-dns-strategy` controls IPv4/IPv6 lookup ordering and filtering for dual-stack targets.

`--service-mode --supervise` is intended for systemd, launchd, Windows Service Control Manager, or another process supervisor. It restarts the runtime after top-level listener/device failures with exponential backoff controlled by `--restart-backoff-ms`, `--restart-max-backoff-ms`, and `--max-restarts`. Operational details are in `docs/OPERATIONS.md`.

## Scheduler Regression Gates

The deterministic simulator exercises the scheduler against heterogeneous path conditions before runtime changes are made. Current gates cover:

- bulk transfer aggregation efficiency across low-latency and high-bandwidth paths
- failover gap after path failure and chunk reinjection onto a survivor path
- interactive p95 latency while a bulk transfer is queued
- bulk tail penalty for heterogeneous RTT/bandwidth paths
- per-class priority and per-flow deficit scheduling
- per-path queued-byte pressure from scheduled payloads
- bulk tail avoidance that promotes the final bulk bytes onto latency-sensitive scoring
- duplication of small control/realtime packets onto a second close-ETA path
- shared-bottleneck suspicion that avoids a low-RTT path when a similar-RTT peer is already queued

## Developer Benchmark Gates

`cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict` runs the manual developer regression profile. It checks modeled page-load completion, interactive p95 under bulk load, video startup/rebuffering, file-download goodput, aggregation efficiency, ideal-lab goodput near 1 Gbps, failover recovery gap, repaired chunks, local AEAD CPU cost for ChaCha20-Poly1305 and AES-256-GCM, and lab RAM-budget diagnostics. These are lab signals only; the release `mptunnel` binary does not contain those thresholds and does not terminate because a lab goal is missed. `cargo run --manifest-path lab/benchmarks/Cargo.toml -- ablation` compares single-link, multipath, and scheduler-ablation profiles. Docker lab comparisons are documented in `docs/LAB.md`.
