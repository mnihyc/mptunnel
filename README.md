# mptunnel

`mptunnel` is a Rust application for encrypted multipath proxy transport.

## Build

```bash
cargo build
cargo test
```

## Configuration Check

Client-side proxy ingress:

```bash
mptunnel --check-config client \
  --secret replace-with-shared-secret \
  --ingress socks5 \
  --listen 127.0.0.1:1080 \
  --path-probe-interval-ms 10000 \
  --path-probe-timeout-ms 2000 \
  --default-tcp-class interactive \
  --tcp-class-rule 22=control \
  --tcp-class-rule 8443=bulk \
  --path 'tcp://203.0.113.10:443?srtt-ms=20&rate-mbps=30&low-latency=true' \
  --path 'tcp://203.0.113.11:443?srtt-ms=180&rate-mbps=300' \
  --path 'udp://203.0.113.10:443?srtt-ms=20&rate-mbps=30'
```

Server-side path listener and direct outbound:

```bash
mptunnel --check-config server \
  --secret replace-with-shared-secret \
  --bind-path tcp://0.0.0.0:443 \
  --bind-path udp://0.0.0.0:443 \
  --outbound direct
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
- `backup`, `expensive`, `low-latency`, `bulk-allowed`, `bulk`, `no-bulk`, `probe-only`, `no-udp`

Boolean hints accept `true`, `false`, `1`, `0`, `yes`, `no`, `on`, and `off`; bare boolean hints mean `true`.

TCP ingress traffic class policy is deterministic and port-based. `--default-tcp-class` accepts `control`, `interactive`, `bulk`, or `background`; `--tcp-class-rule` accepts repeated or comma-separated `port=class` and `port:class` rules. `realtime-datagram` is reserved for UDP datagram flows and is not valid for TCP class rules.

Global resource limits are configurable and validated before runtime starts:

```bash
mptunnel --check-config \
  --secret replace-with-shared-secret \
  --max-frame-bytes 1048576 \
  --max-payload-bytes 1048512 \
  --max-ack-ranges 256 \
  --max-paths 64 \
  --max-streams 65536 \
  --max-stream-window-bytes 16777216 \
  --max-repair-bytes 16777216 \
  --max-reorder-bytes 16777216 \
  --max-datagram-queue-bytes 4194304 \
  client \
  --path tcp://127.0.0.1:7443
```

Common environment variables:

- `MPTUNNEL_LOG`
- `MPTUNNEL_SECURITY`
- `MPTUNNEL_SECRET`
- `MPTUNNEL_CHECK_CONFIG`
- `MPTUNNEL_PATHS`
- `MPTUNNEL_BIND_PATHS`
- `MPTUNNEL_INGRESS`
- `MPTUNNEL_LISTEN`
- `MPTUNNEL_PATH_PROBE_INTERVAL_MS`
- `MPTUNNEL_PATH_PROBE_TIMEOUT_MS`
- `MPTUNNEL_DEFAULT_TCP_CLASS`
- `MPTUNNEL_TCP_CLASS_RULES`
- `MPTUNNEL_OUTBOUND`
- `MPTUNNEL_OUTBOUND_BIND_IP`
- `MPTUNNEL_UPSTREAM_SOCKS5`
- `MPTUNNEL_UPSTREAM_HTTP`
- `MPTUNNEL_MAX_FRAME_BYTES`
- `MPTUNNEL_MAX_PAYLOAD_BYTES`
- `MPTUNNEL_MAX_ACK_RANGES`
- `MPTUNNEL_MAX_PATHS`
- `MPTUNNEL_MAX_STREAMS`
- `MPTUNNEL_MAX_STREAM_WINDOW_BYTES`
- `MPTUNNEL_MAX_REPAIR_BYTES`
- `MPTUNNEL_MAX_REORDER_BYTES`
- `MPTUNNEL_MAX_DATAGRAM_QUEUE_BYTES`

## Current Runtime Scope

The current runtime exposes local SOCKS5 and HTTP CONNECT ingress.

TCP ingress uses encrypted TCP-underlay paths and can reach remote TCP targets through direct outbound, bind-source-IP direct outbound, upstream SOCKS5 CONNECT, or upstream HTTP CONNECT. When several TCP paths are configured, stream setup classifies each target with the configured TCP traffic policy, then uses scheduler ETA scoring from path hints, current path health, active stream load, and the selected class. It retries the next schedulable path after path-level open failures. Successful opens feed measured latency and live load back into later path choices; finished relays release that load, while failed opens put the path into a short cooldown before probing resumes. The client also runs bounded authenticated path probes on the configured interval, using `PING`/`PONG` after `PATH_JOIN` so TCP path health can recover without opening remote target connections.

SOCKS5 UDP ASSOCIATE ingress uses authenticated encrypted UDP path sessions. It opens compact internal datagram flows per target, then sends repeated datagrams with flow ID, datagram ID, TTL, and payload without repeating target metadata. When several UDP paths are configured, UDP session setup uses the same scheduler inputs, adaptive health records, and active association load, then retries after path-level handshake failures. Closed associations release their scheduler load. The same bounded authenticated probe loop exercises UDP path handshakes and `PING`/`PONG` without opening datagram flows. Server UDP listeners demux peers on one bound socket into bounded per-peer encrypted session tasks.

UDP targets can be reached through direct UDP, bind-source-IP direct UDP, or upstream SOCKS5 UDP ASSOCIATE. Plain HTTP CONNECT outbound is TCP-only.
