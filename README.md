# mptunnel

`mptunnel` is a Rust application for encrypted multipath proxy transport.

## Build

```bash
cargo build
cargo test
scripts/check-line-counts.sh
```

`scripts/check-line-counts.sh` is a warning-only maintainability gate. It reports tracked source and public documentation files above 2,000 lines so large modules are split by cohesive ownership before they become harder to maintain.

The current product/carrier ownership map and mutation contracts are in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Read it before changing mixed
TCP+QUIC scheduling, path evidence, or reliable-stream ownership.

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

The lab emulates low-latency, balanced, a 0.1%-loss half-bandwidth/double-latency balanced companion, cross-continent high-bandwidth, harsh poor-Internet, saturated-link, flapping-link, ideal 0%-loss, controlled 2^3 bandwidth/latency/loss matrix, UDP, and failover scenarios, then records direct, single-path, multipath, mixed-workload, and endpoint traffic-accounting results under `lab/results/`. It mutates Docker network namespaces only. Shaped, unconstrained, fault, protocol-family, and real-Internet evidence are separate cohorts; Docker results are not real-Internet measurements.

## Config File

Starting `mptunnel` without arguments loads `./config.toml`. Use `--config PATH` or `-c PATH` to select another file, and use `--config PATH --check-config` to validate a file without starting listeners. The TOML file is role-free and V2Ray-style: `[[inbounds]]` accept traffic, `[[outbounds]]` forward it, every entry can have a `tag`, and `protocol = "mpp"` is the mptunnel protocol itself.

Minimal local forwarding node:

```toml
[management]
listen = ["127.0.0.1:7600"]
token = "replace-with-management-token"

[[inbounds]]
tag = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]
outbound = "edge-mpp"

[[outbounds]]
tag = "edge-mpp"
protocol = "mpp"
endpoints = ["tcp://203.0.113.10:443", "udp://203.0.113.10:443"]

[outbounds.security]
secret = "replace-with-shared-secret-at-least-32-bytes"
```

Minimal edge/relay node:

```toml
[management]
listen = ["127.0.0.1:7601"]
token = "replace-with-management-token"

[[inbounds]]
tag = "edge-mpp"
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:443", "udp://0.0.0.0:443"]
outbound = "direct-egress"

[inbounds.security]
secret = "replace-with-shared-secret-at-least-32-bytes"

[[outbounds]]
tag = "direct-egress"
protocol = "direct"
```

To chain requests through another proxy, point an `inbound(protocol = "mpp")` at an outbound tagged with `protocol = "socks5"`, `http-connect`, or `http-connect-udp` and provide `proxy = "host:port"`.

Routing policy is selected explicitly with `balancer = "tag"` on an inbound. MPP paths belong to `protocol = "mpp"` outbounds, and routing balancers reference outbound tags; balancers do not own endpoints or security directly. `strategy = "combined-mpp"` combines multiple MPP outbounds for a local SOCKS5/HTTP/TUN inbound while preserving each MPP outbound's own security. `strategy = "sequence"` and `strategy = "random"` apply to egress outbounds used by MPP inbounds.

## Management API

The release binary includes a lightweight JSON management API when `[management].listen` or `--management-listen` is configured. Endpoints include `GET /healthz`, `GET /status`, `GET /paths`, `GET /traffic`, `GET /diagnostics`, and client-side `POST /control/path`. If a token is set, use `Authorization: Bearer <token>` or `X-Mptunnel-Token: <token>`. Node status includes local inbound tags, each MPP outbound or balancer route target, path summaries, and traffic trends. Proxy credentials are never returned; the API reports only whether local proxy auth is required. Path control accepts either `client_index` or `client_tag`.

Client-side proxy ingress:

```bash
mptunnel --check-config client \
  --secret replace-with-shared-secret \
  --listen 127.0.0.1:1080 \
  --listen '[::1]:1080' \
  --path tcp://203.0.113.10:443 \
  --path tcp://203.0.113.11:443 \
  --path udp://203.0.113.10:443
```

Proxy ingress accepts repeated or comma-separated `--listen` addresses, so IPv4 and IPv6 listeners can be configured explicitly without relying on platform-specific dual-stack socket defaults.

Multiple ingress types can run in one client process. Use per-ingress listener flags when more than one proxy ingress is enabled:

```bash
mptunnel --check-config client \
  --secret replace-with-shared-secret \
  --socks5-listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://203.0.113.10:443 \
  --path udp://203.0.113.10:443
```

Client-side TUN L4 ingress with dual-stack addressing and explicit DNS forwarding:

```bash
mptunnel --check-config client \
  --secret replace-with-shared-secret \
  --tun \
  --tun-name mptun0 \
  --tun-ipv4 10.88.0.1 \
  --tun-ipv4-prefix 24 \
  --tun-ipv6 fd00:88::1 \
  --tun-ipv6-prefix 64 \
  --tun-mtu 1500 \
  --tun-dns-resolver 1.1.1.1:53 \
  --tun-dns-resolver '[2606:4700:4700::1111]:53' \
  --path tcp://203.0.113.10:443 \
  --path udp://203.0.113.10:443
```

For IPv6-only TUN ingress, add `--tun-disable-ipv4` and provide `--tun-ipv6`.
TUN L4 can be launched with TCP paths, UDP paths, or both; TCP flows use the reliable-stream layer over the configured carrier set, while TUN UDP flows use datagram flows over the same evidence-driven TCP or QUIC UDP carrier selection used by SOCKS5 UDP ASSOCIATE.

Local proxy authentication is disabled by default. Set both `--proxy-username` and `--proxy-password` to require SOCKS5 username/password authentication and HTTP CONNECT Basic proxy authentication:

```bash
mptunnel --check-config client \
  --secret replace-with-shared-secret \
  --proxy-username operator \
  --proxy-password replace-with-proxy-password \
  --listen 127.0.0.1:1080 \
  --http-listen 127.0.0.1:8080 \
  --path tcp://203.0.113.10:443
```

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
  --outbound-dns-strategy ipv4-then-ipv6 \
  --outbound-connect-timeout-ms 10000
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

Internal transport is encrypted by default with `aes-256-gcm`. `--secret` must be either a random UUID or at least 32 bytes of high-entropy secret text; mptunnel derives domain-separated 256-bit transport/auth material from it before use. Session and path authentication frames carry authenticated issue times and are rejected outside `--auth-freshness-window-seconds`; QUIC UDP paths use QUIC packet protection with a shared-secret-derived certificate identity, and servers keep a bounded `PATH_JOIN` nonce replay cache so captured setup traffic cannot establish another fresh path within the freshness window. Operators can select `--cipher chacha20-poly1305` on both client and server when that is a better fit for their CPU or deployment. Plaintext lab mode requires an explicit security mode and acknowledgement. It removes confidentiality for lab use only; session/path integrity remains authenticated:

```bash
mptunnel --check-config \
  --secret replace-with-shared-secret \
  --cipher aes-256-gcm \
  --auth-freshness-window-seconds 300 \
  --security plaintext-lab \
  --i-understand-this-is-insecure \
  client \
  --path tcp://127.0.0.1:7443
```

Normal client launches only need endpoint paths. Optional path URI query parameters can seed the runtime scheduler before live health observations exist, but Auto must correct those hints from measured link status and flow demand. Supported path hints are:

- `srtt-ms`, `rtt-ms`, `jitter-ms`
- `rate-bps`, `rate-kbps`, `rate-mbps`, `rate=unknown`, `rate=unlimited`
- `mtu`, `mtu-bytes`, `payload-mtu`
- `backup`, `expensive`, `low-latency`, `bulk-allowed`, `bulk`, `no-bulk`, `probe-only`, `no-udp`

Boolean hints accept `true`, `false`, `1`, `0`, `yes`, `no`, `on`, and `off`; bare boolean hints mean `true`.

TCP ingress is always adaptive Auto. There is no fixed user-selectable transmission mode. New TCP streams start latency-first for browsing, SSH, and other small interactive demand, then promote sustained larger transfers toward throughput-first scheduling when live byte counts, BDP, delivery rate, queue pressure, jitter, loss, and repair state show bulk demand. Auto can switch back toward latency-sensitive scoring for tails, stalls, congestion, or path failures, so low latency and high throughput are chosen from current link status and client/server flow behavior instead of hardcoded port rules.

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
  --max-path-flight-bytes 4194304 \
  --max-reliable-relay-chunk-bytes 262144 \
  --tcp-path-heartbeat-interval-ms 10000 \
  --tcp-path-heartbeat-timeout-ms 30000 \
  client \
  --path tcp://127.0.0.1:7443
```

Common environment variables:

- `MPTUNNEL_LOG`
- `MPTUNNEL_SECURITY`
- `MPTUNNEL_SECRET`
- `MPTUNNEL_CIPHER`
- `MPTUNNEL_AUTH_FRESHNESS_WINDOW_SECONDS`
- `MPTUNNEL_CHECK_CONFIG`
- `MPTUNNEL_SERVICE_MODE`
- `MPTUNNEL_SUPERVISE`
- `MPTUNNEL_RESTART_BACKOFF_MS`
- `MPTUNNEL_RESTART_MAX_BACKOFF_MS`
- `MPTUNNEL_MAX_RESTARTS`
- `MPTUNNEL_PATHS`
- `MPTUNNEL_BIND_PATHS`
- `MPTUNNEL_LISTEN`
- `MPTUNNEL_SOCKS5_LISTEN`
- `MPTUNNEL_HTTP_LISTEN`
- `MPTUNNEL_PROXY_USERNAME`
- `MPTUNNEL_PROXY_PASSWORD`
- `MPTUNNEL_TUN`
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
- `MPTUNNEL_OUTBOUND`
- `MPTUNNEL_OUTBOUND_BIND_IP`
- `MPTUNNEL_UPSTREAM_SOCKS5`
- `MPTUNNEL_UPSTREAM_HTTP`
- `MPTUNNEL_OUTBOUND_DNS_RESOLVERS`
- `MPTUNNEL_OUTBOUND_DNS_STRATEGY`
- `MPTUNNEL_OUTBOUND_DNS_TIMEOUT_MS`
- `MPTUNNEL_OUTBOUND_CONNECT_TIMEOUT_MS`
- `MPTUNNEL_MAX_FRAME_BYTES`
- `MPTUNNEL_MAX_PAYLOAD_BYTES`
- `MPTUNNEL_MAX_ACK_RANGES`
- `MPTUNNEL_MAX_PATHS`
- `MPTUNNEL_MAX_STREAMS`
- `MPTUNNEL_MAX_STREAM_WINDOW_BYTES`
- `MPTUNNEL_MAX_REPAIR_BYTES`
- `MPTUNNEL_MAX_REORDER_BYTES`
- `MPTUNNEL_MAX_DATAGRAM_QUEUE_BYTES`
- `MPTUNNEL_MAX_PATH_FLIGHT_BYTES`
- `MPTUNNEL_MAX_RELIABLE_RELAY_CHUNK_BYTES`
- `MPTUNNEL_TCP_PATH_HEARTBEAT_INTERVAL_MS`
- `MPTUNNEL_TCP_PATH_HEARTBEAT_TIMEOUT_MS`

## Current Runtime Scope

The current runtime exposes local SOCKS5, HTTP CONNECT, and TUN L4 ingress. A client can run multiple ingress entries in one process; all entries share the same encrypted multipath path set and adaptive scheduler.

Request source staging is carrier-neutral even though capacity proof is not.
For TCP and QUIC, the source queue plus ACK-retained repair starts at the
derived 4 MiB bulk reservoir. Its epoch is anchored to the exact ordered Service
instance, grows only from exact unambiguous same-family `OwnerData` ACK turnover
within that Service PTO, and resets on an exact ordered-Service handoff. This
product window never supplies carrier rate, pacing, or congestion authority.
The bounded request Validation startup, ordered receipt, and follow-on
ACK-clock calibration described below are TCP-only; QUIC request Subflows
require fresh post-attachment, non-app-limited native packet-ACK evidence.

SOCKS5 and HTTP CONNECT ingress support explicit IPv4/IPv6 dual-stack operation through repeated `--listen`, `--socks5-listen`, or `--http-listen` values such as `127.0.0.1:1080` and `[::1]:1080`. TUN, server bind paths, outbound DNS resolvers, upstream proxy endpoints, and direct target addresses all preserve IPv4 and IPv6 socket addresses; DNS resolution order is controlled by `--outbound-dns-strategy`.

TCP ingress uses encrypted TCP-underlay paths and can reach remote TCP targets through direct outbound, bind-source-IP direct outbound, upstream SOCKS5 CONNECT, upstream HTTP CONNECT, or `http-connect-udp` using ordinary CONNECT for TCP targets. Each configured TCP path has a bounded pair of lazy persistent encrypted carrier sessions: attachments opened as control, latency, or realtime share one session, while attachments opened as throughput or background share a separate session. The pair is the same in single-path and multipath deployments, and multiple local SOCKS5/HTTP CONNECT streams multiplex within their attachment-open class instead of creating a fresh internal TCP connection for every proxy stream. A latency-first attachment that later promotes to throughput keeps its current carrier; promotion changes product queue priority and budgets, not TCP association identity. Priority queues, bounded writer runs, and independent MPTE records protect control and interactive work at product-frame boundaries, but kernel TCP head-of-line blocking still applies within a shared carrier. When several TCP paths are configured, stream setup starts in Auto latency-first mode, then uses scheduler ETA scoring from path hints, current path health, observed delivery rate, active stream load, and live demand. Auto promotes sustained flows to bulk from live BDP/resource observations, can detach and reattach the same logical stream to a better bulk path without closing the remote TCP target, and returns to repair/latency-sensitive behavior for stalls, failures, and tails. It retries the next schedulable path after path-level open failures. Active TCP streams use one logical stream ID across TCP path sessions; when a path fails, the client can re-open that logical stream on a survivor path and replay unacknowledged data from the reliable-stream repair cache, while the server reattaches the same outbound TCP connection through its shared stream registry. Successful opens feed measured latency and live load back into later path choices; completed relays feed measured payload delivery rate after enough useful payload has been observed and release that load, while failed opens use PTO-derived path reuse dampening before probes resume. The relay caps unacknowledged TCP-underlay stream payload with `--max-path-flight-bytes`, and uses adaptive effective inflight/chunk budgets under that cap from live BDP, queue, jitter, loss, and Auto's current flow-demand lane, so local reads pause until end-to-end tunnel ACKs free budget instead of burying unlimited data in a kernel TCP send buffer. During idle carrier intervals, TCP path sessions send encrypted internal `PING`/`PONG` heartbeats using `--tcp-path-heartbeat-interval-ms` and `--tcp-path-heartbeat-timeout-ms`; active failover instead uses data-plane progress, PTO, ownership, and repair evidence. The TCP path-session reader is isolated from writer/heartbeat scheduling so encrypted frame reads are not cancelled mid-frame. The client also runs bounded authenticated idle-path probes on the configured interval, using `PING`/`PONG` after `PATH_JOIN` so TCP path health can recover without opening remote target connections or competing with live streams.

For a sustained request/upload stream, the exact ordered Service attachment anchors the shared TCP/QUIC product-window epoch and PTO. Window growth is credited to the exact live same-family attachment that uniquely owned acknowledged `OwnerData`, not to whichever carrier delivered the `STREAM_ACK`; repair, duplicated, ambiguous, cross-family, or stale-instance ownership may release product flight but cannot grow the window. TCP alone may give one freshly proven Validation instance a cumulative, non-refilling, resource-clamped 256 KiB startup sample. Frames are never split to fill the last credit: after a useful sample, a next frame that cannot fit irrevocably seals the sample at the bytes actually admitted, sends that frame on Service, and queues one ordered receipt marker behind the sealed sample. The marker rate uses the sealed byte count, and smaller later frames cannot reopen the sample. Normal Service product-flight residence age is not stale-tail authority for this startup gate; the later TCP ACK-clock calibration retains its separate stale-tail age guard. Graduation waits for exact owner flights to release, preserves Service ownership, and is followed by at most one exact same-underlay TCP candidate's cumulative 2 MiB default ACK-clock calibration horizon before ordinary measured admission. Calibration is pressure/debt/resource gated, rollback-safe, non-refilling, and advances to another candidate only after an exhausted candidate's exact flights drain. A QUIC request path receives no product startup sample or ordered receipt; optional ownership requires exact fresh post-attachment non-app-limited native packet-ACK evidence and native emission credit. Bulk lower-frontier owners use an adaptive approximately `2 * BDP` budget capped by the reorder envelope; actual latency pressure retains the smaller preemptible service horizon. Response-side startup sampling retains its separate two-active-response-flow gate. After its OwnerData flight drains, the current response startup owner releases the exclusive slot only from direction-correct bulk proof: TCP may use its exact unambiguous product ACK rate, while QUIC UDP still requires local carrier ACK-derived evidence. Graduation preserves Service ownership and sampled membership. A graduated TCP response candidate then receives an exact-instance staged ACK-clock calibration: cumulative spent credit never refills, starts at a 2 MiB service horizon, and doubles its authorized cumulative ceiling only after the current stage is fully spent and a strictly causal later ACK window has every sampled send preceding the prior ACK and its earliest send no earlier than that stage's authorization, up to the path-flight, repair, reorder, and stream-window resource ceiling. Credit growth remains independent from rate publication. Within each stage, all strict current-stage ACK windows contribute bytes and raw ACK-to-ACK elapsed time to one aggregate; timer granularity is applied once to the aggregate, not once per ACK callback. At stage authorization, that aggregate enters the rolling five-stage rate window only when its coverage reaches half the initial credit, 1 MiB by default and clamped to the initial resource limit; an under-covered causal sample may still grow or prove the stage without publishing a rate. The startup rate remains in force until three qualifying stage aggregates exist, after which their median overwrites the prior rate instead of max-filtering ACK-compressed bursts. When an exact active TCP calibration stage has less credit left than the normal response chunk, two-pass planning first tries that residual size and emits the smaller product frame only if the result carries the exact calibration commit; otherwise it discards the first pass and gives Service the normal chunk. Product-flight and carrier-queue counters are overlapping views and use union-style debt accounting, while cumulative calibration spend remains exact. The Service owner does not move during calibration, failed enqueue rolls back its reservation, and the exact active fence remains through proof until the candidate's calibration flights drain before the serial slot or ordinary ownership advances. Once that TCP calibration fence clears, a mature single-family response Service keeps the first derived Service horizon of assigned union debt; an already-admitted, strictly measured same-underlay Subflow may use the remainder of the existing feed reservoir while total ordered tail stays within it. This soft overflow never changes the Service identity, remains subject to ECF, BDP, reorder, command-credit, exact-epoch, and latency-pressure gates, and falls back to Service when no candidate passes. TCP and QUIC share this product ownership policy but remain separate transport families: TCP optional-path proof comes from strict product ACK evidence, while QUIC optional-path proof requires strict non-app-limited local carrier ACK evidence and native emission credit. The current QUIC Service alone may treat either substantial uniquely owned product `STREAM_ACK` progress or a durable local carrier ACK-derived DATA estimate as feed evidence; the carrier estimate may be app-limited. Neither authority is carrier capacity proof, but either may let bounded source and emission staging graduate when same-path latency pressure is absent, without admitting an optional Subflow or authorizing handoff. Before feed evidence, switchable same-family source staging and QUIC Service emission use the derived feed reservoir, 4 MiB with defaults; that bounded source/emission bootstrap is not a carrier congestion window. Bulk TCP and QUIC `STREAM_MAX_DATA` instead advertise the configured receiver-memory window independently of path proof. TCP product-ACK calibration and residual framing remain TCP-specific; the generalized same-family reservoir includes QUIC under its native carrier controller.

When a client is launched with both `tcp://` and `udp://` endpoint paths, mixed underlay is treated as its own Auto track. The same product ownership, ordering, admission, and bounded-repair ledger unifies TCP-underlay reliable streams and QUIC UDP reliable streams, while each family keeps its own ACK clock, congestion control, pacing, flow control, and liveness rules. Auto may move an ordered reliable stream across TCP and QUIC UDP paths only from direction-correct measured capacity, exact clear-frontier ownership, queue/debt, and completion evidence; configured ETA hints can rank validation but cannot authorize that move. After promotion, ordinary repair stays with the current carrier subflow set until that subflow set fails or is exhausted. UDP/QUIC receive progress is periodically replayed after established progress; TCP multipath replays progress only while reorder debt exists, and each replay timer follows the request-side Active carrier. A persistent bulk ACK gap exactly owned by TCP may repair one modeled service flight on one selected distinct output with live bulk-model evidence, bounded by approximately `2 * BDP`, the selected output's remaining service-flight headroom, gap debt, repair, path-flight, and queue resources. The queued event stays bound to that exact attachment or output incarnation rather than migrating frame by frame to a differently modeled path. If that identity detaches, is replaced, or cannot drain before the persistent-gap deadline, its remaining queued batch is cancelled so a later authoritative gap replay can select and size a fresh output. An unproven output, UDP/QUIC owner, or latency work retains one smaller bounded repair event. This avoids blind same-stream TCP+UDP striping while still allowing mixed-carrier recovery and measured cooperative aggregation without adding a manual transmission mode.

For multi-flow response sessions, carrier-neutral response ownership stays above carrier-specific proof and emission. The current bootstrap is directional: when a measured active TCP Service family leads UDP by at least two flows and has no latency pressure, one reachable, unmeasured QUIC Validation path may receive a bounded `PATH_CAPACITY_DATA` train. TCP keeps exact product-owner/ACK-clock calibration. QUIC instead gates ordinary connection writers, emits the typed train as bounded records followed by `PATH_CAPACITY_FINISH`, and requires the peer to return an exact same-stream `PATH_CAPACITY_RECEIPT`. Quinn ACK timing remains provisional carrier diagnostics because its callbacks are connection-aggregate and cannot identify a stream or calibration token. Receipt of the full declared train is the capacity authority; it freezes the conservative full sender-to-receipt rate interval and placement expiry, publishes independently of later native ACK telemetry, and releases the carrier writer gate. A separate sent-time quarantine excludes probe-era and early post-receipt ACKs from generic product evidence through that expiry without blocking new writes. Pre-existing cross-stream traffic can only lengthen the receipt interval and lower the measured available rate. The typed command freezes `calibration_id`, sample floor, accounting slack, live carrier window, proof-validity duration, and attempt deadline. A completed marker is installed only after exact token/path-instance/session validation, and its three-PTO placement lifetime is frozen from the planning snapshot rather than recomputed from later RTT. Every attempt uses the exact `max(sample floor, live carrier window + fresh strict-proof window)` geometry and must fit the cumulative session envelope. Eligible fitting unattempted paths rank ahead of retries, each exact session/path/path-instance may attempt at most twice, and one session may hold only one probe or handoff drain. Failed provisional command admission rolls back only its exact reservation; publication resolves the command ticket without turning it into carrier cancellation, while expiry, detach, and close remain cancellation. An indeterminate partial, cancelled, or expired QUIC carrier write fail-closes that exact connection before its epoch can be reused.

A measured target can receive one sticky whole-flow Service handoff only when its projected per-flow capacity is non-degrading. If sustained source feed prevents a clear frontier, the session may reserve one bounded, one-shot drain for one exact response binding. That binding pauses only fresh `OwnerData`; control, ACK/credit, correctness-critical repair, and other bindings continue. Offset-free raw staging continues only within the existing bounded source-feed/sender-queue reservoir while that binding's queued Data front is blocked. The handoff commits at the resulting exact frontier after identity and model revalidation, or the drain cancels on expiry or projected fair-share regression. This is flow placement across separate TCP and QUIC recovery engines, not per-frame cross-carrier striping.

SOCKS5 UDP ASSOCIATE ingress uses authenticated encrypted datagram flows per target, then sends repeated datagrams with flow ID, datagram ID, TTL, and payload without repeating target metadata. UDP targets do not hardcode a TCP or UDP underlay preference. `tcp://` and `udp://` paths can both carry the product datagram flow; `udp://` paths use QUIC, so packet-level encryption, loss recovery, congestion control, pacing, and connection continuity live below mptunnel's stream/datagram scheduler. Auto starts fresh realtime datagrams latency-first, uses live RTT/jitter/loss/rate/queue evidence and TTL freshness to choose the carrier, may move sustained demand toward higher measured bandwidth, and can shrink back to latency/realtime behavior when that demand disappears. If the selected carrier has a retryable path-level failure, mptunnel tries the next schedulable carrier by the same evidence order instead of treating either TCP or UDP as a fallback family.

Datagrams are acknowledged with internal `DGRAM_FEEDBACK` ranges, and response datagrams are acknowledged back to the server. QUIC UDP feedback updates scheduler RTT, jitter, loss, and rate evidence; TCP-carried feedback updates its association-local response timers, while association close feeds useful-payload delivery rate and releases scheduler load. When several carriers are configured, one local datagram association can lazily own TCP and QUIC UDP carrier associations, but realtime datagrams prefer the ready lowest-ETA carrier instead of spraying ordinary probes across high-RTT paths. One absolute expiry is fixed from the original product TTL before carrier selection. Carrier setup, flow setup, pacing, request emission, feedback/response waiting, and permitted fallback all consume that same remaining TTL. Before `DGRAM_FEEDBACK`, at most one fresh product attempt may follow the original attempt, and only on the next evidence-ordered carrier whose current setup ETA still fits; after feedback or product expiry, mptunnel does not reopen a carrier or replay the product datagram. Request blackholes mark the path failed; an acknowledged request may wait only to the original expiry and never becomes replay permission. The same bounded authenticated idle-path probe loop exercises carrier handshakes and `PING`/`PONG` without opening datagram flows or adding probe traffic to active associations.

TUN L4 ingress uses `tun-rs` for cross-platform TUN device creation and `netstack-smoltcp` for user-space TCP/UDP flow translation. TCP packets accepted from the TUN stack become encrypted internal reliable streams with `IngressKind::TunTcp` over TCP, UDP, or mixed reliable underlays. UDP packets are demuxed by local/remote socket pair into bounded per-flow tasks and sent as encrypted internal datagram flows with `IngressKind::TunUdp` over the best live TCP or QUIC UDP datagram carrier. TUN supports IPv4, IPv6, or dual-stack addressing. UDP port 53 traffic can be explicitly remapped to configured `--tun-dns-resolver` addresses while responses are written back to the TUN client as if they came from the original DNS destination, so DNS traffic can pass through TUN without relying on host resolver defaults.

UDP targets can be reached through direct UDP, bind-source-IP direct UDP, upstream SOCKS5 UDP ASSOCIATE, or upstream HTTP CONNECT-UDP. The `http-connect-udp` outbound performs the RFC 9298 HTTP/1.1 Upgrade handshake, requires a `101 Switching Protocols` response with capsule support, and carries UDP payloads in HTTP Datagram capsules. Plain `http-connect` outbound is TCP-only.

Server direct and bind-source outbounds resolve domain targets through `--outbound-dns-resolver` when configured, or the system resolver when no explicit resolver is supplied. `--outbound-dns-strategy` controls IPv4/IPv6 lookup ordering and filtering for dual-stack targets. `--outbound-connect-timeout-ms` scopes target or upstream-proxy dial setup to the selected egress outbound. `--path-probe-timeout-ms` bounds only one configured idle MPP health-probe transaction. Initial Active setup budgets the serialized carrier and product-open exchanges: three PTOs for TCP and two for QUIC UDP when another candidate remains, or nine and eight respectively when a sole candidate must tolerate the `1 + 2 + 4` persistent-congestion PTO backoff after its phase prefix. Every initial Active TCP attempt retains the conservative initial PTO floor because its shared session actor may have to establish or re-establish the carrier; Active reattach, Repair, and Validation/recovery use one live candidate PTO. Queue wait, carrier setup, authenticated MPP and `PATH_JOIN`, product-open emission, current path-metric publication, and peer acceptance consume one absolute role-specific deadline rather than restarting it.

`--service-mode --supervise` is intended for systemd, launchd, Windows Service Control Manager, or another process supervisor. It restarts the runtime after top-level listener/device failures with exponential backoff controlled by `--restart-backoff-ms`, `--restart-max-backoff-ms`, and `--max-restarts`. Operational details are in `docs/OPERATIONS.md`.

## Scheduler Regression Gates

The deterministic simulator exercises the scheduler against heterogeneous path conditions before runtime changes are made. Current gates cover:

- bulk transfer aggregation efficiency across low-latency and high-bandwidth paths
- failover gap after path failure and chunk reinjection onto a survivor path
- interactive p95 latency while a bulk transfer is queued
- bulk tail penalty for heterogeneous RTT/bandwidth paths
- per-lane priority and per-flow deficit scheduling
- per-path queued-byte pressure from scheduled payloads
- bulk tail avoidance that promotes the final bulk bytes onto latency-sensitive scoring
- duplication of small control/realtime packets onto a second close-ETA path
- shared-bottleneck suspicion that avoids a low-RTT path when a similar-RTT peer is already queued

## Developer Benchmark Gates

`cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict` runs the manual developer regression profile. It checks modeled page-load completion, interactive p95 under bulk load, video startup/rebuffering, file-download goodput, aggregation efficiency, ideal-lab goodput near 1 Gbps, failover recovery gap, repaired chunks, local AEAD CPU cost for ChaCha20-Poly1305 and AES-256-GCM, and lab RAM-budget diagnostics. These are lab signals only; the release `mptunnel` binary does not contain those thresholds and does not terminate because a lab goal is missed. `cargo run --manifest-path lab/benchmarks/Cargo.toml -- ablation` compares single-link, multipath, and scheduler-ablation profiles. Docker lab comparisons are documented in `docs/LAB.md`.
