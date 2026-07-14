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

The current matched clean TCP upload result is Iteration 109: two logical
flows over five 500 Mbps, 180 ms, 1 ms jitter, zero-loss paths reached
691.368 Mbps multipath versus 314.999 Mbps single-path. The ratios are 2.195x
overall, 2.935x over the broad `[9,18)` sink-ACK window, and 2.409x over the
supporting `[15,18)` window. Client transmit shares were 37.15%, 33.44%,
4.78%, 10.47%, and 14.17%. Against the same-condition Iteration 69 pair,
multipath improved 10.38% overall and 11.77% broad but changed -6.36% late;
the matched single control changed -0.32%, +0.56%, and +3.53%. This is earlier
delivery rather than a hidden tail loss: `[9,15)` improved 22.02%. These are
exact, diagnostics-disabled lab rows, not real-Internet or wire-rate claims. They prove
multi-flow TCP aggregation for this upload cohort; they do not prove one-flow
optional-path aggregation, QUIC or mixed-carrier aggregation, failover, or a
current MPTCP/Hysteria2 baseline comparison.

The initial matched clean TCP download result is Iterations 118-121. With one
logical response flow over the same five-path 500 Mbps, 180 ms, 1 ms-jitter,
zero-loss profile, multipath reached 273.437 and 263.841 Mbps in two clean runs,
or 1.182x and 1.139x their matched single-path controls; final-window ratios
were 1.477x and 1.488x. A detached `7fa7789` control reached 235.147 Mbps under
the current host conditions, matching the current 231.579 Mbps single-path row
and attributing the lower absolute rate versus historical Iteration 111 to lab
or host drift rather than this code change. The two-flow guard reached 488.684
Mbps, 2.078x that detached single-path control. These rows prove current TCP
response aggregation in this shaped cohort, including one-flow optional-path
use; they do not prove QUIC, mixed-carrier, heterogeneous, failover,
real-Internet, or current MPTCP/Hysteria2 results.
Those initial results were not ideal: overall one-flow gain was only
1.139-1.182x, absolute goodput remained below one nominal 500 Mbps path, and
matched container samples showed materially higher CPU and memory for
multipath.

A final ownership audit found that Service could assign beyond the reservoir
assumed by TCP calibration opportunity. The corrected final binary bounds total
ordered tail by the exact calibration prefix plus one Service feed reservoir and
does not double-count queue/native flight already present in Service ETA.
Iteration 124 reaches 319.401 Mbps overall and 561.482 Mbps in the final window;
against the adjacent Iteration 122 single control at 301.301/439.178 Mbps, this
is 1.060x/1.278x. The unsafe pre-cap A/B reaches 346.852/459.599 Mbps. Thus the
correction gives up 7.9% overall but improves late delivery 22.2% and uses the
alternate path more materially. Iteration 126 then proved the remaining cost:
one endpoint-only candidate serialized 4.46 MiB of staged calibration from
5.3-8.8 seconds and published only a 19.6 Mbps capacity prior on a nominal
500 Mbps path. Iteration 127 replaces that redundant phase with the typed
Service opportunity prior; the matched causal row improves from 110.189 to
175.841 Mbps, with no calibration event and ordinary alternate ownership
beginning immediately after startup drain.

The final diagnostics-disabled Iteration 128 pair reaches 236.774 Mbps
multipath against 112.274 Mbps single, or 2.109x overall and 2.368x in the
final three seconds. The adjacent single confirms another slow host epoch, so
the ratio and phase shape are authoritative rather than comparison with old
absolute rates. The two material server paths carry 75.5% and 24.5% of bytes.
This closes the silent early-throughput downgrade, but resource and gap cost
remain non-ideal: average server CPU is 9.398% versus 2.462%, peak server memory
is 208.8 versus 134.6 MB, and maximum read gap is 0.599 versus 0.494 seconds.

The separate default heterogeneous Iteration 129 guard is not ideal. Multipath
reaches 104.531 Mbps versus the adjacent 500 Mbps-path single at 110.489 Mbps,
or 0.946x, although first body and maximum read gap improve from 1.428/0.845 to
0.170/0.203 seconds. It aggregates low-latency and balanced paths but leaves the
fat path at control-only traffic. This is still 41.5-42.1% above the closest
preserved one-flow heterogeneous results, so it is an unresolved capacity gap,
not a silently accepted regression. Attempts to serially sample later cold TCP
paths were rejected: they made fat-path traffic material but increased maximum
read gap to 0.525-1.269 seconds. Iterations 136-154 instead add exact Linux
per-socket TCP evidence and an offset-free capacity receipt. Passive `TCP_INFO`
remains liveness/pressure evidence only; a typed receipt is required before it
can authorize bulk placement. Native delivery may lift the receipt by at most
the existing 2x BBR cwnd gain; pacing alone is never capacity authority. The
final Iteration 153 pair reaches 172.853 Mbps versus 84.992 Mbps single, or
2.034x. Its 0.504 second gap is a disclosed boundary miss; the clean Iteration
154 repeat reaches 182.917 Mbps with a 0.368 second gap. Both use lowlat,
balanced, and mild materially, while the 500 Mbps fat path remains
inconsistently recruited. One-PTO TCP prefix reinjection is bounded to one
modeled owner flight and the shared feed reservoir. QUIC keeps native loss
recovery and its existing one-quantum product repair.
The Iteration 135 negative guard puts one 200 Mbps low-latency Service beside
four 50 Mbps, 420 ms, 10%-loss optional paths. Multipath stays at 182.247 Mbps
versus 182.777 Mbps single, with 0.251/0.247 second maximum gaps, while every
slow optional remains control-only. Thus the current startup completion gate
prevents a clearly slower candidate from receiving the borrowed prior.

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
  --max-stream-window-bytes 67108864 \
  --max-repair-bytes 67108864 \
  --max-reorder-bytes 67108864 \
  --max-datagram-queue-bytes 16777216 \
  --max-path-flight-bytes 67108864 \
  --max-reliable-relay-chunk-bytes 524288 \
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
ACK-clock calibration described below are TCP-only. QUIC request Subflows use a
separate carrier-only capacity train and an exact post-proof product-ACK
handoff; product ACK timing never becomes QUIC capacity evidence.
Fresh TCP request startup and fresh zero-spend calibration also require at least
two active logical bulk request flows whose exact committed Service is TCP.
Demand is request-direction-specific and must include present queued or
outstanding request data: reverse-direction bytes, idle completed uploads, and
QUIC-Service flows never count. Each stream counts once regardless of its path
attachments. Begun exact-owner epochs may drain after a two-to-one
transition. No path-wide completion estimate may veto fresh request calibration
until request-direction, provenance-bound authority exists; such estimates are
not negative evidence. The two-flow, exact-owner, debt, resource, pressure, and
2 MiB cumulative-cap gates remain mandatory. These gates protect one-flow
stability; they do not establish one-flow optional-path aggregation for either
carrier family.

SOCKS5 and HTTP CONNECT ingress support explicit IPv4/IPv6 dual-stack operation through repeated `--listen`, `--socks5-listen`, or `--http-listen` values such as `127.0.0.1:1080` and `[::1]:1080`. TUN, server bind paths, outbound DNS resolvers, upstream proxy endpoints, and direct target addresses all preserve IPv4 and IPv6 socket addresses; DNS resolution order is controlled by `--outbound-dns-strategy`.

TCP ingress uses encrypted TCP-underlay paths and can reach remote TCP targets through direct outbound, bind-source-IP direct outbound, upstream SOCKS5 CONNECT, upstream HTTP CONNECT, or `http-connect-udp` using ordinary CONNECT for TCP targets. Each configured TCP path has a bounded pair of lazy persistent encrypted carrier sessions: attachments opened as control, latency, or realtime share one session, while attachments opened as throughput or background share a separate session. The pair is the same in single-path and multipath deployments, and multiple local SOCKS5/HTTP CONNECT streams multiplex within their attachment-open class instead of creating a fresh internal TCP connection for every proxy stream. A latency-first attachment that later promotes to throughput keeps its current carrier; promotion changes product queue priority and budgets, not TCP association identity. Priority queues, bounded writer runs, and independent MPTE records protect control and interactive work at product-frame boundaries, but kernel TCP head-of-line blocking still applies within a shared carrier. When several TCP paths are configured, stream setup starts in Auto latency-first mode, then uses scheduler ETA scoring from path hints, current path health, observed delivery rate, active stream load, and live demand. Auto promotes sustained flows to bulk from live BDP/resource observations, can detach and reattach the same logical stream to a better bulk path without closing the remote TCP target, and returns to repair/latency-sensitive behavior for stalls, failures, and tails. It retries the next schedulable path after path-level open failures. Active TCP streams use one logical stream ID across TCP path sessions; when a path fails, the client can re-open that logical stream on a survivor path and replay unacknowledged data from the reliable-stream repair cache, while the server reattaches the same outbound TCP connection through its shared stream registry. Successful opens feed measured latency and live load back into later path choices; completed relays feed measured payload delivery rate after enough useful payload has been observed and release that load, while failed opens use PTO-derived path reuse dampening before probes resume. The relay caps unacknowledged TCP-underlay stream payload with `--max-path-flight-bytes`, and uses adaptive effective inflight/chunk budgets under that cap from live BDP, queue, jitter, loss, and Auto's current flow-demand lane, so local reads pause until end-to-end tunnel ACKs free budget instead of burying unlimited data in a kernel TCP send buffer. During idle carrier intervals, TCP path sessions send encrypted internal `PING`/`PONG` heartbeats using `--tcp-path-heartbeat-interval-ms` and `--tcp-path-heartbeat-timeout-ms`; active failover instead uses data-plane progress, PTO, ownership, and repair evidence. The TCP path-session reader is isolated from writer/heartbeat scheduling so encrypted frame reads are not cancelled mid-frame. The client also runs bounded authenticated idle-path probes on the configured interval, using `PING`/`PONG` after `PATH_JOIN` so TCP path health can recover without opening remote target connections or competing with live streams.

For a sustained request/upload stream, the exact ordered Service attachment
anchors the shared TCP/QUIC product-window epoch and PTO. Window growth is
credited to the exact live same-family attachment that uniquely owned
acknowledged `OwnerData`, not to whichever carrier delivered the `STREAM_ACK`;
repair, duplicated, ambiguous, cross-family, or stale-instance ownership may
release product flight but cannot grow the window.

Fresh TCP request discovery requires at least two active logical bulk request
flows whose exact committed Service is TCP. TCP alone may give one freshly
proven Validation instance a cumulative, non-refilling, resource-clamped
256 KiB startup sample. Request demand must include present queued or
outstanding request data; reverse bytes, idle completed uploads, QUIC-Service
flows, and extra attachments never count as another logical flow. Frames are
not split to fill the last startup credit. An oversized next frame seals the
sample, returns that frame to Service, and queues one ordered receipt marker
behind the sealed bytes. Either the exact product ACK that completes the sealed
startup `OwnerData` or the exact ordered receipt ACK establishes the causal
boundary for follow-on calibration; receipt is not the only valid boundary.

After graduation and exact startup-flight drain, one explicit exact-instance
TCP calibration owner may spend one frozen cumulative target, 2 MiB with the
default resource envelopes. The target is fixed when ownership is claimed,
never recomputed from a growing BDP estimate, never refills, and remains bounded
by path-flight, repair, reorder, stream-window, debt, pressure, and enqueue
credit. Only exact candidate `OwnerData` sent after the causal boundary can
complete this proof. Failed enqueue rolls back the owner/spend reservation; an
exhausted owner serializes the next candidate until exact proof completes or
that exact path-instance lifecycle ends. Flight drain alone does not mint a new
owner. A
fresh zero-spend owner requires the same two-flow TCP-Service gate, while an
already-spent exact owner may finish after a two-to-one transition.

Once candidate ownership is exact, the scheduler may provisionally seed an
endpoint-only TCP candidate's rate and pipe from the current Service model so
kernel TCP can leave slow start without turning the bounded proof into a
pipe-sized probe. A configured candidate retains its own capacity hint.
That prior is scheduling credit, not candidate capacity evidence. The
candidate's own continuous exact product-ACK model replaces it after ten exact
candidate samples. A QUIC request path receives no product startup sample,
ordered receipt calibration, or TCP product-ACK rate proof. Instead, one
session-serialized, non-refilling `PATH_CAPACITY_*` train measures the exact
Validation carrier without product offsets. A fenced native tail or exact
receipt lower bound grants bounded approximately `2 * BDP` product authority.
The desired warmup target is the larger of the candidate's own native flight
window and twice the effective Service-rate BDP at the candidate RTT. The
preassigned session envelope may bound the rate-derived part, but never below
the candidate's native flight. Product flight and ordering debt never enlarge
this carrier transaction; they remain separate product admission inputs.
The same stream-local relay instance must then ACK one fixed post-proof product
floor before expiry; only bytes sent after proof acceptance qualify, and those
ACK bytes confirm ordered ownership without estimating QUIC rate. Completed
ordered ownership is durable, but its numeric carrier prior is fresh for only
one proof-validity horizon after completion; newer native evidence may then
correct it without waiting for another full product window. Expiry or
data-plane replacement removes an incomplete handoff. Latency pressure keeps
the smaller preemptible Service horizon.

Response-side discovery has a separate directional gate: one active sustained
bulk response may spend the first bounded same-family startup sample. That first
candidate is the non-circular discovery bootstrap. After one measured Subflow
exists, every later fresh candidate must project completion of its whole startup
sample within the current Service backlog reservoir; an already-bound exact
startup epoch may finish. This prevents serial cold candidates from inserting a
slow ordered prefix.

After startup flight drains, endpoint-only TCP with no independent carrier
evidence inherits the proven same-family Service rate as a temporary typed
path-capacity prior and enters ordinary bounded Subflow admission. Ten completed
ordinary exact-ACK windows plus a usable continuous delivery sample replace it
atomically with per-flow goodput. Configured or independently measured paths
preserve their own evidence and may run one exact-instance staged product-ACK
fallback calibration. Its credit is cumulative and non-refilling; strict causal
windows grow the authorized ceiling only within path-flight, repair, reorder,
and stream resources. Qualifying aggregates publish a robust median after exact
drain as the same typed path-capacity prior. Fragmented callbacks do not count
as windows, and the calibration clock
is reset so provisional capacity cannot silently become permanent per-flow
evidence. Service ownership does not move during calibration, and failed enqueue
rolls back the exact reservation. While its exact prefix remains outstanding,
total ordered assignment cannot exceed that prefix plus one bounded Service feed
reservoir, clamped to the product resource envelope.

For sustained backlog, a measured same-underlay Subflow may share the configured
product/reorder/stream envelope while the first Service horizon stays protected.
Completion admission compares against the complete Service-assigned backlog;
receiver reorder exposure separately excludes Service-assigned and candidate
bytes. Per-path BDP, inflight, repair, command-credit, epoch, and latency gates
remain mandatory. TCP and QUIC share this carrier-neutral ownership reservoir but
not proof: TCP uses strict product-ACK evidence, while QUIC requires strict
non-app-limited native carrier ACK evidence and native emission credit. QUIC
Service feed evidence and its bounded 4 MiB pre-evidence source/emission staging
do not prove an optional path. Each carrier retains its own congestion control,
pacing, flow control, and recovery.

When a client is launched with both `tcp://` and `udp://` endpoint paths, mixed underlay is treated as its own Auto track. The same product ownership, ordering, admission, and bounded-repair ledger unifies TCP-underlay reliable streams and QUIC UDP reliable streams, while each family keeps its own ACK clock, congestion control, pacing, flow control, and liveness rules. Auto may move an ordered reliable stream across TCP and QUIC UDP paths only from direction-correct measured capacity, exact clear-frontier ownership, queue/debt, and completion evidence; configured ETA hints can rank validation but cannot authorize that move. After promotion, ordinary repair stays with the current carrier subflow set until that subflow set fails or is exhausted. UDP/QUIC receive progress is periodically replayed after established progress; TCP multipath replays progress only while reorder debt exists, and each replay timer follows the request-side Active carrier. A persistent bulk ACK gap exactly owned by TCP may repair one modeled service flight on one selected distinct output with live bulk-model evidence, bounded by approximately `2 * BDP`, the selected output's remaining service-flight headroom, gap debt, repair, path-flight, and queue resources. The queued event stays bound to that exact attachment or output incarnation rather than migrating frame by frame to a differently modeled path. If that identity detaches, is replaced, or cannot drain before the persistent-gap deadline, its remaining queued batch is cancelled so a later authoritative gap replay can select and size a fresh output. An unproven output, UDP/QUIC owner, or latency work retains one smaller bounded repair event. This avoids blind same-stream TCP+UDP striping while still allowing mixed-carrier recovery and measured cooperative aggregation without adding a manual transmission mode.

Separately from one-active-response same-family discovery, the cross-family QUIC
capacity train remains a multi-flow mechanism. When a measured active TCP
Service family leads UDP by at least two flows and has no latency pressure, one
reachable, unmeasured QUIC Validation path may receive a bounded
`PATH_CAPACITY_DATA` train. TCP keeps exact product-owner/ACK-clock calibration.
QUIC instead gates ordinary connection writers, emits typed bounded records and
`PATH_CAPACITY_FINISH`, and requires an exact same-stream
`PATH_CAPACITY_RECEIPT`. Full-train receipt owns proof; connection-aggregate
Quinn ACK timing remains diagnostic. The command freezes token, path instance,
byte geometry, validity, and deadline, uses a cumulative non-refilling session
envelope, and permits at most two attempts per exact path instance. Publication
resolves the command ticket; partial, cancelled, expired, or indeterminate writes
fail-close that exact connection before its epoch can be reused.

A measured target can receive one sticky whole-flow Service handoff only when its projected per-flow capacity is non-degrading. If sustained source feed prevents a clear frontier, the session may reserve one bounded, one-shot drain for one exact response binding. That binding pauses only fresh `OwnerData`; control, ACK/credit, correctness-critical repair, and other bindings continue. Offset-free raw staging continues only within the existing bounded source-feed/sender-queue reservoir while that binding's queued Data front is blocked. The handoff commits at the resulting exact frontier after identity and model revalidation, or the drain cancels on expiry or projected fair-share regression. This is flow placement across separate TCP and QUIC recovery engines, not per-frame cross-carrier striping.

SOCKS5 UDP ASSOCIATE ingress uses authenticated encrypted datagram flows per target, then sends repeated datagrams with flow ID, datagram ID, TTL, and payload without repeating target metadata. UDP targets do not hardcode a TCP or UDP underlay preference. `tcp://` and `udp://` paths can both carry the product datagram flow; `udp://` paths use QUIC, so packet-level encryption, loss recovery, congestion control, pacing, and connection continuity live below mptunnel's stream/datagram scheduler. Auto starts fresh realtime datagrams latency-first, uses live RTT/jitter/loss/rate/queue evidence and TTL freshness to choose the carrier, may move sustained demand toward higher measured bandwidth, and can shrink back to latency/realtime behavior when that demand disappears. If the selected carrier has a retryable path-level failure, mptunnel tries the next schedulable carrier by the same evidence order instead of treating either TCP or UDP as a fallback family.

Datagrams are acknowledged with internal `DGRAM_FEEDBACK` ranges, and response datagrams are acknowledged back to the server. QUIC UDP feedback updates scheduler RTT, jitter, loss, and rate evidence; TCP-carried feedback updates its association-local response timers, while association close feeds useful-payload delivery rate and releases scheduler load. When several carriers are configured, one local datagram association can lazily own TCP and QUIC UDP carrier associations, but realtime datagrams prefer the ready lowest-ETA carrier instead of spraying ordinary probes across high-RTT paths. One absolute expiry is fixed from the original product TTL before carrier selection. Carrier setup, flow setup, pacing, request emission, feedback/response waiting, and permitted fallback all consume that same remaining TTL. Before `DGRAM_FEEDBACK`, at most one fresh product attempt may follow the original attempt, and only on the next evidence-ordered carrier whose current setup ETA still fits; after feedback or product expiry, mptunnel does not reopen a carrier or replay the product datagram. Request blackholes mark the path failed; an acknowledged request may wait only to the original expiry and never becomes replay permission. The same bounded authenticated idle-path probe loop exercises carrier handshakes and `PING`/`PONG` without opening datagram flows or adding probe traffic to active associations.

TUN L4 ingress uses `tun-rs` for cross-platform TUN device creation and `netstack-smoltcp` for user-space TCP/UDP flow translation. TCP packets accepted from the TUN stack become encrypted internal reliable streams with `IngressKind::TunTcp` over TCP, UDP, or mixed reliable underlays. UDP packets are demuxed by local/remote socket pair into bounded per-flow tasks and sent as encrypted internal datagram flows with `IngressKind::TunUdp` over the best live TCP or QUIC UDP datagram carrier. TUN supports IPv4, IPv6, or dual-stack addressing. UDP port 53 traffic can be explicitly remapped to configured `--tun-dns-resolver` addresses while responses are written back to the TUN client as if they came from the original DNS destination, so DNS traffic can pass through TUN without relying on host resolver defaults.

UDP targets can be reached through direct UDP, bind-source-IP direct UDP, upstream SOCKS5 UDP ASSOCIATE, or upstream HTTP CONNECT-UDP. The `http-connect-udp` outbound performs the RFC 9298 HTTP/1.1 Upgrade handshake, requires a `101 Switching Protocols` response with capsule support, and carries UDP payloads in HTTP Datagram capsules. Plain `http-connect` outbound is TCP-only.

Server direct and bind-source outbounds resolve domain targets through `--outbound-dns-resolver` when configured, or the system resolver when no explicit resolver is supplied. `--outbound-dns-strategy` controls IPv4/IPv6 lookup ordering and filtering for dual-stack targets. `--outbound-connect-timeout-ms` scopes target or upstream-proxy dial setup to the selected egress outbound. `--path-probe-timeout-ms` bounds only one configured idle MPP health-probe transaction. Initial Active setup budgets the serialized carrier and product-open exchanges: three PTOs for TCP and two for QUIC UDP when another candidate remains, or nine and eight respectively when a sole candidate must tolerate the `1 + 2 + 4` persistent-congestion PTO backoff after its phase prefix. Every initial Active TCP attempt retains the conservative initial PTO floor because its shared session actor may have to establish or re-establish the carrier. Active reattach, Repair, and Validation/recovery use one candidate PTO only when the command targets the same authenticated lane-class carrier generation; cold, concurrently connecting, or reconnect-spanning TCP attachments retain the three-phase setup budget and initial PTO floor. Queue wait, carrier setup, authenticated MPP and `PATH_JOIN`, product-open emission, current path-metric publication, and peer acceptance consume one absolute actor-selected deadline rather than restarting it.

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
