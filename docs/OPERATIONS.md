# Operations

## Platform check

Run before installing a service or enabling TUN mode:

```bash
mptunnel platform
```

It reports the OS/architecture, expected packet-device backend and privileges,
host-lifecycle guidance, and release target matrix. Only Linux
`/dev/net/tun` accessibility is probed immediately; other packet-device
providers open their device when the runtime starts.

`scripts/check-line-counts.sh` is a warning-only developer maintainability
check, not an operator health check.

## Privileges

SOCKS5 and HTTP CONNECT ingress need no elevated privilege when bound to normal
user ports. TUN mode needs host-approved network privileges.

- **Linux**: TUN and route setup need `CAP_NET_ADMIN` or equivalent service
  capability. Ports below 1024 need `CAP_NET_BIND_SERVICE`.
- **macOS**: packet-device and route/DNS configuration require an approved
  privileged service or launchd arrangement.
- **Windows**: package `wintun.dll` beside `mptunnel.exe`; device and route
  changes require an elevated process or service wrapper.
- **Android**: the embedding application obtains user `VpnService` consent,
  establishes the descriptor, and owns addresses, routes, MTU, revocation, JNI,
  and service lifecycle.

Android hosts must provide both `runtime::PacketDeviceProvider` and
`transport::CarrierNetworkProvider` to `runtime::run_with_host_providers`.
Resolve each endpoint on the selected native network and protect or bind every
TCP and QUIC socket before connect. The packet-device-only entry point uses the
system carrier network and is not valid for a catch-all Android VPN.

## Service mode

```bash
mptunnel --service-mode --supervise ...
```

`--service-mode` declares process intent. It does not install or register a
systemd unit, launchd job, Windows SCM service, or Android component.
`--supervise` restarts failed runtime generations with bounded exponential
backoff.

Use the host service manager as the outer process guard. A Windows SCM wrapper
or native adapter remains required; Android lifecycle callbacks remain owned by
the embedding application.

## Config and validation

The default path is `./config.toml`:

```bash
mptunnel --config ./config.toml --check-config
mptunnel --config ./config.toml
```

The graph contains tagged `[[inbounds]]`, `[[outbounds]]`, and optional
balancers. An inbound selects one outbound or balancer. MPP security and path
endpoints belong to each MPP inbound/outbound rather than to a global path role.

Use repeated explicit IPv4 and IPv6 listener/bind/resolver values. Do not rely
on OS-specific dual-stack defaults. Egress DNS strategy and connect timeout
belong to the outbound that performs resolution and connection.

Use `mptunnel --help` and subcommand help as the complete option and environment
variable reference. Validate configs in the target binary whenever possible;
cross-target parsing can also be smoke-tested under Wine.

## Path policy and status

Path URI query values have two different owners. `backup`, `expensive`,
`bulk-allowed`/`no-bulk`, `probe-only`, and `no-udp` are configured operator
restrictions for that path runtime; they remain in force until configuration or
management policy changes them. Rate, RTT, and jitter are startup measurement
priors that live evidence may replace. Neither category is a persistent
stream-placement role.

A configured path ordinal is endpoint-local composition identity. The server
preserves the accepting ordinal with its bound socket or QUIC endpoint; a
peer-supplied `path_id` is opaque and never indexes local configuration. Client
and server path lists therefore do not need matching order.

During authenticated setup the peer advertises sequence-zero directional
`PathUsage::{Available, Backup}`. This is separate from local path health.
Ordinary scheduling considers available paths first and uses backup paths only
when no eligible available choice exists. Metrics rank paths within the chosen
set. The receiver accepts only strictly newer later sequences, but this release has no runtime
control that originates a post-handshake preference change.

Management path controls change endpoint-local policy or lifecycle. They do not
rewrite peer usage, forge transport evidence, or assign a fixed data path to a
stream.

## Management API

Enable HTTP with `[management].listen` or `--management-listen`. Every listener
requires `[management].token` or `--management-token` and must use a loopback
address. For remote access, terminate TLS in a same-host reverse proxy or use an
SSH tunnel; the built-in HTTP server never binds directly to a non-loopback
address. Enable the embedded page explicitly with `dashboard = true` or
`--management-dashboard`; a browser then opens `/`.

All data and controls are under `/api/`:

- `GET /api/` returns the authenticated endpoint index and schema identifier.
- `GET /api/health` is the only public API route.
- `GET /api/status` returns the complete cached snapshot.
- `GET /api/paths` returns configured listeners, logical client paths, and live
  server carrier instances with their actual lifecycle state.
- `GET /api/traffic` returns monotonic product totals, one-second rates, and
  five minutes of one-second trend samples.
- `GET /api/sessions` returns authenticated MPP session ownership; `GET
  /api/flows` returns the bounded active reliable/datagram product-flow detail.
- `GET /api/diagnostics` returns local diagnostic capability and typed peer
  service/index/session selectors.
- `POST /api/control/path` changes endpoint-local client path lifecycle policy.
- `POST /api/diagnostics/peer` manually requests a sanitized peer snapshot.

Static page assets and `GET /api/health` are public so the browser and local
health checks can load before authentication. Every runtime-data and control
request requires `Authorization: Bearer <token>`. The default page retains it
only in memory and browser session storage, not a URL or persistent local
storage. The API has no CORS support and sends restrictive browser security
headers.

Tokens must contain 16-256 visible ASCII characters. The server rejects
duplicate authorization/content-type headers, transfer encoding, ambiguous
content lengths, pipelining, and non-origin request targets.

Forwarded totals are monotonic logical product counters. `to_peer` counts bytes
or datagrams accepted from the local product source; `from_peer` counts bytes or
datagrams delivered to the local product destination. They do not grow from carrier retransmission, MPP
reinjection, or multipath copies. Path delivery rate, queue, and flight remain
separate current carrier evidence. Numeric identifiers and monotonic byte
totals are decimal strings so browser clients do not lose 64-bit precision.
Per-flow detail is capped independently from forwarding capacity; aggregate
counters remain exact. Diagnostics report both current and cumulative detail
overflow, and per-session flow counts carry an explicit completeness flag.

Set `allow_peer_diagnostics = true` or
`--management-allow-peer-diagnostics` only on an endpoint that should answer an
authenticated peer. The permission is independent of local HTTP and disabled
by default. Either endpoint may initiate from its own management API; the
remote endpoint's flag decides whether it returns data. Responses contain only
per-session path state, usage, and metrics. They exclude endpoints, route
targets, local tags, credentials, and every other authenticated session. One
request per session may be in flight, requests time out, and responders admit
at most one snapshot request per session per second. A rate-limited or
codec-oversized complete snapshot returns `unavailable`; it is never truncated.
The dashboard does not poll the peer automatically.

Path control uses `enabled`, `suspect`, `failed`, or `disabled`. Enabling clears
the operator disable but leaves a path suspect until fresh carrier liveness
evidence restores it; management never manufactures an active observation.

## Resource envelopes

Leave `[resources]` unset first. These values are safety envelopes, not manual
transmission modes and not desired memory occupancy.

| Field | Default | Purpose |
| --- | ---: | --- |
| `max_frame_bytes` | 1 MiB | hard MPP-frame cap |
| `max_payload_bytes` | frame minus header room | MPP payload cap |
| `max_ack_ranges` | 256 | sparse Data ACK range cap |
| `max_paths` | 64 | registered path cap, not an aggregation target |
| `max_streams` | 65,536 | logical MPP stream cap |
| `max_quic_concurrent_bidi_streams` | 65,536 | QUIC stream concurrency envelope |
| `max_stream_window_bytes` | 64 MiB | per-direction MPP receive window shared across attachments |
| `max_repair_bytes` | 64 MiB | public compatibility name for retained MPP data available to reinject |
| `max_reorder_bytes` | 64 MiB | receive-hole and ordering-debt envelope |
| `max_datagram_queue_bytes` | 16 MiB | MPP datagram burst envelope |
| `max_path_flight_bytes` | 64 MiB | per-path MPP-flight ceiling |
| `max_reliable_relay_chunk_bytes` | 512 KiB | local read-buffer ceiling |
| TCP heartbeat | 10 s / 30 s | idle TCP carrier liveness |
| outbound connect timeout | 10 s | target/upstream dial bound |

Raise a bound only after diagnostics identify it as the limiting resource.
Increasing a window cannot authorize a path, prove delivery rate, or force
aggregation. Data sequence offsets, retained ranges, per-copy flight, transport queues,
and receive reordering have separate accounting owners.

Native TCP/QUIC congestion windows and flow-control windows are additional
independent limits. Each MPP direction has independent DSN, Data ACK, and
`STREAM_MAX_DATA` state. `STREAM_ACK` releases MPP ranges and flight but
grants no offset; `STREAM_MAX_DATA` grants offsets but acknowledges no byte. A
native transport ACK does neither at the MPP data level.

## Traffic and failover expectations

MPP may reinject an exact missing data-level range after causal stall/failure
evidence. Native TCP and QUIC recovery continue independently. Extra traffic
must therefore be interpreted as:

```text
wire payload = unique MPP data + native transport retransmission
             + MPP range reinjection + bounded control/measurement traffic
```

An operator should investigate sustained duplicate overhead, long zero-progress
gaps, or a healthy available path remaining unused under bulk backlog. Do not
work around these with a fixed path role or a Linux-only eligibility rule.

Ordinary reinjection is limited by a cumulative allowance derived from a
bounded startup floor and unique MPP bytes acknowledged by Data ACK.
Critical path-failure, persistent authoritative Data ACK gap, and bounded
live-tail recovery may exceed the remaining allowance by one cause-specific
event quantum so that budget exhaustion cannot deadlock recovery. Exact
retained ranges, queue and flight limits, overlap/repeat suppression, and
alternate-output requirements still apply. Exception bytes remain charged,
reducing later optional authority. A continuous over-budget stream is therefore
a defect, not expected failover overhead.

The current timers are cause-specific. Exact path-instance failure permits an
immediate bounded copy, preferring measured survivors but using any eligible
live survivor when necessary. An authoritative lowest missing Data Sequence
frontier must persist for three owner-carrier recovery intervals; TCP uses RTO
and QUIC uses PTO. Growth of the ACK horizon above it does not restart the
timer. A contiguous live tail may send one bounded probe after one such
interval and waits three intervals before another probe without progress. A
request path becomes stale for new placement after four TCP RTOs or three QUIC
PTOs without exact Data ACK progress when another attachment exists; this does
not terminate native recovery. These are MPP recovery policies, not native TCP
or QUIC retransmission timers.

MPP datagram feedback confirms target-worker admission, not end-to-end target
delivery. An alternative-path attempt receives a new flow-local datagram ID;
operators must therefore allow for duplicate delivery if a delayed first
attempt and its retry both reach the target.

Idle TCP heartbeats and authenticated path probes detect reachability. Active
failover additionally uses exact MPP progress, path-instance lifetime, PTO,
and queue/flight evidence. A reconnect creates a new physical path instance;
old flights and evidence cannot be inherited from its numeric path ID. A
stream attachment incarnation is separate, so detach and reattach
also cannot inherit ownership merely because the carrier stayed live.

The concrete fences differ by direction. Request scheduling uses the physical
path instance plus `attachment_id`. Response new-data dispatch uses the physical
path instance plus output incarnation and the response-model generation observed
during planning; apply revalidates them while reserving queue credit and
recording the logical path key and stream-unique output incarnation in the exact
original flight.

On the response sender, the output carrying the contiguous Data Sequence
frontier remains governed by the shared MPP receive window and native carrier
credit. An additional output without durable, unambiguous Data ACK coverage of
original transmissions is restricted to one bounded startup flight. Reaching
the startup sample floor unlocks mature additional-path placement. Native TCP
or QUIC ACK evidence alone does not unlock it, and Data ACK of duplicated bytes
is not attributed to either copy.

## Optional Linux telemetry

Linux carriers may sample `TCP_INFO` from the exact authenticated socket. The
adapter reports only fields actually returned by the kernel; missing fields are
unknown rather than zero. Passive native observations are scheduling evidence,
not MPP Data ACKs.

TCP capacity transactions use receiver-confirmed receipts and may combine them
with exact-socket telemetry. QUIC publishes fresh native packet-ACK-derived
evidence with an explicit expiry and relies on its native congestion controller
for send credit; it has no separate MPP calibration transaction. These proof
states are not interchangeable. MPP Data ACK remains the carrier-neutral delivery authority;
for response bulk admission, QUIC requires locally sourced ACK-derived carrier
evidence, while durable unambiguous Data ACK progress may additionally establish
a per-flow TCP MPP rate.

The adapter is optional. Windows, macOS, Android, unsupported kernels, and
restricted hosts use the portable fallback and remain correct and eligible.
No config or topology should require `TCP_INFO` for normal operation.

## Encryption

All MPP paths are encrypted. TCP paths use the MPP record layer, which defaults
to AES-256-GCM. Configure `chacha20-poly1305` on both peers when appropriate for
the deployment CPU; the TCP record cipher is not negotiated. QUIC paths use TLS
1.3 and QUIC packet protection through rustls; the MPP `cipher` setting does not
select the QUIC TLS cipher suite.

The shared secret must be a random UUID or at least 32 bytes of high-entropy
text. MPP derives domain-separated transport and authentication keys. Session
and path authentication issue times are checked against the configured
freshness window, 300 seconds by default.

## Packaging

```bash
scripts/package-release.sh --target x86_64-unknown-linux-musl
pwsh scripts/package-release.ps1 -Target x86_64-pc-windows-msvc
```

Release targets:

- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`
- `aarch64-pc-windows-msvc`

Each archive contains the product binary, README, license, and docs. Packaging
emits a sibling SHA-256 checksum. Windows archives also contain the pinned
architecture-matched Wintun DLL and upstream license. Packaging validates that
artifact; only a native Windows host can prove Wintun device, route, DNS, and
service integration.

Linux release archives use musl targets and do not depend on a host glibc
baseline. CI performs an Android aarch64 library source check with the NDK; it
does not build an APK or prove device runtime.

## Releases

Tags matching `v*` run format, clippy, tests, target packages, checksums, and
GitHub Release publication. Manual workflow dispatch creates artifacts but does
not publish unless the ref is a tag.

The release archive does not include the benchmark crate, Docker lab scripts,
generated results, or lab-only diagnostics. The production binary is built as
`--bin mptunnel` without the `lab-diagnostics` feature.

## Verification policy

Normal format, clippy, unit, integration, and target checks run on hosts without
modifying networking. TUN, route, DNS, netem, blackhole, and privileged service
experiments belong in Docker or a dedicated native test machine.

Wine is suitable for Windows executable startup, CLI, and config parsing only.
Record native Windows integration and real-Internet results as not run when the
required environment is unavailable; do not substitute Linux Docker evidence.

Performance acceptance follows [`docs/LAB.md`](LAB.md). Historical pre-v2 rows
are regression references, not current release proof.
