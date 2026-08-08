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

## Privileges

SOCKS5 and HTTP CONNECT ingress need no elevated privilege when bound to normal
user ports. TUN mode needs host-approved network privileges.

- **Linux**: TUN and route setup need `CAP_NET_ADMIN` or equivalent service
  capability. Ports below 1024 need `CAP_NET_BIND_SERVICE`.
- **macOS**: proxy mode can run as an ordinary user. VPN packet flow
  and route/DNS publication require a signed, entitled Network Extension host;
  a privileged process supervisor alone is insufficient.
- **Windows**: keep the package's architecture-matched `wintun.dll` beside
  `mptunnel.exe`; managed VPN auto-discovers that sibling DLL. Wintun creation
  and route/DNS changes require an elevated process or service wrapper.
- **Android**: the embedding application obtains user `VpnService` consent,
  establishes the descriptor, and owns addresses, routes, MTU, revocation, JNI,
  and service lifecycle.

Catch-all Android hosts must call `runtime::run_with_vpn_host_providers` with:

- a `platform::PacketDeviceProvider` for the established `VpnService`
  descriptor;
- a `transport::CarrierNetworkProvider` that resolves and constructs MPP
  carrier sockets on the selected native network; and
- one `transport::HostSocketProtector` that synchronously calls
  `VpnService.protect` for the borrowed descriptor it receives.

The same protector is applied exactly once to every carrier and
MPTUNNEL-created native target/proxy/DNS TCP/UDP socket, after source binding
and before connect or first send. Returning an error drops the socket and fails
the egress attempt closed. Operating-system DNS policies are rejected before
device or socket startup: OS resolver sockets are hidden from this callback and
therefore cannot safely bypass a catch-all VPN. Configure literal-bootstrap or
outbound-backed DNS instead.
Never close or retain the borrowed descriptor; duplicate it explicitly if the
host needs separate ownership. The carrier provider must only resolve,
select/bind a native network, and create the socket; it must not invoke the
protector independently. Apple packet-tunnel embeddings use the same callback
for their equivalent native-network exclusion or binding.

`run_with_packet_device_provider` and `run_with_host_providers` leave some
egress socket classes on system behavior and are not valid for a catch-all
embedded VPN. `run_with_all_host_providers` remains an advanced seam for hosts
that deliberately own separate adapters.

## Service mode

```bash
mptunnel --service-mode --supervise ...
```

`--service-mode` declares process intent. It does not install or register a
systemd unit, launchd job, Windows SCM service, or Android component.
`--supervise` restarts failed runtime generations with bounded exponential
backoff.

Use the host service manager as the outer process guard. A Windows SCM wrapper
is optional operational integration rather than a missing VPN adapter; Android
lifecycle callbacks remain owned by the embedding application.

### Process logs

```toml
[logging]
level = "info"          # off, error, warn, or info
format = "text"         # text or json
console = true          # standard error
# file = "logs/mptunnel.log"
flow_events = false
```

The console and optional append-only file receive the same structured text or
newline-delimited JSON records. Relative file paths are resolved beside
`config.toml`, while absolute paths are used as written; their parent directory
must already exist. A CLI-relative `--log-file` is resolved by the operating
system from the process working directory. The log file must not alias the
canonical configuration document or its recovery sidecars. Check-only and API
validation never create the file. Startup fails clearly if a requested file
cannot be opened, and API apply rejects an unusable sink before it can replace
or interrupt the active runtime.

Default text records use UTC RFC 3339 time, an uppercase level, and one stable
`component.event` name:

```text
2026-08-06T02:14:03.482Z INFO  process.starting: MPTUNNEL <version> starting
2026-08-06T02:14:03.483Z INFO  configuration.loaded: Loaded ./config.toml (revision <revision>)
2026-08-06T02:14:03.491Z INFO  inbound.listening: local-socks: SOCKS5 listening on 127.0.0.1:1080
2026-08-06T02:14:03.496Z INFO  process.generation_ready: runtime generation <revision> is ready
```

Startup records identify the configuration generation and safely summarize
configured inbounds, outbounds, MPP paths, balancers, DNS, and management
listeners. Lifecycle records never include credentials, tokens, or keys, and
sensitive forms in fault messages are redacted. Listener records are emitted
only after binding succeeds; generation readiness means the configured local
listeners and host-facing services have started. Outbound carrier health is
independent and remains visible through path diagnostics.

Startup also launches one process-scoped update check against GitHub's public
latest-release API. It uses a direct, server-authenticated HTTPS connection to
`api.github.com`, sends no credentials or machine identity, and therefore
reveals only the request's public source address and timing to GitHub. The
five-second bounded task never gates listeners, readiness, forwarding, or
shutdown. It reports `update.available` with the canonical GitHub release URL,
`update.current` with the newest checked tag, or `update.check_failed` at
information level when the network or metadata is unavailable.

Simple CLI profiles expose the same common settings through `--log-level`,
`--log-format`, `--log-file`, `--log-no-console`, and `--log-flow-events`.
The matching environment variables are `MPTUNNEL_LOG_LEVEL`,
`MPTUNNEL_LOG_FORMAT`, `MPTUNNEL_LOG_FILE`, `MPTUNNEL_LOG_NO_CONSOLE`, and
`MPTUNNEL_LOG_FLOW_EVENTS`.

When logging is enabled, at least the console or file sink must be enabled.
Flow events require `info`, because they are information records. `error` means
the process or a configured sink cannot continue; `warn` means the service is
still running but degraded, retrying, saturated, or recovering; `info` covers
configuration and service lifecycle transitions.

All records are length-bounded and remove terminal control characters. Repeated
saturation and fault records are rate-limited per call site and report the
suppressed count on the next record. Authorization, cookies, tokens, passwords,
credential secrets, and private-key forms are redacted as a final defense.
`flow_events = true` additionally emits one sanitized
open and close record for each observable flow, including its inbound,
destination, selected concrete outbound, duration, outcome, and byte/packet
counts. It deliberately omits source addresses, principals, credentials,
session/protocol IDs, carrier endpoints, and payload. Destinations remain
privacy-sensitive, so flow logging is disabled by default.

A live logging change applies to newly opened flows. A flow whose open record
was emitted retains that sink and format through its close record, so enabling,
disabling, or moving flow logs does not split lifecycle pairs.

Logging performs no per-packet or per-byte output and never enters forwarding,
scheduling, recovery, congestion, or carrier loops. File rotation is
host-owned. Changing logging settings or restarting the process reopens the
file; an unrelated runtime-generation replacement retains the already-open
descriptor. New files are owner-only on Unix hosts; permissions on existing
files and non-Unix ACLs remain host-owned. Failed QUIC handshakes are
pre-authentication attacker input and are intentionally silent.

## Config and validation

The default path is `./config.toml`:

```bash
mptunnel --config ./config.toml --check-config
mptunnel --config ./config.toml
```

The graph contains explicitly named `[[inbounds]]`, `[[outbounds]]`, and
optional `[[routing.balancers]]`. Every configured resource name is canonical
lowercase ASCII. Configured-resource references use the selected resource noun:
`inbounds`, `outbound`, `balancer`, or `dns_plan`. The `_id` suffix is
reserved for protocol, principal, or signed-artifact identities such as
`credential_id`, `principal_id`, `rule_set_id`, and `publisher_id`; `target`
is reserved for an application or active-probe destination authority, while
`endpoint` is reserved for a listener, connector, or carrier network endpoint.
MPP security and named carrier paths belong to each MPP inbound/outbound
rather than to a global path role.

When `initial_demand` is omitted or set to `"automatic"`, reliable streams
begin latency-oriented and datagrams begin realtime-oriented. Live reliable
demand can move between latency and throughput scheduling without reopening the
stream. Set `initial_demand = "throughput"` only for a route whose workload is
known to be bulk from its first bytes. This is an admission hint, not a fixed
path choice or a permanent traffic class.

Each MPP outbound selects one credential with `credential_id`; each MPP
inbound accepts one or more credentials with `credential_ids`. Separate MPP
outbounds may select separate credentials. `tls_server_name` defaults to
`mptunnel.example` and identifies the pinned QUIC and TLS-fallback TCP
certificate. Shipped configurations set `transport_secret_file` to one raw
32-byte endpoint-wide secret shared by the two peers; it is optional and is not
an MPP client credential.

`tcp-forward` and `udp-forward` local inbounds require explicit non-zero
listeners and one canonical `target`. TCP overload is closed immediately at
`max_connections`; UDP silently drops new sources at `max_associations`,
expires source associations at `idle_timeout_ms`, and bounds each datagram by
`datagram_ttl_ms`. Both use the ordinary route/DNS/ACL/outbound/balancer path;
they do not dial around configured outbounds.

Both `protocol = "tun"` (TUN-L4) and `protocol = "tun-l3"` are experimental.

`protocol = "tun-l3"` is the raw IP-tunnel ingress. It selects exactly one MPP
outbound and receives its IPv4 and/or IPv6 host address from that MPP server's
authenticated principal allocation. The server configures pools, its own TUN
addresses, and explicit principal allocations under `[inbounds.tun_l3]`;
`allowed_ips` adds externally routed prefixes owned by that principal. This
packet plane does not use Product routing, DNS, destination ACLs, target
outbounds, or the TUN-L4 userspace TCP/UDP stack. Carrier endpoint names still
use normal carrier resolution; inner packet destinations do not. Operators
remain responsible for host routes, IP forwarding, DNS, firewall rules, and
NAT on both ends. TCP and QUIC carrier paths are both eligible.

The nested server packet service is additive to that MPP inbound's ordinary
reliable-stream and datagram proxy service, whose egress, DNS plan, and
destination ACL remain active. For a packet-only inbound, finish its destination
ACL with a catch-all deny rule:

```toml
[[inbounds.destination_acl.rules]]
name = "deny-proxy-plane"
effect = "deny"
```

One authenticated principal has one live logical IP tunnel per MPP inbound. A
new session for the same principal supersedes the previous tunnel attachments;
this provides locator-independent restart and roaming rather than deriving
identity from a source address. See `examples/config.reference.toml` for the
complete commented shape.

Balancer strategies are `manual`, `ordered-failover`, `round-robin`, `random`,
`weighted-random`, `least-latency`, and `least-load`. Members may start
`enabled`, `draining`, or `disabled`; only enabled members receive new flows.
Optional destination or principal stickiness is bounded by both TTL and entry
capacity. Active probes require a literal IP TCP target and run under one
process-wide concurrency bound. Least-latency uses only fresh end-to-end
observations, while least-load uses active-flow leases. Neither
strategy reads MPP carrier/path metrics.

Each selected local TCP or native UDP member receives its configured outbound
connect timeout, and an MPP TCP member receives its MPP stream-open timeout. A
blackholed member therefore cannot consume a successor's attempt budget, and
unrelated outbounds do not lengthen or shorten the selected balancer. MPP UDP
has no pre-commit network-open stage; its first send supplies the open outcome.
If an IP-only member needs target DNS, the flow performs one lookup under the
selected DNS plan's own timeout. Successful authorization promotes the target
to one retained address set. Failure is retained for that flow, skips later
IP-only members without another lookup, and may continue to a domain-capable
member without marking any skipped member unhealthy. The complete pre-commit
operation remains bounded by the configured member count and stage timeouts. A
failed member may be retried only before the target connection or association
commits. After commit, success/failure is passive health evidence and never
authorizes transparent replay through another member.

Use repeated explicit IPv4 and IPv6 listener/bind/resolver values. Do not rely
on OS-specific dual-stack defaults. Egress DNS strategy and connect timeout
belong to the outbound that performs resolution and connection.
`[dns].default_dns_plan` selects the named plan used when no DNS rule matches.

`[session].retention_timeout_ms` and
`--session-retention-timeout-ms` set the absolute time an established logical
stream may remain without any authenticated carrier and the absolute ceiling
for graceful TCP carrier retirement. The default is 300,000 ms. Retries and
retirement progress never extend a deadline. Healthy idle streams with a live
carrier do not consume it; TCP heartbeat and native QUIC idle timers remain
separate.

Use `mptunnel --help` and subcommand help as the complete option and environment
variable reference. Validate configs in the target binary whenever possible;
cross-target parsing can also be smoke-tested under Wine.

## Operational CLI

`config.toml` remains the only persistent profile. Operational commands do not
create a second configuration format and do not start a runtime generation.

Run the read-only doctor before first start or after editing the file:

```bash
mptunnel --config ./config.toml doctor
```

It strictly parses the complete config, resolves every referenced secret and
TLS file, validates the target's managed-VPN lifecycle contract, reports the
platform packet-device capability, and checks configured control endpoints.
Literal-IP MPP UDP and other datagram endpoints are address-checked. Literal-IP
TCP carrier, proxy, DoT, and DoH endpoints receive only a bounded connect
probe; application destinations are never probed. Domain endpoints never use
host/system DNS during doctor: they are reported as skipped because configured
runtime DNS and routing own resolution and connection setup. A configured
routed or source-bound literal endpoint is not dialled outside its owner.
A ranged outbound carrier endpoint is likewise reported as `INFO` and skipped:
probing one concrete port cannot validate an externally published range, and
runtime carrier selection remains authoritative.

Every port in an outbound carrier range must forward or redirect to the same
fixed server listener. `port-hop-interval-ms` is accepted on ranged TCP and
QUIC paths, defaults to five minutes, and has a 5000 ms minimum. QUIC uses a
fresh protected socket while retaining the authenticated native connection;
native QUIC migration and validation remain authoritative. TCP waits for an
exact active-work quiescent boundary and replaces only that member ordinal.
With a
spare physical reservation it may establish the successor before draining the
predecessor; at the full envelope it retires the predecessor first. It never
transfers native transport state or closes active work to meet the interval.
`tcp-carriers=MIN-MAX` applies only to TCP outbound paths and defaults to
`1-3`; only `MAX` controls the current pool. `MIN` is obsolete and ignored.
With one configured TCP endpoint, every member is regular capacity. With
multiple TCP endpoints, each primary is regular and its additional ready
members are backup capacity. Server listener paths reject this client capacity
policy.

Each check is `PASS`, `WARN`, `FAIL`, or `INFO`. Invalid configuration,
invalid target VPN configuration, or a failed explicitly requested
`--management-address` check exits non-zero. A currently stopped configured
runtime or unavailable remote endpoint is a warning and exits zero, so offline
preflight remains useful. Doctor never changes host routes, DNS, caches, or
runtime configuration.

Explain the exact immutable route table without opening a socket:

```bash
mptunnel --config ./config.toml --principal-id local-user route explain \
  --target api.example:443 \
  --network tcp \
  --source 127.0.0.1:41000 \
  --inbound local-socks
```

Omitting `--resolved-ip` evaluates the pre-resolution stage. Supplying it
evaluates post-resolution policy. Route explanation accepts only attributes
that every live ingress supplies: destination, resolved IP, network, source,
principal, and inbound. Output separately identifies the pre-resolution rule
and DNS plan that owned resolution, then the selected stage rule, action,
outbound or balancer, initial demand, every rule's first mismatch, and the
ID/publisher/revision/expiry/hash of each consulted signed rule set.

Runtime status and DNS operations use only the authenticated versioned API:

```bash
mptunnel --config ./config.toml status
mptunnel --config ./config.toml dns status
mptunnel --config ./config.toml dns explain example.com
mptunnel --config ./config.toml dns query example.com --type HTTPS
mptunnel --config ./config.toml dns flush
mptunnel --config ./config.toml dns flush --dns-plan private
```

With `--config`, the client uses the first configured management listener and
its resolved token. Without it, pass `--address` plus
`--management-token-file` or `--management-token-env`. The address must be
loopback. The client has fixed connection/I/O, HTTP header, body, and JSON
bounds; rejects redirects, transfer encoding, duplicate framing headers and
non-JSON responses; and never includes a token in errors, debug output, or
rendered JSON. Status and DNS output is pretty JSON. DNS explain is read-only;
DNS query performs the requested lookup; DNS flush is an explicit cache
mutation.

## Path policy and status

Path URI query values have two different owners. `backup`, `expensive`,
`bulk-allowed`, `probe-only`, and `no-udp` are configured operator
restrictions for that path runtime; they remain in force until configuration or
management policy changes them. Rate, RTT, and jitter are startup measurement
priors that live evidence may replace. Neither category is a persistent
stream-placement role.

A configured path `name` is its stable endpoint-local path identity. The
server preserves that name with its bound socket or QUIC endpoint; a
peer-supplied `path_id` is opaque runtime identity and never indexes local
configuration. Client and server path lists therefore do not need matching
names or order.

During authenticated setup the peer advertises sequence-zero directional
`PathUsage::{Available, Backup}`. This is separate from local path health.
Ordinary scheduling considers available paths first and uses backup paths only
when no eligible available choice exists. Metrics rank paths within the chosen
set. The receiver accepts only strictly newer later sequences. Runtime control
does not originate a post-handshake preference change.

Management path controls change endpoint-local policy or lifecycle. They do not
rewrite peer usage, forge transport evidence, or assign a fixed data path to a
stream.

## Management API

Enable HTTP with `[management].listen` or `--management-listen`. Every listener
requires `[management].token`, `--management-token-file`, or
`--management-token-env` and must use a loopback address. For remote access,
terminate TLS in a same-host reverse proxy or use an SSH tunnel; the built-in
HTTP server never binds directly to a non-loopback address. Enable the embedded
page explicitly with `dashboard = true` or `--management-dashboard`; a browser
then opens `/`.

TOML and CLI accept the management token only through a file or environment
reference, for example
`token = { from = "file", path = "management-token.key" }`. Secret bytes never
belong in TOML, argv, diagnostics, or the runtime configuration API.

All data and controls are authenticated under `/api/v2/`:

- `GET /api/v2/` returns the endpoint index with
  `mptunnel.management.v6`.
- `GET /api/v2/health`, `GET /api/v2/health/live`, and
  `GET /api/v2/health/ready` return `mptunnel.health.v2`. The latter two gate
  terminal generation failure and serving readiness respectively.
- `GET /api/v2/status` returns the complete cached
  `mptunnel.management.v6` snapshot, including sanitized Product inbound and
  outbound inventory plus a separate TUN-L3 service inventory. Credentials,
  address pools, allocation contents and identities, and native proxy connector
  endpoints are absent; configured MPP carrier endpoints are present in the
  authenticated local path inventory.
- `GET /api/v2/paths` returns configured named paths and live carrier
  instances with their lifecycle state.
- `GET /api/v2/traffic` returns monotonic forwarded totals, one-second rates,
  and five minutes of one-second trend samples.
- `GET /api/v2/sessions` returns authenticated MPP session ownership.
- `GET /api/v2/flows` returns bounded active reliable/datagram logical-flow
  detail, including the origin inbound, application target, selected outbound,
  and optional balancer.
- `GET /api/v2/diagnostics` returns local diagnostic capability, peer session
  references, controls, and path state.
- `GET /api/v2/config` returns `mptunnel.config.v2` with the canonical path,
  desired, active, runtime, and pending revisions, mutation endpoints, and
  required precondition. It never returns TOML or resolved secrets.
- `GET /api/v2/balancers` returns `mptunnel.balancer.v1` with named balancer
  and outbound-member readiness, freshness, load, observations, probes,
  circuit state, and counters.
- `GET /api/v2/dns/status` returns `mptunnel.dns.status.v2` with DNS
  generation, cache, in-flight query, upstream, and FakeDNS state.
- `GET /api/v2/dns/explain?domain=<domain>` returns
  `mptunnel.dns.explain.v2` without issuing a query.
- `POST /api/v2/actions/path` accepts exactly
  `{ "outbound": "...", "path": "...", "state": "..." }`; `state` is
  `enabled`, `suspect`, `failed`, or `disabled`.
- `POST /api/v2/diagnostics/peer` accepts exactly
  `{ "service": "mpp_outbound", "service_name": "...", "session_id": "..." }`
  or the corresponding `mpp_inbound` service.
- `POST /api/v2/config/validate` accepts one bounded UTF-8
  `application/toml` document, validates it and its referenced material, and
  returns its revision without writing or reloading.
- `POST /api/v2/config/apply` accepts the same complete document and exactly
  one `If-Match: sha256:...` revision from `GET /api/v2/config`. It persists
  only when the desired revision still matches.
- `POST /api/v2/balancers/actions` accepts exactly `balancer`, `action`, and,
  except for `automatic`, `outbound`. Actions are `enable-member`,
  `drain-member`, `disable-member`, `pin-member`, and `automatic`; responses
  use `mptunnel.balancer.v1`.
- `POST /api/v2/dns/query` accepts exactly
  `{ "domain": "...", "type": "..." }` and returns
  `mptunnel.dns.query.v2`.
- `POST /api/v2/dns/cache/flush` accepts `{}` or
  `{ "dns_plan": "..." }` and returns `mptunnel.dns.flush.v2`.

Every `service_index` in a response is presentation-only. Mutations select
stable configured names (`outbound`, `path`, `balancer`, and `service_name`)
plus the protocol `session_id`; they never accept an index.

Configuration mutation is deliberately full-document only: there is no
`PATCH`, field update, history, or diff API. Process logging and inbound
credential-authority changes may publish live when they are the complete
change. A changed logging sink is prepared inside the serialized apply
transaction after the document is staged; preparation failure rolls the
document back before the active runtime is changed or a reload is requested.
Every routing, DNS, transport, resource, timeout, client-credential, TLS, or
shared-transport-secret change activates through a clean runtime-generation
replacement. A changed logging sink included with such a replacement is
preflighted on apply and opened definitively at activation; unchanged logging
retains its existing descriptor. Management listener or authentication
changes are rejected by the API and require a local file edit and restart.

An identical TOML document is idempotent even if a referenced file was replaced.
For an online certificate, key, CA, pin, transport-secret, proxy-password, or
signed-rule-set rotation, write the material under a new versioned path and
apply the document that names it. A shared transport secret changes as a
coordinated generation cutover; it is not tried alongside an old key. A normal
process restart re-reads material at unchanged paths. Credential principal or
secret rotation uses a new credential ID so overlap and retirement remain
explicit.

Persistence is activation-safe. A newly persisted document remains pending
while the prior active document is the durable last-good configuration. The
candidate becomes last-good only after every required service in its
generation reports ready. Failure before readiness rolls back the canonical
file, and startup recovery resolves an interrupted activation from the pending
journal and last-good document; inconsistent external edits fail closed
instead of being overwritten.

Only static dashboard assets are unauthenticated. Every API request, including
health probes, requires `Authorization: Bearer <token>`. The default page
retains it in same-origin browser local storage until the operator uses
**Forget token** or the server rejects it; it never places the token in a URL.
The API has no CORS support and sends restrictive browser security headers.

`live` means the process can answer and its generation has not entered terminal
failure. `ready` additionally requires a ready generation, a connected MPP
outbound when one is configured, and at least one ready member in each
configured balancer. `degraded` reports partial MPP/balancer/path loss, a pending
configuration activation, or a queried DNS plan that has never produced a
successful upstream result. Listener and DNS/session/balancer facts used for the
decision are included in the response; inbound-only servers remain ready while
waiting for clients.

Tokens must contain 16-256 visible ASCII characters. The server rejects
duplicate authorization/content-type headers, transfer encoding, ambiguous
content lengths, pipelining, and non-origin request targets.

Forwarded totals come from one generation-owned forwarding observer and are
monotonic logical counters. `to_peer` counts bytes or datagrams accepted from
the local source; `from_peer` counts bytes or datagrams delivered to the local
destination. They do not grow from
carrier retransmission, MPP reinjection, multipath copies, DNS connector work,
or path probes. Native and MPP boundaries share the owner but never observe one
flow twice. Path delivery rate, queue, and flight remain separate current
carrier evidence. Numeric identifiers and monotonic byte totals are decimal
strings so browser clients do not lose 64-bit precision.
Per-flow detail is capped independently from forwarding capacity; aggregate
counters remain exact. Diagnostics report both current and cumulative detail
overflow, and per-session flow counts carry an explicit completeness flag.

Set `allow_peer_diagnostics = true` or
`--management-allow-peer-diagnostics` only on an endpoint that should answer an
authenticated peer. The permission is independent of local HTTP and disabled
by default. Either endpoint may initiate from its own management API; the
remote endpoint's flag decides whether it returns data. Responses contain only
per-session path state, usage, and metrics. They exclude endpoints,
application targets, local resource names, credentials, and every other
authenticated session. One
request per session may be in flight, requests time out, and responders admit
at most one snapshot request per session per second. A rate-limited or
codec-oversized complete snapshot returns `unavailable`; it is never truncated.
The dashboard auto-refresh control applies one completion-driven cadence to
both local status and the currently selected peer diagnostic request: 1 s,
5 s, 30 s, or manual only. It never overlaps cycles. Peer requests occur only
when the local endpoint advertises a connected diagnostic control session;
the returned `ok`, `disabled`, or `unavailable` code exposes the remote
endpoint's decision. Either client or server may be the requesting side. The
Overview and Diagnostics pages show whether this local endpoint will answer
peer requests. Manual mode sends no periodic local or peer request.

Path control uses `enabled`, `suspect`, `failed`, or `disabled`. Enabling clears
the operator disable but leaves a path suspect until fresh carrier liveness
evidence restores it; management never manufactures an active observation.

Balancer actions have the same evidence rule. Enabling a member permits new
selection but does not invent a successful probe. `drain-member` immediately
stops new selection while established flows finish on their existing leaf.
`pin-member` is an explicit manual override; `automatic` returns a non-manual
strategy to configured ranking. A balancer configured with strategy `manual`
must always retain a pin and therefore rejects `automatic`. These actions have
`runtime-generation` scope: persist the corresponding member mode or
`manual_outbound` in TOML through the configuration API when it must survive a
restart or configuration reload.

## Resource envelopes

Leave `[resources]` unset first. These values are safety envelopes, not manual
transmission modes and not desired memory occupancy.

| Field | Default |
| --- | ---: |
| `max_frame_bytes` | 1 MiB |
| `max_payload_bytes` | 1,048,512 B |
| `max_ack_ranges` | 256 |
| `max_paths` | 64 |
| `max_streams` | 65,536 |
| `max_quic_concurrent_bidi_streams` | 65,536 |
| `max_stream_window_bytes` | 64 MiB |
| `max_repair_bytes` | 64 MiB |
| `max_reorder_bytes` | 64 MiB |
| `max_reinjection_cache_chunks` | 65,536 |
| `max_reorder_buffer_chunks` | 65,536 |
| `max_retained_receive_ranges` | 65,536 |
| `max_datagram_queue_bytes` | 16 MiB |
| `max_path_flight_bytes` | 64 MiB |
| `max_reliable_relay_chunk_bytes` | 512 KiB |
| `tcp_path_heartbeat_interval_ms` | 10,000 ms |
| `tcp_path_heartbeat_timeout_ms` | 30,000 ms |
| `quic_path_keep_alive_interval_ms` | 10,000 ms |
| `quic_path_idle_timeout_ms` | 30,000 ms |

The four byte-window fields compose rather than replace one another. Estimate
aggregate BDP as `sum(rate_bps × RTT_seconds) / 8`. Aggregate admitted work is
bounded by the applicable `max_stream_window_bytes`, `max_repair_bytes`, and
`max_reorder_bytes` envelopes plus the sum of independently applicable
per-path `max_path_flight_bytes` envelopes. Each path must still cover its own
BDP. Raise the relevant fields coherently on both endpoints only when
diagnostics show that envelope, and keep
`max_path_flight_bytes <= max_repair_bytes`. If
`max_path_flight_bytes` is omitted from TOML, it inherits the effective repair
limit. Larger windows increase worst-case retained memory; available RAM alone
is not an automatic sizing signal.

The 64 MiB defaults cover one raw BDP up to about 537 ms at 1 Gbps or 53.7 ms
at 10 Gbps. They are configurable local bounds, not protocol constants. At
10 Gbps and 100 ms RTT, a 64 MiB logical window has a rough 5.37 Gbps
window/RTT ceiling and must be raised for line rate. Frame, payload, chunk, and
sparse-range limits are separate safeguards and do not need to grow with BDP.

Each proxy outbound has its own `connect_timeout_ms`; the default is 10,000 ms.

`[admission]` is the independent new-flow envelope used before DNS, target
connects, or other flow-opening I/O. Defaults are finite:

| Field | Default |
| --- | ---: |
| `max_live_flows` | 4,096 |
| `max_concurrent_work` | 512 |
| `max_live_flows_per_principal` | 1,024 |
| `max_live_flows_per_outbound` | 3,072 |
| `max_connects_per_outbound` | 256 |
| `max_live_flows_per_target` | 256 |
| `max_connects_per_target` | 32 |
| `max_dns_work` | 128 |

SOCKS5, HTTP CONNECT, fixed forwarding, TUN-L4, and authenticated MPP server
opens share this one generation owner. Their listener/source/association
limits still compose at their narrower boundary. Permits release exactly on
close, error, cancellation, or generation retirement and never enter payload
forwarding. TUN-L3 packet forwarding has its own bounded packet queues and does
not consume Product flow admission. These fields do not derive from
`[resources]`; raising an MPP stream or queue budget never raises new-flow
admission.

An MPP client outbound that has previously authenticated a carrier rejects new
new flows while its exact authenticated-carrier count is zero. Existing
flows remain retained under `[session]`, and the normal TCP/QUIC maintenance
services continue reconnection. A newly started configuration generation may
perform its first bounded establishment attempt; no source-address heuristic or
additional retry timer is involved.

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
live survivor when necessary. Complete Data ACKs establish missing ranges;
positive partial ACK ranges may extend established state but cannot infer an
omission. Fragmented request feedback waits one owner RTO/PTO from first
authoritative gap observation. Response feedback may use a later-ACK TCP RACK
5/4-SRTT or QUIC 9/8-SRTT time threshold; ACK silence waits owner RTO/PTO. A
contiguous live tail may send one bounded probe per recovery interval without
progress. A request path becomes stale for new placement after four TCP RTOs
or three QUIC PTOs without exact Data ACK progress when another attachment
exists; this does not terminate native recovery.

MPP datagram feedback confirms target-worker admission, not end-to-end target
delivery. Before feedback, the runtime makes at most two product attempts. Both
attempts retain the same session, flow, and datagram identity; the shared server
flow forwards that identity to the target at most once and replays a bounded
cached response to the retry carrier. A ranked alternative is tried after one
modeled response timeout; the final or only attempt keeps three such timeouts,
capped by the absolute TTL. After feedback, the request is never replayed and
its response may be awaited until that TTL. The guarantee is at-most-once target
forwarding within retained MPP state, not end-to-end exactly-once UDP delivery.

Configured QUIC paths retain their authenticated native connection, while each
TCP endpoint reconciles its bounded group toward the configured maximum.
Diagnostic probes use isolated connections; idle TCP heartbeats and native
QUIC keep-alives own carrier liveness. Together they detect reachability
without placing diagnostic work in an MPP data stream. Active
failover additionally uses exact MPP progress, path-instance lifetime, PTO,
and queue/flight evidence. A reconnect creates a new physical path instance;
old flights and evidence cannot be inherited from its numeric path ID. A
stream attachment incarnation is separate, so detach and reattach also cannot
inherit ownership merely because the carrier stayed live.

When every carrier disappears, an established logical stream retains its MPP
sequence, Data ACK, receive-window, FIN, and bounded repair/reorder state while
the client rotates reconnect attempts across configured TCP and QUIC paths.
Both endpoints stop reading their local product socket so ordinary TCP
backpressure bounds memory. Reattachment within the session-retention deadline
continues the same stream; expiry closes both local sockets and registry state.

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

## Optional native TCP telemetry

TCP carriers may sample the exact authenticated socket through a host adapter:

- Linux and Android use the stable `TCP_INFO` UAPI prefix. Newer kernels expose
  RTT, congestion flight/window, sender queue, ACK, loss, pacing, and delivery
  counters; a shorter returned prefix exposes only the groups it contains.
- macOS uses `TCP_CONNECTION_INFO` for RTT and congestion-window shape. Its
  socket-buffer occupancy is not reported as exact network flight.
- Windows uses `SIO_TCP_INFO` version 0 for RTT, bytes in flight, and congestion
  window. This API requires Windows 10 version 1703 or Windows Server 2016 and
  does not expose RTT variance or cumulatively acknowledged bytes in version 0.

Every native field is optional and missing fields are unknown rather than zero.
Passive native observations are scheduling evidence, not MPP Data ACKs.
Native drain-based reinjection requires both exact bytes in flight and the
unsent queue from one snapshot; otherwise it waits on exact MPP product flight.

TCP capacity transactions use receiver-confirmed receipts and may combine them
with exact-socket telemetry. QUIC publishes fresh native packet-ACK-derived
evidence with an explicit expiry and relies on its native congestion controller
for send credit; it has no separate MPP calibration transaction. These proof
states are not interchangeable. MPP Data ACK remains the carrier-neutral delivery authority;
for response bulk admission, QUIC requires locally sourced ACK-derived carrier
evidence, while durable unambiguous Data ACK progress may additionally establish
a per-flow TCP MPP rate. While fresh, that exact rate may serve only as a
demonstrated lower bound for native TCP carrier capacity. It is not multiplied
across flows, does not lower a faster native observation, and remains divided
among active flows.

The adapter is optional. Older systems, unsupported kernels, restricted hosts,
and compatibility layers that reject the socket query use the portable fallback
and remain correct and eligible. Unproven paths retain one bounded startup
flight; after durable original-data progress, shared MPP flow-control/reorder
limits and the configured resource envelope govern product work while the
socket writer supplies native backpressure. Data ACK rate remains completion
evidence, not a replacement TCP congestion window. The process prints one
explicit warning that high-bandwidth, high-latency multipath may be slower
without native TCP send credit. No config or topology may require native
telemetry for normal operation.

## QUIC UDP capability fallback

QUIC uses Quinn's native UDP adapter when the host provides its expected socket
facilities. On Windows compatibility layers that reject optional ECN or
segmentation features with an unsupported-capability error, endpoint creation
falls back to one-datagram send and receive without ECN, GSO, or GRO. The
process prints this choice once because throughput and CPU efficiency may be
lower. Quinn continues to own QUIC congestion control, packet recovery, and
timeouts. Other socket errors remain fatal rather than silently selecting the
fallback.

This path makes proxy operation possible on limited Windows environments; it
does not prove native MSVC performance, Wintun, source-address selection on a
multihomed wildcard listener, or native kernel integration. Bind a specific
address when source-address selection matters.

## Encryption

With `transport_secret_file`, TCP uses
`Noise_NNpsk0_25519_AESGCM_SHA256`; the Noise
PSK, length masks, admission binding, and record keys are domain-separated from
the endpoint secret. Public and wrong-secret probes receive no handshake
response. Freshness and a bounded process-local replay cache admit a valid
first flight before the server responds. TCP never changes into HTTP.

Without the file, TCP uses TLS 1.3 with no ALPN, followed by one bounded
exporter-bound binary admission prelude and raw MPP frames.

QUIC negotiates `h3`. The configured endpoint secret derives private Initial
keys, so a public or wrong-key Initial receives no response and cannot elicit
the certificate flight. QUIC version and packet shape remain visible. Each
encrypted request carries a credential-derived selector; the same gate
requires HTTPS, authority equal to the negotiated TLS SNI, and exactly `/`
without a query before request DATA reaches the MPP parser. Normal
`SESSION_AUTH`, `PATH_JOIN`, replay, and freshness validation still follow.
Reliable records use H3 DATA and UDP payloads use RFC 9297 datagrams. Ordinary,
nonmatching, and rejected requests receive the same marker-free 404, and a
successful response is withheld until full MPP authentication. QUIC and
TLS-based TCP authenticate the explicitly configured server certificate;
shared-secret TCP authenticates possession of the endpoint group secret and
does not transmit the certificate. The MPP application credential is
independent and never derives the TLS identity, verifier, or endpoint transport
secret. QUIC path groups require a DNS TLS identity because IP identities do
not produce SNI; carrier endpoints may still be literal IP addresses. MPP
carrier 0-RTT is disabled.

The selector removes an unauthenticated MPP-parser oracle; it does not make the
endpoint indistinguishable. Source-aware clients and observers can still
fingerprint QUIC packet shape and version, Noise ephemeral keys, timing, and
response behavior. MPTUNNEL is not a cover service. See the RFC's
[TCP presentation](../RFC.md#61-tcp-carrier-protection) and
[HTTP/3 presentation](../RFC.md#62-quic-over-http3) for the exact
admission, request, DATA-record, and native-datagram contracts.

Define named credentials globally and reference them from MPP inbounds and
outbounds. Each key must be a random UUID or at least 32 bytes of high-entropy
text loaded from a file or environment reference. Overlap old and new
credential IDs during rotation; a server may map both to the same principal.
Session and path authentication bind the credential ID and check issue time
against the configured freshness window, 300 seconds by default. Revocation
rejects new authentication immediately and retires only work admitted by that
credential after its configured grace.

Local SOCKS5 and HTTP CONNECT logins are declared once in `[[local_users]]`
with a canonical `name` and referenced by inbound `local_users = [...]`. Each
login maps explicitly to a `principal_id`, so routing and per-principal
admission do not depend on the presented username. Local and upstream proxy
passwords use the same
file/environment reference shape as MPP credentials and the management token.
Local proxy inbounds separately bound total connections, connections per
source IP, connections per principal, and their authentication/header deadline
under `[inbounds.admission]`; these limits never derive from MPP capacity.

## Installed files

Download the archive for the operating system and architecture listed in the
root [README](../README.md#release-assets), use the digest reported by GitHub when
independent verification is needed, and keep its example configurations beside
the operator's own protected configuration.

The Linux archive includes a systemd unit that runs as `mptunnel` and permits
writes below `/etc/mptunnel`. Create that directory, install `config.toml` as
the service account, and keep referenced credentials and private keys readable
only by that account. Directory write access is required when the management
API persists a replacement configuration.

The Windows archive keeps its architecture-matched `wintun.dll` beside the
executable. macOS and Android archives are command-line artifacts and require
the host integrations described in [Platform lifecycle](PLATFORM.md) for a
full device VPN.
