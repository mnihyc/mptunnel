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

SOCKS5, HTTP CONNECT, and mixed SOCKS5/HTTP CONNECT ingress need no elevated
privilege when bound to normal user ports. TUN mode needs host-approved network
privileges.

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
the egress attempt closed. On these strict callback APIs, operating-system DNS
policies are rejected before device or socket startup: OS resolver sockets are
hidden from this callback and therefore cannot safely bypass a catch-all VPN.
Configure a DNS server with a literal `address`, optionally routed through an
`outbound`, instead.
Never close or retain the borrowed descriptor; duplicate it explicitly if the
host needs separate ownership. The carrier provider must only resolve,
select/bind a native network, and create the socket; it must not invoke the
protector independently. Apple packet-tunnel embeddings use the same callback
for their equivalent native-network exclusion or binding.

The Android JNI `nativeStart` protector is nullable as one explicit alternative
host contract. Passing a non-null callback selects the strict behavior above.
Passing null is valid only when the embedding application has excluded its own
package/process from the VPN: that process-wide exclusion must already place
every MPTUNNEL carrier, direct/proxy egress, and internally created DNS socket
on the ordinary host network. This is a whole native-socket ownership choice,
not a DNS-only exception, and it does not force internal DNS direct: an
internal DNS upstream configured through an `outbound` still uses that
outbound. MPP endpoint hostnames still use the host system carrier resolver.
Android VPN DNS publication and the DNS traffic originating from VPN client
applications remain owned by the embedding application and can traverse its
local proxy independently of MPTUNNEL's internal `[dns]` policy. Never pass
null while the embedding process itself is captured by the VPN.

`run_with_packet_device_provider` and `run_with_host_providers` leave some
egress socket classes on system behavior and are not valid for a catch-all
embedded VPN. `run_with_all_host_providers` remains an advanced seam for hosts
that deliberately own separate adapters.

## Runtime supervision

```bash
mptunnel --supervise ...
```

`--supervise` restarts failed runtime generations with bounded exponential
backoff.

Use the host service manager as the outer process guard. A Windows SCM wrapper
is optional operational integration rather than a missing VPN adapter; Android
lifecycle callbacks remain owned by the embedding application.

### Process logs

```toml
[logging]
level = "info"          # off, error, warn, info, or debug
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
Flow events require at least `info`, because they are information records. `error` means
the process or a configured sink cannot continue; `warn` means the service is
still running but degraded, retrying, saturated, or recovering; `info` covers
configuration and service lifecycle transitions. `debug` includes those levels
and adds an immediate per-connection trace for routed L4 traffic.

Each debug trace uses one process-local, monotonically increasing `id`. Every
record repeats the same immutable ingress context so an isolated routing,
balancer, or outbound line still identifies the accepted request. Event-owned
fields then describe only that component's decision or outcome:

```text
2026-08-06T02:15:10.100Z DEBUG inbound.accepted: id=17 network=tcp origin="local_inbound" inbound="local-socks" principal="anonymous" source="127.0.0.1:52144" source_kind="local_peer" requested_destination="example.com:443"
2026-08-06T02:15:10.101Z DEBUG routing.selected: id=17 network=tcp origin="local_inbound" inbound="local-socks" principal="anonymous" source="127.0.0.1:52144" source_kind="local_peer" requested_destination="example.com:443" rule="default" decision="allow" egress="outbound:remote-mpp" target_resolution="full-resolve"
2026-08-06T02:15:10.102Z DEBUG outbound.connecting: id=17 network=tcp origin="local_inbound" inbound="local-socks" principal="anonymous" source="127.0.0.1:52144" source_kind="local_peer" requested_destination="example.com:443" outbound="remote-mpp" outbound_destination="198.51.100.8:443" protocol="mpp" attempt=1
2026-08-06T02:15:10.118Z DEBUG outbound.connected: id=17 network=tcp origin="local_inbound" inbound="local-socks" principal="anonymous" source="127.0.0.1:52144" source_kind="local_peer" requested_destination="example.com:443" outbound="remote-mpp" outbound_destination="198.51.100.8:443" protocol="mpp" underlay="tcp" mpp_path="primary-tcp" attempt=1
2026-08-06T02:15:10.118Z DEBUG inbound.established: id=17 network=tcp origin="local_inbound" inbound="local-socks" principal="anonymous" source="127.0.0.1:52144" source_kind="local_peer" requested_destination="example.com:443"
```

The JSON format exposes the same common and event-owned fields without parsing
the text message. For example, a server-side MPP route record may be:

```json
{"timestamp_unix_ms":1785982510101,"level":"debug","component":"routing","event":"selected","connection_id":"42","network":"tcp","origin":"mpp_inbound","inbound":"edge-in","principal":"alice","source":"203.0.113.7:51000","source_kind":"mpp_carrier_peer","requested_destination":"example.net:443","session_id":"91","ingress_underlay":"quic","ingress_path":"mobile-quic","ingress_path_id":"7","ingress_path_instance":"44","rule":"default","decision":"allow","egress":"outbound:direct","target_resolution":"as-is"}
```

The trace begins after an L4 inbound has authenticated (when applicable),
parsed a target, and accepted the logical request; it is not a raw-socket or
pre-authentication access log. Common fields are `network`, `origin`, `inbound`,
`principal`, and `requested_destination`. Local inbounds also report the local
socket peer as `source` with `source_kind="local_peer"`. An MPP inbound instead
reports the server-observed opening carrier endpoint with
`source_kind="mpp_carrier_peer"`, plus `session_id`, `ingress_underlay`,
`ingress_path_id`, and `ingress_path_instance`; `ingress_path` appears only when
that carrier maps to a named configured path. A carrier peer can be a NAT or
proxy endpoint. It is not a forwarded application-client address and never
becomes source-CIDR routing evidence.

Routing owns the exact winning `rule`, configured `egress`, and
`target_resolution`. A balancer owns each member choice. Outbound records own
the configured outbound, protocol, attempt, outcome, and
`outbound_destination`: a hostname delegated as-is or the ordered literal
candidate set passed after resolution. This remains separate from immutable
`requested_destination`; when full resolution changes a hostname into an IP,
both are visible. A connected reliable MPP outbound also reports its selected
TCP/QUIC `underlay` and configured `mpp_path`, distinct from the `ingress_*`
fields. Direct routes omit the balancer record. For UDP, one trace represents
the logical association becoming ready, not every packet; per-packet MPP path
choices are intentionally omitted because they may change. Rejected and
dropped requests stop at the routing record. MPP L3 packets, internal
DNS/probe traffic, and raw carrier authentication remain outside this
connection trace; use the management path and session views for current
carrier state.

All records are length-bounded and remove terminal control characters. Repeated
saturation and fault records are rate-limited per call site and report the
suppressed count on the next record. Authorization, cookies, tokens, passwords,
credential secrets, and private-key forms are redacted as a final defense.
Debug connection records are not rate-limited, so all attempts are visible
while `debug` remains enabled and the sink remains writable. Repeated common
context increases log volume and cardinality per connection; source/carrier
endpoints, destinations, session/path identities, and authenticated principal
IDs are privacy-sensitive. Records never add credential IDs, credentials,
secrets, payloads, or per-packet data. All dynamic fields remain bounded,
control-sanitized, and secret-redacted.

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

The configuration contains explicitly named `[[inbounds]]`, `[[outbounds]]`, and
optional `[[routing.balancers]]`. Every configured resource name is canonical
lowercase ASCII. Configured-resource references use the selected resource noun:
`inbounds`, `outbound`, `balancer`, or `dns_policy`. The `_id` suffix is
reserved for protocol, principal, or signed-artifact identities such as
`credential_id`, `principal_id`, `rule_set_id`, and `publisher_id`; `target`
is an application or active-probe destination; a listen address accepts local
traffic; and `endpoint` is a proxy connector or MPP carrier URI. A DNS server
defines the protocol, literal connection address, TLS identity, HTTP path, and
optional routed outbound. A DNS policy selects servers, address-family and
security behavior, query limits, cache behavior, named static
`override_records`, and at most one named `synthetic_capture`. Those
attachments apply only when that policy is selected.
MPP security and named carrier paths belong to each MPP inbound/outbound
rather than to a global path role.

Every TOML duration key ends in `_s` and uses seconds. Whole seconds may be
written as TOML integers and sub-second values as decimals, such as
`fallback_s = 0.05`. The one security exception is
`auth_freshness_window_s`: authentication carries a Unix-seconds timestamp, so
that window must be a positive whole number of seconds. Legacy millisecond and
long-form unit keys are rejected; there is no compatibility alias or implicit
conversion of their old values.
When converting an earlier file, use the current `_s` or `-s` name and convert
the old value to seconds. Apply the same conversion to duration options inside
inline tables and carrier URI query strings.

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
certificate. Shipped configurations set `transport_secret` to one exact
32-byte endpoint-wide secret shared by the two peers; it is optional and is not
an MPP client credential.

Every L4 inbound, including an MPP listener, enters one ordered
`[[routing.rules]]` table. The first matching rule wins. A normal rule names
exactly one `outbound` or `balancer`, which implies ordinary allow; a final
catch-all is optional because unmatched traffic rejects. Omitted `inbounds` or
`principal_ids` means any, and scalar `"*"` is the equivalent explicit form.
Concrete selector names must exist and be reachable from that rule.
Unauthenticated local proxy, fixed-forward, and TUN-L4 flows use principal
`anonymous`; authenticated proxy users and MPP peers use their configured
principal IDs.

`[routing].target_resolution` makes hostname ownership explicit on both client
and server nodes:

- `"as-is"` performs no routing DNS lookup. CIDR/post-resolution rules cannot
  match a hostname, and a domain-capable SOCKS5, HTTP CONNECT, HTTPS CONNECT,
  or MPP outbound receives the canonical hostname unchanged.
- `"route-only"` resolves only when the ordered routing table or an explicit
  `dns_policy` needs address evidence. The answer selects and authorizes the
  rule, but a domain-capable outbound still receives the original hostname.
- `"full-resolve"` resolves every hostname at this node and passes authorized
  literal addresses to the outbound.

Omitting the field retains the historical demand-driven compatibility behavior:
stable domain routes delegate the hostname, while IP-dependent routes pass the
authorized address. A route-selected `dns_policy` is an instruction under
`route-only`, `full-resolve`, and compatibility behavior. Under `as-is`, it is
not queried by routing; the outbound owns resolution instead. MPP carrier
endpoint/bootstrap DNS is separate and always occurs on the MPP client because
that node must first reach the server.

Under `route-only`, the local answer is routing evidence rather than a
connect-address pin for a domain-capable next hop: MPP/SOCKS/HTTP may resolve
the forwarded hostname differently. IP-only direct leaves reuse the retained
authorized answer and do not perform a second lookup. Use `full-resolve` when
this node must pin the outbound to its authorized answer; an MPP server always
applies its own routing and restricted-address policy to the target it receives.

When MPTUNNEL has or resolves a literal address, ordinary allow does not
authorize loopback, private, link-local, metadata, multicast, or unspecified
destinations. A domain delegated unchanged is resolved at its next hop. To
reach an internal network, put a narrow rule before the broader route, match
the required inbound, principal, CIDR, port, and network, then set
`decision = "allow-restricted"` and the intended egress. `decision = "reject"`
returns the normal flow-local refusal;
`drop` sends no proxy status or MPP frame (a QUIC request is transport-cancelled
to release its stream). In both cases sibling flows and the shared carrier stay
alive. MPP inbounds use this same table and may select only a non-MPP outbound
or a balancer with no MPP member, so an inbound MPP flow cannot chain into
another MPP outbound.

Target resolution never changes literal-address authorization: the same
restricted-address rule applies on clients and servers under all three modes.
An MPP client that intentionally forwards private literals therefore needs its
own narrow `allow-restricted` rule, and the server independently needs one for
its native egress. For a hostname under `as-is`, no client-side CIDR rule is
consulted because the client does not resolve it; the server receives the
hostname and applies its own selected resolution mode and routing policy.

When one domain returns several addresses, each address is checked against the
same ordered rules. Public addresses selected by reject/drop rules are omitted
when another allowed address remains. A private or special-use address selected
by an ordinary allow rule fails the complete answer; it is never silently
filtered into a weaker result. If nothing is allowed, drop takes precedence
over reject.

`tcp-forward`, `udp-forward`, and `mixed-forward` local inbounds require
explicit non-zero listeners and one canonical `target`; `mixed-forward` binds
both transports on every listed address. TCP overload is closed immediately at
`max_connections`; UDP silently drops new sources at `max_associations`,
expires source associations at `idle_timeout_s`, and bounds each datagram by
`datagram_ttl_s`. All three use the ordinary route/DNS/outbound/balancer
path; they do not dial around configured outbounds.

The inbound protocols determine the forwarding family. SOCKS5, HTTP CONNECT,
mixed proxy, fixed port forwarding, `tun`, and `mpp` are L4. Experimental
`tun-l3` and `mpp-l3` are L3. One document cannot mix the two families, and no
separate mode switch is required. L4 requires an explicit `[routing]` section;
L3 forbids it. Both TUN-L4 and TUN-L3 remain experimental.

`protocol = "tun-l3"` is the raw IP-tunnel ingress. It selects exactly one MPP
outbound and receives its IPv4 and/or IPv6 host address from that MPP server's
authenticated principal allocation. The server configures pools, its own TUN
addresses, and explicit principal allocations on a `protocol = "mpp-l3"`
inbound under `[inbounds.tun_l3]`;
`allowed_ips` adds externally routed prefixes owned by that principal. This
packet plane does not use L4 routing, DNS, target
outbounds, or the TUN-L4 userspace TCP/UDP stack. Carrier endpoint names still
use normal carrier resolution; inner packet destinations do not. Operators
remain responsible for host routes, IP forwarding, DNS, firewall rules, and
NAT on both ends. TCP and QUIC carrier paths are both eligible. A given MPP
outbound may be selected by only one `tun-l3` inbound.

The nested server address plan belongs only to an `mpp-l3` inbound. Each
configured family requires a pool and usable server address, and the plan
requires at least one pool and allocation. Every allocation names an active
accepted principal and supplies at least one usable, unique address in its
pool. Addresses and `allowed_ips` cannot overlap ownership across principals or
contain a server address. MTU defaults to 1500, with minimum 576 and 1280 when
IPv6 is configured. L4 TCP/UDP opens are rejected before they can enter the
server's egress services.

One authenticated principal has one live logical IP tunnel per MPP inbound. A
new session for the same principal supersedes the previous tunnel attachments;
this provides locator-independent restart and roaming rather than deriving
identity from a source address. See `examples/config.reference.toml` for the
complete commented shape.

### Migrating from 0.4.3 to 0.4.4

Version 0.4.4 intentionally makes configuration durations seconds-only. Start
from the current `examples/client.toml`, `examples/server.toml`, or exhaustive
`examples/config.reference.toml`, transfer the deployment's intended values in
seconds, and validate the rebuilt file; do not mix it with an earlier schema.

- `[flow].idle_timeout_s` defaults to 300 seconds; zero disables payload-idle
  expiry. TCP payload and accepted UDP datagrams refresh it, while transport
  control and keep-alive traffic do not.
- Omitted `[flow].initial_rate_mbps` means that carrier startup capacity is
  unknown. A positive whole-Mbit/s value supplies a global MPP path prior;
  any path-URI `initial-rate-*` form wins, including `initial-rate=unknown`.
  Finite QUIC targets remain the scheduling basis until exact post-
  authentication native evidence from two packet-timed rounds qualifies;
  omission does not enable that gate.
- Omitted `[flow].optional_reinjection_budget_percent` is 10. It meters only
  optional reliable-payload reinjection, not native recovery, control, probes,
  or the separately bounded cause-specific critical recovery authorities in
  RFC Section 15.2.
- Omitted `[flow].quic_loss_compensation_percent` is separately 10. It adjusts
  sender-local QUIC delivery/loss evidence without sending bytes or consuming
  the optional reinjection budget. Ordinary compensated loss carries a
  deterministic three-operating-round burst envelope and is classified only
  at completed packet-timed boundaries; ECN, persistence, and unknown evidence
  remain immediate.
- Omitted new-flow admission limits, local-inbound
  connection/association/source/principal limits, and the MPP
  pending-authentication limit default to 4,096. Explicit deployment limits
  remain valid.

### Migrating a 0.3 configuration

Version 0.4 is a clean configuration break; removed fields are rejected rather
than guessed. Apply these mechanical changes before validation:

- Remove root `forwarding_mode`. L4 is inferred from ordinary inbound
  protocols; an L3 server uses `protocol = "mpp-l3"` and an L3 client uses
  `tun-l3`. Do not mix L3 and L4 inbounds.
- Remove `[routing].generation`. Reload identity is runtime state, not an
  operator setting.
- Replace `action = "outbound"` or `"balancer"` with the corresponding
  `outbound` or `balancer` field alone. Rename terminal `action = "reject"` or
  `"drop"` to `decision`. A final catch-all is optional; unmatched traffic
  rejects.
- Delete both destination-ACL blocks. Move their match selectors into the one
  ordered routing table. A private/special-use exception uses
  `decision = "allow-restricted"` plus its outbound or balancer.
- Remove `outbound`, `balancer`, and `dns_policy` from an MPP inbound. Central
  routing now makes those choices for local and MPP L4 traffic alike.
- Replace global `[[dns.records]]` and `[dns.override]` with named
  `[[dns.override_records]]` and `[[dns.synthetic_capture]]` definitions, then
  attach their names to each intended DNS policy. There is no global record or
  capture fallback.
- Use `peer_diagnostics_principal_ids` on an MPP inbound for per-principal
  permission. The management-global switch remains an unconditional override.
- Remove `[service].service_mode`; it only emitted intent and never installed
  or changed a host service. Run MPTUNNEL under the platform supervisor instead.
- Update management clients from `/api/v3/` to `/api/v4/`.

Run `mptunnel --config ./config.toml --check-config` after migration. Unknown
or mixed-version fields fail with a configuration error.

Balancer strategies are `manual`, `ordered-failover`, `round-robin`, `random`,
`weighted-random`, `least-latency`, and `least-load`. Members may start
`enabled`, `draining`, or `disabled`; only enabled members receive new flows.
Optional destination or principal stickiness is bounded by both TTL and entry
capacity. Active probes require a literal IP TCP target and run under one
process-wide concurrency bound. Least-latency uses only fresh end-to-end
observations, while least-load uses active-flow leases. Neither
strategy reads MPP carrier/path metrics.

Any local L4 inbound may select a balancer. A route reachable from an MPP
inbound may select a balancer only when none of its members is MPP. Balancers
cannot nest and select one member outbound per new flow; separate MPP members
retain independent sessions and carriers rather than merging them.

Each selected local TCP or native UDP member receives its configured outbound
connect timeout, and an MPP TCP member receives its MPP stream-open timeout. A
blackholed member therefore cannot consume a successor's attempt budget, and
unrelated outbounds do not lengthen or shorten the selected balancer. MPP UDP
has no pre-commit network-open stage; its first send supplies the open outcome.
If an IP-only member needs target DNS, the flow performs one lookup under the
selected DNS policy's own timeout. Successful authorization promotes the target
to one retained address set. Failure is retained for that flow, skips later
IP-only members without another lookup, and may continue to a domain-capable
member without marking any skipped member unhealthy. The complete pre-commit
operation remains bounded by the configured member count and stage timeouts. A
failed member may be retried only before the target connection or association
commits. After commit, success/failure is passive health evidence and never
authorizes transparent replay through another member.

Use repeated explicit IPv4 and IPv6 listen, source, and DNS-server addresses.
Do not rely on OS-specific dual-stack defaults. Egress DNS strategy and connect timeout
belong to the outbound that performs resolution and connection.
`[dns].default` selects the named policy used when no DNS rule matches.

DNS server protocols are `system`, `udp`, `tcp`, `udp-tcp`, `dot`, `doh`, and
`doq`. A system server delegates to the operating system and accepts no other
connection fields. Every network server uses a usable unicast
`address = "<literal-IP>:<nonzero-port>"`; unspecified, multicast, and IPv4
broadcast addresses are rejected. This is the socket to contact, not a name
that requires another DNS lookup.
DoT and DoQ also require `tls_name`; DoH requires both `tls_name` and `path`.
The TLS name authenticates the DNS service and is also the HTTPS authority for
DoH. A DoH path is 1..=256 visible-ASCII bytes, begins with one `/` (not `//`),
and contains no `?`, `#`, or backslash. An optional `outbound` routes supported
DNS transports through a named outbound without changing the server's identity;
omission uses the built-in direct connection.

A `[[dns.policies]]` entry names its ordered `servers`, address `family`,
`security`, server-selection `strategy`, optional answer allowlist
`answer_cidrs`, and grouped `query` and `cache` limits. `ordered` advances on a
transport/server failure but treats a negative DNS response as authoritative.
`race` requires at least two servers and `fallback_s`; zero starts all servers
immediately, and the delay cannot exceed `query.timeout_s`. Decimal seconds
preserve sub-second races. `ipv4-only` and
`ipv6-only` query and return only that family. A `*-then-*` policy queries its
second family only when the preferred query returns no addresses and is not
NXDOMAIN; a `*-and-*` policy queries both and returns the preferred family
first. `query.timeout_s` is the complete deadline for each A or AAAA lookup
across that policy's servers. Exact DNS
rules win before the longest suffix rule, and an unmatched query uses
`[dns].default`. Omitting the entire `[dns]` section creates a system server and
a policy named `default` with the documented defaults.

`[[dns.override_records]]` defines named exact domain-to-address answers.
`[[dns.synthetic_capture]]` defines named bounded pools for captured A/AAAA
answers. Definitions have no effect until a named DNS policy lists their IDs in
`override_records` or selects one `synthetic_capture`. An attached override
wins before capture and applies to ordinary MPTUNNEL resolution and managed DNS
listeners. Synthetic answers are produced only for queries received on managed
`dns_listeners`; ordinary resolution never synthesizes. Every synthetic lease
records its configuration generation, policy, and capture. A recovered flow
keeps that policy when its route omits `dns_policy`; a route naming another
policy is rejected. See `examples/config.reference.toml` for every field and
protocol form, with operator-facing defaults and bounds.

External/manual TUN uses `dns_redirects` (CLI `--tun-dns-redirect`) for explicit
UDP port-53 destination sockets; the corresponding environment variable is
`MPTUNNEL_TUN_DNS_REDIRECTS`. Managed TUN instead publishes local
`dns_listeners` (CLI `--tun-dns-listener`, environment
`MPTUNNEL_TUN_DNS_LISTENERS`) and answers captured queries through the
configured DNS policy. Managed DoT shorthand uses
`--tun-dns-dot-address` plus `--tun-dns-dot-tls-name`; the first is a literal
socket and the second is the authenticated DNS name, with matching
`MPTUNNEL_TUN_DNS_DOT_ADDRESS` and `MPTUNNEL_TUN_DNS_DOT_TLS_NAME` variables.
The simple server profile uses repeatable `--outbound-dns-server`,
`--outbound-dns-protocol system|udp-tcp`, `--outbound-dns-family`, and
`--outbound-dns-timeout-s` (with the matching `MPTUNNEL_OUTBOUND_DNS_*`
variables); use TOML for the complete seven-protocol model.

External `dns_redirects` only forward UDP port 53 to the listed resolver
sockets; they do not serve local override or synthetic answers. Managed mode
requires every active DNS policy to be encrypted and non-system. Full mode also
requires at least one `dns_listeners` address; at most one managed TUN inbound
is allowed. Listener IPs must be usable, use a configured TUN address family,
and remain outside `exclude_cidrs`. In managed mode `dns_ttl_s` caps returned
DNS TTLs; in external mode it bounds redirected UDP associations.
`local_lan = true` bypasses directly connected LAN prefixes outside the tunnel.

`[session].retention_timeout_s` and
`--session-retention-timeout-s` set the absolute time an established logical
stream may remain without any authenticated carrier and the absolute ceiling
for graceful TCP carrier retirement. The default is 300 seconds. Retries and
retirement progress never extend a deadline. Healthy idle streams with a live
carrier do not consume it; TCP heartbeat and native QUIC idle timers remain
separate.

`[flow].idle_timeout_s` is the independent established Product-flow lifetime;
the default is 300 seconds and zero explicitly disables it. Positive TCP
payload or an accepted UDP datagram in either direction refreshes activity.
TCP FIN, acknowledgements, MPP/QUIC control traffic, carrier heartbeats, and
keep-alives do not. A half-closed TCP stream can therefore continue delivering
payload in its remaining direction, but it does not become immortal when that
payload stops. Payload accepted at the deadline rearms the lifetime before
retirement; a late producer cannot revive an incarnation after retirement has
committed. Cancellation still transfers cleanup to the owning actor and
releases the exact admission, attachment, and route state rather than aborting
cleanup halfway. Expiry closes only that logical flow and releases its
admission, telemetry, and datagram-route ownership; it does not mark an MPP
carrier or session failed. Direct and MPP egress use the same Product lifecycle.

`[flow]` also supplies global MPP sender defaults. Optional
`initial_rate_mbps` is a positive whole-Mbit/s startup prior inherited by every
MPP TCP and QUIC path. Omission means unknown. Any path-URI `initial-rate-*`
form overrides it; explicit `initial-rate=unknown` disables the global prior
for that path. TCP uses the resolved value only for MPP startup scheduling and
does not modify or replace the operating system's native TCP congestion
controller. The rate is local configuration, never peer evidence or a
guaranteed capacity claim. For typed Section 10.2 authority, only an exact QUIC
NativeOperational observation may replace startup; TCP retains typed startup.
Qualified TCP, peer, Product, or generic evidence may replace only the
temporary compatibility scalar read by the legacy rank.

For a finite resolved QUIC rate `R` bits/s and initial RTT `T` (the path's
`initial-srtt-s`, or 333 ms when omitted), QUIC starts with native window target
`max(IW10, ceil(R*T/8))` bytes and native pacing target `ceil(R/8)` bytes/s.
This changes startup geometry only: it does not seed BBR `bw`, `max_bw`, or MPP
operational-rate authority. `unknown` and `unlimited` retain exact Quinn BBR3
startup defaults even when an initial RTT is present. Native observations and
BBR state transitions govern the controller after construction. Overestimating
the prior authorizes a larger first burst and may cause queueing or loss;
native congestion control and recovery remain active.

MPP does not let pre-authentication setup traffic immediately replace a finite
QUIC prior. After the carrier-ready boundary, the first subsequently sent Data
packet establishes a controller-local floor. BBR's exact completed sample must
then be valid, positive, Data-space, at or beyond that floor, and
non-application-limited in two distinct packet-timed source rounds. The first
round alone cannot qualify because Quinn stamps packets with the preceding
transmit poll's application-limited state. Invalid, app-limited, zero,
pre-floor, and same-round samples are no-ops rather than resets. Once the
second round qualifies, MPP permanently uses the controller's live rate for
that activation; later idle or absence cannot restore the prior. Native BBR
continues to own bandwidth sampling, window, pacing, loss, and recovery during
the entire handoff. A fresh migrated controller obtains a fresh floor; a
same-controller clone or retained rollback preserves its handoff state.

Configuration rejects a resolved finite QUIC pair unless
`ceil(R/8) <= 2^53` bytes/s and `ceil(R*T/8) <= u64::MAX` bytes. This prevents
silent rounding in native BBR pacing state and saturation of the startup
window. The QUIC-native exactness bound does not restrict a TCP-only prior.

Omission uses 10 for both `optional_reinjection_budget_percent` and
`quic_loss_compensation_percent`. The former meters only optional reliable MPP
payload reinjection; native TCP/QUIC recovery, MPP control and probes, and the
separately bounded cause-specific critical authorities in RFC Section 15.2 are
outside that optional allowance. The latter
changes sender-local QUIC delivery/loss evidence and does not itself send or
budget bytes. Its nonzero policy includes the RFC's fixed three-round
authorized-loss burst envelope; this prevents random or correlated placement
from repeatedly tripping the population boundary while retaining a bounded
response to sustained excess loss. A matching MPP inbound/outbound
`performance` table overrides its `[flow]` value. For QUIC loss compensation
only, an explicit
`loss-compensation-percent` path URI value has highest precedence. The complete
loss-policy order is therefore path URI, node performance, `[flow]`, then the
built-in 10; optional reinjection uses node performance, `[flow]`, then the
built-in 10. Each endpoint resolves its local sending direction independently.

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
Literal-IP MPP QUIC and other datagram endpoints are address-checked. Literal-IP
TCP carrier, proxy, DoT, and DoH endpoints receive only a bounded connect
probe; application destinations are never probed. Domain endpoints never use
host/system DNS during doctor: they are reported as skipped because configured
runtime DNS and routing own resolution and connection setup. A configured
routed or source-bound literal endpoint is not dialled because a doctor probe
would bypass its selected outbound or source binding.
A ranged outbound carrier endpoint is likewise reported as `INFO` and skipped:
probing one concrete port cannot validate an externally published range, and
runtime carrier selection remains authoritative.

Every port in an outbound carrier range must forward or redirect to the same
fixed server listener. `port-rotation-interval-s` is accepted on ranged TCP and
QUIC paths, defaults to five minutes, and has a 5-second minimum. QUIC uses a
fresh protected socket while retaining the authenticated native connection;
native QUIC migration and validation remain authoritative. A successful QUIC
port rotation is therefore warm rather than a reconnect: configured hints and
the retained connection identity remain, and Quinn alone decides how its live
native state responds. A later full QUIC reconnect receives the configured
hints but never inherits the predecessor's measurements, queue, flight, ACK, or
sample authority. TCP authenticates one group-scoped transient successor,
atomically publishes its fresh wire and instance identities, and then drains
the predecessor. Existing logical work uses ordinary detachment and recovery;
native TCP state is never transferred. Configured startup hints remain
available to the logical member, while the successor's own readiness RTT warms
its local timing hint. Live predecessor rate/sample authority is not inherited.
The server's ordinary session path limit counts the overlap and is never
relaxed by a client replacement claim. Normal successful QUIC and TCP rotations
are debug events; `quic.carrier_port_migrated` records the group/path and old/new
ports, while `tcp.carrier_port_replaced` records the logical group/member plus
the old and new wire IDs, instance IDs, and selected ports. Rotation failures
remain warnings.
`max-tcp-carriers=N` applies only to TCP client paths, accepts 1..=65535, and
defaults to `3`.
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
evaluates post-resolution policy. Supply `--source` for a local inbound; omit
it for an MPP inbound, where the peer has no meaningful local source socket.
Source-constrained rules do not match when source is unavailable. Output
identifies the DNS policy and exact/default/suffix/route selector that owned
resolution, then the selected rule, decision, outbound or balancer, restricted
address authorization, initial demand, every rule's first mismatch, and the
ID/publisher/revision/expiry/hash of each consulted signed rule set. If no
configured rule matches, it reports `rule: none` and `outcome: unmatched`.

Runtime status and DNS operations use only the authenticated versioned API:

```bash
mptunnel --config ./config.toml status
mptunnel --config ./config.toml dns status
mptunnel --config ./config.toml dns explain example.com
mptunnel --config ./config.toml dns query example.com --type HTTPS
mptunnel --config ./config.toml dns flush
mptunnel --config ./config.toml dns flush --policy private
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

Carrier endpoints use `tcp://HOST:PORT[-END]` or
`quic://HOST:PORT[-END]`. Query keys are unique and every value is explicit;
unknown, duplicate, empty, or inapplicable options fail configuration load.
`backup`, `expensive`, `allow-bulk`, `control-only`, and `allow-datagrams` are
operator constraints that remain in force until configuration or management
policy changes them. Regular paths precede backup paths; a later tier is tried
only after the earlier tier is empty or its exact commits fail. Within the same
freshness and regular/backup class, non-expensive paths precede expensive
fallbacks. Neither preference is converted into an arbitrary timing penalty.
`initial-srtt-s`, `initial-rttvar-s`, and the
`initial-rate-*` forms are startup measurement priors that live evidence may
replace only under the authority distinction above: exact QUIC NativeOperational
may replace typed startup, while other qualified evidence affects only the
temporary legacy scalar. An omitted path rate inherits
`[flow].initial_rate_mbps`, then defaults to unknown; explicit
`initial-rate=unknown` suppresses inheritance.
`source-address` selects a TCP or QUIC client source IP;
`max-datagram-payload-bytes` is QUIC-client-only;
`max-tcp-carriers` is TCP-client-only; and `port-rotation-interval-s` requires
a ranged client endpoint. MPP listener paths use one fixed port. They may use
the `initial-*` and scheduling boolean options; TCP listeners may also use
`allow-datagrams`. Source binding, datagram-payload limits, TCP carrier counts,
port rotation, and port ranges are rejected on listeners. The reference
configuration defines every range, default, and complete example.

A configured path `name` is its stable endpoint-local path identity. The
server preserves that name with its bound socket or QUIC endpoint; a
peer-supplied `path_id` is opaque runtime identity and never indexes local
configuration. Client and server path lists therefore do not need matching
names or order.

Peer diagnostics retain an authenticated endpoint-local PathId mapping after
local carrier teardown until a complete successful peer snapshot proves that
the peer no longer reports it. Each request then freezes the active and
retained mappings at dispatch. Failure convergence therefore keeps its
historical path name, while later PathId reuse cannot relabel an already
requested response. A carrier authenticated after the request boundary is
resolved by the next diagnostic request.

During authenticated setup the peer advertises sequence-zero directional
`PathUsage::{Available, Backup}`. This is separate from local path health.
Ordinary scheduling attempts available paths first and advances to backup only
when that tier is empty or all exact commits there fail. Metrics rank paths
within the current tier. The receiver accepts only strictly newer later
sequences. Runtime control does not originate a post-handshake preference
change.

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

TOML accepts the management token through the common byte-material source, for
example `token = { from = "file", path = "management-token.key" }` or
`token = { from = "base64", value = "..." }`. The CLI retains its dedicated
file/environment flags. Inline values are plaintext-equivalent configuration
content; they remain absent from diagnostics and management projections.

All data and controls are authenticated under `/api/v4/`:

- `GET /api/v4/` returns the endpoint index with
  `mptunnel.management.v4`.
- `GET /api/v4/health`, `GET /api/v4/health/live`, and
  `GET /api/v4/health/ready` return `mptunnel.health.v4`. The latter two gate
  terminal generation failure and serving readiness respectively.
- `GET /api/v4/status` returns the complete cached
  `mptunnel.management.v4` snapshot, including sanitized inbound and
  outbound inventory plus a separate TUN-L3 service inventory. Credentials,
  address pools, allocation contents and identities, and native proxy connector
  endpoints are absent; configured MPP carrier endpoints are present in the
  authenticated local path inventory.
- `GET /api/v4/paths` returns configured named paths and live carrier
  instances with their lifecycle state.
- `GET /api/v4/traffic` returns monotonic forwarded totals, one-second rates,
  and five minutes of one-second trend samples.
- `GET /api/v4/sessions` returns authenticated MPP session ownership.
- `GET /api/v4/flows` returns bounded active reliable/datagram logical-flow
  detail, including the origin inbound, application target, selected outbound,
  optional balancer, and a required typed source observation. Unscoped
  DNS/probe/test transport work is excluded from active inbound rows rather
  than projected with an invented or unknown source. `local_peer` is
  the accepted socket peer for socket inbounds (or the logical packet source
  endpoint for TUN-L4). `mpp_carrier_peer` is the server-observed authenticated
  carrier that opened the logical flow; it may be a NAT endpoint, is not a
  forwarded end-client identity, and never becomes source-CIDR routing
  evidence. The value is snapshotted at logical-flow open, so later carrier
  migration, reattachment, or retirement does not relabel an active row.
- `GET /api/v4/diagnostics` returns local diagnostic capability, peer session
  references, controls, and path state.
- `GET /api/v4/config` returns `mptunnel.config.v4` with the canonical path,
  desired, active, runtime, and pending revisions, mutation endpoints, and
  required precondition. It never returns TOML or resolved secrets.
- `GET /api/v4/balancers` returns `mptunnel.balancer.v4` with named balancer
  and outbound-member readiness, freshness, load, observations, probes,
  circuit state, and counters.
- `GET /api/v4/dns/status` returns `mptunnel.dns.status.v4` with DNS
  generation, policy, cache, in-flight query, server, and override state.
- `GET /api/v4/dns/explain?domain=<domain>` returns
  `mptunnel.dns.explain.v4` without issuing a query.
- `POST /api/v4/actions/path` accepts exactly
  `{ "outbound": "...", "path": "...", "state": "..." }`; `state` is
  `enabled`, `suspect`, `failed`, or `disabled`.
- `POST /api/v4/diagnostics/peer` accepts exactly
  `{ "service": "mpp_outbound", "service_name": "...", "session_id": "..." }`
  or the corresponding `mpp_inbound` service.
- `POST /api/v4/config/validate` accepts one bounded UTF-8
  `application/toml` document, validates it and its referenced material, and
  returns its revision without writing or reloading.
- `POST /api/v4/config/apply` accepts the same complete document and exactly
  one `If-Match: sha256:...` revision from `GET /api/v4/config`. It persists
  only when the desired revision still matches.
- `POST /api/v4/balancers/actions` accepts exactly `balancer`, `action`, and,
  except for `automatic`, `outbound`. Actions are `enable-member`,
  `drain-member`, `disable-member`, `pin-member`, and `automatic`; responses
  use `mptunnel.balancer.v4`.
- `POST /api/v4/dns/query` accepts exactly
  `{ "domain": "...", "type": "..." }` and returns
  `mptunnel.dns.query.v4`.
- `POST /api/v4/dns/cache/flush` accepts `{}` or
  `{ "policy": "..." }` and returns `mptunnel.dns.flush.v4`.

Every `service_index` in a response is presentation-only. Mutations select
stable configured names (`outbound`, `path`, `balancer`, and `service_name`)
plus the protocol `session_id`; they never accept an index.

Configuration mutation is deliberately full-document only: there is no
`PATCH`, field update, history, or diff API. Process logging and changes to an
inbound's accepted credentials may publish live when they are the complete
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

Configuration replacement is crash-recoverable. A newly persisted document remains pending
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
configuration activation, or a queried DNS policy that has never produced a
successful server result. Listener and DNS/session/balancer facts used for the
decision are included in the response; inbound-only servers remain ready while
waiting for clients.

Tokens must contain 16-256 visible ASCII characters. The server rejects
duplicate authorization/content-type headers, transfer encoding, ambiguous
content lengths, pipelining, and non-origin request targets.

Shared forwarding counters provide monotonic logical totals. `to_peer` counts
bytes or datagrams accepted from
the local source; `from_peer` counts bytes or datagrams delivered to the local
destination. They do not grow from
carrier retransmission, MPP reinjection, multipath copies, DNS connector work,
or path probes. Native and MPP boundaries never count one flow twice. Path
delivery, pacing, queue, and flight remain separate from those forwarding
counters. Their management fields are best-effort diagnostic observations:
where the local runtime carries an availability signal, an unobserved value is
JSON `null` and renders as `-`; peer wire fields without such a signal remain
advisory observations and are not reclassified from numeric zero. Native carrier,
Product goodput, MPP feedback, configured-prior, and scheduler-default values
are labeled by source and rate scope. On client-local rows, a native pacing
value is shown only when the carrier actually supplied one;
scheduler-normalized delivery is not called native pacing. Measured delivery
and pacing values remain visible after their shared three-PTO freshness window,
prefixed with `~`; effective sample age includes time the management snapshot
has resided in the browser. RTT, loss, queue, flight, and other instantaneous
snapshot fields use API-result residence instead of the age of the most recent
delivery sample. Evidence sample counts and bytes belong to that same sender
direction and rate epoch; they are not bidirectional forwarding totals. A
download therefore contributes server-to-client sender evidence, not the
client's client-to-server path record. Path usage direction and metric
direction are displayed independently. For a port-hopping client path, the
dashboard shows only the remote port observed on the current live carrier and
never substitutes a configured range endpoint. A peer diagnostic may retain
the exact last port of a retired authenticated carrier for correlation, but
marks that historical port with `~`. Numeric identifiers and
monotonic byte totals are decimal strings so browser clients do not lose 64-bit
precision.
The dashboard renders Session IDs as lowercase, fixed-width 16-digit
hexadecimal for compact visual correlation, while matching, control payloads,
and the management API retain the original decimal strings.
Per-flow detail is capped independently from forwarding capacity; aggregate
counters remain exact. Diagnostics report both current and cumulative detail
overflow, and per-session flow counts carry an explicit completeness flag.

Peer diagnostics have two endpoint controls. On an MPP outbound,
`allow_peer_diagnostics = true` permits its authenticated peer. On an MPP
inbound, `peer_diagnostics_principal_ids = ["..."]` permits only the listed
authenticated principals, while the scalar `"*"` permits all of them; omission
denies. The management-global `allow_peer_diagnostics = true` (or
`--management-allow-peer-diagnostics`) unconditionally permits every
authenticated peer on every MPP endpoint regardless of those endpoint
controls. Permissions are independent of the local HTTP listener and default
to deny. Either endpoint may initiate from its own management API; the remote
endpoint's effective policy decides whether it returns data. Wire responses
contain only per-session path state, usage, and metrics. They exclude endpoints,
application targets, local resource names, credentials, and every other
authenticated session. The requester separately correlates each returned
`(session_id, underlay, path_id)` with the endpoint-local configured path that
admitted that authenticated carrier. The management response and dashboard may
therefore show the local path name and configured carrier endpoint, including a
draining assignment captured while that carrier registration was live; those
fields were not disclosed by the peer. Correlation is snapshotted when the
request is dispatched, so later authenticated reuse of the same numeric Path ID
cannot relabel the in-flight request or its cached diagnostics. Carrier
retirement removes only its exact live assignment and cannot erase a newer
owner. An unavailable local correlation is shown as unknown and is never
guessed from configuration order.
Peer metrics are labeled advisory, and their effective age adds residence in
the requester's cached result to the wire-reported metric age; stale values
remain visible with the same `~` convention. The peer's usage direction is
presented separately from the direction of the reported sender metrics. One
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

The chart-history selector bounds retained browser history; it is not a zoom
control. Hovering with a pointer or tapping selects the nearest actual retained
sample in both charts, and focused charts accept Left/Right/Home/End selection.
The detail below each chart reports the sample's localized date, time, time
zone, and raw series values; cumulative byte totals remain lossless decimal
strings. Selection never interpolates telemetry or changes polling, retention,
or the management API. Compact axis endpoints remain time-only, so use the
date-bearing detail when retained history crosses a calendar day.

Path control uses `enabled`, `suspect`, `failed`, or `disabled`. Enabling clears
the operator disable but leaves a path suspect until fresh carrier liveness
evidence restores it; management never manufactures an active observation.

Balancer actions have the same evidence rule. Enabling a member permits new
selection but does not invent a successful probe. `drain-member` immediately
stops new selection while established flows finish on their existing outbound.
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
| `tcp_path_heartbeat_interval_s` | 10 s |
| `tcp_path_heartbeat_timeout_s` | 30 s |
| `quic_path_keep_alive_interval_s` | 10 s |
| `quic_path_idle_timeout_s` | 30 s |

Each heartbeat/keep-alive interval is a maximum idle delay. The client renews
the next idle probe within 80%--100% of that interval; authenticated activity
defers it, while an outstanding response deadline is never extended. The
server does not originate a second QUIC keep-alive schedule.

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

Each proxy outbound has its own `connect_timeout_s`; the default is 10 seconds.
Its required `endpoint` is `HOST:PORT`, with brackets around IPv6. When
`[outbounds.auth]` is present, both username and password are required; the
username is 1..=255 UTF-8 bytes with no colon or ASCII control characters.
For HTTPS CONNECT, `tls_server_name` defaults to the endpoint host and optional
`tls_ca_certificate` material adds private roots without disabling hostname
verification.

`[admission]` is the independent new-flow envelope used before DNS, target
connects, or other flow-opening I/O. Defaults are finite:

| Field | Default |
| --- | ---: |
| `max_live_flows` | 4,096 |
| `max_concurrent_work` | 4,096 |
| `max_live_flows_per_principal` | 4,096 |
| `max_live_flows_per_outbound` | 4,096 |
| `max_connects_per_outbound` | 4,096 |
| `max_live_flows_per_target` | 4,096 |
| `max_connects_per_target` | 4,096 |
| `max_dns_work` | 4,096 |

SOCKS5, HTTP CONNECT, mixed proxy, fixed forwarding, TUN-L4, and authenticated
MPP server opens share one L4 admission budget. Their listener/source/association
limits still compose at their narrower boundary. Permits release exactly on
close, error, cancellation, or generation retirement and never enter payload
forwarding. TUN-L3 packet forwarding has its own bounded packet queues and does
not consume L4 flow admission. These fields do not derive from
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
While the exact original carrier is live, a persistent authoritative Data ACK
gap's full target service window remains within that allowance. At or after the
original-owner boundary, either a gap or a contiguous tail may exceed remaining
credit by only one exact frontier quantum. A target-bound ranked quantum covers
only the maximal lowest prefix with one live exact owner and an unchanged exact
copy-avoidance set; cache chunking alone does not change its rank. A pre-existing
target-unbound tail retains a bounded unassigned prefix and is revalidated
against the exact native target at dispatch. They share one
non-accumulating over-credit token per stream send direction: acceptance at or
after the owner boundary while that token is available consumes it, and target churn,
queue expiry, or evidence transitions do not mint another attempt. Cumulative
optional credit remains spendable before the owner boundary and while the token
is closed, and does not renew it. Exact terminal path failure retains separate bounded
critical authority. Exact retained ranges, queue and flight limits,
overlap/repeat suppression, and alternate-output requirements still apply.
Every accepted byte remains charged, reducing later optional allowance. A
continuous over-budget stream is therefore a defect, not expected failover
overhead.

The current timers are cause-specific. Exact path-instance failure permits an
immediate bounded copy, preferring measured survivors but using any eligible
live survivor when necessary. Complete Data ACKs establish missing ranges;
positive partial ACK ranges may extend established state but cannot infer an
omission. Fragmented request feedback waits until one original-carrier RTO/PTO
from the exact OriginalData assignment epoch. Response feedback may use a
later-ACK TCP 5/4-SRTT or QUIC 9/8-SRTT time threshold; ACK silence waits that
carrier's RTO/PTO. A live-owner gap/tail batch accepted at or after the owner
boundary while the over-credit token is available fixes the next frontier-floor
eligibility one full recovery interval later. Optional-funded work remains
cause-eligible before that boundary and while the token is closed, and cannot
move its deadline. Newly
acknowledged contiguous Data-ACK frontier progress, not sparse suffix ACKs,
polling, or target changes, restarts that interval. A request path becomes
stale for new placement after four TCP RTOs
or three QUIC PTOs without exact Data ACK progress when another attachment
exists; this does not terminate native recovery.

Response finite-tail repair that was ranked from one target's measured
capacity remains bound to that exact output incarnation through Product and
native dispatch. If that incarnation disappears or the decision expires, the
queued intent is removed for fresh evaluation; it is never silently moved to a
different carrier.

MPP datagram feedback confirms that the server accepted a datagram for target
forwarding, not end-to-end delivery. Before feedback, the runtime makes at most
two carrier delivery attempts. Both retain the same session, flow, and datagram
identity; the shared server
flow forwards that identity to the target at most once and replays a bounded
cached response to the retry carrier. A ranked alternative is tried after one
calculated response timeout; the final or only attempt keeps three such timeouts,
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
reattached stream receives a fresh binding, so it also cannot inherit an old
scheduling decision merely because the carrier stayed live.

When every carrier disappears, an established logical stream retains its MPP
sequence, Data ACK, receive-window, FIN, and bounded repair/reorder state while
the client rotates reconnect attempts across configured TCP and QUIC paths.
Both endpoints stop reading their local application socket so ordinary TCP
backpressure bounds memory. Reattachment within the session-retention deadline
continues the same stream; expiry closes both local sockets and registry state.

Immediately before queueing data, the runtime rechecks that the selected live
carrier attachment and output are still valid. A reconnect or reattachment
cannot inherit an old scheduling decision, queue reservation, or in-flight
accounting merely because a numeric path identifier was reused.

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
Passive native TCP observations are diagnostics, not MPP Data ACKs or Core
scheduling-rate authority.
Native drain-based reinjection requires both exact bytes in flight and the
unsent queue from one snapshot; otherwise it waits on exact MPP application-data
flight.

TCP capacity receipts and exact-socket telemetry remain separate diagnostics;
neither supplies typed Section 10.2 authority. Qualified observations may
temporarily feed the historical scalar read by the legacy rank, without
granting admission, pacing, Product, or native-controller authority. QUIC publishes the named
controller-local `QuinnBbr3NativeOperationalV1` rate and relies on its native
congestion controller for send credit; it has no separate MPP calibration
transaction. These sources are not interchangeable. MPP Data ACK remains the
authoritative carrier-neutral Product-delivery signal and its per-flow rate is
completion evidence, not physical-carrier capacity.

The adapter is optional. Older systems, unsupported kernels, restricted hosts,
and compatibility layers that reject the socket query use the portable fallback
and remain correct and eligible. Unproven paths retain one bounded startup
flight; after durable original-data progress, shared MPP flow-control/reorder
limits and the configured resource envelope govern application data while the
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

With `transport_secret`, TCP uses
`Noise_NNpsk0_25519_AESGCM_SHA256`; the Noise
PSK, length masks, admission binding, and record keys are domain-separated from
the endpoint secret. Public and wrong-secret probes receive no handshake
response. Freshness and a bounded process-local replay cache admit a valid
first flight before the server responds. TCP never changes into HTTP.

Without the secret, TCP uses TLS 1.3 with no ALPN, followed by one bounded
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

A peer reset or stop of one HTTP/3 request stream with application code zero is
normal operation-local abandonment. It closes that request operation without a
carrier-failure warning; the QUIC connection and sibling request streams remain
authoritative. Nonzero application errors, malformed or truncated records, and
terminal carrier-control loss keep their ordinary smallest-safe-scope failure
handling.

The selector removes an unauthenticated MPP-parser oracle; it does not make the
endpoint indistinguishable. Source-aware clients and observers can still
fingerprint QUIC packet shape and version, Noise ephemeral keys, timing, and
response behavior. MPTUNNEL is not a cover service. See the RFC's
[TCP presentation](../RFC.md#61-tcp-carrier-protection) and
[HTTP/3 presentation](../RFC.md#62-quic-over-http3) for the exact
admission, request, DATA-record, and native-datagram contracts.

Define named credentials globally and reference them from MPP inbounds and
outbounds. Each key must be an exact textual UUID or at least 32 bytes of
high-entropy material loaded from a configured source. Material sources do not
trim whitespace or line endings. Relative material paths, including a relative
path read from an environment variable, resolve beside the selected TOML.
Overlap old and new
credential IDs during rotation; a server may map both to the same principal.
Session and path authentication bind the credential ID and check issue time
against the configured freshness window, 300 seconds by default. Revocation
rejects new authentication immediately and retires only work admitted by that
credential after its configured grace.

Local SOCKS5 and HTTP CONNECT logins are declared once in `[[local_users]]`
with a canonical `name` and referenced by inbound `local_users = [...]`. Each
login maps explicitly to a `principal_id`, so routing and per-principal
admission do not depend on the presented username. Local and outbound proxy
passwords use the same material-source shape as MPP credentials and the
management token.
An inbound with `protocol = "mixed"` accepts SOCKS5 and HTTP CONNECT on one TCP
listener while retaining the same local-user and admission policy.
Local proxy inbounds separately bound total connections, connections per
source IP, connections per principal, and their authentication/header deadline
under `[inbounds.admission]`; the defaults are 4,096 for every count and 10
seconds for `handshake_timeout_s`. These limits never derive from MPP capacity.

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
