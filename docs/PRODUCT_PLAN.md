# MPTUNNEL v0.1.2 Product capability map

Status: release capability map with evidence-scoped platform claims

Scope: Product and user-facing integration only

Core authority: [RFC.md](../RFC.md)

Companion: [Performance/Core capability map](PERFORMANCE_PLAN.md)

Configuration authority: the strict parser and
[configuration reference](../examples/config.reference.toml)

This document records what the v0.1.2 source actually implements. A capability
is not treated as shipped on a platform until its package and native evidence
exist. Future ideas are listed only as explicit exclusions or evidence gaps.

## 1. Product/Core boundary

| Product owns | Core owns |
| --- | --- |
| SOCKS5, HTTP CONNECT, fixed forwarding, and TUN ingress | MPP v4 frames and identities |
| Canonical flow context and principals | Logical sessions, streams, datagrams, and path instances |
| Routing, destination ACL, DNS, and outbound selection | Directional sequence numbers, Data ACKs, flow control, and reassembly |
| Product balancer selection between independent outbounds | Carrier selection inside one selected MPP session |
| Credentials, admission policy, configuration, API, and dashboard | TCP/QUIC carrier lifecycle, scheduling, reinjection, and failover |
| Host VPN transactions and release packaging | RFC-defined timing, evidence, congestion, and recovery boundaries |

The separation is structural:

- A Product balancer selects one outbound for a new flow. It never merges paths
  from separate MPP sessions.
- The Core scheduler selects TCP or QUIC carriers only inside that chosen MPP
  session.
- Product may supply only the abstract `interactive`, `throughput`, `realtime`,
  or `background` traffic intent. It cannot supply path scores, congestion
  state, reinjection thresholds, or benchmark-derived timing.
- Core cannot import route rules, DNS plans, users, TOML, UI, or host VPN
  policy.
- Balancer retry is allowed only before flow commit. An established flow is
  never replayed through an unrelated server.
- Product actions and observability use bounded control/snapshot ports and do
  not enter per-byte or per-packet Core hot paths.
- RFC parameters and timing are not Product tuning targets. Their presence in
  configuration does not authorize changing the model outside the RFC and
  Performance track.

## 2. Supported entry surfaces

| Entry surface | Persistence | Actual behavior |
| --- | --- | --- |
| `mptunnel` with no arguments | Persistent | Loads `./config.toml` on each process start. |
| `mptunnel --config FILE` | Persistent | Loads one strict canonical TOML graph. |
| `mptunnel --config FILE --check-config` | None | Parses, resolves referenced material, validates the graph and host contract, then exits without listeners. |
| `mptunnel ... client` | Ephemeral | Builds one simple local-ingress plus MPP-outbound profile. It can expose SOCKS5, HTTP CONNECT, one TCP forward, one UDP forward, and optional TUN. |
| `mptunnel ... server` | Ephemeral | Builds one MPP inbound plus one native outbound selected from `direct`, `bind`, `socks5`, `http-connect`, or `https-connect`. |
| `MPTUNNEL_*` environment variables | Ephemeral | Mirrors the simple CLI fields. Secret values are accepted only through referenced files, named environment variables, or credential stdin where explicitly supported. |
| Authenticated config API | Persistent | Validates or atomically replaces the complete TOML document with revision compare-and-swap. It is not a field-patch API. |
| Runtime action API | Generation-local | Controls path lifecycle, balancer member mode/pin, DNS cache/query, and peer diagnostics. These actions persist only when also represented in TOML through config apply. |

Simple CLI commands do not write `config.toml`. Advanced routing, split DNS,
multiple independent MPP servers, balancers, named users, signed rule sets, and
combined client/server nodes require TOML.

The only operational commands are:

- `mptunnel platform`
- `mptunnel --config FILE status`
- `mptunnel --config FILE doctor`
- `mptunnel --config FILE route explain ...`
- `mptunnel --config FILE dns status`
- `mptunnel --config FILE dns explain DOMAIN`
- `mptunnel --config FILE dns query DOMAIN --type TYPE`
- `mptunnel --config FILE dns flush [--dns-plan PLAN]`

There is no `init`, `config set`, profile, subscription, connect/disconnect,
install, update, or rollback command.

## 3. Canonical TOML map

Unknown keys are rejected. Referenced files are resolved relative to the
selected TOML document.

Every configured inbound, outbound, balancer, route rule, ACL rule, DNS
upstream, DNS plan, and DNS rule has an explicit canonical `name`.
Configured-resource references use `inbounds`, `outbound`, `balancer`, or
`dns_plan`; `_id` is reserved for protocol, principal, and signed-artifact
identities such as `credential_id`, `principal_id`, `rule_set_id`, and
`publisher_id`. `target` denotes an application or active-probe destination
authority; `endpoint` denotes a listener, connector, or carrier network
endpoint.

| Path | Implemented responsibility |
| --- | --- |
| Root `check_config`, `[logging]` | Check-only startup plus typed level, format, console, file, and opt-in Product flow lifecycle records. Runtime config apply rejects `check_config = true`. |
| `[[credentials]]` | MPP `credential_id`, `principal_id`, file/environment secret, optional expiry, revocation, and revocation grace. |
| `[[local_users]]` | Canonically named SOCKS5/HTTP CONNECT username/password mapped to a stable Product `principal_id`. |
| `[service]` | Service intent and optional in-process generation supervision/backoff. It does not install a native service. |
| `[session]` | Core-owned authenticated-carrier outage retention envelope. |
| `[management]` | Loopback listeners, referenced bearer token, dashboard switch, and peer-diagnostic permission. |
| `[resources]` | Core capacity and liveness envelopes documented by the reference config. These are not Product traffic profiles. |
| `[admission]` | Product-wide live-flow, concurrent-work, principal, outbound, target, connect, and DNS-work bounds. |
| `[dns]` | One immutable DNS generation with explicit `default_dns_plan` and no implicit fallback. |
| `[[dns.upstreams]]` | `system`, UDP, TCP, UDP+TCP, DoT, DoH, or DoQ upstream; literal bootstrap; TLS name/path; optional named outbound egress. |
| `[[dns.plans]]` | Upstream set, IP strategy, encryption requirement, ordered/race behavior, expected CIDRs, deadline, cache, answer, stale, and prefetch bounds. |
| `[[dns.rules]]` | Exact or longest-suffix plan selection. |
| `[[dns.hosts]]` | Exact immutable host overrides. |
| `[dns.fake_dns]` | Bounded reserved IPv4/IPv6 pools and recovery lifetimes for captured A/AAAA traffic. |
| `[[inbounds]]` | `socks5`, `http-connect`, `tcp-forward`, `udp-forward`, `tun`, or `mpp`. |
| `[[outbounds]]` | `mpp`, `direct`, `socks5`, `http-connect`, or `https-connect`. |
| `[routing]` | Immutable generation containing routes, destination ACL, balancers, and signed rule-set references. |
| `[[routing.rules]]` | Ordered first-match Product routing. |
| `[[routing.balancers]]` | New-flow outbound selection. |
| `[[routing.rule_set_publishers]]`, `[[routing.rule_sets]]` | Pinned Ed25519 publisher and signed domain/CIDR artifact loading. |
| `[routing.destination_acl]` | Local-ingress destination authorization. |
| `[inbounds.destination_acl]` | Destination authorization scoped to one MPP server inbound. |
| `[inbounds.security]`, `[outbounds.security]` | MPP credentials, TLS identity, freshness, and bounded server authentication. |
| `[inbounds.performance]`, `[outbounds.performance]` | Core-owned MPP repair budget input; behavior remains governed by the RFC. |

## 4. Implemented Product capabilities

### 4.1 Inbounds

| Inbound | Implemented behavior | Boundary |
| --- | --- | --- |
| SOCKS5 | TCP CONNECT, UDP ASSOCIATE, domain/IPv4/IPv6 targets, optional username/password authentication, multiple explicit listeners, and bounded admission. | SOCKS5 BIND is not implemented. |
| HTTP CONNECT | TCP CONNECT with domain/IPv4/IPv6 authority and optional Basic proxy authentication. | It is not a general HTTP forward proxy and does not carry UDP. |
| TCP forward | One fixed target per inbound, multiple listeners, bounded concurrent accepts, and the normal route/DNS/ACL/outbound/balancer pipeline. | No connector bypass. |
| UDP forward | One fixed target, source-keyed bounded associations, idle expiry, datagram TTL, and generation-safe response mapping. | New associations are dropped at capacity; no transparent replay after commit. |
| TUN | IPv4 and/or IPv6 L4 TCP/UDP, optional local-netstack ICMP handling, configurable MTU, external/manual mode, managed full/split policy, DNS capture, and FakeDNS recovery. | It is not an arbitrary L2 bridge or arbitrary-IP-protocol tunnel. |
| MPP server | TCP and/or QUIC listeners, named credential authority, TLS identity, per-inbound ACL and DNS plan, and one native outbound or native balancer. | An MPP inbound cannot select an MPP outbound or both an outbound and balancer. |

SOCKS5, HTTP CONNECT, fixed forwards, TUN flows, and authenticated MPP opens
share the Product admission owner. Listener/source/association bounds remain
additional narrower limits.

### 4.2 Routing and destination authorization

Routing is immutable, strict, ordered, and first-match. Supported match
categories are:

- exact domain, suffix, keyword, and regex;
- signed domain rule set;
- destination CIDR and signed destination rule set;
- source CIDR;
- destination and source port or port range;
- TCP or UDP;
- named inbound;
- authenticated principal ID; and
- pre-resolution or post-resolution stage.

Nonempty categories are ANDed; values inside a category are ORed. Local
ingress configuration requires a final catch-all rule.

Supported route actions are exactly:

- `action = "outbound"` with a named outbound;
- `action = "balancer"` with a named Product balancer;
- `action = "reject"`; and
- `action = "drop"`.

`reject` gives a protocol-native refusal where one exists: SOCKS5 returns
`connection not allowed` and HTTP CONNECT returns `403`; raw TCP forward and
TUN TCP close promptly, while UDP is discarded because those inbounds have no
portable application-level refusal. `drop` emits no application response:
accepted TCP is read and discarded for at most ten seconds (or until the peer
closes), then released; UDP is silently discarded. The bounded hold keeps
`drop` distinct without permitting denied peers to retain listener or TUN flow
capacity indefinitely. MPTUNNEL does not expose a misleading generic `reset`
action because an abortive TCP reset is not uniformly available through local
proxy, userspace TUN, and UDP ingress APIs.

Direct access is represented by selecting a `protocol = "direct"` outbound;
there is no `direct` route action.

A rule may also select a DNS plan, traffic intent, and bounded explanation
text. DNS plans are pre-resolution decisions; a post-resolution-only rule that
sets one is rejected. `route explain` evaluates the same compiled
pre/post-resolution policy and separately reports the pre-resolution rule and
DNS plan that own resolution, then the selected stage rule, action,
outbound/balancer, intent, mismatch trace, and verified signed-rule-set
identity.

Destination ACLs run before and after resolution. Their effects are `deny`,
`allow`, and explicit `allow-restricted`. Without an explicit restricted
override, metadata, unspecified, loopback, private, link-local, and multicast
addresses are denied. Every returned DNS address is authorized; one
disallowed answer fails the resolution instead of silently filtering into a
different result.

Address checks apply to literal targets and whenever this MPTUNNEL node
obtains IP evidence. If a domain is delegated unchanged to SOCKS5, HTTP(S)
CONNECT, or MPP, only domain-level policy can be enforced here; the configured
upstream is explicitly trusted to resolve and connect it. Operators who need
restricted-address or IP-allowlist enforcement before a proxy add an
applicable destination-IP rule, destination rule set, or post-resolution ACL
rule. That requires evidence through the selected DNS plan—which may itself
use routed remote transport—and sends only the authorized literal target.

Signed rule sets pin publisher, signed ID, minimum revision, expiry, checksum,
and Ed25519 signature. Invalid, expired, oversized, rolled-back, or
incorrectly signed artifacts reject the complete candidate generation.

### 4.3 DNS

The DNS runtime implements:

- one immutable resolver generation;
- exact, longest-suffix, then default plan selection;
- system, UDP, TCP, UDP+TCP, DoT, DoH, and DoQ upstreams;
- literal bootstrap addresses and authenticated TLS names;
- direct or explicitly named outbound DNS egress;
- six IPv4/IPv6 ordering/filter strategies;
- ordered upstreams or bounded delayed racing;
- plaintext-allowed or encrypted-required plans;
- expected-CIDR answer validation;
- positive and negative caches;
- cache capacity, query coalescing, inflight limits, TTL caps, stale-if-error,
  and bounded prefetch;
- exact host overrides;
- explicit cache flush and typed query operations;
- TCP and UDP DNS capture for TUN;
- bounded FakeDNS A/AAAA allocation and TUN domain recovery; and
- status and explanation without issuing a query.

There is no fallback outside a compiled DNS plan. Omitting `[dns]` in a simple
proxy/server profile synthesizes the named `system` upstream and `default`
plan; once `[dns]` is present, every upstream and fallback is explicit.
Managed full-VPN mode rejects system or plaintext resolution before host
routes are published. Stream proxy and MPP DNS egress support TCP/DoT/DoH.
DoQ is available only through direct or source-bound native egress.

Target resolution is demand-driven and separate from resolver transport. The
immutable original domain remains the routing identity. An earlier applicable
destination-IP route or explicit destination ACL rule requests IP routing
evidence through the selected DNS plan; a stable domain route does not.
SOCKS5, HTTP(S) CONNECT, and MPP carry the canonical domain when no IP evidence
is required, so the selected upstream egress becomes the resolution authority.
Direct and source-bound leaves require an IP target and query the selected DNS
plan, whose upstream may itself be system, direct, or routed remotely.

Balancer retries retain one target representation. A domain-capable member is
tried without a target query; reaching an IP-required member resolves and
authorizes once. Later attempts receive those same authorized literal
addresses and never revert to the domain. Policy-required resolution is
fail-closed and every returned address must pass destination authorization.
Proxy-control and carrier-bootstrap resolution are independent of application
target resolution.

### 4.4 Outbounds

| Outbound | TCP targets | UDP targets | Configuration |
| --- | --- | --- | --- |
| MPP | Yes | Yes | Multiple TCP/QUIC carrier endpoints under one logical session, one credential, exact TLS leaf pin, path policy, and Core repair envelope. |
| Direct | Yes | Yes | OS-selected source or optional `bind_ip`, DNS plan, and connect timeout. |
| SOCKS5 | Yes | Yes when the upstream supports UDP ASSOCIATE | Proxy endpoint and optional referenced username/password. |
| HTTP CONNECT | Yes | No | Proxy endpoint, optional referenced authentication, and timeout. |
| HTTPS CONNECT | Yes | No | HTTP CONNECT over WebPKI-authenticated TLS, optional additional CA file, optional authentication, and timeout. |

Every target receives a pre-resolution policy decision before opening.
Address authorization additionally applies whenever this node has IP evidence.
IP-required leaves use the selected DNS plan; domain-capable proxy leaves
preserve the canonical domain unless routing or ACL policy required address
evidence. Proxy and carrier sockets cross the host socket-protection or binding
boundary before I/O in catch-all VPN hosts.

### 4.5 Product balancers

A balancer contains leaf outbounds only and cannot nest another balancer.
Implemented strategies are:

- `manual`
- `ordered-failover`
- `round-robin`
- `random`
- `weighted-random`
- `least-latency`
- `least-load`

Implemented policy includes:

- enabled, draining, and disabled member modes;
- an optional initial/manual pin;
- destination or principal stickiness with TTL and capacity;
- Product-owned active TCP probes to a literal IP authority;
- passive open and completed-flow outcomes;
- freshness, failure/recovery hysteresis, cooldown, and bounded backoff;
- one absolute open deadline shared by pre-commit member retries; and
- per-member load, latency, health, error, selection, and probe counters.

Least-latency and least-load never inspect MPP path metrics. Draining stops
new selection while established flows remain on their existing leaf.

### 4.6 Identity, admission, and carrier presentation

MPP identity and admission implement:

- named credentials mapped to stable principals;
- overlapping server credentials for rotation;
- expiry and revocation;
- bounded revocation grace for authenticated carrier retirement;
- one outbound credential and a set of accepted inbound credentials;
- fresh nonce/timestamp authentication and replay bounds;
- independent TLS server identity and MPP application credential;
- bounded pending server authentication; and
- atomic live publication when an API candidate changes only inbound
  credential authorities.

Carrier presentation is implemented as:

- TCP: TLS 1.3 only, no ALPN, no 0-RTT, then an encrypted fixed-size
  exporter-bound binary admission prelude and raw bounded MPP records.
  Unauthenticated input is closed without application response bytes.
- QUIC: TLS 1.3 with standard `h3`, no 0-RTT, one full-duplex `POST /` per
  logical carrier stream, reliable records in HTTP/3 DATA, and native
  datagrams through RFC 9297 HTTP Datagrams. The request gate requires HTTPS,
  exact authority equality with the negotiated TLS SNI, and exactly `/`
  without a query. QUIC path groups therefore require a DNS TLS identity,
  while the carrier endpoint may remain a literal IP. An encrypted
  credential-derived selector gates each request before the MPP parser; full
  `SESSION_AUTH` and `PATH_JOIN` still follow. Nonmatching and rejected
  requests receive the same marker-free 404.

No private ALPN or cleartext MPP marker is exposed. SNI, certificate, TLS and
QUIC implementation behavior, standard H3 negotiation and settings, transport
parameters, packet shape, timing, and endpoint behavior remain observable or
probeable. Encrypted request fields and selector gating do not make a
standalone authenticated tunnel a cover service.

### 4.7 Configuration and lifecycle

The canonical store implements:

- strict full-document validation;
- SHA-256 revision IDs;
- `If-Match` compare-and-swap;
- external-edit conflict detection;
- durable atomic replacement;
- desired, active, runtime, and pending revision state;
- last-good and pending sidecars;
- activation only after the replacement generation reports ready;
- rollback when a candidate generation fails before readiness; and
- interrupted-activation recovery on the next start.

Only an inbound credential-authority-only change activates in place. Routing,
DNS, listeners, outbounds, resources, TLS, client credentials, and mixed
changes persist and request a clean generation replacement. Management
listener/authentication changes are rejected by the API and require a local
edit plus restart.

Manual edits are loaded at process restart; there is no background file
watcher. `--supervise` restarts failed generations inside the process with
bounded backoff. `service_mode` is intent only. Shutdown handles process
signals with bounded orderly retirement.

### 4.8 Management and user presentation

The management listener is disabled by default, loopback-only,
bearer-token-protected, bounded, and has no CORS support. Every `/api/v2/`
request, including health, requires authentication. Optional static dashboard
assets are public only when dashboard serving is enabled.

The management surface is exactly:

- `GET /api/v2/`
- `GET /api/v2/health`
- `GET /api/v2/health/live`
- `GET /api/v2/health/ready`
- `GET /api/v2/status`
- `GET /api/v2/paths`
- `GET /api/v2/traffic`
- `GET /api/v2/sessions`
- `GET /api/v2/flows`
- `GET /api/v2/diagnostics`
- `GET /api/v2/config`
- `GET /api/v2/balancers`
- `GET /api/v2/dns/status`
- `GET /api/v2/dns/explain?domain=<domain>`
- `POST /api/v2/actions/path`
- `POST /api/v2/diagnostics/peer`
- `POST /api/v2/config/validate`
- `POST /api/v2/config/apply`
- `POST /api/v2/balancers/actions`
- `POST /api/v2/dns/query`
- `POST /api/v2/dns/cache/flush`

The main snapshot schema is `mptunnel.management.v5`; health uses
`mptunnel.health.v2` and balancer status/actions use
`mptunnel.balancer.v1`. `GET /api/v2/config` returns
path/revision/activation state, not secret-bearing
TOML. Config mutation accepts a complete `application/toml` document; there is
no PATCH, diff, history, or per-field endpoint.

Path control accepts endpoint-local `enabled`, `suspect`, `failed`, or
`disabled` state through stable `{outbound, path, state}` names. Peer requests
use `{service, service_name, session_id}`; response indexes are not mutation
identity. Balancer actions are exactly `enable-member`, `drain-member`,
`disable-member`, `pin-member`, and `automatic`.

Observability includes:

- readiness/liveness/degraded reasons;
- Product balancer state;
- local and peer path state;
- logical sessions and bounded active-flow detail;
- sanitized inbound/outbound inventory and immutable per-flow origin,
  original target, and selected leaf/balancer member;
- exact logical forwarded byte/datagram totals excluding carrier
  retransmission and multipath copies;
- one-second rates and bounded history;
- Product admission saturation/rejection state;
- DNS cache/upstream/FakeDNS state;
- structured text or JSON logs with redaction, bounds, and rate limiting; and
- opt-in sanitized authenticated peer diagnostics.

The embedded dashboard presents health, balancers and member controls, paths,
traffic, sessions, flows, and peer diagnostics. It is not a separate
configuration system.

## 5. Core capability consumed by Product

Product relies on the following already-wired Core contract without owning or
tuning it:

- MPP wire version 4 only;
- reliable streams and datagrams over TCP, QUIC, or both;
- one logical stream identity and directional offset space across carrier
  changes;
- independent Data ACK and receive-window state per direction;
- measured available-first carrier selection with backup-path semantics;
- aggregation across eligible paths for sustained demand;
- bounded cross-path reinjection and duplicate suppression;
- logical stream retention across complete carrier outage;
- TCP and QUIC native congestion control and recovery;
- path-instance and attachment identity fences across reconnect; and
- endpoint-local path lifecycle control and read-only diagnostics.

All algorithm constants, timing, scheduling, aggregation, recovery, and
competitive-performance claims remain outside this Product document and are
governed by the RFC and Performance track.

## 6. Platform boundary

Portable desired state stays generic. Platform-specific device, route, DNS,
and socket work remains under `src/platform/`; generic Product/Core code
consumes prepared providers and host-protection interfaces.

| Platform | Package target | Actual v0.1.2 Product status |
| --- | --- | --- |
| Linux amd64/arm64 | Static musl CLI | Proxy and external TUN implemented. Managed full/split TUN owns device, RPDB/routes, endpoint bypass, DNS publication, socket marking, readiness, and exact rollback. Linux-only RPDB tuning is isolated under `[inbounds.host.linux]`. |
| Windows amd64/arm64 | Static-runtime MSVC CLI plus Wintun | Proxy implemented. Built-in managed VPN bridge covers Wintun, native-route snapshot, route/DNS transaction, native socket binding/protection, two-phase publication, and reverse cleanup. Native clean-machine lifecycle/performance evidence remains pending. |
| macOS amd64/arm64 | Native CLI | Proxy CLI builds and runs. Lower-level utun and route transaction primitives exist, but daily-use managed VPN remains `AdapterRequired` until a first-party Network Extension packet-flow/DNS host, signing, entitlements, and native evidence exist. |
| Android arm64 | NDK CLI | Host/core CLI and Rust provider contracts exist. It is not an APK/AAB/AAR/JNI application. A host-owned `VpnService`, consent/lifecycle UI, established descriptor, network binding, and `protect(fd)` integration are required; process-managed mode is rejected. |
| iOS | None | Unsupported. |

No platform silently falls back from requested managed VPN operation to an
unconfigured tunnel.

## 7. Release package boundary

The tag-triggered GitHub release workflow is the authoritative cross-platform
build and publication path. It builds on native GitHub runners where required,
verifies each archive and its platform dependency closure, then assembles one
exact public inventory. Publication preflights the tag as absent, draft, or
published: it creates and verifies an absent draft, resumes only a byte-exact
matching draft, and treats an exact published release as an immutable no-op.
Any mismatched release is rejected. Failure cleanup can delete only the draft
created by that workflow run. The manual release-check workflow runs the same
package proof without publishing.

Public assets are exactly:

- `mptunnel-linux-amd64.tar.gz`
- `mptunnel-linux-arm64.tar.gz`
- `mptunnel-windows-amd64.zip`
- `mptunnel-windows-arm64.zip`
- `mptunnel-macos-amd64.zip`
- `mptunnel-macos-arm64.zip`
- `mptunnel-android-arm64.tar.gz`
- `SHA256SUMS`

Every archive contains only:

- one `mptunnel` binary;
- package `README.md`;
- `LICENSE`;
- `examples/client.toml`; and
- `examples/server.toml`.

Platform additions are:

- Linux: `service/systemd/mptunnel.service`
- Windows: architecture-matched `wintun.dll` and `WINTUN-LICENSE.txt`
- macOS and Android: no extra service or host application

`THIRD_PARTY_LICENSES.html`, internal plans, audits, lab output, temporary
evidence, and build caches are not release-bundle contents. Local package
scripts stage under `.tmp/release/dist`; generated archives are not committed
or pushed as release assets.

## 8. Explicit non-goals

v0.1.2 deliberately does not provide:

- compatibility with MPP v1-v3, old configuration aliases, or unversioned
  APIs;
- V2Ray/Xray/Hysteria protocol interoperability;
- enterprise RBAC, SSO, billing, fleet control, compliance, or Kubernetes
  control planes;
- arbitrary plugin, outbound-chain, multi-hop, reverse-proxy, mesh, or
  cross-server live-flow migration;
- SOCKS5 BIND, UDP through HTTP/HTTPS CONNECT, transparent TPROXY, or arbitrary
  L2 tunneling;
- profiles, share URI/QR, subscriptions, tray UI, mobile UI, or browser-based
  config editing;
- install/update/uninstall commands or automatic package-manager integration;
- Windows Service installation, macOS Network Extension, Android application,
  or iOS package;
- system-proxy mutation;
- kill switch, crash/reboot route restoration, or a persistent firewall
  policy;
- built-in log rotation or support-bundle generation;
- ECH, REALITY, port hopping, pluggable transports, or a claim of
  censorship-proof indistinguishability;
- binary code signing/notarization beyond GitHub release provenance and the
  separately signed Wintun runtime; or
- Product-owned Core parameter optimization.

## 9. Evidence gaps and release truth

| Gap | Consequence |
| --- | --- |
| Source version alone is not release proof. | A valid release requires the package version, exact stable tag, successful native GitHub workflow, checksum inventory, and freshly downloaded public assets to agree. |
| A local or CI build is not the shipped artifact. | Package claims apply only to the exact published inventory after archive, binary, architecture, dependency, and checksum verification. |
| Linux is the primary packaged-process and performance evidence platform. | Other-platform build success is not equivalent to native daily-use evidence. |
| Windows managed Wintun lacks clean-machine privilege, interface-loss, suspend/resume, crash, and native throughput/failover evidence. | Windows managed VPN remains implemented but not fully proven. |
| macOS lacks the first-party Network Extension packet-flow/DNS adapter. | macOS may claim proxy CLI support, not managed daily-use VPN. |
| Android lacks an application-owned `VpnService` integration and user journey. | Android may claim a host/core CLI artifact, not a complete VPN product. |
| No independent cryptographic or application-security audit exists. | The custom protocol must remain clearly labeled unaudited. |
| Controlled tests do not establish passive indistinguishability or resistance to active classification on arbitrary surveillance networks. | Tests prove parser gating and uniform rejection only. Source-aware probes may still fingerprint the carrier without producing a valid selector. |
| Parser and unit tests are not complete ecosystem interoperability, long-soak, or hostile-network evidence. | Final audit must distinguish wired capability from native and operational proof. |
| Competitive aggregation, failover, latency, throughput, CPU, memory, and real-Internet conclusions belong to the Performance track. | This Product map makes no competitive-performance declaration. |

## 10. Current conclusion

The source implements the main daily-use Product graph: authenticated SOCKS5
and HTTP CONNECT, fixed TCP/UDP forwarding, TUN L4, strict routing and ACLs,
split/encrypted DNS and FakeDNS, native and MPP outbounds,
independent-server balancers, canonical persistent configuration,
generation-safe runtime apply, operational CLI, management API, and
dashboard.

The honest platform claim is narrower: Linux has the strongest complete
Product path; Windows managed VPN is wired but still needs native clean-machine
proof; macOS is proxy-only until its Network Extension host exists; Android is
a host/core artifact until an application supplies `VpnService`.

MPTUNNEL must not yet be described as a universal daily-use replacement for
V2Ray/Xray or Hysteria until the release, native-platform, security, and
independent Performance evidence gaps above are closed.
