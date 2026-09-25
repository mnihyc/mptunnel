# Webhooks

Webhooks send HTTP notifications for MPP lifecycle events through a named
outbound or balancer. Use them to report path outages and recovery attempts,
observe authenticated client address changes, or export periodic path snapshots.
They are optional; omitting `[webhooks]` starts no webhook worker or timer.

## Path down and every recovery check

Add this to a configuration with an MPP outbound named `edge-mpp` and an
independent delivery outbound named `notify-direct`:

```toml
[[webhooks.rules]]
name = "down-and-rechecks"

[webhooks.rules.when]
outbounds = ["edge-mpp"]
paths = ["wan-tcp", "wan-quic"]

[[webhooks.rules.when.any]]
events = ["path.state_changed"]
from = ["up"]
to = ["down"]

[[webhooks.rules.when.any]]
events = ["path.probe_completed"]
probe_state_at_start = ["down"]

[webhooks.rules.target]
url = "https://notify.example.net/mpp"
outbound = "notify-direct"
```

This reports the up-to-down transition and each actual background connection
check that started while the path was down, including the successful recovery
check. To receive only failed checks, add `outcome = ["failure"]` to the second
branch. To include an initial connection failure, omit `from = ["up"]`.

The normal probe and reconciliation owners decide when to check a path. A
webhook never starts extra checks. Skipped checks and callers waiting for the
same establishment attempt do not generate extra completion events. Repeated
failed attempts are distinct events; a state-change notification is emitted
only when the state changes. A failing recovery attempt that first makes an
unknown path down did not *start* down, so that first completion does not match
`probe_state_at_start = ["down"]`.

`when.events` selects events directly. Use `when.any` instead when different
events need different conditions. Common source filters apply to every branch;
branches are OR, fields are AND, and values within a list are OR. An event
matching multiple branches still produces one delivery for that rule.

## Event vocabulary

| Event | Observation |
| --- | --- |
| `path.state_changed` | A configured outbound path changes between `unknown`, `up`, `down`, and `idle`. TCP pool members share one aggregate path. |
| `path.policy_changed` | A configured outbound path's control state changes: `enabled`, `suspect`, `failed`, or `disabled`. Reapplying the same value is silent. |
| `path.probe_completed` | One actual background establishment check finishes with `success`, `failure`, or `cancelled`. Includes the state at attempt start and whether the result was applied. |
| `path.interval` | A periodic snapshot of a configured outbound path. Requires `interval_s`; adds no network probe. |
| `carrier.state_changed` | An authenticated physical TCP or QUIC carrier changes to `ready`, `draining`, or `closed`. |
| `carrier.policy_changed` | Accepted directional peer usage changes between `available` and `backup` for an exact carrier. Sequence-only refreshes are silent. |
| `carrier.address_changed` | A validated address change within a carrier, such as QUIC migration or NAT rebinding. |
| `session.state_changed` | A session becomes `attached` on its first ready carrier, `detached` on losing its last one, or explicitly `retired`. Reattachment is another `attached` transition. |
| `session.peer_addresses_changed` | The locally observed peer IP set across ready carriers changes. |
| `node.state_changed` | An activated runtime generation becomes `ready`, `stopping`, or `failed`. |
| `balancer.member_changed` | A member's recorded health (`healthy`/`unhealthy`) or administrative mode (`enabled`/`draining`/`disabled`) changes. Backoff countdown and probe ownership are silent. |
| `balancer.probe_completed` | A configured balancer target-connect probe completes. |

Path events refer to names configured on the local **outbound**. An inbound
server does not know the client's path names or TCP pool grouping; filter its
carrier/session events by `inbounds` instead. Local path names, opaque member
slots, physical carrier incarnations, and session IDs are distinct identities.

An `up` path has at least one authenticated ready carrier. Losing one TCP pool
member leaves the path up while another member is ready. `down` requires a
committed failure with no ready carrier left; `idle` denotes deliberate
retirement without failure. An unattempted path is `unknown`. Administrative
control is a separate axis: marking a path suspect or disabling its use is
distinct from observing physical availability. Peer `backup` usage is a carrier
policy, not a path outage.
For L3, carrier readiness does not imply that the host route or IP attachment is
ready. Node readiness likewise describes local services, not Internet access.

Session attachment counts ready carriers, not application flows. A disconnect
can detach a session without retiring it. Address-set events represent overlap
honestly: reconnecting from another network may produce `A → A+B → B`. A second
address is not automatically evidence of physical roaming. On the server this
is the client IP set; on the client it is the server IP set. A TCP reconnect is a
new carrier; QUIC address-change events require successful path validation.
Carrier `closed` records retirement from MPP ownership; final socket cleanup
can finish afterward.
Native address observations include a revision: a slow observer can coalesce
rapid successive validations, and a revision gap identifies that loss of detail.
The fields are `carrier.address_revision`, `change.skipped_revisions`, and
`change.coalesced`; `change.components` lists `ip` and/or `port`.

## Periodic snapshots and address changes

```toml
[[webhooks.rules]]
name = "path-summary"
[webhooks.rules.when]
events = ["path.interval"]
outbounds = ["edge-mpp"]
interval_s = 60
[webhooks.rules.target]
url = "https://notify.example.net/metrics"
outbound = "notify-direct"

[[webhooks.rules]]
name = "client-addresses"
[webhooks.rules.when]
events = ["session.peer_addresses_changed"]
inbounds = ["mpp-listener"]
initial = false
[webhooks.rules.target]
url = "https://notify.example.net/clients"
outbound = "notify-direct"
json = { session = "{session.id}", before = "{change.before}", after = "{change.after}" }
```

Missed interval ticks are skipped. A pending periodic snapshot for the same
rule and path is replaced by the newest snapshot; lifecycle/probe events are
not coalesced. Intervals do not create new measurement or probe schedules.

Common source selectors are `outbounds`, `inbounds`, `balancers`, `paths`, and
`transports` (`tcp`, `quic`). Event-specific selectors include `from`, `to`,
`outcome`, `probe_state_at_start`, `initial`, and `changed`. Values in ordinary
filter lists are alternatives; every component listed in `changed` must be
present in the event's `change.components`. Configuration validation rejects
unsupported combinations, unknown names, and templates for fields unavailable
to the selected event families.

## Request and templates

Targets require a URL and exactly one `outbound` or `balancer` selector. The
method defaults to `POST`. Without a body setting, POST/PUT/PATCH send the
standard JSON event envelope; GET/HEAD have no body. Set one of `json`, `form`, or `text` for a custom
payload. Header names and the method are fixed configuration. Header values can
be templates or the existing byte-material sources, for example:

```toml
[webhooks.rules.target]
url = "https://notify.example.net/paths/{path.name}"
outbound = "notify-direct"
method = "POST"
query = { event = "{event.type}" }
headers = { Authorization = { from = "file", path = "notify-authorization.txt" } }
json = { event = "{event.type}", id = "{event.id}", path = "{path.name}", state = "{change.to}", local_ips = "{path.local_ips}" }
```

This target example belongs to a `path.state_changed` rule. Secret files must
contain a valid HTTP header value without line breaks. As elsewhere in MPTUNNEL,
`from = "env"` names an environment variable whose value is a **file path**;
it does not read the secret directly from the variable.

A whole JSON value such as `"{path.local_ips}"` retains its array/number/boolean
or null type. An embedded token becomes text. Path substitutions and values in
the target `query` table are escaped as individual components; header values
reject line breaks. Keep query text in `url` static and put dynamic values in
`query`. URL scheme, host, port, method, header names, and outbound selection
cannot be templated.
Missing required text/address fields fail that delivery instead of silently
substituting an empty string. Literal braces use `{{` and `}}`.

Events carry a versioned envelope, event ID and type, occurrence/observation
time, process identity, configuration generation, and subject identity. Event
and delivery IDs remain stable across explicit retries. Different independent
subjects have no promised global causal order. Snapshots are frozen when
published; a queued request does not substitute newer state at send time.

Every HTTP request includes the reserved `MPTUNNEL-Event-ID` header, equal to
`event.id`, and `MPTUNNEL-Delivery-ID`, which identifies this event's delivery
for one rule and remains the same across retries. Use the delivery ID when
deduplicating: one event can fan out to several rules. These headers are
generated by MPTUNNEL and cannot be configured. The JSON envelope has
`schema_version = 1`, an `event` object, a `subject` object, and event-specific
data at the top level. `event` contains `id`, `type`, `occurred_at`,
`observed_at`, `process_boot_id`, `configuration_generation`, `subject_id`,
`subject_sequence`, and optional `reason` and `initial`. Timestamps are UTC
with millisecond precision; `process_boot_id` is a 16-digit hexadecimal string.
Event timestamps record the local publisher's observation, not a remote clock
or the exact arrival time of a network packet.
`subject.id` and `subject.sequence` repeat the event's subject identity and
sequence; the sequence is null when the source has none. Payload roots such as
`path`, `carrier`, `session`, `probe`, and `change` vary by event type and role.

Configured and rendered values have fixed bounds: each target's compiled
templates, headers, and additional TLS roots total at most 64 KiB; one event
snapshot is at most 16 KiB, with at most 8,192 JSON nodes and depth 32; the
rendered URL is at most 8 KiB; the request body is at most 64 KiB; and request
and response header blocks are at most 16 KiB (responses are limited to 64
headers). A target may add at most 128 dynamic query pairs, with keys at most
512 bytes each.

Use explicit endpoint fields: `carrier.local.ip/port`, `carrier.peer.ip/port`,
`path.local_ips`, and `session.peer_ips`. A client local IP is not its public NAT
address. A wildcard-bound local address may be unavailable. Missing metrics are
null; retained measurements must retain their freshness information.

The selected outbound follows the existing native socket, proxy, MPP, and DNS
policies. `target_resolution = "full-resolve"` is the default. With
`target_resolution = "as-is"`, a domain-capable proxy or MPP server receives the
hostname unchanged; a native IP-only outbound still resolves the domain locally.
Optional `dns_policy` selects a named DNS policy when local resolution occurs;
otherwise split-DNS rules apply. As-is delivery through a domain-capable proxy
or MPP server does not resolve the webhook target hostname locally. Delivery
carrier endpoints can still require DNS. There is no route-matching or implicit
direct fallback for the webhook target. L3 configurations use native delivery
outbounds.

A lifecycle rule cannot deliver through the outbound or balancer it monitors,
including DNS dependencies that would lead back to that resource. This prevents
a notification from recursively creating its own connection events. Use a
separate delivery route. Timer-only rules may deliberately use the measured
outbound; their HTTP traffic still consumes its resources.

HTTP delivery accepts 2xx responses, does not follow redirects, and does not
replay requests automatically. Response headers are bounded and response bodies
are discarded by closing the connection. TLS certificate validation remains
required; additional trust material uses the existing material-source format.
For a private certificate authority, set
`tls_ca_certificate = { from = "file", path = "notify-ca.pem" }` in the target.

## Queue, deadlines, and retries

Defaults usually suffice. Override shared settings once and override an
individual rule's `[webhooks.rules.delivery]` only where needed:

```toml
[webhooks]
# Defaults shown; omit unchanged values.
max_in_flight = 4
max_pending_deliveries = 256
max_pending_bytes = 1048576
shutdown_timeout_s = 2

[webhooks.delivery]
timeout_s = 10
max_age_s = 30
max_attempts = 1
# For opt-in retries, for example max_attempts = 3:
# initial_backoff_s = 1
# max_backoff_s = 5
```

`max_in_flight` limits simultaneous HTTP attempts across the generation. Each
rule has one active delivery and preserves FIFO order. A retry delay releases
the global network slot but retains that rule's place. `max_pending_deliveries`
and `max_pending_bytes` bound admitted work, including rule fanout, queued
deliveries, active attempts, and retry waits. Full queues
drop new work without waiting on tunnel owners and expose drop counters.
The byte budget counts serialized event snapshots; active request buffers and
transport state have their own finite limits and are additional memory.

`timeout_s` bounds a whole attempt, including resolution and connection.
`max_age_s` bounds the delivery's complete lifetime, including queueing and retry
waits. `max_attempts` counts the first attempt, defaults to one, and is limited
to five. Explicit retries use bounded jittered backoff for transient transport
failures, timeout, HTTP 408/429, and 5xx. Invalid templates, certificate failures,
and ordinary 4xx responses are terminal. A lost response can mean the receiver
processed a request: receivers should deduplicate by delivery ID when retries
are enabled. There is no durable queue or exactly-once delivery guarantee.

An uncommitted configuration does not send callbacks. Runtime replacement
retires the old queue; it does not replay old deliveries with new credentials.
`shutdown_timeout_s` permits a bounded best-effort drain while transport owners
remain available; zero disables draining. Shutdown and failure notifications
cannot be guaranteed after a crash, loss of the delivery route, or process kill.

Enabled webhooks consume bounded CPU, memory, and network resources. Event
publication is restricted to lifecycle boundaries; packet, payload, and ACK
processing do not publish hooks. Failing webhook targets do not change MPP path
policy or feed webhook HTTP results into passive balancer health.

## Diagnostics

`GET /api/v4/webhooks` uses the existing management authentication and reports
queue occupancy, drops, coalescing, expiration, and delivery outcomes. Status
contains rule names and sanitized result categories, not credentials, rendered
URLs, headers, or payloads. Use the normal configuration validation/apply
workflow to change rules.
