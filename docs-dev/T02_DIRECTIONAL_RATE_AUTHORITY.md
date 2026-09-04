# T02 directional scheduling-rate authority

Status: typed component checkpoint complete. The exact authority and
publication fences are locally GREEN, but the checkpoint is not deployable or
a release-acceptance claim. T03 runtime migration was rejected, so the legacy
scalar consumer must retain its pre-`b5b4b5a` projection until a complete
allocator consumes nonnumeric `Unlimited` and typed `C` directly. T04 separately owns
inferred-rate admission coupling.

## Decision

The rate input to the isolated Section 10.2 action-score component is an
**exclusive typed authority automaton**, not a numeric lattice. No current
runtime owner consumes that component. Calling it a lattice would imply that two
independent rates can be joined, commonly by `max`; that operation has no
proven meaning here and cannot represent the nonnumeric `Unlimited` startup
form.

One reducer is scoped to exactly:

```text
(carrier instance, original-sender direction)
```

It has one immutable startup basis:

```text
PortableStartup(C0)
ConfiguredStartup(positive u64 bits/s)
UnlimitedStartup
```

and at most one effective basis:

```text
QUIC: QuinnBbr3NativeOperationalV1(positive u64 bits/s)
TCP:  the immutable startup basis
```

Source choice is semantic, never numeric. No branch compares two source values,
adds them, divides them by a flow count, or takes their maximum. Product
delivery, `PATH_CAPACITY`, generic measured rates, and peer `PATH_METRICS` are
not members of this automaton.

`UnlimitedStartup` means that the startup service-duration term is zero. It is
not a number, not one terabit per second, and grants no native credit, Product
window, pacing, or admission. A valid transport-specific source may replace it
in the same way that it replaces a finite startup basis.

## Why these are the only sources

`QuinnBbr3NativeOperationalV1` is already defined by RFC Section 10.2.1. It is
the exact gain-free operational bandwidth component used by the active Quinn
controller and carries the controller activation, identity, and central
revision. A valid publication persists for that activation because the live
controller continues to use the retained value. An absent later poll is not a
revocation.

Core declares no named TCP NativeOperational adapter. Kernel TCP delivery,
pacing, congestion-window, and queue observations are therefore diagnostic
under RFC Section 17.1 and cannot replace startup in T02. In particular, a
`TCP_INFO` delivery-rate sample is achieved same-socket evidence, not the
gain-free operational bandwidth of CUBIC's live send model. Its availability
also differs by platform. Promoting it here would silently invent a new
profile, make scheduling semantics platform-dependent, and contradict the
authoritative RFC.

The existing fixed client output copies one `f64` TCP sample when the stream is
attached and never refreshes it. That copy is not an authority source. T02
removes it from `C`; TCP fixed and switchable outputs retain the exact immutable
startup basis. A future profile may add a distinct TCP adapter only after it
declares the full rate domain, incarnation/direction reducer, validity,
freshness, and replacement contract required by Sections 10.2.1 and 17.1.

## Excluded evidence and counterexamples

### Product delivery is allocation-dependent

Let a 500-Mbit/s carrier receive only one 64-KiB MPP service quantum per second
from its scheduler. A Product ACK clock can then report approximately:

```text
8 * 65,536 / 1 second = 524,288 bit/s
```

even though the carrier can still deliver 500 Mbit/s. The current Product
qualification floors prove attribution, volume, and freshness; they do not
prove continuous backlog. Substituting that Product value for carrier service
would lower future allocation, which lowers the next Product value, forming a
self-reinforcing underfeed loop. Taking `max(startup, Product)` hides the loop
but creates the opposite defect: a high configured prior can never downshift.
Therefore Product rate remains exact-output diagnostic/completion evidence and
does not supply `C` in T02.

### `PATH_CAPACITY` is a finite ingress transaction

A receipt proves that one bounded train reached the peer in order during its
measured interval. It does not prove current continuously available service,
and RFC Section 11.2 explicitly grants it no Product or scheduling completion-
rate authority. Current code paths that label the receipt as native carrier
rate evidence are model violations; the receipt remains diagnostic.

### Peer and generic metrics have the wrong lifetime

Peer metrics are detached presentation data and can describe the opposite
sender, an older carrier lifetime, or a different observation time. Generic
measured rate fields merge multiple provenances. Numeric equality with a valid
local source does not repair either identity. Both remain diagnostics.

## Normalized work and rate units

The canonical reliable scheduling action is one unsplit Core `STREAM_DATA`
frame. For payload length `p`, its normalized Core work is:

```text
M(p) = 10-byte MPTF header
     + 8-byte stream id
     + 8-byte offset
     + 4-byte payload length
     + p
     = p + 30 bytes
```

This is a checked integer type, not a payload count and not the codec's
capacity-reservation hint. Thus the portable startup quantum is:

```text
M0 = 14,600 + 30 = 14,630 bytes
C0 = ceil(8 * M0 * 1,000 / 333) = 351,472 bit/s
```

T03 extends this same normalized-Core domain with action-specific checked
constructors rather than reusing a raw payload count: `IP_PACKET` is likewise
`p+30`, while `DATAGRAM_DATA` is `p+34` because it also carries a four-byte
TTL. This does not change the T02 rate authority or the reliable startup prior.

QUIC currently re-records a Core action larger than 12,000 payload bytes below
Product scheduling. A 14,600-byte action therefore emits two MPTF frames and
14,660 MPTF bytes, plus two four-byte record prefixes and native H3/QUIC
overhead. Those carrier-adapter bytes are intentionally outside canonical
Core work. TCP encryption/framing similarly remains outside it. This choice
keeps `M` carrier-neutral for the same logical action and must be stated in the
RFC; describing `M` as every actually emitted carrier-side MPTF frame would
instead require a path-specific work value.

The named QUIC native source counts its own acknowledged byte domain. T02 uses
the adapter projection:

```text
1 native congestion-accounted byte := 1 normalized Core service byte
```

This is a nominal identity projection for advisory ordering, not physical byte
equality. Quinn accounts QUIC packet bytes, so presentation, crypto, packet,
and retransmission overhead create bounded practical error. Exact physical
conversion would require an attributed MPP-to-native efficiency ledger that
the runtime does not have and T01 rejected as a Core prerequisite.
Consequently the rank is monotone and dimensionally stable but is not an exact
cross-underlay completion theorem. Close candidates remain subject to the final
matched runtime matrix.

## Symbolic properties

For one canonical action with local pre-native work `A`, timing `T`, and the
effective rate state above:

```text
Finite(C):          S_ms = T_ms + ceil(8000 * (A_bytes + M_bytes) / C_bps)
UnlimitedStartup:   S_ms = T_ms
```

The factor 8000 is the exact bits/byte and milliseconds/second conversion.
T03 owns the checked millisecond calculation; T02 supplies only typed `C` and
canonical byte-domain `M`.

The model establishes:

1. **Exclusivity:** exactly one rate basis contributes to a score.
2. **Direction safety:** an observation from the other original sender cannot
   change this reducer.
3. **Incarnation safety:** carrier replacement cannot inherit learned rate.
4. **Numeric safety:** finite zero is unrepresentable and `u64` precision is
   retained beyond `2^53`; checked overflow makes the action unrankable.
5. **Monotonicity:** for fixed `T`, `A`, and `M`, a larger finite effective
   `C` cannot increase `S`.
6. **No rate ceiling:** `C` orders eligible actions only. It does not pace,
   window-limit, admit, or deny native work.
7. **No feedback collapse inside T02:** scheduler-supplied Product service is
   excluded from carrier-rate authority.

Property 6 is a cross-transaction obligation. The future sustained allocator
must consume this already-reduced state without modifying it, and T04b must
remove any inferred-rate admission denial. Until those transactions close,
T02 GREEN is not a throughput acceptance claim.

## Disposition of `a4679b5`

Retain:

- endpoint-local configured rate resolution;
- explicit path override versus `[flow]` inheritance;
- output-incarnation attachment and reset behavior; and
- the rule that peer telemetry cannot overwrite local configuration.

Supersede:

- `RateHint -> f64` inside the typed authority automaton, including a fabricated
  1-Tbit/s representation of typed Unlimited; the still-live legacy scalar
  retains that historical ordering sentinel until its allocator is replaced;
- payload-only derivation of `C0`;
- `max(startup/native, Product)` as a way to construct typed `C` in request,
  fixed, and switchable projections;
- treating a fixed attachment-time TCP telemetry copy as scheduling authority;
- relabelling `PATH_CAPACITY` as typed native carrier authority;
- active-flow division while selecting `C`; and
- the unused ReceiptMode scheduling-rate draft. The actual wire
  `PathCapacityReceipt` remains unchanged and diagnostic.

## Focused proof sequence

Production changes begin only after REDs establish these current failures:

1. Unlimited becomes a finite 1-Tbit/s authority.
2. A configured rate above `2^53` loses integer identity through `f64`.
3. The portable prior is derived from 14,600 payload bytes rather than 14,630
   canonical work bytes.
4. A fresh `PATH_CAPACITY` receipt changes typed request scheduling `C`.
5. Changing only Product delivery changes typed request, fixed, or switchable
   carrier `C`.
6. Changing only same-socket TCP telemetry changes fixed or switchable `C`,
   despite Core declaring no TCP adapter.
7. Canonical encoded work for payloads 1 and 14,600 is exactly payload plus 30
   and agrees with unsplit Core codec output.

GREEN requires one shared typed reducer and the same result/provenance in
request, fixed response, switchable response, and L3 projections. QUIC may use
its exact native reducer; TCP must retain startup regardless of optional kernel
telemetry. No scheduler formula, admission, recovery, requalification,
dashboard, or lab threshold is changed in T02.

## Corrected checkpoint oversight

The first committed draft incorrectly treated a carefully qualified
`TCP_INFO` delivery sample as sufficient to create a Core TCP scheduling-rate
adapter. That conclusion considered the sample's local provenance but not the
full standards hierarchy: RFC Section 17.1 explicitly reserves such an
adapter for a separately declared future profile. The draft therefore crossed
from auditing existing authority into inventing a new one. This correction is
made before production code or REDs were written; no TCP runtime behavior was
changed by the rejected proposal.

## Implementation outcome

The runtime now carries `DirectionalServiceRate` alongside every production
request, fixed response, switchable response, and L3 scheduling projection.
The value binds one carrier instance and original-sender direction, preserves
finite `u64` identity, represents `UnlimitedStartup` without a sentinel, and
allows only the named Quinn BBR3 native source to replace startup. Product,
`PATH_CAPACITY`, peer/generic measurements, and TCP kernel telemetry remain
separate from typed authority. While the shipped scorer still reads its
historical scalar projection, qualified owner-specific carrier, Product, peer,
or generic observations may continue to supply that compatibility scalar;
they cannot alter typed `C`.

QUIC planning is advisory. Its final request, fixed-response,
switchable-response, client-L3, and server-L3 ownership transfers now execute
under the exact Native activation/stamp fence and consume the current full
shape returned inside that fence. This distinction is required because Quinn
RTT, RTT variance, congestion window, flight, pacing, and application-limited
state can change without advancing central generation `G`. TCP has no named
NativeOperational adapter and retains its immutable startup basis and existing
non-native commit path.

`PATH_METRICS` no longer serializes endpoint-local configured or Unlimited
startup policy. With no independent observed diagnostic it emits the public
portable C0 placeholder, `351472`, together with `rate_observed=false` and zero
validity. It publishes only the diagnostic source selected by the compatibility
projection: an exact Native shape first, otherwise qualified carrier, winning
qualified Product, or generic evidence. A lower or unqualified source cannot
lend its value, epoch, or ACK provenance. Controller-local NativeOperational
rate may remain numerically visible, but it has no transferable observation
epoch and therefore remains `rate_observed=false` with zero validity.

## Foreseen oversights closed before checkpoint

The final integration audit found and closed these defects inside the same T02
authority transaction:

1. A lazy request observation acquired Native authority while holding the
   health lock, reversing the publisher's Native-to-health order. Native
   shapes are now materialized before health observation.
2. Request, response, and client-L3 apply initially checked only the central
   stamp or retained a planning-time shape. Same-stamp Quinn timing/window
   changes could therefore authorize stale Product ownership or freeze a stale
   recovery clock. All affected publication paths now use the closure-provided
   current shape before their first irreversible mutation.
3. Client-L3 advisory flow lookup refreshed activity before carrier
   acceptance. Activity is now committed only after the planned send is
   accepted.
4. Legacy UDP request fixtures treated missing Native authority as
   `NotApplicable` and permitted an authority-less commit. The test-only
   escapes were removed; UDP fixtures now carry the exact production authority
   their premise declares, while missing-authority UDP fails closed in every
   build.
5. The first no-observation `PATH_METRICS` correction covered the generic
   serializer but missed StartupPrior cached through QUIC health. The producer
   is now basis-gated: only NativeOperational can supply that native wire
   diagnostic.

An audit hypothesis that native authority and a separate TCP capacity proof
could coexist on one production carrier was rejected after producer tracing:
the state is unreachable, so no fallback or provenance branch was added.
Bound-affinity reselection and per-congestion-window packet admission were also
rejected from T02 because they change policy rather than rate authority; their
existing owner transactions remain assigned to T04 or later.

## Focused GREEN evidence

- typed rate/work model: 7/7;
- central carrier-rate reducer: 19/19;
- runtime Native authority: 20/20;
- complete T02 filter: 11/11;
- health and wire-diagnostic projection: 29/29;
- request multipath and outer request: 63/63 and 26/26;
- fixed response, switchable original commit, and response delivery: 27/27,
  12/12, and 32/32;
- client L3 and server L3 Native transaction: 3/3 and 13/13; and
- `cargo check --lib`, formatting, and diff checks: GREEN.

The response snapshot module is 33/35 only because the two exact Product
confidence/rate-floor failures frozen for T12 remain unchanged. T02 introduces
no new failure there.

## Post-checkpoint transaction-order correction

The first implementation checkpoint changed two surfaces at once. It correctly
added the typed `DirectionalServiceRate` sidecar and exact QUIC Apply fences,
but it also projected that sidecar back into `PathSnapshot.delivery_rate_bps`.
The still-live legacy scorer reads only that scalar; it does not read the typed
sidecar. Because T03 runtime-owner migration is rejected, this changed
production scheduling before a complete replacement existed.

The effect is not cosmetic. For TCP, qualified live Product/native diagnostics
that previously supplied the legacy scalar were replaced by the immutable
startup projection. Legacy `Unlimited` changed from its old one-terabit
ordering sentinel to the portable 351,472-bit/s startup value, a factor of
about 2.85 million. Existing fixtures were changed accordingly--for example,
a fixed-output 500-Mbit/s Product observation remained at an 80-Mbit/s startup
scalar, and a server TCP L3 120-Mbit/s observation remained at a 25-Mbit/s
startup scalar. Those expectations prove typed exclusivity but also encode an
unaccepted legacy behavior change.

The bounded correction is projection-only. An exact activation-scoped QUIC
Native shape has first refusal; every remaining non-Native branch restores the
complete scalar decision tree that existed immediately before `b5b4b5a`:

1. retain typed startup/Unlimited/NativeOperational state;
2. retain exact direction/incarnation scopes and current-shape QUIC commit
   fences;
3. restore every pre-checkpoint scalar source, precedence rule, rate scope,
   and legacy Unlimited shim at still-live legacy consumers, including the
   generic measured-rate fallback; changing only a subset would itself be an
   unproved scheduling-policy transaction;
4. test that changing a legacy diagnostic can change only the compatibility
   scalar while the typed `C` remains unchanged; and
5. delete the compatibility projection only when a complete runtime allocator
   consumes the typed sidecar.

This rollback does not endorse the old mixed scalar as the final model. It
restores transaction isolation and prevents an unfinished component checkpoint
from changing scheduling or performance. Any removal of a legacy Product,
peer, generic, or carrier source requires its own reachable RED and isolated
transaction after this checkpoint; the typed authority model alone is not that
proof. Dynamic service discovery and the sustained allocator remain separate,
later transactions.
