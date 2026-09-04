# T02 directional scheduling-rate authority

Status: symbolic design checkpoint. This document fixes the model boundary for
T02 before production code changes. It is not a release-acceptance claim.

## Decision

The rate input to the advisory action rank is an **exclusive typed authority
automaton**, not a numeric lattice. Calling it a lattice would imply that two
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
TCP:  TcpInfoDeliveredCapacityV1(positive u64 bits/s)
else: the immutable startup basis
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

`TcpInfoDeliveredCapacityV1` is a distinct, capability-graded source rather
than a claim that CUBIC exposes a native operational bandwidth. A valid sample
must satisfy all of the following:

- it comes from one coherent `TCP_INFO` read on the exact locally sending
  socket and names that carrier instance and direction;
- `tcpi_delivery_rate` is positive and its paired
  `tcpi_delivery_rate_app_limited` bit is false in that same read;
- positive acknowledged-byte advancement has occurred after authenticated
  carrier readiness;
- acknowledged coverage reaches the frozen delivery-window floor;
- bytes/s to bits/s conversion is checked, not saturating;
- the observation time and immutable expiry belong to that exact sample epoch;
- a later app-limited or partial poll cannot refresh the epoch; and
- a replacement carrier starts with no predecessor epoch.

The source temporarily replaces startup while it is current. On expiry it
falls back to the immutable startup basis. This is deliberately different from
the QUIC adapter: Linux `TCP_INFO` exports an achieved delivery-rate sample,
not persistent CUBIC controller state. Retaining it after expiry would recreate
the stale-low, restart-dependent recovery symptom. Linux and Android can
provide the complete source today; a platform that omits either delivery rate
or its paired provenance bit honestly remains on startup authority.

The existing fixed client output copies one `f64` sample when the stream is
attached and never refreshes it. That copy is not the source above. T02 must
give fixed outputs a read of the exact carrier's current typed sample, or leave
them on startup; it may not describe a frozen copy as live authority.

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

QUIC currently re-records a Core action larger than 12,000 payload bytes below
Product scheduling. A 14,600-byte action therefore emits two MPTF frames and
14,660 MPTF bytes, plus two four-byte record prefixes and native H3/QUIC
overhead. Those carrier-adapter bytes are intentionally outside canonical
Core work. TCP encryption/framing similarly remains outside it. This choice
keeps `M` carrier-neutral for the same logical action and must be stated in the
RFC; describing `M` as every actually emitted carrier-side MPTF frame would
instead require a path-specific work value.

Native sources count their own acknowledged byte domain. T02 uses the named
adapter projection:

```text
1 native congestion-accounted byte := 1 normalized Core service byte
```

This is a nominal identity projection for advisory ordering, not physical byte
equality. Quinn accounts QUIC packet bytes and TCP accounts encrypted sequence
bytes, so presentation, crypto, packet, and retransmission overhead create
bounded practical error. Exact physical conversion would require an attributed
MPP-to-native efficiency ledger that the runtime does not have and T01 rejected
as a Core prerequisite. Consequently the rank is monotone and dimensionally
stable but is not an exact cross-underlay completion theorem. Close candidates
remain subject to the final matched runtime matrix.

## Symbolic properties

For one canonical action with local pre-native work `A`, timing `T`, and the
effective rate state above:

```text
Finite(C):          S = T + ceil(8 * (A + M) / C)
UnlimitedStartup:   S = T
```

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

Property 6 is a cross-transaction obligation. T03 must consume this already-
reduced state without modifying it, and T04 must remove any inferred-rate
admission denial. Until those transactions close, T02 GREEN is not a throughput
acceptance claim.

## Disposition of `a4679b5`

Retain:

- endpoint-local configured rate resolution;
- explicit path override versus `[flow]` inheritance;
- output-incarnation attachment and reset behavior; and
- the rule that peer telemetry cannot overwrite local configuration.

Supersede:

- `RateHint -> f64`, including the fabricated 1-Tbit/s Unlimited value;
- payload-only derivation of `C0`;
- `max(startup/native, Product)` in request, fixed, and switchable projections;
- relabelling `PATH_CAPACITY` as native carrier authority;
- active-flow division while selecting `C`; and
- the unused ReceiptMode scheduling-rate draft. The actual wire
  `PathCapacityReceipt` remains unchanged and diagnostic.

## Focused proof sequence

Production changes begin only after REDs establish these current failures:

1. Unlimited becomes a finite 1-Tbit/s authority.
2. A configured rate above `2^53` loses integer identity through `f64`.
3. The portable prior is derived from 14,600 payload bytes rather than 14,630
   canonical work bytes.
4. A fresh `PATH_CAPACITY` receipt changes request scheduling `C`.
5. Changing only Product delivery changes request, fixed, or switchable
   carrier `C`.
6. A qualified same-socket TCP sample cannot update a fixed output after its
   attachment snapshot.
7. A wrong-direction, expired, app-limited, partial-window, or predecessor-
   instance TCP sample is rejected.
8. Canonical encoded work for payloads 1 and 14,600 is exactly payload plus 30
   and agrees with unsplit Core codec output.

GREEN requires one shared source reducer and the same result/provenance in
request, fixed response, and switchable response projections. No scheduler
formula, admission, recovery, requalification, dashboard, or lab threshold is
changed in T02.
