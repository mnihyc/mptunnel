# T03 advisory action score

Status: the pure single-action score is an implemented component checkpoint,
but the v0.4.7 T03 runtime-migration candidate is **rejected**. T02 has
established typed directional rate and normalized MPP work. A post-checkpoint
owner audit proved that the frozen `A=0` order is not a sufficient sustained
allocator and can leave an untested carrier with material independent capacity
idle indefinitely. No T03 runtime owner was migrated; T02b must separately
restore the compatibility scalar that the still-live legacy scorer reads.
This document is not a release, performance, admission, or delivery-time
claim.

## Foreseen oversight: one-action rank is not an allocator

The checked one-action calculation below remains internally valid, but using
it unchanged for every action in a sustained sequence creates a static-priority
dispatcher. For fixed action size and fixed observations, each carrier's score
is constant. Canonical identity makes the result deterministic; it does not
make the allocation fair or capable of discovering aggregate capacity.

For repeated 16-KiB payloads, for example:

```text
P1: R=20 ms,  C=10 Mbit/s   -> S approximately 24 ms
P2: R=100 ms, C=500 Mbit/s  -> S approximately 51 ms
```

Under the rejected owner migration, every action could legally commit to P1
while its Product, queue, and native gates kept reopening. P2 could then
receive no work even though it remained in the same structural tier and could
supply far more service. Filling a configured window is not an allocation
proof: the window may be large, and every Data-ACK-released quantum could
immediately return to P1. Equal scores have the same defect because a fixed
canonical key would replace the former rotating cursor.

The current bounded writer is not the missing allocator. At default limits its
data queue has 131 item slots:

```text
ceil(64 MiB max_path_flight / 512 KiB command payload) + 3 priority slots
```

Normal 64-KiB actions can place about 8.19 MiB in those slots; the item bound
permits about 65.5 MiB at the 512-KiB action cap. More importantly, a slot and
its byte charge are released after native write/flush, before native ACK or
MPP Data ACK. The same static winner can therefore refill each released slot.
Its finite instantaneous envelope does not give a sibling a finite service
opportunity. The 64-MiB per-output Product envelope alone corresponds to about
53.7, 134.2, or 268.4 seconds at 10, 4, or 2 Mbit/s, respectively, before RTT
and recovery delay.

This geometry is intentional resource and pipeline capacity, not allocation
history. Commit `17ae042` first tied queue items to the per-path flight
envelope; `282b8e1` made the calculation payload-aware, `5c43d96` replaced
magic clamps with explicit lane headroom, `b0663c1` enlarged the common
TCP/QUIC envelope for high-BDP paths, and `0ab3fb9` unified its ownership.
Commit `6149e86` retained the charge across dequeue, while `3a6d0ea` tightened
release to successful native write/flush and explicitly kept multiple Product
actors pipelined through one shared writer. Commit `f4206d0` removed a
pressure-derived feed shrink because that second feedback loop could keep a
recovered path idle. Shrinking the queue, releasing at dequeue, installing a
one-action stop-and-wait lease, or feeding less merely to force spillover would
each restore a previously proved defect.

This counterexample does not authorize restoring the old mixed queue/flight,
active-flow, synthetic-horizon, or native-window terms. Those values still
have incompatible ownership and were only incidental spreading mechanisms.
It also does not authorize the rejected receipt-retained all-stage ledger.
The missing concept is a carrier-direction allocation owner shared across
logical streams whose reservation lifetime matches its declared service
boundary without taking Product or native admission authority. Existing
counters cannot be relabelled to manufacture that owner.

Any future profile claiming sustained aggregation must prove all of the
following before runtime code changes:

1. A carrier whose marginal service is unknown cannot be permanently excluded
   solely by a static score; bounded exploration either obtains evidence or
   makes the profile's aggregation nonclaim explicit.
2. When independent marginal service is established, sustained backlog does
   not leave that usable capacity idle. A carrier proved redundant at a shared
   bottleneck need not receive a fixed or proportional share.
3. Sparse and warm-start actions do not inherit old bulk allocation delay.
4. A low-to-high rate publication reprices remaining state without requiring
   a restart or waiting for a duration frozen under the old rate.
5. Portable-rate underestimation, carrier replacement, changing attachment
   subsets, temporary native gates, and failed Apply cannot create unbounded
   phantom debt or catch-up bursts.
6. Concurrent logical streams cannot all observe zero allocation state and
   overbook one carrier before the state changes.
7. Rank remains advisory: the allocator cannot pace, wait, deny, cap Product
   or recovery work, mint native credit, or install a second congestion
   controller. A failed candidate continues through a finite fallback order.
8. Unlimited and unrankable candidates have explicit semantics; neither may
   acquire a fabricated finite rate merely to make a proof pass.

Both initially proposed allocation families are rejected under the current
T02 authority:

- Absolute elapsed virtual work is demand-aware, but portable-rate TCP can
  retain enormous fictitious debt. Two TCP carriers declared at the portable
  approximately 351-Kbit/s rate but jointly serving 300 Mbit/s for ten seconds
  can retain approximately 187.5 MB each, or about 71 minutes of modeled work.
  A newly attached path or replacement can then monopolize until that fiction
  drains; a restart clears it. Common subtraction cannot determine whether the
  common debt was genuinely queued or merely caused by a bad rate.
- A persistent relative virtual-service/stride value proves weighted attempt
  shares for a stable finite set, but it retains irrelevant history across
  sparse traffic, freezes old low-rate duration after recovery, needs a shared
  busy-demand epoch, and needs atomic refundable reservations across streams.
  Charging failed Apply records service that never occurred; resetting on a
  transient gate lets a flapping path erase its history. Candidate-subset-local
  normalization is not a coherent global state.

More fundamentally, strictly rate-proportional weighting cannot repair a
knowingly non-operational rate without independent exploration or evidence. A
default TCP carrier fixed at approximately 351 Kbit/s versus a QUIC carrier
publishing 300 Mbit/s receives about 0.117 percent of a proportional
allocation even when TCP can physically deliver hundreds of Mbit/s. Honoring
that immutable value therefore cannot also establish a different value from
the same allocation loop.

The T08a audit rejects `cwnd/SRTT` as that missing portable authority. The
quotient is congestion-window geometry, not gain-free service: receiver-window
closure, native recovery, full flight, idle restart, and controller-specific
cwnd gain can all make actual service differ while the quotient is unchanged.
The current cross-platform snapshot also lacks one common recovery/send-window
state and a finite publication contract. It may remain a diagnostic proxy but
cannot become `C`.

Wall-time, rate-revalued virtual work is conditionally sound only after a
trustworthy dynamic `C` exists; that prerequisite is absent for portable TCP.
Busy-period WFQ/DRR is also rejected because the runtime has no exact shared
unscheduled-demand owner: guessed reset boundaries either retain stale bulk
history across sparse traffic or reset after every ACK-clocked handoff and
restore static priority. Neither family proceeds to production.

The writer-backpressure candidate is therefore NO-GO. TCP write/flush proves
only acceptance into the OS/TLS socket path, not network service. QUIC accepts
work into stream/connection send state with a 64-MiB connection window; its
logical-stream command writers are also not the physical connection-wide
congestion domain. A per-writer pull token would permit sibling QUIC streams to
overbook that shared domain, while a connection-wide token still cannot turn
native-buffer acceptance into delivery service.

Any later sustained allocator is a separate transaction and needs, before
code:

1. one exact physical-carrier-instance, original-sender-direction allocation
   owner shared by every logical stream using that native serialization domain,
   with subordinate stream credit so one blocked QUIC stream cannot idle the
   connection;
2. an atomic reservation/refund transition, so concurrent streams cannot all
   observe the same uncharged state;
3. a remaining-work lifetime ending at the service boundary represented by
   its rate--native write/flush is not that boundary for a network rate;
4. a trustworthy dynamic TCP service authority or an explicit bounded
   unknown-rate exploration rule--never `cwnd/SRTT` relabelled as service;
5. carrier replacement, temporary gating, rate-change, and sparse-traffic
   reset semantics that cannot retain phantom debt or create a catch-up burst;
6. preservation of high-BDP native pipelining and all Product, resource,
   recovery, and native congestion-control authorities.

No current producer satisfies that contract across TCP and QUIC. T03 runtime
migration closes with no T03 owner change. The sustained-allocation question
is promoted explicitly behind dynamic service discovery instead of remaining
an implicit prerequisite or inviting another queue tweak.

## Final component/runtime boundary

The remainder of this document freezes the **single-action component** that
was reviewed and implemented. R1 and the client-side producer subset of R3
are component checkpoints. R2, R4, R5, the listed owner migration order, and
the former completion rule are withdrawn as runtime instructions: their REDs
remain useful evidence about the legacy projection, but no production owner
may be migrated merely to make them GREEN. Response-direction timing is not
yet a completed R3 slice.

The formulas and ordering below therefore answer only: “given one already
chosen candidate action and coherent inputs, what is its checked advisory
coordinate?” They do not answer: “how should a sustained sequence be divided
among carriers?” Any later allocation model must explicitly supersede this
fence rather than silently interpreting the component score as a dispatcher.

## Decision

The implemented T03 component computes one coordinate for an exact action
within an already selected structural policy tier. No runtime owner consumes
that coordinate. For a possible future profile consumer, no current
runtime producer supplies comparable exact local pre-native predecessor work
for every candidate. The component therefore uniformly omits `A` and computes:

```text
Finite(C):          D_ms = ceil(8000 * M_bytes / C_bits_per_second)
                    S = T + D_ms milliseconds

UnlimitedStartup:   D_ms = 0
                    S = T
```

For ordinary Core data actions with Product payload length `p`:

```text
STREAM_DATA:   M(p) = p + 30 bytes
IP_PACKET:     M(p) = p + 30 bytes
DATAGRAM_DATA: M(p) = p + 34 bytes
```

For `STREAM_DATA`, the 30 bytes are the 10-byte MPTF header, 8-byte stream id,
8-byte offset, and 4-byte payload length. `IP_PACKET` has the same width using
the tunnel id, packet id, and payload length. `DATAGRAM_DATA` adds the flow id,
datagram id, 4-byte TTL, and 4-byte payload length to the header, for 34 bytes.
Carrier-specific re-recording or splitting, native record, encryption,
HTTP/3, QUIC, TCP, and retransmission bytes do not enter `M`. `M` is the
carrier-neutral pre-native projection of one unsplit logical Core action, not
a claim that every adapter emits exactly that physical byte count.

The factor 8000 is dimensional, not a tuning constant:

```text
8 bits/byte * 1000 milliseconds/second = 8000
```

`S` is an advisory local-service coordinate. It does not itself order a live
runtime owner, estimate receiver or application completion, or grant Product
credit, queue capacity, native send credit, pacing, a congestion window,
attachment validity, or recovery authority.

## Exact inputs

The pure component accepts the following already-reduced values for one
action:

- `M`: checked `NormalizedMppWorkBytes` for the exact action. Action-specific
  constructors produce exactly `p+30` for `STREAM_DATA` and `IP_PACKET`, and
  `p+34` for `DATAGRAM_DATA`; raw Product payload is not accepted as `M`.
- `C`: the T02 `DirectionalServiceRate` for the same carrier instance and
  original-sender direction as the action. The pure scorer consumes this
  value; it does not choose, maximize, merge, divide, or reinterpret rate
  sources.
- `T`: the propagation projection defined below, from the exact action's
  validated directional timing view.
- `J`: an optional nonnegative timing variation from the same timing tuple as
  `T`. A present J contributes only to incumbent uncertainty `U`, never to
  `S`; absence uses the timer floor and is not measured zero.
- `K`: the caller-supplied canonical exact action identity defined below.

Product delivery rate, `PATH_CAPACITY`, generic or peer rate, TCP kernel
telemetry, raw pacing, loss, ECN, confidence, application-limited state,
active-flow counts, and health labels cannot replace `C`. That provenance
decision is complete in T02 and is not reopened here.

## Timing projection

Let `R` be the validated SRTT for the exact carrier instance and action
direction. The pure T03 component uses:

```text
T = R / 2
```

Half-SRTT is selected because the runtime has a sender-local round-trip timing
observation but no generally valid synchronized one-way delay. It is a
monotone propagation proxy for an already established output, not a claim that
the forward and reverse Internet paths are symmetric. Handshake time, PTO,
loss recovery, queueing, and jitter are not added to it.

A producer supplying the component must choose one tuple `(R, optional J)`,
not perform independent field-by-field maxima or fallbacks. A live tuple is
usable by the component only when it is:

- finite and nonnegative;
- bound to the exact carrier instance and original-sender direction;
- current for the same attachment/controller activation as the action; and
- published from one coherent timing epoch. A present J must be from that
  tuple; the tuple may legitimately contain no J.

The component accepts that validated tuple as one immutable observation. If a
future T08b owner consumes the component, it must freeze the tuple for one
finite candidate attempt pass; a later timing update would affect the next
pass without invalidating an exact commit whose structural, Product, rate-
authority, and native reservation fences remain current. `T/J` grant no Apply
authority. Such a future owner must not add a timing-generation commit fence
that lets continuously changing measurements force unbounded retries and
break work conservation.

The completed client timing-producer slice uses the configured startup timing
tuple before a valid live R exists. Its omitted configured SRTT uses the
Profile 7 portable 333-ms SRTT; omitted configured jitter remains unavailable,
so the component's `U` uses its one-millisecond timer floor. A valid live R
remains usable when its source cannot publish J; this is required for platform-
neutral TCP because Windows exposes same-socket SRTT but not RTT variation.
Absence is not measured zero and that producer never borrows configured, old,
or other-source J. Malformed present input is not clamped into apparent
validity or mixed into another tuple; rejection preserves the prior accepted
live tuple, or uses a valid exact-scope startup tuple when no live tuple was
accepted. If neither a valid live nor startup R can be represented, the pure
component reports the action as unrankable. Response-direction and other owner
producer behavior remains uncompleted and cannot be inferred from this slice.

Only the positive service duration is rounded to whole milliseconds. `T`
retains the validated duration precision of `R / 2`; it is not independently
rounded upward. An implementation may expose milliseconds for diagnostics,
but its comparison must not pass exact integer rate or service arithmetic
through a lossy floating-point rate.

## Why the component omits `A`

The general RFC formula permits exact local pre-native predecessor work `A`.
The current runtime does not have one comparable producer across request,
response, and packet scheduling candidates:

- carrier queue and native flight are at or below native handoff;
- Product/Data-ACK flight survives after native handoff;
- connection-wide MPP queue may be common to every candidate;
- response `external_flight` is a target-specific projection rather than one
  common pre-native predecessor ledger; and
- the available queue fields have different owners, units, reset points, and
  observation epochs.

Adding or maximizing these fields does not create exact predecessor work. It
can double-count one stage, omit a sibling writer, or compare shared Product
debt with native-carrier debt. Treating a missing value as zero would then
favor the least observable candidate.

The T03 component consequently sets `A = 0` uniformly. Queue, flight, Product,
and native backpressure retain their existing runtime resource/admission owners
and diagnostic visibility; only the isolated component omits them from `S`.
A future profile may add `A` only after one producer proves the same action,
direction, work unit, ownership boundary, and epoch for every candidate in a
comparison.

## Checked arithmetic and rankability

For a finite positive `C`, the service calculation is integer arithmetic:

```text
N = checked_u128(8000) * checked_u128(M)
D_ms = N / C + (N % C != 0)
S = checked_add(T, milliseconds(D_ms))
```

The widened product prevents a valid `u64` `M` from overflowing merely because
of the unit conversion. Conversion to the score's duration representation and
the final addition remain checked. Saturation is forbidden because a saturated
value could tie unrelated actions and appear to win. A positive nonzero amount
of work at a finite rate therefore costs at least one millisecond.

The three rate/arithmetic outcomes are exact:

1. `Finite(C)` uses the formula above. Finite zero is unrepresentable in the
   T02 type.
2. `UnlimitedStartup` contributes zero service duration. It is nonnumeric and
   is never converted to a large finite sentinel.
3. `Unrankable` results when the scorer receives a missing effective input,
   mismatched exact scope, an unrepresentable effective timing tuple or
   normalized action work, or checked arithmetic/duration overflow. A
   malformed raw publication is rejected by its producer before this boundary
   and cannot overwrite a previously accepted effective tuple.

The pure comparator places an unrankable action after every rankable action in
its supplied structural tier, then by `K`. This component result grants no
runtime attempt. A future T08b consumer must preserve structural eligibility
and try an unrankable action when earlier actions fail; only that complete
owner contract could make rank failure distinct from path failure without
turning absent diagnostics into a hidden traffic ban.

Examples that freeze the unit model are:

```text
p = 1 byte, C = 100,000,000 bit/s, R = 100 ms
M = 31 bytes, D = ceil(248,000 / 100,000,000) = 1 ms
S = 50 ms + 1 ms = 51 ms

p = 65,536 bytes, C = 100,000,000 bit/s, R = 100 ms
M = 65,566 bytes, D = ceil(524,528,000 / 100,000,000) = 6 ms
S = 50 ms + 6 ms = 56 ms

p = 4,194,304 bytes, C = 200,000,000 bit/s
M = 4,194,334 bytes, D = 168 ms
R = 20 ms  -> S = 178 ms
R = 80 ms  -> S = 208 ms
```

Changing an active-flow count cannot turn the first 200-Mbit/s carrier into
`C/3` and reverse the last comparison.

## Structural tiers and numeric order

This section freezes an implemented but unconsumed comparator contract. It is
not the order used by current runtime owners; any live adoption requires the
separate T08b allocation model and owner REDs. The component's complete order
is lexicographic, not one blended score:

```text
structural tier
    -> rankable before unrankable
    -> smaller S
    -> evidence-free configured-order coordinate, when applicable
    -> canonical exact action key K
```

In this candidate comparator contract, regular paths precede backup paths
regardless of `S`. Within one usage tier, an operator-marked cheap path
precedes an operator-marked `expensive` path. This is a categorical policy
order, not a claim that cost has a time value. A future reliable-source
consumer would refine its existing four structural tiers as:

```text
nonstale regular cheap
nonstale regular expensive
nonstale backup cheap
nonstale backup expensive
stale regular cheap
stale regular expensive
stale backup cheap
stale backup expensive
```

The candidate contract preserves the stronger pre-existing reliable policy
order: freshness outermost, then usage, then cost. Every nonstale tier would
precede every stale tier; regular would precede backup within one freshness
class; and cheap would precede expensive within one freshness/usage class,
regardless of `S`. A future T08b owner would advance to a later tier only when
the preceding tier had no eligible candidate or every exact commit there
failed. Within a supplied tier, the pure comparator builds the canonical base
order by rankability, `S`, the evidence-free configured-order coordinate where
Section 7.2 applies, and `K`.
Failed or draining carriers, a forbidden command/traffic class, a missing
attachment, exhausted configured Product/queue/session resources, and a
failed exact reservation are structural ineligibility or Apply failure. They
are not large numeric penalties.

Within this candidate contract, `Suspect` is not a structural failure and adds
no time. Its underlying typed timing/rate evidence could change the component
coordinate; terminal lifecycle evidence could change eligibility. The label
itself would do neither by proxy. The live legacy scorer still applies its
existing `Suspect` behavior.

`expensive` likewise has no time unit and adds no milliseconds to the component
`S`. In the candidate contract it is an explicit endpoint-local operator
preference and therefore forms the cheap-before-expensive sub-tier above.
Existing policy that forbids automatic bulk discovery on an expensive path
remains separate. A future consumer would keep an otherwise eligible expensive
action as fallback after cheaper actions in the same freshness/usage class.

## Canonical action identity

`K` is a component input supplied by a caller that knows the whole action. It
contains, in a canonical `Ord` representation:

```text
logical output identity
carrier instance identity
attachment incarnation, where applicable
original-sender direction
command identity
```

For reliable data, command identity includes the command kind, logical stream,
and exact Product range. Reinjection and original transmission remain distinct
when their command identity differs. The concrete tuple may reuse existing
direction-specific identity types, but it must compare their full values; it
must not hash them into a collision-prone shortcut.

A bare reusable `PathId`, configured vector position, cyclic cursor distance,
active-flow count, or input iteration order is not `K`. Those values can change
without changing the action, or remain equal across carrier/attachment
replacement. Consequently the pure scorer cannot manufacture the final tie
key from `PathSnapshot`; any future T08b runtime call site would have to provide
it.

`K` conveys only deterministic identity. It does not imply topology,
independent congestion, capacity, fairness, or preference.

The Section 7.2 configured-order coordinate is not `K` and is not caller input
order. The candidate comparator represents it as a separately declared startup
policy computed from durable configured `(member ordinal, endpoint ordinal)`;
a future T08b consumer would use it only when eligible candidates had otherwise
equal evidence before the final key. This preserves the intended endpoint/
member traversal without claiming that current owners consume the coordinate.

The action part of `K` is frame-specific. Reliable data names stream, exact
range, and original/reinjection cause; L3 names tunnel and packet; application
datagrams name flow and datagram. An acquisition proposal has its own
transaction identity and cannot borrow the identity or `M` of hypothetical
future data.

## Incumbent uncertainty

The pure, unconsumed comparator keeps timing uncertainty separate from the
service score:

```text
J present: U(a) = max(J(a), 1 millisecond)
J absent:  U(a) = 1 millisecond
```

For a supplied rankable incumbent `i` and challenger `c` in the same structural
tier, the pure comparator promotes the challenger only when:

```text
S(i) - S(c) > U(i) + U(c)
```

Equality retains the incumbent. Equivalently, the challenger must satisfy the
strict comparison `S(c) + U(i) + U(c) < S(i)` with checked duration
arithmetic. Raw queue bytes are not tested a second time. `max(J_i, J_c)` is
not the same rule as `U_i + U_c`.

This pairwise deadband is not a sort comparator: it can be non-transitive for
three candidates with different uncertainties. The component first constructs
the canonical base order above and then compares a supplied exact eligible
incumbent only with the base-best challenger in the same structural tier. If
strict displacement fails, its returned order promotes the incumbent and
leaves every other candidate in base order. A future T08b owner would have to
continue that unchanged base order after an incumbent commit failure; the pure
component alone proves neither live fallback nor that retention cannot hide a
challenger.

For the candidate comparator, structural order decides when the supplied tier
changes. Rankable actions precede unrankable actions without fabricating an
uncertainty for the latter; an unrankable incumbent is displaced by a rankable
challenger, a rankable incumbent is not displaced by an unrankable challenger,
and two unrankable actions use the base order. Equal rankable S uses the base
key when no exact incumbent is supplied; incumbent promotion is the one
declared exception. `U` is not a statistical rate-confidence interval and does
not encode a fixed percentage improvement threshold. The stronger future
no-flap question remains assigned to T09.

## Why the former terms existed and why they are excluded

| Former input or override | Original intent | T03 disposition |
| --- | --- | --- |
| Carrier queue/native flight and MPP queue/Data-ACK flight | Approximate predecessor work and head-of-line delay. | No coherent comparable `A` exists; all are omitted uniformly from `S` and retained under their resource owners. |
| Active-flow division of physical capacity | Approximate equal sharing under concurrency. | A flow count neither proves equal backlog nor a scheduler share. Division can create allocation-dependent underfeed; typed `C` is consumed unchanged. |
| Active-flow/PTO additive penalty | Spread cold work and protect latency traffic. | It is an independent demand heuristic with no place in the service formula and can override lower `T`/higher `C`. It is excluded from the pure component. |
| Jitter added to `S` | Avoid unstable paths and reduce flapping. | Jitter is uncertainty, not mean service. It appears only in `U`. |
| Loss/reinjection penalty | Estimate future retransmission cost and avoid lossy paths. | Raw loss is not a deterministic duration and native transport already owns recovery/congestion response. It may validate evidence or trigger reconsideration, but adds no time. |
| Confidence penalty | Distrust immature rate samples. | Confidence belongs to the typed source-validity reducer. Once `C` is selected it cannot be penalized again. |
| `Suspect`/PTO penalty | Move traffic away from an apparently degrading path. | The label is derived, coarse, and direction-sensitive. Valid `T`/`C` or terminal lifecycle state must express the actual effect. |
| Pacing-rate fallback or maximum | Reflect immediate native send intent. | Gain-scaled pacing is not the T02 operational service authority and cannot replace or inflate `C`. |
| Synthetic bulk horizon | Prefer long-run bandwidth for a hypothetical future object. | A rank is for one exact action. Replacing `p` with a larger horizon changes `M` and can reverse the frozen action order. |
| QUIC unused-window/application-limited comparator | Use apparent spare native credit during startup. | It is a second score over native-controller state and can override `S`. Native reservation/backpressure remains authoritative at Apply. |
| Numeric `expensive` delay | Prefer lower-cost paths without forbidding fallback. | Cost has no duration unit. Preserve the intent as a categorical cheap-before-expensive sub-tier; add no fabricated milliseconds. |
| Cursor/input-order tie | Round-robin equal candidates. | It makes equal actions history- or permutation-dependent. Full `K` is the final tie. |
| Response `external_flight` tie | Avoid an output believed to own more work. | It is not comparable exact pre-native `A`; Product/resource owners still enforce its actual bound. |

These component exclusions do not disable native TCP or QUIC congestion
control. They have not removed the same terms from the live legacy scorer;
doing so requires the future T08b owner transaction rather than this component
checkpoint.

## Withdrawn runtime preflight map (historical evidence only)

Line numbers below preserve the evidence that motivated the rejected runtime
plan. They are not implementation instructions. Only the pure component and
independently completed timing-producer slices survive this map; every owner
migration described below is withdrawn and requires a new T08b model and RED.

### Former proposed reliable and L3 owner sites

- One new typed exact-action primitive owns checked score arithmetic,
  rankability, base ordering, and bounded incumbent promotion. The rejected
  plan would have migrated established reliable/L3 owners to it while leaving
  the shared legacy projected-path scorer at deferred transactions.
- `src/runtime/path/model.rs:677-681,986-998` and
  `src/runtime/stream/response/snapshot.rs:350-373`: current fallback reducers
  can combine RTT and variation from different sources or epochs instead of
  selecting one coherent tuple.
- `src/runtime/sender/request/scheduling.rs:137`, `:419-459`, `:857-862`,
  `:1266-1271`, `:1621-1626`, `:1825-1828`, `:1861-1867`, `:1930-1943`, and
  `:2086-2091`: synthetic bulk horizons, cyclic cursor ties, and the QUIC
  unused-window/application-limited comparator override exact next-data rank.
- `src/runtime/sender/request/multipath.rs:3204-3264`: final path choice uses
  cyclic cursor/input position as a tie rather than full exact identity.
- `src/runtime/sender/response/scheduling.rs:330-336`: target
  `external_flight` precedes carrier identity on an equal score, and the
  requested byte count is ranked before connection credit clips the actual
  action.
- Request scheduling currently treats an unrankable nonstale action as absent,
  so a stale rankable action may cross a structural freshness boundary. It
  also projects an exact lower-flight owner down from attachment instance to a
  reusable path key, allowing a replacement attachment to inherit ownership.
- Response scheduling can rank the requested payload before clipping it to the
  actual connection-credit action, and its key-only ties discard physical and
  output incarnation.
- Client L3 fresh ranking uses raw payload, prospective flow load, key/config
  order, and hash-map iteration despite having a full packet attachment.
  Server L3 has the symmetric defects and additionally converts a rank failure
  during Apply into path staleness.

L3 inner-flow affinity is separate state and is not converted into an action
score term. A healthy existing affinity may continue under its own exact
lifecycle; only a future T08b fresh-ranking migration would follow this
contract.

### Sites the rejected plan would have fenced to later transactions

- `src/scheduler/policy.rs:110,165-287` remains the legacy projected-path
  scorer. The rejected plan would have made its scalar rate, queue/flight,
  loss, confidence, flow, Suspect, numeric-expensive, and old deadband behavior
  unreachable from migrated established reliable/L3 exact actions, but no such
  migration is authorized now.
- `src/runtime/path/model.rs:495-558` and
  `src/runtime/path/selection.rs:193,664-713` rank carrier acquisition or
  global admission projections, including UDP loss and synthetic horizons.
  They are not established exact next-data actions.
- `src/model/capacity.rs:336-455` is a pre-frame reliable source/window
  projection. It has no exact action identity and cannot pretend that its
  synthetic quantum is a T03 action; rank-derived denial is T04.
- `src/runtime/path/model.rs:389-456`,
  `src/runtime/path/selection.rs:1052-1188`,
  `src/runtime/datagram/quic.rs:153-175,289-325`, and
  `src/runtime/datagram/association.rs:1046-1118` intertwine established
  `DATAGRAM_DATA` ordering with TTL, setup, session-readiness, or deadline
  decisions. Its checked `p+34` work constructor is model groundwork in T03,
  but this complete runtime chain is deferred intact to T04 rather than
  partially changing deadline behavior.
- Initial `OPEN_STREAM`, additional-carrier opening, recovery-cohort planning,
  zero-payload owner-completion projections, source-window sizing, ACK cadence,
  generic response-feedback preference, inferred BDP limits, and simulator
  hypotheses retain their current owners until independent REDs exist.

The former T03 completion audit quantified the established reliable and L3
sites above plus the pure primitive/timing producers. The list remains only to
prevent those sites from becoming unreviewed leftovers in T08b.

## Boundary retained after rejecting runtime migration

The T03 component changes no advisory runtime calculation or ordering. It also
must not change Product windows, queue/session limits, native writer capacity,
path lifecycle, recovery copy authority, pacing, congestion control,
requalification, or timeout policy.

The isolated typed scorer remains unconsumed. Deferred acquisition, datagram,
deadline, and ordinary owners continue to share `scheduler::score_path` until
their named transactions provide complete replacements. Modifying that helper
as part of this closed component would violate the boundary even if its name
sounds generic. T04b may change only separately proved admission use; T08b owns
any later sustained allocation replacement.

The preflight also found inferred-rate behavior in `src/model/admission.rs`:
completion-horizon/`ecf_no_completion_gain` filtering and truncation,
rate-derived BDP/confidence budgets, and callers that use `score.is_some()` as
eligibility. Those are T04. A future T08b migration may give rank-only callers
an explicit rankable-versus-unrankable result, but neither closed T03 nor T04
may silently broaden or narrow admission. T04 must first prove each reachable
denial with its own exact RED.

Similarly, although the exact `DATAGRAM_DATA` work is `p+34`, score comparisons
used as TTL or delivery-deadline proof are not made valid by correcting `S`;
they remain outside this transaction. `S` is not a deadline theorem. Initial
`OPEN_STREAM` and additional-carrier ordering likewise rank an acquisition
transaction and projected future demand, not one ordinary `STREAM_DATA`
action. T03 does not replace their current projection with the encoded open
frame or with a fabricated data quantum.

Generic response-feedback preference, recovery-cohort planning, zero-payload
owner-completion projections, source-window sizing, ACK cadence, BDP-derived
limits, and global carrier acquisition remain with their owning transactions.
If one of them needs a new structural tier or action model, it requires an
independent RED; removing its behavior as a side effect of T03 would be another
model violation.

## Completed component proof and withdrawn runtime slices

R1 and the client timing-producer subset of R3 are completed component proofs.
R2, R4, R5, and every owner-level part of R3 are retained only as historical
test proposals; they authorize no production edit.

### R1 — completed typed arithmetic and work

- A finite exact `C` and `M=p+30` produce the three numeric examples above.
- Payloads 1 and 14,600 agree with T02's unsplit encoded Core work.
- `UnlimitedStartup` produces `S=T` without a finite sentinel.
- A scorer presented with a missing effective input or rate/timing scope that
  does not match the exact action produces `Unrankable` with no internal
  fallback; work and duration overflow do the same. None becomes 1 bit/s,
  infinity, NaN, or saturation.
- The pure comparator places an unrankable but structurally eligible sole
  action in its returned order; this is not a live runtime attempt.

GREEN is one checked typed score primitive. It consumes
`DirectionalServiceRate` and `NormalizedMppWorkBytes` directly.

### R2 — withdrawn runtime score-input proposal

Hold `T`, `M`, `C`, and `K` fixed, then independently mutate carrier queue,
native flight, MPP queue, Data-ACK flight, pacing, loss, confidence,
application-limited state, active-flow counts, latency-flow counts,
`Suspect`, and `expensive`. `S` must not change and a nonterminal action must
not disappear.

The withdrawn GREEN would have removed legacy additive/divisive terms and set
`A=0` uniformly. It would not have deleted observations or their resource
owners, but the static-winner proof rejects this owner migration.

### R3 — partial component timing proof; owner migration withdrawn

- A live timing tuple with a mismatched carrier, direction, activation, or
  epoch cannot be combined. A live R with absent J remains rankable and does
  not borrow J from another tuple.
- A producer rejects a malformed raw timing publication before replacing
  state: its prior accepted live tuple remains, or exact-scope startup is used
  if no live tuple exists. This producer fallback is distinct from a scorer
  receiving an already-effective wrong-scope tuple, which is unrankable.
- With `J_i=4 ms`, `J_c=3 ms`, `S_i=107 ms`, and `S_c=100 ms`, equality at
  `U_i+U_c=7 ms` retains the incumbent.
- With both timing variations absent or explicitly zero, `S_i=102 ms` versus
  `S_c=100 ms` retains the incumbent, while 103 versus 100 switches.
- Mutating raw queue after scoring cannot veto either result.
- Three candidates with different J values have one permutation-invariant base
  order; U is applied only between the exact incumbent and base-best same-tier
  challenger. The pure comparator returns the unchanged base-best challenger
  after a promoted incumbent; a future owner must separately preserve that
  order after an exact commit failure.

The pure-component GREEN implements the one-millisecond absent-J floor,
`U=max(J,1ms)` for present J, and the strict swap boundary with one coherent
timing tuple. It changes no runtime owner.

### R4 — withdrawn runtime tier/identity proposal

- A regular action precedes a numerically faster backup; backup remains
  available when the regular tier cannot commit.
- A cheap action precedes a numerically faster `expensive` action within the
  same freshness/usage class, while the expensive action remains fallback.
- Reliable source admission orders nonstale before stale, regular before
  backup within one freshness class, and cheap before expensive within one
  freshness/usage class.
- `Suspect` is not a separate tier. Failed/draining and explicit command-class
  restrictions remain structural.
- Equal `S` under every input permutation selects the same full `K`, including
  cases where bare `PathId` is reused across carrier or attachment
  incarnation.
- Equal evidence-free startup candidates preserve the stable configured
  endpoint/member traversal from Section 7.2 without using input order as a
  surrogate.
- Changing cursor, vector position, active load, QUIC unused credit, or
  response external flight cannot change an otherwise equal canonical order.

The withdrawn GREEN would have centralized the lexicographic base comparator
and bounded incumbent promotion. It did not infer topology or fairness from
`K`, but the static final key also supplied no sustained exploration.

### R5 — withdrawn ordinary-owner projection

The withdrawn R5 would have made request original/reinjection, fixed response,
switchable response, and fresh L3 rank each pass with exact action work and a
full caller-owned identity. Reliable callers would no longer have substituted
a bulk horizon; UDP-specific loss additions and QUIC/native-window ordering
would no longer have wrapped the common result.

The typed-work RED proves codec equality for `STREAM_DATA` and `IP_PACKET` at
`p+30`, and records `DATAGRAM_DATA` at `p+34` without changing its T04 TTL or
deadline decision. Acquisition/control callers using a synthetic horizon or
zero payload are explicitly excluded until their owning transaction defines a
complete action model.

Three response-owner REDs are mandatory before that owner changes: connection
credit must first clip the proposed payload and the rank must use that final
exact `p+30` action; and a newer path-proof R with no J must not inherit J from
older liveness or peer/opposite-direction metrics. The latter remains rankable
with the one-millisecond absent-J floor. Separately, when exact local/proof
timing is absent, peer-only R/J must change neither T nor U: the response action
uses its exact local startup tuple rather than opposite-end timing.

The withdrawn GREEN would have required focused owner tests for both
original-sender directions and one input-permutation case per distinct owner.
Existing exact T02 authority fences remain unchanged and must stay GREEN.

The former proposed migration order—pure score/timing, request owner, response
owner, then L3 owners—is superseded at the component boundary. Only the pure
score and independently coherent timing producers may be checkpointed.
Request, response, and L3 owner migration remains prohibited unless T08b
establishes a new sustained-allocation proof. T04 retains its independent
admission/window/TTL scope and cannot be changed as an incidental way to make a
T03 owner RED pass.

## Historical preliminary diagnostic state

The preliminary legacy-policy diagnostic reported 10 passed and 6 failed
before its rejected owner REDs were removed from the active test tree. Five
failures exposed real causes still inherited by established action owners:

- `throughput_scoring_uses_physical_capacity_without_flow_division` — current
  active-flow division gives about 513.316 ms instead of 178 ms;
- `suspect_label_does_not_override_typed_completion_rank` — current bulk
  choice is changed by the label penalty;
- `rfc_service_score_ignores_non_score_observations` — current baseline is
  55.24288 ms instead of the upward-rounded 56 ms and independent fields add
  more time;
- `rfc_service_score_rounds_service_time_up` — current result is 50.00008 ms
  instead of 51 ms; and
- `rfc_incumbent_deadband_sums_uncertainty_without_recounting_queue` — current
  rule uses maximum jitter plus a second queue test instead of `U_i+U_c`.

They are diagnostics, not an instruction to migrate runtime owners: the shared
legacy scorer remains in production after this migration candidate was
rejected. The sixth
failure, `rfc_service_score_ties_use_path_identity_not_input_order`, was itself
an invalid fixture because bare `PathId` is not full action identity. A future
T08b owner migration would need owner-level permutation REDs over complete `K`;
the generic helper must not acquire another incomplete tie merely to revive
that fixture.

Two currently GREEN expectations are stale under the frozen `A=0` decision
and must be rewritten rather than preserved:

- `completion_scoring_counts_queues_but_not_data_ack_ownership`; and
- `ordered_bulk_completion_includes_the_data_ack_frontier`.

Both treat non-comparable queue or post-native Product flight as action-score
work. Their resource/accounting assertions belong in their owning modules,
not in the advisory-score test.

The tests around `effective_path_rate_bps`, `PathRateScope`, and raw pacing are
also legacy scalar plumbing tests. T02 owns typed source selection, while T02b
must retain the compatibility projection read by the live legacy scorer. A
future T08b migration may replace that scorer dependency with direct typed-`C`
owner tests only after its allocation model and REDs authorize the change.

The preliminary diagnostic does not cover scope mismatch, Unlimited, overflow,
coherent optional-J timing, full identity reuse, structural stale tiers,
synthetic bulk horizons, response credit clipping/peer-only timing, or the
outer request/response overrides. The component tests cover R1 and the bounded
client producer subset of R3. The other REDs cannot authorize production
changes until a replacement sustained-allocation contract names their role.

## Symbolic guarantees and nonclaims

For rankable finite actions with fixed `T` and `M`, `C_2 >= C_1 > 0` implies
`S(C_2) <= S(C_1)`. With fixed `T` and `C`, `M_2 >= M_1` implies
`S(M_2) >= S(M_1)`. Lower `T` cannot increase `S`. The ceiling preserves these
monotonic relations and prevents positive finite service from rounding to
zero.

Uniformly omitting `A` proves that T03 does not intentionally count Product,
queue, or native flight twice. The lexicographic base relation gives one
deterministic total order for a finite captured action set. Incumbent retention
is one bounded promotion against the base-best same-tier challenger, never a
pairwise sorting relation, so non-transitive uncertainty cannot create an
ordering cycle. The returned order remains finite after that promotion. It does
not prove live fallback or prevent an owner from turning missing evidence into
a ban; those are obligations for a future T08b consumer. None of these
component facts imposes a rate ceiling or a second congestion controller.

T03 does not prove that half-SRTT equals one-way propagation, that paths have
independent bottlenecks, that a typed rate is statistically accurate, that
recovery is timely, that rate requalification occurs without restart, that
mixed carriers aggregate, or that MPP beats a baseline. T08a, T08b, T09, and the
frozen final matrix own those questions. A focused T03 GREEN is a model
checkpoint, not v0.4.7 acceptance.

## Completion rule

The T03 component checkpoint is complete only for checked arithmetic/work and
each independently reviewed timing-producer slice. It grants no runtime-owner
completion. The current runtime-migration candidate is closed as rejected:
writer backpressure has an exact counterexample, and no replacement allocator
has the authority contract listed above. Fresh owner REDs may be frozen only
in the separately promoted sustained-allocation transaction after dynamic
service discovery closes; they cannot silently reopen T03.

The enumerated acquisition, source projection, application-datagram TTL/
deadline, zero-payload, admission, and inferred-window sites remain unchanged
and assigned to T04 or their named owner. A component checkpoint must not be
released as a scheduling or performance fix.
