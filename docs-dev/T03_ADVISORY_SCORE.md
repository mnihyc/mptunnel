# T03 advisory action score

Status: authoritative internal design for the v0.4.7 T03 transaction. T02 has
already established the typed directional rate and normalized MPP work. This
document freezes the scorer and ordering model before production code changes.
It is not a release, performance, admission, or delivery-time claim.

## Decision

T03 ranks one exact action within an already selected structural policy tier.
For Core Profile 7, no current runtime producer supplies comparable exact
local pre-native predecessor work for every candidate. Therefore `A` is
uniformly omitted and the score is:

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

`S` is an advisory local-service order. It does not estimate receiver or
application completion and does not grant Product credit, queue capacity,
native send credit, pacing, a congestion window, attachment validity, or
recovery authority.

## Exact inputs

One action supplies the following already-reduced values:

- `M`: checked `NormalizedMppWorkBytes` for the exact action. Action-specific
  constructors produce exactly `p+30` for `STREAM_DATA` and `IP_PACKET`, and
  `p+34` for `DATAGRAM_DATA`; raw Product payload is not accepted as `M`.
- `C`: the T02 `DirectionalServiceRate` for the same carrier instance and
  original-sender direction as the action. The scorer consumes this value; it
  does not choose, maximize, merge, divide, or reinterpret rate sources.
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
direction. T03 uses:

```text
T = R / 2
```

Half-SRTT is selected because the runtime has a sender-local round-trip timing
observation but no generally valid synchronized one-way delay. It is a
monotone propagation proxy for an already established output, not a claim that
the forward and reverse Internet paths are symmetric. Handshake time, PTO,
loss recovery, queueing, and jitter are not added to it.

The timing reducer must choose one tuple `(R, optional J)`, not perform
independent field-by-field maxima or fallbacks. A live tuple is usable only
when it is:

- finite and nonnegative;
- bound to the exact carrier instance and original-sender direction;
- current for the same attachment/controller activation as the action; and
- published from one coherent timing epoch. A present J must be from that
  tuple; the tuple may legitimately contain no J.

That validated tuple is frozen in the immutable advisory observation for one
finite candidate attempt pass. A timing update after selection is ordinary
rank staleness: the next pass consumes it, but it does not invalidate an exact
commit whose structural, Product, rate-authority, and native reservation
fences remain current. `T/J` grant no Apply authority. Adding a timing-
generation commit fence would allow continuously changing measurements to
force unbounded retries and violate work conservation.

Before a valid live R exists, the configured startup timing tuple is used. An
omitted configured SRTT uses the Profile 7 portable 333-ms SRTT; an omitted
configured jitter remains unavailable, so `U` uses its one-millisecond timer
floor. A valid live R remains usable when its source cannot publish J; this is
required for platform-neutral TCP because Windows exposes same-socket SRTT but
not RTT variation. Absence is not measured zero and the reducer never borrows
configured, old, or other-source J. Malformed present input is not clamped into
apparent validity or mixed into another tuple; rejection preserves the prior
accepted live tuple, or uses a valid exact-scope startup tuple when no live
tuple was accepted. If neither a valid live nor startup R can be represented,
the action is unrankable.

Only the positive service duration is rounded to whole milliseconds. `T`
retains the validated duration precision of `R / 2`; it is not independently
rounded upward. An implementation may expose milliseconds for diagnostics,
but its comparison must not pass exact integer rate or service arithmetic
through a lossy floating-point rate.

## Why `A` is zero for every Profile 7 comparison

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

T03 consequently sets `A = 0` uniformly. Queue, flight, Product, and native
backpressure retain their existing resource/admission owners and diagnostic
visibility; they simply do not add time to `S`. A future profile may add `A`
only after one producer proves the same action, direction, work unit, ownership
boundary, and epoch for every candidate in a comparison.

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

An unrankable action sorts after every rankable action in its structural tier,
then by `K`. It remains structurally eligible and must still be attempted if
earlier actions fail. Rank failure is not path failure. This is essential for
work conservation and prevents absent diagnostics from becoming a hidden
traffic ban.

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

The complete order is lexicographic, not one blended score:

```text
structural tier
    -> rankable before unrankable
    -> smaller S
    -> evidence-free configured-order coordinate, when applicable
    -> canonical exact action key K
```

Regular paths precede backup paths regardless of `S`. Within one usage tier,
an operator-marked cheap path precedes an operator-marked `expensive` path.
This is a categorical policy order, not a claim that cost has a time value.
Reliable source admission refines its existing four structural tiers as:

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

The stronger pre-existing reliable policies retain their order: freshness is
outermost, then usage, then cost. Every nonstale tier precedes every stale tier;
regular precedes backup within one freshness class; and cheap precedes
expensive within one freshness/usage class, regardless of `S`. The owner
advances to a later tier only when the preceding tier has no eligible candidate
or all exact commits there fail. Within the current tier, T03 builds the
canonical base order by rankability, `S`, the evidence-free configured-order
coordinate where Section 7.2 applies, and `K`.
Failed or draining carriers, a forbidden command/traffic class, a missing
attachment, exhausted configured Product/queue/session resources, and a
failed exact reservation are structural ineligibility or Apply failure. They
are not large numeric penalties.

`Suspect` is not a structural failure and adds no time. Its underlying typed
timing/rate evidence can change the rank; terminal lifecycle evidence can
change eligibility. The label itself cannot do either by proxy.

`expensive` likewise has no time unit and adds no milliseconds to `S`. It is
an explicit endpoint-local operator preference and therefore forms the cheap-
before-expensive sub-tier above. Existing policy that forbids automatic bulk
discovery on an expensive path remains separate. Neither rule removes an
already eligible expensive action: it remains a fallback after cheaper
actions in the same freshness/usage class.

## Canonical action identity

`K` is supplied by the owner that knows the whole action. It contains, in a
canonical `Ord` representation:

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
key from `PathSnapshot`; production call sites must provide it.

`K` conveys only deterministic identity. It does not imply topology,
independent congestion, capacity, fairness, or preference.

The Section 7.2 configured-order coordinate is not `K` and is not caller input
order. It is a separately declared startup policy computed from durable
configured `(member ordinal, endpoint ordinal)`, used only when eligible
candidates have otherwise equal evidence before the final key. T03 preserves
it so redundant members of one endpoint do not displace distinct endpoints at
evidence-free startup.

The action part of `K` is frame-specific. Reliable data names stream, exact
range, and original/reinjection cause; L3 names tunnel and packet; application
datagrams name flow and datagram. An acquisition proposal has its own
transaction identity and cannot borrow the identity or `M` of hypothetical
future data.

## Incumbent uncertainty

Timing uncertainty remains separate from the service score:

```text
J present: U(a) = max(J(a), 1 millisecond)
J absent:  U(a) = 1 millisecond
```

For rankable incumbent `i` and challenger `c` in the same structural tier,
switching is permitted only when:

```text
S(i) - S(c) > U(i) + U(c)
```

Equality retains the incumbent. Equivalently, the challenger must satisfy the
strict comparison `S(c) + U(i) + U(c) < S(i)` with checked duration
arithmetic. Raw queue bytes are not tested a second time. `max(J_i, J_c)` is
not the same rule as `U_i + U_c`.

This pairwise deadband is not a sort comparator: it can be non-transitive for
three candidates with different uncertainties. The owner first constructs the
canonical base order above. It then compares the exact eligible incumbent only
with the base-best challenger in the same structural tier. If strict
displacement fails, the incumbent is promoted to the first attempt; every
other candidate remains in base order. If the incumbent's exact commit fails,
the owner continues that unchanged base order. Retention therefore cannot hide
the challenger or stop fallback.

When structural tier changes, the structural order decides. Rankable actions
precede unrankable actions without fabricating an uncertainty for the latter;
an unrankable incumbent is displaced by a rankable challenger, a rankable
incumbent is not displaced by an unrankable challenger, and two unrankable
actions use the base order. Equal rankable S uses the base key when no exact
incumbent is retained; incumbent promotion is the one declared exception. `U`
is not a statistical rate-confidence interval and does not encode a fixed
percentage improvement threshold. The stronger no-flap question remains
assigned to T09.

## Why the former terms existed and why they are excluded

| Former input or override | Original intent | T03 disposition |
| --- | --- | --- |
| Carrier queue/native flight and MPP queue/Data-ACK flight | Approximate predecessor work and head-of-line delay. | No coherent comparable `A` exists; all are omitted uniformly from `S` and retained under their resource owners. |
| Active-flow division of physical capacity | Approximate equal sharing under concurrency. | A flow count neither proves equal backlog nor a scheduler share. Division can create allocation-dependent underfeed; typed `C` is consumed unchanged. |
| Active-flow/PTO additive penalty | Spread cold work and protect latency traffic. | It is an independent demand heuristic with no place in the service formula and can override lower `T`/higher `C`. Remove it from action rank. |
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

These removals do not disable native TCP or QUIC congestion control. They
remove a second, untyped controller-like ordering layer above those transports.

## Preflight audit map

Line numbers below are audit anchors and may move during implementation. The
map is partitioned so T03 completion cannot silently absorb or claim a later
transaction.

### T03-owned established reliable and L3 action sites

- One new typed exact-action primitive owns checked score arithmetic,
  rankability, base ordering, and bounded incumbent promotion. Established
  reliable/L3 owners migrate to it; it does not replace the shared legacy
  projected-path scorer used by deferred transactions.
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
lifecycle; when fresh ranking occurs, the rank follows this contract.

### Explicitly fenced later-transaction sites

- `src/scheduler/policy.rs:110,165-287` remains the legacy projected-path
  scorer for deferred owners during T03. Its scalar rate, queue/flight, loss,
  confidence, flow, Suspect, numeric-expensive, and old deadband behavior MUST
  NOT remain reachable from a migrated established reliable/L3 exact action,
  but changing the helper itself would alter later transactions prematurely.
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

The T03 completion audit quantifies only the established reliable and L3 sites
above plus the pure primitive/timing producers. Deferred sites must remain
behaviorally unchanged and are separately enumerated so they cannot become
unreviewed leftovers.

## Boundary with T04

T03 changes advisory calculation and ordering only. It must not change Product
windows, queue/session limits, native writer capacity, path lifecycle,
recovery copy authority, pacing, congestion control, requalification, or
timeout policy.

Because deferred acquisition/datagram/deadline owners currently share
`scheduler::score_path`, T03 introduces a separate typed exact-action scorer
and migrates only the T03-owned established reliable/L3 sites. Modifying the
shared helper would violate this boundary even if its old name sounds generic.
T04 may later retire or replace that projection after its own denial/deadline
REDs.

The preflight also found inferred-rate behavior in `src/model/admission.rs`:
completion-horizon/`ecf_no_completion_gain` filtering and truncation,
rate-derived BDP/confidence budgets, and callers that use `score.is_some()` as
eligibility. Those are T04. T03 may give callers an explicit rankable versus
unrankable result so rank-only code can order it correctly, but it must not
silently broaden or narrow admission while closing this transaction. T04 must
first prove each reachable denial with its own exact RED.

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

## Exact RED and GREEN slices

Production changes may start only after the following independent REDs exist.
Each GREEN changes only the named cause.

### R1 — typed arithmetic and work

- A finite exact `C` and `M=p+30` produce the three numeric examples above.
- Payloads 1 and 14,600 agree with T02's unsplit encoded Core work.
- `UnlimitedStartup` produces `S=T` without a finite sentinel.
- A scorer presented with a missing effective input or rate/timing scope that
  does not match the exact action produces `Unrankable` with no internal
  fallback; work and duration overflow do the same. None becomes 1 bit/s,
  infinity, NaN, or saturation.
- An unrankable but structurally eligible sole action remains in the frozen
  attempt order.

GREEN is one checked typed score primitive. It consumes
`DirectionalServiceRate` and `NormalizedMppWorkBytes` directly.

### R2 — exact score inputs

Hold `T`, `M`, `C`, and `K` fixed, then independently mutate carrier queue,
native flight, MPP queue, Data-ACK flight, pacing, loss, confidence,
application-limited state, active-flow counts, latency-flow counts,
`Suspect`, and `expensive`. `S` must not change and a nonterminal action must
not disappear.

GREEN removes the legacy additive/divisive terms and sets `A=0` uniformly. It
does not delete the observations or their resource owners.

### R3 — timing and retention

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
  challenger. If the promoted incumbent's exact commit fails, the unchanged
  base-best challenger is attempted next.

GREEN implements the one-millisecond absent-J floor, `U=max(J,1ms)` for present
J, and the strict swap boundary with one coherent timing tuple.

### R4 — structural tier and identity order

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

GREEN centralizes the lexicographic base comparator and the separate bounded
incumbent-promotion operation, or makes every owner use those same typed
operations. It does not infer topology or fairness from `K`.

### R5 — exact ordinary action projection at each owner

Request original/reinjection, fixed response, switchable response, and fresh
L3 ranking each pass the exact action work and full caller-owned identity.
Reliable callers no longer substitute a bulk horizon. UDP-specific loss
additions and QUIC/native-window ordering no longer wrap the common result.

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

GREEN requires focused owner tests for both original-sender directions and
one input-permutation case per distinct ordering owner. Existing exact T02
authority fences remain unchanged and must stay GREEN.

The bounded production order is the pure typed score and coherent timing
tuple, then the exact request owner, the exact response owner, and finally the
two L3 owners. T04 admission/window/TTL work starts only after those slices are
independently GREEN. This keeps one observed failure paired with one causal
change and prevents acquisition heuristics from being silently changed with
ordinary-data rank.

## Known current test state

The preliminary legacy-policy diagnostic currently reports 10 passed and 6
failed. Five failures expose real causes inherited today by established action
owners:

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

They are diagnostics, not the final T03 RED slice: the shared legacy scorer is
intentionally preserved for fenced later transactions, while established
reliable/L3 owners migrate to the separate exact-action primitive. The sixth
failure, `rfc_service_score_ties_use_path_identity_not_input_order`, is itself
an invalid fixture because bare `PathId` is not full action identity. It must
be replaced by owner-level permutation REDs over complete `K`, not made GREEN
by teaching the generic helper another incomplete tie.

Two currently GREEN expectations are stale under the frozen `A=0` decision
and must be rewritten rather than preserved:

- `completion_scoring_counts_queues_but_not_data_ack_ownership`; and
- `ordered_bulk_completion_includes_the_data_ack_frontier`.

Both treat non-comparable queue or post-native Product flight as action-score
work. Their resource/accounting assertions belong in their owning modules,
not in the advisory-score test.

The tests around `effective_path_rate_bps`, `PathRateScope`, and raw pacing are
also legacy scalar plumbing tests. T02 already owns source selection. T03 must
replace their scorer dependency with direct typed-`C` tests rather than keep a
second effective-rate reducer for test compatibility.

The preliminary diagnostic does not cover scope mismatch, Unlimited, overflow,
coherent optional-J timing, full identity reuse, structural stale tiers,
synthetic bulk horizons, response credit clipping/peer-only timing, or the
outer request/response overrides. R1--R5 require those independent REDs before
the corresponding production sites change.

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
ordering cycle. If that attempt fails, resuming the unchanged base order proves
finite fallback. Retaining unrankable actions at the end of a structural tier
prevents missing advisory evidence from becoming a permanent ban. None of
these facts imposes a rate ceiling or a second congestion controller.

T03 does not prove that half-SRTT equals one-way propagation, that paths have
independent bottlenecks, that a typed rate is statistically accurate, that
recovery is timely, that rate requalification occurs without restart, that
mixed carriers aggregate, or that MPP beats a baseline. T08, T09, and the
frozen final matrix own those questions. A focused T03 GREEN is a model
checkpoint, not v0.4.7 acceptance.

## Completion rule

T03 is complete only when all R1--R5 REDs fail for their stated cause, one
bounded implementation makes them GREEN, T02 authority/fence tests remain
GREEN, and an independent audit finds no score override in the T03-owned
established reliable/L3 map. The enumerated acquisition, source projection,
application-datagram TTL/deadline, zero-payload, and inferred-window sites must
remain unchanged and explicitly assigned to T04 or their named owner.
Admission behavior is neither accepted nor repaired here. The resulting
checkpoint proceeds immediately to T04 and must not be released by itself.
