# Native packet evidence follows transport lifetime

2026-09-06 18:29 UTC. Existing post-QoS QUIC collapse; bounded model proposal
before changing runtime behavior. Not a release acceptance.

## Proven mismatch and original intent

The exact bound-action trace contains124 lower-bound calls,62 with raw
unknown-evidence authority and62 with compensated-budget authority. Every raw
call has `unknown=true` and `explicit=false`; it is not ECN or an explicit
persistent-congestion signal. The final observed reductions include
63,830→44,681→31,276 bytes before additional budget-driven reductions. The
complete727 native events and probe are preserved in
QOS_NATIVE_BOUND_ACTIONS_20260906.json. Diagnostic throughput is not an
acceptance record.

BBR3 independently marks an outstanding send snapshot stale after ten
delivery rounds, then removes it at the next ACK. The transport may still
own that packet: after QoS the learned loss deadline can exceed a second
while younger originals complete40--80ms delivery rounds. The later loss
callback cannot recover its paired send counters, so MPP's missing-evidence
rule bypasses compensation and conservatively reduces the native model.
Later live ACKs can also lose their rate-sampling snapshot. Raising the ten-
round number or relaxing unknown-evidence handling would hide, not fix, this
ownership mismatch.

The expiry came from the [maintained BBR3 PR source at e19f9e25](https://raw.githubusercontent.com/quinn-rs/quinn/e19f9e25/quinn-proto/src/congestion/bbr3/mod.rs),
imported by local d5a7413. Its purpose was bounded tracking under the assumption
that native loss detection would already have retired such old packets.
That is a heuristic, not a packet-lifetime guarantee. The held adaptive
reordering model makes this assumption fail in an observed configuration.
Do not label this a general official Quinn release defect: this controller
came from a PR, and MPP owns the composition and missing-evidence response.

A targeted production-controller replay sends one original, retains it in
flight, and completes16 younger40ms delivery rounds using actual callbacks.
It never writes the round counter or sends more than the minimum window.
The delayed loss is RED: it returns no recovery transaction because its
snapshot has disappeared. The same replay also checks the delayed-live-ACK
branch after the correction. This is a component counterexample compatible
with the measured delayed detector, not by itself a complete packet-engine
or end-to-end recovery proof.

The counterexample has a simple general form. Keep original A unresolved
until native deadline D. Send one younger B_i after each preceding B ACK;
each ACK can advance the delivery-round frontier while A stays unresolved.
With younger RTT r, approximately D/r delivery rounds fit inside A's valid
lifetime. QUIC's selective ACKs do not require A to arrive before those rounds
advance. No model constraint bounds D/r by ten: the observed >1.1s learned
excess and40--80ms younger RTT readily exceed it. Only two packets need be
in flight, so this is not an overload or excessive-concurrency premise.

## Proposed invariant and smallest complete correction

For each extant controller lineage copy, a live transport packet which that
copy observed at send time retains its immutable send snapshot until the
transport supplies a terminal fact. Delivery-round age is not such a fact.

- Delete the independent ten-round expiry rule and its constant.
- Keep current ACK-batch retention through post-ACK ECN processing. Current
  ACK and actual loss callbacks keep their existing evidence/model semantics.
- Add a storage-only discard notification for packets removed without a
  controller feedback callback: key-space discard, Retry/0-RTT rejection,
  MTU-probe abandonment and feedback suppressed during path validation.
- Send that storage terminal to matching parked migration copies as well.
  They must not retain dead packets until rollback, and must not receive a
  synthetic ACK, loss, congestion response or bandwidth sample.
- MPP's instrumentation wrapper must forward the storage notification without
  changing delivery/loss counters or manufacturing an operational revision.

The removal sites, not a timeout scan, own cleanup. The exact ACK callback's
active controller is excluded from immediate metadata discard so same-ACK
ECN still finds its send snapshot; the next ACK's existing reclamation deletes
that completed batch. Genuine loss, explicit congestion, unknown evidence
that is actually unavailable, journal bounds and transaction-scoped undo
remain unchanged. The learned reordering algorithm is unchanged by this fix.

## Work/storage argument and risks

Each snapshot corresponds to either a transport-live ack-eliciting packet or
the active controller's most recent ACK batch. A parked clone has only the
subset present at cloning; terminal facts remove that subset as well. Thus
retention is O(live native flight + one ACK batch) per extant controller copy,
not O(connection lifetime). No new data buffer or controller history is added.
All transport removal paths must be covered for this argument to hold.
This is a live-entry bound, not an instantaneous process-RSS bound: the existing
VecDeque may keep allocation capacity from peak live flight, as before. This
correction does not introduce a new allocator-shrinking policy or establish
closure of the separately reported random RAM incident.

Retaining genuinely live delayed packets can use more metadata than silently
discarding their evidence. That is required for correct classification; it
does not authorize more flight or raise any congestion window. Existing
native flight and connection lifetime still own that work. Removing the old
round-age scan predicate also avoids treating packet-timed rounds as elapsed
time. The existing packet container/lookup model is not redesigned here.

## Bounded validation

Run the delayed live loss/ACK counterexample, discard identity/idempotence,
current-ACK ECN ordering, parked clone and unrelated-epoch checks. Verify
actual transport discard producers and wrapper forwarding; then the native
suite and affected adapter tests. Ordinary jittered/no-jitter QoS recovery,
steady loss/jitter and mixed cases must show timing-series and latency, not
only a high average. If they remain poor, keep the exact component proof and
attribute the remaining decision class; do not call every budget response a
bug or alter its parameters. Global startup/upload, reset, retention, browser,
aggregation and matched-baseline gates stay open.

## Component verification — 18:37 UTC

The delayed live-packet test is GREEN after removing the independent age
expiry. All466 native library tests pass, including explicit clone/epoch/
packet-space/idempotent cleanup, actual lost-MTU-probe dispatch and existing
ECN, spurious undo, migration, Retry and0-RTT coverage. The targeted MPP
instrumentation forwarding test passes and observes no extra delivery/loss
telemetry from discard. All 25 adapter tests pass. The ordinary optimized
build and all-target/all-feature Clippy with warnings denied also pass.
No performance acceptance or release follows merely from these component
results. No temporary native trace remains in the candidate source.

## Ordinary comparison — 2026-09-06 19:04 UTC

NATIVE_PACKET_LIFETIME_COMPARISON_20260906.json preserves 15 full probes and
one-second application, native flight, router and process observations. The
control and candidate include the same held companion/native stack; the
packet-lifetime correction is their behavioral difference. These runs do not
prove acceptance of that surrounding stack or of this candidate for release.

The QoS profile is an asymmetric 500-Mbps path, downshifted to 10 Mbps from
15 to 25 seconds, with changing 3--10% downstream loss (mean 6%) and directional
jitter. There is no UDP blackhole in this discriminator. Post-QoS QUIC service
over seconds 25--40 improves from 50.391 Mbps in the fresh control (30.860 in
the earlier control) to 156.335 and 143.228 Mbps. Whole-run candidate rates
are 115.406 and 67.265 Mbps: the second has a slower startup despite stronger
recovery. Do not substitute recovery speed for full-run stability. Mixed mode
still has low-service intervals and remains unaccepted. All QoS runs lose the
first echo connection to the existing 3-second timeout during downshift;
later unavailable slots are not independent failed requests.

Steady 25-second loss/jitter ablations, without downshift or blackhole:

| Mode / build | Mean Mbps, two runs | Maximum read gap, seconds | Failed echo requests |
| --- | --- | --- | --- |
| QUIC control | 196.568 / 160.626 | 0.217 / 0.290 | 0 / 0 |
| QUIC candidate | 177.882 / 171.794 | 0.290 / 0.316 | 0 / 0 |
| Mixed control | 206.145 / 144.812 | 0.419 / 0.508 | 0 / 0 |
| Mixed candidate | 166.175 / 167.464 | 0.320 / 0.565 | 0 / 0 |

The direction of the rate difference reverses across repeats. This neither
establishes a small gain nor causally attributes a steady regression. All
scheduled steady echo requests succeed; mixed candidate repeat p95/max are
404/825 ms versus control 284/867 ms, so latency is not uniformly improved.
The no-jitter QoS comparison is immediately useful after recovery in both
builds (candidate/control post-recovery 413.643/448.589 Mbps); its long gap is
during the physical downshift. No claim of exact non-regression follows.

Verdict: packet-lifetime correctness is established; QoS recovery benefit is
observed, but practical non-regression and global usability remain open.
Checkpoint this bounded correction as a candidate, not an accepted release.
The next exact discriminator reuses the archived bound-action observations
to check whether unknown-evidence reductions disappear after the correction.
It must separately classify remaining compensated-budget actions and mixed
ordered-progress stalls. No packet-container, gain, timeout or compensation
change is justified by the variable steady averages alone.

## Causal follow-up — 2026-09-06 19:14 UTC

Reusing the archived BBR3-only observations on the candidate yields zero raw
or unknown-evidence lower-bound actions in both runs: 141 budget-only actions
in QUIC and 95 in mixed mode, versus 62 raw/unknown actions in the old QUIC
trace. This confirms removal of the observed missing-send-evidence decision
class in the tested composition. NATIVE_PACKET_LIFETIME_CAUSAL_FOLLOWUP_20260906
preserves all 749/585 server events and full probes. The diagnostic binary is
separate; all temporary source hooks are removed again.

These diagnostic runs still have 3.771/3.597-second maximum read gaps; mixed
delivery remains uneven after recovery. Their throughput is not an acceptance
comparison. Among completed QUIC budget epochs at model-clock seconds 2--15,
ordinary declared loss is 18.33% of delivered-plus-lost volume; at seconds
27--41 it is 14.08%. Those are controller declaration populations, not measured
physical erasure fractions. They cannot alone establish false loss: packet
timing, bursts and qdisc aggregation must not be silently equated.

The remaining classification question is narrower: the transport recognizes
individual late originals, while compensation reclassification is notified
only when an entire recovery transaction completes. Keeping native undo
transaction-wide protects genuine coexisting loss. Whether applying that
same condition to each compensation record discards useful packet truth
requires a mixed real-loss/late-original counterexample. No new policy is
accepted from this source observation alone. Retained-proof expiry is another
existing authority; do not extend it speculatively.
