# Live-owner frontier: equivalent work-bounded sweep

2026-09-06 17:24 UTC. Existing mixed-path reply/drain stall; bounded
implementation correction, not a new recovery policy. Pending RED/GREEN and
ordinary integration comparison; no global performance acceptance.

## Evidence and origin

`53d9ab5` introduced the uniform live-owner frontier on September 3. Its purpose
is valid: storage chunk boundaries must not change which exact original owners
and accepted-copy identities cover a ranked recovery extent. `bfac5b8` later
preserved that ranked extent through publication; its amplification correction
must remain intact. The frontier implementation collects every range boundary,
then scans every retained span at each boundary. N adjacent chunks of one owner
therefore require N squared coverage checks even though the answer is one
uniform prefix. Fragmentation is ordinary successful traffic, not malformed
input or an assumed exceptional state.

The healthy 500 Mbps mixed-upload relay-stage profile completes in 38.821 s
with a 7.342 s confirmation gap. Preparation before event selection consumes
21.533 s overall and 11.771 s in intervals ending at/after 28 s of process
profile elapsed time (after the 25 s source phase); input ACK
handling consumes only 0.383 s in those tail intervals. Preparation can include
awaits, so this duration alone is not CPU attribution. A separate repeated
run's native worker stack at approximately 29 s catches
`reliable_live_owner_uniform_frontier`, called by completion-tail recovery,
inside that preparation region. Other executor workers are parked. This is
one stack sample, not a statistical profile, but directly confirms the costly
calculation is reachable during the observed tail. The debugger run completes
in 41.724 s with a 7.350 s gap; debugger-paused rates are not acceptance evidence.

The earlier profiles exclude full ACK handling, repair-queue pruning and source
admission as dominant post-source costs. Flight ACK release has substantial
bulk cost, but little tail cost. Do not bundle that separate optimization here.

## Equivalent model, before implementation

Clip spans to requested half-open range [a,b). For each exact identity i and
boundary x, let O_i(x) count covering OriginalData spans, and A_i(x) count all
covering accepted spans. The original-owner set is {i:O_i(x)>0}; the avoidance
set is {i:A_i(x)>0}. These sets are constant between consecutive endpoints.

Process all start/end events at one coordinate atomically. Counts change only
at those events; adjacency of chunks with the same owner must not create a
temporary hole. Freeze the two sets at a, require a nonempty original set,
and stop at the first boundary where either set changes. An incremental count
of membership differences is zero exactly when both sets still match. This
proves the resulting prefix equals the rescan result, including overlapping
copies, owner replacement, coverage holes and arbitrary input order.

Output vectors retain the first segment's input-span order, not hash-map order.
For each original owner, retain the maximum immutable assignment time of all
original spans intersecting the accepted prefix. A span's first intersection
is its clipped start event; fold it only after that event group passes the
membership check. Never fold assignments starting at the rejected boundary.
End events do not remove assignment history from the already accepted prefix.

Sorting at most 2N endpoints costs O(N log N); each span/event is then visited
a constant number of times. Identity-count lookup is expected O(1), using the
existing exact identities' Hash/Eq semantics. The map is never iterated to
choose output order. Memory is O(N+P), with P distinct identities; no persistent
cache, new timer, configuration, score, congestion state or copy permission.

Tradeoff: indexed endpoint records and identity counts have larger temporary
allocation constants than a vector of bare offsets. They remain linear and
are dropped after the call. Small ledgers can incur hash-lookup overhead;
ordinary bidirectional comparisons and RSS observations are therefore still
required. No persistent retention or per-byte state is introduced. The RFC's
ownership/recovery model is unchanged, so an unrelated RFC rewrite is not
justified by this implementation work-bound defect.

This removes repeated reconstruction of the same coverage facts. It does not
shorten the queried extent to a cache chunk, enlarge the ranked repair quantum,
weaken ownership qualification, or prioritize a transport type. Thus neither
the original chunk-independence fix nor the later ranked-extent fix is undone.

## Verification contract

RED: the unchanged rescan plus the test-only visit observation fails with
4,196,352 visits for 2,048 adjacent chunks (2,048 input visits plus 4,194,304
coverage checks). Reproduced with `cargo test --release --locked -j1 --config
'profile.release.package.mptunnel.opt-level=0' --lib
uniform_frontier_span_visits_do_not_multiply_storage_boundaries -- --nocapture`.
This is actual executable work growth, not a timing threshold or a fake path
health criterion. No native/network policy is modified to make the test pass.

GREEN: the sweep needs 12,286 visits for the same 2,048 spans, plus sorting
4,096 endpoint events. All seven focused model tests pass, including exact
output comparison against the independent cell oracle for 4,096 deterministic
overlap/clipping/input-order cases. No wall-clock performance claim follows
from the approximately 342-fold reduction in these counted visits alone.
The affected production callers also pass: 244 sender, 246 relay and 253 stream
tests, using the same locked release/opt-level-zero functional test build.
Ordinary comparisons use the normally optimized binary, without the removed
temporary duration scopes.

1. Count actual span/event visits (not wall-clock timing): 2,048 adjacent spans
   reproduce quadratic coverage work before the change; after it, visits are
   linear apart from endpoint sorting. This is a test-only observation.
2. Keep existing geometry tests; compare exact output, including vector order
   and timestamps, against an independent small integer-cell oracle across
   gaps, overlaps, duplicates, clipping and permutations.
3. Run both request/response recovery callers' focused suites. No diagnostic
   overlay belongs in the component commit.
4. Compare ordinary healthy and adverse mixed traffic in both directions,
   retaining complete bytes, timing series, confirmation gaps, latency and
   resource observations. Component proof is not global closure; any remaining
   stall stays open, without attributing it to a new cause by intuition.

## Ordinary comparison and bounded verdict — 2026-09-06 17:44 UTC

Ten ordinary runs are retained in LIVE_OWNER_FRONTIER_COMPARISON_20260906.json,
including complete probes, all application interval rates, interactive attempt
outcomes and one-second I/O/flight/process observations. Both endpoints use
the same candidate stack; the only algorithmic difference is this sweep.
The lab-diagnostics feature is compiled but diagnostics are disabled. No build
overlaps a network run. Final lint cleanup changes only lazy/eager construction
of the same owned result and test-oracle conditional syntax, not decisions.

| Case | Before | Sweep | Verdict |
| --- | --- | --- | --- |
| Healthy upload, nearby repeat |177.816 Mbps;39.613 s completion;5.185 s max confirmation gap |276.473 /196.768 Mbps;28.775 /28.633 s;2.628 /1.274 s |Complete byte equality throughout; shorter drain in both sweep runs, but bulk rate still varies substantially. |
| Adverse upload |Prior FIN run145.290 Mbps;42.249 s;4.425 s gap |184.379 Mbps;44.885 s;2.487 s gap |Source runs40 s with different accepted amounts; full completion/gap matter. Not a same-byte speed claim. |
| Healthy download |408.541 Mbps;.832 s read gap;loaded success p95 779.5 ms |398.836 Mbps;.343 s;465.9 ms |About2.4% lower bulk, better observed timing; one pair is not a statistical performance guarantee. |
| Adverse download, two runs each |78.953 /48.574 Mbps;4.679 /6.371 s read gaps |76.078 /92.037 Mbps;4.513 /5.165 s |Both fail interactive usability; no release acceptance. |

The first adverse sweep echo timeout begins18.079 s into QoS and closes the
probe connection at21.081 s. Its repeated run times out17.066--20.068 s. The
first control times out during UDP outage30.160--33.163 s, but repeated control
already times out15.226--18.227 s during QoS. Thus the QoS stall predates this
change. These are one timeout followed by `unavailable_after_disconnect` rows,
not independent failures for every later row. Success-only latency percentiles
cannot be used to call the incomplete interactive experience better. Neither
the three-second timeout nor any traffic admission parameter is changed.

RSS is not accepted by the work proof: first sweep upload peaks are790492 KiB
client healthy /461848 KiB adverse, versus earlier FIN552624 /780712 KiB.
The variability neither proves a new sweep leak nor closes existing retention.
Keep the global sustainability gate and raw service observations; no memory
cap is lowered and no adjacent ledger rewrite is bundled here.

Accept only the equivalent work-bound correction: the quadratic algorithm is
reachable, its repeated scans are unnecessary, exact outputs are preserved,
and ordinary upload drain improves without an identified decision change.
Do not promote this into global non-regression or competition acceptance.
The existing adverse QoS/ordered-delivery stall remains the next exact timing
owner to discriminate. Restart/reset attribution, wider aggregation, actual
browser trials, retention and baseline gates remain open as recorded globally.

Local all-target/all-feature Clippy found two lints in this change, corrected
without policy edits. It also found four lints in the separate held companion
stream stack (queue conditional, repair-binding boolean, two eight-argument
server helpers). These are build-quality obligations, not additional runtime
defects or permission to mix companion redesign into this component commit.
