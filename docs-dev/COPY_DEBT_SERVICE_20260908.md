# Exact copy-debt accounting and ordinary service

Recorded:2026-09-08 14:39 +08:00. Category: bounded work correction and practical
verification. No release or general performance acceptance.

## Why this correction

The [winning-reply trace](PREPARED_REPLY_SERVICE_20260908.md) locates2.226s
after authenticated TCP decode before ordered mux delivery. Its surrounding
one-second recovery-dispatch elapsed buckets are.846837/.869930s. Other long
gaps occur before publication/decode; this cost does not explain all pauses.
Product debt changes6,068,248B inside the sampled held interval, ruling out a
blanket assumption of unchanged recovery state across actor turns.

The precise avoidable work is narrower. `reinjected_data_in_flight_bytes(i)`
scanned every retained Original and copy flight for each candidate target,
including final Apply. Its additive accounting predates the recent structural
ordering correction at1436ff4 and exists in3a6d0ea. The intention is correct:
accepted copies must consume their exact target's recovery allowance until
Product acknowledgement, regardless of native queue drain or suppression
deadline expiry. Recomputing that same quantity from unrelated Original ranges
is not required by this contract. Exact-offset sent-instance lookup, Native
observation, qualification and queue-accounting costs are separate and unchanged.

## Model and equivalence

For exact attachment i, J_i is the sum of byte lengths of all retained
ReinjectedData transmissions on i, including overlapping copies separately.
It is neither covered-byte union nor currently attached-only traffic. The
serialized ledger has exactly three byte-mutation boundaries:

1. Accept a copy of q bytes: J_i becomes J_i+q.
2. Release a Product ACK atom intersecting a retained copy: subtract that
   copy's released bytes; each retained split keeps its previous contribution.
3. Drain the ledger: clear the exact per-instance totals along with flights.

Empty initialization and partition conservation prove equality to the old
full scan after every operation. Repeated ACKs release nothing twice. Deadline
aging and evidence invalidation change no bytes. Replacement instances have
distinct keys; predecessor debt is not transferred or silently erased. Keys
are removed at zero; live entry count is bounded by retained copy-owner count.
Hash-map capacity can retain its peak allocation, as the existing Original map
does; this is not historical unbounded key accumulation.

Final admission already requires q<=cap−sat(J_i+queued_i), where cap is a usize.
Thus every legally accepted J_i+q<=cap<=usize::MAX, even if a later cap shrinks.
Checked u64 addition/subtraction preserves this invariant on supported32/64-bit
targets. A maintained saturated total would not generally preserve subtraction
after overflow, so it is deliberately not used. The existing usize return
conversion is retained. No new limit or fabricated overflow scenario is added.

The correction9ea25e2 maintains this one exact aggregate beside the existing
Original aggregate. Query work becomes one indexed lookup, not O(flights).
Costs are an indexed update per accepted copy/released atom and one entry per
retained copy-owner instance. No Native snapshot, queue credit, eligibility,
byte-order cursor, deadline, congestion setting or frame quantum is cached or
changed. RFC15's quantity and current Apply remain valid; no RFC policy change
is warranted for an equivalent accounting implementation.

A separate audited shortcut is deliberately excluded: one shared Native
observation across all target evaluations is coherent Observe–Decide design,
but not equivalent to today's resampling when a target changes mid-selection.
Fresh final Apply can reject an invalid target but cannot recover a better
alternative missing from an older view. There is no demonstrated practical
need or accepted correction for that policy in this transaction.

## Actual RED and preservation checks

Checkpoint3396087 retains the pre-fix control and failing test. The production
send cache and exact Original-flight producer create69632 retained bytes on A:
a4096-byte head plus65536-byte suffix. One case stores the suffix as one legal
frame; the other uses64 legal1024-byte frames. A becomes stale; fresh C keeps
the same unmodified default admission. The actual structural dispatcher and
actual target command receiver both publish exactly[0,4096), retain the same
source/F and charge exactly4096 accepted-copy/optional bytes.

Only after those semantic controls pass does the work assertion fail:
two copy-debt queries visit4 versus130 flight records. The test does not claim
Original packets traversed a real network. After the correction both actual
queries use the index with no flight iteration. This proves deletion of the
identified irrelevant work, not a32.5x runtime or end-to-end speedup.

One independent full-scan lifecycle oracle checks the whole aggregate map:
repeated/overlapping accepted copies, exact replacement identity, middle and
disjoint ACK splits, repeated ACKs, expiry retaining debt, zero-key removal and
reuse, final drain release totals and repeated-empty drain. Existing global
recovery ordering, independent-target service, changed Native authority,
selected-instance removal and actual TCP/QUIC EOF controls remain unchanged.

RED and GREEN builds each take1m13s, warning-free. RED:1control passes and1
intended work assertion fails. GREEN:538 focused checks pass1.28s. Two independent
source/diff reviews accept the conservation and opposite-case coverage. The
scoped build/test logs are retained with the ordinary raw evidence below.

## Planned ordinary discriminator

One normal optimized9ea25e2 build; then candidate first and frozen ordinary
b3dfef1 parent second, both endpoints using their cell binary. No diagnostics,
build overlap, harness/profile change or favorable third trial. This reverses
the earlier pair order. Use the same mixed-upload500Mbps asymmetric changing
loss/jitter,10Mbps QoS15–25s, UDP outage30–33s,40s offered load and existing
observation boundaries. Exact profile is retained in CURRENT_CLOSURE_PLAN.

Compare exact completion, first confirmation/write, all timing bins, maximum
gaps, phase-specific S/T/Rs/Rc progress and CPU/RSS/wire costs. Random packet
realizations and unequal accepted work remain limits; the earlier failed
ordinary result is not replaced by a favorable average. Any remaining severe
stall stops practical promotion. Results will be recorded here after execution.

## Executed ordinary pair — practical progress, no promotion

Normal build: warning-free3m29s. Candidate first, parent second, without
diagnostics/build overlap. Raw cells are
`./.tmp/reflection/results/mixed-combined-up-copy-debt-{candidate,parent}-0908/`.
[All raw outputs and build/test/run logs](COPY_DEBT_SERVICE_20260908.raw.tar.gz)
are preserved, including zeros, failure and untrimmed telemetry.

| Observation | Parent b3dfef1 | Candidate9ea25e2 |
| --- | ---: | ---: |
| Runner / exact completion |Guard / incomplete |0 / complete |
| Confirmed / accepted bytes |93570384 /180748288 |254083072 /254083072 |
| Probe elapsed seconds |85.948541 |48.973579 |
| Reported Mbps |8.709 incomplete |41.505 complete |
| First confirmation seconds |.467841 |.397566 |
| Maximum confirmation gap seconds |66.779472 |11.042148 |
| First local write seconds |.154499 |.128872 |
| Maximum local write gap seconds |15.796057 |1.011139 |

Parent guard-triggered teardown produces the probe reset, not an independent
network-reset observation. ACK accounting is invalid and raw confirmation
bins are absent. Do not divide partial parent Mbps into candidate Mbps as a
speedup, reconstruct its bins, or call observation-boundary failure completion.

S=consumed client source, T=ordered server target-socket acceptance, Rs/Rc=
server-read/client-written reply bytes. These are not claimed C or mux F.
Endpoint-generated timestamps give sampled hold lower bounds:

| Held counter | Parent Unix-ms interval / duration | Candidate Unix-ms interval / duration |
| --- | --- | --- |
|Rc |261;1788849886556–1788849952557 /66.001s |433;1788849816832–1788849826833 /10.001s |
|T |157295376;1788849901558–1788849961558 /60.000s |230311809;1788849821825–1788849830825 /9.000s |

Parent also has an8s terminal sampled Rc274 hold. Candidate Rc433 spans
nominal31.09–41.14s while T224472641→230311809 and Rs1021→1049: held return
work coexists with forward/response production. Candidate's separate target
plateau spans36.14–45.14s. An opposite early phase remains: at3s, parent
T49500656 exceeds candidate2621281; at10s candidate176589825 exceeds parent
129639184. No uniformly better phase history is proved.

Stable client TCP ports: parent36444/36456/36452, candidate51906/51914/51892.

| Sampled native service / cost | Parent | Candidate |
| --- | ---: | ---: |
| Peak / final aggregate client TCP Recv-Q B |1573897 /1525708 |1429228 /0 |
| TCP consumption delta, nominal10–48s, B |103909 |2591373 |
| Upload class bytes / packets / drops |221912331 /176466 /3032 |347677613 /279283 /4682 |
| Return class bytes / packets / drops |12645820 /51883 /738 |15013619 /68020 /1055 |
| Peak client / server RSS KiB |511988 /90968 |677496 /129508 |
| Client CPU percent peak / final |115 /102 |103 /99.6 |
| Server CPU percent peak / final |37.4 /4.6 |35.4 /10.4 |

Native consumption is change in bytes_received−Recv-Q on stable sockets, not
logical payload goodput. Candidate backlog empties at46s alongside Rc503→1063;
during31–41s consumption advances490147B and sampled server Send-Q is zero
at both boundaries. Parent instead retains744157B server Send-Q at85s with
continuing receive-window limitation.

Candidate RSS is higher. Work and duration differ, preventing normalized
per-byte cost conclusions. CPU is ps lifetime-average percentage, not interval
utilization or handler cost. Router class1:10 eth1=upload, eth0=return includes
control, retransmission and copies; not repair-only bytes or summed qdiscs.

### Full candidate timing series

Raw one-second confirmation bins including zeros and the final partial bin's
reported value.556.890Mbps is buffered confirmation observation, not a500Mbps
wire violation. Parent raw bins are unavailable, not zero.

| Bin end (s) | Confirmed Mbps |
| --- | ---: |
|1|3.144|
|2|8.389|
|3|8.389|
|4|5.767|
|5|556.890|
|6|26.118|
|7|235.265|
|8|27.071|
|9|0.000|
|10|0.000|
|11|70.683|
|12|0.000|
|13|33.126|
|14|0.000|
|15|24.738|
|16|0.000|
|17|22.877|
|18|0.000|
|19|26.598|
|20|0.000|
|21|56.096|
|22|0.000|
|23|0.000|
|24|0.000|
|25|0.000|
|26|113.773|
|27|0.332|
|28|0.000|
|29|0.000|
|30|0.000|
|31|86.463|
|32|0.000|
|33|0.000|
|34|0.000|
|35|0.000|
|36|0.000|
|37|0.000|
|38|0.000|
|39|0.000|
|40|0.000|
|41|0.000|
|42|75.778|
|43|0.000|
|44|0.000|
|45|47.710|
|46|413.523|
|47|0.857|
|48|0.192|
|49|188.885|

### Disposition

The pair demonstrates better settlement and shorter holds, alongside adverse
early delivery and absolute RSS. The exact scan deletion is independently
proved; random realizations/unequal work prevent attributing every difference
to it.11.042s confirmation gaps and9s target holds remain unacceptable.
No release promotion or favorable third ordinary run.

Next locate the actual winning missing reply in a fresh sparse diagnostic on
9ea25e2; do not transplant older diagnostic attribution. Reuse reply-stage joins
and aggregate costs, adding nested recovery range/selection, Native resolution,
Product projection, queued-copy debt, Apply and fenced-bookkeeping scopes.
No per-attempt logs, profile change or policy correction. Prepublication or
predecode gaps must not be assigned wholesale to client accounting; diagnostic
Mbps does not establish ordinary acceptance.
