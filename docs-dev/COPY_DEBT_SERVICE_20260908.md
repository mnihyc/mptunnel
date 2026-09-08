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

## Executed diagnostic — exact winning response joins

Source9ea25e2 plus the temporary
[COPY_DEBT_SERVICE_TRACE overlay](COPY_DEBT_SERVICE_TRACE_20260908.patch).
The diagnostic build took3m33s; hooks were archived and reversed before the
single capture. The completed cell is
`./.tmp/reflection/results/mixed-combined-up-copy-debt-service-0908/`;
[full diagnostic raw evidence](COPY_DEBT_SERVICE_20260908.diagnostic.raw.tar.gz)
is preserved separately from the ordinary pair.

Runner exits0, with exact366018560B in43.726528s and maximum confirmation
gap4.375291s. This does not reproduce the ordinary11.042148s gap and is not
performance acceptance. C/S below denote one-based client.log/server.log
lines, not counters. Session5583289822543737970 and logical stream0 persist;
client PID267927/server PID273959. Unless a full timestamp is shown, table
times are Unix milliseconds minus1788850700000, a common wall-clock domain
rather than process-relative clocks.

### Complete frontier coverage and exact identity

All73 frontier-advancing applications among231 mux applications join to their
sender publication, local native write, decode, routing, shared input and mux
acceptance. They advance F through1156 and comprise62 QUIC-Original triggers,
1 QUIC-repair,2 TCP-Original and8 TCP-repair triggers. Six triggers also release
previously buffered suffixes: F237→263,276→317,345→401,401→429,429→485 and
485→513. Nonadvancing applications are therefore not all losing duplicates;
some previously installed the suffix that a later head unlocks. Delay claims
below follow the actual frontier-triggering copy, not a late duplicate maximum.

| TCP wire PathId | Client runtime index | Client physical instance | Attachment |
| --- | ---: | ---: | ---: |
|0 |2 |4 |2 |
|1 |0 |2 |0 |
|2 |1 |3 |3 |

Initial authenticated-decode→route joins establish this translation at
C14–15, C29–30 and C98–99. QUIC is runtime0/physical1/attachment1; its ordinary
H3 request ID is4 and repair request ID is8. Endpoint physical-identity
namespaces remain distinct even when numeric values coincide. Initial
management membership independently retains the same session and wire IDs.

Publication→native-write completion is at most4ms for these winners. No
server publication repeats an exact path/stream/range key. One final decoded
interval is coalesced: client[1129,1156), C3187@57046→C3230@57141, covers two
adjacent Original publications[1129,1143) and[1143,1156), S948–949@57014, with
write begin/end S950–953@57014. Its route is C3206@57090, shared send
C3223–3224@57132 and dequeue C3229@57141. This is complete range coverage,
not a missing publication or an invented single27-byte sender command.

### Largest frontier holds select different service stages

| Held F / complete gap | Winning publication / write completion | Decode → mux | Supported split |
| --- | --- | --- | --- |
|639 /4374ms, C1652@29099→C1856@33473 |QUIC Original S566–568@30531 |C1849@33339→C1856@33473 |1432ms before publication;2808ms write→decode;134ms postdecode |
|68 /3240ms, C134@15260→C311@18500 |QUIC Original S51–53@15468/15469 |C264@18208→C311@18500 |208ms before publication;1ms to write completion;2739ms predecode;292ms postdecode |
|821 /3165ms, C2533@48287→C2645@51452 |QUIC Original S662–664@42390/42394 |C2638@51020→C2645@51452 |Publication precedes this frontier;2733ms of its hold is predecode and432ms postdecode |
|737 /2205ms, C2077@39712→C2193@41917 |QUIC Original S608–610@39908/39909 |C2116@40714→C2193@41917 |196ms before publication;1ms to write completion;805ms predecode;1203ms already-decoded current-frontier delay |
|681 /1754ms, C1929@36176→C1981@37930 |QUIC Original S575–577@37852 |C1974@37888→C1981@37930 |1676ms before publication;36ms predecode;42ms postdecode |

For the strongest same-frontier postdecode case, QUIC[737,751), the exact
local chain is:

| Boundary | Time | Evidence |
| --- | ---: | --- |
| Decode |40714 |C2116 |
| Reader mailbox send completes |40715 |C2117; measured1469us |
| Native actor route begins / ends |41164 |C2154–2155 |
| Attachment forwards to shared input, begin / complete |41786 /41789 |C2156–2157 |
| Shared input dequeues |41916 |C2192 |
| Mux F737→751 |41917 |C2193 |

The450ms before routing,622ms before shared-forwarding begins, and131ms
thereafter locate distinct boundaries. They are not exclusive CPU or exact
channel-blocked durations. Routing reports mailbox_free0 but result=queued;
shared forwarding reports131 used slots. Do not substitute free-slot snapshots
for the actual queued result or for a measured blocked interval.

The largest winning copy's complete postdecode residence is instead TCP
[185,198):1352ms, C625@24977→C736@26329. It is the wire2/runtime1/physical3/
attachment3 repair, published and flushed S128/S133–134@18584. Its route is
C689@25922, shared send C711–712@26225 and dequeue C733@26329. However, F185
only becomes current at C672@25496: its actual held-frontier overlap is833ms,
not1352ms. Adjacent winning[198,211) and[211,224) release in that same batch
at C737–738@26329, without additional frontier gaps.

Likewise, QUIC[835,849) has8814ms write→decode residence, S669–671@42705 to
C2646@51519, then588ms to C2703@52107. F835 itself only holds655ms after
C2645@51452. This is not an8.8s current-frontier stall.

### Winning predecode intervals include local reader service

The archived reader hook accumulates completed, sequential sends of nondata
frames into the reader channel, then resets after each response frame's own
mailbox send completes. These are elapsed awaits, including scheduling time;
the fields do not classify all predecessors as DataACKs or provide their sizes.

For F821, previous response mailbox completion C2484@47650 resets the counters.
C2638@51020 reports2198 preceding nondata sends totaling3361778us. Only the
reported637ms between47650 and current-frontier onset48287 can precede this
hold. Subtraction gives2724778us; allowing millisecond-boundary rounding, a
conservative **at least2.722s** of sequential send-awaited elapsed overlaps
the actual2733ms predecode portion of the F821 hold. The following432ms
decode→mux is separate. This is a local reader-service bound, not an assignment
of the whole8626ms write→decode residence to current-frontier delay or CPU.

For F68, C128@15232→C264@18208 contains2729 preceding sends totaling2923982us.
At most the reported237ms precedes winning write completion15469. Thus at
least approximately2.685s, conservatively allowing timestamp rounding, lies
within its2739ms write→decode interval. These two winning intervals cannot
be called pure network delay; the actual response's native arrival time is
still unobserved, and awaited elapsed is not exclusively channel backpressure.

Contrary case F681 reports only1us preceding send time at C1974; its hold is
chiefly prepublication. F667 holds2357ms, C1864@33819→C1929@36176, but its
winning QUIC Original S572–574@33564 reaches decode C1922@36169 and mux7ms
later. One postdecode bottleneck cannot explain every long gap.

The sole winning QUIC repair[877,891) closes the separate repair-H3 path:
S792–794@52672, request8 decode/route C2708@52704, shared send
C2748–2749@53007/53009, dequeue/mux C2750–2751@53051. Its write→decode is32ms
and postdecode347ms. Its original TCP wire1 publication S774@51670 and losing
TCP repair siblings are not substituted for this winning chain.

These observations identify actual return-service boundaries after the exact
copy-debt index change. They do not identify every intervening work item, prove
an exclusive lock/backpressure cause, or justify a new queue/policy correction.
Native write completion is local acceptance, not peer arrival; prepublication
time is not itself a server-scheduler defect. No ordinary acceptance or causal
explanation of its11s confirmation gap follows from this shorter diagnostic.

## Aggregate service costs aligned to the winning replies

Recorded:2026-09-08. Category: completed diagnostic attribution, not a runtime
correction or performance acceptance. This section uses the same completed
`./.tmp/reflection/results/mixed-combined-up-copy-debt-service-0908/` capture
and archived overlay/raw evidence linked above. C denotes a one-based
`client.log` line. Short timestamps below retain the preceding section's
Unix-millisecond origin1788850700000. No process-relative clock is joined
across processes.

### What these costs measure

The reused aggregate recorder applies `elapsed.as_micros().max(1)` to every
completed scope. Consequently,1us-per-call values are resolution-floor
dominated, not evidence of that much CPU consumption. Means below are computed
from recorded total microseconds divided by calls; they do not recover
submicrosecond durations. Synchronous elapsed time can include lock waiting
and scheduler preemption. It is not CPU time.

The new `request.*` scopes have these boundaries:

| Component suffix | Actual measured boundary |
| --- | --- |
|`recovery_range_prepare` |Two separately ended phases: queued/cache preview and exclusion setup before target selection; then the final clipped slice/frame/cause construction before Apply. Calls count phases, not recovery attempts. |
|`recovery_target_selection` |The complete regular/backup pass in `reinjection_path_snapshot`. |
|`native_resolve` |Detached all-path Native reads, including synchronous authority-lock waiting. |
|`scheduling_projection` |Native receipt validation and context health/Product projection. |
|`queued_copy_debt` |Exact-target queued repair accounting, preserving the existing `exclude_front` distinction. |
|`repair_apply` |Reinjection-only `send_frame_at_frontier`, including repair callers outside structural recovery. |
|`fenced_product_commit` |Reinjection-only Product flight/qualification/load/receipt bookkeeping. |

Native resolution/projection also execute outside structural recovery. Claim
scopes run concurrently in writers; actor preparation scopes start after
acquiring Product and therefore omit that acquisition wait. The claim scope
includes the synchronous claim after weak-registration upgrade, not an early
failed upgrade. ACK, Product ACK and flight release nest, as do recovery and
Apply scopes. Their totals cannot be summed into exclusive costs or divided
by one worker's wall time to infer utilization.

Periodic rows contain cumulative and interval fields. Whole-capture values
below use each component's last cumulative row: an inactive component need
not emit again at final close. Intervals use adjacent actual flush timestamps,
not the nominal `interval_ms=1000`; two quiet intervals below last1490/1139ms.
A flush takes about1–2ms across component rows. Completed scopes are attributed
at completion and may cross a flush boundary, so even a wholly contained
flush bracket is not a claim that every included scope started inside it.
This also explains occasional nested-scope count differences across buckets.

The event filter was
`server_sender_dispatch,receive_hole,receive_hole_release,response_tcp_handoff,response_quic_handoff,response_quic_reader,response_handoff,response_handoff_mux_apply,client_cost_profile`.
`client_cost_profile` enables aggregates without emitting a per-attempt event;
`MPTUNNEL_LAB_PERF_SAMPLES` was unset. The sparse frame observers log response
StreamData, not every ACK. Nondata reader predecessors are not all identified
as ACKs.

### Whole-capture totals

Durations are recorded seconds; means and maxima are microseconds. Every
component in these tables has the `request.` prefix.

| Component | Calls | Total s | Mean us | Max us | Last evidence |
| --- | ---: | ---: | ---: | ---: | --- |
|`recovery_range_prepare` |710894 |0.738753 |1.039 |2634 |C2736 |
|`recovery_target_selection` |677233 |0.787405 |1.163 |2749 |C2737 |
|`native_resolve` |688345 |0.724257 |1.052 |2665 |C3247 |
|`scheduling_projection` |708058 |1.852427 |2.616 |5236 |C3252 |
|`queued_copy_debt` |95135 |0.095304 |1.002 |32 |C2510 |
|`repair_apply` |37445 |1.166456 |31.151 |5298 |C2513 |
|`fenced_product_commit` |1366 |0.014816 |10.846 |204 |C2495 |
|`path_recovery_dispatch` |14212 |2.042711 |143.731 |8035 |C2729 |
|`path_recovery_collect` |23033 |2.660879 |115.525 |5278 |C3248 |
|`dispatch_handler` |25523 |2.146539 |84.102 |8158 |C3240 |
|`dispatch_collect` |25523 |2.719844 |106.564 |5285 |C3239 |
|`ack_handler` |17721 |8.160194 |460.482 |8250 |C3238 |
|`product_ack_transaction` |4786 |7.925317 |1655.938 |8201 |C3250 |
|`flight_ack_release` |4786 |7.808784 |1631.589 |8176 |C3241 |
|`prepared_claim.blocked` |26164 |13.800723 |527.470 |23378 |C3132 |
|`prepared_claim.busy` |200 |0.065670 |328.350 |4038 |C3133 |
|`prepared_claim.claimed` |19516 |7.684838 |393.771 |12799 |C3134 |
|`prepared_claim.empty` |21145 |5.662917 |267.814 |16531 |C3135 |

Claimed bytes total366018560, exactly the completed transfer. The last claim
rows are at56902; the range/target rows at52897; queued-debt/Apply/fenced rows
at47889; final Native/projection/ACK rows at57142. Flight release accounts
for about95.69% of the enclosing ACK-handler recorded elapsed over the whole
capture. This locates substantial work inside ACK handling, not its causal
share of any particular frontier hold.

| Other component | Calls | Total s | Mean us | Max us | Last evidence |
| --- | ---: | ---: | ---: | ---: | --- |
|`loop_prepare.admission` |137687 |5.338625 |38.774 |5889 |C3242 |
|`loop_prepare.topology` |137687 |5.061394 |36.760 |4212 |C3245 |
|`loop_prepare.disconnected` |137688 |0.148103 |1.076 |3287 |C3243 |
|`loop_prepare.waits` |137687 |0.148022 |1.075 |943 |C3246 |
|`loop_prepare.local_write` |229 |0.000272 |1.188 |16 |C3244 |
|`ack_gap_evaluation` |142473 |3.420579 |24.009 |5065 |C3237 |
|`path_staleness` |5213 |0.063296 |12.142 |753 |C3249 |
|`retained_frontier` |137538 |0.673913 |4.900 |5179 |C3251 |
|`source_admission` |137687 |0.713065 |5.179 |2813 |C3253 |

The separately awaited TCP reader queue-send aggregate is89.239359s over66936
calls, maximum751979us(C3254); TCP routing is68.784028s over57215 calls,
maximum743905us(C3255). These are waits summed across tasks, not independent
CPU costs or an explanation of every delay.

### Contained windows: F737 and F68

F737's exact winning QUIC[737,751) decode40714→mux41917 has1203ms local
residence. The central flush bracket40877→41879 lies wholly inside it.
F68's write15469→decode18208 includes the independently established at-least
2.685s reader-send-awaited overlap. The bracket16423→17425 lies wholly inside
that interval. The full F holds remain2205ms and3240ms respectively; costs
are not assigned to their unobserved portions.

Each cell below is **total us / calls / maximum us (C line)**. `admission`
and `topology` abbreviate `loop_prepare.*`; claim rows abbreviate
`prepared_claim.*`. These are overlapping recorded scopes, not additive time.

| Scope | F737:40877→41879 | F68:16423→17425 |
| --- | --- | --- |
|ACK handler |118802 /159 /5320(C2162) |100705 /397 /2185(C225) |
|Product ACK |111965 /54 /5116(C2179) |95100 /98 /2116(C242) |
|Flight ACK release |109822 /54 /5085(C2166) |94066 /98 /2099(C229) |
|Dispatch collection |44275 /313 /3005(C2163) |52150 /722 /388(C226) |
|Dispatch handler |372511 /313 /5203(C2164) |316379 /722 /4046(C227) |
|Recovery collection |43348 /294 /2994(C2172) |50750 /631 /384(C235) |
|Recovery dispatch |371503 /296 /5202(C2173) |310847 /643 /4016(C236) |
|Admission preparation |188673 /1936 /4381(C2167) |222497 /4392 /712(C230) |
|Topology preparation |92109 /1936 /3707(C2169) |99608 /4391 /2995(C232) |
|Disconnected preparation |2041 /1936 /46(C2168) |4463 /4391 /18(C231) |
|Wait preparation |1965 /1936 /6(C2170) |4949 /4392 /136(C233) |
|Claim blocked |304514 /288 /11251(C2175) |311762 /468 /12550(C238) |
|Claim busy |2468 /4 /1759(C2176) |1387 /6 /554(C239) |
|Claim successful |227710 /322 /5900(C2177) |228003 /579 /7072(C240) |
|Claim empty |180821 /287 /10432(C2178) |108009 /409 /5275(C241) |
|Range preparation |32309 /31868 /91(C2181) |31840 /31508 /29(C244) |
|Target selection |47914 /22480 /1000(C2182) |45411 /23923 /1226(C245) |
|Native resolution |111918 /109939 /238(C2171) |99197 /97759 /120(C234) |
|Scheduling projection |176156 /110258 /2137(C2185) |171521 /98326 /2252(C248) |
|Queued-copy debt |9390 /9390 /1(C2180) |7611 /7611 /1(C243) |
|Repair Apply |296157 /9430 /2387(C2183) |248305 /7722 /3414(C246) |
|Fenced Product commit |47 /2 /29(C2165) |135 /14 /42(C228) |

These windows contain substantial recovery/Apply, preparation and concurrent
claim elapsed. In F737, recovery dispatch records371503us while the enclosing
dispatch handler records372511us; Apply records296157us but is not proven to
be exclusively nested in that dispatch. The far larger count of
Apply attempts than successful fenced commits is observable work, not proof
that all retries examined equivalent state. Native and queued-copy figures
near1us/call are particularly affected by the recorder's floor.

F737's neighboring flushes39876→40877 and41879→42880 overlap only about163ms
and38ms respectively of the1203ms local residence. Their recovery-dispatch
totals157732us/256calls/max6322(C2133) and229228us/518/max6985(C2216), and ACK
totals147671us/295/max7395(C2122) and120282us/220/max6952(C2204), must not be
assigned wholesale to those short overlaps. Full neighboring records are
C2122–2145 and C2204–2228.

### Contained windows: F821's local reader service

F821 holds48287→51452. Its winning predecode segment48287→51020 contains
at least2.722s of sequential nondata reader-send-awaited elapsed, established
from the exact reader counters above. Both flush brackets48890→49893 and
49893→50894 are contained in that segment. They have a different work mix
from F737/F68. Cell units and abbreviations are unchanged.

| Scope |48890→49893 |49893→50894 |
| --- | --- | --- |
|ACK handler |251258 /381 /6049(C2576) |294055 /317 /6646(C2609) |
|Product ACK |247282 /103 /5978(C2592) |290863 /122 /6438(C2625) |
|Flight ACK release |246899 /104 /5944(C2579) |285275 /121 /6399(C2612) |
|Dispatch collection |119359 /552 /3013(C2577) |106173 /549 /644(C2610) |
|Dispatch handler |44515 /553 /1363(C2578) |40792 /549 /1347(C2611) |
|Recovery collection |117931 /552 /3006(C2585) |104913 /549 /640(C2618) |
|Recovery dispatch |44206 /553 /1362(C2586) |40502 /549 /1346(C2619) |
|Admission preparation |38780 /3836 /2005(C2580) |34929 /3740 /256(C2613) |
|Topology preparation |202629 /3836 /3306(C2582) |175813 /3740 /3433(C2615) |
|Disconnected preparation |3883 /3836 /8(C2581) |3842 /3740 /26(C2614) |
|Wait preparation |4015 /3836 /55(C2583) |3964 /3739 /85(C2616) |
|Claim blocked |228135 /402 /23378(C2588) |266713 /452 /10382(C2621) |
|Claim busy |1337 /1 /1337(C2589) |553 /3 /337(C2622) |
|Claim successful |241634 /609 /6562(C2590) |265349 /712 /5783(C2623) |
|Claim empty |198960 /520 /16531(C2591) |160495 /627 /6206(C2624) |
|Range preparation |39832 /38129 /366(C2593) |39215 /37347 /1214(C2626) |
|Target selection |38159 /38129 /18(C2594) |37431 /37347 /32(C2627) |
|Native resolution |5293 /4848 /78(C2584) |5283 /4907 /33(C2617) |
|Scheduling projection |19731 /5455 /297(C2596) |19408 /5615 /201(C2629) |
|Queued-copy debt |0 completed calls |0 completed calls |
|Repair Apply |0 completed calls |0 completed calls |
|Fenced Product commit |0 completed calls |0 completed calls |

ACK release, topology/preparation, recovery collection and concurrent claims
are visible here; Repair Apply is not. Neither Native resolution nor indexed
copy-debt lookup is established as the dominant local service cost in these
two windows. The ACK/Product/flight counts straddling the bucket edge are not
evidence of missing or invented transactions.

### Contrary F639 window: little recovery work during the largest hold

F639 holds29099→33473 for4374ms. The winning Original is written at30531,
decoded at33339 and reaches mux134ms later. Three complete flush brackets
lie inside the hold and before decode:29443→30447(1004ms),
30447→31937(1490ms),31937→33076(1139ms). In all three, structural recovery
dispatch, range preparation and target selection have zero completed calls.
The first bracket is also before the winning publication, so it cannot be
described as receiver processing of that as-yet-unpublished response.

| Scope |29443→30447 |30447→31937 |31937→33076 |
| --- | --- | --- | --- |
|ACK handler |31980 /246 /3304(C1754) |10832 /33 /1309(C1801) |1222 /4 /705(C1830) |
|Product ACK |25751 /34 /1873(C1769) |9104 /12 /1092(C1816) |1060 /2 /620(C1842) |
|Flight ACK release |25389 /34 /1857(C1758) |8968 /12 /1075(C1805) |1035 /2 /605(C1833) |
|Dispatch collection |3245 /247 /116(C1755) |441 /35 /25(C1802) |53 /4 /18(C1831) |
|Dispatch handler |272 /247 /26(C1756) |63 /35 /29(C1803) |4 /4 /1(C1832) |
|Recovery collection |2792 /246 /42(C1765) |385 /34 /21(C1812) |46 /4 /15(C1839) |
|Recovery dispatch |0 completed calls |0 completed calls |0 completed calls |
|Admission preparation |294588 /1993 /4027(C1759) |60433 /452 /1447(C1806) |1537 /14 /189(C1834) |
|Topology preparation |39882 /1992 /593(C1762) |9711 /452 /469(C1809) |530 /14 /124(C1836) |
|Disconnected preparation |2109 /1992 /47(C1760) |559 /452 /11(C1807) |18 /14 /3(C1835) |
|Wait preparation |2003 /1993 /5(C1763) |458 /452 /2(C1810) |14 /14 /1(C1837) |
|Claim blocked |261771 /603 /6359(C1767) |82980 /180 /4773(C1814) |5450 /13 /1046(C1841) |
|Claim successful |3762 /8 /1503(C1768) |1327 /5 /636(C1815) |0 completed calls |
|Claim busy / empty |0 /0 completed calls |0 /0 completed calls |0 /0 completed calls |
|Range preparation |0 completed calls |0 completed calls |0 completed calls |
|Target selection |0 completed calls |0 completed calls |0 completed calls |
|Native resolution |6245 /6078 /31(C1764) |1148 /1104 /4(C1811) |56 /43 /4(C1838) |
|Scheduling projection |21570 /6087 /396(C1773) |4704 /1109 /237(C1820) |257 /43 /21(C1844) |
|Queued-copy debt |5 /5 /1(C1770) |401 /401 /1(C1817) |0 completed calls |
|Repair Apply |24 /1 /24(C1771) |27 /1 /27(C1818) |0 completed calls |
|Fenced Product commit |2 /1 /2(C1757) |2 /1 /2(C1804) |0 completed calls |

Across those3633ms, ACK-handler elapsed is44034us/283calls, flight release
35392us/48, recovery collection3223us/284, Native resolution7449us/7225 and
projection26531us/7239. Admission/topology preparation total356558us/50123us;
blocked claims350201us/796. The two repair Apply calls total51us; fenced
bookkeeping totals4us. These are still overlapping elapsed sums, but they
contradict an explanation that heavy structural recovery dispatch causes all
current long frontier holds.

The preceding flush28441→29443 overlaps only344ms after F639 begins; its
ACK105816us/598calls/max3336 and admission521033us/4315/max2943 cannot be
assigned wholly to that overlap(C1665–1686). The following33076→34150
includes predecode time, the134ms local stage and time after the mux advances;
its ACK18618us/52/max3737, admission208186us/2411/max2729 and blocked-claim
326309us/822/max8543 likewise cannot be assigned wholly to the local stage
(C1873–1894). Structural recovery dispatch/range/target calls remain absent
from those neighboring buckets too.

### Bounded conclusion

The exact winning chains establish local service delay in this capture, and
the contained cost brackets show different contributing work mixes. F737/F68
coincide with substantial recovery/Apply work; F821 instead has substantial
ACK release, preparation and collection with no completed Apply; F639 is a
contrary mainly predecode/prepublication case with little recovery work.
The whole-capture7.808784s flight-release total is therefore not a sole causal
explanation for every gap, nor an allocation of that total to F737.

Frequent Native/projection/range queries alone do not prove equivalent inputs:
membership, exact ownership, ACKs, queue debt, qualification and Native service
can change. No query cache, policy change, threshold adjustment or additional
correction is justified by these counts alone. Exclusive CPU, Product-lock
contention and executor service remain distinct causal questions. This
diagnostic's4.375291s maximum confirmation gap does not reproduce the ordinary
11.042148s gap, establish performance acceptance, or explain that ordinary
gap by substitution from a different run.
